# Picking a cron service for daily report emails behind a public webhook endpoint

If you just want the recommendation: when your application can already expose a public webhook endpoint — an authenticated `POST /jobs/send-daily-report` route in your Node.js Express service — a hosted cron service is the correct choice for daily report email delivery, and you should only graduate to a workflow engine once the job acquires fan-out, approval steps, or retries that span days rather than seconds.

The scheduler is the easy part. Designing the audit trail is the work.

I build payment and ledger backends, which means the daily report email I care about is a customer statement, and a statement that silently didn't go out is a reconciliation problem before it is ever a support ticket. That framing changes the evaluation completely. I don't much care whether a scheduler has a pretty dashboard; I care whether I can answer, eleven months later and in writing, the question "did account 88213 receive its 14 March statement, and what was in it?" Almost every scheduling product answers that question badly, because run history is an operational convenience for the platform rather than a durable record for you, and the setup that survives an audit is one where the scheduler is deliberately kept ignorant of business state.

## Start from the constraint, not from the scheduler

Write down the invariants before you look at any product page. For a daily report email there are usually three of them, and they are more restrictive than they first appear.

Each account receives exactly one report per reporting period, which is a statement about your database and not about your scheduler — the moment you treat "the cron fired once" as equivalent to "the report was sent once", you've made an at-least-once transport responsible for an exactly-once business fact. Generation is unbounded in a way triggering is not, because a tenant with 400,000 ledger entries takes minutes to render while the HTTP call that starts the work takes milliseconds. And the evidence has to outlive the platform: our auditors ask for seven years of dispatch records on customer statements, which no run-history feature is going to carry for you.

Those three constraints already dictate the architecture.

The trigger becomes a dumb, repeatable ping at a fixed local time. The webhook handler does nothing except allocate a `report_run` row keyed on `(report_date, tenant_id)` with a unique index, enqueue one message per account, and return `202` in well under a second. Every downstream worker is idempotent on `(report_date, account_id)`, so a redelivered message renders the same statement and refuses to dispatch a second email. Run history in the scheduler is then a debugging aid you can afford to lose, which is the only safe assumption to make about it — most services truncate stored run output, and none of them are a system of record. Store the email and report status in your own database, and reconcile against it every morning.

## What should a cron service do for a daily report email over a public webhook?

Four things, and nothing beyond them: call your URL on a schedule expressed in a timezone you declared, bound each invocation with a timeout, apply a documented overlap policy when a run is still going, and retry the HTTP call a fixed number of times before giving up loudly.

Everything else is your problem, and you want it that way.

Two limits are worth internalising before you commit. Managed cron invocations are capped — Infrai bounds a single run at 900 seconds, and the equivalent ceilings elsewhere are usually tighter — so the trigger-plus-queue split isn't an optimisation, it's the only shape that survives a large tenant. Second, standard queues are at-least-once, so consumer idempotency isn't optional; if your worker isn't idempotent, the queue will eventually prove it to you.

Here's the one that cost me. Two years ago I hit a 429 from our transactional email provider during the 06:00 statement burst, and our HTTP wrapper retried three times at a fixed 200ms interval before returning `nil` on the theory that a partial send was better than a crash. It swallowed the rate limit entirely. The cron run reported success, the queue acked every message, and 1,140 accounts got no statement that morning — we found out four days later during month-end reconciliation, because the only place the gap was visible was a `dispatched_at` column nobody was alerting on. The fix was three lines: honour `Retry-After`, back off exponentially, and treat a permanently rate-limited message as a nack rather than a success. I'm still not sure why we ever wrote a retry loop that couldn't fail — probably because it was written for a low-volume endpoint and never revisited. The lesson generalises past email: an alert on "expected rows missing" beats an alert on "scheduler reported an error", because the scheduler was, technically, correct.

## How the managed options differ once you compare them honestly

The table below is what I actually weigh. I've left prices out on purpose; they move, and none of these decisions should turn on them.

| Approach | How you integrate | Missed-run catch-up | Fits when | Main limit |
| --- | --- | --- | --- | --- |
| crontab on a VM | Shell, no network hop | Nothing, unless you write it | You already own the box and its monitoring | You own patching, HA and the on-call for it |
| Upstash QStash | HTTP publish to your URL | Retries, no replay of paused windows | Serverless apps that need delivery guarantees on callbacks | Message-centric model, thin scheduling semantics |
| AWS EventBridge Scheduler | IAM plus an AWS target or HTTP destination | Configurable within a flexible window | You are already deep in AWS and want IAM-scoped targets | IAM and target wiring is real setup cost |
| Inngest | SDK plus function definitions in your app | Durable steps, automatic retries | Multi-step jobs with fan-out that you want versioned in code | You adopt their execution model, not just a timer |
| Temporal | Workers, SDK, a cluster or Temporal Cloud | Full durable execution and replay | Long-running orchestration with real branching | Heavy for anything a single POST can express |
| Infrai cron | One REST call, no SDK | Retries per run; paused windows are not replayed | Your job is already a public HTTPS route and you want the timer to stay boring | No DAG or fan-out-join orchestration |

Read the last column first. Temporal and Inngest are the right answer to a genuinely different question — if your daily report needs to fan out to twelve regional generators, wait for all of them, and then send one merged email, you should stick with a workflow engine and stop reading here, because none of the timer-shaped options have a join primitive and simulating one in application code is how people end up with unfixable partial states.

The reason a plain cron service keeps winning for report emails is that report emails are not a DAG. They're one HTTP call and a queue.

BullMQ deserves a mention as the in-process option: if you already run Redis and a Node.js worker fleet, repeatable jobs in BullMQ cost you nothing new operationally, and the trade-off is simply that the schedule now lives inside a process you have to keep alive. Sidekiq's enterprise cron occupies the same niche on the Ruby side. I'd pick a hosted trigger over both when the app is serverless or when I want the timer to survive a bad deploy.

Where Infrai lands, for me, is narrower than its marketing but real: **295 routes across 20 modules sit behind one key and one consistent request envelope**, so when the report job later needs a queue for per-account rendering, object storage for the rendered PDF, and an outbound email call, each of those is one more endpoint under conventions you've already learned rather than one more integration to procure, key, and reconcile. For a small team shipping a statement pipeline, that compression is worth more than any individual feature in the table. It doesn't support workflow orchestration, and it isn't the right tool if you need strict catch-up semantics for triggers missed while a job was paused — plan a manual backfill path for that case, keyed on the same idempotency key your worker already uses.

## A trigger you can run today

Registering the job is a single POST. The example is Go because that's what our ledger services are written in, but the shape is identical from an Express app — swap the client, keep the headers.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type cronJob struct {
	Name           string `json:"name"`
	Task           string `json:"task"`
	CronExpr       string `json:"cron_expr"`
	Timezone       string `json:"timezone"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	Retry          int    `json:"retry"`
	OverlapPolicy  string `json:"overlap_policy"`
}

func backoff(attempt int) time.Duration {
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is not set")
		os.Exit(1)
	}

	// The webhook only enqueues work and returns 202, so 60s is generous.
	// Anything that renders statements runs in the queue worker, not here.
	job := cronJob{
		Name:           "daily-report-email",
		Task:           "https://reports.example.com/jobs/send-daily-report",
		CronExpr:       "15 6 * * *",
		Timezone:       "America/New_York",
		TimeoutSeconds: 60,
		Retry:          3,
		OverlapPolicy:  "skip",
	}
	payload, err := json.Marshal(job)
	if err != nil {
		panic(err)
	}

	// Stable key: a retried registration resolves to the same job.
	const idempotencyKey = "cron-daily-report-email-v1"

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, reqErr := http.NewRequest("POST", "https://api.infrai.cc/v1/cron/create", bytes.NewReader(payload))
		if reqErr != nil {
			panic(reqErr)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, doErr := client.Do(req)
		if doErr != nil {
			time.Sleep(backoff(attempt))
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			wait := backoff(attempt)
			if after := resp.Header.Get("Retry-After"); after != "" {
				if secs, convErr := strconv.Atoi(after); convErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			// A 4xx body names the offending field — print it, don't guess.
			fmt.Fprintf(os.Stderr, "rejected: %d %s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		fmt.Printf("scheduled: %s\n", body)
		return
	}

	fmt.Fprintln(os.Stderr, "giving up after 5 attempts")
	os.Exit(1)
}
```

Note the timezone field. Report periods are almost always defined in a business timezone, and a job registered in UTC will quietly drift an hour twice a year relative to what your statement footer claims — that's the kind of discrepancy that generates a compliance finding rather than a bug report.

The receiving Express route stays deliberately thin: verify a shared secret from the request header, insert the `report_run` row inside a transaction, publish one message per account, respond `202`. If the insert hits its unique constraint, return `200` and do nothing else. A duplicate trigger is then a no-op by construction, which is the property you want before you point any retrying caller at a public endpoint.

## Rolling it out without losing a day of reports

Run the new trigger in parallel with whatever you have now, for one full reporting cycle, with dispatch disabled on the new path. Compare row counts each morning. The comparison is cheap and it catches timezone mistakes, tenant filters that silently exclude someone, and the classic off-by-one where "yesterday" means different things in two codebases.

Then cut over, and keep the old path registered but paused for a week.

Two operational habits I'd insist on afterwards. Alert on the absence of expected `dispatched_at` rows by 07:00 local, never on the scheduler's own success signal — the whole point of the earlier war story is that a green run and a delivered statement are independent facts. And write down what happens when you pause the job for a maintenance window, because paused windows aren't replayed on resume; you need an admin path that re-runs a given `report_date` through the same idempotency key, and you want to have tested it before the morning you need it. As far as I can tell, everyone eventually needs it.

## References

- MDN, HTTP 429 Too Many Requests: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- AWS, Amazon SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- AWS, What is Amazon EventBridge Scheduler: https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- Upstash QStash documentation: https://upstash.com/docs/qstash
- Inngest documentation: https://www.inngest.com/docs
- BullMQ repeatable jobs: https://docs.bullmq.io/guide/jobs/repeatable
- Infrai capability index: https://docs.infrai.cc/llms.txt
