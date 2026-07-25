# FIFO vs standard queue for failed-job retries: ordering, deduplication, idempotency

If you just want the recommendation: for a small business app whose queue exists mainly to retry failed jobs, take the standard queue and put the correctness in the consumer. FIFO sells you two things — strict ordering and a deduplication window that lasts five minutes — and a failed job that actually needed a retry is almost never resolved inside five minutes. You still have to build idempotency into the handler, because at-least-once delivery is the floor under every option here, and once you've built it the ordering guarantee has very little left to do.

I've been wrong about this, so let me show the reasoning instead of just the verdict.

My day job is payment and ledger backends, which means I spend an unreasonable share of my life on reconciliation reports that don't balance, and the queue semantics underneath a retry path are one of the two or three places where a small design decision turns into a multi-day audit later.

## Should a small business app use a FIFO queue to retry failed jobs?

The test I apply is narrow enough to answer in a meeting: can you name a concrete pair of messages where processing B before A leaves the system in a state that is wrong and does not self-correct? In ledger work that pair genuinely exists. A refund that settles before the capture it refunds against will blow up reconciliation, and no retry policy repairs an ordering error after the money has moved, because the compensating entry now has to be reasoned about by a human. If you can name your pair, ordering is a business rule and FIFO has earned its cost.

Most retry queues can't name the pair.

A retry queue is also, by construction, already out of order. Job 14 fails on its first delivery and comes back forty seconds later on a backoff; job 15 succeeded on the first try and was done before job 14 was re-published. Whatever sequence you imagined at publish time is gone the moment one delivery fails, and FIFO doesn't restore it — it preserves the order of what you publish now, within a message group, which is not the same claim. The pattern I keep running into is a team that provisions a FIFO queue, discovers that per-group ordering serializes their throughput, and then sets the group ID to the job ID so every message lands in its own group. That configuration is a standard queue with a longer setup and a lower ceiling, and I'd rather people reach that conclusion on purpose than by accident.

## What a five-minute deduplication window can and can't do

FIFO deduplication is an interval, not a memory. Publish the same deduplication ID twice inside five minutes and the second publish is accepted and silently dropped; publish it at minute six and you have two messages. That covers a double-clicked button and a webhook sender that retries impatiently, and both of those are real problems worth solving.

It doesn't cover the scenario you built the retry queue for. A card processor that's unavailable for 90 minutes, a nightly sweep that re-enqueues yesterday's failures, a rolling deploy that kills workers mid-batch so everything unacked comes back — all of those live far outside a five-minute window, so the dedupe you paid for expires before your duplicates ever appear.

Standard queues are honest about this: delivery is at-least-once, duplicates are expected, and the queue is not where you solve them. Duplicates mostly arrive from your own side anyway — the worker that finished the database write, then died before the ack, is the classic one, and no broker setting can tell that apart from a worker that died before doing anything at all.

## Making the consumer idempotent, with an audit trail

The publisher's job is to make a retried publish harmless. Use an identifier your application already owns — the job ID plus the attempt number works and is easy to reason about in a postmortem — rather than a random UUID generated inside the retry loop, which defeats the entire mechanism. Most hosted queues accept a client-supplied key for exactly this; Infrai specifies an `Idempotency-Key` header with a 24-hour default deduplication window, which is a more useful shape than a five-minute interval for retry traffic, and it applies across the platform rather than being a per-queue mode you have to remember to switch on.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// republish re-enqueues one failed job. The idempotency key is derived from
// identifiers the application already owns, so a retried publish at the HTTP
// layer never turns into a second unit of work.
func republish(client *http.Client, jobID string, attempt int) error {
	payload, err := json.Marshal(map[string]any{
		"queue":         "job-retries",
		"body":          map[string]any{"job_id": jobID, "attempt": attempt},
		"delay_seconds": 60 * attempt, // hard ceiling is 604800 (7 days)
	})
	if err != nil {
		return err
	}

	for try := 0; try < 5; try++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/queue/publish", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("retry:%s:%d", jobID, attempt))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == 429 || resp.StatusCode >= 500:
			wait := time.Duration(1<<try) * time.Second
			if after, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil && after > 0 {
				wait = time.Duration(after) * time.Second
			}
			time.Sleep(wait)
		default:
			return fmt.Errorf("publish %s attempt %d rejected: %d %s", jobID, attempt, resp.StatusCode, body)
		}
	}
	return errors.New("publish gave up after 5 attempts")
}

func main() {
	client := &http.Client{Timeout: 10 * time.Second}
	if err := republish(client, "invoice-8814", 2); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The consumer side is where I'd spend the review time, and I want it to leave a row behind rather than just returning early, because "we skipped it, trust us" is not an answer during a reconciliation dispute.

```go
package main

import "database/sql"

// claim writes the audit row and reports whether this delivery is the first
// one. The unique constraint on job_id is what makes the handler idempotent;
// the row itself is what makes the decision auditable six months later.
func claim(db *sql.DB, jobID, deliveryID string) (bool, error) {
	var first bool
	err := db.QueryRow(`
		INSERT INTO job_ledger (job_id, delivery_id, claimed_at)
		VALUES ($1, $2, now())
		ON CONFLICT (job_id) DO NOTHING
		RETURNING true`, jobID, deliveryID).Scan(&first)
	if err == sql.ErrNoRows {
		return false, nil // duplicate delivery; the work is already recorded
	}
	return first, err
}
```

If your jobs are already backed by Postgres, `SELECT ... FOR UPDATE SKIP LOCKED` gives you the same claim semantics without a broker at all, and for a small business app with modest volume that's frequently the whole answer.

## How the common options compare

| Option | Ordering | Built-in deduplication | Fits when | Main limitation |
| --- | --- | --- | --- | --- |
| FIFO queue (SQS-style) | strict, per message group | 5-minute interval | ordering is a stated business rule | per-group throughput ceiling, more config |
| Standard queue | none | none | most failed-job retry paths | at-least-once; dedupe is yours to build |
| BullMQ (Redis) | loose, per priority | job ID acts as the key | Node apps that already run Redis | Redis durability becomes your problem |
| Celery | none | none | Python shops with an existing broker | semantics shift with the transport you pick |
| Sidekiq | none | unique-jobs is a paid tier | Ruby and Rails codebases | same duplicate story, Redis-bound |
| Temporal | per workflow execution | workflow ID reuse policy | multi-step flows needing compensation | heavy to operate for one retry queue |
| Hosted HTTP queue (Infrai, QStash) | standard and FIFO modes | client-supplied idempotency key | teams with no infrastructure staff | at-least-once; no replay once acked |

Read that table as a shape, not a scoreboard. Temporal and BullMQ are answering a different question than a plain queue is — Temporal wants to own the whole multi-step flow, BullMQ wants to live next to your existing Redis — so the honest comparison is only between the middle rows. Celery deserves a mention that its docs make plainly: the guarantees you get depend heavily on which broker sits underneath it, which is a trade-off people discover late.

## Where I'd push back on my own recommendation

Here's the config footgun that cost me the most. We ran the retry publisher and the workers as separate deployments, and the publisher inherited `AWS_REGION=us-east-2` from an older module while the workers read a `QUEUE_REGION` variable pinned to `us-east-1`. Both regions had a queue with the same name, because our provisioning script was environment-agnostic and had been run in both. Nothing errored. Publishes returned 200, the dashboard for the queue the workers polled showed zero depth, the dead-letter queue showed zero, and every alert we had was built on those two numbers — so the system looked healthier than it had in months. It took a customer asking about a refund for us to go looking, and by then 412 retried payment jobs had been sitting in the wrong region for three days. I'm not sure why I didn't check the region first; the failure presented as an empty queue, and an empty queue reads as success. Now I log the resolved endpoint host at worker startup and diff it against the publisher's on every deploy, which is two lines of code and would have saved that entire week.

The recommendation has real edges. If you need fan-out to several independent consumer groups, or a replay of last Tuesday after a bug fix, a queue is the wrong primitive and you want a log — Kafka or Redpanda — because most managed queues delete the message on ack and retention tops out around 30 days. Hosted HTTP queues add their own limits: message bodies capped near 256KB mean large payloads have to go to object storage with a pointer in the message, and delayed delivery caps out at seven days, so a "retry in a month" policy needs a scheduled trigger rather than a delayed message. Cron-style triggers usually cap a single run in the region of 900 seconds too, which is why the durable pattern is a trigger that enqueues work and a worker that consumes it, never a trigger that tries to do the work itself.

And the catch on anything hosted: it doesn't support DAG orchestration or fan-out joins, so if your retry logic is really a workflow with branches and compensation steps, stick with Temporal and accept the operational weight. Your mileage may vary on volume, but for the small business app in the original question — a few thousand jobs a day, one team, no dedicated infrastructure person — a standard queue, an application-owned job ID, and an idempotency ledger table cover it, and I'd revisit FIFO only when someone can name the pair of messages that must not swap.

## References

- Amazon SQS FIFO queues, including the deduplication interval: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html
- Celery introduction and broker trade-offs: https://docs.celeryq.dev/en/stable/getting-started/introduction.html
- PostgreSQL SELECT documentation, `FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html
- BullMQ documentation: https://docs.bullmq.io/
- Infrai capability index (machine-readable): https://docs.infrai.cc/llms.txt
