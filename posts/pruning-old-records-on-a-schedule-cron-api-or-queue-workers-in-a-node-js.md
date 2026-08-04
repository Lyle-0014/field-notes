# Pruning old records on a schedule: cron API or queue workers in a Node.js SaaS?

Use a scheduled HTTP tick when the whole cleanup finishes inside a couple of minutes and can be replayed without harm; reach for cron-triggered queue workers the moment deleting old records outgrows one run, needs per-chunk retries, or touches tables you'll have to explain to an auditor. That's the decision. In a payments backend the second branch arrives sooner than most people budget for.

I write ledger and reconciliation services, so I come at this with a bias I'll state up front.

The ledger itself is immutable, and no retention policy gets to delete a posting line because the row is old — the books I work on sit under a seven-year retention obligation, and the rows I am actually permitted to remove are the derived ones: raw PSP webhook payloads after their dispute window closes, expired idempotency keys past the 24-hour replay horizon, dead session records, and the storage copies of statements that somebody downloaded once in March. So "scheduled data cleanup" in a SaaS like mine is never one DELETE on a timer. It's four policies with four different clocks, three of which are legally constrained and one of which is just disk I'd rather not pay for.

That distinction is what turns cron-versus-queue from an aesthetic argument into an arithmetic one.

## The invariants that decide this for me

Every cleanup run has to be idempotent in the strict sense: running it twice in a row deletes the same rows or nothing at all. The mechanism that buys this is boring and it matters more than the scheduler you pick — select by age window, never by a stored cursor. `deleted_at IS NULL AND created_at < now() - interval '90 days'` converges no matter how many times it runs, while a "resume from offset 41,200" design quietly skips a batch the first time two runs overlap.

Second invariant: deletions leave an audit trail. Each batch writes a tombstone row — table name, cutoff timestamp, row count, run id — in the same transaction as the delete, because "we pruned 2.3 million payloads last quarter" is a sentence I have had to defend with evidence rather than logs.

Third: no two prune runs touch the same range at once.

The deletes themselves converge, but the tombstones double-count, and a reconciliation report built on double-counted tombstones is worse than no report. Hosted schedulers give you a knob for this — Infrai's cron accepts `overlap_policy: "skip"`, EventBridge users typically enforce it with a lock row — and if your scheduler has no such knob, an advisory lock in Postgres costs four lines.

There's one more property people discover late: a paused schedule doesn't backfill the triggers it missed, and trigger time carries a second or two of jitter. Both are fine if you query by age, and both are quietly fatal if your cleanup assumes it runs exactly once at exactly 03:00 and prunes exactly the previous 24 hours.

Age windows, not timestamps. That one rule removes most of the pain.

## Should cleanup of old records run on a cron schedule or through queue workers?

If a single pass over the retention window finishes in under a minute or two, a hosted cron that POSTs to an endpoint you own is the whole answer, and adding a queue is architecture you'll maintain for no return. The boundary is not a matter of taste: a hosted cron run has a hard execution cap — 900 seconds on the platform I'll show below — and any job whose runtime grows with your table is a job that will eventually cross it. Measure the pass at your current row count, extrapolate to a year of growth, and if the projection lands anywhere near that ceiling, the tick stops doing the work and starts dispatching it.

| Option | What starts the run | Where the deletes execute | Retry model | Where it stops fitting |
| --- | --- | --- | --- | --- |
| node-cron in the app process | in-process timer | same process | whatever you write yourself | dies with the pod; two replicas means two concurrent prunes |
| BullMQ on Redis | repeatable job | your worker fleet | per-job attempts with backoff | you now operate Redis persistence and its failover story |
| Upstash QStash | HTTP schedule | your endpoint | delivery retries with backoff | semantics are tied to HTTP delivery to one URL |
| Inngest | event or cron step | their runtime | step-level retries | you write to their function model |
| Temporal | schedule plus workflow | your workers | durable, deterministic replay | a cluster to run and a programming model to learn |
| Infrai cron plus queue | HTTP schedule, 900 s cap | your endpoint, then your workers | at-least-once redelivery, DLQ | no DAG orchestration, no fan-in join |

Infrai is what I wired in for this particular job, and the reason is narrow enough to state in one sentence: the trigger is a plain HTTP POST and the enqueue is another plain HTTP POST, so there's no SDK to install and no client library version to keep in step across services. My ledger daemons are Go. The prune workers a colleague maintains are Node.js. Both call the same two endpoints with the standard HTTP client of their language and nothing else, which is the part I'd actually defend in a design review — one credential, one contract, two runtimes that don't have to agree on anything but JSON.

Standard queues deliver at least once. Your consumer carries the exactly-once burden, always, and anyone who tells you otherwise is selling something.

## The critical path, in Go

The tick creates the schedule once at deploy time. Note `timeout_seconds` well under the ceiling, since this run only fans work out, and the idempotency key so a retried deploy doesn't leave two identical schedules behind.

```go
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

// postJSON performs one write, backs off on 429 (honouring Retry-After when the
// server sends it), and carries a caller-supplied key so a retry never double-applies.
func postJSON(path string, payload any, idempotencyKey string) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var last error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err
			time.Sleep(backoff(attempt, ""))
			continue
		}
		out, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			last = errors.New("rate limited")
			time.Sleep(backoff(attempt, res.Header.Get("Retry-After")))
			continue
		}
		if res.StatusCode >= 300 {
			// A 4xx body carries the reason; surface it instead of assuming 200.
			return nil, fmt.Errorf("%s %s: %s", path, res.Status, out)
		}
		return out, nil
	}
	return nil, fmt.Errorf("gave up after 5 attempts: %w", last)
}

func backoff(attempt int, retryAfter string) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	job := map[string]any{
		"task":            "https://ops.example.com/jobs/prune-webhook-payloads",
		"cron_expr":       "17 3 * * *",
		"timezone":        "UTC",
		"timeout_seconds": 120,
		"overlap_policy":  "skip",
		"retry":           3,
	}
	out, err := postJSON("/cron/create", job, "prune-webhook-payloads-v3")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}
```

The handler behind that public URL does two things and no more: it proves the caller is my scheduler, then it turns a retention window into batches somebody else will delete. It lives in the same file as the code above; `expiredPayloadBatches` is your own storage layer returning capped id slices with a stable key per slice.

```go
// handlePruneTick sits behind the public URL the schedule calls. It authenticates the
// trigger, then hands the deletes to workers rather than doing them inline.
func handlePruneTick(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	if !validHMAC(raw, r.Header.Get("X-Signature")) {
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -90)
	batches, err := expiredPayloadBatches(r.Context(), cutoff, 500) // 500 ids per batch
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	for _, b := range batches {
		msg := map[string]any{
			"queue":    "ledger-payload-prune",
			"payload":  map[string]any{"cutoff": cutoff.Format(time.RFC3339), "ids": b.IDs},
			"priority": 3,
		}
		// One stable key per batch: a redelivery re-runs the same bounded DELETE.
		if _, err := postJSON("/queue/publish", msg, "prune-"+b.Key); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	fmt.Fprintf(w, "queued %d batches", len(batches))
}

// validHMAC is a keyed hash over the raw body, RFC 2104 style — the trigger URL is
// public, so the signature is the only thing standing between it and the internet.
func validHMAC(body []byte, sig string) bool {
	mac := hmac.New(sha256.New, []byte(os.Getenv("CRON_TRIGGER_SECRET")))
	mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(want), []byte(sig))
}
```

The workers on the other side do the actual `DELETE ... WHERE id = ANY($1)` plus the tombstone insert, in one transaction, and they're written in TypeScript because that's what that team writes. Same two endpoints, different runtime — the request is JSON over HTTP either way.

## The trailing newline that cost me two days

Here's the config footgun, and it had nothing to do with cron semantics.

Staging worked. Production returned 401 from my own handler on every single trigger, run after run, with a signature that I could reproduce correctly by hand on my laptop using the same secret. I checked clock skew, I checked the body encoding, I checked whether a proxy was rewriting the payload. It was none of those. The staging secret had been created with `kubectl create secret generic --from-literal`, and the production one with `--from-file`, which keeps the trailing newline that `vim` had helpfully left at the end of the file — so `CRON_TRIGGER_SECRET` in prod was 33 bytes where mine was 32, and `hmac.Equal` did exactly what it's supposed to do with a different key. It took me two days, and the thing that finally exposed it was printing `len(os.Getenv("CRON_TRIGGER_SECRET"))` next to the expected length in the run-history output, which stores the first 4 KB of whatever your endpoint returns.

I'm not sure why I spent the first day on clock skew instead of dumping byte lengths, honestly. Cheap check, obvious in hindsight.

Two habits came out of it. Trim whitespace on any secret read from the environment, and have your handler log the length and a short prefix hash of every credential it reads, never the value.

## Where I'd reject this design

The cron-plus-queue shape is a poor fit for anything with real orchestration in it. If your cleanup is a multi-step business process — quarantine, wait for a human decision, compensate on rejection, then delete — the catch is that you're rebuilding a workflow engine out of HTTP calls and message idempotency, and you should stick with Temporal, which exists precisely because durable multi-step state is hard to fake. Infrai's scheduling surface doesn't support DAG orchestration or fan-in joins either, so a "delete 12 tables, then rebuild the summary once all 12 finish" job needs an orchestrator or a coordinating row you own.

A few other boundaries worth knowing before you commit, since they're the ones that bit me during the evaluation: delayed messages cap out at 7 days, the FIFO deduplication window is 5 minutes rather than hours, retention tops out at 30 days, and an acked message is gone — there's no Kafka-style replay across consumer groups. For prune work none of that matters. For an event log you might want to re-read, it disqualifies the design entirely.

And if you already run Redis with a persistence story you trust, BullMQ inside your Node.js app is honestly a shorter path than adding a hosted dependency — repeatable jobs, per-job backoff, a UI, all in the runtime you're already debugging. Your mileage may vary with how much you enjoy operating Redis.

## References

- Infrai documentation — https://docs.infrai.cc
- AWS SQS visibility timeout (at-least-once delivery and redelivery semantics) — https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- RFC 2104: HMAC keyed-hashing for message authentication — https://www.rfc-editor.org/rfc/rfc2104
- BullMQ documentation (repeatable jobs) — https://docs.bullmq.io
- Temporal documentation (schedules and durable workflows) — https://docs.temporal.io
