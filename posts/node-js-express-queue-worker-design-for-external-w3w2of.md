# Node.js Express Queue Worker Design for External API Throttling

Short answer: accept work in Express, persist it before responding, and let a durable worker combine a shared token bucket with explicit 429 backoff and delayed retry. The queue, rather than an HTTP client, must own retry state so a restart cannot erase the reason a call is waiting.

This is a constraint-led design. The external API controls part of the schedule, while your service controls admission, identity, and evidence. In payment and ledger systems, I treat “exactly once” as an accounting objective built on at-least-once delivery: the request may be dispatched again, but its externally visible effect must be idempotent and explainable.

## Start with invariants, not a queue brand

An Express route should validate the command, derive a stable operation key, insert one durable job under a uniqueness constraint, and return an accepted response. It should not hold an open request while waiting for a token or a provider response. A worker then claims a due row (or equivalent message) with a lease. Every replica consults the same limiter state; a process-local bucket quietly multiplies the allowance when the deployment scales out.

I write four invariants into the design review. A logical operation keeps one idempotency key through duplicate enqueue requests, process loss, timeouts, and replay. Token consumption is atomic across workers. A 429 records a future eligibility timestamp instead of sleeping in memory. Each transition appends an audit event containing the job, attempt ordinal, old state, new state, and reason. The attempt number is not the idempotency key.

The HTTP boundary has an awkward failure window: the client can time out after the database commit but before it sees the response. The worker has another: the provider can accept a write while the worker dies before saving the result. Deduplication on the business key and replay with the same idempotency key make those windows reconcilable. An HMAC-derived key can keep a raw ledger identifier out of a request; RFC 2104 defines the construction, while secret rotation, retention, and regulated-data handling remain application decisions.

## What should a Node.js Express worker do after an external API 429?

The sequence is deliberately boring. Claim a due job, acquire one token from the shared bucket, and move `run_at` forward when the token is not available. Dispatch once. If the response is 429, honor a usable `Retry-After` value (seconds or an HTTP date); otherwise calculate capped exponential backoff with jitter. Persist that time, release the lease, and let another worker claim the job later. Token waiting is pacing, not a failed attempt; provider throttling is a recorded outcome.

Persist it.

Do not let an SDK retry state-changing calls underneath your audit layer. A transport timeout is ambiguous, so replay with the same operation key and reconcile the provider result. A business-level 4xx is normally terminal and should enter a reviewable exception state. Exhausted retries deserve an explicit review state too. They should never disappear into a generic `error` counter.

The following Go sketch shows the state transitions without coupling them to a particular Node.js queue library. In an Express deployment, `Queue.Delay` maps to a durable delayed-job update and `Bucket.Take` must be backed by one atomic shared store.

```go
package worker

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

type Job struct {
	ID             string
	IdempotencyKey string
	Attempt        int
}

type Bucket interface {
	Take(context.Context, string, time.Time) (bool, time.Duration, error)
}

type Queue interface {
	Delay(context.Context, string, string, time.Time) error
	Complete(context.Context, string) error
	Fail(context.Context, string, string) error
}

type Sender interface {
	Do(context.Context, Job) (*http.Response, error)
}

func Process(ctx context.Context, now time.Time, job Job, bucket Bucket, queue Queue, sender Sender) error {
	granted, wait, err := bucket.Take(ctx, "external-api", now)
	if err != nil {
		return fmt.Errorf("acquire token: %w", err)
	}
	if !granted {
		return queue.Delay(ctx, job.ID, "token_wait", now.Add(wait))
	}

	res, err := sender.Do(ctx, job)
	if err != nil {
		return queue.Delay(ctx, job.ID, "ambiguous_transport", now.Add(backoff(job.Attempt)))
	}
	defer res.Body.Close()

	switch {
	case res.StatusCode == http.StatusTooManyRequests:
		delay := retryAfter(res.Header.Get("Retry-After"), now)
		if delay == 0 {
			delay = backoff(job.Attempt)
		}
		return queue.Delay(ctx, job.ID, "rate_limited", now.Add(delay))
	case res.StatusCode >= 200 && res.StatusCode < 300:
		return queue.Complete(ctx, job.ID)
	case res.StatusCode >= 400 && res.StatusCode < 500:
		return queue.Fail(ctx, job.ID, fmt.Sprintf("terminal_status_%d", res.StatusCode))
	default:
		return queue.Delay(ctx, job.ID, "retryable_status", now.Add(backoff(job.Attempt)))
	}
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 6 {
		attempt = 6
	}
	ceiling := time.Second * time.Duration(1<<attempt)
	return time.Duration(rand.Int64N(int64(ceiling) + 1))
}
```

The database update behind `Delay` should compare the lease or version and append its audit event in the same transaction. That conditional write prevents a late worker from moving a completed job backward. Small detail. Large consequence.

## How do token buckets, delayed retries, and audit trails fit together?

A queue owns durable item state and leases. A scheduler decides when an item is eligible. A token bucket decides whether rate budget exists at that instant. One platform may provide all three, but the ownership boundaries still need to be visible in metrics and schema. Record limiter decisions separately from HTTP outcomes: `token_wait`, `rate_limited`, `ambiguous_transport`, and `completed` answer different operational questions.

Jitter matters because a provider outage or a shared `Retry-After` value can release thousands of jobs at once. Cap both the delay and the attempt count, then expose queue age, due-job lag, token denials, 429 rate, and reconciliation mismatches. Audit records should contain policy version, chosen `run_at`, status, and a safe request hash; authorization headers and regulated payload fields do not belong there. Compliance limits are part of correctness.

I once saw a client retry a 429 after 250 milliseconds and report only the later success. The provider counted two dispatches while our worker counted one, and a 17-minute settlement run required reconciliation before its evidence was trustworthy. The business effect survived. The audit trail did not. The defect was not a dramatic outage; it was a quiet disagreement between two ledgers of activity. We had a completion row, a normal latency metric, and a drained queue, so the usual dashboards offered no useful clue. Reconciliation forced us to line up edge request records, worker attempt identifiers, and the provider's response timestamps before we could prove which call had produced the accepted effect. That reconstruction also exposed a second risk: a manual replay could have created a new idempotency key and duplicated the business action while trying to repair the first record. Since then, the queue has been the only owner of retries, every dispatch is counted before the network call, and replay preserves the original operation key while adding a distinct attempt event. For a ledger system, that difference between a correct outcome and a defensible history is not cosmetic; it determines whether an auditor can explain the result without trusting undocumented client behavior.

I'm not sure why retry counts are still treated as sufficient telemetry; they answer a debugging question but miss the reconciliation question. Your mileage may vary for idempotent reads, where duplicate calls have no financial effect. For writes, the chain should be explicit: accepted, eligible, token granted, dispatched, delayed, dispatched again, completed, reconciled.

## Choosing an implementation boundary

| Shape | Durable delayed retry | Shared rate budget | Main trade-off |
| --- | --- | --- | --- |
| Database queue plus token state | Yes, with transactional rows | Yes, with atomic updates | Inspectable history, but contention and cleanup are yours |
| Broker plus external limiter | Yes, when delayed delivery is durable | Yes | Higher throughput, with two availability domains to operate |
| Managed workflow engine | Usually | Depends on limiter semantics | Less infrastructure, more coupling to its event model |
| Scheduled function plus database | Only if each item is modeled | Separate component required | Simple batches, weaker latency and overlap control |
| In-process timers | No across restart | No across replicas | Suitable for tests, not durable work |

The catch is that a scheduled invocation is not queue semantics. A cron trigger can wake a process, but it does not by itself provide per-item ownership, delayed retry, or a shared token account. Choose a durable queue when recovery evidence matters; choose a simpler scheduler only when losing or duplicating work is acceptable and the compliance boundary permits it. Cost includes audit storage, write amplification, replay tooling, on-call ownership, and deletion work, not just an invoice line.

## A controlled rollout for rate-limited processing

Start with a shadow limiter that records would-have-denied decisions without changing dispatch. Then enable durable `run_at` transitions for one job class, verify idempotency-key reuse during forced worker termination, and compare internal dispatch counts with provider records. Keep a manual replay command that preserves the original operation key while creating a new attempt event. Finally, test clock skew, lease expiry, malformed `Retry-After`, duplicate enqueue, and a provider response received just before process loss.

The least complex system that satisfies those tests is usually the right one. If the workload has no durable business effect, an in-process timer may be enough. For regulated writes, it is not suitable: the audit and reconciliation obligations dominate the convenience of a hidden retry loop.

Make the waiting state visible.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://developers.cloudflare.com/workers/configuration/cron-triggers/

## Further reading

- https://www.rfc-editor.org/rfc/rfc2104
- https://developers.cloudflare.com/workers/configuration/cron-triggers/
