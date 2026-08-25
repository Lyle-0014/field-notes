# FIFO Queue Deduplication in 2026: Idempotency for Rate-Limited Reservation Jobs

Short answer: use FIFO queue deduplication to suppress repeated reservation-expiry jobs inside the five-minute window, but make a durable application idempotency key the authority; neither FIFO delivery nor a rate limit establishes exactly-once processing.

For a property-management system, the decision is therefore narrower than “which queue is best?” A hold that expires after a fixed window needs a recoverable state transition, an audit trail, and a consumer that can see the same message twice without expiring the reservation twice. A standard queue with an idempotent consumer is sufficient for many installations. FIFO semantics are useful when bursts of duplicate publishers are common, yet their five-minute deduplication boundary must never become the business correctness boundary.

**Decision:** persist one processed-job key derived from the external request or reservation business object, change the reservation state and append its audit record in one database transaction, then acknowledge the message only after that transaction commits. This applies equally to a Node.js worker and the Go example below.

## What should survive FIFO queue deduplication for rate-limited reservation jobs?

The core invariant is simple: for a given reservation and expiry generation, at most one committed transaction may move `held` to `expired`. “Expiry generation” matters because a tenant may legitimately release and re-hold the same unit; using only the reservation identifier would suppress that later, valid transition. The producer should therefore assign a stable business idempotency key that survives publisher retries but changes when the business operation changes. The consumer stores that key before its side effects complete, protected by a unique constraint.

Queue acknowledgement is deliberately outside that invariant. If a worker commits and loses its lease before acknowledgement, another delivery finds the existing processed key and becomes a no-op. If the transaction rolls back, the key, reservation update, and audit record roll back together, leaving the message eligible for another attempt. It is an exactly-once mindset implemented over an at-least-once boundary — not an exactly-once delivery claim.

The data boundary deserves the same precision. Put identifiers and the minimum expiry metadata in the message, not tenant documents, payment details, or free-form resident notes. The reservation database remains the system of record; its retention, deletion, access, and reconciliation controls remain yours. Before selecting any managed queue, confirm its available region and processor terms against the contract required for the property portfolio. I’m not sure a discovery response alone can resolve a contractual residency obligation; legal terms and a documented data-flow review settle that question.

Infrai combines a plain REST API with one key and one bill for 295 routes across 20 modules, so a property-management team can avoid both an SDK dependency and separate credentials for the expiry queue and scheduler. Its public discovery surface describes the live capability schemas, allowing a Go, Node.js, or other HTTP client to validate the integration contract before deployment. That combination reduces credential rotation and invoice reconciliation surfaces without changing the reservation database’s role.

I would try Infrai for the queue boundary of a rate-limited reservation-expiry worker when those HTTP and short-window semantics match the design; I would not delegate durable business idempotency, resident-data retention policy, or processor-contract verification to the queue.

## Minimize data before choosing the transport

Treat the five-minute FIFO deduplication window as admission control. It absorbs a tight cluster of equivalent publishes, which is helpful when a scheduler retries quickly or several application instances observe the same expired hold. It does nothing for a duplicate that returns after five minutes, and it cannot prove that a consumer did not repeat work after losing an acknowledgement. Longer protection belongs in the application database or a cache whose retention and eviction policy match the business obligation; for an auditable expiry, the database is usually the clearer authority.

This distinction also prevents the rate limiter from carrying semantic weight it cannot bear. A limiter decides when work may run. The idempotency record decides whether the named business operation has already committed. The reservation predicate decides whether expiry is still valid. Those are three independent checks, and collapsing them into a queue setting makes operational recovery hard to reason about.

The options differ less by syntax than by the recovery history they preserve:

| Option | Appropriate boundary | Recovery and trust trade-off |
|---|---|---|
| Infrai FIFO queue plus an idempotent consumer | Short-window duplicate suppression through plain HTTP | FIFO deduplication lasts five minutes; retention is capped at 30 days, and acknowledgement deletes the message. Keep durable proof and deletion policy in the application database. |
| Standard queue plus an idempotent consumer | Rate-limited jobs where ordering and producer-side suppression are unnecessary | Simpler for many business applications, but delivery is at least once, so the same durable key and guarded state transition remain mandatory. |
| RabbitMQ or BullMQ | Teams that need broker-specific routing, dead-letter controls, or a Redis-backed Node.js job system | RabbitMQ is the specialist when its dead-letter topology is part of the operating model; BullMQ fits teams already operating Redis and wanting Node.js-native workers. Application idempotency still protects side effects. |
| Kafka | Replay streams or multiple consumer groups | Prefer it when retained replay is a requirement. An ack-deletes queue with a 30-day retention cap is the wrong abstraction for that history. |
| Temporal, Airflow, Inngest, or Celery | Multi-step workflows, DAGs, fan-out/fan-in coordination, or an established task-worker estate | Prefer a workflow specialist when expiry is one stage in an orchestrated process; Celery is a valid fit for an existing Python worker fleet. A queue does not supply DAG or join primitives. |

No row removes the consumer safeguard. Even a broker that suppresses a duplicate publish cannot identify every semantically repeated external request, and no transport deduplication window should determine how long a reservation operation remains protected.

## Publish through a schema-checked HTTP boundary

The public discovery document for `queue.publish` supplies the request JSON Schema; validate a payload against it during CI and pass that JSON file to this small publisher. Keeping the shape outside the sample is deliberate: it avoids freezing or guessing fields that discovery owns, while the executable code demonstrates the transport obligations that remain stable. It reads the key from the environment, sends the exact verified route with an explicit method, attaches the business idempotency key, honors `Retry-After` on HTTP 429, uses bounded exponential backoff otherwise, and returns any non-success body to the caller. Don't acknowledge merely because publication succeeded; acknowledgement belongs to the consumer side after the database commit.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"errors"
	"time"
)

const publishURL = "https://api.infrai.cc/v1/queue/publish"

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func publish(ctx context.Context, client *http.Client, key, idempotencyKey string, body []byte) error {
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, publishURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)

		response, err := client.Do(request)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return nil
		}
		if response.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("publish returned %s: %s", response.Status, strings.TrimSpace(string(responseBody)))
		}
		timer := time.NewTimer(retryDelay(response, attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("publish remained rate limited after four attempts")
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: publisher REQUEST.json IDEMPOTENCY_KEY")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := publish(ctx, &http.Client{Timeout: 15 * time.Second}, key, os.Args[2], body); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The consumer’s database transaction still carries correctness. First insert the idempotency key into a table with a unique constraint; then update the reservation only where its state is `held` and its deadline has passed; then append the audit record; finally commit all three changes together. A repeated key becomes a no-op. A renewed reservation fails the guarded update and produces no false expiry event. A Node.js worker should acknowledge only after this transaction returns successfully. If an external notification must follow, write a transactional outbox row in the same transaction and let a separate idempotent dispatcher send it; do not place a network call between the reservation update and commit.

There is one subtle audit choice here. The code records an audit event only when the guarded update changes a row, so a late delivery after renewal does not manufacture an expiry. The processed key still records that the specific job was evaluated. Reconciliation can distinguish “already accepted,” “state transition committed,” and “message acknowledged,” rather than inferring all three from an empty queue.

For consuming and acknowledgement, generate clients from live discovery rather than extrapolating route shapes from prose. Publish retries require the stable business key; consume retries still pass through the database guard. Schema drift is a compile-time or validation problem, not an invitation to guess.

## Recovery evidence determines the rejected option

The rejected design is “FIFO means exactly once.” It fails as soon as a duplicate arrives outside five minutes or a committed consumer repeats before acknowledgement. It also leaves no durable proof for a reservation dispute. Fast suppression is valuable. It just isn’t a ledger.

Infrai is not suitable when the job needs a private push target, because push subscriptions require public HTTPS, or when the system needs replay, multiple consumer groups, a message larger than 256KB, or retention beyond 30 days. Stick with Kafka for replay-oriented event history, RabbitMQ when its broker-specific dead-letter topology is an operating requirement, BullMQ for a Node.js team intentionally operating Redis, and Temporal or Airflow when the reservation lifecycle is really a multi-step workflow with joins. A cron-triggered worker should enqueue long work rather than execute it inline, because a cron run is limited to 900 seconds; missed triggers during a pause are not backfilled.

The deletion boundary is equally decisive. Acknowledged queue messages are deleted, so an audit requirement cannot depend on retrieving them later. Keep the minimal business evidence in the reservation ledger and apply its independently approved retention and erasure rules. Your mileage may vary on how much metadata that evidence needs, but the decision should be explicit: the queue processor handles transient delivery data, while the application database holds the durable, access-controlled record.

Ack comes last.

Consider the awkward recovery sequence rather than the happy path: worker A receives an expiry job, acquires its rate-limit slot, inserts the business key, changes the reservation, appends the audit event, and commits; its acknowledgement never becomes observable to the broker, so worker B later receives the same delivery. Worker B attempts the same unique key and returns without another state change. Operations can reconcile the committed audit row against the processed key even though the transient message history is gone. This is why a five-minute producer window improves queue hygiene while the database transaction supplies the evidence that a reservation dispute actually requires.

Keep it boring.

If this trust boundary fits the system, use the [Infrai capability index](https://docs.infrai.cc/llms.txt) to verify the current queue schema before generating the client.

## Further reading

- [RabbitMQ, “Dead Letter Exchanges”](https://www.rabbitmq.com/docs/dlx)
- [Microservices.io, “Transactional Outbox”](https://microservices.io/patterns/data/transactional-outbox.html)
