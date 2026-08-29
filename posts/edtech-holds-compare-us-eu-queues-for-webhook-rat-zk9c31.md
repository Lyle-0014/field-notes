# Edtech Holds: Compare US/EU Queues for Webhook Rate Limiting, Delayed Jobs, Retry, DLQ

Short answer: choose the queue only after defining an idempotent reservation-expiry transaction; then compare US and EU deployment, delayed retry, DLQ recovery, and measured webhook rate limits against one fixed workload, because an advertised request price cannot establish the cheapest correct design.

For an edtech enrollment service, the constraint is concrete: a learner reserves a seat for a fixed hold window, payment or confirmation may arrive near the deadline, and an expiry worker must never release a seat that has already been confirmed. Queue delivery can be duplicated or delayed without corrupting enrollment only if the database, rather than the transport, decides whether the reservation can still move from `held` to `expired`.

That is the decision rule.

## How should an edtech backend compare a queue for US and EU webhook rate limiting?

Start with a workload sheet, not a pricing page. Record reservations created per region, expiry delay, payload bytes, peak expirations per second, receiver limit, expected attempts, retention requirement, and the operator time allowed for dead-letter recovery. Run the same sheet for SQS, RabbitMQ or a managed CloudAMQP deployment, Upstash QStash, and Cloud Tasks. I'm not sure which one is cheapest for an unspecified workload, and neither is anyone else: regional transfer, idle capacity, request rounding, retries, worker runtime, and operational labor can reverse a headline comparison.

The compliance boundary comes first in US/EU architecture. Identify where reservation identifiers and webhook bodies are stored, which systems can access them, how long audit evidence is retained, and how deletion requests propagate. A regional endpoint label isn't, by itself, a data-flow assessment. The exact legal and institutional requirements vary by deployment, so counsel and the institution's data owner must resolve them; a queue selection note can document controls but cannot supply that approval.

The receiver limit should be represented as an explicit admission policy, for example 40 deliveries per second with a burst of 10, rather than an informal promise that workers will “go slowly.” Delayed jobs schedule eligibility, while a rate limiter controls admission at delivery time. Those are different clocks. If 12,000 holds expire after a popular class launch, assigning the same due time merely moves the spike; workers still need a shared limiter, bounded concurrency, and jittered retry.

Finally, define the recovery contract. A DLQ is an operational state, not a trash can: every entry needs a reason, attempt history, next owner, and a redrive rule that preserves the original idempotency key. Don't issue a fresh logical command during redrive. That turns one failed expiry into two independently applicable instructions, precisely the ambiguity an audit trail should prevent.

## The invariant belongs in the reservation transaction

Model expiry as a conditional state transition. The command contains `reservation_id`, `expires_at`, and a stable `command_id`; the database transaction locks or conditionally updates the reservation, verifies that its state is still `held` and its deadline has passed, writes `expired`, releases the seat, and appends an audit record. If confirmation won the race, expiry records a no-op disposition. Repeating either outcome with the same command ID returns the stored result.

Exactly-once transport is not required for an exactly-once business effect.

The focused Go example below keeps the queue behind a small interface and puts the correctness boundary in SQL. It is intentionally incomplete as an application, but the transition itself is executable database logic rather than in-memory deduplication. The audit insert and reservation update share one transaction; a unique constraint on `command_id` is the final duplicate barrier.

```go
package expiry

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Command struct {
	CommandID    string
	Reservation string
	ExpiresAt    time.Time
}

func Apply(ctx context.Context, db *sql.DB, c Command, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var previous string
	err = tx.QueryRowContext(ctx,
		`SELECT outcome FROM expiry_audit WHERE command_id = $1`, c.CommandID,
	).Scan(&previous)
	if err == nil {
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE reservations
		SET state = 'expired', updated_at = $1
		WHERE id = $2 AND state = 'held' AND expires_at <= $1`,
		now, c.Reservation)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}

	outcome := "already_resolved"
	if changed == 1 {
		outcome = "expired"
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO expiry_audit
		(command_id, reservation_id, scheduled_for, applied_at, outcome)
		VALUES ($1, $2, $3, $4, $5)`,
		c.CommandID, c.Reservation, c.ExpiresAt, now, outcome)
	if err != nil {
		return err
	}
	return tx.Commit()
}
```

There is a subtle race worth spelling out. Suppose reservation `rsv_84C1` expires at `14:30:00Z`; confirmation starts at `14:29:59.970Z`, while an expiry attempt reaches the database at `14:30:00.012Z`. Wall-clock arrival cannot decide correctness. Both paths must contend on the same state transition, and exactly one may change `held`. A queue-side deduplication window, an acknowledgement, or a webhook's `204` response cannot prove which business transition committed. The audit rows can.

For authenticated inbound webhooks, verify the sender's message authentication before accepting a command, use constant-time comparison, and retain the key version plus a payload digest rather than scattering sensitive bodies through logs. RFC 2104 defines HMAC; it authenticates a message, but it does not provide freshness, authorization policy, idempotency, or an audit ledger. Those remain application responsibilities.

## Retries are a state machine, not a counter

A useful attempt record distinguishes `scheduled`, `leased`, `delivered`, `retryable`, `terminal`, and `dead_lettered`. Map receiver outcomes deliberately: authentication failure is terminal until configuration changes; a rate-limit response is retryable using the receiver's valid retry guidance when available; a timeout is ambiguous and therefore retryable with the same command ID. Never infer “not applied” from a missing response.

Backoff must obey two budgets: the receiver's admission rate and the reservation workflow's operational deadline. Exponential delay with jitter prevents synchronized retries, but an upper bound is still needed so an operator knows when automation stops. Store the policy version with each attempt. Otherwise, a later policy edit makes an old delivery history impossible to reconstruct.

Keep the logs boring — and useful.

At minimum, correlate `command_id`, reservation ID, region, scheduled time, attempt number, policy version, queue message identifier, receiver outcome, and audit disposition. Metrics should expose due-to-start lag, processing latency, retry count, rate-limit responses, oldest DLQ age, and the count of holds whose deadlines passed without a terminal audit disposition. Alert on the last measure, because a quiet worker can look healthy while reservations remain stuck.

Test the ugly sequence before production: duplicate the same command concurrently; confirm one millisecond before and after the deadline; lose the response after the database commit; redrive a DLQ item twice; lower the receiver rate during a burst; and move a deployment between US and EU paths without changing command identity. The assertion is never “one delivery occurred.” It is “one authorized state transition occurred, and every attempt is explainable.”

## Compare candidates with evidence, not a universal price claim

The named services expose different operating models, and current details can change. Without primary vendor documentation in the evidence set, it would be irresponsible to assert present limits, regional availability, or per-request prices here. Use the following matrix as a request for evidence during a proof of concept, then attach dated documentation and quotes to the architecture decision record.

| Candidate | Evidence to collect | Failure drill to run |
|---|---|---|
| SQS | Regional placement, delay and retention boundaries, delivery semantics, DLQ/redrive controls, full workload quote | Duplicate delivery after the expiry transaction commits |
| RabbitMQ / CloudAMQP | Hosting region, broker ownership boundary, delayed-delivery mechanism, recovery duties, idle and peak cost | Broker or consumer restart with outstanding acknowledgements |
| Upstash QStash | Target reachability, signing contract, retry and scheduling rules, regional data path, full workload quote | Receiver throttles a synchronized expiry burst |
| Cloud Tasks | Queue location, target model, dispatch controls, retry policy, retention behavior, full workload quote | Confirmation races a delayed expiry attempt |

Normalize every quote to the same month: original commands, expected retry attempts, request and payload units, cross-region transfer, worker compute, observability, support, and estimated operator hours. Do not hide RabbitMQ administration as “free,” and do not treat managed operations as zero labor. Price matters, but it is one row beside correctness, compliance evidence, recovery time, and team familiarity.

The catch is architectural fit. A self-managed broker is not suitable when nobody owns upgrades, capacity, backup, and recovery; use a managed option in that organizational situation. Conversely, stick with a broker the team can operate when broker-specific routing or network placement is a hard requirement that an HTTP-oriented task dispatcher cannot satisfy. A platform-native queue may reduce integration work inside one cloud, while a cross-cloud design may value a smaller portability boundary. Your mileage may vary because the cost of operational ownership is local, not a universal constant.

No candidate removes the transactional invariant. This matters more than the logo.

## Roll out without gambling the enrollment ledger

Begin in shadow mode: create expiry commands and compute their intended disposition, but do not release seats. Compare those decisions with the existing expiry process, investigate every disagreement, and retain command IDs so the comparison is auditable. Then enable one course or tenant in one region, cap dispatch below the receiver's measured limit, and increase traffic only while due-to-start lag and unresolved-expiry counts remain within explicit objectives.

During migration, dual-publish only if one path is authoritative and both use the same command ID; otherwise the rollout creates a second source of truth. Keep rollback mechanical: pause new dispatch, let in-flight transactions settle, reconcile audit dispositions against reservation states, and resume the prior scheduler. GitHub Actions supports scheduled workflow triggers, but its documentation should be checked before using it as a scheduling dependency; for this reservation path, the durable queue and database audit record must carry the workload's correctness contract.

Finish with a DLQ exercise involving an operator who did not write the code. If that person cannot identify the failed reservation, understand the terminal or retryable classification, correct the condition, and redrive the original command without creating a new business effect, the system isn't ready. The cheapest acceptable queue is the one that passes this test and the dated workload quote within the required US/EU boundary; anything cheaper but unreconcilable is an accounting liability.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
