# Email Bounce Complaint Suppression Polling for Auditable Seller Order Deliverability

The largest cost in a seller-notification evidence system is usually retained event data, not the poll request: multiply notification volume by feedback events per message, stored bytes per event, replica count, and retention days. A transactional app should therefore poll email bounce and complaint feedback into a compact, immutable decision record, maintain its current suppression list separately, and expire bulky raw payloads on a declared schedule. This preserves deliverability evidence that a new-order notice was allowed or blocked without treating every provider response as permanent evidence.

**Short answer:** poll delivery feedback with an overlap window, normalize bounce and complaint events into an idempotent ledger, update suppression state transactionally, and make the send path consult that state before dispatch. Keep the minimal audit facts long enough to satisfy the applicable policy; retain raw payloads only while they remain necessary for replay or dispute analysis.

## What does the evidence actually cost?

Storage dominates.

Start with a retention equation because it exposes the dominant term:

`daily notifications x events per notification x normalized event bytes x retention days x copies`.

For an illustrative capacity plan, 2,000,000 order notices per day, two normalized events per notice, 600 bytes per event, 90 days, and three copies consume about 648 GB before indexes, page overhead, backups, or raw payloads. These are planning assumptions, not a measured benchmark. Doubling poll frequency barely changes that storage term; doubling retention does.

Raw provider payloads can be much larger than the normalized facts needed to explain a decision. The useful change is to split storage by purpose. The suppression table answers a present-tense question: may this recipient receive this class of message? The append-only event ledger answers a historical question: what evidence caused that state, and when did the application observe it? A short-lived raw archive supports parser replay after a schema change.

Do not retain message bodies merely because they are available. A new-order notice may contain seller and buyer data whose continued storage increases disclosure scope. NIST SP 800-63B expresses a broader but relevant data-minimization principle: collect and retain only personal information necessary for the transaction, and document retention requirements. The precise legal retention period depends on jurisdiction, contract, and the marketplace's compliance program; engineering cannot manufacture one universal number.

## How should an app poll email bounce and complaint suppression lists?

Polling is a reconciliation job, not a timer callback. Each run reads a bounded page, converts external observations into stable internal events, commits them, and advances a checkpoint only after the transaction succeeds. An exclusive `last_seen_at` cursor is unsafe when several events share a timestamp or when visibility is delayed. Use a compound cursor supplied by the source when one exists, or reread an overlap window and depend on deduplication. The same practices apply when the transactional application is written in Node.js, although the example below uses Go so that the transaction boundary remains conspicuous.

The event identity should be derived from an immutable source event identifier. If none exists, a documented digest of stable fields can serve, but only after testing which fields remain stable across redelivery. Arrival time is not stable. Neither is page position.

Exactly-once delivery across a remote feedback source, a database, and an email sender is not a credible primitive. **Exactly-once effect inside the application is the useful target.** At-least-once ingestion plus a unique event key makes repeated polling harmless; a transaction then ties the accepted event to the suppression-state transition and its audit entry.

```go
package feedback

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Event struct {
	ID        string
	Recipient string
	Kind      string // "hard_bounce" or "complaint"
	Occurred  time.Time
}

func Apply(ctx context.Context, db *sql.DB, e Event) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO delivery_feedback
			(event_id, recipient, kind, occurred_at, observed_at)
		VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
		ON CONFLICT (event_id) DO NOTHING`,
		e.ID, e.Recipient, e.Kind, e.Occurred)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 0 {
		return err
	}

	if e.Kind != "hard_bounce" && e.Kind != "complaint" {
		return errors.New("unsupported feedback kind")
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO recipient_suppression
			(recipient, reason, source_event_id, suppressed_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (recipient) DO UPDATE SET
			reason = EXCLUDED.reason,
			source_event_id = EXCLUDED.source_event_id,
			suppressed_at = EXCLUDED.suppressed_at`,
		e.Recipient, e.Kind, e.ID)
	if err != nil {
		return err
	}
	return tx.Commit()
}
```

This focused example deliberately omits provider parsing and pagination. Those belong in adapters, where fixtures can prove that each external schema maps to the same small event vocabulary. The database must enforce a unique constraint on `delivery_feedback.event_id`; code-level checks alone race when two workers process an overlapping page.

There is another edge: the transaction can commit while checkpoint persistence fails. That is acceptable because the next poll replays already accepted events, which the unique key absorbs. Advancing the checkpoint before applying the page is not acceptable; it can create a silent evidence gap.

The gap matters.

## The send decision needs its own audit boundary

Before sending a seller's new-order notice, read current suppression state and write a decision record containing a locally generated notification ID, recipient reference, order reference, template revision, decision, policy revision, and decision time. Avoid copying the full order into this row. If the address is suppressed, record `blocked` and do not enqueue a send. If it is allowed, use an outbox row in the same transaction as the business decision so a process crash cannot leave an order marked as notified without a dispatch intent.

Fast feedback and a queued send can cross. A complaint may arrive after the allow decision but before a worker dispatches the outbox item. Recheck suppression immediately before the external send, then append the final dispatch outcome. This cannot erase every race with a remote system, but it narrows the interval and leaves evidence of both checks.

Audit records should distinguish `occurred_at`, supplied by the feedback source, from `observed_at`, assigned by the application. Clock ordering across systems is not proof of causality. Store the source event ID and parser version as well, so a later reconciliation can explain why a record changed state.

**Never infer consent or permission from the absence of a suppression record.** Suppression is one control. The application still needs a lawful basis, sender authentication, content classification, and any jurisdiction-specific requirements that apply to the notice.

## Operational checks that catch silent gaps

The useful service-level signals are about completeness rather than HTTP success: age of the last fully committed checkpoint, age of the oldest unprocessed event, duplicate-event rate, parser rejection count by schema version, and the difference between fetched and committed records. Alert on a stalled checkpoint even if every poll returns successfully. A loop that repeatedly reads the first page can be perfectly healthy by transport metrics and completely broken as reconciliation.

Testing should include the uncomfortable sequences: the same event in two adjacent pages, equal timestamps at a page boundary, a crash after event insertion, a crash before checkpoint advancement, complaint arrival between queue and dispatch, and an unknown event kind. Property tests can permute duplicate and out-of-order events and assert that the final suppression state and ledger cardinality remain invariant.

Deployment deserves the same caution. Run a new parser against captured, redacted fixtures before it owns the checkpoint. During a schema migration, keep one checkpoint owner and compare shadow output rather than allowing two versions to advance independently. Reconciliation should periodically compare source counts, internal accepted counts, rejected counts, and cursor coverage for a fixed interval; totals alone will not reveal that the wrong recipients were mapped.

Amazon SES documents account-level and configuration-set suppression mechanisms as well as bounce and complaint feedback. Other delivery systems expose different event transports and retention boundaries, so adapters must make those boundaries explicit rather than leaking them into order-processing code. Product selection should follow evidence requirements: stable event identifiers, documented redelivery behavior, exportability, timestamps with clear semantics, and a way to test feedback safely. Brand familiarity is not an audit property. The main limitation of polling is detection latency: it is unsuitable when policy requires a suppression change to become visible immediately, while event delivery can reduce latency at the cost of another authenticated ingestion path and its own retry ledger. A hybrid is justified only when the latency requirement outweighs that operational burden; otherwise, polling with overlap is easier to reconcile and audit.

## Retention is a deletion design

Define separate clocks for raw feedback, normalized events, decision records, and current suppression state. A deletion job should be observable and auditable: record the policy revision, cutoff, row count, completion time, and failures without retaining the deleted personal data in the deletion log. Legal holds require an explicit, authorized override rather than an undocumented pause in cleanup.

The deliberate sacrifice is forensic richness. Once raw payloads and old message content expire, an investigator may be unable to reconstruct an undocumented source field or reproduce an obsolete parser byte for byte. Keeping normalized event identity, relevant timestamps, parser version, suppression transition, and send decision preserves the evidence needed for routine reconciliation while accepting that rare deep investigations will have less material. That is a real cost, but indefinite retention is also a decision with compliance and breach consequences.

The resulting architecture is modest: overlap-safe polling, idempotent ingestion, transactional suppression, a rechecked outbox, and purpose-specific retention. Its value is not that messages can never fail. It is that every seller-order notification has a bounded, reviewable chain from delivery feedback to the next send decision.

## Further reading

- Amazon Simple Email Service Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Amazon SES suppression list documentation: https://docs.aws.amazon.com/ses/latest/dg/sending-email-suppression-list.html
- NIST SP 800-63B, Digital Identity Guidelines: https://pages.nist.gov/800-63-3/sp800-63b.html
- OWASP Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
