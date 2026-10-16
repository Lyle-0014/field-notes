# SMS Alerts API Demystified: Sender Registration Compliance for Report Dispatch in 2026

Short answer: keep canonical message templates in the application repository, render them against an immutable report snapshot, and treat email and SMS providers as transports rather than sources of business meaning. For a customer-support workflow that emails a generated report as an attachment and sends a field technician an SMS alert, this boundary makes consent, sender registration, retries, and delivery evidence reviewable in one place.

The deciding constraint is auditability. A provider-hosted template may be convenient, but an incident reviewer must still reconstruct exactly which report, subject, SMS text, recipient authority, and template revision produced an outbound attempt. A delivery receipt does not prove delivery to a human, and an accepted API request does not prove carrier or mailbox acceptance. Those are separate states.

## What should an SMS alerts API preserve for sender registration compliance?

This architecture decision starts with invariants because an easy SDK integration cannot repair an ambiguous communication ledger. The report bytes must come from a stable data snapshot; the attachment digest and template revision must be recorded before dispatch; each intended communication must have a durable idempotency key; and consent or another applicable authority must be evaluated for the recipient, purpose, channel, and time of send. The sending identity must also be valid for the destination. In the United States, application-to-person traffic over 10-digit long codes follows carrier registration requirements commonly described as A2P 10DLC. In the European Union, electronic marketing rules derive from the ePrivacy Directive and interact with national implementation, so one global `consented` boolean is too weak.

There is a sharp distinction between a transactional dispatch update and marketing. Do not infer that a phone number collected for a service visit authorizes unrelated promotions. Preserve the declared purpose with the evidence used at send time, and have counsel determine the applicable legal basis and retention period for each jurisdiction; architecture can preserve evidence, but it cannot manufacture permission.

The failure boundaries are concrete. Report generation can succeed while attachment storage fails. Email submission can succeed while SMS submission times out. A callback may be duplicated, delayed, or arrive after a retry has already selected a different transport attempt. The recipient can also receive the email while the system never receives a final event. State transitions must therefore be monotonic where possible, raw events must be retained according to policy, and reconciliation must tolerate an unresolved state.

Unknown is honest.

Retries are not evidence.

## Record intent before touching either transport

Use an outbox record as the accounting boundary. The business transaction creates a communication intent containing the report snapshot identifier, attachment digest, recipient references, purpose, locale, and application-owned template revision. A worker later renders and submits channel-specific attempts. This is an exactly-once mindset, not a claim of exactly-once delivery: the system guarantees one logical intent, while every external side effect remains retryable and deduplicated as far as the transport permits.

The initial design assumption is often that one `send` row can represent the whole operation. It cannot. Consider the ordinary sequence: the database commits intent 1842, the worker submits the email, the process loses its connection before recording the response, and the SMS submission has not started. A blind retry risks a duplicate email; marking the whole row failed hides a potentially accepted message; marking it complete suppresses the SMS. Splitting one intent into two attempts and many immutable events preserves all three truths without pretending that the timeout reveals what happened beyond the network boundary. This costs extra tables, constraints, and reconciliation work, but those costs purchase an answer to the question an operator will actually receive: what did this system know, and when did it know it?

A useful state model separates `intent`, `attempt`, and `event`. One intent may produce an email attempt and an SMS attempt. Each attempt owns its transport correlation identifier and submission state. Every callback becomes an append-only event with a uniqueness constraint over the transport and event identifier; a reducer advances the attempt only when the transition is valid. Never let a late `queued` callback overwrite `delivered`.

The attachment deserves its own controls. Record a cryptographic digest, media type, byte length, and storage reference rather than copying report bytes into an event log. Validate the generated file before enqueueing the email, apply an explicit retention policy, and prevent logs or SMS text from containing sensitive report fields. Google documents attachment and message-size limits for Gmail senders, but the durable operational rule is broader: enforce a configurable size ceiling before submission and test the complete MIME message, since encoding increases its wire size.

## Compare ownership models at the change boundary

| Template owner | Review and rollback | Audit reconstruction | Appropriate use | Main cost |
|---|---|---|---|---|
| Application repository | Same review path as code; revision is deployable | Render inputs and a content-addressed revision reproduce output | Operational messages coupled to domain rules | Copy changes require an application release |
| Separate content repository | Independent editorial release path | Strong if each intent pins an immutable revision | Teams with distinct content approvers | Two release systems must remain compatible |
| Transport control plane | Fast edits through a hosted editor | Depends on export, versioning, and retention | Low-risk content with little domain branching | Business meaning can drift outside code review |

For this workflow, the application repository wins because the email subject, attachment naming, and SMS wording all depend on the same report state. The choice is deliberately conservative. A template identifier such as `dispatch-report-ready/v4` belongs on the intent, while transport-specific identifiers belong on attempts; mixing them makes migrations and historical reconstruction harder.

The ownership decision does not imply that engineers approve every comma. Content and compliance reviewers can own protected paths, snapshot tests can expose the rendered result, and localization checks can run in continuous integration. Ownership here means authoritative versioning and release control, not sole authorship.

Convenience has a cost.

## The critical path in Go

The following sketch omits transport-specific SDKs on purpose. It shows the transaction boundary, deterministic idempotency key, immutable render input, and the split between email and SMS attempts. In production, the database methods must enforce unique constraints, and the worker lease must expire so abandoned work can be reclaimed.

```go
package dispatch

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
)

type Report struct {
    ID, CaseID, Revision, ObjectRef, SHA256 string
    SizeBytes                               int64
}

type SendRequest struct {
    RecipientID, Email, Phone, Locale string
    Purpose                           string
    Report                            Report
}

type Intent struct {
    Key, TemplateRevision string
    Request               SendRequest
}

type Store interface {
    CreateIntentAndOutbox(context.Context, Intent) error
}

func RecordReportReady(ctx context.Context, db Store, r SendRequest) error {
    if r.Purpose != "service_report_delivery" {
        return fmt.Errorf("unsupported communication purpose")
    }
    if r.Report.SHA256 == "" || r.Report.ObjectRef == "" {
        return fmt.Errorf("report evidence is incomplete")
    }

    material := r.Report.CaseID + "|" + r.Report.Revision + "|" + r.RecipientID
    sum := sha256.Sum256([]byte(material))
    intent := Intent{
        Key:              hex.EncodeToString(sum[:]),
        TemplateRevision: "dispatch-report-ready/v4",
        Request:          r,
    }
    return db.CreateIntentAndOutbox(ctx, intent)
}
```

The database uniqueness key should represent the business action, not a worker execution. A random key generated inside each retry defeats deduplication. Conversely, reusing one key for every revised report suppresses a legitimate second delivery, which is why the report revision participates in the example key. The precise material is a domain decision: if a recipient requests a resend, model that request as a new intent with its own reason and audit link instead of silently mutating the old one.

Before the worker sends SMS, it should resolve destination country, approved sender identity, registered use case where required, consent evidence, quiet-hour policy, and opt-out suppression. Before email, it should validate recipient state, compose a standards-compliant MIME message, attach the verified object, and apply authenticated sending configuration. Google's sender guidance covers SPF, DKIM, DMARC, TLS, alignment, and extra requirements for higher-volume senders; those controls belong to domain and infrastructure configuration, not to a template editor.

Persist the outbound attempt before the network call. If the call times out, reconcile using the idempotency key or transport correlation data before issuing a replacement. Retry only transient outcomes with capped backoff and jitter; permanent address, policy, or registration failures should enter a reviewable terminal state. This is ledger discipline: uncertainty becomes explicit state rather than an excuse to create another side effect.

## How should delivery tracking be reconciled?

Normalize transport events without erasing their source semantics. `submitted`, `accepted`, `delivered`, `failed`, and `unknown` can support an operational view, but retain the original status, timestamp, signed payload or digest, and correlation identifier. Authenticate callbacks according to the transport's documented mechanism, reject stale replays where the protocol permits it, and make ingestion idempotent.

Three numbers are more useful than a decorative success rate: intents with no attempt after the worker service-level objective, attempts stuck without a terminal event beyond a channel-specific threshold, and duplicate callback counts. Reconcile those sets on a schedule against transport records while respecting data-minimization and retention obligations. Do not label an email `read` unless a separate, legally reviewed tracking mechanism supports that claim; even then, privacy features make opens an unreliable proxy for human attention.

A deployment should include template snapshot tests, MIME parsing tests, oversized-attachment tests, jurisdiction and opt-out cases, duplicate callback tests, and a forced timeout after submission. Roll out a new template revision gradually, but pin every existing intent to its original revision. Operational dashboards should group by channel, sender identity, destination jurisdiction, template revision, and failure class without exposing recipient content.

## Rejected option and the case where it fits

This record rejects transport-hosted templates for the report workflow. The rejection is narrow: the attachment name and both channel messages are projections of case state, so moving their authoritative text into a transport control plane would split review, rollback, and audit history across systems. It would also tempt the application to send a mutable alias rather than an immutable revision.

Transport-owned templates remain valid for low-risk, channel-specific messages whose content team needs independent release authority, provided the platform supplies immutable versions, exportable history, access controls, and a tested rollback path. In that model, persist the exact hosted-template version and rendered variables with each attempt, and periodically verify that exports can reconstruct historical sends. The decision changes when editorial independence is more valuable than atomic coupling to domain code.

The application-owned approach also has a real limitation: it is unsuitable when non-engineering editors must publish urgent copy independently of an application deployment, or when a transport requires preapproved template content that cannot be represented by the repository's release cadence. Choose a separate content repository when editorial autonomy is the governing constraint, and choose a transport control plane when channel approval rules require it. The trade-off is slower copy iteration in exchange for atomic domain review and stronger reconstruction; there is no universal winner.

For a generated customer-support report, application-owned templates and a durable outbox create the clearest boundary: one auditable intent, channel-specific compliance checks, immutable evidence, and honest reconciliation of uncertain outcomes. Ease of integration should be measured by how quickly an operator can explain a disputed send, not by how few lines create the first API request.

## References

- https://support.google.com/a/answer/81126
- https://www.twilio.com/docs/messaging/compliance/a2p-10dlc
- https://www.fcc.gov/rules-political-campaign-calls-and-texts
- https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:02002L0058-20091219
- https://www.rfc-editor.org/rfc/rfc5322
- https://www.rfc-editor.org/rfc/rfc2045
