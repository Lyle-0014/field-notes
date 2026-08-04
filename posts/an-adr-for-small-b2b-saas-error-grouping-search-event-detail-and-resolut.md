# An ADR for Small B2B SaaS Error Grouping, Search, Event Detail, and Resolution

## TL;DR

For a small B2B SaaS, choose an error grouping API by replaying the same server-error corpus through every candidate and proving the complete operator loop: search for one occurrence, inspect immutable event detail, resolve the group with an audit record, and reopen it after a controlled regression. Treat US and EU data handling, export, access, and retention as pass/fail constraints before comparing workflow convenience; an error tracker is useful evidence, but it cannot prove that a business side effect happened.

## How should a small B2B SaaS compare an error grouping API for US and EU server errors?

I would compare Rollbar, Bugsnag, Sentry, and any alternative as unproven implementations of the same operating contract. I wouldn't begin with screenshots, feature totals, or a synthetic exception from a quick-start guide. I would begin with six production-shaped events: two stack traces that should form one group despite different tenant references, one similar-looking trace that must remain separate, one retry of an existing event, one regression after resolution, and one event containing a field that policy says must be removed. Send the identical corpus to each candidate. Then ask an on-call engineer, without coaching, to find a single occurrence by correlation ID, move from group to event detail, identify its release and region, record a resolution reason, and find the regression later.

The decision is mostly about preserved meaning. Grouping may be heuristic, but the underlying event needs a stable identity; search needs to return the event rather than an approximate narrative; and resolve needs to be an operator decision, not deletion. For my payment and ledger systems, the minimum useful event has an opaque tenant reference, correlation ID, operation name, error class, release identifier, deployment region, and occurrence time. It doesn't contain authorization headers, payment credentials, raw bodies, email addresses, or idempotency secrets. The allowlist is reviewed like a schema because telemetry is another data flow subject to contractual retention, access-control, and deletion limits.

US and EU are therefore separate test lanes, not decorative tags. Before a trial passes, I want the team responsible for compliance to document where each lane may be processed and retained, which roles may inspect event detail, how deletion and export are performed, and how long audit records survive. I'm not sure why residency is so often checked after SDK installation; as far as I can tell, a reversible proof in a trial is cheaper than discovering an unacceptable data path after customer events exist. Your mileage may vary with contracts, so current provider terms and legal review must settle those questions rather than a comparison article.

## Which invariants and failure boundaries make grouping trustworthy?

My architecture decision record starts with four invariants. First, a customer transaction must not fail because error reporting is unavailable. Second, retries must preserve the application's event identity so the reporting boundary can deduplicate deliberately. Third, enrichment is allowlisted and deterministic: the same error shouldn't acquire whatever request fields happen to be nearby. Fourth, changing a group's status never mutates or erases the occurrence record. Those constraints reflect an exactly-once mindset, even though delivery across a process boundary can't be declared exactly once by wishful naming; I separate the immutable observation, the derived group, and the mutable workflow state.

The difficult boundary is silence.

I learned this from one incident: a call returned HTTP 200, the side effect never happened, and reconciliation exposed the gap 7 hours later. There was no exception to group, so the error tracker accurately showed nothing while the ledger lacked a posting that the caller believed existed. We reconstructed the path from a correlation ID and append-only command records, then compared accepted commands with completed postings; the repair decision was attached to the discrepancy record and executed under the original idempotency key. Since then, I require a positive completion signal for consequential work and a reconciliation control that compares intent with effect. Error capture covers observed failures. It doesn't cover absent effects.

I draw the boundaries this way:

| Record | Purpose | May change? | Failure response |
| --- | --- | --- | --- |
| Error occurrence | Preserve the approved facts observed at one instant | No | Buffer within a strict bound; never block the customer path |
| Error group | Derive a triage cluster from occurrences | Recomputed under a versioned rule | Review splits and merges against the test corpus |
| Workflow state | Assign, resolve, reopen, and annotate | Yes, with actor and time | Keep the prior transition in an audit trail |
| Business completion | Prove that an intended side effect occurred | Append corrections; don't rewrite history | Reconcile by idempotency key and escalate discrepancies |

This division also keeps logs in their proper role. The Twelve-Factor App describes logs as event streams and says applications should not concern themselves with routing or storage; I use structured application events as portable evidence, while grouping and issue state remain downstream views. An alert can point to a group. It can't replace a ledger control.

## What should search, event detail, and resolve prove in an error tracking trial?

A trial should produce evidence that another engineer can rerun, so I score observable tasks rather than promises. For search, start with the exact correlation ID, then combine release, environment, region, error class, and a narrow time window. Test missing and malformed fields as well. Record whether results identify an occurrence, a group, or both; those objects answer different questions. Search latency matters operationally, but I won't invent a universal threshold because team size, incident objectives, and event volume differ. Set the threshold in the trial plan before seeing results.

For event detail, verify the raw approved fields against the submitted corpus, inspect how stack traces are normalized, and confirm that retries remain distinguishable without multiplying business effects. Change one stack frame at a time to learn the grouping boundary. A sensible grouping result on the sample exception supplied by a vendor proves very little — the useful question is whether your framework wrappers, generated code, and release changes preserve the distinctions your operators need. Keep the corpus in source control without customer data, version its expected groups, and rerun it during SDK, runtime, and configuration changes.

Resolve is where a tidy UI can conceal weak auditability. I require actor, time, reason, and an incident or deployment reference for the state transition, followed by a test event that should reopen the issue. If the tool's workflow record doesn't meet the organization's audit-retention requirement, keep the authoritative transition in the incident system and store only a reference in the error platform. Don't pretend that a green badge proves recovery. The completion control described above owns that assertion.

| Trial dimension | Pass evidence | Reason to reject or defer |
| --- | --- | --- |
| Grouping | Versioned corpus yields the expected splits and merges | Materially different failures collapse into one unexplainable group |
| Search | Operator finds an exact occurrence from approved identifiers | Only free-text guessing reaches the event |
| Event detail | Submitted allowlisted fields survive; prohibited fields don't | Required evidence is absent or sensitive fields remain |
| Resolve and reopen | State changes retain actor, time, reason, and regression behavior | Resolution destroys history or can't be audited to policy |
| US/EU controls | Written processing, access, retention, deletion, and export review passes | A mandatory regional or contractual boundary remains unresolved |

Hard gates first. Workflow preferences only matter among candidates that pass them.

## How can a Go boundary preserve idempotency and audit trails without blocking requests?

The application contract should be smaller than any vendor SDK. I pass a typed, allowlisted observation to a reporter and give it a deterministic event ID derived from identifiers already stable in the business operation; the adapter may enqueue or transmit it, but the caller records reporting failure through local operational telemetry and does not convert that failure into a customer transaction failure. The event ID is for observability deduplication. It is never a substitute for the idempotency key that protects the business side effect.

```go
package errorreporting

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

type Observation struct {
	EventID       string
	CorrelationID string
	TenantRef     string
	Operation     string
	ErrorClass    string
	Release       string
	Region        string
	OccurredAt    time.Time
}

type Reporter interface {
	Capture(context.Context, Observation) error
}

func EventID(operationID, errorClass, release string) string {
	sum := sha256.Sum256([]byte(operationID + "\x00" + errorClass + "\x00" + release))
	return hex.EncodeToString(sum[:])
}

func CaptureFailure(ctx context.Context, r Reporter, event Observation) error {
	// The caller handles this error outside the customer transaction outcome.
	return r.Capture(ctx, event)
}
```

I would unit-test `EventID` for stability across retries and separation across releases, then use a fake `Reporter` to assert that no forbidden request field can enter `Observation`. An integration test can submit the versioned corpus through each adapter and save a trial artifact containing event IDs, expected group labels, query cases, and workflow transitions. Keep timestamps controlled where the test runner permits it. This makes differences visible without coupling domain code to a provider's search syntax.

There is a catch: deriving an identifier from `operationID`, error class, and release is appropriate only if those fields define one observation in your system. A batch operation that can fail independently for many items needs an item-level discriminator; otherwise distinct failures collide. Conversely, adding attempt number makes every retry unique and defeats deduplication. I can't choose that semantic boundary for another ledger. Document it beside the command's idempotency policy, review it with the reconciliation design, and make collision tests part of deployment approval.

## Why reject an all-in-one observability decision, and when is it still valid?

I reject the proposal to choose an error tracker solely because the organization already buys logs, metrics, or traces from the same place. Consolidation can reduce operational surfaces, but it doesn't prove the grouping corpus, exact search, event detail, resolve audit, regional controls, or export path. The reverse proposal is weak too: a dedicated tracker shouldn't become the only store for correlation IDs, deployment history, or incident decisions. Either shortcut concentrates evidence in a mutable operational view and leaves migration or audit reconstruction harder than it needs to be.

The rejected option is valid when a small team has measured that the integrated workflow passes every hard gate and the cost of another operational dependency exceeds a dedicated tool's demonstrated benefit. Stick with the existing platform in that case, and preserve application-owned identifiers plus incident records outside it. Choose a dedicated error-grouping service when the controlled trial shows a material triage advantage that the team will actually use. Choose a self-managed standards-oriented pipeline when contractual control or extensibility justifies owning storage, upgrades, access policy, alerting, and on-call maintenance. None is a universal winner.

Feature flags are adjacent, not an alternative to error evidence. An open-source feature-flag and A/B experimentation platform such as GrowthBook illustrates the separate rollout-control category: a flag may limit exposure or disable a change, while error events and business reconciliation determine what occurred. I don't let a rollout switch stand in for an audit trail — and I don't let an error count decide automatically whether a financial side effect is safe to replay.

The final ADR should name the chosen operating model, trial corpus version, data classification, regional decision, owners, audit-retention limit, export plan, and review date. It should also record why the other architecture remains legitimate under different constraints. That is enough. A small B2B SaaS needs a recoverable decision and a repeatable test more than it needs a permanent ranking of three brand names.

## References

- https://12factor.net/logs
- https://www.growthbook.io/
