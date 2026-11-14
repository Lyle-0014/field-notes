# E-commerce SMS Alerts API: Owning Templates, Delivery Evidence, and Recipient Suppression

The bill for an e-commerce SaaS sending SMS alerts through an API should be examined as several separate terms: submission charges under the chosen contract, delivery status polling, retained evidence, and the engineering work needed to reconcile uncertain outcomes. For a concrete sizing exercise, assume 100,000 order alerts and three status observations per alert. If every submission and observation is retained as a separate row, observations contribute 300,000 of 400,000 rows, or 75%. This is an illustrative record-count model, not a traffic measurement or a claim that storage dominates the monetary bill. Row size, retention duration, and contractual charges decide that.

**Short answer:** Keep template ownership and recipient suppression in the application's control, retain a compact delivery decision with its template revision, and give repetitive observations a shorter, explicit lifetime. Choose an SMS alerts API by whether it can support that evidence lifecycle, including status polling and cancellation, rather than by a headline unit price. For a SaaS backend serving US and EU customers, the simplest integration is the one whose uncertain outcomes can be reconciled without submitting an order alert again blindly.

Keep the evidence.

## What delivery status evidence should a SaaS SMS alerts API retain?

In the sizing exercise above, replacing three retained observations with one current status reduces observation rows from 300,000 to 100,000. Together with the original submission rows, that leaves 200,000 rows instead of 400,000. It does not establish a corresponding reduction in charges or bytes, and it sacrifices the intermediate sequence unless that sequence is retained elsewhere. That distinction matters: a projection sufficient for a customer-support screen may be insufficient for explaining why a recipient was suppressed before an order update went out.

Use two evidence lifetimes. The proposed durable decision record contains the application attempt identifier, order reference, channel, recipient reference, template revision, scheduling intent, normalized outcome, suppression decision, and timestamps needed for reconciliation. The temporary observation record contains the evidence used to reach that decision. Assign its expiry deliberately. Both records still need an access policy; calling a record an audit trail does not justify keeping it indefinitely.

A delivery observation is evidence about one attempt. A suppression decision governs future attempts. Treating those as the same record creates an avoidable trap: expiring old delivery history can accidentally remove the only record preventing another send to an invalid destination. Instead, let the suppression record survive under its own policy, with a reason and a reference to the evidence that established it. Record reversals too.

For selection purposes, ask a candidate API to demonstrate how an adapter retrieves status for an existing attempt and identifies an outcome precisely enough to support a decision. Do not assume that a generic failure label proves a destination invalid. If the documented evidence cannot distinguish an invalid destination from a temporary transport problem, the application should preserve uncertainty and route the case for reconciliation rather than create a permanent suppression.

This boundary is also where email and SMS must remain distinct. RFC 8058 specifies an email one-click unsubscribe mechanism; it does not define SMS suppression or a cross-channel consent policy. The Mail Privacy Protection guide describes privacy behavior for email. Neither source establishes SMS delivery semantics, so neither can certify a candidate's polling or cancellation behavior. Their useful contribution here is narrower: channel-specific evidence needs channel-specific interpretation.

## Template ownership determines what an audit can explain

For this design, the application owns the semantic template revision, even if another system renders or stores the message. Consider an illustrative order reference, `order-1042`, whose shipping alert uses revision `shipping-v7`. The revision should identify an immutable definition of purpose, required variables, and approved content. Reusing the label after changing its meaning would make an old delivery decision ambiguous: the audit would identify a template name without establishing what that name meant when the decision occurred. Keep the revision distinct from the rendered body. The former supports explanation and reproducibility; the latter can contain recipient or order details that deserve a shorter lifetime. Retaining a revision alone will not reconstruct an exact message when the variable values have expired. Accept that loss explicitly, or document why a narrowly scoped record of those values is necessary. A content digest may help check a surviving copy, but it cannot recreate deleted content and should not be treated as anonymization.

The primary comparison is therefore between application-owned definitions and externally owned mutable templates. Require an integration with external rendering to preserve a revision association that the application can audit; where it cannot, the team must either retain enough approved material locally or accept a weaker explanation of historical sends. The trade-off is editing convenience versus historical explainability, because mutable external content requires additional evidence to establish what was approved. External ownership may reduce editing work for engineers, while local ownership puts review, versioning, and deployment on the application team.

Choose deliberately.

The same revision must be attached before scheduling an alert. Otherwise, a queued shipping notice could acquire different wording when a template is edited later, leaving the suppression and dispatch decisions attached to a moving target. The desired contract is an explicit snapshot or revision binding; verify that behavior in documentation and acceptance tests instead of inferring it from the presence of a template identifier.

## Put the recipient decision next to the dispatch boundary

A proposed suppression key is tenant, channel, and recipient reference. The tenant boundary expresses this design's isolation policy; it is not a claim that every deployment must scope every suppression that way. A broader platform policy may also be needed, but it should have an explicit authority and reason. Do not turn an email unsubscribe into an SMS opt-out merely because both destinations belong to one customer account.

The Go example below is a pure policy function for a hypothetical application. Its values are internal states, not an external API's status vocabulary. It exposes which evidence changes the disposition of an alert and leaves persistence and dispatch outside the function.

```go
package alerts

type Evidence struct {
    RecipientInvalid bool
    ChannelOptOut    bool
}

type Attempt struct {
    ID               string
    TemplateRevision string
    Cancelled        bool
    Suppressed       bool
}

type Decision struct {
    AttemptID        string
    TemplateRevision string
    Action           string
    Reason           string
}

func Decide(a Attempt, e Evidence) Decision {
    d := Decision{AttemptID: a.ID, TemplateRevision: a.TemplateRevision}
    switch {
    case a.ID == "" || a.TemplateRevision == "":
        d.Action, d.Reason = "hold", "missing_provenance"
    case a.Cancelled:
        d.Action, d.Reason = "stop", "application_cancelled"
    case a.Suppressed || e.ChannelOptOut || e.RecipientInvalid:
        d.Action, d.Reason = "stop", "recipient_policy"
    default:
        d.Action, d.Reason = "eligible", "policy_clear"
    }
    return d
}
```

Eligibility is provisional.

The function is deterministic, so replaying identical inputs yields the same decision; that property alone does not make an external send exactly once. Persist the attempt and its dispatch intent under an application idempotency key, and commit the associated audit decision in the same transaction. Use a uniqueness constraint for that key. When submitting externally, require documented duplicate-handling behavior or a documented way to reconcile an uncertain submission; a timeout must not become permission to create a fresh alert automatically.

For observations, persist a documented stable event identity when one exists, or define an adapter-specific deduplication policy whose limitations are understood. Deduplicating transport events and deduplicating business intent solve different problems. One order alert may generate several legitimate observations, and two identical-looking alerts may represent different order events. The audit trail needs enough provenance to tell those cases apart.

Recheck recipient policy immediately before dispatch. That narrows the window between a newly recorded suppression and a scheduled send, although a local check cannot recall an attempt already accepted by an external system. A strict ordering requirement needs a defined serialization boundary and documented external guarantees. Keep that limitation visible in the design review.

## Reconcile polling and cancellation before expanding the rollout

In a Node.js SaaS backend, keep the adapter contract independent of the runtime: submit an application attempt, inspect its documented status, and request cancellation where that operation is supported. The Go policy example expresses the business rule; it is not a runtime-specific client recipe. Require each candidate to explain terminal states, ambiguous results, polling limits, and the point beyond which cancellation cannot take effect. Those answers must come from that candidate's public contract, since email standards cannot supply them.

Cancellation needs two records in this design: the application's request to stop and the external disposition of the scheduled attempt. Recording the request immediately prevents the application from intentionally dispatching it later. It does not establish that an already submitted message was recalled. Until there is adequate evidence, preserve an unresolved disposition instead of showing a successful cancellation. That is the same reconciliation discipline used for uncertain submissions.

Test the races with illustrative fixtures: `order-1042` is scheduled, its recipient becomes invalid, and a previously captured dispatch task resumes; another attempt receives duplicate observations; a third times out during cancellation and later produces delivery evidence. Verify that a stale task rechecks suppression, a duplicate observation does not multiply audit effects, and conflicting evidence remains explainable. Add a template revision change between scheduling and dispatch to check that the original binding survives. These are acceptance cases, not reported production incidents.

For deployment, first compute decisions alongside the existing workflow without dispatching additional messages, compare the results, then enable enforcement in a bounded cohort. Observe unresolved attempts, suppression decisions by reason, reconciliation age, and template revision coverage. Prefer recipient references over rendered bodies in routine telemetry. A dashboard can show fewer failures while hiding an expanding queue of unresolved submissions, so measure that queue directly.

US and EU operation also requires jurisdiction-specific legal review of purpose, consent, opt-out handling, access, and retention. Neither an unsubscribe implementation nor a delivery receipt establishes regulatory compliance. This note defines engineering controls; it does not prescribe a universal retention period or assert that a region label settles the legal boundary.

## Retire observations without erasing the reason for suppression

Return to the dominant term in the example: repeated status observations. A compact projection and a shorter lifetime for raw evidence move that term, while application-owned template revisions preserve an explanation of the decision without requiring indefinite retention of every rendered order notice. Polling frequency remains a separate control: shortening retention changes stored history, whereas polling less often changes observation work and can delay discovery of an outcome. Do not confuse those effects when assessing the bill.

At each retention review, ask which unresolved cases still require their underlying evidence and which completed cases can be explained by the durable decision record. Give any exception a reason, owner, and expiry; an unbounded exception would defeat the retention policy. Preserve suppression independently so cleanup cannot silently authorize another attempt.

The deliberate deletion is specific: repetitive polling snapshots, routine rendered bodies, and expired variable values whose purpose has ended. The cost appears during an investigation. Support may be able to establish the template revision and suppression reason while being unable to reconstruct the exact body or sequence of intermediate observations. If that loss is unacceptable for a defined class of alerts, retain narrowly scoped evidence for that class under a reviewed policy. Otherwise, accept the limit and document it.

## References

RFC 8058 supports the email unsubscribe boundary discussed above. The Mail Privacy Protection guide supplies email privacy context. Neither reference documents an SMS service, its prices, delivery guarantees, cancellation contract, or legal sufficiency; the SMS workflow in this note is a proposed application design with explicit acceptance criteria.

## Further reading

- RFC 8058: One-Click Unsubscribe — https://datatracker.ietf.org/doc/html/rfc8058
- Mail Privacy Protection guide — https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios
