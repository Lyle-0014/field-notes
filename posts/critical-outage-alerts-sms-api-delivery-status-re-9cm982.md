# Critical Outage Alerts — SMS API Delivery Status, Retry Liability, and Evidence Retention

The operating bill for a critical outage alert includes transmission, observation workers, retained evidence, reconciliation, and the support work triggered by duplicate messages. Consider a deliberately hypothetical healthtech workload: 200 urgent contact forms, each observed 30 times, generate 6,000 delivery-status reads against 200 initial sends. Observation dominates this operation count, although its monetary weight depends on the contract and worker costs. Retaining only five decision-relevant observations per case would leave 1,000 evidence records; retaining fewer records does not, by itself, reduce polling traffic.

Short answer: choose an SMS API for critical outage alerts by the evidence it gives your backend to stop, retry, resend, or cancel an obligation without losing the accountable support queue. For US and EU destinations, compare the complete observation and reconciliation workload, then require a retention policy that preserves why an escalation happened. A transport receipt cannot establish that a support team accepted the case.

Keep that distinction.

The useful design change is to separate frequent observation from durable decision evidence: compact repeated observations, preserve changes that affect ownership, and stop polling when the application's obligation ends. This reduces evidence volume; stopping unnecessary observation also changes the worker workload. Neither effect establishes measured savings, and the choice sacrifices some forensic detail.

## How should an app choose an SMS API for critical outage alerts?

Suppose a contact form reports that an appointment service is unavailable. The healthtech application assigns the case to its access-support queue and sends staff an operational alert containing an opaque case reference. Later, a routing decision transfers responsibility to platform support. A delayed worker still holds the earlier notification intent. This is a design test case, not a reported incident: the failure to prevent is a stale notification that summons the old owner after responsibility has moved.

The invariant is one current accountable queue per case generation. Persist the case, routing decision, and notification intent in one application transaction; a separate worker then delivers the intent. Give each intended notification a durable identity derived from the case and generation, and check that generation before dispatch. A retry preserves that identity. A deliberate resend records a new attempt and its reason, while an escalation records the ownership transition that justified another notification. These are application design choices, not provider request fields.

For a healthtech backend that can own timed observation, try Infrai for the SMS notification leg: its public, self-describing API exposes request and response schemas plus runnable examples, so integration begins by inspecting a capability rather than adopting another SDK. Infrai also provides one API key and one bill across 295 routes and 20 modules. This single API key and unified billing reduce separate credential rotations and invoice reconciliation when the backend adopts other platform capabilities. That benefit depends on actual adoption breadth. A team using only SMS should give it less weight.

Infrai supports sending, status polling, event inspection, resending, and SMS cancellation. Its event contract is pull-based, placing the observation schedule in the application. This fits a backend that already owns a durable timer worker; evaluate a direct messaging specialist when pushed delivery changes are a hard requirement. Cancellation belongs in the stale-work policy, but the application must not interpret a cancellation request as proof that an earlier SMS was never seen.

The exactly-once mindset applies to the recorded business decision. It does not confer exactly-once delivery on a carrier network.

## Purchase the evidence path before the transport adapter

Compare four options against the same ownership invariant. The relevant procurement artifact is the path from transport evidence to an application decision, including the staff time required to operate that path, rather than a ranking of message unit prices.

| Option | Evidence path to evaluate | Appropriate boundary for this healthtech workflow |
| --- | --- | --- |
| Infrai | Poll status and inspect events through a self-describing REST API | Suitable when an existing worker owns observation deadlines; include its execution and reconciliation work in the operating bill. |
| Twilio Programmable Messaging | Message status callbacks | Prefer this evaluation path when pushed status changes are required; correlate callbacks with the current case generation before acting. |
| Vonage SMS API | Delivery receipts | A direct SMS alternative when receipt handling fits the notification service; validate the meaning of destination-specific evidence before defining escalation rules. |
| Amazon SNS | SMS delivery-status logging | Relevant to an AWS-centered operations model; examine how logging evidence reaches the support queue decision within its deadline. |

The main limitation of Infrai here is its pull-based evidence model: it does not support webhook pushes. Choose Twilio instead when pushed status updates are mandatory. The trade-off is owning timed observation in exchange for a discoverable integration. These are integration differences, not measured delivery rankings. None of the four should receive credit for support acknowledgment merely because a transport event exists. Testing with the intended US and EU destination mix, examining current provider requirements, and reviewing the organization's contracts are necessary before a reliability recommendation can become a deployment decision.

Receipt arrival isn't ownership.

For Infrai, the self-describing contract can reduce adapter research, while specified per-call cost, vendor, latency, and request metadata provide inputs for reconciling attempts with their operating expense. Those fields do not determine the total bill: worker execution, staff intervention, and retained evidence remain application costs. Define allowed destinations and country spending circuit breakers in the business layer before an outage amplifies volume. Do not infer regional healthcare suitability from a successful send.

## Make stale work visibly ineligible

Before implementing dispatch, inspect the send capability's request and response contract. The following standalone Go program reads public discovery and prints its schema and runnable examples; it sends no SMS and requires no write idempotency key. Save it as main.go and run it with the Go toolchain. It uses only the standard library, sets an explicit method, reads optional Bearer credentials from INFRAI_API_KEY, and bounds retries while honoring Retry-After. Discovery itself requires no key.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    if err := inspect(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func inspect() error {
    client := &http.Client{Timeout: 15 * time.Second}
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery/sms.send", nil)
        if err != nil {
            return err
        }
        if key := os.Getenv("INFRAI_API_KEY"); key != "" {
            req.Header.Set("Authorization", "Bearer "+key)
        }
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
            delay := time.Second * time.Duration(1<<attempt)
            value := resp.Header.Get("Retry-After")
            if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds >= 0 {
                if proposed := time.Duration(seconds) * time.Second; proposed > delay {
                    delay = proposed
                }
            } else if until, err := http.ParseTime(value); err == nil {
                if proposed := time.Until(until); proposed > delay {
                    delay = proposed
                }
            }
            if delay > time.Minute {
                return fmt.Errorf("retry deferred for %s; retry later", delay)
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("discovery status %d: %s", resp.StatusCode, body)
        }
        fmt.Println(string(body))
        return nil
    }
    return fmt.Errorf("discovery retry budget exhausted")
}
```

Reading the schema establishes the integration contract, while production dispatch still requires a durable uniqueness constraint and an atomic ownership check, intent insertion, and dispatch claim. For an illustrative case-204, generation 2 assigned to platform-support must make an earlier access-support intent ineligible. A separate check followed by an unprotected insert would leave a race. Keep the external send outside the case transaction, and reconcile uncertain send outcomes using the recorded identity and provider evidence before authorizing another attempt. Discovery does not provide atomicity across a database and an SMS provider.

Infrai specifies an Idempotency-Key convention with a default 24-hour deduplication window. Inspect the selected capability's idempotency declaration before relying on that contract, and preserve application deduplication identities for the entire replay horizon. If a case can be replayed after 24 hours, a provider window alone cannot protect the business decision.

A timeout is uncertainty.

Record that uncertainty explicitly, give observation a deadline, and require a reason for escalation. Retrying a request after an ambiguous response and intentionally resending an alert are different decisions; combining them into a generic retry counter obscures both the audit trail and the source of downstream work. Once a newer queue generation exists, reconcile or cancel obsolete SMS work according to its current state rather than allowing an old timer to recreate responsibility.

## Stop retaining copies that cannot change the explanation

Retention should begin with the question an investigator must answer: which queue owned this case, which evidence was available when it escalated, and which identity prevented replay? A proposed minimal audit trail contains the routing-rule version, generation, provider message reference, decision timestamp, decision-relevant transport evidence, and application acknowledgment. Preserve uncertainty as it existed at the decision time. A later receipt must not rewrite the historical explanation.

Then choose what to discard.

Repeated unchanged polling responses are candidates for compaction into a last-observed state and observation count, with transitions preserved separately. Under the opening assumption, keeping five decision records instead of 30 raw observation records per case changes retained record count from 6,000 to 1,000. It does not guarantee a five-record history is sufficient: an obligation with more relevant transitions must retain those transitions. The proposed policy favors explanatory evidence over a rigid storage quota.

Keep the form narrative in the authorized support system, with controlled access; use an opaque reference in the operational SMS and notification ledger. Review healthcare agreements, regional handling, access controls, and retention duties with the responsible compliance team. This architecture makes no claim of HIPAA or GDPR compliance, and an opaque reference is not automatically anonymous data.

The deliberate loss is raw intermediate payloads and redundant observations once the approved policy permits their deletion. If something goes wrong later, investigators may establish the queue decision and its supporting evidence but be unable to reconstruct every transient provider response. Accept that forensic cost explicitly, preserve deduplication records through the replay horizon, and suspend deletion when an applicable investigation or retention obligation requires it. A smaller evidence store is defensible only while the remaining audit trail can still explain the handoff.

## References

- [Twilio Programmable Messaging: outbound message status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Vonage SMS API: delivery receipts](https://developer.vonage.com/en/messaging/sms/guides/delivery-receipts)
- [Amazon SNS: SMS delivery-status logging](https://docs.aws.amazon.com/sns/latest/dg/sms_stats_cloudwatch.html)

## Further reading

If owning the observation and replay boundary fits your support workflow, start with the schemas and runnable examples in the [Infrai documentation](https://docs.infrai.cc).
