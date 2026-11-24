# Go Gaming Rollback: Reconciling Feature Flag Writes After Toggle Endpoint Errors

Use deterministic feature flag writes for a gaming agent's kill switch, and reconcile duplicate attempts after backend errors before allowing retries to admit another iteration. Rollback safety is the deciding constraint: a retry must preserve the intended disabled state even when the original write succeeded and its response disappeared.

Short answer: read the current feature flag state, persist the desired value and operation identity, then use set or an explicit rollout value rather than retrying a toggle endpoint. Measure latency and cost against the agent iteration and configuration revision, so duplicate attempts cannot masquerade as additional useful work.

This architecture decision record examines the evidence needed to resume a gaming agent loop after a rollback, including the seam between an SMS delivery status and the application's own operational logs. All numeric examples below are proposed acceptance cases, not production measurements.

## How should feature flag retries reconcile duplicate writes after a lost response?

A toggle changes the current value to its opposite. If the first request disables the agent but loses its response, repeating that request can enable the agent again; neither a timeout nor a backend integration error establishes that the original write failed. A deterministic assignment makes the desired state explicit, although concurrent writers still require a separate ordering policy.

Keep it disabled.

For this design, the application records an operation identity, the requested state, the configuration revision it observed, and the result of subsequent reconciliation. These are application audit records, distinct from diagnostic logs. Reading before writing helps identify an unnecessary mutation, but a read followed by a write is not an atomic compare-and-set, and an old retry can overwrite a newer decision unless the application serializes changes or enforces revisions at a boundary it controls. Exactly-once intent therefore means one accepted state transition per operation identity, with repeated transport attempts attached to that same decision; it does not mean that a network performs exactly one delivery.

Consider a proposed acceptance case with 2 agent iterations and 3 attempts to disable admission. All 3 attempts carry the same rollback identity. The expected final state is disabled, and the operation history contains one logical rollback with three transport attempts. A different identity is necessary for a later deliberate reenable. Do not recycle an old key merely because the desired Boolean happens to match.

The failure boundary matters.

Closing admission prevents new iterations after clients observe the state; it does not reverse an already committed game action or an SMS already accepted for delivery. Use a dedicated kill-switch flag and deterministic set logic, keep it separate from rollout percentages, and treat readback as state evidence rather than proof that every polling client has applied the change.

## Decision: reconcile outcomes before counting useful work

The measurement unit is an agent iteration, not an HTTP attempt.

Record the iteration identity, rollback identity when applicable, observed flag revision, attempted action, elapsed duration, and cost evidence in an application-owned record. These are proposed application fields, not a provider request schema. Measure local duration around each operation, retain provider-reported cost separately, and distinguish an unknown outcome from a confirmed failure; otherwise, retry traffic can inflate the denominator of a latency report while duplicate side effects make the apparent cost per successful iteration meaningless.

There is a concrete trap here: a successful rollback response cannot prove that an earlier SMS was never sent. Reconcile delivery status using the original message identity, and attach that result to the same iteration record. Do not issue another send merely to discover what happened to the first one. Repeated status reads are observational; repeated sends require their own idempotency policy.

The proposed acceptance boundary is narrow. One message identity, one iteration identity, and one rollback identity must survive a timeout and a process restart. A test should suppress a mutation response after acceptance, replay the operation, and verify both the final flag state and the number of committed application actions. Counting successful HTTP responses alone cannot establish that invariant.

For an operational report, separate time until the control service acknowledges a change from time until the gaming worker observes it. Likewise, distinguish an SMS status lookup's duration from message delivery latency. Neither number substitutes for end-to-end evidence, and this note claims no measured latency, uptime, or savings.

## Compare the evidence boundary, not the dashboard

The following options occupy different parts of the system. A selection should be conditional on the contract required at that boundary; a flag service, a messaging provider, and an observability service are not interchangeable implementations of one complete application. The comparison lists candidate uses and acceptance questions rather than asserting that a product supplies every required guarantee.

| Option | Place in this decision | Evidence required before accepting it | Appropriate boundary |
| --- | --- | --- | --- |
| LaunchDarkly or Unleash | Candidates for the flag control plane | Check desired-state mutation semantics, client observation, and required audit evidence | Retain a dedicated flag integration when it satisfies the deterministic rollback acceptance case |
| Twilio plus Datadog | Separate messaging and operational evidence boundaries | Carry the message identity into application telemetry and reconcile delivery status with the iteration record | Suitable when the team accepts ownership of correlation and reconciliation code |
| Sentry | Candidate when the primary investigation starts from application errors | Test whether an error record can be connected to the durable rollback decision and iteration identity | Evaluate for error investigation; do not infer a flag-write contract from an error dashboard |
| Grafana | Candidate when the investigation starts from operational measurements | Verify the underlying data source, correlation identities, and the evidence available after retry | Evaluate for the measurement view while keeping the mutation ledger independently authoritative |
| Shared API | One REST API contract across capability vendors: swapping the vendor behind a capability does not change calling code; per-call cost, latency, vendor, and request metadata support attribution | Check the specific capability's idempotency declaration and retain application-owned rollback evidence | SMS status and operational log ingestion use the same base URL, avoiding a second credential for the evidence handoff |

Infrai provides a single API key and one bill for multiple backend services, including the SMS status and log ingestion handoff here. Its 295 routes across 20 modules share a REST interface; replacing the vendor behind a capability preserves the calling contract. For this decision, the additional practical benefit is per-call cost, vendor, latency, and request metadata for attributing an agent iteration's work.

For a new Twilio plus Datadog arrangement, the comparison entails 2 service signups and 2 credential sets, plus glue that maps a messaging identity to an iteration, reads delivery status, and writes correlated evidence into telemetry. Existing accounts can change the onboarding work, but they do not remove that application mapping. A shared API reduces this particular credential and integration seam; it does not establish transactionality across the two operations.

One vendor to trust, one bill, and one outage surface is the combined approach's trade-off.

Acceptance should also cover what the game actually needs: threshold notifications, distributed span analysis, crash symbolication, session replay, and silent-job detection are separate requirements. Do not infer any of them from the presence of logs or trace identifiers. If a worker must prove it ran, evaluate a heartbeat tool such as Healthchecks alongside its logs.

## Critical path: carry delivery evidence into the operational record

This Go program demonstrates the narrow handoff: read an existing SMS status, then ingest a log record containing that returned JSON, using the same configured key and base URL. It sends no new message and performs no flag mutation. The rollback controller described above remains responsible for desired-state writes and durable operation identities.

Request fields must come from the current capability schema. To avoid guessing those fields, the program accepts a complete schema-valid log request as LOG_BODY_TEMPLATE, with exactly one unquoted __SMS_STATUS_JSON__ token where the returned JSON belongs. Obtain that shape from the public discovery description for log ingestion; the template must place the returned value in a field that accepts it, or encode it as the schema requires before adapting this example. No particular log field is asserted here. The template is supplied configuration, not a fabricated payload contract.

Set API_BASE_URL to the configured API root including its version, INFRAI_API_KEY to its key, SMS_ID to an existing message identity, and OPERATION_ID to a durably allocated identity for this particular evidence record. Save the code as main.go and run it with Go. Retrying this program requires the same template and operation identity. A changed record needs a new identity.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func required(name string) string {
    v := os.Getenv(name)
    if v == "" { panic("missing environment variable: " + name) }
    return v
}

func request(ctx context.Context, client *http.Client, method, target, key, id string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer " + key)
        if body != nil { req.Header.Set("Content-Type", "application/json") }
        if id != "" { req.Header.Set("Idempotency-Key", id) }
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1 << uint(attempt))
            retryAfter := resp.Header.Get("Retry-After")
            if seconds, e := strconv.Atoi(retryAfter); e == nil && seconds >= 0 {
                if d := time.Duration(seconds) * time.Second; d > delay { delay = d }
            } else if until, e := http.ParseTime(retryAfter); e == nil {
                if d := time.Until(until); d > delay { delay = d }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return nil, ctx.Err()
            case <-timer.C:
            }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s returned %d: %s", method, resp.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
    base, key := strings.TrimRight(required("API_BASE_URL"), "/"), required("INFRAI_API_KEY")
    id, operation := required("SMS_ID"), required("OPERATION_ID")
    template := required("LOG_BODY_TEMPLATE")
    if strings.Count(template, "__SMS_STATUS_JSON__") != 1 {
        return fmt.Errorf("template requires exactly one status token")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10*time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
    }
    status, err := request(ctx, client, http.MethodGet, base+"/sms/status/"+url.PathEscape(id), key, "", nil)
    if err != nil { return err }
    if !json.Valid(status) { return fmt.Errorf("status response is not JSON") }
    body := []byte(strings.Replace(template, "__SMS_STATUS_JSON__", string(status), 1))
    if !json.Valid(body) { return fmt.Errorf("completed log request is not JSON") }
    result, err := request(ctx, client, http.MethodPost, base+"/logs/ingest", key, operation, body)
    if err != nil { return err }
    fmt.Println(string(result))
    return nil
}

func main() {
    if err := run(); err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
}
```

The program bounds rate-limit retries to 4 attempts, backs off exponentially, honors a longer Retry-After delay, and returns other unsuccessful responses with their bodies. Transport errors terminate the run because the caller must retain the evidence record and reconcile its outcome. Do not treat the presence of an idempotency header as proof that every capability honors it: inspect the capability declaration and scope replay behavior to its documented deduplication window. Persist the exact log payload before allowing replay across process restarts; otherwise a later status response could change the body beneath the same operation identity.

That last distinction is deliberate. This example shows a live status-to-log handoff; it is not a durable outbox. Production recovery needs a persisted pending record containing the resolved payload, its operation identity, and its delivery state, with access controls appropriate to the status data. Log ingestion itself is another write whose ambiguous outcome needs reconciliation.

## Rejected option: repeat the toggle until the error disappears

Repeated toggles are rejected for incident rollback because each successful attempt can reverse the previous result. A toggle remains a valid interactive operation when a human deliberately requests inversion from a known state and the interaction reconciles an uncertain result before allowing another inversion. That use case does not justify automatic replay during an incident.

A rollout endpoint with an explicit desired value can be appropriate for staged exposure, provided the controller preserves the operation identity and prevents older decisions from overtaking newer ones. A kill switch has a stricter acceptance criterion: once disablement is accepted, retries cannot become reenabling decisions.

Finally, a reconciled operational record is not automatically a compliance audit trail. Require independently retained evidence of who authorized the change, what changed, and how long the record must remain accessible; verify erasure, export, and retention obligations before putting player identifiers or message contents into logs. Prefer a pseudonymous correlation identity. Client polling, dependency management, deletion recovery, and evaluation accounting also require explicit acceptance tests rather than assumptions about a flag API. The architectural decision is complete only when the application can explain both the final state and the side effects that crossed its rollback boundary.

## References

- LaunchDarkly documentation: https://launchdarkly.com/docs/
- Unleash documentation: https://docs.getunleash.io/
- Twilio messaging documentation: https://www.twilio.com/docs/messaging
- Datadog documentation: https://docs.datadoghq.com/
- Sentry documentation: https://docs.sentry.io/
- Grafana documentation: https://grafana.com/docs/
- Healthchecks documentation: https://healthchecks.io/docs/
- ClickHouse documentation: https://clickhouse.com/docs
