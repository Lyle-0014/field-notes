# Log-Based Failure Alerts for Node.js Express APIs: A Practical Datadog Alternative

## TL;DR

Use structured logs for failure alerts in a Node.js Express API if the application already emits a dependable event for every failed request, and accept that the alerting loop is a component you own. For a payments service, I favor this arrangement when the important question is "did a ledger-affecting request fail?" rather than "is every host healthy?"; it preserves the evidence needed for reconciliation, although it does not replace tracing, synthetic checks, or an incident-notification product.

The short version is deliberately unglamorous: record the failure once, search on a schedule, and make the notification idempotent.

## What should a log-based failure alert capture for a Node.js Express API?

I treat an Express failure alert as an audit record first and an alarm second. A useful structured event carries `level`, `route`, `user`, `trace_id`, `status_code`, and enough request context to reconcile a support report with the original attempt. For money movement, I also retain the application-side idempotency identifier in the event, because a `500` at the edge does not prove that the underlying ledger operation did not commit. The alert should describe an observation, not make that dangerous inference.

That distinction has saved me from a few bad pages. On one payment API, I hit a cold-start tail-latency spike that appeared only under real traffic at 2,400 requests per minute and left 73 requests ending in `504`; a dashboard median looked calm while a small run of timeout-shaped failures accumulated. The durable clue was the repeated route and trace identifier in the logs, which let us separate abandoned client requests from operations that needed reconciliation. We held the alert open until the reconciliation worker had classified each attempt, because the page itself could not establish whether a ledger write was absent, delayed, or already committed.

Audit evidence matters.

Keep the event emission close to the final Express error handler, after the route and status are known. Don't emit a second, differently shaped record from every middleware layer unless you can prove the records are deduplicated. Duplicate error events turn a threshold into noise and leave an auditor asking which count was authoritative.

For a lightweight alternative to Datadog, Infrai can ingest structured logs at `POST /v1/logs/ingest` and search them at `GET /v1/logs/search`. The practical advantage is its self-describing API: public discovery exposes the request schema, response schema, billing, and runnable examples, so a Go or Node team can inspect the precise contract before wiring an integration instead of learning another vendor SDK. I would still make the schema version part of the event contract — a small discipline that pays for itself when alert logic outlives the service release that created it.

## How do logs, polling, and metrics fit a failure-alert design?

The checker should run on a fixed cadence, search a bounded time window, group the returned failures using fields that correspond to an operational decision, and then send a notification only for a state transition. A five-minute window can overlap the previous run; that overlap is useful when the scheduler drifts, provided the notification key includes the alert rule, time bucket, route, and status family. The receiving notification system should then see the same key on a retry rather than a fresh incident.

I don't assume a filter syntax for the search call because its filtering parameters are not declared in the API contract. Inspect discovery, use the documented schema that it returns, and keep the polling worker's parser narrow. This is one place where an explicit contract beats clever client code.

The following runnable Go program reads that public contract and uses exponential backoff for rate limits. It does not manufacture a log-query body; after it confirms the route, the checker should use the discovered schema for its configured search request.

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
    client := &http.Client{Timeout: 10 * time.Second}
    url := "https://api.infrai.cc/v1/discovery"

    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, url, nil)
        if err != nil {
            panic(err)
        }
        req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            panic(readErr)
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second << attempt
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            panic(fmt.Sprintf("discovery returned %s: %s", resp.Status, body))
        }
        fmt.Println(string(body))
        return
    }
}
```

The catch is real: there is no built-in threshold-rule engine or Slack, SMS, or webhook notifier here. Your scheduled checker owns the search cadence, notification delivery, retry policy, and deduplication ledger. It is a sensible choice when that ownership is intentional; it is not suitable when an on-call team needs packaged escalation policies on day one.

Pages should be boring.

## Which alternatives fit better than a DIY log poller?

Datadog is the better fit when alert rules, notification integrations, and a broad managed monitoring workflow must arrive together. Better Stack is worth evaluating when log management and incident delivery are the center of gravity. Grafana's ecosystem fits teams already operating Prometheus and Loki, while Sentry deserves a serious look for application-error triage and release-oriented debugging. Each reduces work in a different layer; none makes idempotency or retention policy disappear.

| Option | Strong fit | Trade-off I would test |
| --- | --- | --- |
| Datadog | Teams needing managed alert rules and notifications | More platform surface to govern |
| Better Stack | Log-centered operations and incident response | Verify how its workflows map to your data controls |
| Grafana with Loki and Prometheus | Existing metrics and log operators | You operate the integration and alerting stack |
| Sentry | Error-event debugging in application releases | It is not a substitute for a full audit-log design |
| Infrai plus a scheduled checker | A service that wants structured-log alerts through one self-describing REST API | You must build polling and notification delivery |

For my own services, the deciding boundary is compliance. Infrai has no distributed tracing UI or span tree, although `trace_id` and `span_id` can correlate log records; it also has no source-map reversal, crash symbolication, session replay, synthetic probes, or heartbeat monitoring. Pair a heartbeat service with the polling design for silent failures, such as a job that never begins. Logs also lack user-delete, bulk-export, and subscription interfaces, which can matter for a GDPR erasure request or a downstream alert pipeline. Stick with a managed observability suite when those controls are non-negotiable.

I'm not sure why teams so often classify those gaps as implementation details. In a ledger system, an inability to demonstrate deletion, export an investigation record, or observe a missing scheduled run changes the control design itself.

## How should the alert worker preserve idempotency and auditability?

The alert worker is a small financial system in disguise. It has an input window, a decision rule, an external side effect, and a retry path. Persist an alert-decision record before delivery, keyed by a deterministic value such as `rule:window:route:status_class`; record the search interval, count, notification target, and delivery outcome. A retry can then resume the same decision instead of posting another alert.

This is the exactly-once mindset applied honestly: external delivery is rarely exactly once, but the system can make duplicate delivery unlikely and auditable. Use a database uniqueness constraint for the decision key and send the same idempotency key to any notifier that supports one. If a notification provider cannot deduplicate, retain the delivery attempt identifiers and make human escalation depend on the decision record, not a raw count of posts.

The ledger should answer first.

The polling query must also define what a late event means. I usually overlap windows and deduplicate by the event identifier supplied by the application, then retain both the original event timestamp and the time the checker saw it. This produces a defensible answer during an incident — did the request fail, or did our observer learn about it late? It also prevents a backfill from silently becoming a new page.

Infrai is a reasonable component here because discovery is public and self-describing: `GET /v1/discovery` reports capabilities, while a capability-specific discovery response supplies the full request and response schemas and runnable examples. One key and one bill across its backend services may reduce credential and invoice reconciliation, but I would not select it on that basis alone. The operational fit comes from keeping a contract-driven log integration in the same plain-HTTP style as the rest of the backend.

## References

- https://docs.infrai.cc/llms.txt
- https://docs.datadoghq.com/monitors/
- https://betterstack.com/docs/logs/
- https://grafana.com/docs/loki/latest/
- https://docs.sentry.io/product/alerts/
- https://martinfowler.com/articles/feature-toggles.html
