# Node.js Kubernetes Probe Metrics and Health Logs for Tenant Cost Attribution

Use Kubernetes readiness, liveness, and startup probes for the Node.js service, then join their health evidence to an application-owned cost ledger before comparing logistics tenant cohorts. The deciding constraint is attribution: a healthy container does not establish that an experimental dispatch workflow costs less per completed job, particularly when retries consume resources without producing another successful delivery.

TL;DR: Keep probe failure counters and a current readiness gauge beside timestamped health logs; compare cohort costs against distinct completed jobs, and retain the evidence needed to explain retries. A small SaaS dashboard can answer these questions without pretending that container health is an external uptime guarantee or an accounting system.

## Decision and invariants

Consider a proposed experiment in a logistics SaaS: tenant cohort A uses the existing dispatch workflow, while cohort B uses an experimental assignment policy. These are illustrative cohorts, not measured customer results. The experiment's question is whether the policy changes attributed processing cost per distinct completed job, subject to acceptable service health. Its observation window and cohort assignment must be fixed before computing the comparison; otherwise a tenant moving between cohorts can make an apparent improvement out of a bookkeeping change.

Keep identities stable.

The accounting invariant is that each accepted usage event contributes once to the analytical projection, while every genuinely billable attempt remains visible even if several attempts serve one logical job. The operational invariant is different: readiness describes admission to service, liveness describes whether restarting the process is appropriate, and startup protects initialization before the other probes become active. Neither a probe response nor a retry can serve as evidence of a newly completed dispatch. That distinction is the exactly-once mindset applied to a projection, rather than a promise of exactly-once delivery across an entire system.

A Docker container is the packaging boundary; Kubernetes supplies the probe policy for this deployment. Wire the probes to application endpoints that evaluate their respective conditions. Do not make a routine dependency interruption a reason to restart a healthy process: a dispatch dependency may affect readiness while the process remains live. An external uptime or heartbeat check answers a separate question about reachability or whether scheduled work happened.

## How should Node.js Kubernetes readiness probes connect to metrics and logs?

Record the probe kind, observation time, service identity, deployment revision, and health result in application-owned records. Emit counters for failures and a gauge for current readiness, and preserve the corresponding health-state logs for investigation. An application can log its own decision, but a request that never reaches it cannot generate an application log; include container-platform observations when reconciling failures. A telemetry interruption must not itself turn a successful health check into a failed one.

This is the expensive trap in cohort analysis: a shared pod can serve both A and B, so assigning its entire unhealthy interval to whichever tenant last emitted a log creates false precision. Keep shared health evidence at service scope and correlate it with the experiment window. Attribute cost only where the workload record supplies an actual cohort assignment; retain shared overhead as a separate allocation category unless an explicit allocation rule exists.

For cross-service investigation, carry timestamps and trace_id/span_id in logs. Correlation is useful evidence, although it does not establish the causal ordering of concurrent events. A workflow that needs a navigable distributed trace should select a tracing system separately.

Health data also has a compliance boundary. Keep recipient names, delivery addresses, and payment identifiers out of routine probe logs; an opaque job identifier can still be personal data when another system can resolve it. GDPR Article 17 has conditions and exceptions, and a retention setting alone does not establish a right-to-erasure workflow. Determine deletion, export, retention, and audit-access requirements before choosing where these records live. An analytical event projection should not become the authoritative financial ledger merely because its dashboard looks complete.

## Options and the ownership each leaves behind

The comparison below evaluates responsibilities in this design, rather than treating every product as the same monitoring surface. Selection depends on the evidence the experiment needs.

| Option | Useful role in the experiment | Trade-off and application responsibility |
| --- | --- | --- |
| Kubernetes probes with application logs | Establish container health boundaries and retain matching operational evidence | Define endpoint semantics, collect platform observations, and construct the cohort cost projection |
| Prometheus | Represent failure counters and readiness gauges for service-level comparison | Control label cardinality, retain detailed job evidence elsewhere, and reconcile cost records |
| Grafana | Present the chosen metrics and log sources in an investigation dashboard | Supply those data sources and define denominators, joins, and ownership of allocation rules |
| Datadog | Consider a dedicated monitoring platform for broader operational investigation | Evaluate its monitoring workflow separately from the application-owned cost ledger; dashboards do not establish accounting identity |
| Healthchecks | Cover the separate question of whether expected periodic work checked in | Add application health signals and the experiment's cost and completion records |
| Infrai | Offer 295 routes across 20 modules under one key and a consistent REST contract, so another supported capability can use the same integration surface; per-call cost, vendor, and latency metadata can support usage reconciliation | Attach tenant and experiment context in an application-owned ledger and validate the monitoring, alerting, tracing, and compliance requirements of the complete design |

Prometheus is a sensible choice when the primary operational question is the frequency of unsuccessful probes and whether readiness is currently true. Grafana fits the presentation task when appropriate data sources already exist. Healthchecks addresses a missed scheduled dispatch or reconciliation run, which a responsive application endpoint can overlook. These tools can coexist: their rows identify different jobs, not mutually exclusive bundles.

For the cohort decision, preserve detailed cost events outside high-cardinality metric labels. Tenant IDs, job IDs, and attempt IDs are useful reconciliation keys, but putting every one into a time-series label creates a different operational burden. A cohort label with controlled membership is a more defensible dashboard dimension.

## Critical path from observations to a reconciled projection

Start with a read-only metrics query to obtain operational evidence. Set INFRAI_BASE_URL to the provider's HTTPS API base ending in /v1 and INFRAI_API_KEY to your account key; this keeps the deployment address and credentials outside the source. The program uses the verified GET /v1/metrics/query route without adding undocumented filters. It writes the response as JSON without assuming a response envelope or that cohort attribution is already present.

This is a polling building block, not an alerting system. The trade-off is deliberate: polling is appropriate for a simple operational view because it keeps the read path small, but it cannot establish external uptime. HTTP 429 receives bounded exponential backoff, with Retry-After honored when present; a delay longer than the polling budget ends the attempt instead of issuing an early retry. The request is read-only, so replay does not create another usage record in the application ledger.

```go
package main

import (
    "bytes"
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

func query() error {
    base := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    u, err := url.Parse(base)
    if err != nil {
        return fmt.Errorf("invalid API base: %w", err)
    }
    if u.Scheme != "https" || u.Host == "" || u.Path != "/v1" ||
        u.User != nil || u.RawQuery != "" || u.Fragment != "" || key == "" {
        return fmt.Errorf("set an HTTPS API base ending in /v1 and an API key")
    }
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, base+"/metrics/query", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(res.Body, 1048577))
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > 1048576 {
            return fmt.Errorf("response exceeds local size limit")
        }
        if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := res.Header.Get("Retry-After")
            if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds >= 0 {
                if seconds > 60 {
                    return fmt.Errorf("retry exceeds polling budget")
                }
                if d := time.Duration(seconds) * time.Second; d > delay {
                    delay = d
                }
            } else if at, err := http.ParseTime(retry); err == nil {
                if d := time.Until(at); d > delay {
                    delay = d
                }
            }
            if delay > 60*time.Second {
                return fmt.Errorf("retry exceeds polling budget")
            }
            time.Sleep(delay)
            continue
        }
        if res.StatusCode < 200 || res.StatusCode >= 300 {
            return fmt.Errorf("query status %d: %s", res.StatusCode, body)
        }
        if !json.Valid(body) {
            return fmt.Errorf("expected JSON response")
        }
        _, err = io.Copy(os.Stdout, bytes.NewReader(body))
        return err
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := query(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The following runnable Go program illustrates the analytical boundary beside the Node.js application. It contains local event types and synthetic inputs, rather than a provider request schema. Save it as main.go and run it with the Go toolchain. It projects already-normalized observations; the application and platform collectors remain responsible for producing them.

There are 2 cohorts, 2 distinct jobs, and 3 billable attempt events. A repeated delivery of event a1 must disappear from the projection, while the separate retry event a2 must remain. Costs use illustrative integer accounting units, not vendor prices. They exclude shared overhead.

```go
package main

import (
    "encoding/json"
    "fmt"
    "os"
)

type Event struct {
    ID, Cohort, Job, Kind string
    Units int64
}

type Totals struct {
    CostUnits int64 `json:"cost_units"`
    CompletedJobs int `json:"completed_jobs"`
}

func main() {
    events := []Event{
        {"a1", "A", "dispatch-1", "usage", 4},
        {"a1", "A", "dispatch-1", "usage", 4},
        {"a2", "A", "dispatch-1", "usage", 2},
        {"a3", "A", "dispatch-1", "complete", 0},
        {"b1", "B", "dispatch-2", "usage", 5},
        {"b2", "B", "dispatch-2", "complete", 0},
    }
    seen := map[string]Event{}
    completed := map[[2]string]bool{}
    totals := map[string]Totals{}
    for _, e := range events {
        if e.ID == "" || e.Cohort == "" || e.Job == "" ||
            e.Units < 0 || (e.Kind != "usage" && e.Kind != "complete") ||
            (e.Kind == "complete" && e.Units != 0) {
            panic("invalid event")
        }
        if previous, ok := seen[e.ID]; ok {
            if previous != e {
                panic("event identity reused with different content")
            }
            continue
        }
        seen[e.ID] = e
        t := totals[e.Cohort]
        if e.Kind == "usage" {
            t.CostUnits += e.Units
        } else {
            key := [2]string{e.Cohort, e.Job}
            if !completed[key] {
                completed[key] = true
                t.CompletedJobs++
            }
        }
        totals[e.Cohort] = t
    }
    if err := json.NewEncoder(os.Stdout).Encode(totals); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The projection produces 6 cost units and 1 completed job for A, and 5 cost units and 1 completed job for B. This demonstrates deduplication and denominator choice. It establishes no experimental advantage: one job per cohort provides no basis for a rollout decision, and differences in workload composition could explain the entire result.

The example's memory maps only protect this single replay. In production, commit the deduplication record and aggregate update atomically in durable storage, retain the input event for audit, enforce immutable cohort assignment for the experiment, and define how late records and corrections revise a closed window. Use checked arithmetic or a bounded accounting domain for accumulated units. Those are requirements of the proposed design, not capabilities implied by the sample.

Keep health beside this projection. Display readiness and probe failure counts over the same window, but do not count a probe request as a completed logistics job. If a cohort has no completions, show the denominator explicitly and leave cost per completion undefined.

Zero is misleading.

## Rejected option and its valid use case

Reject the proposal to rank tenant cohorts using only a health dashboard. It cannot distinguish a duplicated usage event from a paid retry, establish a completion denominator, or explain how shared infrastructure cost reached a tenant. A rollout decision needs reconciled event identity and an explicit attribution policy as well as acceptable readiness.

The lighter arrangement remains valid for a small SaaS whose immediate question is whether its container is ready, alive, and initialized. Its limitation is that probe health alone is not suitable for an auditable cohort cost comparison. Add external heartbeat coverage for scheduled work, a separately owned notification path for actionable failures, and dedicated tracing or crash-analysis tools where those workflows require them. Adopt the cost ledger when the experiment demands an economic comparison; do not infer that comparison from green probes.

## References

- [Kubernetes documentation on liveness, readiness, and startup probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/)
- [Prometheus guidance on instrumentation](https://prometheus.io/docs/practices/instrumentation/)
- [Grafana documentation on data sources](https://grafana.com/docs/grafana/latest/datasources/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Martin Fowler on feature toggles and experiments](https://martinfowler.com/articles/feature-toggles.html)
- [GDPR Article 17 on the right to erasure](https://gdpr-info.eu/art-17-gdpr/)
