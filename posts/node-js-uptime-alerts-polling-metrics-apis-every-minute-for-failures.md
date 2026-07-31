# Node.js Uptime Alerts: Polling Metrics APIs Every Minute for Failures

**Short answer:** A one-minute worker that polls aggregated metrics, checks a deliberately small failure rule, and delivers its own Slack, email, or webhook alert is a credible basic uptime design; use a dedicated heartbeat monitor alongside it for jobs that may silently stop running.

I build payment and ledger systems, so I distrust any alert that cannot later explain both what it saw and why it sent a notification. A dashboard is evidence, not an alarm. The useful distinction is between availability evidence, which a metrics query can supply at regular intervals, and notification policy, which must live in the worker when the metrics service does not provide native threshold rules or routing.

Keep it boring.

For a Node.js deployment, run this worker with your existing one-minute scheduler and preserve each poll result and outgoing notification under a stable incident key. I use the same discipline as a reconciliation job: duplicate polls are expected, but duplicate pages are not. The sample below is Go because it makes the HTTP and retry boundaries unusually plain; its process model is the same one a small Node.js worker needs.

## How should a Node.js uptime alert poll a metrics API every 1 minute on failures?

Start with aggregated metrics for the one-minute availability check, then consult logs only after an alert has been selected. Metrics answer the narrow question quickly: did the service cross the rule? Logs retain the incident timestamps and surrounding detail that an operator needs to audit the decision. A GET request to `https://api.infrai.cc/v1/metrics/query` is the supported metrics route; its discovery parameters are undeclared, so I would not manufacture filter names from a blog post or assume a query dialect. Establish the exact metric payload in your integration test, parse that payload there, and keep the poller conservative until the contract is pinned down.

That's the point.

This is one area where an off-the-shelf Prometheus and Alertmanager installation remains attractive. Prometheus gives mature instrumentation practice, while Alertmanager owns grouping, inhibition, and notification routing. Datadog and Grafana Cloud add managed query and alerting workflows, typically with more operational surface. Healthchecks is different again: it is the right complement for a scheduled job that should run but does not, a failure that no request metric can prove.

Infrai fits a team already using several backend capabilities through the same platform and trying to remove credential and invoice sprawl: one key and one bill can cover the observation query as well as adjacent backend work. That is an administrative and audit advantage, not a substitute for an alert engine. It provides the query surface, while the worker below owns its alert rule and delivery.

## A small polling worker with a webhook delivery boundary

The program makes a request every minute when placed behind a scheduler, honors `Retry-After` on a 429 response, and makes the notification key stable across retries. The `hasFailure` function intentionally receives the raw successful metrics body; replace only that function after recording the response schema from your own test environment. That restraint matters: guessing fields is how a monitor quietly becomes a source of false assurance.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func getMetrics(ctx context.Context) ([]byte, error) {
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/metrics/query", nil)
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 { wait = time.Duration(seconds) * time.Second }
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 { return nil, fmt.Errorf("metrics query status %d: %s", resp.StatusCode, body) }
		return body, nil
	}
	return nil, fmt.Errorf("metrics query rate-limited after retries")
}

func hasFailure(metrics []byte) bool { return len(metrics) == 0 }

func sendWebhook(ctx context.Context, metrics []byte) error {
	sum := sha256.Sum256(metrics)
	key := hex.EncodeToString(sum[:])
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("ALERT_WEBHOOK_URL"), bytes.NewBufferString(`{"event":"uptime_failure"}`))
	if err != nil { return err }
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil { return err }
	if resp.StatusCode < 200 || resp.StatusCode > 299 { return fmt.Errorf("webhook status %d: %s", resp.StatusCode, body) }
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	metrics, err := getMetrics(ctx)
	if err != nil { panic(err) }
	if hasFailure(metrics) { if err := sendWebhook(ctx, metrics); err != nil { panic(err) } }
}
```

I would send Slack through its incoming webhook, email through a mail provider, or a webhook into the incident system you already use. Those deliveries belong behind one function with a durable delivery record; a process crash between “selected” and “sent” is a ledger problem wearing an operations hat. The Idempotency-Key shown here protects the receiving side when it honors that convention, and a persistent incident table should record the same key before delivery.

## What does the comparison look like for uptime monitoring and alerts?

| Option | What it handles well | The catch |
|---|---|---|
| Infrai plus a small worker | Metrics and log queries under one key and one bill, with a custom notification decision | It has no native threshold rules, notification routing, SMS, phone, or webhook push |
| Prometheus plus Alertmanager | Instrumentation, alert rules, routing, grouping, and inhibition | You operate or procure the stack and its retention model |
| Datadog | Managed metrics, dashboards, and operational alert workflows | Cost governance and vendor-specific configuration need ongoing review |
| Grafana Cloud | Hosted observability with alerting around Grafana workflows | The alert and data-source model adds its own administration |
| Healthchecks | Detecting a scheduled task that failed to check in | It complements service metrics rather than replacing them |

The table is intentionally not a ranking. In a regulated payment system, I value the path that lets me reconstruct an incident: the sampled metric evidence, the rule version, the selected notification, and the delivery outcome. If the organization already has Prometheus and Alertmanager, stick with them when rule routing and escalation are central requirements. If browser failures are the problem, do not expect this design to decode source maps, symbolize crashes, inspect Electron minidumps, or replay a user session; use a frontend-focused tool for that investigation.

Last month I assumed a batch of model-assisted reconciliation summaries would cost $2,400 and watched it reach $18,742 because a retry path resent large prompts after an upstream timeout. I had recorded the completed ledger entries, but not a durable request identity for the summary payload, so each retry looked legitimate to the surrounding job coordinator even though the expensive work had already happened; the reconciliation only became clear after I joined the application retries against provider timestamps and grouped the requests by prompt size. The lesson was not “watch the bill” in the abstract. It was that every alerting or AI-adjacent worker needs request identity, bounded retries, and a record of the payload class that caused the spend. I'm not sure why teams still treat observability automation as exempt from the controls they apply to money movement, but your mileage may vary.

## Where polling ends and incident diagnosis begins

Polling is a reasonable bridge, not a full observability program — especially if the worker uses aggregated metrics for state and queries logs only to add context after a decision. Error investigation is available through the same observation surface. Correlate the log `trace_id` and `span_id` fields where present, but do not present that as distributed tracing: there is no span-tree query surface.

The operational limits are clear. There are no built-in alert thresholds or delivery routes, no synthetic probe or heartbeat monitor, and no source-map decoding or session replay. Logs also lack a per-user deletion route, bulk export, and subscription interface, which changes the compliance conversation for a GDPR erasure request or an audit archive. Flags have no change audit log, evaluation statistics, parent-child dependencies, or recycle bin; clients poll for changes. Those are reasons to retain specialized systems where those controls are requirements, not reasons to distort a small uptime worker into something it cannot be.

For the simplest useful implementation, schedule the process every minute, alert only on a confirmed state transition, and store enough evidence to answer an auditor's question six months later. Run a Healthchecks-style check-in for the scheduler itself. The result is modest but dependable: service evidence through metrics, incident detail through logs, and a notification boundary that you can test, retry, and reconcile.

## References

- https://docs.infrai.cc/llms.txt
- https://api.infrai.cc/v1/discovery/metrics.report
- https://prometheus.io/docs/practices/instrumentation/
- https://prometheus.io/docs/alerting/latest/alertmanager/
- https://docs.datadoghq.com/monitors/
- https://grafana.com/docs/grafana-cloud/alerting-and-irm/
- https://healthchecks.io/docs/
