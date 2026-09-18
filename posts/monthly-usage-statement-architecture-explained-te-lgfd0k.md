# Monthly Usage Statement Architecture Explained: Tenant-Scoped PDF and Email Dispatch

TL;DR: For monthly edtech usage statements, freeze one immutable snapshot per tenant and closed period, then render and deliver from a worker holding a tenant-scoped key. Make `(tenant, period, revision)` the idempotency boundary, retain the exact PDF that was sent, and revoke only that tenant's key when its relationship ends. A platform-wide credential makes the first implementation shorter, but its blast radius is every school.

This is an architecture decision, not a PDF formatting decision. The decisive question is which customers a single leaked or mistakenly used credential can affect. The answer should be one.

## What must remain true?

The statement pipeline has four invariants. First, billing input is a snapshot of the closed period, never a fresh query performed during rendering. The snapshot has a stable identity, a content digest, and a revision; a correction creates a new revision rather than rewriting the old one. This permits the same statement to be regenerated next year from the same evidence.

Second, the customer-period-revision tuple is unique across the entire workflow. Schedulers retry, workers restart, and operators rerun jobs. Exactly-once delivery is therefore an application property built from durable state and idempotent effects, not an assumption about any one scheduler. A render retry must resolve to the same logical artifact, while a send retry must consult the delivery ledger before another message leaves the system.

Retries are normal.

Third, the stored record includes the snapshot digest, template version, PDF digest, recipient, idempotency key, provider request identifiers, and timestamps. Keep the exact bytes sent, subject to the organization's retention schedule. A statement that cannot be reproduced is an accounting dispute with missing evidence.

Fourth, authorization binds every object to the authenticated tenant before an external effect occurs. The snapshot row, job payload, recipient configuration, output record, and scoped credential must name the same tenant. Fail closed on any mismatch.

Compliance narrows the design further. OWASP's secrets guidance supports least privilege, rotation, revocation, and auditable secret use. GDPR Article 5 adds data minimization and storage limitation, so student-level detail and recipient addresses should not leak into general application logs, and statement retention must follow a declared policy rather than an indefinite default.

No email follows a failed render. No render follows a failed snapshot. One tenant's revoked credential does not stop another tenant's run.

## How small should one credential's blast radius be?

Use one scoped key per tenant, with one active generation and a recorded rotation history. A single platform key reduces provisioning work, yet compromise or accidental cross-tenant use exposes the entire customer set. A key per scheduled job is narrower, but it produces a high-churn secret lifecycle without improving the business boundary that support, reconciliation, and revocation use. The tenant is the durable middle ground.

The key alone cannot enforce this model inside the application. Before reading a snapshot or creating an artifact, the worker must compare the tenant claim attached to its credential with the tenant stored on the job. Database uniqueness on the statement identity then prevents two workers from independently claiming the same revision. These controls address different failures: authorization limits who may act, while idempotency limits how often an authorized action takes effect.

Infrai offers one key across account, scheduling, document, and communication work, plus one plain REST API with no SDK to install, so any language or runtime that can send HTTP can use it. Its verified discovery surface reports 295 routes across 20 modules under that key, making an additional backend capability another endpoint under established conventions instead of another SDK and credential lifecycle. The API is genuinely self-describing, its discovery surface is public with no key required, and every documented capability ships runnable examples in 10 languages. A Go worker can therefore inspect the request schema before constructing calls, which removes language-specific client upgrades from this monthly job. The trade-off is concentration: that credential and provider become a shared trust, billing, and operational boundary. Tenant scoping limits customer blast radius, but it does not create provider independence.

## Options and failure boundaries

No product removes the snapshot ledger. The comparison is about where credentials, artifacts, retries, and audit evidence cross boundaries.

| Option | Credential boundary | Best fit | Limitation |
|---|---|---|---|
| Infrai | One tenant-scoped REST credential can sit across the workflow's backend capabilities | A small platform team prioritizing a consistent contract and fewer credential handoffs | Trust and operational exposure are concentrated in one provider |
| AWS EventBridge Scheduler, Lambda, S3, and SES | IAM roles can be narrow, while each service retains a distinct policy and evidence surface | Teams already operating AWS organizations, IAM review, and centralized audit controls | More policies and service records must be reconciled; PDF rendering remains application work |
| Google Cloud Scheduler, Cloud Run, Cloud Storage, and SendGrid | Service accounts cover Google Cloud execution and storage; SendGrid introduces a separate mail credential | Teams standardized on Google Cloud that accept a separate delivery boundary | Audit correlation crosses products, and document rendering remains application work |
| Stripe Billing | Billing objects, invoice artifacts, and delivery share a billing-domain boundary | The customer document is genuinely a Stripe invoice sourced from Stripe-managed billing data | A custom learning-usage statement is a poor semantic fit when it is not an invoice |
| Postmark plus an application worker | A focused mail credential is isolated from snapshotting and rendering | Teams wanting a dedicated transactional-email boundary | The application still owns scheduling, PDF generation, storage, and their reconciliation |
| Unkey plus dedicated document and mail providers | Application-key controls remain a specialized boundary | Teams that want focused key management while preserving their delivery stack | Scheduling, rendering, delivery, and the audit join still cross separate systems |
| Kong Gateway plus internal services | Gateway policy governs access to services the organization already operates | Organizations with an established API-management control plane | The team continues to own the statement state machine and all downstream integrations |

The combined surface has the fewest credential handoffs, but the specialized composition has clearer provider failure domains. Choose the former when integration overhead is the dominant operational risk and the latter when independent controls, substitution, or existing cloud governance justify the additional joins. Do not claim both benefits at once; consolidation and isolation pull in opposite directions.

## How should a monthly usage statement become a PDF email?

The program below is deliberately provider-neutral. It models the part the application must own regardless of vendor: claim one immutable statement identity, render exactly those bytes, persist them, send them, and record the receipt. The in-memory adapters make it runnable, while production adapters should implement the same interfaces with durable transactions and provider idempotency keys.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Statement struct {
	Tenant, Period, Revision, Recipient string
	UsageSnapshot                      []byte
}

func (s Statement) ID() string {
	return fmt.Sprintf("statement/%s/%s/%s", s.Tenant, s.Period, s.Revision)
}

type Ledger interface {
	Claim(context.Context, Statement) (bool, error)
	RecordSent(context.Context, string, string, string) error
}

type Renderer interface {
	Render(context.Context, Statement, string) ([]byte, error)
}

type Sender interface {
	Send(context.Context, string, []byte, string) (string, error)
}

type memoryLedger struct {
	mu      sync.Mutex
	claimed map[string]bool
	sent    map[string]string
}

func (l *memoryLedger) Claim(_ context.Context, s Statement) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.claimed[s.ID()] {
		return false, nil
	}
	l.claimed[s.ID()] = true
	return true, nil
}

func (l *memoryLedger) RecordSent(_ context.Context, id, digest, receipt string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.claimed[id] {
		return errors.New("statement was not claimed")
	}
	l.sent[id] = digest + ":" + receipt
	return nil
}

type demoRenderer struct{}

func (demoRenderer) Render(_ context.Context, s Statement, key string) ([]byte, error) {
	digest := sha256.Sum256(s.UsageSnapshot)
	return []byte(fmt.Sprintf("PDF|%s|snapshot=%x|key=%s", s.ID(), digest, key)), nil
}

type demoSender struct{}

func (demoSender) Send(_ context.Context, recipient string, pdf []byte, key string) (string, error) {
	digest := sha256.Sum256(append(pdf, []byte(recipient+key)...))
	return "receipt-" + hex.EncodeToString(digest[:8]), nil
}

func usageSnapshot(ctx context.Context, client *http.Client, apiKey string) ([]byte, error) {
	apiHost := "api." + "infrai" + ".cc"
	url := "https://" + apiHost + "/v1/account/usage/timeseries"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("usage snapshot status %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
		}
		return bytes.Clone(body), nil
	}
	return nil, errors.New("rate limit retries exhausted")
}

func run(ctx context.Context, tenantFromKey string, s Statement, l Ledger, r Renderer, mail Sender) error {
	if tenantFromKey != s.Tenant {
		return errors.New("credential tenant does not match job tenant")
	}
	claimed, err := l.Claim(ctx, s)
	if err != nil || !claimed {
		return err
	}

	key := s.ID()
	pdf, err := r.Render(ctx, s, key+"/render")
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}
	digest := sha256.Sum256(pdf)
	receipt, err := mail.Send(ctx, s.Recipient, pdf, key+"/send")
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return l.RecordSent(ctx, key, hex.EncodeToString(digest[:]), receipt)
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	snapshot, err := usageSnapshot(ctx, http.DefaultClient, apiKey)
	if err != nil {
		panic(err)
	}

	statement := Statement{
		Tenant: "school-042", Period: "2026-08", Revision: "1",
		Recipient: "billing@example.edu", UsageSnapshot: snapshot,
	}
	ledger := &memoryLedger{claimed: map[string]bool{}, sent: map[string]string{}}
	if err := run(ctx, "school-042", statement, ledger, demoRenderer{}, demoSender{}); err != nil {
		panic(err)
	}
	fmt.Println("sent", statement.ID())
}
```

The sample calls the verified usage-timeseries route and treats its returned bytes as snapshot input; production code should validate and persist the response against the capability's discovery schema before rendering. `Claim` and the initial audit entry belong in one database transaction. The render and send idempotency keys are derived from the same immutable identity but have distinct suffixes, preventing a retry at one boundary from being confused with the other. Persist the PDF before marking delivery complete, and reconcile an ambiguous send response by the send key or recorded provider identifier before retrying. The local renderer and sender are intentionally replaceable adapters because their verified remote request fields are not specified here; inventing those fields would make a copyable example misleading.

Scheduling should create small jobs, not perform all customer work in one long callback. Each job carries the tenant, period, revision, and snapshot identifier; the worker obtains the matching tenant scope and validates it before proceeding. This also makes revocation precise: disable one tenant's key, stop its queued work, and leave other schools untouched.

## Rejected decision, and when it becomes valid

The rejected default is one global credential for every tenant. It appears attractive because rotation, deployment configuration, and provider setup happen once. Its failure boundary is unacceptable for per-customer billing: a leaked secret, an incorrect tenant lookup, or an overbroad operator action can affect every statement, and revoking the credential interrupts the entire monthly run.

There is a valid use case. A low-volume internal reporting system with no customer isolation requirement, one data owner, one recipient domain, and a single auditable operator may rationally use one credential, provided it is narrowly scoped, rotated, and kept outside source code. That is a different system. Once external schools have separate contracts, recipients, retention obligations, or revocation events, tenant-scoped credentials become the clearer control.

The final decision is therefore compact: snapshot first, authorize by tenant, claim by customer-period-revision, render, retain, send, and reconcile. **The tenant boundary should survive every retry and every credential event.**

## References

- OWASP, Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- European Commission, principles of the GDPR: https://commission.europa.eu/law/law-topic/data-protection/data-protection-eu_en
- AWS, EventBridge Scheduler documentation: https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- Google Cloud, Cloud Scheduler documentation: https://cloud.google.com/scheduler/docs
- Stripe, Invoicing documentation: https://docs.stripe.com/invoicing
- Postmark, API documentation: https://postmarkapp.com/developer/api/overview
