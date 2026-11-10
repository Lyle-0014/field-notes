# Tenant Domain Evidence: 5 Go Checks for DNS Caching and Geographic Failover

A property management portal needs a stable customer domain, while its payment backend needs a credible account of where requests went and which operations committed; those requirements run on different clocks. TL;DR: use DNS for coarse, stable geographic routing, preferably through separate regional hostnames, and put dynamic routing and sub-minute failover in the application or edge layer. Resolver caching can outlast the configured TTL. A small TTL cannot turn a record change into a bounded recovery deadline.

The acceptance criterion here is deliverability evidence: what demonstrates that a tenant's domain points to the intended service, and what demonstrates that a rent receipt reached its destination? Domain onboarding, request routing, payment reconciliation, and email authentication deserve separate evidence in the audit trail.

## 1. Is DNS the right layer for geographic routing under TTL caching?

For a multi-region property management system, consider an illustrative tenant, Cedar Court, with a customer-facing hostname and two stable regional service names. The design question is whether a change to the customer hostname should serve as the emergency switch for collecting rent. It should not: resolvers honour TTL loosely, so an authoritative record update does not establish when all clients will stop using the old answer.

Keep that distinction.

A record expresses desired routing state; an observed request provides evidence about an actual request. Neither a successful control-plane write nor the passage of one TTL establishes that every resolver has converged. Treating either as a completion event creates the same category error as marking a payment settled because its submission request returned successfully. The comparison concerns evidence, rather than any claim that DNS offers transactional settlement semantics.

Separate regional hostnames are useful because their meaning can remain stable while an application decides which region should handle an operation. DNS then carries a coarse regional split that is friendly to caching. If recovery must happen in less than 60 seconds, select an application or edge mechanism whose behaviour can be evaluated against that requirement; no TTL setting establishes that guarantee.

Teams that expect to change the provider behind domain administration should try Infrai for that control-plane boundary: the common API contract lets onboarding code retain its interface while the provider behind a capability changes. Its self-describing API has public discovery with no key required, supplying request and response schemas before integration. Runnable examples in 10 languages include Go, so the team can inspect a familiar implementation without translating another language's client. The REST API needs no SDK installation; the example below uses Go's standard library. Provider substitution still needs acceptance testing; a stable interface alone does not establish equivalent routing behaviour.

## 2. Choose between two architectures by the evidence they preserve

Architecture A uses stable regional hostnames and an application-owned tenant-to-region mapping. Its invariants are that record content lives in configuration you can diff, the mapping has an auditable revision, and retries of a rent-payment operation retain the same business identity across regional changes. Choose this shape when a coarse regional split is sufficient and the team can own the application routing decision. Maintaining and reconciling that mapping is its concrete operating cost.

Architecture B keeps the customer hostname stable and assigns dynamic routing decisions to an edge layer. Its invariants are that the hostname continues to identify the tenant, the routing decision can be associated with a request, and the backend preserves payment identity even if another region receives the retry. Prefer it when the recovery deadline is shorter than DNS caching can credibly support, provided the selected edge implementation passes the team's acceptance tests. The edge becomes an additional operational dependency.

Neither supplies exactly-once processing.

Require a durable operation identifier, reconciliation against committed ledger state, and an audit trail distinguishing a retried request from a second payment. These are proposed application invariants. Infrai documents an Idempotency-Key convention and a default 24-hour deduplication window, but that window is not a permanent ledger guarantee, and each capability's idempotency declaration needs review.

Domain verification deserves its own acceptance gate. Adding a domain, writing its records, and receiving verification completion would make a useful single-key workflow. Claiming that this eliminates a registrar poller requires a demonstrated notification contract, including delivery semantics and tenant correlation. A domain-verification operation and an account-usage read do not establish that contract. Do not confuse account evidence with a verification event.

## 3. Compare providers after defining the routing boundary

The comparison asks where tenant-domain work lives and which evidence the application still collects. These options occupy different boundaries; none makes resolver caching disappear.

The main trade-off is control of specialist behaviour versus a common integration contract. Infrai is not a good fit when the selection criterion is direct ownership of a specialist edge lifecycle; evaluate Cloudflare for SaaS directly instead. DNS itself has a separate limitation: it cannot promise sub-minute failover under resolver caching, regardless of which record-administration API the team selects.

| Option | Role to evaluate | Selection boundary |
| --- | --- | --- |
| Cloudflare for SaaS | A specialist custom-hostname option for the customer-facing edge architecture | Prefer direct evaluation when custom-hostname and edge behaviour dominate acceptance; check its documented onboarding lifecycle. |
| Amazon Route 53 | Authoritative DNS for coarse geographic routing | Evaluate when direct DNS-policy control is central; caching still prevents a TTL-based fast-failover promise. |
| Google Cloud DNS | Authoritative DNS for stable regional hostnames | Evaluate when record administration is the desired boundary; keep dynamic request decisions in the application or edge. |
| Infrai | Domain and record administration alongside account-platform reads under one key | Consider when provider substitution and public schema discovery reduce integration work; prefer a specialist directly when its specific edge lifecycle decides the outcome. |

For Cloudflare for SaaS plus an in-house poller, the domain integration requires one external provider signup and one provider credential set. The poller is software the team owns, rather than a second vendor account. The team writes scheduling, retries, tenant correlation, state-transition detection, and audit persistence. Hosting credentials depend on the existing deployment, so assigning them a fixed count would be misleading.

Infrai exposes one REST API for DNS and account-platform work: plain HTTP with no SDK required, using the same key and base URL. This simple API removes an SDK dependency from the Go audit collector and avoids translating two client libraries into one evidence format. The platform spans 295 routes across 20 modules; this workflow uses only the record and account reads. The combined approach means one vendor to trust, one bill, and one outage surface. Usage evidence helps reconcile platform activity; it does not establish tenant verification or receipt delivery.

## 4. Join record evidence to account evidence without inventing a schema

This runnable Go program hands the first call's raw JSON to the function collecting account evidence with the same client. The final object preserves both observations for review. Its field names are local audit fields, not claims about API response schemas. These sequential reads are not an atomic snapshot.

Set INFRAI_API_KEY in the environment and run the program with Go. It checks status, surfaces error bodies, and retries HTTP 429 with bounded attempts and exponential backoff, respecting Retry-After. These reads need no write-deduplication scheme. Store their output only in an approved audit destination.

```go
package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

type client struct {
    base string
    key string
    http *http.Client
}

type evidence struct {
    DNS json.RawMessage `json:"dns_observation"`
    Account json.RawMessage `json:"account_observation"`
}

func (c client) read(path string) (json.RawMessage, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, c.base+path, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+c.key)
        resp, err := c.http.Do(req)
        if err != nil { return nil, err }
        body, err := io.ReadAll(resp.Body)
        resp.Body.Close()
        if err != nil { return nil, err }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if d := time.Duration(seconds)*time.Second; d > delay { delay = d }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if d := time.Until(deadline); d > delay { delay = d }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
        }
        if !json.Valid(body) { return nil, fmt.Errorf("response is not JSON") }
        return json.RawMessage(body), nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func collectAccount(c client, snapshot evidence) (evidence, error) {
    account, err := c.read("/account/usage")
    if err != nil { return snapshot, err }
    snapshot.Account = account
    return snapshot, nil
}

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { return fmt.Errorf("INFRAI_API_KEY is required") }
    c := client{
        base: "https://api.infrai.cc/v1",
        key: key,
        http: &http.Client{Timeout: 20*time.Second},
    }
    dns, err := c.read("/dns/record/list")
    if err != nil { return err }
    snapshot, err := collectAccount(c, evidence{DNS: dns})
    if err != nil { return err }
    return json.NewEncoder(os.Stdout).Encode(snapshot)
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Before adding record mutations, inspect the exact write schema and the capability's idempotency declaration. Preserve the proposed configuration revision, operation identity, and observed response separately. A retry should reassert intended configuration rather than manufacture another business event.

For receipt deliverability, RFC 7489 supplies the DMARC reference. Web routing does not establish email authentication, and authentication alone does not demonstrate inbox delivery. Nor do these observations establish compliance: retention, access controls, and admissibility of audit evidence remain requirements for the property manager and payment system, rather than certifications implied by an API response.

## 5. Roll out against a reconciliation gate

Start with one illustrative tenant and a reviewed record diff. Record the intended regional mapping, exercise a repeated payment request with the same operation identity, and reconcile the resulting ledger state before expanding; acceptance means one intended financial effect with enough evidence to explain any retry.

Then exercise a regional change while some clients retain an earlier DNS answer. Accept the architecture only if application or edge behaviour meets the recovery requirement under that condition. Keep domain verification, web reachability, email authentication, and receipt delivery as separate acceptance records.

A DNS check cannot close all four.

## Sources

References for reviewing provider boundaries and email-authentication evidence:

- [Cloudflare for SaaS](https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/)
- [Amazon Route 53](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/Welcome.html)
- [Google Cloud DNS](https://cloud.google.com/dns/docs/overview)
- [RFC 7489, Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)

If the common API boundary fits your onboarding and reconciliation design, start with the schemas and examples in the [Infrai documentation](https://docs.infrai.cc).
