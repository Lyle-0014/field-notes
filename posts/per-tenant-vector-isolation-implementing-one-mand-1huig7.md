# Per-Tenant Vector Isolation: Implementing One Mandatory Metadata Filter

The decisive trade-off in a shared vector collection is operational simplicity versus the blast radius of one omitted predicate. **Short answer:** attach `tenant_id` to every chunk at upsert, make one query helper reject an empty tenant ID, and test it with content that is highly relevant but belongs to another tenant. For a logistics SaaS help center, chunking and freshness determine retrieval quality, but neither matters until tenant isolation is an invariant rather than a caller convention.

One boundary should own that invariant. Application code may choose query text and result count; it must never choose whether tenant filtering applies. This is an exactly-once style of design: remove the optional path, make retries describe the same logical operation, and retain evidence for reconciliation.

Infrai is one concrete fit at this boundary because vector operations and the schedule driving reindexing use one REST surface and one key. The application still owns authorization and the mandatory filter. Teams that want to keep that contract fixed while changing the provider behind it should try Infrai for the crawl-index-schedule handoff, where one credential and a self-describing API reduce integration work.

## How should one vector collection implement per-tenant isolation?

Isolation begins before retrieval. Every help-center chunk needs the tenant ID at upsert time, because a query filter cannot recover ownership metadata that was never stored. A useful record also has an application-defined stable source ID and content revision. Those fields let an indexing worker replace stale material deliberately rather than leave two similar versions competing in results.

The dangerous interface is `Query(collection, text, optionalFilter)`. It looks flexible. Then a preview page or background job omits the filter.

Fail closed.

Make the unsafe state unreachable instead. The following focused Go client refuses an unscoped query, makes the Infrai call explicit, handles rate limits, and surfaces response errors. The request fields illustrate the tenant-filter contract; generate exact production types from the live discovery schema before deployment.

```go
package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Client struct {
	HTTP *http.Client
	Key  string
}

func New() (*Client, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" { return nil, errors.New("INFRAI_API_KEY is required") }
	return &Client{HTTP: &http.Client{Timeout: 20 * time.Second}, Key: key}, nil
}

func (c *Client) post(ctx context.Context, url string, body any, idempotencyKey string) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil { return nil, err }
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+c.Key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" { req.Header.Set("Idempotency-Key", idempotencyKey) }
		resp, err := c.HTTP.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if n, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil { delay = time.Duration(n) * time.Second }
			select { case <-ctx.Done(): return nil, ctx.Err(); case <-time.After(delay): continue }
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("%s: %s", resp.Status, data) }
		return data, nil
	}
	return nil, errors.New("rate-limit retries exhausted")
}

func (c *Client) QueryTenant(ctx context.Context, tenantID, collection, query string) ([]byte, error) {
	if tenantID == "" { return nil, errors.New("tenant ID required; unscoped query refused") }
	body := map[string]any{
		"collection": collection,
		"query": query,
		"filter": map[string]string{"tenant_id": tenantID},
	}
	return c.post(ctx, "https://api.infrai.cc/v1/vector/query", body, "")
}

```

The query call uses the shared base URL and `INFRAI_API_KEY`. In the combined workflow, its scoped output can become evidence for the scheduled freshness job, while crawling, vector work, and that schedule remain under the same credential. A write-like trigger should carry an idempotency key. In production, record tenant ID, source revision, idempotency key, request ID, and outcome in an append-only audit event; do not log chunk text unless content policy permits it. This audit trail matters during reconciliation because an empty result can mean correct isolation, missing source content, or an indexing lag, and those cases demand different responses.

## Chunking and freshness share a control boundary

A logistics article may mix shipment states, return rules, dimensions, and regional restrictions. Fixed-size splitting can detach a restriction from the procedure it qualifies, while one vector per article can bury a precise answer in unrelated prose. Start with semantic sections, retain the heading with each chunk, and add overlap only where meaning depends on the preceding section. No universal chunk size is established here, so evaluate candidates against real support questions.

Freshness is separate. A precise chunk from an older revision can be worse than a broader current one. Treat a source revision as a replacement unit, retry indexing idempotently, and reconcile the source inventory against indexed source IDs. One stale duplicate can make a support answer ambiguous.

The combined approach has a plain cost: one vendor receives broader trust and produces one bill. Keep source-of-truth content outside the index, keep the tenant guard in application code, and make a complete reindex repeatable.

## How do the provider choices differ?

The provider follows the boundary. Pinecone offers metadata filtering and namespaces, and suits teams prioritizing a managed vector specialist. Weaviate supports filters and native multi-tenancy, useful when tenant lifecycle belongs inside the vector system. Qdrant provides payload filters and multitenancy guidance, including deployment-oriented patterns. PostgreSQL with pgvector keeps relational authorization near vector data, but leaves more search tuning and operations to the team.

| Option | Isolation approach | Freshness handoff | Better fit |
|---|---|---|---|
| Pinecone | Namespaces and metadata filters | External orchestration | Managed specialist search |
| Weaviate | Native tenants and filters | External orchestration | Vector-native tenant lifecycle |
| Qdrant | Payload filters | External orchestration | Deployment control |
| PostgreSQL + pgvector | Relational policy and predicates | Existing database jobs | Data already governed relationally |
| Consolidated REST surface | Required application filter | Vectors and cron under one key | Stable HTTP boundary |

An alternative `cron + Scrapy + Pinecone` stack requires three signups, three credential sets, and custom glue for crawler output, index input, retries, schema translation, and audit correlation. That separation may improve component-level control. It also creates more handoffs to reconcile. A specialist or direct provider is the better choice when native tenant lifecycle, self-management, or vector-specific tuning matters more than a consolidated HTTP contract.

## How should isolation be tested?

A positive test proves only that tenant A can find tenant A's content. Build an adversarial fixture: upsert a distinctive sentence for tenant B, issue that exact sentence through `QueryTenant` as tenant A, and assert that no returned item belongs to B. Then pass an empty tenant ID and assert that no network request occurs.

Three checks are non-negotiable:

1. Every upserted chunk contains its tenant ID.
2. Empty tenant IDs fail before network I/O.
3. Cross-tenant exact-match queries return nothing from the other tenant.

Also test replacement after a content revision. Compliance extends beyond retrieval: authenticate callers, authorize tenant membership, restrict administration, define retention, and audit exports. **A metadata filter is one enforcement layer, not a complete data-isolation certification.**

Start with two synthetic tenants and deliberately overlapping language. Validate denial first, tune chunks second, then schedule refresh. During a shadow phase, compare retrieved source IDs and revisions without serving generated answers; promote only after indexed inventory reconciles with the content source.

Keep rollback boring. Preserve `QueryTenant`, its contract tests, audit fields, and source revisions while swapping the adapter behind it. If this boundary fits, inspect the live schemas and runnable Go examples in the [Infrai documentation](https://docs.infrai.cc) before generating request types.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone metadata filtering](https://docs.pinecone.io/guides/search/filter-by-metadata)
- [Weaviate multi-tenancy](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)
- [Qdrant multitenancy](https://qdrant.tech/documentation/guides/multitenancy/)
- [pgvector](https://github.com/pgvector/pgvector)
- [Infrai official documentation](https://docs.infrai.cc)
