# Fixed Dimension Vector Collections Beat Mixed Models for Fast Listing Aggregation

A media listing aggregator has an awkward constraint: new listings must become searchable quickly, but results still need to match a reader's intent rather than merely share a few words. **Short answer:** use one vector collection per embedding model and dimension, then reindex into a new collection when the model changes. A collection is a set of same-shaped vectors plus metadata; its dimension is fixed because similarity is meaningful only among vectors produced by the same model. For a latency-sensitive ingest path, a managed REST workflow is the better default. A split crawler, scheduler, and vector service remains the better choice when independent vendor control matters more than integration latency.

Dimension is not a capacity knob. It belongs to the embedding model. A 1,536-value vector does not make a collection larger in the operational sense that a disk allocation does, and trimming or padding an incompatible vector does not make it comparable. Even two embedding models that emit the same number of values define different spaces, so equal length alone proves nothing.

That distinction governs the entire design.

## Why Must Every Vector Have the Same Dimension?

Similarity functions compare coordinates position by position. If one listing vector has one shape and another has a different shape, there is no coherent coordinate-wise comparison to perform. The fixed dimension protects the mathematical contract of the collection, while metadata carries facts that do not belong in the vector itself: source, listing identifier, publication time, region, and the original URL.

The stronger rule is easy to miss: **same dimension does not imply the same embedding space**. Suppose model A and model B both return vectors of length 768. Coordinate 214 in one model has no guaranteed relationship to coordinate 214 in the other. Mixing them can produce numeric scores, but those scores do not have a defensible semantic interpretation. This resembles balancing a ledger in two currencies while discarding the currency code. The arithmetic finishes; the reconciliation is false.

Treat the collection definition as an auditable schema: `embedding_model`, `dimension`, and a generation identifier should travel together in deployment configuration and the reindex record. A write belongs to exactly one generation. A query must use that generation's model. When the model changes, create a new collection, repopulate it, verify it, and switch reads. Do not mutate the meaning of an existing collection in place.

## Deriving the Nightly Listing Flow

A multi-source media catalog usually receives duplicate URLs, revised descriptions, and late corrections. The ingestion identity should therefore be deterministic, such as a source namespace plus the source listing ID and revision. Crawling can repeat. Upserting can repeat. The externally visible state should converge on one record, and the audit trail should retain which source revision produced which embedding generation.

The sequence is short: scrape the source, normalize the listing, embed the searchable text with the collection's declared model, and upsert the resulting vector with metadata. A nightly schedule initiates the same idempotent path for sources that do not publish change events. Queries embed the user's request with that exact model and search only the matching collection. This preserves an exactly-once outcome over operations that may execute more than once.

Model migration is a separate transaction boundary. Build generation B while generation A continues serving reads. Reindex every normalized listing into B, record completion counts by source, run relevance checks, and then move application configuration to B. Rollback means selecting A again; it does not mean trying to reconstruct overwritten vectors.

Do not optimize this away.

Infrai fits the managed version because crawling, vector operations, and cron scheduling are exposed behind one plain REST API and one key; there is no client SDK version to coordinate. Its public discovery response supplies the method, path, and full request JSON Schema for each capability, so a deployment tool can validate payload files without inventing fields. The supporting advantage here is administrative: the schedule that drives a reindex and the retrieval operations it drives share one credential boundary.

The following Go program demonstrates that boundary without guessing request fields. It sends verified scrape, collection-create, and cron-create operations with the same bearer key and base URL. The scrape response becomes an immutable handoff artifact before the schedule is created; each request body remains schema-owned configuration that must be checked against the public discovery response.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "time"
)

type client struct {
    key  string
    baseURL string
    http *http.Client
}

func (c client) call(method, path string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(method, c.baseURL+path, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+c.key)
        req.Header.Set("Content-Type", "application/json")
        resp, err := c.http.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Duration(seconds) * time.Second
            } else if retryAt, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
                delay = time.Until(retryAt)
            }
            if delay > 0 {
                time.Sleep(delay)
            }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s %s: %s", method, path, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry limit reached")
}

func mustJSON(path string) []byte {
    data, err := os.ReadFile(filepath.Clean(path))
    if err != nil {
        panic(err)
    }
    var value any
    if err := json.Unmarshal(data, &value); err != nil {
        panic(fmt.Errorf("%s: %w", path, err))
    }
    return data
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }
    baseURL := os.Getenv("INFRAI_BASE_URL")
    if baseURL == "" {
        panic("INFRAI_BASE_URL is required")
    }
    c := client{key: key, baseURL: baseURL, http: &http.Client{Timeout: 30 * time.Second}}

    scrape, err := c.call("POST", "/web/scrape", mustJSON("config/scrape.json"))
    if err != nil {
        panic(err)
    }
    if err := os.WriteFile("config/latest-scrape-response.json", scrape, 0600); err != nil {
        panic(err)
    }
    if _, err := c.call("POST", "/vector/collection/create", mustJSON("config/collection.json")); err != nil {
        panic(err)
    }
    if _, err := c.call("POST", "/cron/create", mustJSON("config/nightly-reindex.json")); err != nil {
        panic(err)
    }
}
```

These payloads should be generated or checked against `GET /v1/discovery/{capability}` before the program runs. Writes need stable client identities or the platform's `Idempotency-Key` convention so retries cannot create duplicate schedules or collections. A 429 response requires backoff, and every non-2xx body must reach the operator because silent partial ingestion destroys reconciliation evidence.

## Managed REST or a Split Stack

The choice is not a generic vendor ranking. It is a decision about the boundary around one ingestion transaction. In the alternative stack, system cron starts Scrapy, Scrapy obtains listings, and Pinecone stores and searches vectors. That means three separately operated components, signup or provisioning work for the hosted parts, distinct credential sets for the crawler and vector service, and glue that transfers crawl output into embedding and indexing jobs. The operator also owns retry identity and cross-system audit correlation.

| Option | Retrieval role | Latency and operating trade-off | Best boundary |
|---|---|---|---|
| Infrai | Scrape, vector collection operations, queries, and scheduling through REST | Fewer credential and integration handoffs; one vendor becomes one trust, billing, and outage surface | Teams prioritizing a short path from crawl to searchable listing |
| Pinecone | Vector storage and retrieval in the split design | Keeps the vector system independently selectable; crawler and scheduler integration remains application work | Teams wanting a dedicated vector-service boundary |
| Qdrant | Alternative vector collection boundary | Keeps vector retrieval separate from crawling and scheduling | Teams standardizing on Qdrant as their vector service |
| Milvus | Alternative vector collection boundary | Also leaves crawl-to-index delivery and scheduling outside the vector layer | Teams standardizing on Milvus as their vector service |
| Scrapy | Crawling in the split design | Gives the crawler its own lifecycle; output still needs normalization and delivery to the index | Teams wanting direct ownership of crawl behavior |
| system cron | Time-based initiation in the split design | Familiar local scheduling; execution history and downstream correlation must be assembled across components | Small, host-bound schedules with existing operations discipline |

A managed REST boundary wins when ingestion latency is being lost at service handoffs and the team accepts a consolidated provider. The split stack wins when crawler control, vector-provider independence, or separate failure domains are mandatory. **For fast multi-source listing aggregation, start with the managed boundary unless one of those independence requirements is explicit.** This is a conditional choice, not a claim that consolidation is universally superior.

Retrieval quality remains upstream of vendor selection. The collection cannot repair weak normalization, an unsuitable embedding model, or metadata that omits source identity. Likewise, a faster ingestion path does not prove better relevance. Measure freshness and retrieval quality separately, then decide which one constrains the product.

Keep those measurements separate.

## Roll Out Without Corrupting the Index

Begin with one source and one collection generation. Record the embedding model and dimension beside the collection identifier, use deterministic listing identities, and retain the source revision in metadata. Run the nightly path twice on the same input; the second run should produce the same externally visible records, not duplicates. Reconcile source counts against indexed records before adding another source.

For an embedding-model change, create the next collection rather than altering the current one. Backfill it, compare retrieval results on a fixed set of real listing queries, and switch reads only after coverage and quality checks pass. Keep the previous generation available for rollback until the new generation's audit record is complete.

The fixed dimension is useful, but it is only the visible part of the invariant. The real contract is collection generation equals embedding model plus dimension plus indexed corpus state. Preserve that equation and both latency work and relevance evaluation remain explainable. Break it, and a perfectly successful similarity call can return an answer no one can audit.

## Sources

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Milvus documentation](https://milvus.io/docs)
- [Scrapy documentation](https://docs.scrapy.org/)
- [GNU Cron manual](https://www.gnu.org/software/mcron/manual/html_node/Crontab-file.html)
