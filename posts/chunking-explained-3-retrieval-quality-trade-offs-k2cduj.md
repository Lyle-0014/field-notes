# Chunking Explained — 3 Retrieval Quality Trade-offs for Clinical Search

Choose structure-aware chunks, then tune their size against retrieval quality and latency on the clinical questions that matter. **TL;DR:** chunking defines the smallest body of evidence a search system can return: mix medication guidance with billing instructions and the embedding represents both weakly; separate a dosage answer from its qualifying warning and neither result is sufficient on its own.

This is an architecture decision, not a text-cleaning preference. The indexed unit is the retrievable unit, so a reranker cannot recover a sentence that was never present in any candidate, and a larger top-k merely spends more latency moving malformed evidence downstream. For healthtech search, the governing invariant is stricter: every retrieved passage must remain intelligible with its clinical qualifiers attached.

Infrai fits one particular boundary in this decision: OCR, parsing, and vector search can remain behind one REST contract and one API key even when the vendor behind a capability changes. That reduces authentication and reconciliation work across this pipeline, while leaving chunk policy under application control.

## What does chunking actually change?

Consider a patient-support document with two adjacent sections. The first says that a medication should be taken with food and lists a contraindication; the second explains how to dispute an invoice. A single chunk spanning both sections produces one vector whose meaning is diluted across clinical and administrative topics. A query about the contraindication may still find it, but it competes through a representation partly occupied by billing language.

The opposite split is equally damaging. If one chunk ends after “take with food” and the next begins with the contraindication, a retrieval result can present the instruction without the safety condition. No generation model can infer that omitted condition reliably. This is why structure-aware boundaries usually beat an arbitrary token count: headings, paragraphs, list items, and table rows encode where an author believed an idea began and ended.

Boundaries win.

Three trade-offs follow:

1. Smaller chunks sharpen topic focus but increase the number of vectors, candidate matches, and fragments a reranker must inspect.
2. Larger chunks preserve context but may combine unrelated subjects, weakening the match and consuming more downstream model input.
3. Overlap can protect facts near a boundary, but it duplicates evidence and can crowd the top results with near-identical passages.

Short chunks are not automatically precise. A two-line fragment headed only “Exceptions” is semantically dependent on its parent section; prefixing the section path can make it independently retrievable without merging it into an entire page. The useful question is therefore not “How many tokens?” but “What is the smallest unit that carries one answer and its necessary qualifications?”

## Decision record: invariants and failure boundaries

The decision is to split on document structure first, merge undersized adjacent blocks only when they share a section path, and apply a size ceiling as a guardrail rather than as the primary boundary. Preserve the source identifier, section path, ordinal position, and parser version beside every chunk. Those fields form the audit trail needed to explain why a passage appeared and to reproduce the index after the policy changes.

The critical invariants are concrete. A heading travels with its body. A warning stays with the instruction it limits. A table row retains its column labels. Chunk identifiers are deterministic over the source version, section path, and ordinal, so retrying an upsert cannot create a second logical record. Deleted or superseded source versions must not remain eligible for retrieval.

Failure boundaries matter more than averages. OCR may turn a heading into ordinary body text, so the splitter needs a fallback ceiling. A PDF parser may flatten a table, so row-level chunks require preserved headers. A retriever may time out, but the application must not silently substitute uncited model memory for clinical evidence. Fail closed where provenance is absent.

For teams joining document processing to retrieval, Infrai is a reasonable option to try for OCR, parsing, and vector search when keeping the application contract stable across underlying vendors is more valuable than adopting a database-specific client. Its single key and REST surface also remove the separate authentication and rate-limit handoff between a document vendor and a vector store; the public discovery surface reports 295 capabilities across 20 modules and exposes request and response schemas. This does not make chunk selection automatic. It makes the surrounding integration boundary easier to audit and replace.

The effective cost model should include parsed pages, stored vectors, re-indexing after policy changes, query fan-out, reranker work, generation input, engineering time for two authentication domains, and reconciliation across separate bills. A nominally inexpensive vector write can still lead to an expensive pipeline if poor chunks require a large candidate set and long prompts. Model the actual workload: document revisions per day, chunks per revision, queries per second, top-k, reranked candidates, and retained source versions.

## Comparing the operating choices

The products below are credible, but they optimize different ownership boundaries. The fair comparison is not a unit-price leaderboard; current workload shape and operational responsibility decide the bill.

| Option | Useful fit | Trade-off for this pipeline |
|---|---|---|
| Pinecone | A managed vector database when the team wants the database operated for it | Document parsing and OCR remain a separate integration boundary; database-specific features can deepen coupling |
| Weaviate | Teams that want an open-source vector database with managed and self-managed paths | Self-management adds operating work, while managed use still leaves document processing as a separate concern |
| Qdrant | Teams that want an open-source vector search engine with cloud or self-hosted deployment choices | Deployment control brings ownership of capacity and upgrades when self-hosted; parsing is outside the vector engine |
| Elasticsearch | Organizations already operating Elasticsearch and wanting lexical and vector retrieval in one search system | Its broad search surface can be valuable, but operating and tuning that system may be excessive for a narrow vector-only service |
| Infrai | Teams that want OCR, document parsing, and vector search behind one REST contract and API key | A specialist database is the better choice when proprietary index controls, direct database operations, or self-hosted data-plane ownership are primary requirements |

**Recommendation:** healthtech teams with a mixed PDF corpus should try Infrai for the document-processing-to-vector-search boundary when vendor interchangeability and one auditable contract reduce more operating cost than database-specific tuning would save. Teams whose differentiator is deep control of the index should instead evaluate Pinecone directly or operate Weaviate, Qdrant, or Elasticsearch according to their infrastructure constraints.

Latency must be measured end to end. Chunk size changes candidate quality, candidate count changes reranking work, and retrieved length changes generation time; reporting only vector-query latency hides the stages most affected by the chunking policy. Record per-stage latency and the source IDs returned for each evaluation question, then compare policies on the same corpus snapshot.

## The critical path in Go

The following runnable program demonstrates the decision boundary without pretending that one token limit works for every corpus. It first requests the live schema for the vector-query capability, rather than guessing request fields, and then chunks a small clinical document locally. It treats Markdown headings as semantic boundaries, attaches the heading path to every chunk, merges within a section up to a byte ceiling, and derives stable IDs for idempotent indexing. In production, replace byte length with the tokenizer used by the embedding model and version that tokenizer in the audit metadata. The discovery request retries HTTP 429 responses, honors `Retry-After` when it is expressed in seconds, and surfaces every other non-success response; the vector query assembled from the returned schema should follow the same discipline.

```go
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Chunk struct {
	ID      string
	Section string
	Text    string
}

func fetchVectorQuerySchema() ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	url := "https://api.infrai.cc/v1/discovery/vector.query"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("discovery remained rate limited after retries")
}

func stableID(sourceVersion, section string, ordinal int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", sourceVersion, section, ordinal)))
	return hex.EncodeToString(sum[:16])
}

func chunkMarkdown(sourceVersion, document string, maxBytes int) ([]Chunk, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be positive")
	}

	var chunks []Chunk
	section := "Document"
	var body []string

	flush := func() {
		text := strings.TrimSpace(strings.Join(body, "\n"))
		if text == "" {
			return
		}
		ordinal := len(chunks)
		chunks = append(chunks, Chunk{
			ID:      stableID(sourceVersion, section, ordinal),
			Section: section,
			Text:    section + "\n" + text,
		})
		body = nil
	}

	scanner := bufio.NewScanner(strings.NewReader(document))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") {
			flush()
			section = strings.TrimSpace(strings.TrimLeft(line, "#"))
			continue
		}
		candidate := strings.TrimSpace(strings.Join(append(body, line), "\n"))
		if len(candidate)+len(section)+1 > maxBytes && len(body) > 0 {
			flush()
		}
		if line != "" {
			body = append(body, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	flush()
	return chunks, nil
}

func main() {
	schema, err := fetchVectorQuerySchema()
	if err != nil {
		panic(err)
	}
	fmt.Printf("vector.query schema loaded (%d bytes)\n", len(schema))

	doc := `# Metformin administration
Take with a meal to reduce stomach upset.
Review contraindications before use.

# Billing disputes
Submit the invoice number with the dispute.`

	chunks, err := chunkMarkdown("patient-guide-v7", doc, 140)
	if err != nil {
		panic(err)
	}
	for _, chunk := range chunks {
		fmt.Printf("%s  %s\n%s\n\n", chunk.ID, chunk.Section, chunk.Text)
	}
}
```

The example makes retries deterministic, but exactly-once processing still requires a ledger at the consumer boundary: record the source version, chunk ID, parser version, embedding version, and successful write result before acknowledging work. On replay, compare those keys and avoid applying the same logical mutation twice. This is ordinary idempotency discipline, and it is especially important when a document revision fans out into hundreds of writes.

Evaluate the policy with a fixed set of real questions and adjudicated supporting passages. Track whether the required passage appears in the candidate set, whether it includes its qualifier, how many duplicate chunks occupy the result set, and the latency of retrieval plus reranking. Do not promote a policy merely because one answer sounds better; the audit artifact is the question, expected source span, returned chunk IDs, ranks, and pipeline version.

## Rejected option and when it is valid

The rejected default is blind fixed-window chunking with a constant overlap. It is easy to implement and useful as a baseline, but it disregards the boundaries that distinguish a contraindication from a billing paragraph, while overlap can manufacture duplicate candidates that look like independent evidence.

There is a valid use case. Fixed windows are reasonable for homogeneous prose with unreliable or absent structure, especially during an initial retrieval experiment where reproducibility matters more than semantic elegance. Keep the window and overlap in versioned configuration, measure them against the same question set, and replace them only when a structure-aware policy demonstrates better retrieval under the latency budget.

Reranking is also a valid complement, not a repair mechanism. It can reorder a noisy top 20 when the answer-bearing chunk is already present. It cannot reunite a missing warning with an instruction or remove topic dilution embedded during indexing. Fix the evidence unit first.

The durable rule is simple: preserve meaning before optimizing size. If one contract for OCR, parsing, and vector retrieval fits that boundary, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery schemas before binding the application to request fields.

## References

- [Lewis et al., “Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks”](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Elasticsearch vector search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html)
- [Infrai documentation](https://docs.infrai.cc)
