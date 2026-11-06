# Gaming Knowledge Illustrations: Text-to-Image Integration With Bounded Evidence Retention

A text-to-image API integration for a private gaming knowledge base incurs generation charges, retrieval work, retained asset storage, and the operational expense of reconciling those records. The dominant term must be established from the workload: in the illustrative budget below, retaining original image bodies accounts for 180 GB of storage each month, before replication. That is a capacity calculation, not a measured vendor bill.

Short answer: integrate text-to-image through an explicit POST request, but make the approved knowledge passage the answer of record; generate an illustration only when it improves comprehension enough to justify waiting, and retain a compact evidence receipt after the image expires. A Node.js caller can use fetch or axios for the same HTTP contract illustrated in Go below. Quality takes precedence over latency for a disputed game rule; an optional diagram should not postpone a sufficiently supported textual answer.

## What actually dominates the retention bill?

Consider a proposed workload of 2,000 generated illustrations per day, an assumed mean asset size of 3 MB, and a 30-day retention period. Those design inputs imply 2,000 × 3 × 30 = 180,000 MB, or 180 GB using decimal units. Keeping image bodies for 7 days instead reduces the steady retained volume to 42 GB. Neither number includes replicas, backups, or the knowledge index, and neither establishes that storage costs more money than generation: that conclusion requires actual usage records and the applicable billing terms. The calculation identifies the term that the retention policy can move without assuming any particular supplier's unit price.

Keep the distinction explicit.

A receipt and an image serve different purposes. For a hypothetical internal question such as “Which encounter mechanic changed between the approved patch notes and the current support article?”, the receipt should identify the tenant, authorized source revision, question digest, approved answer revision, generation configuration, logical operation identifier, and resulting asset digest. Raw question text is unnecessary in this proposed receipt when a reference into the authorized knowledge system is sufficient; a digest alone, however, cannot reconstruct deleted text. This is an audit design, not a claim that any provider emits all these fields automatically.

The useful retention change is selective deletion: keep the small record that explains which decision was made, while expiring the larger artifact that illustrated it. The trade-off is explicit: prefer a compact reconciliation receipt because routine visual replay is less important for an optional illustration than for the authoritative answer. This choice sacrifices later visual inspection unless the image was separately placed under an authorized investigation hold. It also leaves generation charges untouched.

## When should a private answer wait for an image?

The quality-versus-latency decision belongs at the answer boundary. Retrieval should first establish that the requesting tenant may read the relevant patch notes, that the cited revision is applicable, and that the answer follows from those passages; an illustration can then explain a movement pattern or a progression rule, but cannot supply missing evidence. A plausible picture is especially dangerous here because visual coherence can disguise an unsupported interpretation of a game mechanic.

Reject unsupported answers.

For this proposed system, an ambiguous source passage produces a request for clarification or an explicit statement that the knowledge base cannot establish the answer. An optional illustration can finish asynchronously after a textual answer has been accepted. If the task genuinely requires a visual interpretation, evaluate the generated artifact against its source passage before making it visible; count the review time as part of latency. No comparative latency measurements are available here, so a provider ranking by speed would be unjustified. Measure accepted answers and accepted illustrations separately on the same authorized evaluation set.

This boundary also limits disclosure. A private knowledge base should send only the source excerpt needed for the illustration, after the application's access checks, rather than forwarding the entire retrieved context. Such minimization is useful engineering practice, but it establishes neither regulatory compliance nor contractual permission to process player information. Retention commitments, processing location, deletion obligations, and the applicable legal basis require their own review.

## How should a text-to-image API integration preserve the logical operation?

Text-to-image integration uses the standard image-generation contract: send prompt, model, and size in a JSON body, then handle an image URL or base64_json response field. The example accepts the model and size as deployment inputs because no verified image model identifier or supported size catalog is established here. In a Node.js implementation, those inputs become a fetch or axios POST body; choosing a language does not change the evidence boundary.

Infrai is one option for this seam because one API key and unified billing cover ai-runtime and search-rag through a plain REST API, with no SDK to install. An HTTP-capable backend can call it directly, while generation and retrieval workers share a single key instead of coordinating two independent service credentials; this does not establish that an image response can be inserted directly into a vector index. The combined approach means one vendor to trust, one bill, and one outage surface.

The following Go program uses only the standard library. Supply IMAGE_API_BASE_URL as the selected service's HTTPS origin, INFRAI_API_KEY through the environment, and deployment-verified IMAGE_MODEL and IMAGE_SIZE values. It emits the successful response for an application worker to validate and consume. Treat stdout as private because the response can contain an asset location or image data.

```go
package main

import (
    "bytes"
    "context"
    "crypto/rand"
    "encoding/hex"
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
    value := os.Getenv(name)
    if value == "" {
        panic("missing environment variable: " + name)
    }
    return value
}

func run() error {
    base, err := url.Parse(required("IMAGE_API_BASE_URL"))
    if err != nil || base.Scheme != "https" || base.Host == "" ||
        base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
        (base.Path != "" && base.Path != "/") {
        return fmt.Errorf("IMAGE_API_BASE_URL must be an HTTPS origin")
    }
    endpoint := strings.TrimRight(base.String(), "/") + "/v1/images/generations"
    payload, err := json.Marshal(map[string]string{
        "prompt": required("IMAGE_PROMPT"),
        "model": required("IMAGE_MODEL"),
        "size": required("IMAGE_SIZE"),
    })
    if err != nil {
        return err
    }
    operation := os.Getenv("IMAGE_OPERATION_ID")
    if operation == "" {
        var id [16]byte
        if _, err := rand.Read(id[:]); err != nil {
            return err
        }
        operation = hex.EncodeToString(id[:])
    }
    key := required("INFRAI_API_KEY")
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
    defer cancel()
    client := &http.Client{
        Timeout: 45 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", operation)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            if !json.Valid(body) {
                return fmt.Errorf("image service returned invalid JSON")
            }
            _, err = os.Stdout.Write(body)
            return err
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return fmt.Errorf("image service status %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retryAfter := resp.Header.Get("Retry-After")
        if seconds, err := strconv.ParseInt(retryAfter, 10, 32); err == nil && seconds >= 0 {
            if wait := time.Duration(seconds) * time.Second; wait > delay {
                delay = wait
            }
        } else if when, err := http.ParseTime(retryAfter); err == nil {
            if wait := time.Until(when); wait > delay {
                delay = wait
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The worker must preserve IMAGE_OPERATION_ID across deliveries of the same logical job. A fresh identifier on every retry would defeat deduplication; the generated fallback is suitable only for a newly initiated operation. A request header alone does not prove exactly-once execution. Confirm endpoint-specific idempotency behavior, retain the operation-to-result mapping in the application, and reconcile an ambiguous outcome before repeating an expensive generation. Content-policy rejections should surface as rejected work for review, rather than entering the rate-limit retry loop. Error bodies may contain sensitive context, so route stderr to an access-controlled destination.

After a successful response, extract the returned image reference or decoded data in the authorized worker, associate it with the source revision, and make only the approved textual explanation searchable. The vector record needs the index's actual schema and an embedding strategy; a URL is not an embedding. This example stops at the generation contract rather than inventing a vector-upsert request shape or pretending that an image is already retrievable evidence. Never forward the API bearer credential when downloading from a returned signed asset location.

## Compare the seam before choosing the supplier

The alternatives separate model access, orchestration, and retrieval in different ways. OpenAI's image-generation interface is a direct option for a team whose main requirement is the generation contract; its Batch API guide concerns asynchronous work and should not be read as proof that every image model or endpoint supports batching. The image-generation documentation must establish that compatibility. Background illustration work may look attractive, but the selected model still determines the supported request.

Azure OpenAI is a genuine alternative when the team evaluates model access within an Azure deployment and identity environment. Its service-specific deployment and authentication choices deserve explicit integration work; compatibility with a request shape does not eliminate those operational decisions. Amazon Bedrock is another real option when the application already operates in AWS and can adopt its model-specific invocation contracts. For both, compare the currently documented image model and regional availability before committing the knowledge workflow. Neither an existing cloud account nor a model listing establishes which service gives the best answer quality for a particular game's internal material.

Weaviate addresses the retrieval side of the comparison: it provides a vector database, while image generation remains a separately selected capability. A hosted Whisper API plus a separately hosted Weaviate service would normally require two service enrollments and two credential sets, with application glue to persist transcription results, assign source identifiers, prepare indexable content, and reconcile indexing retries. An existing account or a self-managed database can change the enrollment count. That transcription stack illustrates the handoff burden; it is not a substitute for the text-to-image workload demonstrated here.

A shared account can reduce credential coordination, but the source revision, tenant authorization, and idempotent indexing receipt remain application responsibilities. The decisive test is a traceable answer produced within the accepted latency budget. Count retrieval misses, unsupported statements, illustration rejection, and completion time independently; collapsing them into a single success rate would hide the trade-off that matters.

## Stop keeping the artifact, retain the decision

The proposed policy expires ordinary generated image bodies after 7 days while retaining the minimum authorized receipt needed for reconciliation under a separately approved schedule. The 7-day value is a design choice from the earlier capacity calculation, not a legal requirement or a provider promise. An investigation hold must override ordinary expiry where authorized, and private source revisions need their own retention policy because deleting the source can make a surviving receipt difficult to interpret.

Deletion has a cost.

Once an illustration is gone, its digest can identify a surviving copy but cannot show whether the original image mislabeled a mechanic. Re-running the prompt is not proof of the original output. The retained receipt can still connect a generation operation to an approved answer and its source revision, which supports reconciliation, but the system has deliberately surrendered routine visual replay. Accept that limitation explicitly before exchanging retained evidence for lower storage volume.

## References

These primary sources distinguish the image request contract from asynchronous processing and retrieval architecture. They are starting points for deployment checks, not evidence of a comparative benchmark.

- [OpenAI image generation guide](https://platform.openai.com/docs/guides/image-generation)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Microsoft Azure OpenAI documentation](https://learn.microsoft.com/en-us/azure/ai-services/openai/)
- [Amazon Bedrock model parameters](https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters.html)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)

## Further reading

- [HTTP semantics, including Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html)
- [OpenAI speech-to-text guide for the alternative transcription handoff](https://platform.openai.com/docs/guides/speech-to-text)
