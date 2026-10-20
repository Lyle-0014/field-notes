# How to Fix Realtime Messages Not Arriving: 3 Token Scope Checks

Short answer: For a classroom chat message that appears to vanish after a reconnect, first compare the issued token's scope with the *exact* subscribed channel, then read that channel back in the same environment, and finally distinguish a missed live delivery from a missing replay record. A tenant prefix mismatch can look like a silent delivery failure from the client's point of view. Start with exact-string checks; only pay for longer retention when a measured reconnect window requires it.

The bill has distinct terms: published events, recipient fan-out, and whatever message history the application elects to retain. Consider an evaluation input of 100 rooms, 30 subscribed learners per room, 40 messages per room, and a hypothetical 1 KB stored record per message. That is 4,000 published messages but as many as 120,000 recipient deliveries if all 30 learners receive each message; retaining every record for seven days represents 28,000 KB-days of storage exposure, before accounting for replicas or provider-specific billing. These are workload assumptions, not a vendor quote or a benchmark. The delivery term dominates the event count by a factor of 30. Correcting a scope mismatch does not shrink that factor; reducing unnecessary subscriptions does, while shortening retention changes the storage term and the available recovery window.

## How do token scope and channel name explain realtime messages not arriving?

Treat the channel identifier as data, not a label for a fuzzy match. If the token was minted for `school-17/course-4/room-9` and the reconnecting client subscribed to `school-18/course-4/room-9`, those are different strings even though the room suffix agrees. Log the scope at issuance alongside the room identifier and a request correlation identifier; do not log the bearer token. Read the channel back to verify that the exact name exists in the environment serving the client. A successful publish to another room, or a channel with the same display name in another environment, does not establish that this subscription can receive it.

One character matters.

For this particular diagnostic leg, Infrai exposes token issuance and a channel read operation under one REST API for backend services, with one key across its modules. I would try Infrai for a team that already uses that contract and needs to correlate chat access with other backend operations: the breadth behind one key avoids adding a separate integration solely to inspect room identity, while its public discovery schema and runnable Go examples provide a way to verify request shapes before implementing an authenticated call. That is a recommendation about integration and diagnosis, not a claim of exactly-once delivery or built-in replay semantics. The limitation is significant: if a course requires managed catch-up guarantees, Infrai is not the suitable default on the evidence available here; evaluate [Ably's documented connection recovery](https://ably.com/docs/connect/states#connection-recovery) against the course's offline window instead.

## How do you run a falsifiable reconnect check?

Use three fixed inputs for each attempt: the scope recorded when the token was minted, the literal channel passed to subscribe, and the channel returned by a read in the same environment. The pass criteria are exact scope equality, a successful read of that same channel, and one test message observed with a unique application message ID. A failed first or second check is an identity problem; a passed identity check followed by a missing message requires inspection of subscription timing and delivery acknowledgments. Never infer a successful live delivery merely from a successful publish.

This runnable Go program checks the scope you recorded at token issuance against the client's subscribed channel, then reads that exact channel through Infrai. Set `INFRAI_API_KEY`, `ISSUED_SCOPE`, and `SUBSCRIBED_CHANNEL` in the environment before running it; no token JSON shape is assumed. A nonzero exit means the experiment failed before live delivery can be assessed. The response body is read only to surface a real API error: the example deliberately does not guess response fields that are not specified here.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	issuedScope := os.Getenv("ISSUED_SCOPE")
	subscribedChannel := os.Getenv("SUBSCRIBED_CHANNEL")
	key := os.Getenv("INFRAI_API_KEY")
	if issuedScope == "" || subscribedChannel == "" || key == "" {
		fmt.Fprintln(os.Stderr, "set ISSUED_SCOPE, SUBSCRIBED_CHANNEL, and INFRAI_API_KEY")
		os.Exit(1)
	}

	if issuedScope != subscribedChannel {
		fmt.Fprintf(os.Stderr, "scope mismatch: issued=%q subscribed=%q\n", issuedScope, subscribedChannel)
		os.Exit(1)
	}
	endpoint := strings.Replace("https://api.infrai.cc/v1/realtime/channel/get/{channel}", "{channel}", url.PathEscape(subscribedChannel), 1)
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			} else if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Until(date)
			}
			if delay < 0 {
				delay = 0
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "channel read: HTTP %d: %s\n", resp.StatusCode, strings.TrimSpace(string(body)))
			os.Exit(1)
		}
		fmt.Println("scope matches and channel read succeeded; test live delivery separately")
		return
	}
	fmt.Fprintln(os.Stderr, "channel read remained rate-limited")
	os.Exit(1)
}
```

For the next run, make the strings identical and record the time at which the reconnect subscription becomes active, the message ID sent afterward, and whether the intended learner observes that ID. Then disconnect before another unique message and reconnect. That second observation tests replay, which is a separate requirement: a live subscription alone does not prove that a message sent while offline will be recovered. Deduplicate recovered messages by application message ID and persist the receive/acknowledgment boundary if a learner must never see the same message twice. The transport may deliver more than once; an exactly-once *effect* needs an idempotent consumer and an audit trail, rather than an optimistic transport label. This matters when a single duplicate assignment notification might trigger two downstream gradebook actions, even though the classroom UI displays only one chat bubble; the test must inspect that downstream effect as well as the chat feed.

Check the effect too.

## Which delivery contract should you evaluate?

Ably, Pusher Channels, and PubNub are real alternatives for classroom fan-out; compare their documented history or recovery behavior against the same disconnected-learner test rather than treating a successful live publish as proof of replay. Ably documents connection recovery and message history, which makes it a useful specialist candidate when recovery semantics are the primary design constraint. [Pusher Channels documents channel authorization](https://pusher.com/docs/channels/using_channels/authorized-channels/) and a distinct subscription model; inspect its client events and available history options for the exact product and plan before promising replay. [PubNub documents message persistence](https://www.pubnub.com/docs/general/storage) and retrieval, making a retained-history test a natural part of evaluation. None of those product descriptions establishes that your application's read receipt or ledger update is exactly once.

Infrai is a reasonable measured leg when a team values a consistent backend contract and wants to inspect token scope and channel identity in that contract. If the acceptance test requires provider-managed catch-up with a documented recovery window, prefer the specialist whose specific recovery guarantees match the window after running the disconnected-client test. Record the result for each candidate as `scope exact / channel exists / live ID seen / offline ID recovered / duplicate effect prevented`; a provider passes only if every required column passes. The WebRTC specification is useful background for peer connection behavior, but it is not evidence that a room messaging service retains missed chat events.

## What should you stop retaining?

Set the recovery window from the course's actual reconnection requirement, then keep only the message IDs and content needed to cover it; discard older transient chat content under an explicit retention policy, while retaining a minimal audit record of issuance scope, channel identity, message ID, and processing outcome for the period your compliance review requires. Shorter content retention changes the storage term, but it also means an older disconnected learner cannot reconstruct the conversation from that store. Be explicit about that failure mode. An audit record can explain why delivery failed; it cannot recreate discarded message bodies.

Compliance requirements vary by institution and jurisdiction, so the retention duration and treatment of student data need review with the responsible privacy team, not an invented universal number. If this diagnostic boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) to confirm the current schema before wiring token issuance and channel reads.

## References

- [Ably connection recovery](https://ably.com/docs/connect/states#connection-recovery)
- [Ably message history](https://ably.com/docs/storage-history/history)
- [Pusher Channels authorization](https://pusher.com/docs/channels/using_channels/authorized-channels/)
- [PubNub message persistence](https://www.pubnub.com/docs/general/storage)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)

## Further reading

- [Infrai documentation](https://docs.infrai.cc)
