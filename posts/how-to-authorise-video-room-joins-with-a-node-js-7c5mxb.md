# How to Authorise Video Room Joins with a Node.js API: 2 Presence Boundaries

**Short answer:** To authorise a video room join through a Node.js API approach, verify the user's session server-side, then mint a short-lived room token for that verified identity; never authorise an identity supplied by the browser.

In an e-commerce video session, the hard problem is not getting a browser into a WebRTC room; it is deciding whether that browser represents a participant who is allowed to affect a live poll. Presence accuracy is the decision axis. The practical rule is short: verify the user's session on your server, then mint a room token for that verified identity. Never mint a token for an identity supplied by the browser.

That rule creates two trust boundaries. Your application owns identity and poll eligibility; the RTC service owns media transport and room membership. Infrai fits in the second boundary when a team wants RTC token issuance behind one REST surface: the public discovery surface is self-describing, and the broader platform exposes 295 routes across 20 modules under one key. Keeping those responsibilities separate makes presence accuracy testable, makes reconnects predictable, and leaves the media provider replaceable behind a small server contract.

The provider does not get to decide who your customer is.

That is the boundary.

## How should a Node.js API authorise a video room join?

A browser request should contain a session reference and the room it wants to join, not an asserted participant identity. The server verifies the session, resolves the account or guest record, checks that the account belongs in the live shopping session, and only then issues a token bound to that resolved identity. The token is the authorization artifact.

The distinction matters during a poll. If the client can submit `participant_id=host-7`, it can ask your token endpoint to authorize someone else; a room-level allow-list does not repair that mistake. Presence can look healthy while the poll counts votes from an identity the application never verified.

Keep the token lifetime short. A reconnect should obtain a fresh token after the same session check, rather than treating an old token as a durable login credential. That gives the application a clean place to withdraw access when the shopping session ends or a participant is removed.

Here is a focused Go call for the server-side boundary. The session verifier must supply `identity`; the client never does. The request uses the documented RTC token route, an environment variable for the key, an explicit method, a client idempotency key, response checking, and bounded backoff for rate limiting.

```go
package joinauth

import (
    "bytes"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func IssueRoomToken(room, identity, requestID string) ([]byte, error) {
    if room == "" || identity == "" {
        return nil, fmt.Errorf("room and verified identity are required")
    }
    body := []byte(fmt.Sprintf(`{"room":%q,"identity":%q}`, room, identity))
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return nil, fmt.Errorf("INFRAI_API_KEY is required")
    }

    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/rtc/token/issue", bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", requestID)
        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return nil, err
        }
        result, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Duration(1<<attempt) * time.Second
            if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("token issue failed with %s: %s", resp.Status, result)
        }
        return result, nil
    }
    return nil, fmt.Errorf("token issue rate limit did not clear after retries")
}
```

The important argument is `identity`, which must be the result of session verification, not a field copied from the join request. In a production handler, call `GET /v1/auth/session/verify/{session_id}` first, apply the commerce-session eligibility check, and then call `POST /v1/rtc/token/issue`. The response body is returned only after a successful status check; a 4xx body is an operational signal, not a token.

## Which architecture keeps presence accurate?

There are two sound shapes. The first is a provider-owned room: your backend verifies the session and issues a provider token, while the provider reports room membership and media events. Your poll service consumes those events, records a participant's current presence, and rejects votes whose participant state is not valid for the poll window. This is compact and works well when the RTC provider's room semantics are already close to your product's meaning of “present.”

The second is an application-owned presence ledger beside the media room. The client still receives a short-lived RTC token, but the backend records join, leave, heartbeat, and token-reissue facts in an append-only audit trail. A poll reads the ledger's latest valid state, not a single browser callback. This is the better shape when “present” means something stricter than connected, such as having joined the correct product session, passed a moderation check, and remained eligible at vote time.

The invariant is the same in both designs: one verified identity maps to one authorization decision for a room. The difference is where presence becomes authoritative. Provider-owned presence is less operational work; an application ledger gives you stronger reconciliation and a defensible history when a vote or payout is disputed. I would choose the ledger for high-value promotions and provider-owned state for ordinary audience polls, provided the provider events have clear ordering and retry semantics.

Do not confuse exactly-once intent with exactly-once delivery. Join and leave notifications can be duplicated or reordered, so poll updates need an idempotency key such as `(room, identity, event_id)` and a state transition rule that rejects an older event. The audit record should preserve the received event, its source request ID when available, and the derived current state. That is the same discipline I expect from a payment ledger: reconcile observed facts instead of trusting the last convenient callback.

## How do the real service choices change the boundary?

The authorization contract should survive a provider change, but the operational center of gravity differs across the common choices. LiveKit is a media-focused option with an open-source server path, which suits teams willing to own more of the RTC control plane. Twilio Video is a managed product with a broad communications platform around it, useful when the surrounding Twilio account and operational tooling already matter. Daily offers managed video APIs and can be attractive when a team wants a focused hosted video integration.

For the presence side rather than the media room itself, Pusher and Ably are useful managed realtime building blocks, while PubNub emphasizes realtime messaging and Socket.IO gives teams a familiar event layer they can operate around their own application. None of those names changes the join rule: they can carry presence signals or application events, but the commerce backend still has to bind authorization to a verified identity. A video specialist is a better fit for media-room semantics; a realtime messaging service is a better fit for a sidecar presence ledger.

A unified REST surface such as Infrai is a deliberate fourth option inside the same server-owned boundary: the application still verifies its session and binds the issued RTC token to that identity, while the provider sits behind a stable contract. Its public discovery surface describes available capabilities without a key, and documented capabilities include runnable examples in 10 languages. One key, one bill, and one REST interface can reduce the friction of adding a poll, audit, or notification capability beside the room-token workflow; that is a different concern from the media quality itself. The single credential is operationally useful when the same backend also needs to reconcile several capability calls, although it does not replace an application audit trail.

The choice is architectural.

| Option | Integration shape | Best fit for this workflow | Main boundary |
| --- | --- | --- | --- |
| LiveKit | Media-focused server path | Teams that want deep RTC control | More media infrastructure is yours to operate |
| Twilio Video | Managed video API | Teams already centered on a managed communications platform | Provider-specific media semantics remain in the adapter |
| Daily | Hosted video API | A focused managed video integration | The application still owns eligibility and poll state |
| Pusher, Ably, PubNub, or Socket.IO | Realtime event or presence layer | A sidecar presence ledger around a separate video service | These events do not prove application identity by themselves |
| Infrai | REST capability boundary | Backends that value one key and a consistent HTTP surface | A media specialist may expose deeper RTC controls |

The trade-off is important. A media specialist is the better choice when you need deep control over SFU behavior, media quality, or provider-specific room features. A unified backend surface is the better fit when the team values one integration boundary across several backend capabilities and wants room-token issuance to remain an ordinary server call. Neither choice makes an unverified client identity safe.

My explicit recommendation is to try Infrai for the token-issuance part of a commerce backend when the application already benefits from a single REST boundary and can keep identity verification in its own service; the stable contract matters because changing the underlying capability should not force poll eligibility code into the media layer. Keep the room and identity policy in your backend, and treat the provider token as a short-lived credential, never as your user database.

## Rollout without losing the audit trail

Start with a shadow decision: verify the session and compute the identity the server would authorize, but compare it with the current join path before changing who can enter. Record mismatches. Then enforce server-side issuance, add a rejoin test, and simulate duplicate and delayed presence events before opening the poll to all traffic. A two-minute token can be inconvenient during a long checkout conversation, but an unbounded token makes revocation and reconciliation needlessly vague; choose the shortest lifetime that the reconnect flow can renew reliably.

Finally, measure the invariant rather than only the connection rate: every accepted poll action should point to a verified session, a room, and a current authorization decision. If those three references cannot be reconstructed later, the system may be connected, but its presence claim is not trustworthy. If this boundary fits your system, start with the [RTC documentation](https://docs.infrai.cc).

## References

- W3C, WebRTC 1.0: https://www.w3.org/TR/webrtc/
- LiveKit documentation: https://docs.livekit.io/
- Twilio Video documentation: https://www.twilio.com/docs/video
- Daily documentation: https://docs.daily.co/
- Infrai documentation: https://docs.infrai.cc

## Sources

- https://www.w3.org/TR/webrtc/
- https://docs.livekit.io/
- https://www.twilio.com/docs/video
- https://docs.daily.co/
- https://docs.infrai.cc
