---
description: "Look up any status code Narad returns, what it means, and whether to retry the request."
search:
  boost: 2
---

# Status codes and errors

Look up any status code Narad returns, what it means, and whether to retry the request.

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/invoices/produce" \
  -H "Content-Type: application/json" \
  -d '{"id": 1}'
```

```http title="Response"
HTTP/1.1 404 Not Found
Content-Length: 28
Content-Type: application/json
Date: Mon, 28 Sep 2026 21:48:17 GMT

{"error":"topic not found"}
```

- `$NARAD` is the base URL of any node or of the load balancer, for example `http://127.0.0.1:7942`.
- `$AUTH` is `username:password` of a user with the grant the request needs.

An error answer is JSON with one field, `{"error": "..."}`. A few are plain text: an unknown route or method (`404`, `405`), an oversized header block (`431`), and some answers to a request forwarded to another node. Those are `502`, `503` when a partition owner is down or the leader cannot be reached, and some `500` and `400` answers to a consume forwarded to another node. Decide on the status code; the message is for people. Which endpoint returns which code is listed with each endpoint in the [HTTP API reference](http-api.md).

The "Go SDK" lines name the error the [Go SDK](../build/go-sdk.md) returns for each code, checked against narad-go at commit `377c853`. Match them with `errors.Is`.

| Code | Meaning | Retry |
|---|---|---|
| [`200`](#status-200) | Here is the data | No |
| [`201`](#status-201) | Created | No |
| [`202`](#status-202) | Message accepted and on disk | No |
| [`204`](#status-204) | Done, or nothing to return | No |
| [`400`](#status-400) | The request is wrong | No: fix the request first |
| [`401`](#status-401) | Missing or wrong credentials | No: fix the request first |
| [`403`](#status-403) | Not allowed | No: fix the request first |
| [`404`](#status-404) | No such topic, user, member or route | No: fix the request first |
| [`405`](#status-405) | Wrong method for this route | No: fix the request first |
| [`409`](#status-409) | Conflicts with the current state | After re-reading the state |
| [`410`](#status-410) | The lease is gone, or the offset aged out | No |
| [`413`](#status-413) | Body too large | No: fix the request first |
| [`415`](#status-415) | Missing content type | No: fix the request first |
| [`421`](#status-421) | A partition's owner could not serve it | Yes, with backoff |
| [`429`](#status-429) | Too many requests for this user | Yes, with backoff |
| [`431`](#status-431) | Header block too large | No: fix the request first |
| [`499`](#status-499) | The client went away | Not applicable |
| [`500`](#status-500) | The node failed | Yes, with backoff |
| [`501`](#status-501) | The leader's release cannot do this | No: upgrade the leader first |
| [`502`](#status-502) | A forwarded request got no answer | Yes, with backoff |
| [`503`](#status-503) | Temporarily unavailable | Yes, with backoff |

## 200 OK {#status-200}

**Where:** every read (`GET` on topics, schemas, children, users and the cluster), a consume that returns messages, a topic change, an attach, a grants update, `/healthz`, `/readyz` and `/metrics`. A batch ack (**Unreleased**) answers `200` even when some of its handles failed; each handle's own status is in `results`.

**Meaning:** the body holds what you asked for.

## 201 Created {#status-201}

**Where:** create a topic, create a user.

**Meaning:** created; the body is the new topic or user.

## 202 Accepted {#status-202}

**Where:** produce, and batch produce (**Unreleased**).

**Meaning:** the node that answered has written the message (for a batch, every message) to its [ingress WAL](glossary.md#ingress-wal) and synced it to disk. It will be delivered at least once. What that promises, and what it does not, is in [What a 202 means](../understand/delivery-contract.md#what-202-means).

**What to do:** nothing. Never retry a `202`: the retry is a second message.

## 204 No Content {#status-204}

**Where:** a consume with nothing to return within `wait`, a replay at an offset past the end of the partition, a single ack, extend or nack, deleting a topic or a user, detaching a child, changing a password, and starting or cancelling a decommission.

**Meaning:** done, or for a consume, no message. The body is empty.

**What to do:** after an empty consume, consume again. With a `wait`, the next request waits for you.

## 400 Bad Request {#status-400}

**Where:** any route.

**Meaning:** the request is malformed or breaks a rule, and the same request will fail the same way. The message says which rule. Common causes:

- JSON that does not parse, a field the endpoint does not know, or a value out of range (a partition count under 3, a retention under one hour, a negative number).
- A produce with an empty body, a `partition` the topic does not have, or a `key` or `partition` given twice.
- A produce whose body the topic's schema refuses, or a schema that cannot be registered or is not compatible with the current version ([Schema validation rules](schema-rules.md)).
- An ack, extend or nack without `receipt_handle`, or with a handle that cannot be decoded (a handle is `partition:offset:nonce`).
- A consume with a bad `wait`, `partition`, `offset` or `max`, a replay (`offset`) without `partition`, or `max` together with `offset`.
- A batch consume (`max`) without an `X-Narad-Client` header ([Required headers](../build/connect.md#required-headers)).
- More than 100 messages in a batch produce or 100 handles in a batch ack.
- An invalid username, a password of 0 or more than 72 bytes, or an invalid grant.

**What to do:** fix the request. Do not retry it unchanged.

**Go SDK:** `ErrBadRequest`.

## 401 Unauthorized {#status-401}

**Where:** any `/v1` route and `/metrics` on the API port, when security is on.

**Meaning:** the request has no credentials, or they are wrong. The response carries `WWW-Authenticate: Basic realm="narad"`.

**What to do:** check the username and password. An operator creates users; see [Connect and authenticate](../build/connect.md#credentials).

**Go SDK:** `ErrUnauthenticated`.

## 403 Forbidden {#status-403}

**Where:** any route that checks a [grant](glossary.md#grant), ownership or the `admin` grant.

**Meaning:** the credentials are right, but the user may not do this. The message says what is missing, for example `produce not allowed on this topic`, `no grant on this topic`, `only the topic owner or an admin may modify this topic`, or `admin privileges required`. The user routes also answer `403` for the rules that protect accounts: you cannot change your own grants, delete your own account, give a grant you do not hold, or touch the root admin's grants.

**What to do:** ask an admin for the grant, or send the request as the topic's owner. Which grant each route needs is in [Access model and grants](access-model.md).

**Go SDK:** `ErrForbidden`.

## 404 Not Found {#status-404}

**Where:** any route that names a topic, user or member, and any unknown path.

**Meaning:** one of these:

- The topic, user, parent, child or cluster member does not exist, or the two topics named in a detach are not linked.
- The path is not a Narad route. This answer is plain text, `404 page not found`.
- A batch produce (**Unreleased**) reached a node running v3.0.1 or earlier, which does not have the route.
- `/metrics` on the API port of a node that serves metrics on their own listener (`http.metrics_addr`).

**What to do:** check the name. For a batch produce, fall back to single produces while any node a client can reach runs an older release.

**Go SDK:** `ErrNotFound`.

## 405 Method Not Allowed {#status-405}

**Where:** a known path with a method it does not take, such as `PUT /v1/topics`.

**Meaning:** the answer is plain text and its `Allow` header lists the methods the path takes. A `POST`, `PUT` or `PATCH` without an accepted content type gets `415` first.

**What to do:** use a method from `Allow`.

**Go SDK:** `ErrBadRequest`.

## 409 Conflict {#status-409}

**Where:** create a topic, change a topic, attach a child, create a user, produce to a delay child, and (**Unreleased**) forget a Raft server.

**Meaning:** the request conflicts with the current state:

- The topic or user already exists.
- The attach breaks a [fan-out](glossary.md#fan-out-child) rule: a child has exactly one parent and no children of its own, and a parent has at most 108 children.
- The child's schema history is not identical to the parent's.
- A delay child's delay is longer than the parent's retention can hold, on attach, on create with `parent`, or when the parent's retention shrinks.
- `schema_base_version` is not the current schema version, the topic already holds 1000 schema versions, or the topic is an attached child whose schema its parent manages.
- A produce to a delay child, which only its parent can feed.
- **Unreleased:** a forget names a Raft server that has a member record (decommission it instead), one a partition assignment names as owner or move target, or a voter whose removal could leave the cluster without a quorum (the message names the voters the leader cannot reach).

**What to do:** read the error message and the current state. For a schema conflict, read the current `schema_version` and retry with it as the base. For a create that must succeed once, treat "already exists" as success when the existing topic has the settings you wanted.

**Go SDK:** `ErrExists`.

## 410 Gone {#status-410}

**Where:** ack, extend and nack; a replay.

**Meaning:** for an ack, extend or nack, the [lease](glossary.md#lease) the [receipt handle](glossary.md#receipt-handle) names no longer exists. It ran out, the message was already settled, or it was delivered again under a new handle. The message is not lost; it is on its way to a consumer, maybe another one. For a replay, the record at that offset aged out of retention or cannot be read.

A handle carries no topic, so a handle from another topic, or one naming a partition this topic does not have, is checked against this topic's leases and also answers `410`. If you see `410` on the first ack of a fresh handle, check that the ack goes to the topic the message came from.

**What to do:** do not retry. Treat the work as not committed and expect the message again, which an idempotent handler absorbs ([Handle retries and dead letters](../build/handling-retries.md)). A retried ack that had already landed also answers `410`, which is harmless. For a replay, move to the next offset.

**Go SDK:** `ErrLeaseLost`; `ErrOffsetGone` from a replay read.

## 413 Request Entity Too Large {#status-413}

**Where:** produce, batch produce, create a topic, change a topic, and a batch ack.

**Meaning:** the body is over 1 MiB (1,048,576 bytes), or over 64 KiB for a batch ack. The user routes and attach answer an oversized body with `400` instead.

**What to do:** send a smaller payload. Keep large objects elsewhere and send a reference to them.

**Go SDK:** `ErrTooLarge`.

## 415 Unsupported Media Type {#status-415}

**Where:** every `POST`, `PUT` and `PATCH`, including an ack with no body.

**Meaning:** the request has neither `Content-Type: application/json`, nor `Content-Type: application/octet-stream`, nor an `X-Narad-Client` header. With security on, a request without credentials gets `401` first.

**What to do:** add one of the headers ([Required headers](../build/connect.md#required-headers)).

**Go SDK:** `ErrBadRequest`.

## 421 Misdirected Request {#status-421}

**Where:** get a topic; ack, extend and nack; a replay, or a consume pinned with `partition`.

**Meaning:** `this node does not own the requested partition`. For get a topic, the owner of one of the topic's partitions could not be found or refused to answer. For the others, the partition moved to another node while the request was served.

**What to do:** retry with backoff. If get a topic keeps answering `421` from every node, see [Troubleshooting](../operate/troubleshooting.md#status-421).

**Go SDK:** `ErrUnavailable`.

## 429 Too Many Requests {#status-429}

**Where:** consume, produce, and any request with credentials.

**Meaning:** one of three limits, each counted per node:

| Limit | Setting |
|---|---|
| Concurrent consumes per user, or per client IP with security off; a batch consume counts as its `max`, clamped to the cap | `http.max_consume_in_flight_per_identity`, 1024 by default |
| Concurrent produces per user (unreleased); a batch produce counts as its message count, clamped to the cap | `http.max_produce_in_flight_per_identity`, off by default |
| Wrong passwords for one existing user: 5, then one attempt every 12 seconds; and, for a user with recent failures, the node's failure budget of 32 checks, refilled at 4 a second (unreleased) | none |

The error message says which limit was hit, in the same order:

- `too many in-flight consume requests for this identity (limit N per node)`
- `too many in-flight produce requests for this identity (limit N per node)` (unreleased)
- `too many failed authentication attempts`

**What to do:** back off and retry, or run fewer requests at once. For the authentication limit, fix the password first. See [Troubleshooting](../operate/troubleshooting.md#status-429).

**Go SDK:** `ErrThrottled`.

## 431 Request Header Fields Too Large {#status-431}

**Where:** any request.

**Meaning:** the request's headers are larger than `http.max_header_bytes` (64 KiB by default). The answer is plain text.

**What to do:** send smaller headers.

**Go SDK:** `ErrBadRequest`.

## 499 Client Closed Request {#status-499}

**Where:** any request whose client disconnected before the answer.

**Meaning:** Narad records the request as `499`, not as a server error. No client receives it; it shows in `narad_http_requests_total{status="499"}` and the logs ([Metrics reference](metrics.md#traffic)).

**What to do:** nothing on the server. On the client, look at its timeouts.

## 500 Internal Server Error {#status-500}

**Where:** any route.

**Meaning:** the node failed while handling the request. The message names the operation:

- `produce failed` on every produce: the node could not write or sync its ingress WAL, and it refuses produces until it restarts. Consume keeps working. See [Troubleshooting](../operate/troubleshooting.md#produce-500).
- `authentication unavailable`: the node could not read its user store. See [Troubleshooting](../operate/troubleshooting.md#auth-500).
- `internal server panic`, or another `<operation> failed`: a bug or an unexpected failure, logged on the node.

**What to do:** retry with backoff, against another node if you can. A produce that got `500` may still be delivered, because records written before the failure survive the node's restart, so a retry can store it twice.

**Go SDK:** `ErrServer`.

## 501 Not Implemented {#status-501}

**Where:** **Unreleased:** `POST /v1/cluster/members/{id}/forget`, on a node that forwarded it to a Raft leader running an older release.

**Meaning:** the leader's release does not know the operation, so nothing changed: `the leader runs a release that cannot forget a Raft server; upgrade it first`.

**What to do:** finish upgrading the cluster, the leader included, then send it again.

**Go SDK:** `ErrServer`.

## 502 Bad Gateway {#status-502}

**Where:** ack, extend and nack (single, or one handle's result in a batch ack); a replay, or a consume pinned with `partition`.

**Meaning:** the node you reached forwarded the request to the partition's [owner](glossary.md#owner) and got no answer: the owner stopped responding or the connection failed. The body is plain text.

**What to do:** retry with backoff. An ack cannot settle twice, so a retry of one that had already landed answers `410` and changes nothing. For a batch ack, retry only the handles whose result is `502`. See [Troubleshooting](../operate/troubleshooting.md#status-502).

**Go SDK:** `ErrServer`.

## 503 Service Unavailable {#status-503}

**Where:**

- Ack, extend and nack, a replay, or a consume pinned with `partition`, when the partition's owner is down: `partition owner is down; retry later` (plain text).
- Any change to cluster metadata (topics, fan-out links, users, decommission) while the cluster has no Raft leader or the leader cannot be reached.
- `/readyz` while the node should not take traffic, and `/healthz` once the node is shutting down.
- Never on a produce: a `503` there comes from a proxy in front of Narad ([Troubleshooting](../operate/troubleshooting.md#produce-503)).

**Meaning:** the cluster cannot do this right now. Messages stored on a node that is down wait for it to come back; see the [failure matrix](../understand/delivery-contract.md#failure-matrix).

**What to do:** retry with backoff. See [Troubleshooting](../operate/troubleshooting.md#status-503).

**Go SDK:** `ErrUnavailable`.

## Retry rules {#retry-rules}

- **`2xx`:** done. Never retry a `202`.
- **`4xx`:** do not retry unchanged. Two exceptions: retry `429` after a backoff, and retry `421`, which is a routing race.
- **`410` on an ack:** do not retry. The message comes back; your handler must be idempotent.
- **`500`, `502`, `503`:** retry with backoff. Where you can, send the retry to another node.
- **Timeouts and dropped connections on a produce:** the message may or may not have been accepted, so a retry can store it twice. That is safe only because consumers are idempotent. A batch produce that times out or gets a `5xx` is ambiguous as a whole: any of its messages may have been stored.
- **Timeouts on an ack:** retry. A duplicate ack answers `410` and changes nothing.
- **Batch ack:** the request answers `200`; retry only the handles whose result is `502` or `503`.

The [Go SDK](../build/go-sdk.md) applies these rules: its `Retryable` reports whether a retry can succeed, and `Uncertain` whether the server may already have applied the request. Why acks need retries at all is in [Handle retries and dead letters](../build/handling-retries.md).
