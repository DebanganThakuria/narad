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

An error answer is JSON with one field, `{"error": "..."}`. A few are plain text: an unknown route or method (`404`, `405`), an oversized header block (`431`), and some answers to a request forwarded to another node. Those are `502`, `503` when a partition owner is down, a forwarded ack did not reach its owner, or the leader cannot be reached, and some `500` and `400` answers to a consume forwarded to another node. Decide on the status code; the message is for people. Which endpoint returns which code is listed with each endpoint in the [HTTP API reference](http-api.md).

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
| [`412`](#status-412) | A cluster-wide precondition for remotes is not met (v3.2.0) | No: fix the precondition first |
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

**Where:** every read (`GET` on topics, schemas, children, users, remotes and the cluster), a consume that returns messages, a topic change, an attach, a grants update, `/healthz`, `/readyz` and `/metrics`; and (from v3.2.0) a remote child's dry run, pause, resume and skip, a remote change or test, and a re-encrypt. A batch ack (**v3.1.0**) answers `200` even when some of its handles failed; each handle's own status is in `results`.

**Meaning:** the body holds what you asked for.

## 201 Created {#status-201}

**Where:** create a topic, create a user, and (from v3.2.0) register a remote or attach a remote child.

**Meaning:** created; the body is the new topic, user, remote, or remote child's stub.

## 202 Accepted {#status-202}

**Where:** produce, and batch produce (**v3.1.0**).

**Meaning:** the node that answered has written the message (for a batch, every message) to its [ingress WAL](glossary.md#ingress-wal) and synced it to disk. It will be delivered at least once. What that promises, and what it does not, is in [What a 202 means](../understand/delivery-contract.md#what-202-means).

**What to do:** nothing. Never retry a `202`: the retry is a second message.

## 204 No Content {#status-204}

**Where:** a consume with nothing to return within `wait`, a replay at an offset past the end of the partition, a single ack, extend or nack, deleting a topic or a user, detaching a child, changing a password, starting or cancelling a decommission, and (from v3.2.0) deleting a remote.

**Meaning:** done, or for a consume, no message. The body is empty.

**What to do:** after an empty consume, consume again. With a `wait`, the next request waits for you.

## 400 Bad Request {#status-400}

**Where:** any route.

**Meaning:** the request is malformed or breaks a rule, and the same request will fail the same way. The message says which rule. Common causes:

- JSON that does not parse, a field the endpoint does not know, or a value out of range (a partition count under 3, a retention under one hour, a negative number).
- A produce with an empty body, a `partition` the topic does not have, or a `key` or `partition` given twice.
- A produce whose body the topic's schema refuses, or a schema that cannot be registered or is not compatible with the current version ([Schema validation rules](schema-rules.md)). Since **v3.1.0** that includes a produce body nested deeper than 256 levels (`payload nests deeper than 256 levels`), and a schema whose validation would cost too much: a subschema reached through more than 64 validation paths, or a pattern that costs more than 32 steps per byte ([Schema documents](schema-rules.md#registration)).
- An ack, extend or nack without `receipt_handle`, or with a handle that cannot be decoded (a handle is `partition:offset:nonce`).
- A consume with a bad `wait`, `partition`, `offset` or `max`, a replay (`offset`) without `partition`, or `max` together with `offset`.
- A batch consume (`max`) without an `X-Narad-Client` header ([Required headers](../build/connect.md#required-headers)).
- More than 1,000 messages in a batch produce (more than 100 on v3.1.0), or more than 100 handles in a batch ack.
- A batch produce body sent with `Content-Encoding: zstd` or `gzip` that does not decode (**from v3.2.0**).
- A remote or a remote child whose fields break a rule (**from v3.2.0**): the message names the field and the rule, never the value, so a password never appears in it. A remote URL whose host resolves to an address the address guard refuses is answered `400` too.
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

**Meaning:** the credentials are right, but the user may not do this. The message says what is missing, for example `produce not allowed on this topic`, `no grant on this topic`, `only the topic owner or an admin may modify this topic`, or `admin privileges required`. Every remotes route and a remote child's attach, pause, resume and skip (**v3.2.0**) answer `admin privileges required` to anyone else, and `remotes require security` on a node with security off. The user routes also answer `403` for the rules that protect accounts: you cannot change your own grants, delete your own account, give a grant you do not hold, or touch the root admin's grants.

A topic change is checked twice: by the node that receives it, and again by the cluster leader under the topic's lock, against the topic as it stands there. The leader's refusal reads `only the owner of topic "<name>" or an admin may modify it` (or names the fan-out link, or the missing create grant), and it is what you get when the topic was deleted and created again by someone else after your request was let in. `caller unknown to the leader` means the leader has no record of the user.

**What to do:** ask an admin for the grant, or send the request as the topic's owner. Which grant each route needs is in [Access model and grants](access-model.md).

**Go SDK:** `ErrForbidden`.

## 404 Not Found {#status-404}

**Where:** any route that names a topic, user or member, and any unknown path.

**Meaning:** one of these:

- The topic, user, parent, child or cluster member does not exist, or the two topics named in a detach are not linked. For a change to a topic by a user without `admin`, a topic the receiving node does not have is looked up again once that node has caught up with the cluster leader, so the `404` holds for the whole cluster.
- The path is not a Narad route. This answer is plain text, `404 page not found`.
- A batch produce (**v3.1.0**) reached a node running v3.0.1 or earlier, which does not have the route.
- `/metrics` on the API port of a node that serves metrics on their own listener (`http.metrics_addr`).

**What to do:** check the name. For a batch produce, fall back to single produces while any node a client can reach runs an older release.

**Go SDK:** `ErrNotFound`.

## 405 Method Not Allowed {#status-405}

**Where:** a known path with a method it does not take, such as `PUT /v1/topics`.

**Meaning:** the answer is plain text and its `Allow` header lists the methods the path takes. A `POST`, `PUT` or `PATCH` without an accepted content type gets `415` first.

**What to do:** use a method from `Allow`.

**Go SDK:** `ErrBadRequest`.

## 409 Conflict {#status-409}

**Where:** create, change or delete a topic, attach or detach a child, create a user, produce to a delay child, (from v3.1.0) decommission a node, abort a partition move or forget a Raft server, and (from v3.2.0) produce, consume or ack on a remote child's stub, and register, change or delete a remote.

**Meaning:** the request conflicts with the current state:

- The topic or user already exists, or a topic exists whose name differs from the requested one only in letter case (`Orders` next to `orders`): on a case-insensitive filesystem both would share one directory. The message names the existing topic.
- The attach breaks a [fan-out](glossary.md#fan-out-child) rule: a child has exactly one parent and no children of its own, and a parent has at most 108 children.
- The child's schema history is not identical to the parent's: version by version the same JSON values once every member runs this release (**from v3.1.0**), byte for byte before.
- The topic changed under the request twice in a row (**from v3.1.0**): it was deleted and recreated, or grew, after the leader checked the request against it, and again after the leader read it a second time (`topic changed since it was read`). Nothing was written. Read the topic again before you decide whether the change still applies.
- A delay child's delay is longer than the parent's retention can hold, on attach, on create with `parent`, or when the parent's retention shrinks.
- `schema_base_version` is not the current schema version, the topic already holds 1000 schema versions, or the topic is an attached child whose schema its parent manages.
- A schema change, a create with a schema, or a create-as-child or attach that adopts a parent's schema history would take the topic's stored history past 4 MiB, or every schema in the cluster past 256 MiB (**from v3.1.0**). The message names the budget and what is stored, and says when the history (or the cluster) is already over the budget, stored before it applied, so that no new version fits ([Compatibility](schema-rules.md#compatibility)).
- A produce to a delay child, which only its parent can feed.
- A decommission that could never complete safely (from v3.1.0): the body's `reasons` lists each one with a `code` and a `message` ([Scale out and in](../operate/scaling.md#decommission)).
- A move abort for a partition with no move in flight, or whose move now targets another node than `target`, or that the leader did not apply because the move finished first or is still in flight; the message names the owner and target (from v3.1.0).
- **New in v3.2.0:** a produce, consume or ack on a [remote child](glossary.md#remote-child)'s stub: `remote child "<name>" lives on remote <remote>; consume it there`. Its messages are on the remote.
- **New in v3.2.0:** a detach of a remote child, or a delete of its stub or of its parent, while records of the parent are not yet on the remote. The body carries `lag_messages`, `lag_complete` and `dispatch_backlog` (records each member holds that it answered `202` for and has not committed yet). Wait and try again, or detach with `force=true` to abandon them ([Detach a remote child](../build/remote-children.md#detach)).
- **New in v3.2.0:** a remote child attach that breaks a remote rule: a topic of the child's name exists, the parent has 16 remote children, this cluster already links to that remote topic, the parent's retention is below 24 hours, or a check found the target unusable (the body's `class`; [check classes](remote-children.md#check-classes)). A retention change below 24 hours on a parent with remote children, and any change to a stub, get `409` too.
- **New in v3.2.0:** a remote of that name exists, the cluster holds 64 remotes, the remote changed since it was read, or a delete names a remote that remote children use (the body lists them in `links`).
- A forget (v3.1.0) that names a Raft server that has a member record (decommission it instead), one a partition assignment names as owner or move target, or a voter whose removal could leave the cluster without a quorum (the message names the voters the leader cannot reach).

**What to do:** read the error message and the current state. For a schema conflict, read the current `schema_version` and retry with it as the base. For a create that must succeed once, treat "already exists" as success when the existing topic has the settings you wanted.

**Go SDK:** `ErrExists`.

## 410 Gone {#status-410}

**Where:** ack, extend and nack; a replay.

**Meaning:** for an ack, extend or nack, the [lease](glossary.md#lease) the [receipt handle](glossary.md#receipt-handle) names no longer exists. It ran out, the message was already settled, or it was delivered again under a new handle. The message is not lost; it is on its way to a consumer, maybe another one. For a replay, the record at that offset aged out of retention or cannot be read.

A handle carries no topic, so a handle from another topic, or one naming a partition this topic does not have, is checked against this topic's leases and also answers `410`. If you see `410` on the first ack of a fresh handle, check that the ack goes to the topic the message came from.

**What to do:** do not retry. Treat the work as not committed and expect the message again, which an idempotent handler absorbs ([Handle retries and dead letters](../build/handling-retries.md)). A retried ack that had already landed also answers `410`, which is harmless. For a replay, move to the next offset.

**Go SDK:** `ErrLeaseLost`; `ErrOffsetGone` from a replay read.

## 412 Precondition Failed {#status-412}

**New in v3.2.0.**

**Where:** the remotes routes, and a [remote child](glossary.md#remote-child)'s attach, pause, resume and skip; a delete or detach that involves a remote child, when the leader runs an older release.

**Meaning:** something the whole cluster must satisfy before it sends anything to another cluster is not met, so nothing was sealed, written or sent. The message names it:

- A member does not apply the remote Raft entry types yet: `not every cluster member runs a release that applies the remote Raft entry types; upgrade or remove the member named here: ...`. Every member counts, dead ones and Raft servers without a member record included, so a member that is gone for good must be decommissioned or forgotten. The JSON body names it in `members`.
- A member reports a posture that forbids remotes (security off, or legacy cluster authentication on), or did not answer the posture check. Pause, skip and a remote delete skip the posture check, so they work while a member is down.
- The node that took a create or a password change does not attest an encrypted API hop: `remote writes carry a password, so this node needs an encrypted API hop: set remotes.api_hop_encrypted ...` ([Before you start](../operate/remotes.md#before-you-start)).
- The cluster secret is missing, or decodes to fewer than 32 bytes, so no password may be sealed under it; or the current key has sealed as many passwords as it may (rotate the cluster secret).
- A member holds a stale or unreadable credential, or the members saw different target topic IDs during the checks.
- The cluster leader runs a release without remote children: `the cluster leader runs an older release that does not serve remote children; finish the upgrade first`.

**What to do:** fix the precondition the message names, then send the request again. See [Troubleshooting](../operate/troubleshooting.md#status-412).

**Go SDK:** `ErrBadRequest`.

## 413 Request Entity Too Large {#status-413}

**Where:** produce, batch produce, create a topic, change a topic, a batch ack, and (from v3.2.0) the remotes routes.

**Meaning:** the body is over 1 MiB (1,048,576 bytes), over 64 KiB for a batch ack, or over 128 KiB on a remotes route. **New in v3.2.0:** a batch produce body may be up to 16 MiB, as sent and once decoded, and one message whose payload is over 1 MiB gets `413` for the whole batch (`message 3: message too large`); v3.1.0 caps the whole batch body at 1 MiB. The user routes and attach answer an oversized body with `400` instead.

**What to do:** send a smaller payload. Keep large objects elsewhere and send a reference to them.

**Go SDK:** `ErrTooLarge`.

## 415 Unsupported Media Type {#status-415}

**Where:** every `POST`, `PUT` and `PATCH`, including an ack with no body.

**Meaning:** the request has neither `Content-Type: application/json`, nor `Content-Type: application/octet-stream`, nor an `X-Narad-Client` header. With security on, a request without credentials gets `401` first. **New in v3.2.0:** a batch produce also gets `415` for a `Content-Encoding` other than `zstd`, `gzip` or none (`unsupported Content-Encoding: want zstd, gzip or none`).

**What to do:** add one of the headers ([Required headers](../build/connect.md#required-headers)).

**Go SDK:** `ErrBadRequest`.

## 421 Misdirected Request {#status-421}

**Where:** ack, extend and nack; a replay, or a consume pinned with `partition`; get a topic, from a v3.0.1 node only.

**Meaning:** `this node does not own the requested partition`. The partition moved to another node while the request was served. From a v3.0.1 node, get a topic also answers `421` when the owner of one of the topic's partitions could not be found or refused to answer; an upgraded node (**v3.1.0**) answers `200` instead, with `partial: true` and the unavailable partitions marked `owner_unavailable` ([Get a topic](http-api.md#get-topic)).

**What to do:** retry with backoff. If get a topic keeps answering `421` from every node, see [Troubleshooting](../operate/troubleshooting.md#status-421).

**Go SDK:** `ErrUnavailable`.

## 429 Too Many Requests {#status-429}

**Where:** consume, produce, and any request with credentials.

**Meaning:** one of three limits, each counted per node:

| Limit | Setting |
|---|---|
| Concurrent consumes per user, or per client IP with security off; a batch consume counts as its `max`, clamped to the cap | `http.max_consume_in_flight_per_identity`, 1024 by default |
| Concurrent produces per user (v3.1.0); a batch produce counts as its message count, clamped to the cap | `http.max_produce_in_flight_per_identity`, off by default |
| Wrong passwords for one existing user: 5, then one attempt every 12 seconds; and, for a user with recent failures, the node's failure budget of 32 checks, refilled at 4 a second (from v3.1.0) | none |
| Remote writes (v3.2.0): 10 a minute per node on the remotes routes, 60 a minute per node for remote child attaches, pauses, resumes and skips; one check of a remote per node every 5 seconds; one unshipped check per parent every 10 seconds | none |

The error message says which limit was hit, in the same order:

- `too many in-flight consume requests for this identity (limit N per node)`
- `too many in-flight produce requests for this identity (limit N per node)` (v3.1.0)
- `too many failed authentication attempts`
- (v3.2.0) `at most 10 remote writes a minute on this node`, `too many remote child writes on this node; retry later`, `a check for this remote is already running or ran less than 5s ago; retry`, `an unshipped check for this parent ran less than 10s ago; retry`, each with `Retry-After`

**What to do:** back off and retry, or run fewer requests at once. For the authentication limit, fix the password first. See [Troubleshooting](../operate/troubleshooting.md#status-429).

**Go SDK:** `ErrThrottled`.

## 431 Request Header Fields Too Large {#status-431}

**Where:** any request.

**Meaning:** the request's headers are larger than `http.max_header_bytes` (64 KiB by default). The answer is plain text.

**What to do:** send smaller headers.

**Go SDK:** `ErrBadRequest`.

## 499 Client Closed Request {#status-499}

**Where:** any request whose client disconnected before the answer, including an ack, extend or nack that this node forwarded to the partition's owner (single, or one handle's result in a batch ack).

**Meaning:** Narad records the request as `499`, not as a server error. A forwarded ack whose client left may still have been applied by the owner; a retry of one that landed answers `410`. No client receives it; it shows in `narad_http_requests_total{status="499"}` and the logs ([Metrics reference](metrics.md#traffic)).

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

**Where:** `POST /v1/cluster/members/{id}/forget` (new in v3.1.0), on a node that forwarded it to a Raft leader running an older release; (from v3.2.0) the remotes routes and a remote child's routes on a node that has no remote plane.

**Meaning:** the leader's release does not know the operation, so nothing changed: `the leader runs a release that cannot forget a Raft server; upgrade it first`. **New in v3.2.0:** `remotes are not available on this node` or `remote children are not available on this node` means the node that answered was built without the remote plane; send the request to another node.

**What to do:** finish upgrading the cluster, the leader included, then send it again.

**Go SDK:** `ErrServer`.

## 502 Bad Gateway {#status-502}

**Where:** ack, extend and nack (single, or one handle's result in a batch ack); a replay, or a consume pinned with `partition`; and (from v3.2.0) a remote child's attach or resume.

**Meaning:** the node you reached forwarded the request to the partition's [owner](glossary.md#owner) and got no answer: the owner stopped responding or the connection failed after the request went out. For an ack, extend or nack this means the owner may have applied it: `the partition owner did not answer; the ack may have been applied; retry`. An ack that never left the node you reached is a [`503`](#status-503) instead. The body is plain text and names neither the owner nor the transport error, which the node logs ([Troubleshooting](../operate/troubleshooting.md#status-502)); releases up to v3.2.1 put both in the body and answered `502` for every failed forward of an ack. **New in v3.2.0:** for a remote child's attach or resume, the remote checks got an answer that proves nothing about the target: something in front of it (a load balancer, a proxy) answered instead (`edge`), or it answered with a redirect, which is never followed (`redirect_refused`).

**What to do:** retry with backoff. An ack cannot settle twice, so a retry of one that had already landed answers `410` and changes nothing. For a batch ack, retry only the handles whose result is `502`. See [Troubleshooting](../operate/troubleshooting.md#status-502).

**Go SDK:** `ErrServer`.

## 503 Service Unavailable {#status-503}

**Where:**

- Ack, extend and nack, a replay, or a consume pinned with `partition`, when the partition's owner is down: `partition owner is down; retry later` (plain text), with `Retry-After: 1` (releases up to v3.2.1 sent no `Retry-After`).
- Ack, extend and nack (single, or one handle's result in a batch ack) that this node forwards to the partition's owner, when the forward never left the node: it waited 2 seconds for a free slot to the owner, or no connection to the owner could be had. Nothing was applied: `the ack did not reach the partition owner and was not applied; retry` (plain text), with `Retry-After: 1` on a single ack. A `410` on the retry is then a real loss of the lease, not a sign that this attempt landed. Releases up to v3.2.1 answered these with `502`.
- Any change to cluster metadata (topics, fan-out links, users, decommission) while the cluster has no Raft leader or the leader cannot be reached, including a change by a user without `admin` naming a topic the receiving node does not have, when that node cannot catch up with the leader to confirm it. A leader elected moments ago may also answer `503` once while it finishes applying the log.
- A change to cluster metadata whose Raft leader lost its leadership, or stopped, while committing it (**from v3.1.0**): `control plane temporarily unavailable: the change may still be applied, read it back before retrying: ...`. A later leader may still commit the change, so read the record back before you retry; a retried create of a topic that did land answers `409`, a retried delete `404`.
- `/readyz` while the node should not take traffic, and `/healthz` once the node is shutting down.
- A topic create or partition increase while every live node is being decommissioned (**from v3.1.0**): `every live member is being decommissioned, so no member can take new partitions; ...`. New partitions are never placed on a draining node. The request succeeds once a node that is not draining is alive: wait for restarting nodes, cancel a decommission, or add a node ([Decommission a node](../operate/scaling.md#decommission)).
- Get a topic (**from v3.1.0**), when the answering node cannot read its own copy of the cluster metadata, for example while it catches up after a restart. A partition owner being down is not a `503`: the answer is a `200` with `partial: true`.
- A produce to a topic with a schema whose validation found no free slot on the node within 5 seconds (**from v3.1.0**): `schema: validation capacity busy, retry`. The payload was not checked or stored; retry it, preferably through another node ([Validation capacity](schema-rules.md#validation-capacity)).
- A move abort that reached the leader but whose outcome could not be read back from it (**v3.1.0**). A retry is safe; list the moves to see where the move stands.
- A batch produce body over 1 MiB while the node's batch body budget is full (**from v3.2.0**): `batch produce bodies in flight on this node are at their limit; retry`, with `Retry-After: 1`. Nothing was read or stored ([Produce a batch](../build/producing.md#produce-batch)).
- A remote write, or a remote child's attach, pause, resume, skip or delete, whose forward to the leader got no answer (**from v3.2.0**): `the metastore leader could not be reached; retry` (remotes) or `the cluster leader could not be reached; retry` (remote children). The write may still have committed; read it back first. A detach or delete of a remote child also answers `503` when the leader could not run its unshipped check.
- On a produce (single or batch) to a node being decommissioned (**from v3.1.0**): `this node is being decommissioned and takes no new produce; send it to another node`, with `Retry-After: 1`. Nothing was stored, so a retry on another node cannot duplicate. v3.0.1 never answers a produce with `503`. Any other `503` on a produce comes from a proxy in front of Narad ([Troubleshooting](../operate/troubleshooting.md#produce-503)).

**Meaning:** the cluster cannot do this right now. Messages stored on a node that is down wait for it to come back; see the [failure matrix](../understand/delivery-contract.md#failure-matrix).

**What to do:** retry with backoff. See [Troubleshooting](../operate/troubleshooting.md#status-503).

**Go SDK:** `ErrUnavailable`.

## Retry rules {#retry-rules}

- **`2xx`:** done. Never retry a `202`.
- **`4xx`:** do not retry unchanged. Two exceptions: retry `429` after a backoff, and retry `421`, which is a routing race.
- **`410` on an ack:** do not retry. The message comes back; your handler must be idempotent. A `410` on the retry of an ack that got `502`, `499` or no answer most likely means that attempt landed; after a `503` it means the lease is gone.
- **`500`, `502`, `503`:** retry with backoff. Where you can, send the retry to another node.
- **Timeouts and dropped connections on a produce:** the message may or may not have been accepted, so a retry can store it twice. That is safe only because consumers are idempotent. A batch produce that times out or gets a `5xx` is ambiguous as a whole: any of its messages may have been stored.
- **Timeouts on an ack:** retry. A duplicate ack answers `410` and changes nothing.
- **Batch ack:** the request answers `200`; retry only the handles whose result is `502` or `503`.

The [Go SDK](../build/go-sdk.md) applies these rules: its `Retryable` reports whether a retry can succeed, and `Uncertain` whether the server may already have applied the request. Why acks need retries at all is in [Handle retries and dead letters](../build/handling-retries.md).
