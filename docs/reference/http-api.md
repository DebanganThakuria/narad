---
description: "Look up every Narad HTTP endpoint: its parameters, request body, the grant it needs, the status codes it returns and a real example."
search:
  boost: 2
---
# HTTP API reference

<!-- Edited by hand alongside docs/reference/openapi.yaml: a change to an operation, field, parameter or status code goes into both files in the same pull request. -->

Look up every Narad HTTP endpoint: its parameters, request body, the grant it needs, the status codes it returns and a real example.

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/topics/orders?partition=1"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 456
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "name": "orders",
  "id": "681a429c0b683b2d",
  "partitions": 3,
  "retention_ms": 172800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790623903,
  "owner": "admin",
  "role": "parent",
  "children": ["orders-audit"],
  "schema_version": 0,
  "partition_stats": [
    {
      "index": 1,
      "segments": 1,
      "oldest_offset": 0,
      "next_offset": 2,
      "high_watermark": 2,
      "size_bytes": 166,
      "oldest_segment_at": 1790623903,
      "owner_node": "narad-0"
    }
  ]
}
```

- `$NARAD` is the base URL of any node or of the load balancer in front
  of the cluster, for example `http://127.0.0.1:7942`.
- `$AUTH` is `username:password` of a user with the grant the endpoint
  needs. On a node started with `narad server start --dev`, security is
  off and `-u "$AUTH"` can be left out.

The same API as an OpenAPI 3.1 file:
[openapi.yaml](openapi.yaml). Every example on this page was run
against a single node built from `master`. Long JSON bodies are shown
indented; the node sends each one on a single line, which is what
`Content-Length` counts.

## Basics {#basics}

### Base URL

Send requests to any node's API port (`7942` by default) or to the load
balancer in front of the cluster. Every node serves every route and
forwards work to the node that owns the partition, so a client never
needs to know where a partition lives. API routes live under `/v1`;
`/healthz`, `/readyz` and `/metrics` do not. What a `202` promises, and
when a message can arrive twice, is in the
[delivery contract](../understand/delivery-contract.md).

### Authentication

With security on (the default), every `/v1` route needs HTTP Basic
credentials, and what the user may do is set by its
[grants](access-model.md). Missing or wrong credentials get
[`401`](status-codes.md#status-401) with
`WWW-Authenticate: Basic realm="narad"`. After 5 wrong passwords for an
existing user, a node answers [`429`](status-codes.md#status-429) for
that user and allows one more attempt every 12 seconds. `/healthz` and `/readyz` never need
credentials. Narad expects TLS to end in front of it, at an ingress or a
load balancer. With security off (`narad server start --dev`), no
request needs credentials and every grant check passes. How a client
gets its credentials is in
[Connect and authenticate](../build/connect.md#credentials).

### Required headers

`POST`, `PUT` and `PATCH` requests must send one of these headers, or
they get [`415`](status-codes.md#status-415):

- `Content-Type: application/json`
- `Content-Type: application/octet-stream`
- `X-Narad-Client`, with any value

A `charset` or other parameter on the content type is fine. The rule
holds for a request with no body too, such as an ack. `GET` and
`DELETE` are not checked, except a [batch consume](#consume)
(`max`), which must send `X-Narad-Client` or gets
[`400`](status-codes.md#status-400), as a `GET` or a `HEAD`. The rule
stops a web page in a browser from sending state-changing requests
with an operator's cached credentials.

### Limits

| Limit | Value | Over it |
|---|---|---|
| Request body | 1 MiB (1,048,576 bytes) | [`413`](status-codes.md#status-413); `400` on the user and attach-child routes |
| Batch produce body (v3.2.0) | 16 MiB, each message's payload at most 1 MiB | [`413`](status-codes.md#status-413) |
| Batch ack body (v3.1.0) | 64 KiB | [`413`](status-codes.md#status-413) |
| Remotes request body (v3.2.0) | 128 KiB | [`413`](status-codes.md#status-413) |
| Request headers | 64 KiB by default | [`431`](status-codes.md#status-431) |
| Messages per batch produce | 1,000 (from v3.2.0; 100 in v3.1.0) | [`400`](status-codes.md#status-400) |
| Records per batch consume, handles per batch ack (v3.1.0) | 100 | [`400`](status-codes.md#status-400) |
| Consume `wait` | 10 s by default | clamped, with an `X-Narad-Wait-Clamped` response header |
| Concurrent consumes per user (or per client IP with security off), per node | 1024 by default | [`429`](status-codes.md#status-429) |
| Concurrent produces per user, per node (v3.1.0) | off by default | [`429`](status-codes.md#status-429) |
| Batch produce bodies over 1 MiB being read at once, per node (v3.2.0) | 256 MiB by default | [`503`](status-codes.md#status-503) with `Retry-After: 1` |

The limits marked "by default" are settings: `http.max_header_bytes`,
`http.max_consume_wait`, `http.max_consume_in_flight_per_identity`,
`http.max_produce_in_flight_per_identity` (from v3.1.0) and
`http.max_batch_body_bytes_in_flight` (from v3.2.0), in that order,
all in the [Configuration reference](configuration.md#http).

### Errors

An error answer carries a JSON body with one field:

```json
{"error": "topic not found"}
```

A few answers are plain text instead: a route or method the server does
not know (`404`, `405`), a header block that is too large (`431`), and
some answers to a request forwarded to another node. Those are `502`,
`503` when a partition owner is down or the cluster leader cannot be
reached, and some `500` and `400` answers to a consume.
Read the status code first and treat the body as a message for people.

Each endpoint below lists the codes it answers. On top of those, any
request with credentials can get [`429`](status-codes.md#status-429)
after too many wrong passwords, and any request can get
[`500`](status-codes.md#status-500) when the node fails, for example
`authentication unavailable` when it cannot read its user store.
[Status codes and errors](status-codes.md) lists every code and
whether to retry.

### Request and response bodies

Request bodies are strict: a field the endpoint does not know gets
[`400`](status-codes.md#status-400). Responses can gain fields within
`/v1`, so ignore fields you do not know
([API stability](api-stability.md)). Topic `created_at` and message
`timestamp` are Unix seconds; user `created_at_ms` and `updated_at_ms`
are Unix milliseconds.

### Pagination

Only [list topics](#list-topics) pages. Pass `limit` (default 100, at
most 1000) and the `page_token` from the previous answer, and stop when
`next_page_token` is empty. A page can hold fewer topics than `limit`,
or none, while `next_page_token` is still set, because topics you
cannot read are removed after the page is cut.

### Release markers

This page describes `master`. Anything v3.2.0 added is marked
**New in v3.2.0**, and what v3.1.0 added is marked
**New in v3.1.0**. Where an older node answers a request that uses
it differently, its entry says how. Anything merged since the latest
release, v3.2.2, is marked **Unreleased** until it ships. See
[Which release these docs describe](api-stability.md#docs-version).

## Topics {#topics}

Create, read, change and delete topics. Changes to topics are
written through the cluster's Raft log, so they need a quorum of
nodes and answer [`503`](status-codes.md#status-503) while there is
no leader. Task guide: [Manage topics](../build/topics.md).

### Create a topic {#create-topic}

`POST /v1/topics`

Creates a topic and, with `parent`, links it as a fan-out child of
an existing topic in the same call. The caller becomes the topic's
[owner](glossary.md#owner). A field left out, or sent as `0`, takes
the server default, except `retention_ms`, where `0` keeps messages
forever and only leaving it out takes the default.

**Grant needed:** `create` on the topic name. With `parent`, also ownership of the parent or `admin`.

**Request body**

| Field | Description |
|---|---|
| `name`<br>string, required | 1 to 200 characters from `A-Z a-z 0-9 . _ -`, not `.` or `..`, and not differing from an existing topic's name only in letter case (`409`). Topics an earlier release created with names up to 255 characters keep working. |
| `partitions`<br>integer, optional | At least 3 and at most the server's maximum (108 by default). Defaults to the server default (3), or to the parent's count when `parent` is set. It can grow later, never shrink. Both server settings are in the [Configuration reference](configuration.md#topic-defaults). |
| `retention_ms`<br>integer, optional | How long a message is kept after it is written, whether or not it was consumed. At least 3,600,000 (1 hour), or `0` to keep messages forever; a negative value gets `400`. Left out, it takes the server default ([Configuration reference](configuration.md#topic-defaults)): 7 days in the binary, 12 hours in the Helm chart. |
| `visibility_timeout_ms`<br>integer, optional, default `30000` | How long a consumer's lease lasts before the message is delivered again. Defaults to the server default ([Configuration reference](configuration.md#topic-defaults)). Fixed after creation. |
| `max_in_flight_per_partition`<br>integer, optional, default `1024` | Most messages of one partition leased at once. At the cap, consumes find nothing new in that partition until a lease ends. Defaults to the server default ([Configuration reference](configuration.md#topic-defaults)). |
| `max_acked_ahead_per_partition`<br>integer, optional, default `1024` | Most acks one partition holds above its oldest unacked message. At the cap the partition delivers no new messages until that message is acked. Defaults to the server default ([Configuration reference](configuration.md#topic-defaults)). |
| `schema`<br>JSON, optional | A JSON Schema every message must match; registered as version 1. A JSON object or `true`. |
| `parent`<br>string, optional | Create the topic as a fan-out child of this existing topic. Its partitions are placed on other nodes than the parent's where the cluster allows, which is what makes a [replica child](../operate/backups.md#replica-children). |
| `fanout_delay_ms`<br>integer, optional | With `parent`, make the topic a delay child that receives each message this long after the parent committed it. At most 31,536,000,000 (one year). Attaching an existing topic takes `delay_ms` instead. |
| `owner`<br>string, optional | Ignored. The server sets the owner to the caller. |

**Responses**

| Status | Meaning |
|---|---|
| [`201`](status-codes.md#status-201) | Created. The body is the new topic. |
| [`400`](status-codes.md#status-400) | A field is invalid, the name is not allowed, or the schema cannot be registered (from v3.1.0, that includes a schema whose validation would cost too much). |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No `create` grant on the name, or no right to manage `parent`, as the node that answers or the cluster leader sees it. |
| [`404`](status-codes.md#status-404) | `parent` does not exist, also after the answering node caught up with the leader. |
| [`409`](status-codes.md#status-409) | The topic exists, a topic exists whose name differs only in letter case (the error names it), `parent` cannot take this child (role, child limit, schema, or a delay the parent's retention cannot hold), or (from v3.1.0) the schema, or the parent's schema history a child adopts, would pass a schema byte budget, or `parent` was recreated under the request twice in a row (`topic changed since it was read`). |
| [`413`](status-codes.md#status-413) | The body is over 1 MiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the topic, the answering node could not reach the leader to confirm `parent`, or (from v3.1.0) every live node is being decommissioned, so no node can take the new partitions; nothing was written. |

**Response body (`201`)**: a [Topic](#topic-object).

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics" \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "partitions": 3}'
```

```http title="Response"
HTTP/1.1 201 Created
Content-Length: 224
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "name": "orders",
  "id": "681a429c0b683b2d",
  "partitions": 3,
  "retention_ms": 604800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790623903,
  "owner": "admin"
}
```

### List topics {#list-topics}

`GET /v1/topics`

Lists topics in name order, one page at a time. See
[Pagination](#pagination).

**Grant needed:** Any user. The list holds only topics the caller could read with [get a topic](#get-topic).

**Parameters**

| Name | Description |
|---|---|
| `limit`<br>query, integer, optional, default `100` | Page size. Values above 1000 are treated as 1000. |
| `page_token`<br>query, string, optional | The `next_page_token` of the previous page. Leave it out for the first page. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | A page of topics. |
| [`400`](status-codes.md#status-400) | `limit` is not a positive integer. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`500`](status-codes.md#status-500) | The node could not read its topic list. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `topics`<br>array of [Topic](#topic-object) | Topics the caller can read. |
| `next_page_token`<br>string | Pass as `page_token` for the next page; empty when there are no more. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/topics?limit=2"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 540
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "next_page_token": "orders-audit",
  "topics": [
    {
      "name": "orders",
      "id": "681a429c0b683b2d",
      "partitions": 3,
      "retention_ms": 604800000,
      "visibility_timeout_ms": 30000,
      "max_in_flight_per_partition": 1024,
      "max_acked_ahead_per_partition": 1024,
      "created_at": 1790623903,
      "owner": "admin",
      "role": "standalone"
    },
    {
      "name": "orders-audit",
      "id": "91ad229a6dd04bcf",
      "partitions": 3,
      "retention_ms": 604800000,
      "visibility_timeout_ms": 30000,
      "max_in_flight_per_partition": 1024,
      "max_acked_ahead_per_partition": 1024,
      "created_at": 1790623903,
      "owner": "admin",
      "role": "standalone"
    }
  ]
}
```

### Get a topic {#get-topic}

`GET /v1/topics/{topic}`

Returns the topic, its current schema and statistics for each
partition. The node that answers asks every partition's owner for
its statistics.

**New in v3.1.0:** when some partitions' owners are down, the answer
is still `200`. The partitions that could be read carry their
statistics and `status` `ok`; each other one is a placeholder with
zero statistics, `status` `owner_unavailable` and the owner's
`owner_liveness`, and the body carries `partial: true`. Leave the
placeholders out of any total. An owner that does not answer within
2 seconds counts as `unreachable`. A v3.0.1 node answers `421`
instead.

**New in v3.2.0:** a [remote child](glossary.md#remote-child)'s stub
has `partitions: 0` and a `remote` object that names the remote and
the topic there. Its `created_by` and `paused_by` are shown to
admins only.

**Grant needed:** Any grant that matches the topic name, ownership, or `admin`.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |
| `partition`<br>query, integer, optional | Return statistics for this partition only. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The topic with partition statistics. |
| [`400`](status-codes.md#status-400) | `partition` is not a partition of the topic. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No grant on the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist. |
| [`421`](status-codes.md#status-421) | From a v3.0.1 node, the owner of one of the topic's partitions could not be found or refused to answer. An upgraded node answers `200` with `partial` instead. |
| [`503`](status-codes.md#status-503) | The answering node could not read its own copy of the cluster metadata (for example while it catches up after a restart). Retry. |

**Response body (`200`)**: every field of a [Topic](#topic-object), plus:

| Field | Description |
|---|---|
| `schema_version`<br>integer | Current schema version, `0` without a schema. |
| `schema`<br>JSON | The current schema document. Absent without a schema. |
| `partition_stats`<br>array of [Partition statistics](#partition-stats-object) | One entry per partition, or only the one `partition` asked for. |
| `partial` (v3.1.0)<br>boolean | `true` when some entries are placeholders whose owner could not report (`status` `owner_unavailable`). Absent otherwise. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/topics/orders?partition=1"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 470
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "name": "orders",
  "id": "681a429c0b683b2d",
  "partitions": 3,
  "retention_ms": 172800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790623903,
  "owner": "admin",
  "role": "parent",
  "children": ["orders-audit"],
  "schema_version": 0,
  "partition_stats": [
    {
      "index": 1,
      "segments": 1,
      "oldest_offset": 0,
      "next_offset": 2,
      "high_watermark": 2,
      "size_bytes": 166,
      "oldest_segment_at": 1790623903,
      "owner_node": "narad-0",
      "status": "ok"
    }
  ]
}
```

### Change a topic {#alter-topic}

`PATCH /v1/topics/{topic}`

Changes retention, the per-partition caps or the partition count, or
registers a new schema version. Send at least one field.
`visibility_timeout_ms` is fixed when the topic is created.

Fields are applied one group at a time in this order: retention,
then the caps, then partitions, then schema. There is no
transaction across them: the first failure answers its error and
the groups before it stay applied. Send one field per request when
you need all or nothing.

**Grant needed:** Ownership of the topic, or `admin`.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |

**Request body**

| Field | Description |
|---|---|
| `retention_ms`<br>integer, optional | New retention. At least 3,600,000 (1 hour), or `0` to keep messages forever; a negative value gets `400`. To return to the server default, send its value. |
| `max_in_flight_per_partition`<br>integer, optional | New in-flight cap; `0` sets the server default. A cap left out keeps its value. |
| `max_acked_ahead_per_partition`<br>integer, optional | New acked-ahead cap; `0` sets the server default. A cap left out keeps its value. |
| `partitions`<br>integer, optional | New partition count, larger than the current one and at most the server's maximum (108 by default). New keys may then map to other partitions. |
| `schema`<br>JSON, optional | A new schema version, checked for compatibility with the current one. Sending the current schema again changes nothing and answers `200`. A schema cannot be removed (`null` gets `400`). |
| `schema_base_version`<br>integer, optional | With `schema`, apply it only if the current version is exactly this number; `409` otherwise. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Changed. The body is the topic after the change. |
| [`400`](status-codes.md#status-400) | No field given, a value is invalid, `partitions` is not larger than the current count, or the new schema is not compatible. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not the owner and not `admin`, as the node that answers or the cluster leader sees the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist, also after the answering node caught up with the leader. |
| [`409`](status-codes.md#status-409) | `schema_base_version` is not the current version, the history holds 1000 versions or (from v3.1.0) the new version would take it past 4 MiB or the cluster's schemas past 256 MiB, the topic is a child whose schema its parent manages, the new retention is too short for a delay child, or (from v3.1.0) the topic was deleted and recreated, or grew, under the request twice in a row (`topic changed since it was read`). New in v3.2.0: the topic is a remote child's stub, which takes no change, or the new retention is below 24 hours on a parent with remote children (a retention already below it may still grow). |
| [`413`](status-codes.md#status-413) | The body is over 1 MiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change, the answering node could not reach the leader to confirm a topic it does not have, or (from v3.1.0) a partition increase found every live node being decommissioned, so no node can take the new partitions; nothing was changed. |

**Response body (`200`)**: a [Topic](#topic-object).

```sh title="Request"
curl -i -u "$AUTH" -X PATCH "$NARAD/v1/topics/orders" \
  -H "Content-Type: application/json" \
  -d '{"retention_ms": 172800000}'
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 244
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "name": "orders",
  "id": "681a429c0b683b2d",
  "partitions": 3,
  "retention_ms": 172800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790623903,
  "owner": "admin",
  "role": "standalone"
}
```

### Delete a topic {#delete-topic}

`DELETE /v1/topics/{topic}`

Deletes the topic's metadata and its data on every node, and
removes its fan-out links. There is no undo. Once the metadata
delete is committed the answer is `204`, even if a node that is
down could not remove its files yet; that node removes them when
it next starts.

**New in v3.2.0:** deleting a parent that has
[remote children](glossary.md#remote-child) deletes their stubs in
the same entry, and deleting a remote child's stub deletes the link.
Either is refused with `409` while any record of the parent is not
yet on the remote, unless `force=true` abandons them; the leader
checks every member's ingress backlog and every cursor first. The
same refusal, with its body, is in
[Detach a child](#detach-child).

**Grant needed:** Ownership of the topic, or `admin`. New in v3.2.0: a remote child's stub has no owner; the owner of its parent, or an `admin`, deletes it, with security on.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |
| `force` (v3.2.0)<br>query, boolean, optional, default `False` | `true` deletes a parent with remote children, or a remote child's stub, even while records are unshipped, abandoning them. Ignored for any other topic. `narad topic rm --force` never sends it: there `--force` only skips the prompt. |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | Deleted. |
| [`400`](status-codes.md#status-400) | New in v3.2.0. `force` is not `true` or `false`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not the owner and not `admin`, as the node that answers or the cluster leader sees the topic. For a remote child's stub (from v3.2.0), the caller is neither an `admin` nor the parent's owner, or security is off. For a parent with remote children (from v3.2.0), security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | The topic does not exist, also after the answering node caught up with the leader. |
| [`409`](status-codes.md#status-409) | New in v3.1.0. The topic was deleted and recreated under the request twice in a row (`topic changed since it was read`); nothing was deleted. Read the topic again before deleting it. From v3.2.0, for a parent with remote children or a stub without `force`, records of the parent are not yet on the remote (the body carries `lag_messages`, `lag_complete` and `dispatch_backlog`), or the leader found a remote child the answering node did not know of yet; retry. |
| [`412`](status-codes.md#status-412) | New in v3.2.0. The cluster leader runs a release without remote children; finish the upgrade. |
| [`429`](status-codes.md#status-429) | New in v3.2.0. The leader ran an unshipped check for this parent less than 10 seconds ago; retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | New in v3.2.0. The answering node has no remote plane, so it cannot delete a remote-linked topic. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the delete, or the answering node could not reach the leader to confirm a topic it does not have. From v3.2.0, for a remote-linked topic, also when the unshipped check could not run. For a remote-linked topic, when the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

```sh title="Request"
curl -i -u "$AUTH" -X DELETE "$NARAD/v1/topics/orders-audit"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:43 GMT
```

### Get a topic's schema history {#get-schema-history}

`GET /v1/topics/{topic}/schema`

Returns every schema version of the topic, oldest first, and the
number of the current one. A topic without a schema has version `0`
and an empty list. The rules for schemas are in
[Schema validation rules](schema-rules.md).

**Grant needed:** Any grant that matches the topic name, ownership, or `admin`.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The schema history. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No grant on the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `topic`<br>string | Topic name. |
| `version`<br>integer | Current version, `0` without a schema. |
| `versions`<br>array of object | Every version, oldest first. |
| `versions[].version`<br>integer | Version number, from 1. |
| `versions[].schema`<br>JSON | The schema document. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/topics/payments/schema"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 105
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "topic": "payments",
  "version": 1,
  "versions": [
    {"version": 1, "schema": {"type": "object", "required": ["id"]}}
  ]
}
```

## Fan-out children {#fan-out-children}

Link a topic to a parent so that it receives a copy of every message
produced to the parent from the moment of the link. Task guide:
[Fan out and delay messages](../build/fanout-and-delay.md).

**New in v3.2.0:** a child can also be a
[remote child](glossary.md#remote-child), whose copy goes to a topic
on another Narad cluster. Its attach, pause, resume and skip need the
`admin` grant with security on. Task guide:
[Replicate a topic to another cluster](../build/remote-children.md).

### Attach a child {#attach-child}

`POST /v1/topics/{parent}/children`

Links an existing topic as a [fan-out child](glossary.md#fan-out-child)
of `parent`. The child receives every message produced to the parent
from the [attach point](glossary.md#attach-point) on; nothing older
is copied. A child without a schema adopts the parent's schema
history.

The delay field here is `delay_ms`. Creating a child in one call
with [create a topic](#create-topic) takes `fanout_delay_ms`
instead.

**New in v3.2.0:** with `remote`, the request creates `child` as a
[remote child](glossary.md#remote-child) instead: a stub with no
partitions whose copies go, through the remote's batch produce, to
`remote_topic` on that cluster. The child must not exist yet. The
leader runs the attach checks from every member against the target
(it answers `401` without credentials, the topic exists, is no
delay child or stub and has no remote children, the schemas match,
it takes batch produce, the credential is not an admin there),
resolves the start offsets, and writes one Raft entry. With
`dry_run` it stops before the entry and answers what it found.
Without `remotes.allowed_hosts` on the leader, a dry
run checks from the leader alone, its reports carry only `node`,
`result`, `class` and this cluster's own `credential_version`,
`posture` and an empty `warnings` (nothing the target answered: no
`target_id`, `target_serves_ids`, times or certificate expiry),
the top-level `warnings` of a dry run or a `201` holds only the
parent retention warning, a dry run has no `capabilities`,
and a failed attach or resume answers the
`class` and the failing `members` instead of each member's report.
The
fields `remote_topic`, `from`, `lanes` and `dry_run` without
`remote` get `400`. Task guide:
[Replicate a topic to another cluster](../build/remote-children.md).

**Grant needed:** Ownership of both topics, or `admin`. New in v3.2.0: with `remote`, `admin` and a node with security on; ownership is not enough.

**Parameters**

| Name | Description |
|---|---|
| `parent`<br>path, string, required | Name of the parent topic. |

**Request body**

| Field | Description |
|---|---|
| `child`<br>string, required | Name of the existing topic to attach. |
| `delay_ms`<br>integer, optional | Make the child a delay child that receives each message this long after the parent committed it. At most one year. Fixed while attached. |
| `remote` (v3.2.0)<br>string, optional | Create `child` as a remote child that sends to this remote. `child` must not exist yet. |
| `remote_topic` (v3.2.0)<br>string, optional | With `remote`, the topic on the remote. Defaults to the parent's name. |
| `from` (v3.2.0)<br>string: `attach`, `unconsumed`, `earliest`, optional, default `attach` | With `remote`, where the link starts on each parent partition. `attach` at the parent's committed high watermark, as a local child; `unconsumed` at the parent's consumer ack frontier, so everything not yet acked here is sent; `earliest` at the oldest retained record. |
| `lanes` (v3.2.0)<br>integer, optional, default `1` | With `remote`, ordered streams per parent partition. A key always uses one lane; more lanes help a link with a long round trip. |
| `dry_run` (v3.2.0)<br>boolean, optional, default `False` | With `remote`, run every check and resolve the start offsets, and write nothing. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Attached. The body is the parent topic. From v3.2.0, for a remote `dry_run`: every check passed and nothing was written, and the body is a report instead, with `dry_run`, `attach_offsets` (the start offset per parent partition), `checks` (each member's report), `warnings` and, when the leader could probe the target and `remotes.allowed_hosts` is set, `capabilities` (`max_messages` per batch and `zstd`). |
| [`201`](status-codes.md#status-201) | New in v3.2.0. The remote child was created. The body is its stub, plus `warnings` when there are any (a parent retention below 72 hours; with `remotes.allowed_hosts`, also a target that serves no topic IDs, a target whose batch produce takes at most 1 MiB of body, a certificate that expires within 14 days). |
| [`400`](status-codes.md#status-400) | `child` is missing, the two names are the same, or `delay_ms` is out of range. New in v3.2.0: a remote field without `remote`, a name that is not a remote's or a topic's, `lanes` outside 1 to 8, `from` other than `attach`, `unconsumed` or `earliest`, or a remote that does not exist. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | The caller does not manage both topics, as the node that answers or the cluster leader sees them. From v3.2.0, with `remote`, the caller is not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | The parent or the child does not exist, also after the answering node caught up with the leader. |
| [`409`](status-codes.md#status-409) | The link breaks a fan-out rule (a child has one parent and no children), the parent has 108 children, the schemas differ, the delay is longer than the parent's retention can hold, or (from v3.1.0) the copy of the parent's schema history the child adopts would pass the cluster's schema byte budget, or either topic was recreated under the request twice in a row (`topic changed since it was read`). From v3.2.0, with `remote`: a topic named `child` exists, the parent has 16 remote children, this cluster already links to that remote topic, the parent's retention is below 24 hours, or a check found the target unusable (the body names the `class` and carries each member's report in `checks`). |
| [`412`](status-codes.md#status-412) | New in v3.2.0, with `remote`. A member does not apply the remote Raft entry types, did not answer, or reports a posture that forbids remotes (security off or legacy cluster auth on), and the body names it in `members`; a member holds a stale or unreadable credential; the members disagree about the target's ID; or the leader runs an older release. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | New in v3.2.0, with `remote`. This node took 60 remote child writes in the last minute, or a check of this remote ran less than 5 seconds ago on a member; retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | New in v3.2.0, with `remote`. The answering node, or the leader, has no remote plane. |
| [`502`](status-codes.md#status-502) | New in v3.2.0, with `remote`. Something in front of the target answered instead of it (a load balancer or a proxy), or the target answered with a redirect, which is never followed. |
| [`503`](status-codes.md#status-503) | No leader, the answering node could not reach the leader to confirm a topic it does not have, or the parent's partition owners could not be asked for the attach point. Nothing was linked; retry. From v3.2.0, with `remote`, also when the target or a member was unavailable during the checks. With `remote`, when the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. For an attach, the body also carries the leader's `warnings`, which no read shows again. |

**Response body (`200`)**: a [Topic](#topic-object).

**Response body (`201`)**: a [Topic](#topic-object).

```sh title="Request: a local child"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/children" \
  -H "Content-Type: application/json" \
  -d '{"child": "orders-audit"}'
```

```http title="Response: a local child"
HTTP/1.1 200 OK
Content-Length: 268
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "name": "orders",
  "id": "681a429c0b683b2d",
  "partitions": 3,
  "retention_ms": 172800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790623903,
  "owner": "admin",
  "role": "parent",
  "children": ["orders-audit"]
}
```

```sh title="Request: a remote child, checks only (v3.2.0)"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/children" \
  -H "Content-Type: application/json" \
  -d '{"child": "orders-to-b", "remote": "b", "remote_topic": "orders",
       "from": "unconsumed", "dry_run": true}'
```

```http title="Response: a remote child, checks only (v3.2.0)"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:06:41 GMT
Content-Length: 435

{
  "attach_offsets": [0, 0, 0],
  "capabilities": {"max_messages": 1000, "zstd": true},
  "checks": [
    {
      "node": "narad-0",
      "result": "pass",
      "credential_version": 1,
      "target_id": "128e63dd156ff568",
      "target_serves_ids": true,
      "rtt_ms": 0,
      "lane_capacity_per_s": 20000,
      "server_cert_not_after": "2026-11-05T13:04:25Z",
      "warnings": [],
      "posture": {
        "security_enabled": true,
        "legacy_cluster_auth": false,
        "raft_tls": true,
        "api_hop_encrypted": true
      }
    }
  ],
  "dry_run": true,
  "warnings": []
}
```

```sh title="Request: a remote child (v3.2.0)"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/children" \
  -H "Content-Type: application/json" \
  -d '{"child": "orders-to-b", "remote": "b", "remote_topic": "orders",
       "from": "unconsumed"}'
```

```http title="Response: a remote child (v3.2.0)"
HTTP/1.1 201 Created
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:06:59 GMT
Content-Length: 408

{
  "name": "orders-to-b",
  "id": "d38a4383503cf4ac",
  "partitions": 0,
  "retention_ms": 0,
  "visibility_timeout_ms": 0,
  "max_in_flight_per_partition": 0,
  "max_acked_ahead_per_partition": 0,
  "created_at": 1791292019,
  "role": "child",
  "parent": "orders",
  "attach_epoch": "341e6b8a37a9688a",
  "attach_offsets": [0, 0, 0],
  "remote": {
    "name": "b",
    "topic": "orders",
    "target_id": "128e63dd156ff568",
    "from": "unconsumed",
    "lanes": 1,
    "created_by": "admin"
  }
}
```

### List a parent's children {#list-children}

`GET /v1/topics/{parent}/children`

Lists the parent's children with their delay and how many messages
each is behind.

**New in v3.2.0:** a [remote child](glossary.md#remote-child) also
carries its link: the remote and the topic there, its state (the
worst of its partitions'), its recovery point (`lag_seconds`), the
retention left before drop-behind, the record a cursor is stuck on,
and when the target was last verified. The listing names the
remote, never its URL or credential, and a failure as a state,
never text from the target. The answer also carries `parent_id`,
which another cluster's remote child reads to notice a recreated
target. The states are listed in
[Remotes and remote children](remote-children.md#link-states).

**Grant needed:** Any grant that matches the parent's name, ownership, or `admin`.

**Parameters**

| Name | Description |
|---|---|
| `parent`<br>path, string, required | Name of the parent topic. |
| `partitions` (v3.2.0)<br>query, boolean, optional, default `False` | `true` adds one row per parent partition to each remote child (`partitions`). |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The children. |
| [`400`](status-codes.md#status-400) | New in v3.2.0. `partitions` is not `true` or `false`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No grant on the parent. |
| [`404`](status-codes.md#status-404) | The parent does not exist. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `parent`<br>string | Parent topic name. |
| `parent_id` (v3.2.0)<br>string | The parent's incarnation ID, empty for a topic created before topic IDs (v2.1 and earlier). A remote child on another cluster that sends to this topic reads it to notice a recreate. |
| `children`<br>array of object |  |
| `children[].name`<br>string | Child topic name. |
| `children[].delay_ms`<br>integer | The child's delay, `0` for an immediate child. |
| `children[].lag_messages`<br>integer | Parent messages not yet copied into the child, summed over partitions. For a remote child, not yet accepted by the remote. |
| `children[].lag_complete`<br>boolean | `false` while some partitions have not reported, so `lag_messages` is a lower bound. |
| `children[].remote` (v3.2.0)<br>object | A [remote child](glossary.md#remote-child)'s link. Present only on a remote child's stub, which has `partitions` `0` and no owner. |
| `children[].remote.name`<br>string | The remote the copies go to. |
| `children[].remote.topic`<br>string | The topic on the remote. |
| `children[].remote.target_id`<br>string | The target topic's ID as the attach, or the last resume with `accept_target`, saw it. A target on v3.1.0 serves it in its describe answer, so recreate detection works there too. Empty for a target topic created before topic IDs (v2.1 and earlier); such a link stops in `target_replaced` if the target later reports an ID, because a topic gains one only by being recreated. |
| `children[].remote.from`<br>string: `attach`, `unconsumed`, `earliest` | Where the link started on each parent partition. |
| `children[].remote.lanes`<br>integer | Ordered streams per parent partition, 1 to 8. |
| `children[].remote.paused`<br>boolean | `true` while paused. Absent otherwise. |
| `children[].remote.pause_reason`<br>string | The reason given to pause. |
| `children[].remote.paused_by`<br>string | The admin who paused it; shown to admins only. |
| `children[].remote.paused_at_ms`<br>integer | When it was paused, Unix milliseconds. |
| `children[].remote.skip`<br>object | Per parent partition, the offsets an admin accepted to lose, ascending, at most 4000. A cursor drops a record only while it is stuck on exactly one of them. |
| `children[].remote.created_by`<br>string | The admin who attached it; shown to admins only. |
| `children[].paused` (v3.2.0)<br>boolean | A remote child only. `true` while it is paused. |
| `children[].state` (v3.2.0)<br>string | A remote child only. Its worst partition's [link state](remote-children.md#link-states), `running` or `paused` when healthy; `unknown` when a partition owner did not report, or when its owner is still stuck on a record this node shows as skipped. `running` and `paused` follow the pause flag as the node answering has applied it, so a read straight after a pause or resume agrees with it even while an owner has not caught up. |
| `children[].lag_seconds` (v3.2.0)<br>number | A remote child only. Age, on the owners' clocks, of the oldest parent record not yet accepted by the remote, the worst partition's: the link's live recovery point. |
| `children[].retention_headroom_seconds` (v3.2.0)<br>number | A remote child only. The parent's retention minus `lag_seconds`, the time left before drop-behind. Absent when the parent keeps messages forever. |
| `children[].source_drained` (v3.2.0)<br>boolean | A remote child only. `true` once the parent's consumers have acked past the link's start offset on every partition. |
| `children[].blocked_at` (v3.2.0)<br>object | A remote child only. The one record a cursor is stuck on, as `partition`, `offset` and `state` (`rejected_record` or `record_too_large`); `null` when none is. |
| `children[].target_verified_at` (v3.2.0)<br>string | A remote child only. The oldest of the cursors' last successful target checks, RFC 3339; `null` while any cursor has had none (a node whose checks keep failing is not hidden behind another node's success). |
| `children[].unverified` (v3.2.0)<br>boolean | A remote child only. `true` for a running link with a cursor whose node has had no successful target check in the last 10 minutes (counted from when the node began checking, for a cursor that never had one). |
| `children[].last_success_at` (v3.2.0)<br>string | A remote child only. When the remote last accepted a chunk, RFC 3339. |
| `children[].partitions` (v3.2.0)<br>array of object | A remote child only, with `partitions=true`. One row per parent partition. |
| `children[].partitions[].partition`<br>integer | Parent partition. |
| `children[].partitions[].node`<br>string | The owner that reported the cursor. |
| `children[].partitions[].start_offset`<br>integer | Where the link started on this partition. |
| `children[].partitions[].next_offset`<br>integer | The cursor's next offset; `null` when the owner did not report. |
| `children[].partitions[].high_watermark`<br>integer | The partition's high watermark; `null` when the owner did not report. |
| `children[].partitions[].ack_frontier`<br>integer | The parent's consumer ack frontier on this partition; `null` when unknown. |
| `children[].partitions[].state`<br>string | This partition's link state. |
| `children[].partitions[].last_success_at`<br>string | When the remote last accepted a chunk from this partition. |
| `children[].partitions[].blocked_at`<br>object | The record this partition's cursor is stuck on, if any. |

```sh title="Request: local children"
curl -i -u "$AUTH" "$NARAD/v1/topics/orders/children"
```

```http title="Response: local children"
HTTP/1.1 200 OK
Content-Length: 108
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "parent": "orders",
  "children": [
    {
      "name": "orders-audit",
      "delay_ms": 0,
      "lag_messages": 0,
      "lag_complete": false
    }
  ]
}
```

```sh title="Request: a remote child (v3.2.0)"
curl -i -u "$AUTH" "$NARAD/v1/topics/orders/children"
```

```http title="Response: a remote child (v3.2.0)"
HTTP/1.1 200 OK
Content-Length: 469
Content-Type: application/json
Date: Tue, 06 Oct 2026 13:07:03 GMT

{
  "parent": "orders",
  "parent_id": "4be76b543f9c4091",
  "children": [
    {
      "name": "orders-to-b",
      "delay_ms": 0,
      "lag_messages": 0,
      "lag_complete": true,
      "remote": {
        "name": "b",
        "topic": "orders",
        "target_id": "128e63dd156ff568",
        "from": "unconsumed",
        "lanes": 1,
        "created_by": "admin"
      },
      "paused": false,
      "state": "running",
      "lag_seconds": 0,
      "retention_headroom_seconds": 259200,
      "source_drained": false,
      "blocked_at": null,
      "target_verified_at": "2026-10-06T13:07:00Z",
      "last_success_at": "2026-10-06T13:07:00Z"
    }
  ]
}
```

### Detach a child {#detach-child}

`DELETE /v1/topics/{parent}/children/{child}`

Removes the link. The child keeps the messages and the schema
history it already has and becomes a standalone topic again.

**New in v3.2.0:** detaching a [remote child](glossary.md#remote-child)
deletes its stub. It travels to the leader, which first checks that
every record of the parent is on the remote: no cursor lag, every
partition owner reporting, and no member holding records of the
parent it answered `202` for and has not committed yet. While any
is left it answers `409` with the counts, unless `force=true`
abandons them; the leader's audit line records what a forced delete
abandoned. The check is a point in time, so stop the producers
first. A new check for one parent runs at most every 10 seconds.

**Grant needed:** Ownership of either topic, or `admin`. New in v3.2.0: for a remote child, the owner of the parent, or an `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `parent`<br>path, string, required | Name of the parent topic. |
| `child`<br>path, string, required | Name of the child topic. |
| `force` (v3.2.0)<br>query, boolean, optional, default `False` | `true` deletes a remote child even while records of its parent are unshipped, abandoning them. Ignored for a local child. |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | Detached. For a remote child (from v3.2.0), its stub is deleted. |
| [`400`](status-codes.md#status-400) | New in v3.2.0. `force` is not `true` or `false`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | The caller manages neither topic, as the node that answers or the cluster leader sees them. For a remote child (from v3.2.0), the caller is neither an `admin` nor the parent's owner, or security is off. |
| [`404`](status-codes.md#status-404) | Neither topic exists after the answering node caught up with the leader, or the two are not linked. |
| [`409`](status-codes.md#status-409) | New in v3.1.0. Either topic was deleted and recreated under the request twice in a row (`topic changed since it was read`); nothing was changed. From v3.2.0, for a remote child without `force`: records of the parent are not yet on the remote, with `lag_messages`, `lag_complete` and `dispatch_backlog` (records each member holds, by node) in the body, and `not_answering` or `backlog_over_scan_limit` naming members that could not be counted; or the child turned out to be remote, or local, on the leader; retry. |
| [`412`](status-codes.md#status-412) | New in v3.2.0. The cluster leader runs a release without remote children; finish the upgrade. |
| [`429`](status-codes.md#status-429) | New in v3.2.0. The leader ran an unshipped check for this parent less than 10 seconds ago; retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | New in v3.2.0. The answering node has no remote plane, so it cannot detach a remote child. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change, or the answering node could not reach the leader to confirm the topics. From v3.2.0, for a remote child, also when the unshipped check could not run. For a remote child, when the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

```sh title="Request: a local child"
curl -i -u "$AUTH" -X DELETE \
  "$NARAD/v1/topics/orders/children/orders-audit"
```

```http title="Response: a local child"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:43 GMT
```

```sh title="Request: a remote child with records not yet on its remote (v3.2.0)"
curl -i -u "$AUTH" -X DELETE \
  "$NARAD/v1/topics/orders/children/orders-to-b"
```

```http title="Response: a remote child with records not yet on its remote (v3.2.0)"
HTTP/1.1 409 Conflict
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:18 GMT
Content-Length: 122

{
  "dispatch_backlog": {},
  "error": "remote child \"orders-to-b\" has unshipped records",
  "lag_complete": true,
  "lag_messages": 4
}
```

### Pause a remote child {#pause-remote-child}

**New in v3.2.0.**

`POST /v1/topics/{parent}/children/{child}/pause`

Stops a [remote child](glossary.md#remote-child) sending. Its
cursors keep their positions and the parent's retention clock keeps
running, so a pause longer than the retention headroom loses records
to drop-behind. The leader checks only that every member applies
the remote Raft entry types, from the member records, so a pause
works while a member is down. Pausing a paused child records the new
reason.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `parent`<br>path, string, required | Name of the parent topic. |
| `child`<br>path, string, required | Name of the child topic. |

**Request body**

| Field | Description |
|---|---|
| `reason`<br>string, optional | Why, shown in the listing and the audit line. At most 256 bytes of printable text. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Paused. The body is the stub, with `remote.paused`, `pause_reason`, `paused_by` and `paused_at_ms`. |
| [`400`](status-codes.md#status-400) | A name is not a topic name, or `reason` is over 256 bytes or holds a control character. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote child of that name under that parent. |
| [`409`](status-codes.md#status-409) | The child was detached and attached again under the request; read it and retry. |
| [`412`](status-codes.md#status-412) | A member does not apply the remote Raft entry types (the body names it), or the leader runs an older release. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | This node took 60 remote child writes in the last minute; retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The leader could not be reached or could not write the change; retry. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

**Response body (`200`)**: a [Topic](#topic-object).

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/children/orders-to-b/pause" \
  -H "Content-Type: application/json" \
  -d '{"reason": "target maintenance, CHG-4211"}'
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:15 GMT
Content-Length: 517

{
  "name": "orders-to-b",
  "id": "d38a4383503cf4ac",
  "partitions": 0,
  "retention_ms": 0,
  "visibility_timeout_ms": 0,
  "max_in_flight_per_partition": 0,
  "max_acked_ahead_per_partition": 0,
  "created_at": 1791292019,
  "role": "child",
  "parent": "orders",
  "attach_epoch": "341e6b8a37a9688a",
  "attach_offsets": [0, 0, 0],
  "remote": {
    "name": "b",
    "topic": "orders",
    "target_id": "128e63dd156ff568",
    "from": "unconsumed",
    "lanes": 1,
    "paused": true,
    "pause_reason": "target maintenance, CHG-4211",
    "paused_by": "admin",
    "paused_at_ms": 1791292035849,
    "created_by": "admin"
  }
}
```

### Resume a remote child {#resume-remote-child}

**New in v3.2.0.**

`POST /v1/topics/{parent}/children/{child}/resume`

Runs the attach checks from every member again, then lets a paused
[remote child](glossary.md#remote-child) send. A target topic that
was deleted and recreated since the attach (its ID changed) is
refused with `409` (`target_replaced`) before anything is sent to
it; `accept_target` records the new ID and resumes. Resuming a
running child re-runs the checks and changes nothing else. Unlike
pause, resume needs every member to answer.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `parent`<br>path, string, required | Name of the parent topic. |
| `child`<br>path, string, required | Name of the child topic. |

**Request body**

| Field | Description |
|---|---|
| `accept_target`<br>boolean, optional, default `False` | Accept a target topic that was deleted and recreated since the attach (state `target_replaced`), and send to it. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Resumed. The body is the stub. |
| [`400`](status-codes.md#status-400) | A name is not a topic name, or the body is malformed. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote child of that name under that parent. |
| [`409`](status-codes.md#status-409) | The remote no longer exists (create it again first), the target topic was replaced (resume with `accept_target`), a check found the target unusable (the body names the `class` and carries each member's report), or the child was detached and attached again under the request. |
| [`412`](status-codes.md#status-412) | A member does not apply the remote Raft entry types, did not answer, or reports a posture that forbids remotes (the body names it); a member holds a stale or unreadable credential; or the leader runs an older release. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | This node took 60 remote child writes in the last minute, or a check of this remote ran less than 5 seconds ago on a member; retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`502`](status-codes.md#status-502) | Something in front of the target answered instead of it, or the target answered with a redirect. |
| [`503`](status-codes.md#status-503) | The leader could not be reached, or the target or a member was unavailable during the checks; retry. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

**Response body (`200`)**: a [Topic](#topic-object).

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/children/orders-to-b/resume" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:33 GMT
Content-Length: 423

{
  "name": "orders-to-b",
  "id": "d38a4383503cf4ac",
  "partitions": 0,
  "retention_ms": 0,
  "visibility_timeout_ms": 0,
  "max_in_flight_per_partition": 0,
  "max_acked_ahead_per_partition": 0,
  "created_at": 1791292019,
  "role": "child",
  "parent": "orders",
  "attach_epoch": "341e6b8a37a9688a",
  "attach_offsets": [0, 0, 0],
  "remote": {
    "name": "b",
    "topic": "orders",
    "target_id": "128e63dd156ff568",
    "from": "unconsumed",
    "lanes": 1,
    "skip": {"0": [2]},
    "created_by": "admin"
  }
}
```

### Skip a record a remote child cannot ship {#skip-remote-child-record}

**New in v3.2.0.**

`POST /v1/topics/{parent}/children/{child}/skip`

Records that one parent record may be dropped from a
[remote child](glossary.md#remote-child). The leader asks the
partition's owner first and accepts the skip only while the
link's cursor is stuck on exactly that partition and offset in
`rejected_record` or `record_too_large` (the listing's
`blocked_at`); any other record is refused with `409`, so a
mistyped partition or offset is never stored. With several lanes
stuck, the owner reports the lowest record: skip them in order.
The record stays in the parent's log for its retention. Each
dropped record counts on
`narad_fanout_remote_skipped_records_total` and is logged by the
node that drops it. The child's `remote.skip` keeps skipped
offsets per partition, ascending, at most 4000 of them (one
full slab), always including the one just skipped, so a slab read again (after a
restart or a partition move) drops each of them again.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `parent`<br>path, string, required | Name of the parent topic. |
| `child`<br>path, string, required | Name of the child topic. |

**Request body**

| Field | Description |
|---|---|
| `partition`<br>integer, required | The parent partition of the stuck record. |
| `offset`<br>integer, required | Its offset, as `blocked_at` shows it. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Recorded. The body is the stub, with `remote.skip`. |
| [`400`](status-codes.md#status-400) | `partition` or `offset` is missing or negative, `partition` is not a partition of the parent, or a name is not a topic name. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote child of that name under that parent, or the parent is gone. |
| [`409`](status-codes.md#status-409) | The link's cursor of that partition is not stuck on that offset in `rejected_record` or `record_too_large` (the body names the record it is stuck on, as `blocked_at`, if any), or the child was detached and attached again under the request; read it and retry. |
| [`412`](status-codes.md#status-412) | A member does not apply the remote Raft entry types (the body names it), or the leader runs an older release. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | This node took 60 remote child writes in the last minute; retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The leader could not be reached, could not ask the partition's owner, or could not write the change; retry. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

**Response body (`200`)**: a [Topic](#topic-object).

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/children/orders-to-b/skip" \
  -H "Content-Type: application/json" \
  -d '{"partition": 0, "offset": 2}'
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:33 GMT
Content-Length: 532

{
  "name": "orders-to-b",
  "id": "d38a4383503cf4ac",
  "partitions": 0,
  "retention_ms": 0,
  "visibility_timeout_ms": 0,
  "max_in_flight_per_partition": 0,
  "max_acked_ahead_per_partition": 0,
  "created_at": 1791292019,
  "role": "child",
  "parent": "orders",
  "attach_epoch": "341e6b8a37a9688a",
  "attach_offsets": [0, 0, 0],
  "remote": {
    "name": "b",
    "topic": "orders",
    "target_id": "128e63dd156ff568",
    "from": "unconsumed",
    "lanes": 1,
    "paused": true,
    "pause_reason": "target maintenance, CHG-4211",
    "paused_by": "admin",
    "paused_at_ms": 1791292035849,
    "skip": {"0": [2]},
    "created_by": "admin"
  }
}
```

## Messages {#messages}

Produce, consume and settle messages. Task guides:
[Produce messages](../build/producing.md),
[Consume and acknowledge](../build/consuming.md) and
[Replay messages](../build/replay.md).

### Produce a message {#produce}

`POST /v1/topics/{topic}/produce`

Stores the request body as one message. A
[`202`](status-codes.md#status-202) means the node that answered has
written the message to its [ingress WAL](glossary.md#ingress-wal)
and synced it to disk; it then moves the message to the partition's
owner in the background. What a `202` promises is in the
[delivery contract](../understand/delivery-contract.md#what-202-means).

The body is stored byte for byte. Send JSON with
`Content-Type: application/json` and anything else with
`Content-Type: application/octet-stream`. On a topic with a schema,
the body must be one JSON text that the current schema accepts
([Schema validation rules](schema-rules.md#validation)).

**Grant needed:** `produce` on the topic.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |
| `key`<br>query, string, optional | Routes the message: messages with the same key go to the same partition while the partition count is unchanged and its owner is up. This is not an ordering guarantee. Without a key, messages are spread round-robin. |
| `partition`<br>query, integer, optional | Pins the message to this partition. Wins over `key`. |

**Request body**

The message, 1 byte to 1 MiB. Content types: `application/json`, `application/octet-stream`.

**Responses**

| Status | Meaning |
|---|---|
| [`202`](status-codes.md#status-202) | Accepted and synced to disk. The body is empty. |
| [`400`](status-codes.md#status-400) | Empty body, `partition` out of range, `key` or `partition` given twice, or the body fails the topic's schema (from v3.1.0, that includes a body nested deeper than 256 levels). |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No `produce` grant on the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist. |
| [`409`](status-codes.md#status-409) | The topic is a delay child, which only its parent can feed. New in v3.2.0: the topic is a remote child's stub, whose messages live on its remote (`remote child "<name>" lives on remote <remote>; consume it there`). |
| [`413`](status-codes.md#status-413) | The body is over 1 MiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | Too many produces in flight for this user on this node, only when the operator set a produce cap ([Configuration reference](configuration.md#http)). |
| [`500`](status-codes.md#status-500) | The node could not write to its ingress WAL. It answers every produce this way until it restarts. |
| [`503`](status-codes.md#status-503) | New in v3.1.0. Nothing was checked or stored; retry through another node. Either this node is being decommissioned and takes no new produce (with `Retry-After: 1`), or the topic has a schema and every schema validation slot on the node stayed busy for 5 seconds. |

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/produce?key=customer-42" \
  -H "Content-Type: application/json" \
  -d '{"order_id": "ord_123", "amount": 1250}'
```

```http title="Response"
HTTP/1.1 202 Accepted
Date: Mon, 28 Sep 2026 19:31:43 GMT
Content-Length: 0
```

### Produce a batch {#produce-batch}

**New in v3.1.0.**

`POST /v1/topics/{topic}/produce/batch`

Stores 1 to 1,000 messages in one request, all or none (1 to 100
in v3.1.0). Every message
is checked as a single produce would check it before any is stored.
If one fails, the request gets the status a single produce of that
message would get, its error starts with `message <index>: `, and
nothing is stored. The `202` comes once every message is synced to
the ingress WAL, and carries the same promise as a single `202` for
each of them.

`key` and `partition` belong to each message; as query parameters
they get `400`. A node on v3.0.1 or earlier answers `404`: fall back
to single produces.

**New in v3.2.0:** the body may be up to 16 MiB, with each message's
decoded payload at most 1 MiB, the single-produce cap, so anything a
single produce accepts fits in a batch. It may be sent compressed,
`Content-Encoding: zstd` or `gzip`, decoded under the same cap. A
body over 1 MiB first takes its share of the node's batch body
budget (`http.max_batch_body_bytes_in_flight`, 256 MiB by default)
and is answered `503` with `Retry-After: 1` when the budget is
full; bodies of 1 MiB or less never touch it. v3.1.0 answers a
batch over 100 messages `400` and a body over 1 MiB `413`, and does
not decode a compressed body. This is the route a
[remote child](glossary.md#remote-child) sends to.

**Grant needed:** `produce` on the topic.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |

**Request body**

At most 16 MiB in total, each payload at most 1 MiB (from v3.2.0; 1 MiB in total in v3.1.0), optionally zstd or gzip compressed.

| Field | Description |
|---|---|
| `messages`<br>array of object, required | 1 to 1,000 messages (from v3.2.0; 1 to 100 in v3.1.0), stored in this order. |
| `messages[].payload`<br>JSON, required | A JSON value, stored exactly as written (a JSON string keeps its quotes). With `payload_encoding`, a base64 string of any bytes. |
| `messages[].payload_encoding`<br>string: `base64`, optional | Set to `base64` for a payload that is not JSON. |
| `messages[].key`<br>string, optional | The message's key. Absent or empty means no key. |
| `messages[].key_encoding`<br>string: `base64`, optional | Set to `base64` for a key that is not valid UTF-8. |
| `messages[].partition`<br>integer, optional | Pin the message to this partition. |

**Responses**

| Status | Meaning |
|---|---|
| [`202`](status-codes.md#status-202) | Every message accepted and synced to disk. |
| [`400`](status-codes.md#status-400) | No messages, more than 1,000 (more than 100 in v3.1.0), a bad encoding, an empty payload, a message the schema refuses (one nested deeper than 256 levels included), `key` or `partition` in the query, or (from v3.2.0) a compressed body that does not decode. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No `produce` grant on the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist, or the node runs v3.0.1 or earlier. |
| [`409`](status-codes.md#status-409) | The topic is a delay child. New in v3.2.0: the topic is a remote child's stub, whose messages live on its remote (`remote child "<name>" lives on remote <remote>; consume it there`). |
| [`413`](status-codes.md#status-413) | The body is over 16 MiB, decoded or as sent (1 MiB in v3.1.0), or (from v3.2.0) one message's payload is over 1 MiB (`message <i>: message too large`). |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header, or (from v3.2.0) a `Content-Encoding` other than `zstd`, `gzip` or none. |
| [`429`](status-codes.md#status-429) | The batch does not fit this user's produce cap on this node (only when the cap is set). A batch counts as its message count, clamped to the cap. |
| [`500`](status-codes.md#status-500) | The node could not write to its ingress WAL. |
| [`503`](status-codes.md#status-503) | Nothing was checked or stored; retry through another node. Either this node is being decommissioned and takes no new produce (with `Retry-After: 1`), or the topic has a schema and every schema validation slot on the node stayed busy for 5 seconds, or (from v3.2.0) the body is over 1 MiB and the node's batch body budget is full (with `Retry-After: 1`). |

**Response body (`202`)**

| Field | Description |
|---|---|
| `accepted`<br>integer | How many messages were stored. |

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/produce/batch" \
  -H "Content-Type: application/json" \
  -d '{"messages": [
        {"key": "customer-42", "payload": {"order_id": "ord_124"}},
        {"key": "customer-7", "payload": "aGVsbG8=",
         "payload_encoding": "base64"}
      ]}'
```

```http title="Response"
HTTP/1.1 202 Accepted
Content-Length: 15
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{"accepted":2}
```

### Consume messages {#consume}

`GET /v1/topics/{topic}/consume`

Without `offset`, takes the next available message and gives the
caller a [lease](glossary.md#lease) on it for the topic's
`visibility_timeout_ms`. Settle it with [ack](#ack) before the lease
runs out, or the message is delivered again. The request can reach
any node; the node gathers a message from whichever partition owner
has one.

With `partition` and `offset`, reads the record at that offset
without taking a lease: a [replay](../build/replay.md). The answer
has no `receipt_handle` and nothing needs acking.

**Grant needed:** `consume` on the topic.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |
| `wait`<br>query, string, optional, default `0s` | How long to wait for a message before answering `204`, as a Go duration such as `500ms` or `10s`. Without it the answer is immediate. Values above the server's maximum (10 s by default, [Configuration reference](configuration.md#http)) are cut to it, and the response then carries `X-Narad-Wait-Clamped` with the value used. |
| `partition`<br>query, integer, optional | Take messages from this partition only. Required with `offset`. |
| `offset`<br>query, integer, optional | Replay the record at this offset of `partition`. An offset past the end of the partition answers `204`; one that aged out of retention answers `410`. |
| `max` (v3.1.0)<br>query, integer, optional | Take up to this many messages in one answer, `{"messages": [...]}`, each with its own lease. The request does not wait to fill `max`. Requires an `X-Narad-Client` header ([Required headers](#required-headers)). It counts as `max`, clamped to the cap, against the per-user consume cap. Cannot be combined with `offset`. A v3.0.1 node ignores it and answers with one message in the single-message shape. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | One message, or `{"messages": [...]}` with `max`. |
| [`204`](status-codes.md#status-204) | No message arrived within `wait`, or `offset` is past the end of the partition. |
| [`400`](status-codes.md#status-400) | A parameter is invalid, `partition` is out of range, `offset` came without `partition`, `max` came with `offset`, or `max` came without an `X-Narad-Client` header. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No `consume` grant on the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist. |
| [`409`](status-codes.md#status-409) | New in v3.2.0. The topic is a remote child's stub, whose messages live on its remote (`remote child "<name>" lives on remote <remote>; consume it there`). |
| [`410`](status-codes.md#status-410) | Replay only. The record at `offset` aged out of retention or cannot be read. |
| [`421`](status-codes.md#status-421) | The partition moved to another node while the request was served. Retry. |
| [`429`](status-codes.md#status-429) | Too many consumes in flight for this user on this node. |
| [`500`](status-codes.md#status-500) | The node could not read the partition, for example after a disk error. |
| [`502`](status-codes.md#status-502) | With `partition`, the node forwarded the request to the partition's owner and got no answer. |
| [`503`](status-codes.md#status-503) | With `partition`, the partition's owner is down. Retry after `Retry-After`. Releases up to v3.2.1 sent no `Retry-After`. |

**Response body (`200`)**: a [Message](#message-object).

```sh title="Request: one message"
curl -i -u "$AUTH" "$NARAD/v1/topics/orders/consume?wait=5s"
```

```http title="Response: one message"
HTTP/1.1 200 OK
Content-Length: 179
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "topic": "orders",
  "partition": 1,
  "offset": 0,
  "key": "customer-42",
  "payload": {"order_id": "ord_123", "amount": 1250},
  "timestamp": 1790623903,
  "receipt_handle": "1:0:181499699661178901"
}
```

```sh title="Request: a batch (v3.1.0)"
curl -i -u "$AUTH" -H 'X-Narad-Client: curl' \
  "$NARAD/v1/topics/orders/consume?max=10&wait=5s"
```

```http title="Response: a batch (v3.1.0)"
HTTP/1.1 200 OK
Content-Length: 326
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "messages": [
    {
      "topic": "orders",
      "partition": 1,
      "offset": 1,
      "key": "customer-42",
      "payload": {"order_id": "ord_124"},
      "timestamp": 1790623903,
      "receipt_handle": "1:1:6130356697557286029"
    },
    {
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "key": "customer-7",
      "payload": "hello",
      "timestamp": 1790623903,
      "receipt_handle": "0:0:2681459915124828679"
    }
  ]
}
```

### Ack, extend or nack a message {#ack}

`POST /v1/topics/{topic}/ack`

Settles the lease a [receipt handle](glossary.md#receipt-handle)
names. By default it acks: the message is done and is not delivered
again. `extend=true` renews the lease for a full
`visibility_timeout_ms` from now, for a handler that needs longer.
`extend=0` is a [nack](glossary.md#nack): the lease ends now and the
message can be delivered again at once. A handle whose lease ran out,
or that was already settled, gets `410`.

Without a `receipt_handle` parameter, a JSON body
`{"receipt_handles": [...]}` settles 1 to 100 handles in one
request (**v3.1.0**). Each handle is settled on its own with the
mode `extend` selects, and the answer is `200` with one result per
handle, in request order. A v3.0.1 node answers such a request `400`.

The request is a `POST`, so it needs the `Content-Type` or
`X-Narad-Client` header even without a body.

**Grant needed:** `consume` on the topic.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |
| `receipt_handle`<br>query, string, optional | The `receipt_handle` from the consume answer. Required unless the body lists handles. |
| `extend`<br>query, string: `false`, `true`, `1`, `0`, optional | Leave it out (or `false`) to ack, `true` or `1` to extend the lease, `0` to nack. Any other value gets `400`. |

**Request body**

Only for a batch ack, at most 64 KiB.

| Field | Description |
|---|---|
| `receipt_handles` (v3.1.0)<br>array of string, required | 1 to 100 receipt handles of this topic. |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | Settled. |
| [`200`](status-codes.md#status-200) | A batch ack. One result per handle, each the status a single ack of that handle would have answered. |
| [`400`](status-codes.md#status-400) | No `receipt_handle`, a handle that cannot be decoded, a bad `extend`, or more than 100 handles. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | No `consume` grant on the topic. |
| [`404`](status-codes.md#status-404) | The topic does not exist. |
| [`409`](status-codes.md#status-409) | New in v3.2.0. The topic is a remote child's stub, whose messages live on its remote (`remote child "<name>" lives on remote <remote>; consume it there`). |
| [`410`](status-codes.md#status-410) | The lease ran out, or the message was already settled or delivered again under a new handle. A handle from another topic, or for a partition this topic does not have, also answers `410`. |
| [`413`](status-codes.md#status-413) | A batch body over 64 KiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`421`](status-codes.md#status-421) | The partition moved to another node while the request was served. Retry. |
| [`502`](status-codes.md#status-502) | The node forwarded the request to the partition's owner and got no answer in time, so the ack may have been applied. Retry; an ack that already landed answers `410` the second time. The plain-text body names neither the owner nor the transport error. |
| [`503`](status-codes.md#status-503) | The partition's owner is down, or the node could not get the forwarded request to the owner in time (no connection, or 2 seconds without a free slot to the owner). Nothing was applied. Retry after `Retry-After`; a `410` on the retry means the lease is gone. Releases up to v3.2.1 answered a forward that never left with `502`, and sent no `Retry-After`. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `results`<br>array of object | One result per handle, in request order. |
| `results[].status`<br>integer | The status a single ack of this handle would have answered. |
| `results[].error`<br>string | The error message, for a failed handle. |

```sh title="Request: one handle"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/ack?receipt_handle=1:0:181499699661178901" \
  -H "Content-Type: application/json"
```

```http title="Response: one handle"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:43 GMT
```

```sh title="Request: a batch (v3.1.0)"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/orders/ack" \
  -H "Content-Type: application/json" \
  -d '{"receipt_handles": [
        "1:1:6130356697557286029",
        "0:0:2681459915124828679",
        "0:0:1"
      ]}'
```

```http title="Response: a batch (v3.1.0)"
HTTP/1.1 200 OK
Content-Length: 124
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:43 GMT

{
  "results": [
    {"status": 204},
    {"status": 204},
    {
      "status": 410,
      "error": "receipt handle no longer matches an active reservation"
    }
  ]
}
```

## Users {#users}

Manage users and their grants. Every route needs the `admin` grant,
except a user changing their own password. Task guide:
[Manage users and grants](../operate/users.md).

### Create a user {#create-user}

`POST /v1/users`

Creates a user with a password and a list of
[grants](access-model.md#actions).

**Grant needed:** `admin`. Only the root admin can give the `admin` grant.

**Request body**

| Field | Description |
|---|---|
| `username`<br>string, required | 1 to 64 characters from `A-Z a-z 0-9 . _ -`, not `.` or `..`. |
| `password`<br>string, required | 1 to 72 bytes. A character outside ASCII counts as more than one byte. |
| `grants`<br>array of [Grant](#grant-object), optional | What the user may do. Leave it out for a user with no grants. |

**Responses**

| Status | Meaning |
|---|---|
| [`201`](status-codes.md#status-201) | Created. |
| [`400`](status-codes.md#status-400) | Invalid username, password or grant, or a body over 1 MiB. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`, or giving `admin` without being the root admin. |
| [`409`](status-codes.md#status-409) | The user exists. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the user. |

**Response body (`201`)**: a [User](#user-object).

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/users" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "billing-service",
    "password": "example-only-7Kq2",
    "grants": [
      {"action": "produce", "patterns": ["invoices.*"]},
      {"action": "consume", "patterns": ["payments.*"]}
    ]
  }'
```

```http title="Response"
HTTP/1.1 201 Created
Content-Length: 196
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

{
  "username": "billing-service",
  "grants": [
    {"action": "produce", "patterns": ["invoices.*"]},
    {"action": "consume", "patterns": ["payments.*"]}
  ],
  "created_at_ms": 1790623904012,
  "updated_at_ms": 1790623904012
}
```

### List users {#list-users}

`GET /v1/users`

Lists every user in name order. Password hashes are never returned.

**Grant needed:** `admin`.

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The users. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`. |

**Response body (`200`)**: an array of [User](#user-object).

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/users"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 291
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

[
  {
    "username": "admin",
    "root": true,
    "created_at_ms": 1790623900693,
    "updated_at_ms": 1790623900693
  },
  {
    "username": "billing-service",
    "grants": [
      {"action": "produce", "patterns": ["invoices.*"]},
      {"action": "consume", "patterns": ["payments.*"]}
    ],
    "created_at_ms": 1790623904012,
    "updated_at_ms": 1790623904012
  }
]
```

### Get a user {#get-user}

`GET /v1/users/{username}`

Returns one user and its grants.

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `username`<br>path, string, required | The username. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The user. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`. |
| [`404`](status-codes.md#status-404) | The user does not exist. |

**Response body (`200`)**: a [User](#user-object).

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/users/billing-service"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 196
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

{
  "username": "billing-service",
  "grants": [
    {"action": "produce", "patterns": ["invoices.*"]},
    {"action": "consume", "patterns": ["payments.*"]}
  ],
  "created_at_ms": 1790623904012,
  "updated_at_ms": 1790623904012
}
```

### Delete a user {#delete-user}

`DELETE /v1/users/{username}`

Deletes a user. The root admin and the caller's own account cannot be deleted.

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `username`<br>path, string, required | The username. |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | Deleted. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`, or the user is the root admin or the caller. |
| [`404`](status-codes.md#status-404) | The user does not exist. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the delete. |

```sh title="Request"
curl -i -u "$AUTH" -X DELETE "$NARAD/v1/users/billing-service"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:44 GMT
```

### Replace a user's grants {#update-grants}

`PUT /v1/users/{username}/grants`

Replaces all of the user's grants with the list sent. The password
is not touched. The root admin's grants cannot change, and no one
can change their own grants.

**Grant needed:** `admin`. Only the root admin can give the `admin` grant.

**Parameters**

| Name | Description |
|---|---|
| `username`<br>path, string, required | The username. |

**Request body**

| Field | Description |
|---|---|
| `grants`<br>array of [Grant](#grant-object), required | The complete new list; it replaces the old one. An empty list removes every grant. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Changed. The body is the user after the change. |
| [`400`](status-codes.md#status-400) | A grant is invalid, or the body is over 1 MiB. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`, the caller's own grants, the root admin's grants, or `admin` given by someone other than the root admin. |
| [`404`](status-codes.md#status-404) | The user does not exist. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change. |

**Response body (`200`)**: a [User](#user-object).

```sh title="Request"
curl -i -u "$AUTH" -X PUT "$NARAD/v1/users/billing-service/grants" \
  -H "Content-Type: application/json" \
  -d '{"grants": [{"action": "produce", "patterns": ["invoices.*"]}]}'
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 149
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

{
  "username": "billing-service",
  "grants": [{"action": "produce", "patterns": ["invoices.*"]}],
  "created_at_ms": 1790623904012,
  "updated_at_ms": 1790623904073
}
```

### Change a password {#update-password}

`PUT /v1/users/{username}/password`

Sets a new password. A user who is not `admin` can change their
own password by sending `current_password`. An `admin` can reset
any password without it, except the root admin's, which only the
root admin can change. Grants are not touched.

**Grant needed:** `admin`, or the user themself.

**Parameters**

| Name | Description |
|---|---|
| `username`<br>path, string, required | The username. |

**Request body**

| Field | Description |
|---|---|
| `new_password`<br>string, required | 1 to 72 bytes. |
| `current_password`<br>string, optional | The user's current password. Required when a user who is not `admin` changes their own. |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | Changed. |
| [`400`](status-codes.md#status-400) | `new_password` is empty or over 72 bytes, or the body is over 1 MiB. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin` and not the user, `current_password` is wrong, or someone other than the root admin changing the root admin's password. |
| [`404`](status-codes.md#status-404) | The user does not exist. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change. |

```sh title="Request"
curl -i -u "$AUTH" -X PUT \
  "$NARAD/v1/users/billing-service/password" \
  -H "Content-Type: application/json" \
  -d '{"new_password": "example-only-9Wd4"}'
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:44 GMT
```

## Cluster {#cluster}

See cluster members and partition moves, and drain a node before
removing it. Every route needs the `admin` grant, reads included.
Task guide: [Scale out and in](../operate/scaling.md).

### List cluster members {#list-members}

`GET /v1/cluster/members`

Lists every node the cluster knows, alive or dead, with how many
partitions it owns and how many are moving off it, whether it is a
Raft voter or the leader, how long ago its last heartbeat was
stamped, and why a decommission in progress is blocked. Answered
from the receiving node's replica without asking any other node,
unless `detail` is set.

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `detail` (v3.1.0)<br>query, boolean, optional, default `False` | `true` also asks every member for its own status (dispatch backlog, quarantined copies, move workers), all at once and within 2 seconds in total. A member that cannot answer gets a `status_error` instead; a v3.0.1 node is reported as an older release that cannot report its status. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The members, in ID order. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`400`](status-codes.md#status-400) | `detail` is not `true` or `false`. |
| [`403`](status-codes.md#status-403) | Not `admin`. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `members`<br>array of object |  |
| `members[].id`<br>string | Node ID. |
| `members[].addr`<br>string | The node's API address. |
| `members[].status`<br>string: `alive`, `dead` | `dead` once the node stopped sending heartbeats. |
| `members[].draining`<br>boolean | `true` while the node is being decommissioned. |
| `members[].owned_partitions`<br>integer | Partitions the node owns. |
| `members[].outbound_moves`<br>integer | Partitions moving off the node. |
| `members[].voter` (v3.1.0)<br>boolean | `true` when the node is a Raft voter. |
| `members[].leader` (v3.1.0)<br>boolean | `true` for the Raft leader. |
| `members[].heartbeat_age_seconds` (v3.1.0)<br>integer | Seconds since the leader last stamped the node's heartbeat, by the answering node's clock. |
| `members[].decommission_blocked` (v3.1.0)<br>array of object | For a draining node, every reason its decommission cannot progress that the cluster metadata shows. Absent when none. The leader also logs each reason and exports `narad_decommission_blocked`. |
| `members[].decommission_blocked[].code`<br>string: `below_min_voters`, `no_healthy_majority`, `move_target`, `dispatch_backlog`, `node_status_unavailable`, `no_receivers`, `owner_dead`, `move_budget_full`, `leader_transfer` | The reason, as `narad_decommission_blocked` labels it ([Troubleshooting](../operate/troubleshooting.md#decommission-blocked)). |
| `members[].decommission_blocked[].message`<br>string | What it means here and what to do. |
| `members[].node_status` (v3.1.0)<br>object | With `detail=true`, the node's own report about itself. |
| `members[].node_status.node`<br>string | Node ID. |
| `members[].node_status.draining`<br>boolean | The node's own view of its draining mark. |
| `members[].node_status.produce_in_flight`<br>integer | Client produce requests the node admitted and has not answered yet. Once it is draining it admits none, and a decommission waits for 0. |
| `members[].node_status.dispatch_backlog`<br>integer | Messages its ingress WAL accepted and has not yet handed to their partition owners. A decommission waits for 0. |
| `members[].node_status.quarantine`<br>object | Partition copies the node set aside instead of deleting ([Troubleshooting](../operate/troubleshooting.md#quarantined-copies)). |
| `members[].node_status.quarantine.copies`<br>integer | Every set-aside copy. |
| `members[].node_status.quarantine.bytes`<br>integer | Their total size. |
| `members[].node_status.quarantine.list`<br>array of object | The first 100 copies. |
| `members[].node_status.quarantine.list[].kind`<br>string | `partition`, `topic_incarnation` or `staging`. |
| `members[].node_status.quarantine.list[].topic`<br>string | Topic name. |
| `members[].node_status.quarantine.list[].partition`<br>integer | Partition number, `-1` for a whole topic directory. |
| `members[].node_status.quarantine.list[].dir`<br>string | The copy's directory on the node. |
| `members[].node_status.quarantine.list[].bytes`<br>integer | Its size. |
| `members[].node_status.quarantine.list[].mod_time`<br>string | When it was last modified, RFC 3339. |
| `members[].node_status.moves`<br>array of object | The moves this node runs as the destination. |
| `members[].node_status.moves[].topic`<br>string | Topic name. |
| `members[].node_status.moves[].partition`<br>integer | Partition number. |
| `members[].node_status.moves[].source`<br>string | The node the copy comes from. |
| `members[].node_status.moves[].target`<br>string | This node. |
| `members[].node_status.moves[].started_at`<br>string | When the worker started, RFC 3339. |
| `members[].node_status.moves[].phase`<br>string: `copying`, `frozen`, `flip_pending`, `waiting_for_source`, `blocked` | What the worker is doing. |
| `members[].node_status.moves[].attempts`<br>integer | Copy attempts against a live source. |
| `members[].node_status.moves[].last_error`<br>string | The last thing that failed. |
| `members[].node_status.moves[].copied_bytes`<br>integer | Bytes the current copy fetched. |
| `members[].node_status.moves[].blocked`<br>string: `copy_unverifiable`, `source_dead_copy_behind` | Why the move cannot finish on its own. Absent while it can. |
| `members[].status_error` (v3.1.0)<br>string | With `detail=true`, why the node's own status could not be read. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/cluster/members"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 183
Content-Type: application/json
Date: Mon, 05 Oct 2026 19:14:29 GMT

{
  "members": [
    {
      "id": "narad-0",
      "addr": "127.0.0.1:17970",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0,
      "voter": true,
      "leader": true,
      "heartbeat_age_seconds": 2
    }
  ]
}
```

### List partition moves {#list-moves}

`GET /v1/cluster/moves`

Lists every partition that is moving between nodes right now, with
each side's liveness and why a move is blocked. Abort one with
`POST /v1/cluster/moves/{topic}/{partition}/abort`.

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `detail` (v3.1.0)<br>query, boolean, optional, default `False` | `true` also asks each move's destination for its move worker's own report: phase, copy attempts, copied bytes, last error and why it is blocked. Within 2 seconds in total. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The moves, by topic and partition. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`400`](status-codes.md#status-400) | `detail` is not `true` or `false`. |
| [`403`](status-codes.md#status-403) | Not `admin`. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `moves`<br>array of object |  |
| `moves[].topic`<br>string | Topic name. |
| `moves[].partition`<br>integer | Partition number. |
| `moves[].from`<br>string | Node that owns the partition now. |
| `moves[].to`<br>string | Node the partition is moving to. |
| `moves[].from_status` (v3.1.0)<br>string: `alive`, `dead`, `draining`, `not_a_member` | The owner's liveness. |
| `moves[].to_status` (v3.1.0)<br>string: `alive`, `dead`, `draining`, `not_a_member` | The destination's liveness. |
| `moves[].blocked` (v3.1.0)<br>string: `source_dead`, `target_dead`, `target_not_member` | Why the move cannot progress as things stand. Absent while it can. A dead source finishes only if the destination can force-promote a complete copy; the leader clears a move to a dead destination after two minutes. |
| `moves[].worker` (v3.1.0)<br>object | A destination's own report of one move. |
| `moves[].worker.topic`<br>string | Topic name. |
| `moves[].worker.partition`<br>integer | Partition number. |
| `moves[].worker.source`<br>string | The node the copy comes from. |
| `moves[].worker.target`<br>string | This node. |
| `moves[].worker.started_at`<br>string | When the worker started, RFC 3339. |
| `moves[].worker.phase`<br>string: `copying`, `frozen`, `flip_pending`, `waiting_for_source`, `blocked` | What the worker is doing. |
| `moves[].worker.attempts`<br>integer | Copy attempts against a live source. |
| `moves[].worker.last_error`<br>string | The last thing that failed. |
| `moves[].worker.copied_bytes`<br>integer | Bytes the current copy fetched. |
| `moves[].worker.blocked`<br>string: `copy_unverifiable`, `source_dead_copy_behind` | Why the move cannot finish on its own. Absent while it can. |
| `moves[].worker_error` (v3.1.0)<br>string | With `detail=true`, why the destination's report could not be read. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/cluster/moves"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 119
Content-Type: application/json
Date: Mon, 05 Oct 2026 19:15:09 GMT

{
  "moves": [
    {
      "topic": "orders",
      "partition": 3,
      "from": "narad-0",
      "to": "narad-2",
      "from_status": "alive",
      "to_status": "alive"
    }
  ]
}
```

### Decommission a node {#decommission-member}

`POST /v1/cluster/members/{id}/decommission`

Marks the node as draining. The cluster moves every partition it
owns onto the other nodes and, once it owns none and its ingress WAL
has handed every accepted message to its owner, removes it from
the Raft voters. Remove the node only after that; the steps are in
[Scale out and in](../operate/scaling.md#decommission). While it
drains, the node answers produce with `503`.

The request is checked first, on the node that receives it and
again on the leader. A decommission that could never complete
safely is refused with `409` and every reason, and nothing changes:
`below_min_voters` (fewer than three voters would remain),
`no_healthy_majority` (the voters left alive would not be a
majority), `no_receivers` (no other alive node can take its
partitions) or `owner_dead` (the node is dead and owns partitions).

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `id`<br>path, string, required | The node ID, as `GET /v1/cluster/members` lists it (the pod name under the Helm chart). |
| `dry_run` (v3.1.0)<br>query, boolean, optional, default `False` | `true` answers `200` with what a decommission would do and changes nothing. Answered by the receiving node, never forwarded. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | `dry_run=true`: the verdict. Nothing changed. |
| [`204`](status-codes.md#status-204) | Marked as draining. |
| [`400`](status-codes.md#status-400) | `dry_run` is not `true` or `false`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`. |
| [`404`](status-codes.md#status-404) | No member has this ID. |
| [`409`](status-codes.md#status-409) | The decommission could never complete safely. The body names every reason; nothing changed. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `member`<br>string | Node ID. |
| `would_decommission`<br>boolean | `true` when a decommission would be accepted. |
| `reasons`<br>array of object | Why it would be refused; empty when it would not. |
| `reasons[].code`<br>string: `below_min_voters`, `no_healthy_majority`, `move_target`, `dispatch_backlog`, `node_status_unavailable`, `no_receivers`, `owner_dead`, `move_budget_full`, `leader_transfer` | The reason, as `narad_decommission_blocked` labels it ([Troubleshooting](../operate/troubleshooting.md#decommission-blocked)). |
| `reasons[].message`<br>string | What it means here and what to do. |
| `voter`<br>boolean | `true` when the node is a Raft voter. |
| `owned_partitions`<br>integer | Partitions the node owns, all of which would move off it. |
| `inbound_moves`<br>integer | Moves aimed at the node; a decommission clears them. |

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/cluster/members/narad-0/decommission" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:44 GMT
```

### Cancel a decommission {#cancel-decommission}

`DELETE /v1/cluster/members/{id}/decommission`

Clears the draining mark. Partitions that already moved stay where
they are; the node keeps the rest.

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `id`<br>path, string, required | The node ID, as `GET /v1/cluster/members` lists it (the pod name under the Helm chart). |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | No longer draining. |
| [`400`](status-codes.md#status-400) | `dry_run` was given: a cancel cannot be dry-run. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`. |
| [`404`](status-codes.md#status-404) | No member has this ID. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change. |

```sh title="Request"
curl -i -u "$AUTH" -X DELETE \
  "$NARAD/v1/cluster/members/narad-0/decommission"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:44 GMT
```

### Abort a partition move {#abort-move}

**New in v3.1.0.**

`POST /v1/cluster/moves/{topic}/{partition}/abort`

Clears the move's target, so the partition stays with its owner
and keeps serving there. The destination discards its copy. The
abort is a compare-and-set on the leader: a move re-planned to
another node in the meantime is left alone. The answer is read
back from the leader's assignment after the abort, so a move whose
flip committed before the abort reached the leader is refused
with `409`, never reported as aborted. The cluster may plan a move
for the partition again later. An abort that took effect is
audited as `cluster.move.abort`; one whose outcome could not be
read back is audited with `outcome unknown`.

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `topic`<br>path, string, required | Name of the topic. |
| `partition`<br>path, integer, required | The partition number. |
| `target`<br>query, string, optional | The destination you mean, as `GET /v1/cluster/moves` showed it. When the move now targets another node, the request is refused with `409` and nothing changes. |

**Responses**

| Status | Meaning |
|---|---|
| [`202`](status-codes.md#status-202) | The leader cleared the target; the partition stays with its owner. |
| [`400`](status-codes.md#status-400) | The partition is not a number. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`. |
| [`404`](status-codes.md#status-404) | The partition has no assignment. |
| [`409`](status-codes.md#status-409) | No move is in flight for the partition, or it targets another node than `target`, or the leader's assignment after the abort shows the move finished first (another node owns the partition now) or still in flight; the message names the owner and target. Nothing was aborted. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change, or the abort reached the leader but whether it cleared the target could not be read back; list the moves to see. |

**Response body (`202`)**

| Field | Description |
|---|---|
| `move`<br>object |  |
| `move.topic`<br>string | Topic name. |
| `move.partition`<br>integer | Partition number. |
| `move.from`<br>string | Node that owns the partition now. |
| `move.to`<br>string | Node the partition is moving to. |
| `move.from_status` (v3.1.0)<br>string: `alive`, `dead`, `draining`, `not_a_member` | The owner's liveness. |
| `move.to_status` (v3.1.0)<br>string: `alive`, `dead`, `draining`, `not_a_member` | The destination's liveness. |
| `move.blocked` (v3.1.0)<br>string: `source_dead`, `target_dead`, `target_not_member` | Why the move cannot progress as things stand. Absent while it can. A dead source finishes only if the destination can force-promote a complete copy; the leader clears a move to a dead destination after two minutes. |
| `move.worker` (v3.1.0)<br>object | A destination's own report of one move. |
| `move.worker.topic`<br>string | Topic name. |
| `move.worker.partition`<br>integer | Partition number. |
| `move.worker.source`<br>string | The node the copy comes from. |
| `move.worker.target`<br>string | This node. |
| `move.worker.started_at`<br>string | When the worker started, RFC 3339. |
| `move.worker.phase`<br>string: `copying`, `frozen`, `flip_pending`, `waiting_for_source`, `blocked` | What the worker is doing. |
| `move.worker.attempts`<br>integer | Copy attempts against a live source. |
| `move.worker.last_error`<br>string | The last thing that failed. |
| `move.worker.copied_bytes`<br>integer | Bytes the current copy fetched. |
| `move.worker.blocked`<br>string: `copy_unverifiable`, `source_dead_copy_behind` | Why the move cannot finish on its own. Absent while it can. |
| `move.worker_error` (v3.1.0)<br>string | With `detail=true`, why the destination's report could not be read. |
| `note`<br>string | What happens next. |

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/cluster/moves/orders/3/abort?target=narad-2" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 202 Accepted
Content-Length: 240
Content-Type: application/json
Date: Mon, 05 Oct 2026 19:15:09 GMT

{
  "move": {
    "topic": "orders",
    "partition": 3,
    "from": "narad-0",
    "to": "narad-2",
    "from_status": "alive",
    "to_status": "alive"
  },
  "note": "the move's target was cleared; the partition stays with its owner, and the controller may plan a move for it again"
}
```

### Forget a Raft server with no member record {#forget-server}

**New in v3.1.0.**

`POST /v1/cluster/members/{id}/forget`

Removes a Raft voter or non-voter that has no member record, such as
a joiner a 3.0.x leader admitted that never registered. Such a
server counts against quorum as a voter, and holds back new Raft
entry types whatever its suffrage, and decommission cannot reach
it. Forget moves and deletes no data: it refuses a server with a
member record, alive, dead or draining (decommission it instead),
and one a partition assignment names. It also refuses a voter
unless the leader and the other voters it reaches make a majority
of the voters left after the removal: Raft commits the removal
under the new configuration, so a cluster left without that
majority loses its leader and cannot undo the change. It runs on
the leader; followers forward it. The steps are in
[Troubleshooting](../operate/troubleshooting.md#raft-server-no-member-record).

**Grant needed:** `admin`.

**Parameters**

| Name | Description |
|---|---|
| `id`<br>path, string, required | The Raft server ID. The leader's warnings name a server with no member record as `raft server "<id>" has no member record`; under the Helm chart a node's Raft ID is its pod name, such as `narad-3`. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Removed from the Raft configuration. |
| [`400`](status-codes.md#status-400) | The ID names the leader itself. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not `admin`. |
| [`404`](status-codes.md#status-404) | No Raft server has this ID. |
| [`409`](status-codes.md#status-409) | The server has a member record (decommission it instead), a partition assignment names it as owner or move target, or it is a voter and the voters left could lack a quorum (the leader's Raft heartbeats to too many of them are failing, or it has led for less than 12 s). The message says which. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`501`](status-codes.md#status-501) | The leader runs a release before forget. Upgrade it first. |
| [`503`](status-codes.md#status-503) | The cluster has no leader to write the change, or the leader could not be reached. The server may have been removed; read the leader's log or retry. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `id`<br>string | The Raft server ID that was removed. |
| `voter`<br>boolean | `true` when it was a voter, `false` for a non-voter. |

```sh title="Request"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/cluster/members/narad-3/forget" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 31
Content-Type: application/json
Date: Mon, 05 Oct 2026 14:20:04 GMT

{"id":"narad-3","voter":false}
```

## Remotes {#remotes}

**New in v3.2.0.** Register the other Narad clusters this one may send
[remote children](glossary.md#remote-child) to. Every route needs the
`admin` grant and a node with security on; with security off each
one answers [`403`](status-codes.md#status-403) (`remotes require
security`). Answers carry `Cache-Control: no-store`, never contain a
password, and every request writes one `component=audit` line on the
node that took it. A write also needs every member to run this
release ([`412`](status-codes.md#status-412) until then), and a
create or a password change needs the node to attest an encrypted
API hop (`remotes.api_hop_encrypted`). Task guide:
[Manage remotes](../operate/remotes.md).

### Register a remote {#create-remote}

**New in v3.2.0.**

`POST /v1/remotes`

Registers another Narad cluster this one may send remote children
to. The node that takes the request checks every field, resolves
the URL's host and checks it against the address guard, and seals
the password with AES-256-GCM under a key derived from the cluster
secret; only the ciphertext travels to the leader and into Raft.
The password is never shown again: answers carry a keyed
`fingerprint` instead. Nothing is sent to the remote; run
[test a remote](#test-remote) next.

The URL must be `https`, on a port in `remotes.allowed_ports`
(443 by default), on a host in `remotes.allowed_hosts` when that is
set, with no user, query or fragment. A node takes at most 10
remote writes a minute, and a cluster holds at most 64 remotes.

**Grant needed:** `admin`, with security on.

**Request body**

| Field | Description |
|---|---|
| `name`<br>string, required | A lowercase letter, then up to 62 lowercase letters, digits or `-`. |
| `url`<br>string, required | The remote cluster's `https` URL, such as its ingress. Stored in a canonical form. |
| `username`<br>string, required | The replicator user on the remote. Give it `produce` on the replicated topics only; an admin credential fails the checks. |
| `password`<br>string, required | That user's password, 24 to 72 bytes. Write-only. |
| `ca_pem`<br>string, optional | 1 to 16 PEM certificates, at most 64 KiB, that alone verify the remote. Left out, the system roots do. |
| `limits`<br>object, optional | A remote's limits. Each applies per node and changes live; a field left out keeps its value (its default on a create). |
| `limits.max_in_flight`<br>integer, optional, default `16` | Requests to the remote in flight at once on each node, shared by every cursor that sends to it. |
| `limits.request_timeout_ms`<br>integer, optional, default `30000` | Timeout of one request to the remote. |
| `limits.idle_conn_timeout_ms`<br>integer, optional, default `30000` | How long an idle connection to the remote is kept. |
| `limits.conn_max_age_ms`<br>integer, optional, default `300000` | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `limits.check_interval_ms`<br>integer, optional, default `60000` | How often each node re-checks the target of each link, with records to send or not (with 20% jitter). |
| `limits.compression`<br>string: `none`, `zstd`, optional, default `none` | `zstd` compresses a chunk when that saves at least 10% and the target decodes zstd; otherwise it goes uncompressed. |

**Responses**

| Status | Meaning |
|---|---|
| [`201`](status-codes.md#status-201) | Registered. The body is the remote. |
| [`400`](status-codes.md#status-400) | A field is missing, unknown or invalid (the message names the field, never its value), or the URL's host resolves to an address the guard refuses. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`409`](status-codes.md#status-409) | A remote of that name exists, the cluster holds 64 remotes, or two first creates raced (retry). |
| [`412`](status-codes.md#status-412) | Nothing was sealed or stored: this node does not attest an encrypted API hop (`remotes.api_hop_encrypted`); the cluster secret is missing or decodes to fewer than 32 bytes; the current key's seal budget is spent (rotate the cluster secret); a member does not apply the remote Raft entry types, did not answer, or reports a posture that forbids remotes (the body names it in `members`); or the leader runs an older release. |
| [`413`](status-codes.md#status-413) | The body is over 128 KiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | This node took 10 remote writes in the last minute; retry after `Retry-After`, the seconds until the oldest of them leaves the minute. |
| [`500`](status-codes.md#status-500) | The node failed to seal the password; logged on the node. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The leader could not be reached, or could not write the change; the request ID in the node's audit line joins it to the leader's. Read the remote back before retrying. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

**Response body (`201`)**

| Field | Description |
|---|---|
| `name`<br>string | The remote's name. |
| `id`<br>string | An ID minted at create; a remote created again under the same name gets a new one. |
| `url`<br>string | The canonical URL. |
| `username`<br>string | The replicator user on the remote. |
| `password`<br>object | What can be said about the password: `fingerprint` (keyed, so it reveals nothing without the cluster secret), `set_at`, `set_by` and `key_version`, the key it is sealed under. |
| `credential_version`<br>integer | Moves on every new password or re-encrypt. |
| `ca_pem_sha512`<br>string | SHA-512 of the CA bundle. Absent when the system roots verify the remote. |
| `limits`<br>object | A remote's limits. Each applies per node and changes live; a field left out keeps its value (its default on a create). |
| `limits.max_in_flight`<br>integer | Requests to the remote in flight at once on each node, shared by every cursor that sends to it. |
| `limits.request_timeout_ms`<br>integer | Timeout of one request to the remote. |
| `limits.idle_conn_timeout_ms`<br>integer | How long an idle connection to the remote is kept. |
| `limits.conn_max_age_ms`<br>integer | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `limits.check_interval_ms`<br>integer | How often each node re-checks the target of each link, with records to send or not (with 20% jitter). |
| `limits.compression`<br>string: `none`, `zstd` | `zstd` compresses a chunk when that saves at least 10% and the target decodes zstd; otherwise it goes uncompressed. |
| `revision`<br>integer | Moves on every change. |
| `created_at`<br>string | RFC 3339. |
| `created_by`<br>string | The admin who created it. |
| `links`<br>array of string | The remote children that use it, as `parent/child`. Absent in a write's answer. |
| `nodes`<br>array of object | What each member's credential cache holds for the remote. Absent with `nodes=false` and in a write's answer. |
| `nodes[].node`<br>string | Member ID. |
| `nodes[].state`<br>string: `ready`, `stale`, `credential_unreadable`, `node_insecure`, `missing`, `unknown` | `ready`; `stale` while it holds an older credential version than the record; `credential_unreadable` when it cannot open the password (a secret it does not have); `node_insecure` when its posture forbids remotes; `missing` when it holds no entry yet; `unknown` when it did not answer, with `last_error` `unreachable` or `old_release`. |
| `nodes[].credential_version`<br>integer | The credential version it decrypted. |
| `nodes[].key_version`<br>string | The key that version was sealed under. |
| `nodes[].fingerprint`<br>string | The fingerprint of the password it holds. |
| `nodes[].last_ok_at`<br>string | When it last reached the remote successfully. |
| `nodes[].last_error`<br>string | The class of its last failure toward the remote, `none` when there was none. Never text from the remote. |
| `nodes[].server_cert_not_after`<br>string | When the remote's certificate expires, as the last check saw it; only with `remotes.allowed_hosts` set. |
| `nodes[].rtt_ms`<br>integer | Connect time to the remote; only with `remotes.allowed_hosts` set. |

```sh title="Request"
jq -n --rawfile pw repl-password --rawfile ca narad-b-ca.pem \
  '{name: "b", url: "https://localhost:8443",
    username: "repl-from-a-7f3k9q",
    password: ($pw | rtrimstr("\n")), ca_pem: $ca}' |
curl -i -u "$AUTH" -X POST "$NARAD/v1/remotes" \
  -H "Content-Type: application/json" -d @-
```

```http title="Response"
HTTP/1.1 201 Created
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:06:22 GMT
Content-Length: 622

{
  "name": "b",
  "id": "8ccffc20e36f644c",
  "url": "https://localhost:8443",
  "username": "repl-from-a-7f3k9q",
  "password": {
    "fingerprint": "ced1ea5d18d1",
    "set_at": "2026-10-06T13:06:22Z",
    "set_by": "admin",
    "key_version": "b51c9412df29325d"
  },
  "credential_version": 1,
  "ca_pem_sha512": "a85bc5f06c550eb18ff2a2a29187fe3a290b1fc74c76d0ef2781531bd4df4f84187980295b5484a6e363a2ff24c52fd9cec309bd74405beb5c815fb48b1e80b0",
  "limits": {
    "max_in_flight": 16,
    "request_timeout_ms": 30000,
    "idle_conn_timeout_ms": 30000,
    "conn_max_age_ms": 300000,
    "check_interval_ms": 60000,
    "compression": "none"
  },
  "revision": 1,
  "created_at": "2026-10-06T13:06:22Z",
  "created_by": "admin"
}
```

### List remotes {#list-remotes}

**New in v3.2.0.**

`GET /v1/remotes`

Lists every remote, the remote children that use it, and what each
member's credential cache holds for it: its state, the credential
and key versions it decrypted, when it last reached the remote, and
the server certificate's expiry. `lingering` lists deleted remotes
some member still holds (it has not applied the delete) and the
members that did not answer; `not_answering` names every member
that did not answer, even when no answering member holds a deleted
remote. `key` is the current encryption key's
version and age, absent before the first remote.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `nodes`<br>query, boolean, optional, default `True` | `false` skips asking the members for their caches; the answer then has no `nodes`, no `lingering` and no `not_answering`. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The remotes. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The node could not read its copy of the remotes; retry. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `allowlist`<br>string: `set`, `none` | Whether the answering node has `remotes.allowed_hosts` set. |
| `key`<br>object | The current encryption key's `key_version`, `first_sealed_at` and `age_seconds`. Absent before the first remote. |
| `remotes`<br>array of object |  |
| `remotes[].name`<br>string | The remote's name. |
| `remotes[].id`<br>string | An ID minted at create; a remote created again under the same name gets a new one. |
| `remotes[].url`<br>string | The canonical URL. |
| `remotes[].username`<br>string | The replicator user on the remote. |
| `remotes[].password`<br>object | What can be said about the password: `fingerprint` (keyed, so it reveals nothing without the cluster secret), `set_at`, `set_by` and `key_version`, the key it is sealed under. |
| `remotes[].credential_version`<br>integer | Moves on every new password or re-encrypt. |
| `remotes[].ca_pem_sha512`<br>string | SHA-512 of the CA bundle. Absent when the system roots verify the remote. |
| `remotes[].limits`<br>object | A remote's limits. Each applies per node and changes live; a field left out keeps its value (its default on a create). |
| `remotes[].limits.max_in_flight`<br>integer | Requests to the remote in flight at once on each node, shared by every cursor that sends to it. |
| `remotes[].limits.request_timeout_ms`<br>integer | Timeout of one request to the remote. |
| `remotes[].limits.idle_conn_timeout_ms`<br>integer | How long an idle connection to the remote is kept. |
| `remotes[].limits.conn_max_age_ms`<br>integer | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `remotes[].limits.check_interval_ms`<br>integer | How often each node re-checks the target of each link, with records to send or not (with 20% jitter). |
| `remotes[].limits.compression`<br>string: `none`, `zstd` | `zstd` compresses a chunk when that saves at least 10% and the target decodes zstd; otherwise it goes uncompressed. |
| `remotes[].revision`<br>integer | Moves on every change. |
| `remotes[].created_at`<br>string | RFC 3339. |
| `remotes[].created_by`<br>string | The admin who created it. |
| `remotes[].links`<br>array of string | The remote children that use it, as `parent/child`. Absent in a write's answer. |
| `remotes[].nodes`<br>array of object | What each member's credential cache holds for the remote. Absent with `nodes=false` and in a write's answer. |
| `remotes[].nodes[].node`<br>string | Member ID. |
| `remotes[].nodes[].state`<br>string: `ready`, `stale`, `credential_unreadable`, `node_insecure`, `missing`, `unknown` | `ready`; `stale` while it holds an older credential version than the record; `credential_unreadable` when it cannot open the password (a secret it does not have); `node_insecure` when its posture forbids remotes; `missing` when it holds no entry yet; `unknown` when it did not answer, with `last_error` `unreachable` or `old_release`. |
| `remotes[].nodes[].credential_version`<br>integer | The credential version it decrypted. |
| `remotes[].nodes[].key_version`<br>string | The key that version was sealed under. |
| `remotes[].nodes[].fingerprint`<br>string | The fingerprint of the password it holds. |
| `remotes[].nodes[].last_ok_at`<br>string | When it last reached the remote successfully. |
| `remotes[].nodes[].last_error`<br>string | The class of its last failure toward the remote, `none` when there was none. Never text from the remote. |
| `remotes[].nodes[].server_cert_not_after`<br>string | When the remote's certificate expires, as the last check saw it; only with `remotes.allowed_hosts` set. |
| `remotes[].nodes[].rtt_ms`<br>integer | Connect time to the remote; only with `remotes.allowed_hosts` set. |
| `lingering`<br>array of object | Deleted remotes some member still holds, with the members `holding` them and the ones `not_answering`. |
| `not_answering`<br>array of string | Every member that was asked and did not answer, whether or not an answering member still holds a deleted remote. Such a member may still hold one. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/remotes"
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Length: 1012
Content-Type: application/json
Date: Tue, 06 Oct 2026 13:06:32 GMT

{
  "allowlist": "set",
  "key": {
    "key_version": "b51c9412df29325d",
    "first_sealed_at": "2026-10-06T13:06:22Z",
    "age_seconds": 10
  },
  "remotes": [
    {
      "name": "b",
      "id": "8ccffc20e36f644c",
      "url": "https://localhost:8443",
      "username": "repl-from-a-7f3k9q",
      "password": {
        "fingerprint": "ced1ea5d18d1",
        "set_at": "2026-10-06T13:06:22Z",
        "set_by": "admin",
        "key_version": "b51c9412df29325d"
      },
      "credential_version": 1,
      "ca_pem_sha512": "a85bc5f06c550eb18ff2a2a29187fe3a290b1fc74c76d0ef2781531bd4df4f84187980295b5484a6e363a2ff24c52fd9cec309bd74405beb5c815fb48b1e80b0",
      "limits": {
        "max_in_flight": 16,
        "request_timeout_ms": 30000,
        "idle_conn_timeout_ms": 30000,
        "conn_max_age_ms": 300000,
        "check_interval_ms": 60000,
        "compression": "none"
      },
      "revision": 1,
      "created_at": "2026-10-06T13:06:22Z",
      "created_by": "admin",
      "nodes": [
        {
          "node": "narad-0",
          "state": "ready",
          "credential_version": 1,
          "key_version": "b51c9412df29325d",
          "fingerprint": "ced1ea5d18d1",
          "last_ok_at": "2026-10-06T13:06:32Z",
          "last_error": "none",
          "server_cert_not_after": "2026-11-05T13:04:25Z",
          "rtt_ms": 0
        }
      ]
    }
  ],
  "lingering": [],
  "not_answering": []
}
```

### Get a remote {#get-remote}

**New in v3.2.0.**

`GET /v1/remotes/{name}`

Returns one remote, with what each member's credential cache holds
for it, as [list remotes](#list-remotes) does.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `name`<br>path, string, required | The remote's name. |
| `nodes`<br>query, boolean, optional, default `True` | `false` skips asking the members for their caches. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The remote. |
| [`400`](status-codes.md#status-400) | `name` is not a remote's name. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote of that name. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `name`<br>string | The remote's name. |
| `id`<br>string | An ID minted at create; a remote created again under the same name gets a new one. |
| `url`<br>string | The canonical URL. |
| `username`<br>string | The replicator user on the remote. |
| `password`<br>object | What can be said about the password: `fingerprint` (keyed, so it reveals nothing without the cluster secret), `set_at`, `set_by` and `key_version`, the key it is sealed under. |
| `credential_version`<br>integer | Moves on every new password or re-encrypt. |
| `ca_pem_sha512`<br>string | SHA-512 of the CA bundle. Absent when the system roots verify the remote. |
| `limits`<br>object | A remote's limits. Each applies per node and changes live; a field left out keeps its value (its default on a create). |
| `limits.max_in_flight`<br>integer | Requests to the remote in flight at once on each node, shared by every cursor that sends to it. |
| `limits.request_timeout_ms`<br>integer | Timeout of one request to the remote. |
| `limits.idle_conn_timeout_ms`<br>integer | How long an idle connection to the remote is kept. |
| `limits.conn_max_age_ms`<br>integer | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `limits.check_interval_ms`<br>integer | How often each node re-checks the target of each link, with records to send or not (with 20% jitter). |
| `limits.compression`<br>string: `none`, `zstd` | `zstd` compresses a chunk when that saves at least 10% and the target decodes zstd; otherwise it goes uncompressed. |
| `revision`<br>integer | Moves on every change. |
| `created_at`<br>string | RFC 3339. |
| `created_by`<br>string | The admin who created it. |
| `links`<br>array of string | The remote children that use it, as `parent/child`. Absent in a write's answer. |
| `nodes`<br>array of object | What each member's credential cache holds for the remote. Absent with `nodes=false` and in a write's answer. |
| `nodes[].node`<br>string | Member ID. |
| `nodes[].state`<br>string: `ready`, `stale`, `credential_unreadable`, `node_insecure`, `missing`, `unknown` | `ready`; `stale` while it holds an older credential version than the record; `credential_unreadable` when it cannot open the password (a secret it does not have); `node_insecure` when its posture forbids remotes; `missing` when it holds no entry yet; `unknown` when it did not answer, with `last_error` `unreachable` or `old_release`. |
| `nodes[].credential_version`<br>integer | The credential version it decrypted. |
| `nodes[].key_version`<br>string | The key that version was sealed under. |
| `nodes[].fingerprint`<br>string | The fingerprint of the password it holds. |
| `nodes[].last_ok_at`<br>string | When it last reached the remote successfully. |
| `nodes[].last_error`<br>string | The class of its last failure toward the remote, `none` when there was none. Never text from the remote. |
| `nodes[].server_cert_not_after`<br>string | When the remote's certificate expires, as the last check saw it; only with `remotes.allowed_hosts` set. |
| `nodes[].rtt_ms`<br>integer | Connect time to the remote; only with `remotes.allowed_hosts` set. |

```sh title="Request"
curl -i -u "$AUTH" "$NARAD/v1/remotes/b?nodes=false"
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Length: 622
Content-Type: application/json
Date: Tue, 06 Oct 2026 13:06:32 GMT

{
  "name": "b",
  "id": "8ccffc20e36f644c",
  "url": "https://localhost:8443",
  "username": "repl-from-a-7f3k9q",
  "password": {
    "fingerprint": "ced1ea5d18d1",
    "set_at": "2026-10-06T13:06:22Z",
    "set_by": "admin",
    "key_version": "b51c9412df29325d"
  },
  "credential_version": 1,
  "ca_pem_sha512": "a85bc5f06c550eb18ff2a2a29187fe3a290b1fc74c76d0ef2781531bd4df4f84187980295b5484a6e363a2ff24c52fd9cec309bd74405beb5c815fb48b1e80b0",
  "limits": {
    "max_in_flight": 16,
    "request_timeout_ms": 30000,
    "idle_conn_timeout_ms": 30000,
    "conn_max_age_ms": 300000,
    "check_interval_ms": 60000,
    "compression": "none"
  },
  "revision": 1,
  "created_at": "2026-10-06T13:06:22Z",
  "created_by": "admin"
}
```

### Change a remote {#update-remote}

**New in v3.2.0.**

`PATCH /v1/remotes/{name}`

Changes the fields named and nothing else. A new `url`, `username`
or `ca_pem` needs `password` in the same request: a stored password
is only ever sent to the URL and user it was entered with, verified
against the CA it was entered with. `ca_pem: ""` drops the CA and
uses the system roots. A new password is sealed on this node, as on
a create, and the links pick it up without a restart. `limits`
change live; a limit named with `0` or `""` is refused.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `name`<br>path, string, required | The remote's name. |

**Request body**

| Field | Description |
|---|---|
| `url`<br>string, optional | A new `https` URL. |
| `username`<br>string, optional | A new replicator user. |
| `password`<br>string, optional | A new password, 24 to 72 bytes. |
| `ca_pem`<br>string, optional | A new CA bundle, or `""` for the system roots. |
| `limits`<br>object, optional | A remote's limits. Each applies per node and changes live; a field left out keeps its value (its default on a create). |
| `limits.max_in_flight`<br>integer, optional, default `16` | Requests to the remote in flight at once on each node, shared by every cursor that sends to it. |
| `limits.request_timeout_ms`<br>integer, optional, default `30000` | Timeout of one request to the remote. |
| `limits.idle_conn_timeout_ms`<br>integer, optional, default `30000` | How long an idle connection to the remote is kept. |
| `limits.conn_max_age_ms`<br>integer, optional, default `300000` | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `limits.check_interval_ms`<br>integer, optional, default `60000` | How often each node re-checks the target of each link, with records to send or not (with 20% jitter). |
| `limits.compression`<br>string: `none`, `zstd`, optional, default `none` | `zstd` compresses a chunk when that saves at least 10% and the target decodes zstd; otherwise it goes uncompressed. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Changed. The body is the remote. |
| [`400`](status-codes.md#status-400) | A field is unknown or invalid, nothing to change, or a new `url`, `username` or `ca_pem` came without `password`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote of that name. |
| [`409`](status-codes.md#status-409) | The remote changed since this node read it (retry), or it was created again under the same name. |
| [`412`](status-codes.md#status-412) | As for [register a remote](#create-remote); the hop and secret rules apply only to a change that carries a password. |
| [`413`](status-codes.md#status-413) | The body is over 128 KiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | This node took 10 remote writes in the last minute; retry after `Retry-After`, the seconds until the oldest of them leaves the minute. |
| [`500`](status-codes.md#status-500) | The node failed to seal the password; logged on the node. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The leader could not be reached, or could not write the change. Read the remote back before retrying. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `name`<br>string | The remote's name. |
| `id`<br>string | An ID minted at create; a remote created again under the same name gets a new one. |
| `url`<br>string | The canonical URL. |
| `username`<br>string | The replicator user on the remote. |
| `password`<br>object | What can be said about the password: `fingerprint` (keyed, so it reveals nothing without the cluster secret), `set_at`, `set_by` and `key_version`, the key it is sealed under. |
| `credential_version`<br>integer | Moves on every new password or re-encrypt. |
| `ca_pem_sha512`<br>string | SHA-512 of the CA bundle. Absent when the system roots verify the remote. |
| `limits`<br>object | A remote's limits. Each applies per node and changes live; a field left out keeps its value (its default on a create). |
| `limits.max_in_flight`<br>integer | Requests to the remote in flight at once on each node, shared by every cursor that sends to it. |
| `limits.request_timeout_ms`<br>integer | Timeout of one request to the remote. |
| `limits.idle_conn_timeout_ms`<br>integer | How long an idle connection to the remote is kept. |
| `limits.conn_max_age_ms`<br>integer | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `limits.check_interval_ms`<br>integer | How often each node re-checks the target of each link, with records to send or not (with 20% jitter). |
| `limits.compression`<br>string: `none`, `zstd` | `zstd` compresses a chunk when that saves at least 10% and the target decodes zstd; otherwise it goes uncompressed. |
| `revision`<br>integer | Moves on every change. |
| `created_at`<br>string | RFC 3339. |
| `created_by`<br>string | The admin who created it. |
| `links`<br>array of string | The remote children that use it, as `parent/child`. Absent in a write's answer. |
| `nodes`<br>array of object | What each member's credential cache holds for the remote. Absent with `nodes=false` and in a write's answer. |
| `nodes[].node`<br>string | Member ID. |
| `nodes[].state`<br>string: `ready`, `stale`, `credential_unreadable`, `node_insecure`, `missing`, `unknown` | `ready`; `stale` while it holds an older credential version than the record; `credential_unreadable` when it cannot open the password (a secret it does not have); `node_insecure` when its posture forbids remotes; `missing` when it holds no entry yet; `unknown` when it did not answer, with `last_error` `unreachable` or `old_release`. |
| `nodes[].credential_version`<br>integer | The credential version it decrypted. |
| `nodes[].key_version`<br>string | The key that version was sealed under. |
| `nodes[].fingerprint`<br>string | The fingerprint of the password it holds. |
| `nodes[].last_ok_at`<br>string | When it last reached the remote successfully. |
| `nodes[].last_error`<br>string | The class of its last failure toward the remote, `none` when there was none. Never text from the remote. |
| `nodes[].server_cert_not_after`<br>string | When the remote's certificate expires, as the last check saw it; only with `remotes.allowed_hosts` set. |
| `nodes[].rtt_ms`<br>integer | Connect time to the remote; only with `remotes.allowed_hosts` set. |

```sh title="Request"
curl -i -u "$AUTH" -X PATCH "$NARAD/v1/remotes/b" \
  -H "Content-Type: application/json" \
  -d '{"limits": {"max_in_flight": 32, "compression": "zstd"}}'
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:06:32 GMT
Content-Length: 622

{
  "name": "b",
  "id": "8ccffc20e36f644c",
  "url": "https://localhost:8443",
  "username": "repl-from-a-7f3k9q",
  "password": {
    "fingerprint": "ced1ea5d18d1",
    "set_at": "2026-10-06T13:06:22Z",
    "set_by": "admin",
    "key_version": "b51c9412df29325d"
  },
  "credential_version": 1,
  "ca_pem_sha512": "a85bc5f06c550eb18ff2a2a29187fe3a290b1fc74c76d0ef2781531bd4df4f84187980295b5484a6e363a2ff24c52fd9cec309bd74405beb5c815fb48b1e80b0",
  "limits": {
    "max_in_flight": 32,
    "request_timeout_ms": 30000,
    "idle_conn_timeout_ms": 30000,
    "conn_max_age_ms": 300000,
    "check_interval_ms": 60000,
    "compression": "zstd"
  },
  "revision": 2,
  "created_at": "2026-10-06T13:06:22Z",
  "created_by": "admin"
}
```

### Delete a remote {#delete-remote}

**New in v3.2.0.**

`DELETE /v1/remotes/{name}`

Deletes the remote and its stored password. Refused while remote
children name it, unless `force=true`: they then hold, without
loss while the parent's retention lasts, in state
`remote_missing` until a remote of that name exists again. Each
member drops its cached credential and closes its connections when
it applies the delete; [list remotes](#list-remotes) shows the
members that have not under `lingering`. The leader checks only
the member records, so a delete works while a member is down. In
an emergency, revoke the user on the target first
([Manage remotes](../operate/remotes.md#emergency-revocation)).

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `name`<br>path, string, required | The remote's name. |
| `force`<br>query, boolean, optional, default `False` | `true` deletes the remote even while remote children use it. |

**Responses**

| Status | Meaning |
|---|---|
| [`204`](status-codes.md#status-204) | Deleted. |
| [`400`](status-codes.md#status-400) | `name` is not a remote's name, or `force` is not `true` or `false`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote of that name. |
| [`409`](status-codes.md#status-409) | Remote children use the remote; the body lists them in `links`. |
| [`412`](status-codes.md#status-412) | A member does not apply the remote Raft entry types (the body names it), or the leader runs an older release. |
| [`429`](status-codes.md#status-429) | This node took 10 remote writes in the last minute; retry after `Retry-After`, the seconds until the oldest of them leaves the minute. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The leader could not be reached, or could not write the change. Read the remotes back before retrying. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

```sh title="Request: in use"
curl -i -u "$AUTH" -X DELETE "$NARAD/v1/remotes/b"
```

```http title="Response: in use"
HTTP/1.1 409 Conflict
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:49 GMT
Content-Length: 79

{"error":"remote is used by 1 remote children","links":["orders/orders-to-b"]}
```

```sh title="Request: once no remote child uses it"
curl -i -u "$AUTH" -X DELETE "$NARAD/v1/remotes/b"
```

```http title="Response: once no remote child uses it"
HTTP/1.1 204 No Content
Cache-Control: no-store
Date: Tue, 06 Oct 2026 13:07:49 GMT
```

### Test a remote {#test-remote}

**New in v3.2.0.**

`POST /v1/remotes/{name}/test`

Runs the attach checks against `topic` on the remote, writing
nothing on either cluster: the dial passes the address guard, TLS
verifies against the remote's CA (or the system roots), the target
answers `401` without credentials, the topic exists and is no delay
child or remote child stub, its children include no remote child,
the schemas match (with `source`), an empty batch produce is
answered as a target that takes batch produce answers it, and the
credential is not an admin there. With `remotes.allowed_hosts` set,
every member runs the checks and reports the connect time
(`rtt_ms`) and an estimate of one lane's capacity; without it only
this node runs them, and its report carries only `node`, `result`,
`class` and this cluster's own fields: no time, no target ID, no
`target_serves_ids`, no certificate expiry, no warnings drawn from
the target's answers.

The answer is `200` whether or not the checks pass: read `result`.
`narad remote test` exits non-zero unless it is `pass`.

**Grant needed:** `admin`, with security on.

**Parameters**

| Name | Description |
|---|---|
| `name`<br>path, string, required | The remote's name. |

**Request body**

| Field | Description |
|---|---|
| `topic`<br>string, required | The topic on the remote. |
| `source`<br>string, optional | The parent topic on this cluster, for the schema, source and loop checks. |

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The checks ran. `result` is `pass` only when every member passed at the remote's current credential version; otherwise `class` names the most important failure. |
| [`400`](status-codes.md#status-400) | `topic` is missing or not a topic name, or `source` is not one. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`404`](status-codes.md#status-404) | No remote of that name, or no `source` topic of that name. |
| [`413`](status-codes.md#status-413) | The body is over 128 KiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | A check of this remote ran less than 5 seconds ago on this node (or, with an allowlist, on a member); retry after `Retry-After`. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The cluster's members could not be listed; retry. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `remote`<br>string | The remote. |
| `result`<br>string: `pass`, `fail` | `pass` only when every member passed. |
| `class`<br>string | When `result` is `fail`, the most important failure ([check classes](remote-children.md#check-classes)). |
| `checks`<br>array of object | One report per member that ran the checks. |
| `checks[].node`<br>string | Member ID. |
| `checks[].result`<br>string: `pass`, `fail` | This member's verdict. |
| `checks[].class`<br>string | Why it failed. |
| `checks[].credential_version`<br>integer | The credential version it checked with. |
| `checks[].target_id`<br>string | The target topic's ID. |
| `checks[].target_serves_ids`<br>boolean | `false` for a target whose children listing serves no `parent_id` and no `remote` objects (v3.1.0): it cannot hold a remote child, so loop detection starts once it is upgraded; recreate detection reads the topic id from its describe answer. Absent without `remotes.allowed_hosts` (a blind report) and when the checks stopped before they read the target's children listing. |
| `checks[].rtt_ms`<br>integer | TCP connect time, with `remotes.allowed_hosts` set. |
| `checks[].lane_capacity_per_s`<br>integer | An estimate of one lane's records per second at that round trip, with `remotes.allowed_hosts` set. |
| `checks[].server_cert_not_after`<br>string | When the target's certificate expires, with `remotes.allowed_hosts` set. |
| `checks[].warnings`<br>array of string | Advisories, such as a certificate that expires within 14 days. |
| `checks[].posture`<br>object | The member's `security_enabled`, `legacy_cluster_auth`, `raft_tls` and `api_hop_encrypted`. |

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/remotes/b/test" \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "source": "orders"}'
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Length: 361
Content-Type: application/json
Date: Tue, 06 Oct 2026 13:06:32 GMT

{
  "remote": "b",
  "result": "pass",
  "checks": [
    {
      "node": "narad-0",
      "result": "pass",
      "credential_version": 1,
      "target_id": "128e63dd156ff568",
      "target_serves_ids": true,
      "rtt_ms": 0,
      "lane_capacity_per_s": 20000,
      "server_cert_not_after": "2026-11-05T13:04:25Z",
      "warnings": [],
      "posture": {
        "security_enabled": true,
        "legacy_cluster_auth": false,
        "raft_tls": true,
        "api_hop_encrypted": true
      }
    }
  ]
}
```

### Re-encrypt remote passwords {#reencrypt-remotes}

**New in v3.2.0.**

`POST /v1/cluster/reencrypt-remotes`

After a cluster secret rotation, re-seals every stored remote
password that is still under the previous key: the leader opens it
with `NARAD_CLUSTER_SECRET_PREVIOUS` and seals it under the current
secret, bound to the same remote, URL, username and CA. A remote
changed in between keeps its newer ciphertext. Safe to repeat.
Steps: [Rotate the cluster secret](../operate/remotes.md#rotate-cluster-secret).

**Grant needed:** `admin`, with security on.

**Request body**

Empty, or `{}`. Content types: `application/json`.

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Done. `reencrypted` names the remotes moved to the current key, `already_current` the ones that were, and `failed` the ones the leader could not open (`key_unknown`: sealed under a key neither secret derives, as when `NARAD_CLUSTER_SECRET_PREVIOUS` is not set on the leader; `open_failed`) or write, each with a `reason`. |
| [`400`](status-codes.md#status-400) | The body is not empty or `{}`. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials. |
| [`403`](status-codes.md#status-403) | Not an `admin`, or security is off (`remotes require security`). |
| [`412`](status-codes.md#status-412) | The leader's cluster secret is missing or decodes to fewer than 32 bytes, a member does not apply the remote Raft entry types, did not answer, or reports a posture that forbids remotes (the body names it in `members`), or the leader runs an older release. Nothing was re-sealed. |
| [`413`](status-codes.md#status-413) | The body is over 128 KiB. |
| [`415`](status-codes.md#status-415) | No accepted `Content-Type` and no `X-Narad-Client` header. |
| [`429`](status-codes.md#status-429) | This node took 10 remote writes in the last minute; retry after `Retry-After`, the seconds until the oldest of them leaves the minute. |
| [`501`](status-codes.md#status-501) | The answering node has no remote plane. |
| [`503`](status-codes.md#status-503) | The leader could not be reached; it is safe to repeat. A leader that could not be reached answers with `Retry-After: 2`. When the leader committed the change but the answering node could not confirm that its own copy applied it, `503` with `Retry-After: 2` and an error that says the change is committed: read it back after the delay, or on another node, and do not send it again. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `key_version`<br>string | The current key. |
| `reencrypted`<br>array of string | Remotes moved to the current key. |
| `already_current`<br>array of string | Remotes already under it. |
| `failed`<br>array of object | Remotes not moved, each with `name` and `reason`. |

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/cluster/reencrypt-remotes" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 200 OK
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:49 GMT
Content-Length: 88

{
  "key_version": "b51c9412df29325d",
  "reencrypted": [],
  "already_current": ["b"],
  "failed": []
}
```

## Health and metrics {#health-and-metrics}

Probes and the Prometheus exposition. Where each is served, and which
port the Helm chart probes, is in
[Helm values reference](helm-values.md#ports-and-probes).

### Check liveness {#healthz}

`GET /healthz`

Answers `200` while the process runs, from the moment it starts,
and `503` once a graceful shutdown has begun. It never needs
credentials. It is also served on the metrics listener when
`http.metrics_addr` is set.

**Grant needed:** None.

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The process is up. |
| [`503`](status-codes.md#status-503) | The node is shutting down. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `status`<br>string | `ok` for `/healthz`, `ready` for `/readyz`. |

```sh title="Request"
curl -i "$NARAD/healthz"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 16
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

{"status":"ok"}
```

### Check readiness {#readyz}

`GET /readyz`

Answers `200` only while the node should receive traffic: its
startup work is done, it has a Raft leader in view that it heard
from within the last 5 seconds (or it is the leader), and its copy
of the cluster metadata has caught up with the leader since it
started. Otherwise it answers `503` with the reason in `error`.
The check runs on every request. It never needs credentials.

A `200` can list conditions under `degraded` (from v3.1.0): an
expired Raft TLS certificate or CA bundle. They do not make the
node unready, because one certificate usually serves every node
and expires on all of them at once, and failing readiness would
take every pod out of its Services. See
[Raft TLS certificates](../operate/raft-tls.md#expiry).

**Grant needed:** None.

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | Ready for traffic. |
| [`503`](status-codes.md#status-503) | Not ready. The `error` field says why. |

**Response body (`200`)**

| Field | Description |
|---|---|
| `status`<br>string | Always `ready`. |
| `degraded` (v3.1.0)<br>array of string: `raft_tls_certificate_expired`, `raft_tls_ca_expired` | Present only when something is wrong that does not make the node unready: `raft_tls_certificate_expired` when the node's Raft TLS certificate has expired, `raft_tls_ca_expired` when every CA in its Raft CA bundle has. Peers refuse new Raft connections until the node restarts with renewed files. |

```sh title="Request"
curl -i "$NARAD/readyz"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 19
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

{"status":"ready"}
```

### Scrape metrics {#metrics}

`GET /metrics`

The Prometheus text exposition; every series is in the
[Metrics reference](metrics.md). Where it is served depends on
`http.metrics_addr`:

- Not set (the binary's default): on the API port, and it needs
  the same credentials as the API unless
  `http.metrics_unauthenticated` is `true`.
- Set (the Helm chart sets `:9100`): on that listener without
  credentials, and not on the API port, where a request gets
  `404` (or `401` without credentials).

**Grant needed:** Any valid credentials on the API port; none on the metrics listener.

**Responses**

| Status | Meaning |
|---|---|
| [`200`](status-codes.md#status-200) | The exposition. |
| [`401`](status-codes.md#status-401) | Missing or wrong credentials on the API port. |

```sh title="Request"
curl -sS -u "$AUTH" "$NARAD/metrics" | grep '^narad_topics_total'
```

```text title="Response"
narad_topics_total 2
```

## Objects {#objects}

Bodies that several endpoints share.

### Topic object {#topic-object}

| Field | Description |
|---|---|
| `name`<br>string | Topic name. |
| `id`<br>string | The [incarnation](glossary.md#incarnation) ID, 16 hex characters. A topic deleted and created again under the same name gets a new one. Absent on a topic created before v2.2.0. |
| `partitions`<br>integer | Partition count. |
| `retention_ms`<br>integer | Retention in milliseconds; `0` keeps messages forever. |
| `visibility_timeout_ms`<br>integer | Lease length in milliseconds. |
| `max_in_flight_per_partition`<br>integer | In-flight cap per partition. |
| `max_acked_ahead_per_partition`<br>integer | Acked-ahead cap per partition. |
| `created_at`<br>integer | Creation time, Unix seconds. |
| `owner`<br>string | The user that created the topic. Absent when security was off. |
| `role`<br>string: `standalone`, `parent`, `child` | Fan-out role. Absent in the answer to a create without `parent`, which means `standalone`. |
| `children`<br>array of string | A parent's children, in attach order. |
| `parent`<br>string | A child's parent. |
| `attach_epoch`<br>string | A child's current attachment; it changes on every attach. |
| `fanout_delay_ms`<br>integer | A delay child's delay in milliseconds. |
| `attach_offsets`<br>array of integer | A child's attach point, one offset per parent partition. |
| `remote` (v3.2.0)<br>object | A [remote child](glossary.md#remote-child)'s link. Present only on a remote child's stub, which has `partitions` `0` and no owner. |
| `remote.name`<br>string | The remote the copies go to. |
| `remote.topic`<br>string | The topic on the remote. |
| `remote.target_id`<br>string | The target topic's ID as the attach, or the last resume with `accept_target`, saw it. A target on v3.1.0 serves it in its describe answer, so recreate detection works there too. Empty for a target topic created before topic IDs (v2.1 and earlier); such a link stops in `target_replaced` if the target later reports an ID, because a topic gains one only by being recreated. |
| `remote.from`<br>string: `attach`, `unconsumed`, `earliest` | Where the link started on each parent partition. |
| `remote.lanes`<br>integer | Ordered streams per parent partition, 1 to 8. |
| `remote.paused`<br>boolean | `true` while paused. Absent otherwise. |
| `remote.pause_reason`<br>string | The reason given to pause. |
| `remote.paused_by`<br>string | The admin who paused it; shown to admins only. |
| `remote.paused_at_ms`<br>integer | When it was paused, Unix milliseconds. |
| `remote.skip`<br>object | Per parent partition, the offsets an admin accepted to lose, ascending, at most 4000. A cursor drops a record only while it is stuck on exactly one of them. |
| `remote.created_by`<br>string | The admin who attached it; shown to admins only. |

### Partition statistics object {#partition-stats-object}

| Field | Description |
|---|---|
| `index`<br>integer | Partition number. |
| `segments`<br>integer | Segment files on disk. |
| `oldest_offset`<br>integer | Lowest offset still kept. |
| `next_offset`<br>integer | Offset the next record will get. It can lead `high_watermark` while a commit runs. |
| `high_watermark`<br>integer | One past the last offset consumers can see. |
| `size_bytes`<br>integer | Bytes on disk. |
| `oldest_segment_at`<br>integer | Time of the oldest segment, Unix seconds. Absent when unknown. |
| `owner_node`<br>string | ID of the node that owns the partition. |
| `status` (v3.1.0)<br>string: `ok`, `owner_unavailable` | `ok` when the statistics are the owner's; `owner_unavailable` for a placeholder with zero statistics, because the owner could not report them. Never add a placeholder's numbers to a total. |
| `owner_liveness` (v3.1.0)<br>string: `dead`, `unreachable`, `unknown`, `unassigned` | Why an `owner_unavailable` partition's owner could not report: `dead` (marked dead), `unreachable` (alive, but its statistics did not come back within 2 seconds), `unknown` (no member with an address), `unassigned` (no owner yet). Absent for `ok`. |

### Message object {#message-object}

One message. A batch consume answers `{"messages": [...]}` with one of these per message.

| Field | Description |
|---|---|
| `topic`<br>string | Topic name. |
| `partition`<br>integer | Partition the message is stored in. |
| `offset`<br>integer | Position in the partition. |
| `key`<br>string | The produce key. Absent for a message produced without one. |
| `key_encoding` (v3.1.0)<br>string: `base64` | `base64` when the key is not valid UTF-8 and `key` holds it in base64. |
| `payload`<br>JSON | The message as it was produced. Valid JSON comes back as JSON, other UTF-8 text as a JSON string, and anything else as a base64 string with `payload_encoding`. |
| `payload_encoding`<br>string: `base64` | `base64` when `payload` holds binary data in base64. |
| `timestamp`<br>integer | When the message was committed to its partition, Unix seconds. |
| `receipt_handle`<br>string | The lease to settle with [ack](#ack), `partition:offset:nonce`. Absent on a replay. |

### User object {#user-object}

| Field | Description |
|---|---|
| `username`<br>string | The username. |
| `grants`<br>array of [Grant](#grant-object) | The user's grants. Absent when it has none. |
| `root`<br>boolean | `true` for the root admin, which holds every right and cannot be deleted. Absent otherwise. |
| `created_at_ms`<br>integer | Creation time, Unix milliseconds. |
| `updated_at_ms`<br>integer | Time of the last change, Unix milliseconds. |

### Grant object {#grant-object}

| Field | Description |
|---|---|
| `action`<br>string: `produce`, `consume`, `create`, `admin` | What the grant allows; see [Access model and grants](access-model.md#actions). |
| `patterns`<br>array of string | Topic names or prefix wildcards such as `invoices.*`. Required for every action but `admin`, which takes none. |
