---
description: "Create, inspect, change and delete the topics your services produce to and consume from."
---

# Manage topics

Create, inspect, change and delete the topics your services produce to and consume from.

Before you start: to create a topic you need a `create` grant that matches its name; to change or delete one you must own it or hold `admin`. [Access model and grants](../reference/access-model.md) has the full rules.

## Create a topic {#create}

=== "curl"

    ```sh title="Create the orders topic"
    curl -i -u "$AUTH" -X POST "$NARAD/v1/topics" \
      -H "Content-Type: application/json" \
      -d '{"name": "orders", "partitions": 6, "retention_ms": 86400000}'
    ```

    ```http title="Response"
    HTTP/1.1 201 Created
    Content-Length: 233
    Content-Type: application/json
    Date: Mon, 28 Sep 2026 19:31:25 GMT

    {
      "name": "orders",
      "id": "244de9d4ac35abae",
      "partitions": 6,
      "retention_ms": 86400000,
      "visibility_timeout_ms": 30000,
      "max_in_flight_per_partition": 1024,
      "max_acked_ahead_per_partition": 1024,
      "created_at": 1790623885,
      "owner": "billing-service"
    }
    ```

=== "Go SDK"

    ```go
    topic, err := client.CreateTopic(ctx, "orders",
        narad.WithPartitionCount(6),
        narad.WithRetention(24*time.Hour))
    ```

    `CreateTopic` returns `narad.ErrExists` when the topic is already there. A service that creates its topics at startup should call `EnsureTopic` with the same options, which treats an existing topic as success.

=== "CLI"

    ```sh
    narad topic add orders --partitions 6 --retention 24h
    ```

    The CLI prints the created topic as JSON, with the same fields as the curl response.

- `$NARAD` and `$AUTH` are the base URL and credentials from [Connect and authenticate](connect.md). `$AUTH` needs a `create` grant that matches `orders`.
- `orders` is the topic name: 1 to 200 letters, digits, `.`, `_` or `-`; a longer name gets [`400`](../reference/status-codes.md#status-400). A name that is taken gets [`409`](../reference/status-codes.md#status-409), and so does a name that differs from an existing topic's only in letter case (`Orders` next to `orders`), because on a case-insensitive filesystem the two would share one directory. Topics an earlier release created with longer names (up to 255 bytes) keep working, but one whose name is over 230 bytes cannot become a [fan-out child](fanout-and-delay.md): the name of its fan-out cursor file would pass the filesystem's 255-byte limit.
- The user who creates a topic becomes its [owner](../reference/glossary.md#owner), shown in `owner`.

Every field the request accepts is in the [HTTP API reference](../reference/http-api.md#create-topic). The ones worth deciding up front:

### Partitions

A topic is split into [partitions](../reference/glossary.md#partition), which spread its storage and its consumers across the cluster. The count is at least 3 and at most 108 by default (the operator's `topic.max_partitions`), and it defaults to 3. Pick about as many as the consumers you expect to run in parallel. You can raise the count later but never lower it, so start modest.

### Retention

Narad deletes a message `retention_ms` after it was written, whether or not anyone consumed it. The minimum is one hour. `0` keeps messages forever, and leaving the field out gives the operator's default: 7 days for the binary, 12 hours for a cluster installed with the Helm chart. A negative value gets [`400`](../reference/status-codes.md#status-400). A message that is still unacked when retention removes it is never delivered, so size retention for your slowest consumer plus a margin for replay. The [delivery contract](../understand/delivery-contract.md#retention) has the details.

### Visibility timeout

`visibility_timeout_ms` is how long a consumed message stays hidden from other consumers before Narad hands it out again. It defaults to 30 seconds and cannot be changed after the topic is created. Set it above your usual processing time; a slow job can [extend its lease](consuming.md#extend) instead of forcing a long timeout on every message.

The same request can also give the topic a [schema](schemas.md), make it a [fan-out or delay child](fanout-and-delay.md#create-child) of another topic, and set the two per-partition limits described under [flow control](consuming.md#flow-control).

## Inspect a topic {#inspect}

```sh title="Describe the orders topic"
curl -u "$AUTH" "$NARAD/v1/topics/orders"
```

The answer is the topic's settings plus `partition_stats`: for each partition, the oldest and next offsets, its size on disk, and `owner_node`, the node that stores it. A topic with a schema also carries `schema_version` and the current `schema`.

??? note "Full response"

    ```json
    {
      "name": "orders",
      "id": "244de9d4ac35abae",
      "partitions": 6,
      "retention_ms": 86400000,
      "visibility_timeout_ms": 30000,
      "max_in_flight_per_partition": 1024,
      "max_acked_ahead_per_partition": 1024,
      "created_at": 1790623885,
      "owner": "billing-service",
      "role": "standalone",
      "schema_version": 0,
      "partition_stats": [
        {
          "index": 0,
          "segments": 0,
          "oldest_offset": 0,
          "next_offset": 0,
          "high_watermark": 0,
          "size_bytes": 0,
          "owner_node": "narad-0"
        },
        {
          "index": 1,
          "segments": 0,
          "oldest_offset": 0,
          "next_offset": 0,
          "high_watermark": 0,
          "size_bytes": 0,
          "owner_node": "narad-0"
        },
        {
          "index": 2,
          "segments": 0,
          "oldest_offset": 0,
          "next_offset": 0,
          "high_watermark": 0,
          "size_bytes": 0,
          "owner_node": "narad-0"
        },
        {
          "index": 3,
          "segments": 0,
          "oldest_offset": 0,
          "next_offset": 0,
          "high_watermark": 0,
          "size_bytes": 0,
          "owner_node": "narad-0"
        },
        {
          "index": 4,
          "segments": 0,
          "oldest_offset": 0,
          "next_offset": 0,
          "high_watermark": 0,
          "size_bytes": 0,
          "owner_node": "narad-0"
        },
        {
          "index": 5,
          "segments": 0,
          "oldest_offset": 0,
          "next_offset": 0,
          "high_watermark": 0,
          "size_bytes": 0,
          "owner_node": "narad-0"
        }
      ]
    }
    ```

- You can read a topic if you hold any grant that matches its name, own it, or are an admin. Otherwise the answer is [`403`](../reference/status-codes.md#status-403), or [`404`](../reference/status-codes.md#status-404) when the topic does not exist.
- How far behind consumers are is not in this response; the [metrics reference](../reference/metrics.md#queue-health) has the lag gauges.

### List topics

`GET /v1/topics` returns up to `limit` topics (100 by default, 1000 at most) and a `next_page_token`. Pass the token back as `page_token` to get the next page. The list holds only topics you may read, and that filter runs after each page is cut, so a page can be short or even empty while the token is still set. Keep paging until `next_page_token` is empty. The parameters are in the [HTTP API reference](../reference/http-api.md#list-topics).

## Change a topic {#alter}

```sh title="Keep orders for 48 hours instead of 24"
curl -i -u "$AUTH" -X PATCH "$NARAD/v1/topics/orders" \
  -H "Content-Type: application/json" \
  -d '{"retention_ms": 172800000}'
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 254
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:25 GMT

{
  "name": "orders",
  "id": "244de9d4ac35abae",
  "partitions": 6,
  "retention_ms": 172800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790623885,
  "owner": "billing-service",
  "role": "standalone"
}
```

What can change after creation:

| Field | Can change | Notes |
|---|---|---|
| `retention_ms` | yes | `0` keeps messages forever; to go back to the operator's default, send its value |
| `max_in_flight_per_partition`, `max_acked_ahead_per_partition` | yes | send one or both; the other keeps its value |
| `partitions` | raise only | a lower or equal count gets [`400`](../reference/status-codes.md#status-400) |
| `schema` | yes | registers a new version; see [Evolve a schema](schemas.md#evolve) |
| `visibility_timeout_ms` | no | the field is refused with [`400`](../reference/status-codes.md#status-400) |

- A request with several fields applies them one at a time, in a fixed order: retention, the per-partition limits, partitions, then schema. There is no transaction across them. The first failure stops the sequence and answers its error, and the fields before it stay applied, so send one field per request when you need all or nothing.
- A topic with [delay children](fanout-and-delay.md#delay-children) keeps at least each child's delay plus one hour of retention. A change below that gets [`409`](../reference/status-codes.md#status-409).
- A retention or per-partition limit change applies on every node that owns a partition of the topic, not only on the node that ran it. Each owner applies it to the partitions it has open within about a second of its copy of the metadata receiving the change, idle ones included, and partitions it opens later start under the new values. A raised retention therefore protects the backlog on every partition, and a lowered one frees space on every partition at the next retention pass.

## Delete a topic {#delete}

```sh title="Delete the orders topic"
curl -i -u "$AUTH" -X DELETE "$NARAD/v1/topics/orders"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:25:15 GMT
```

!!! warning "A delete cannot be undone"
    Deleting a topic removes its settings and every message it holds, on every node, and detaches its fan-out children. There is no way to get it back.

- The `204` comes once the topic's metadata is deleted. From then on the topic is gone for every client, even if a node that was down still has its files; that node removes them when it next starts.
- Consumers waiting in a long poll on the topic are answered at once, with `204` or `404`.
- Deleting a topic that is already gone gets [`404`](../reference/status-codes.md#status-404).
- With the CLI, `narad topic rm orders` asks for confirmation first; `--force` skips the question.

## Next steps

- [Produce messages](producing.md): send messages to the topic you created.
- [Enforce schemas on a topic](schemas.md): refuse payloads that do not match a JSON Schema.
- [Fan out and delay messages](fanout-and-delay.md): copy a topic's messages to other topics, now or later.
