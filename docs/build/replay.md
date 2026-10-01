---
description: "Read a partition's retained history from any offset without taking leases, so the consumers doing real work never notice."
---

# Replay messages from an offset

Read a partition's retained history from any offset without taking leases, so the consumers doing real work never notice.

Before you start: your user needs a `consume` grant on the topic. To choose where to start, read each partition's `oldest_offset` and `next_offset` from [Inspect a topic](topics.md#inspect).

## Replay a partition {#replay-partition}

=== "curl"

    ```sh title="Read the message at offset 0 of partition 4"
    curl -i -u "$AUTH" \
      "$NARAD/v1/topics/orders/consume?partition=4&offset=0"
    ```

    ```http title="Response"
    HTTP/1.1 200 OK
    Content-Length: 137
    Content-Type: application/json
    Date: Mon, 28 Sep 2026 19:22:52 GMT

    {
      "topic": "orders",
      "partition": 4,
      "offset": 0,
      "key": "customer-42",
      "payload": {
        "order_id": "ord_123",
        "amount": 4999
      },
      "timestamp": 1790623277
    }
    ```

=== "Go SDK"

    ```go
    err := client.ReadFrom(ctx, "orders", 4, 0, narad.HandlerFunc(
        func(ctx context.Context, msg *narad.Message) error {
            return audit(msg)
        }))
    ```

    `ReadFrom` streams partition 4 from offset 0 to the current end of the log, skips offsets that have aged out, and stops. `client.ReadAt(ctx, "orders", 4, 0)` reads a single offset.

=== "CLI"

    ```sh
    narad replay orders --partition 4
    ```

    ```text title="Output"
    [p4 @0] key=customer-42 03:06:49 {"order_id": "ord_123", "amount": 4999}
    [p4 @1] key=customer-42 03:07:03 {"order_id": "ord_124", "amount": 1250}
    [p4 @2] key=customer-42 03:07:04 hello world
    [p4 @3] key=customer-42 03:07:04 (binary, 8 bytes)
    89504e470d0a1a0a
    [p4 @4] key=customer-42 03:07:05 {"order_id": "ord_125", "amount": 1250}
    5 message(s) replayed from p4 [0, 5)
    ```

    `--from` and `--to` bound the range; `--to` is exclusive and defaults to the current end of the log. `--raw` prints payloads only.

A consume with both `partition` and `offset` is a replay read: it returns the one message at that position and changes nothing.

- There is no lease and no `receipt_handle`, so there is nothing to ack. The message stays exactly where it was: a message not yet consumed is still delivered to your consumers, and one already acked stays settled.
- To read on, ask for the next offset. The same offset read twice returns the same message.
- An offset at or past the end of the log gets [`204`](../reference/status-codes.md#status-204) at once. A replay does not long-poll; `wait` is ignored.
- An offset that retention already deleted gets [`410`](../reference/status-codes.md#status-410). The error message ends with `aged out of retention (oldest retained: N)`, where `N` is the oldest offset still kept; skip forward to it. The Go SDK reports this case as `narad.ErrOffsetGone`, and `narad replay` skips forward by itself.
- `offset` without `partition` gets [`400`](../reference/status-codes.md#status-400) (`partition required for replay-mode consume`), and so does `offset` together with `max`, because a replay reads one message.
- Offsets count per partition, so a replay covers one partition at a time and there is no order across partitions.

The parameters are in the [HTTP API reference](../reference/http-api.md#consume).

## Replay a whole topic {#replay-topic}

The Go SDK's `Replay` reads every partition the topic has, one after another, oldest first, and stops at the end of each:

```go
err := client.Replay(ctx, "orders-dlq", narad.HandlerFunc(
    func(ctx context.Context, msg *narad.Message) error {
        log.Printf("p%d @%d %s", msg.Partition, msg.Offset, msg.Text())
        return nil
    }))
```

It reads what exists when it starts, so it finishes on a busy topic instead of following new writes. With curl or the CLI, replay each partition in turn; the topic's `partitions` field says how many there are.

## Watch a topic live {#peek}

`narad sub --peek` tails every partition of a topic with replay reads, starting at the current end, and prints each new message as it is committed:

```sh
narad sub orders --peek
```

```text title="Output"
[p0 @0] 03:07:19 {"hello":"narad"}
[p1 @0] 03:07:19 {"hello":"narad"}
[p2 @0] 03:07:19 {"hello":"narad"}
[p4 @5] key=customer-42 03:07:19 {"order_id": "ord_126", "amount": 300}
[p3 @0] 03:07:20 {"hello":"narad"}
[p4 @6] 03:07:20 {"hello":"narad"}
```

- The CLI also prints a status line when it starts and a message count when you stop it with Ctrl-C.
- Nothing is reserved or acked, so your consumers still receive every message. It is the tool for "what is flowing through this topic right now?"
- `--partition 4 --from 0` starts in history instead of at the end.
- Without `--peek`, `narad sub` is a real consumer: it acks what it prints and competes with your workers.

## Next steps

- [Handle retries and dead letters](handling-retries.md#dead-letter): inspect a dead-letter topic with replay before you re-produce it.
- [Consume and acknowledge messages](consuming.md): the lease-taking consume that replay leaves alone.
- [CLI command reference](../reference/cli.md#replay): every flag of `narad replay` and `narad sub`.
