# Handling Retries

Narad deliberately does not have a retry engine. It gives you three durable primitives — visibility leases, acks, and topics — and every retry policy you've ever wanted composes out of them **in your consumer**, where you can see it, log it, and change it without a broker upgrade. This page is the complete menu.

One rule before the menu: **your handler must be idempotent.** Crashes, timeouts, nacks, and network blips all cause redelivery — that's the at-least-once contract. Every pattern below assumes processing the same message twice is safe. (Deduplicate on a business ID in the payload if it isn't.)

## Pattern 0: do nothing — the lease is already a retry

If your consumer crashes, hangs, or just never acks, the message becomes visible again after the topic's `visibility_timeout_ms` (default 30s) and someone else gets it. You wrote zero lines of retry code and you already survive process death.

- Retry spacing: constant, = the visibility timeout.
- Attempts: unbounded — a permanently failing message loops forever until a human notices. Fine for prototypes; keep reading for production.

## Pattern 1: sync retries — for blips

The dependency timed out once? Just call it again, in-process, while you still hold the lease:

```text
for attempt in 1..3:
    if handle(msg) succeeds: ack; done
ack-with-extend or fall through to a requeue (below)
```

Cheap, immediate, and invisible to the broker. If your processing time approaches the visibility window, heartbeat while you retry: `POST /ack?receipt_handle=$H&extend=true` restarts your lease.

Sync retries solve *transient* failures. They do nothing for "this message will fail for the next ten minutes" — that needs one of the patterns below.

## Pattern 2: nack — retry soon, on someone else

`POST /ack?receipt_handle=$H&extend=0` gives the message back immediately: the lease drops, the message is available again on the next consume. Use it when *this worker* is the problem (shutting down, missing a local dependency) rather than the message.

Same caveat as pattern 0: the redelivered message is byte-identical, so nothing counts attempts for you. Which brings us to the load-bearing pattern:

## Pattern 3: the envelope — bounded retries, your way

Wrap your payload at produce time:

```json
{"payload": {"order_id": "ord_123"}, "delivery_count": 0}
```

On failure, **produce a new copy with `delivery_count + 1`, then ack the original** — in that order. A crash between the two steps yields a duplicate (idempotency absorbs it), never a loss. The reversed order can lose the message; don't reverse it.

```text
on failure:
    if msg.delivery_count >= MAX:  produce to jobs-dlq;  ack;  done
    produce {payload, delivery_count+1} to the same topic;  ack
```

What this buys you:

- **Bounded attempts** — the counter travels with the message, no broker state needed.
- **A dead-letter queue** — `jobs-dlq` is just a topic you create. Alert on its depth, inspect it with [replay](consuming.md), re-produce from it to the main topic when the bug is fixed. No special broker feature, full API available on it.
- **No head-of-line blocking** — the failed message re-enters at the tail; the partition's frontier moves on immediately.
- **Evolvable policy** — want per-error-type max attempts? Different DLQs per failure class? It's your JSON and your code.

This is the pattern the classic worker frameworks (Sidekiq, and the SQS redrive policy itself) implement — the counter just lives in the message instead of the broker.

## Pattern 4: spaced backoff — the retry-topic pair

Requeue-at-tail retries again *immediately* (an empty queue redelivers in milliseconds). When failures need breathing room — a rate-limited API, a database failing over — use Narad's native delay machinery as your backoff:

```text
jobs-retry            ← a parent topic you produce retries into
  └── jobs-retry-30s  ← its delayed child (fanout_delay_ms: 30000)
```

Your consumers consume `jobs` **and** `jobs-retry-30s`. A failed message gets produced (envelope counter incremented) to `jobs-retry`, and its copy becomes consumable exactly 30 seconds later — durably, surviving restarts of everything, no in-process timers holding your fate. Want tiers? Attach `jobs-retry-2m` and `jobs-retry-10m` children to matching parents and pick by `delivery_count`. Fixed tiers, not arbitrary per-message delays — in practice two or three tiers cover every real policy.

## The one discipline that is not optional: retry your acks

An ack can fail — a node restarting, or a `503` because the partition's out-of-order ack window (`max_acked_ahead_per_partition`) is momentarily full. **A failed ack is not a failed job.** Retry the ack itself with a short backoff until it returns `204` or `410`.

We say this from experience: our own long-running soak once ignored failed acks, and the visibility timeout dutifully redelivered 623,000 duplicates over seven hours. The broker was fine. The messages were fine. The harness just didn't retry a `503`. Retry your acks.

(`410 Gone` on ack means the lease already lapsed and the message was handed to someone else — that's not an error to retry, it's Narad telling you the work may run twice. Idempotency, again.)

## Choosing, quickly

| You want | Use |
|---|---|
| Survive crashes | Nothing — leases do it |
| Ride out a blip | Sync retries (+ `extend=true` heartbeat) |
| "Not me, try another worker" | Nack (`extend=0`) |
| Bounded attempts + DLQ | Envelope counter + requeue-and-ack |
| Backoff between attempts | Retry-topic pair with delayed children |
| Recover after fixing a bug | Replay the DLQ, re-produce |

Compose freely: production setups typically run sync retries → envelope requeue with a 30s retry tier → DLQ after 5, and alert on DLQ depth. Total broker features required: the ones on this site already.
