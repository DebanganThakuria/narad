---
description: "Move a busy topic, its producers and its consumers to another Narad cluster without losing a message, using a remote child as the bridge."
---

# Move a topic to another cluster

Move a busy topic, its producers and its consumers to another Narad cluster without losing a message, using a remote child as the bridge.

**Unreleased:** in master, not in v3.1.0.

Before you start: every item of [Before you start](../remotes.md#before-you-start) on the source cluster `a` and the target cluster `b`, the CLI with a context for each, and a way to stop and start the topic's consumers and to repoint its producers.

The plan: the source keeps taking produces while a [remote child](../../reference/glossary.md#remote-child) copies everything its consumers have not acked yet to the target, the target's consumers take over, and producers move at their own pace. Stragglers still writing to the source reach the target through the link. Once the source is quiet, the link goes.

## Steps {#steps}

1. **Prepare the target.** Create the topic with the source's schema byte for byte, its consumers' and producers' users, and the replicator user ([Prepare the target](../remotes.md#prepare-target)):

    ```bash
    narad --ctx a topic schema orders --current > orders.schema.json
    narad --ctx b topic add orders --partitions 24 --retention 72h \
      --schema @orders.schema.json
    narad --ctx b user add repl-from-a-7f3k9q --grant produce:orders \
      --user-password-stdin < repl-password
    ```

    Leave out `--schema` when the source has none. Undo: delete them.

2. **Register the target as a remote** and check it ([Register a remote](../remotes.md#register)):

    ```bash
    narad --ctx a remote add b --url https://narad-b.example.com \
      --username repl-from-a-7f3k9q --remote-password-stdin < repl-password
    narad --ctx a remote ls
    narad --ctx a remote test b --topic orders --source orders
    ```

    `remote ls` must show every node `ready`, with the remote's fingerprint and `credential_version`, and `remote test` must exit `0`. Then delete `repl-password`. Undo: `narad --ctx a remote rm b`.

3. **Buffer.** Raise the source's retention to cover the migration plus the longest target outage you accept: `narad --ctx a topic edit orders --retention 72h`. While the link exists, the source's retention cannot go below 24 hours.

4. **Stop the source's consumers** gracefully: they finish what they hold and take nothing new. Producers keep writing to the source.

5. **Attach at the consumer frontier.** Check first, then attach:

    ```bash
    narad --ctx a topic attach orders orders-to-b --remote b \
      --from unconsumed --dry-run
    narad --ctx a topic attach orders orders-to-b --remote b \
      --from unconsumed
    ```

    Read the start offsets and any warnings in the dry run, and leave at least 5 seconds between the dry run and the attach: each node checks one remote at most once every 5 seconds, and an attach sent sooner answers `429` (retry after its `Retry-After`). From now on, everything not yet acked on the source, and everything produced to it later, flows to the target.

6. **Start the target's consumers.** Watch the link with `narad --ctx a topic children orders` (`running`, lag near 0) and the target's consumer lag.

7. **Move the producers** to the target, at any pace. Records the stragglers still send to the source reach the target through the link.

8. **Prove the source is quiet.** All of these must hold:

    - `rate(narad_messages_produced_total{topic="orders"}[10m])` is 0 on every source node.
    - `narad_ingress_dispatch_backlog_records` is 0 on every source node. Records a node accepted but has not committed yet are invisible to the link's lag; the gauge counts them (and may count a few already committed, so 0 proves the point).
    - `narad --ctx a topic wait orders orders-to-b --lag-zero --stable 60s` exits `0`.
    - `narad_fanout_child_dropped_messages` and `narad_fanout_remote_skipped_records_total` for `orders-to-b` have not moved since step 5.

9. **Detach the link, without `--force`:** `narad --ctx a topic detach orders orders-to-b`. The detach checks the lag and every node's dispatch backlog again; a refusal means something is still unshipped, so go back to step 8.

10. **Clean up.** Delete the replicator user on the target, which also fences any source node that still holds the credential. Then `narad --ctx a remote rm b`, and delete the source topic after a grace period.

## Why nothing is lost {#why}

Every source record below the consumer frontier was acked on the source, so it was processed. Every record at or above it is copied with commit before advance, and the target's `202` is the same delivery promise as any produce's. Records produced straight to the target are on the target. Nothing ages out as long as the retention headroom never reaches 0, and the [headroom alert](../monitoring.md#remote-alerts) pages first.

Duplicates: messages the source's consumers had processed but not acked when they stopped, any message the target stores twice after a resent request, and anything the target's consumers process twice in their own failures. Consumers must be idempotent, as on any Narad topic.

## Roll back before step 9 {#roll-back}

1. Move the producers back to the source. The target's consumers keep running, and the link keeps feeding them.
2. Once the target takes no produces of its own and its consumer lag is 0, stop the target's consumers and start the source's. The source's frontier has not moved since step 4, so they process again what the target already processed: duplicates, no loss.
3. Detach the link.

## Without stopping the consumers {#no-pause}

Attach with `--from attach` while the source's consumers keep running, then wait until they have acked past the link's start on every partition:

```bash
narad --ctx a topic wait orders orders-to-b --source-drained
```

Then start the target's consumers and stop the source's. Duplicates: every record after the attach that the source's consumers processed before the switch. Continue from step 7.

## Next steps

- [Set up disaster recovery](disaster-recovery.md): keep a copy on another cluster all the time.
- [Replicate a topic to another cluster](../../build/remote-children.md): every field of the attach and the listing.
- [Troubleshooting](../troubleshooting.md#remote-link-stalled): a link that does not reach `running`.
