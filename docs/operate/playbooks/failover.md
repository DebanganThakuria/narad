---
description: "Move producers and consumers to the recovery cluster when the cluster in use, or its region, is lost."
---

# Fail over to the recovery cluster

Move producers and consumers to the recovery cluster when the cluster in use, or its region, is lost.

**Unreleased:** in master, not in v3.1.0.

Before you start: a remote child from the cluster in use, `a`, to the recovery cluster, `b`, set up as in [Set up disaster recovery](disaster-recovery.md), and `a`'s metrics stored outside its region.

## Fail over {#steps}

1. **Decide that `a`'s region is lost, not just the link.** A link that is down while `a` still serves its clients is a lag incident: see [Troubleshooting](../troubleshooting.md#remote-link-stalled), not this page.

2. **Record the data at risk:** the last stored `narad_fanout_remote_lag_seconds` for the link plus one scrape interval, and the last `narad_ingress_dispatch_backlog_records` of each `a` node. That is what `b` may never receive.

3. **Choose the fence.**

    - If `a`'s disks may come back intact, keep `a`'s replicator user on `b`. When `a` returns, its cursors resume from where they stopped and ship the unshipped tail to `b`.
    - If `a` may come back as a stale copy, from an old backup or a clone, delete that user on `b` first (`narad --ctx b user rm repl-from-a-7f3k9q`), so nothing `a` sends can reach `b`.

4. **Mind a split brain.** If `a`'s region is cut off rather than gone, its producers and consumers may still be running. Stop `a`'s consumers by any path you have. If you cannot, the unshipped tail will be processed in both regions once the regions can talk again.

5. **Start `b`'s consumers.** They begin at `b`'s oldest retained record, because nothing was ever acked on `b`: expect duplicates up to `b`'s retention window.

6. **Move the producers to `b`.**

7. **Keep `a`'s consumers stopped** when `a` returns, until you [fail back](failback.md). `a` still holds its backlog from before the failure, which `b` has processed.

## When `a` returns {#a-returns}

With the fence kept, `a`'s cursors resume from their files and ship the tail: watch `narad --ctx a topic children orders` until `lag_messages` reaches 0. Producers that still write to `a` reach `b` through the same link. Then [fail back](failback.md), or keep running on `b` and detach the link once `a` is quiet.

## Next steps

- [Fail back to the original cluster](failback.md): return to `a` without processing its backlog twice.
- [Delivery contract](../../understand/delivery-contract.md): what each kind of failure can lose.
