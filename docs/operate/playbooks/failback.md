---
description: "Return producers and consumers to the original cluster after a failover, without processing its old backlog twice and without a replication loop."
---

# Fail back to the original cluster

Return producers and consumers to the original cluster after a failover, without processing its old backlog twice and without a replication loop.

**Unreleased:** in master, not in v3.1.0.

Before you start: you [failed over](failover.md) from `a` to `b`, `a` is back, and the remote `a` on `b` with its user `repl-from-b-2m8x4d` on `a` exist, as in [Set up disaster recovery](disaster-recovery.md#setup).

The target of a remote child must not have remote children of its own, and every link checks this when it is attached and every `check_interval_ms` after. So the link from `b` back to `a` cannot be attached while `a`'s `orders` still has its link to `b`, and the order below never has both directions at once. A link that finds its target has a remote child stops in `target_has_remote_children` without sending.

## Fail back {#steps}

1. **Prove `a` is quiet.** `rate(narad_messages_produced_total{topic="orders"}[10m])` is 0 on `a`, and `narad_ingress_dispatch_backlog_records` is 0 on every `a` node.

2. **Drain and detach the link from `a` to `b`:**

    ```bash
    narad --ctx a topic wait orders orders-dr --lag-zero --stable 60s
    narad --ctx a topic detach orders orders-dr
    ```

    `a`'s `orders` now has no remote children.

    **If you fenced `a` by deleting its replicator user on `b`** ([failover step 3](failover.md#steps)), the link cannot drain: it stays in `auth_failed` with lag above 0, `topic wait` exits 2 at once (`stalled: state auth_failed`), and a plain detach answers `409` (unshipped records). That tail is the stale copy the fence keeps away from `b`, so abandon it:

    ```bash
    narad --ctx a topic detach orders orders-dr --force
    ```

    The leader's audit line for `remote_child.delete` records `forced=true` and what was abandoned (`abandoned_lag_messages`, `abandoned_dispatch_backlog`).

3. **Clear `a`'s old backlog.** Delete and recreate `a`'s `orders` with the same partitions, retention and schema. Everything in it was either processed on `a` before the failure or is already on `b`; without this step, `a`'s consumers would process its backlog a second time. Narad has no purge, so delete and recreate is the tool. Do it only after step 1: anything produced to `a` after the detach would be lost.

4. **Move the topic back with the offload playbook,** with the roles swapped ([Move a topic to another cluster](offload.md)): stop `b`'s consumers, attach from `b` at its consumer frontier, start `a`'s consumers, move the producers to `a`, prove `b` quiet, and detach.

    ```bash
    narad --ctx b topic attach orders orders-to-a --remote a --from unconsumed
    ```

5. **Restore disaster recovery.** Optionally delete and recreate `b`'s `orders` first: otherwise the records `b`'s consumers processed age out only after `b`'s retention, and a failover inside that window processes them again. Then attach the link from `a` to `b` as in [Set up disaster recovery](disaster-recovery.md#setup), step 4. The new attach records the recreated target's new ID.

## Both regions producing {#active-active}

Two clusters that produce to the same topic and copy it to each other form a cycle, and the loop rule refuses it. What works is one topic per origin: `a`'s `orders-a` copied to `b`'s `orders-a`, and `b`'s `orders-b` copied to `a`'s `orders-b`, with the consumers in each region reading both. Neither target has remote children, so the rule allows it.

## Next steps

- [Set up disaster recovery](disaster-recovery.md): the link, the retention and the alerts.
- [Manage remotes](../remotes.md#rotate-password): rotate the idle pair of credentials too.
