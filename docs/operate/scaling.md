---
description: "Add nodes to a running Narad cluster, or drain nodes and remove them, without stranding any partition."
---

# Scale out and in

Add nodes to a running Narad cluster, or drain nodes and remove them, without stranding any partition.

Before you start: Helm access to the release, every pod ready, and the `narad` CLI with admin credentials in its active context or in `NARAD_ADDR`, `NARAD_USER` and `NARAD_PASS` ([CLI command reference](../reference/cli.md#cluster)).

## Check members and moves {#check-members}

Two commands show where partitions live and what is moving:

```bash
narad cluster members
```

```json title="Output"
{
  "members": [
    {
      "id": "narad-0",
      "addr": "127.0.0.1:7970",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0
    },
    {
      "id": "narad-1",
      "addr": "127.0.0.1:7972",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0
    },
    {
      "id": "narad-2",
      "addr": "127.0.0.1:7974",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0
    }
  ]
}
```

This output comes from a three-node test cluster on one machine; on Kubernetes, `addr` holds each pod's address.

- `status` is `alive`, or `dead` once a node has sent no heartbeat for about 30 seconds.
- `draining` is `true` while a decommission sheds the node's partitions.
- `owned_partitions` counts the [partitions](../reference/glossary.md#partition) the node owns, and `outbound_moves` those it is copying to another node.

`narad cluster moves` lists the partitions being moved right now, and prints an empty `moves` list when nothing moves. Both commands need the `admin` grant.

## Scale out {#scale-out}

Raise `replicaCount`:

```bash
helm upgrade narad ./charts/narad -n narad --reuse-values \
  --set replicaCount=5
```

Each new pod starts empty and finds it is not one of the initial members, so it asks the leader to admit it instead of creating a cluster of its own. It does not need the leader in its peer list: a follower's answer names the leader, and the new pod asks it next, so this works after leadership has moved to a pod outside the pinned list. The existing pods are not restarted: the peer list in their pod template stays pinned to the initial members.

The leader admits a new pod as a Raft non-voter: it replicates the cluster's metadata and serves traffic, but does not count toward quorum, so a pod that cannot be reached costs the cluster nothing. Once its copy has caught up it asks again, and the leader promotes it to voter, normally within seconds (and never before the leader has led for 12 s). Until then `narad_raft_nonvoters` is above 0. If it stays above 0, the new pod logs why the leader defers its promotion ([Troubleshooting](troubleshooting.md#nonvoters-stay)).

Once a new node is admitted, the leader rebalances. It computes the fewest partition moves that even out the number of partitions per node, copies each partition to its new owner, and switches ownership over at the end. At most 8 moves run at once, so a large rebalance drains gradually. Watch it with `narad cluster moves` until the list is empty. How a move copies and hands over a partition is in [Rebalance and decommission](../understand/rebalance.md).

If Helm refuses with a conflict on `.spec.replicas`, someone changed the replica count outside Helm: see [Troubleshooting](troubleshooting.md#helm-field-manager-conflict).

## Scale in {#scale-in}

--8<-- "contract/one-copy.md"

A pod removed while it still owns partitions strands them: they stay assigned to a node that no longer runs, and their messages cannot be consumed until it returns. The removed pod also stays a Raft voter, so the cluster tolerates one fewer failure. So scaling in is two steps: [decommission](../reference/glossary.md#decommission) the node until it owns nothing, then lower `replicaCount`. The StatefulSet always removes the highest-numbered pods, so decommission those.

### Decommission a node {#decommission}

<figure class="nr-dia nr-dia--doc" id="fig-scaling-decommission">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/scaling-decommission.html"
</div>
<figcaption>Drain first, delete second: <code>narad-4</code> hands every partition it owns to the other pods and leaves the Raft voters at <code>owned_partitions: 0</code>, and only then does a lower <code>replicaCount</code> remove the pod. The four moves are the ones from the example below.</figcaption>
</figure>

This example takes a five-node cluster down to four.

1. Mark the highest-numbered node for decommission:

    ```bash
    narad cluster decommission narad-4
    ```

    The node stops receiving partitions and its partitions start moving to the others. While they move, `narad cluster moves` lists them:

    ```json title="Output"
    {
      "moves": [
        {
          "topic": "orders",
          "partition": 4,
          "from": "narad-4",
          "to": "narad-0"
        },
        {
          "topic": "orders",
          "partition": 9,
          "from": "narad-4",
          "to": "narad-1"
        },
        {
          "topic": "orders",
          "partition": 14,
          "from": "narad-4",
          "to": "narad-2"
        },
        {
          "topic": "orders",
          "partition": 19,
          "from": "narad-4",
          "to": "narad-3"
        }
      ]
    }
    ```

    This output comes from a five-node test cluster on one machine, with one topic of 20 partitions.

2. Wait until `narad cluster members` shows `owned_partitions: 0` for `narad-4`. The leader then removes it from Raft (as a voter, or as a non-voter if it was never promoted), and it drops out of the list. The pod keeps running, reports not ready, and its heartbeats are refused.

3. Lower `replicaCount`. `allowScaleIn=true` tells the chart the pods it removes were decommissioned; without it, the chart refuses to lower the replica count of a running StatefulSet.

    ```bash
    helm upgrade narad ./charts/narad -n narad --reuse-values \
      --set replicaCount=4 \
      --set allowScaleIn=true
    ```

To stop a decommission before it finishes, run `narad cluster decommission narad-4 --cancel`. The node starts receiving partitions again, and the next rebalance evens the load out.

Keep these rules while you scale in:

- **Wait for zero partitions.** Lowering `replicaCount` before the node owns nothing deletes a pod whose data has not moved.
- **Do not overlap a decommission with a rolling restart.** A `helm upgrade` that changes the pod template restarts the pods, and a draining node that restarts has no stable source to copy from until it settles. Changing only `replicaCount` does not restart the pods.
- **A rollback of `replicaCount` is a scale-in.** `helm rollback` to a revision with fewer replicas deletes pods exactly like step 3, without steps 1 and 2. The chart refuses it unless `allowScaleIn` is set; decommission first rather than setting it to get past the refusal.
- **Three voters is the floor.** The leader never removes a voter from Raft if that would leave fewer than three voters. A decommission on a three-node cluster moves the partitions away, but the node stays a member. If the leader itself is decommissioned, it hands leadership to another node first. A node that is still a non-voter has no vote, so the floor does not hold it back.
- **Finish a rolling upgrade from 3.0.x before you decommission a non-voter.** A 3.0.x leader removes only voters, so a non-voter it decommissions loses its member record but stays in the Raft configuration, and `narad_raft_nonvoters` stays above 0 ([Troubleshooting](troubleshooting.md#nonvoters-stay)).

### Reuse a decommissioned name {#reuse-name}

The leader remembers a decommissioned node's ID. If that pod restarts with its old volume, it is refused and logs that it was decommissioned ([Troubleshooting](troubleshooting.md#log-join-rejected)). To bring a pod back under that name later, for example when you scale out again, delete its PersistentVolumeClaim first so it starts empty:

```bash
kubectl delete pvc data-narad-4 -n narad
```

A node that starts with an empty data directory under a decommissioned ID is admitted again as a new node.

### Source node failure during a move {#source-dies}

If a node dies while its partitions are still being copied away, the destinations that had caught up with it promote their copies after 2 minutes instead of waiting for it. The 2 minutes count from the source's last heartbeat and, on master, also on each destination's own clock from when it first saw the source dead, so a destination that restarts meanwhile waits up to 2 minutes more. A destination whose copy is behind cannot promote it: it waits for the source, logs that once at error and counts the move in `narad_moves_blocked` ([Troubleshooting](troubleshooting.md#moves-blocked)). This [force-promote](../reference/glossary.md#force-promote) can deliver again messages that consumers acked on the dead node in its last moments. On master it never skips one; on v3.0.1 a force-promote could leave records committed after it undelivered (see [Rebalance and decommission](../understand/rebalance.md#what-if-the-source-dies-mid-move)). What can be lost, and why, is in the [failure matrix](../understand/delivery-contract.md#failure-matrix).

## Next steps

- [Rebalance and decommission](../understand/rebalance.md): how a partition moves without losing a record.
- [Back up and replicate topics](backups.md): check replica placement after the cluster changes shape.
- [Capacity and disk sizing](../reference/capacity.md): decide how many nodes the load needs.
