---
description: "Keep a second copy of a topic on other nodes, and restore a node from a volume snapshot when its disk is lost."
---

# Back up and replicate topics

Keep a second copy of a topic on other nodes, and restore a node from a volume snapshot when its disk is lost.

Before you start: for a replica, a user who may manage the topic and holds a `create` grant matching the replica's name; for snapshots, a CSI driver that supports Kubernetes VolumeSnapshots.

--8<-- "contract/one-copy.md"

## Add a replica child {#replica-children}

A [replica child](../reference/glossary.md#replica-child) is a fan-out child created in the same call as its link to the parent. It receives a full copy of every record the parent commits from then on. When it is created on a cluster of two or more nodes, each of its partitions is placed on a different node from the parent's partition with the same index.

```bash
curl -s -u "$AUTH" -X POST "$NARAD/v1/topics" \
  -H "Content-Type: application/json" \
  -d '{"name": "orders-replica", "parent": "orders"}' | jq .
```

```json title="Response"
{
  "name": "orders-replica",
  "id": "b8c656c46a79c139",
  "partitions": 6,
  "retention_ms": 604800000,
  "visibility_timeout_ms": 30000,
  "max_in_flight_per_partition": 1024,
  "max_acked_ahead_per_partition": 1024,
  "created_at": 1790624743,
  "owner": "admin",
  "role": "child",
  "parent": "orders",
  "attach_epoch": "a46b6a6ecd0bf8e7",
  "attach_offsets": [
    19,
    10,
    9,
    11,
    8,
    18
  ]
}
```

- `$NARAD` is the base URL of any node or of the load balancer.
- `$AUTH` is `username:password` of a user who may manage `orders` and create `orders-replica`.

Leave out `partitions` so the child inherits the parent's count; the placement relies on it. `attach_offsets` shows where the copy starts in each parent partition: records committed before the create are not copied. The child is an ordinary topic, so you can give it a longer `retention_ms` than the parent and keep it as an archive. How fan-out children work is in [Fan out and delay messages](../build/fanout-and-delay.md#create-child).

### Check replica placement {#check-placement}

Compare the owner of each partition in the two topics:

```bash
curl -s -u "$AUTH" "$NARAD/v1/topics/orders" \
  | jq -c '.partition_stats[] | {index, owner_node}'
curl -s -u "$AUTH" "$NARAD/v1/topics/orders-replica" \
  | jq -c '.partition_stats[] | {index, owner_node}'
```

```text title="Output"
{"index":0,"owner_node":"narad-2"}
{"index":1,"owner_node":"narad-2"}
{"index":2,"owner_node":"narad-2"}
{"index":3,"owner_node":"narad-0"}
{"index":4,"owner_node":"narad-2"}
{"index":5,"owner_node":"narad-2"}
{"index":0,"owner_node":"narad-0"}
{"index":1,"owner_node":"narad-1"}
{"index":2,"owner_node":"narad-0"}
{"index":3,"owner_node":"narad-1"}
{"index":4,"owner_node":"narad-1"}
{"index":5,"owner_node":"narad-2"}
```

In this run on a three-node test cluster, partition 5 of both topics ended up on `narad-2`. The create placed the child's partitions away from the parent's, but it also left the nodes unevenly loaded, and the rebalance that followed moved partition 5 of the child onto `narad-2`. A rebalance prefers to keep a child's partition off its parent's node, but only when an equally balanced move exists; even load wins. Scale-outs, scale-ins and decommissions can do the same.

So check the placement after you create a replica and after every change to the cluster's shape. Narad has no command to move a single partition. A partition whose two copies share a node is protected against everything except the loss of that node's volume.

### Replica limits {#replica-limits}

- **The copy is asynchronous.** It trails the parent by the fan-out lag, usually under a second (`narad_fanout_lag_messages` in the [Metrics reference](../reference/metrics.md#fan-out)). Records the parent committed but had not yet copied are lost with the parent's volume.
- **Nothing fails over on its own.** If the parent's node is lost for good, point consumers at `orders-replica` yourself. They receive every message the replica still holds, including ones already handled through the parent, because the replica has its own consumer position. Handlers must be idempotent anyway.
- **It costs what a second topic costs:** twice the disk and twice the write I/O, for the topics that have a replica.
- **Create it in one call.** A child created on its own and attached later keeps the placement it got at creation, which ignores the parent.
- **Some records can share a node.** A record that does not sit on the partition its key hashes to (produced with an explicit `partition`, or moved to a sibling while its owner was unreachable) is copied by key, to another index, so its two copies can share a node. A message without a key keeps its parent partition's index (from v3.1.0; v3.0.1 gives it a generated key, which behaves like any other key).

## Take volume snapshots {#volume-snapshots}

Each pod keeps everything on its volume, the claim `data-narad-<n>`: partition logs, consumer positions, the ingress WAL and its copy of the cluster metadata. A volume snapshot is crash-consistent, so restoring one looks to Narad like the node lost power at the moment of the snapshot. Every message the node answered `202` for before that moment was already on disk.

Snapshot each pod's claim on a schedule, for example with a VolumeSnapshot like this one:

```yaml title="snapshot-narad-2.yaml"
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: data-narad-2-20260929
  namespace: narad
spec:
  volumeSnapshotClassName: csi-snapclass
  source:
    persistentVolumeClaimName: data-narad-2
```

```bash
kubectl apply -f snapshot-narad-2.yaml
```

Change `csi-snapclass` to a VolumeSnapshotClass your cluster has (`kubectl get volumesnapshotclass`).

## Restore a node from a snapshot {#restore}

Restore when a node's volume is lost or damaged. These steps replace the volume of `narad-2`; the other pods keep running.

!!! warning "A restore rolls the node back"
    The node's partitions return to the moment of the snapshot. Messages produced to them after it are lost, and messages acked after it are delivered again. A partition that moved onto the node after the snapshot comes back empty.

1. Remove the StatefulSet but leave its pods running:

    ```bash
    kubectl delete statefulset narad -n narad --cascade=orphan
    ```

2. Delete the pod and its claim:

    ```bash
    kubectl delete pod narad-2 -n narad
    kubectl delete pvc data-narad-2 -n narad
    ```

3. Create a claim with the same name from the snapshot. Match the size to `persistence.size`, and add `storageClassName` if you do not use the default class:

    ```yaml title="restore-narad-2.yaml"
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: data-narad-2
      namespace: narad
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 50Gi
      dataSource:
        apiGroup: snapshot.storage.k8s.io
        kind: VolumeSnapshot
        name: data-narad-2-20260929
    ```

    ```bash
    kubectl apply -f restore-narad-2.yaml
    ```

4. Recreate the StatefulSet. It adopts the running pods and starts `narad-2` on the restored claim:

    ```bash
    helm upgrade narad ./charts/narad -n narad --reuse-values
    kubectl rollout status statefulset/narad -n narad
    ```

The node rejoins with the metadata it had at the snapshot, and the Raft leader brings it up to date before it reports ready. Outside Kubernetes, the same restore is: stop the node, replace its data directory with the snapshot's contents, and start it.

## Next steps

- [Fan out and delay messages](../build/fanout-and-delay.md): children for other purposes, such as delayed retries.
- [Delivery contract](../understand/delivery-contract.md#failure-matrix): what each kind of failure can lose.
- [Scale out and in](scaling.md): recheck replica placement after the cluster changes.
