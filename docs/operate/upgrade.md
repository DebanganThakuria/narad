# Upgrade Narad

Move a cluster to a new Narad release with a rolling restart, and roll it back safely if you need to.

Before you start: every pod ready, Helm access to the release, and the [changelog](https://github.com/DebanganThakuria/narad/blob/master/CHANGELOG.md) entries for every release between yours and the target.

## Upgrade {#upgrade}

1. Get the chart of the target release. In a clone made as in [Deploy on Kubernetes](deploy-kubernetes.md#install):

    ```bash
    git fetch --depth 1 origin tag v3.0.1
    git checkout v3.0.1
    ```

2. Read the [version notes](#version-notes) for every release you cross.

3. Upgrade, pinning the new image tag:

    ```bash
    helm upgrade narad ./charts/narad -n narad \
      --reset-then-reuse-values \
      --set image.tag=v3.0.1
    kubectl rollout status statefulset/narad -n narad
    ```

    `--reset-then-reuse-values` (Helm 3.14 or later) keeps your settings and gives any value the new chart adds its default. If you keep a values file, change `image.tag` there and pass it with `-f` instead.

The StatefulSet restarts one pod at a time, from the highest-numbered pod down, and waits for each to be ready before it restarts the next. A leader that stops hands leadership to another node first and logs `leadership transferred before shutdown`. While a pod restarts, the other nodes keep accepting produces. Records meant for the restarting pod's partitions wait in the accepting node's ingress WAL, and go to other partitions of the topic if the pod stays away for more than a few seconds. Consumers wait for that pod's partitions, and some messages can be delivered twice. The [failure matrix](../understand/delivery-contract.md#failure-matrix) has the details for a rolling restart.

What is stable across releases, and what the docs describe, is in [API stability and versions](../reference/api-stability.md).

## Roll back {#roll-back}

A rollback is the same upgrade with the older tag, using the chart of the release you go back to:

```bash
helm upgrade narad ./charts/narad -n narad \
  --reset-then-reuse-values \
  --set image.tag=v3.0.0
kubectl rollout status statefulset/narad -n narad
```

Prefer this to `helm rollback`. A `helm rollback` to a revision with a smaller `replicaCount` is a scale-in that skips the decommission ([Scale out and in](scaling.md#scale-in)). The chart refuses it unless that revision set `allowScaleIn`.

### Leave a build newer than v3.0.1 {#roll-back-newer}

**Unreleased:** in master, not in v3.0.1.

Builds after v3.0.1 write some data and accept some settings that v3.0.1 and earlier do not understand. Before a node goes back to v3.0.1 or earlier:

1. **Drain the ingress WAL.** Newer builds write accepted produces in a record format older binaries cannot read; a rolled-back node stops delivering from the first such record it still holds, while it keeps answering `202`. Pause producers, keep every partition owner up, and wait until `narad_ingress_dispatch_backlog_records` is 0 on every node, in a sample taken after the pause (the gauge refreshes every 5 seconds). Then roll back and resume.
2. **Turn off segment preparation, if you turned it on.** Remove `storage.ingress_wal_prealloc` from `narad.config`, restart every pod once on the newer image (`kubectl rollout restart statefulset/narad -n narad`), then roll back. Skipping this can leave an older binary unable to start after a crash, with a `corrupt frame` error.
3. **Remove the new config keys.** Take `storage.consumer_offset_commit_interval_ms`, `storage.ingress_wal_prealloc` and `http.max_produce_in_flight_per_identity` out of `narad.config`, even where they hold their defaults. An older binary with one of them in its config file stops at startup with `storage.ingress_wal_prealloc is an internal setting and cannot be configured`, or `json: unknown field "max_produce_in_flight_per_identity"`, before it opens any data. The environment variable `NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY` can stay.
4. **Move clients off the batch forms.** An older node answers a batch produce `404`, ignores `max` on a consume and returns one message in the single shape, and answers a batch ack `400` (`receipt_handle required`).
5. **Set `GOMEMLIMIT` if you relied on the derived one.** Older binaries do not set it from the pod's memory limit; add it to `extraEnv`.
6. **Stop every pod cleanly.** Give pods a `terminationGracePeriodSeconds` long enough to shut down. A pod that crashed should first start once on the newer image and stop cleanly, because an older binary reopens a crashed partition without syncing it first, and a power loss straight after could take back records it already served.

Partition segments, consumer position files and fan-out cursor files need nothing: each binary reads what the other wrote. One gap stays open: a message stored without a key and fanned out to a [replica child](../reference/glossary.md#replica-child) after the rollback is placed round-robin, so both copies can land on one node. The changelog's "Upgrade and rollback notes" have every detail.

## Version notes {#version-notes}

Read the notes for each release boundary you cross, in either direction.

- **From v3.0.0.** Roll every node. A v3.0.0 node that has been up for more than 24 hours cannot be reached by its peers for forwarded produce, consume and ack until it restarts. v3.0.1 fixes this.
- **Across v2.2.0 (node-to-node authentication).** Nodes before and after v2.2.0 cannot talk over the node RPC plane. Roll twice: first with `--set security.allowLegacyClusterAuth=true`, so upgraded pods still speak the old protocol to the pods not yet rolled, then, once every pod runs the new image, with it back at `false`. If you skip the first roll, forwarded requests between old and new pods fail until the roll finishes.
- **Across v2.2.0 (Raft TLS).** From v2.2.0, a secured multi-node cluster refuses to start unless Raft runs over TLS or `allowPlaintextRaft` says the port is fenced. The chart sets `allowPlaintextRaft: true` by default; see the [Production checklist](production-checklist.md#raft-tls).
- **Across v2.2.0 (file modes).** From v2.2.0, Narad creates its data directories with mode `0700` and its files with `0600`. It sets the mode only when it creates a file, so files an older release created keep `0755` and `0644`. On a host where other users can read the data directory, tighten those by hand.
- **Config files.** Every release since v1.0.0 refuses a config file key it does not know, and fails at startup. When you roll back, remove keys the older release does not have first ([Configuration reference](../reference/configuration.md#config-file)).
- **`replicaCount` in a rollback.** Going back to fewer replicas is a scale-in. Decommission first ([Scale out and in](scaling.md#decommission)).

## Next steps

- [Changelog](https://github.com/DebanganThakuria/narad/blob/master/CHANGELOG.md): every change and upgrade note, per release.
- [API stability and versions](../reference/api-stability.md): what can change in `/v1`, and which release these docs describe.
- [Monitor and alert](monitoring.md): watch the cluster through the roll.
