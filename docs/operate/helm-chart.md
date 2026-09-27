# Helm Chart Reference

The chart lives in-repo at [`charts/narad`](https://github.com/DebanganThakuria/narad/tree/master/charts/narad): one chart, no dependencies, nothing to add to a repo index. This page is the guided tour; `values.yaml` itself is commented and is the final authority.

## What the chart creates

| Template | Object | Purpose |
|---|---|---|
| `statefulset.yaml` | StatefulSet | The nodes. Pod name = node ID; `Parallel` pod management; PVC per pod |
| `service-headless.yaml` | Headless Service | Stable per-pod DNS; the Raft peer list is built from it |
| `service-internal.yaml` | ClusterIP Service | In-cluster clients |
| `service-loadbalancer.yaml` | LoadBalancer Service (optional) | External clients without an ingress controller |
| `ingressroute.yaml` | Traefik `IngressRoute` (optional) | Host routing with path blocking (see below) |
| `configmap.yaml` | ConfigMap (optional) | Renders `narad.config` into the `--config` JSON file |
| `servicemonitor.yaml` | ServiceMonitor (optional) | Prometheus-operator scraping |
| `pdb.yaml` | PodDisruptionBudget | `maxUnavailable: 1`: voluntary evictions take one pod at a time whatever the size |
| `validate.yaml` | (none) | Fails fast on nonsense (`replicaCount < initialClusterSize`, even initial sizes, a peer list smaller than the bootstrap set) and refuses a scale-in of a running StatefulSet without `allowScaleIn=true` |

## The values that matter

```yaml
# Identity & size
replicaCount: 3
initialClusterSize: 3          # bootstrap set: write once, never change
clusterPeerCount: 0            # pinned peer list size; 0 = initialClusterSize (see below)
allowScaleIn: false            # must be true to lower replicaCount on a running cluster
clusterDomain: cluster.local   # for the headless-DNS peer list

image:
  repository: ghcr.io/debanganthakuria/narad
  tag: v0.2.0-beta.3           # pin releases

# Storage
persistence:
  size: 50Gi                   # per pod; see Scaling & Recovery for the math
  storageClassName: ""         # your EBS/PD/whatever class

# Pod placement & platform conventions
commonLabels: {}               # extra labels on EVERY resource's metadata (admission policies)
podLabels: {}                  # extra POD labels (never touches the immutable selector)
podAnnotations: {}
resources: {}
affinity: {}                   # spread across zones here if you have them

# Engine
narad:
  logLevel: info
  logFormat: json
  defaultRetentionAgeMs: 43200000
  maxConsumeWait: 10s
  pprof: { enabled: false }
  config: {}                   # engine JSON (storage codec etc.) → --config

# Security
security:
  enabled: true
  existingSecret: ""           # defaults to <release>-security
  clusterTLS: { enabled: false } # mTLS on Raft; turn on for production
  allowPlaintextRaft: true     # the broker refuses secure multi-node without
                               # Raft TLS unless this says the port is fenced
  allowLegacyClusterAuth: false # only for the upgrade across the auth change
  allowInsecureCluster: false  # multi-node with enabled: false needs this

# Observability
metrics:
  enabled: true                # ServiceMonitor

# External access (pick one, or bring your own ingress)
service:
  loadBalancer: { enabled: false }
traefik:
  enabled: false
  host: narad.example.com
  ingressClass: traefik
  blockedPathPrefixes: ["/metrics"]
```

### `replicaCount`, `initialClusterSize`, `clusterPeerCount`: what scales and what is pinned

Three numbers, three jobs:

- **`initialClusterSize`** is the set of pods that may bootstrap a brand-new Raft cluster (`narad-0` … `narad-N-1`). The bootstrap configuration is seeded with exactly these pods, so a fresh seven-replica install with `initialClusterSize: 3` needs two of three votes for its first election, not four of seven. Write it once and never change it.
- **`clusterPeerCount`** sizes the peer list every pod receives in `NARAD_CLUSTER_PEERS`. It defaults to `initialClusterSize` and is deliberately **not** derived from `replicaCount`: the peer list is part of the pod template, and a list that changed with every scale operation would roll every existing member at the same moment as the join or the decommission (the docs tell you never to overlap those, and the chart used to make them inseparable). Each pod advertises its own address through `NARAD_CLUSTER_ADVERTISE_ADDR`, so pods beyond the list join and work normally. Raise it only when you mean to roll the whole cluster.
- **`replicaCount`** is the only number you change to scale. Raising it adds join-only pods that the leader admits. **Lowering it is a scale-in, and so is a rollback to an older values file**: the StatefulSet deletes the highest-ordinal pods, whose partition data is a single copy. Decommission them first (`narad cluster decommission <pod>`, wait for zero owned partitions and Raft removal), then pass `--set allowScaleIn=true`; the chart refuses to reduce a running StatefulSet's replicas otherwise. `helm rollback` to a smaller `replicaCount` is refused for the same reason unless the release being rolled back to already carried `allowScaleIn: true`, so prefer an explicit `helm upgrade` for scale-ins.

### `commonLabels` & `podLabels`: the platform-convention escape hatches

Some platforms want their own labels on everything: cost attribution, team routing, an admission webhook that rejects any Service without the sacred label (every company has one of these; ours does). Two values cover it, both supplied at deploy time so site-specific conventions stay out of the chart:

- **`commonLabels`** goes on every resource's **metadata**; this is what satisfies label-enforcing admission policies (Kyverno, Gatekeeper).
- **`podLabels`** goes on the **pod template** for label-based pod selection/attribution.

```bash
helm upgrade narad ./charts/narad --reuse-values \
  --set commonLabels.my_platform_label=my-team \
  --set podLabels.my_platform_label=my-team
```

Neither ever touches two places, by hard-won design: the **StatefulSet selector** (immutable: a chart that bakes site labels into it has decided you may never change them) and the **volumeClaimTemplates** (also frozen spec: an operator label added a month later must not brick every future `helm upgrade`). The chart keeps both on a fixed, boring label set. Ask us how we know. Actually don't; it's documented in the commit history.

### The Traefik route and the `/metrics` hole

`/metrics` is deliberately auth-exempt (it's a scrape target), which means it leaks topic names and traffic volumes: fine in-cluster, not fine on a public host. The IngressRoute template therefore **excludes `blockedPathPrefixes` from the public match** (`/metrics` by default; add `/healthz`, `/readyz` if nothing external probes them). The metrics listener also serves `/healthz` and `/readyz`. The chart sends the startup and liveness probes there whenever `metrics.enabled` is on, so a queue of client traffic on the API port can never get a healthy node killed; with metrics off they fall back to the API port. Readiness deliberately stays on the API port either way: it decides whether traffic is routed here, so it has to be answered by the listener that serves it. Prometheus still scrapes pods directly through the ServiceMonitor. If you use a different ingress controller, replicate the same idea:

```yaml
# generic Ingress equivalent: route everything, then deny /metrics
# at your ingress controller's path level, or just don't expose
# the API publicly at all, which is the actual best practice.
```

## Secrets contract

The chart reads a Secret named `<release>-security` (override via `security.existingSecret`):

| Key | Required | Meaning |
|---|---|---|
| `cluster-secret` | yes | Shared secret for node-to-node QUIC RPC |
| `admin-password` | no | Root admin password; omitted = generated and logged once |

Nothing secret goes in values files. Values files end up in git; see "NDA" in your nearest dictionary.

## Upgrades

```bash
helm upgrade narad ./charts/narad -n narad --reuse-values --set image.tag=v0.2.0-beta.4
```

Rolling update, reverse ordinal order, leadership hands off gracefully; we ship under live traffic routinely, and we've force-killed pods mid-rollout under a loss-detecting harness for fun. Scale-out is the same command with a bigger `replicaCount` ([details](scaling-and-recovery.md)).

**Upgrading across the node-to-node auth change** (fixed token to session-bound proofs): nodes on either side of it cannot talk to each other. Roll twice: first with `--set security.allowLegacyClusterAuth=true` so upgraded pods still speak the old protocol to the pods that have not rolled yet, then, once every pod is on the new image, with it back to `false`. Skipping the first roll works too; forwarded requests between old and new pods just fail until the roll finishes.

### Rolling back to an earlier release

A rollback is the same `helm upgrade` with the older `image.tag`. Going back to v3.0.1 or earlier from a later release has three conditions:

- **Drain the ingress WAL first.** Later releases write every accepted produce for a topic with an incarnation id (any topic created on v2.2.0 or later) to the ingress WAL as record format 2, which v3.0.1 and earlier cannot decode. A node rolled back with such a record still undispatched stops dispatching at it: it keeps answering produce with `202`, but nothing it accepted from that record on is delivered until it runs the newer binary again. Nothing is lost, but nothing from that node flows. A node stops dispatching as soon as it receives `SIGTERM`, while it is still finishing the produce requests in flight, so a plain rolling rollback can leave a few such records behind. Pause your producers, keep every partition owner up, and wait until `narad_ingress_dispatch_backlog_records` is 0 on every node, in a sample taken after the pause (the gauge is refreshed every 5 s and counts the records each node would replay after a restart). Then roll back, and resume.
- **Segment preparation, if you enabled it.** With `storage.ingress_wal_prealloc: true`, an older binary opens a cleanly stopped WAL without losing records, but can refuse to start (a `corrupt frame` error) on a WAL where a crash tore a write inside a prepared segment and left valid frames behind a hole. Start the newer binary once to recover the WAL, or roll back only after a clean stop. Either way, remove the key from `narad.config` first (next point).
- **Remove the new storage keys from `narad.config`.** v3.0.1 and earlier reject any storage key they do not know, so remove `storage.consumer_offset_commit_interval_ms` and `storage.ingress_wal_prealloc` from the values you roll back with, even where they hold their default values (the full config shape in [Configuration](configuration.md) lists both). A node rolled back with either key still in its config file fails to start (`storage.ingress_wal_prealloc is an internal setting and cannot be configured`), and the rollout stops at that pod. It fails while loading its configuration, before it opens any storage, so nothing on disk is touched: remove the key and the pod starts.

The partition files need nothing. After a clean stop the `hwm` file holds the exact boundary, as it always did; after a crash it can be empty, which an older binary reads as "take the record tail", so no acked record is hidden. An older binary also parses the new fan-out cursor record. The [changelog](https://github.com/DebanganThakuria/narad/blob/master/CHANGELOG.md) lists the details per release.
