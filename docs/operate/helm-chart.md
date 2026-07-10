# Helm Chart Reference

The chart lives in-repo at [`charts/narad`](https://github.com/DebanganThakuria/narad/tree/master/charts/narad) — one chart, no dependencies, nothing to add to a repo index. This page is the guided tour; `values.yaml` itself is commented and is the final authority.

## What the chart creates

| Template | Object | Purpose |
|---|---|---|
| `statefulset.yaml` | StatefulSet | The nodes. Pod name = node ID; `Parallel` pod management; PVC per pod |
| `service-headless.yaml` | Headless Service | Stable per-pod DNS — the Raft peer list is built from it |
| `service-internal.yaml` | ClusterIP Service | In-cluster clients |
| `service-loadbalancer.yaml` | LoadBalancer Service (optional) | External clients without an ingress controller |
| `ingressroute.yaml` | Traefik `IngressRoute` (optional) | Host routing with path blocking (see below) |
| `configmap.yaml` | ConfigMap (optional) | Renders `narad.config` into the `--config` JSON file |
| `servicemonitor.yaml` | ServiceMonitor (optional) | Prometheus-operator scraping |
| `pdb.yaml` | PodDisruptionBudget | Keeps voluntary evictions from eating your quorum |
| `validate.yaml` | — | Fails fast on nonsense (`replicaCount < initialClusterSize`, even initial sizes) |

## The values that matter

```yaml
# Identity & size
replicaCount: 3
initialClusterSize: 3          # bootstrap set — write once, never change
clusterDomain: cluster.local   # for the headless-DNS peer list

image:
  repository: ghcr.io/debanganthakuria/narad
  tag: v0.2.0-beta.3           # pin releases

# Storage
persistence:
  size: 50Gi                   # per pod; see Scaling & Recovery for the math
  storageClassName: ""         # your EBS/PD/whatever class

# Pod placement & platform conventions
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

### `podLabels` — the platform-convention escape hatch

Some platforms want their own labels on every pod (cost attribution, routing, team ownership — every company has one of these). `podLabels` applies them to the **pod template only**, never to the StatefulSet selector — selectors are immutable in Kubernetes, and a chart that bakes site-specific labels into a selector has decided for you that you can never change them. Ask us how we know.

```bash
helm upgrade narad ./charts/narad --reuse-values \
  --set podLabels.my_platform_label=my-team
```

### The Traefik route and the `/metrics` hole

`/metrics` is deliberately auth-exempt (it's a scrape target), which means it leaks topic names and traffic volumes — fine in-cluster, not fine on a public host. The IngressRoute template therefore **excludes `blockedPathPrefixes` from the public match** (`/metrics` by default; add `/healthz`, `/readyz` if nothing external probes them). Prometheus still scrapes pods directly through the ServiceMonitor. If you use a different ingress controller, replicate the same idea:

```yaml
# generic Ingress equivalent: route everything, then deny /metrics
# at your ingress controller's path level — or just don't expose
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

Rolling update, reverse ordinal order, leadership hands off gracefully — we ship under live traffic routinely, and we've force-killed pods mid-rollout under a loss-detecting harness for fun. Scale-out is the same command with a bigger `replicaCount` ([details](scaling-and-recovery.md)).
