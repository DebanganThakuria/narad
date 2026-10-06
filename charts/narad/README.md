# Narad Helm Chart

This chart runs Narad as a three-pod StatefulSet with stable DNS and
PVC-backed storage.

## Install

```sh
helm upgrade --install narad ./charts/narad \
  --namespace narad \
  --create-namespace
```

With security on (the default), Raft needs mutual TLS or a fence. The
chart's NetworkPolicy, on by default, is that fence; see
[Network policy and Raft TLS](#network-policy-and-raft-tls).

Enable an EKS LoadBalancer when you want to hit Narad from outside the cluster:

```sh
helm upgrade --install narad ./charts/narad \
  --namespace narad \
  --create-namespace \
  --set service.loadBalancer.enabled=true
```

For an internal AWS NLB, set annotations in a values override:

```yaml
service:
  loadBalancer:
    enabled: true
    annotations:
      service.beta.kubernetes.io/aws-load-balancer-type: external
      service.beta.kubernetes.io/aws-load-balancer-scheme: internal
      service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
```

## Important Defaults

| Value | Default |
| --- | --- |
| `replicaCount` | `3` |
| `image.repository` | `ghcr.io/debanganthakuria/narad` |
| `image.tag` | `latest` |
| `persistence.enabled` | `true` |
| `persistence.size` | `10Gi` |
| `narad.defaultRetentionAgeMs` | `43200000` |
| `service.loadBalancer.enabled` | `false` |

The chart exposes:

* Headless service for StatefulSet peer DNS.
* ClusterIP service for in-cluster HTTP clients and Prometheus scraping.
* Optional LoadBalancer service for API traffic.
* TCP `7942` for the public HTTP API.
* UDP `7942` for Narad peer RPC.
* TCP `7943` for Raft/bootstrap cluster traffic.

## Network policy and Raft TLS

Raft (`7943/tcp`) carries the cluster metadata, password hashes and grants
included, and has no authentication of its own. With `security.enabled`,
the broker refuses to start on plaintext Raft unless it is told the port
is fenced, and the chart tells it so (`NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT`)
only when that is true. Pick one:

* `security.clusterTLS.enabled=true`: Raft mutual TLS, with the
  certificates in the `narad-cluster-tls` Secret. The production choice.
* `networkPolicy.enabled=true`, the default: the chart renders a NetworkPolicy that
  admits Raft (`7943/tcp`) and the node RPC plane (`7942/udp`) from this
  release's pods only, and leaves the API (`7942/tcp`) and metrics ports
  open, or limited to `networkPolicy.apiFrom` and
  `networkPolicy.metricsFrom`. It needs a CNI that enforces
  NetworkPolicy; on one that does not, nothing is fenced.
* `security.allowPlaintextRaft=true`: you fence `7943/tcp` and `7942/udp`
  to the narad pods some other way.

Turn the policy off (`networkPolicy.enabled=false`) with neither of the
others and the install fails and names all three. The policy is useful
with Raft TLS too: it keeps the node RPC plane to the release's pods. An
upgrade with `--reuse-values` from a chart older than the policy renders
without it, as before.

## Scaling in

Lowering `replicaCount`, or rolling back to a revision with fewer
replicas, deletes the highest-numbered pods. Decommission each first
(`narad cluster decommission <pod>`, then wait until `narad cluster
members` no longer lists it), then upgrade with
`--set replicaCount=<N> --set allowScaleInTo=<N>`. `allowScaleInTo`
approves that one size only; the old `allowScaleIn` is no longer read.

A pre-upgrade and pre-rollback hook Job (`scaleInGuard.enabled`, on by
default) also checks the cluster: it refuses an upgrade or a
`helm rollback` that would delete a pod still listed as a member. An
upgrade that deletes no pod passes without calling the API. On a
scale-in it signs in as `admin` with the `admin-password` key of the
security Secret, which must hold the root password; without it, it
refuses and says so. After checking by hand, `--no-hooks` skips it for
one command. A rollback to a revision rendered by an older chart runs no
guard.

## Metrics

The chart annotates pods with the standard prometheus.io convention:

```yaml
prometheus.io/path: /metrics
prometheus.io/port: "7942"
prometheus.io/scrape: "true"
```

Add any cluster-specific scrape annotations through `metrics.annotations` or
set `metrics.enabled=false` to disable annotation-based scraping. If your
cluster uses Prometheus Operator, set `serviceMonitor.enabled=true`.

## Verify

```sh
kubectl rollout status statefulset/narad -n narad
kubectl get pods,svc,pvc -n narad

kubectl port-forward -n narad svc/narad 7942:7942
curl http://127.0.0.1:7942/healthz
curl http://127.0.0.1:7942/readyz

The same two paths are served on the metrics port (9100) when `metrics.enabled` is on. The chart points the startup and liveness probes there, so a queue of client traffic on the API port cannot get a healthy node killed. Readiness stays on the API port: it decides whether traffic is routed here, so a wedged API listener has to fail it.
```

## Storage Permissions

The pod runs as UID/GID `10001` and sets `fsGroup: 10001` so dynamically
provisioned volumes are writable by Narad. Your Kubernetes identity still needs
permission to create PVCs, or the install will fail when the StatefulSet creates
its `volumeClaimTemplates`.
