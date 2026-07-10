# Narad Helm Chart

This chart runs Narad as a three-pod StatefulSet with stable DNS and
PVC-backed storage.

## Install

```sh
helm upgrade --install narad ./charts/narad \
  --namespace narad \
  --create-namespace
```

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
```

## Storage Permissions

The pod runs as UID/GID `10001` and sets `fsGroup: 10001` so dynamically
provisioned volumes are writable by Narad. Your Kubernetes identity still needs
permission to create PVCs, or the install will fail when the StatefulSet creates
its `volumeClaimTemplates`.
