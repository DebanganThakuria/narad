---
description: "Check a Narad deployment against this list before it takes production traffic."
---

# Production checklist

Check a Narad deployment against this list before it takes production traffic.

Before you start: a cluster installed as in [Deploy on Kubernetes](deploy-kubernetes.md), and its values file.

| Check | How |
|---|---|
| Raft runs over mutual TLS, or its port is fenced | [Secure the Raft port](#raft-tls) |
| Only Narad pods reach 7943/tcp and 7942/udp | [Fence the cluster ports](#network-policy) |
| The root admin password came from your secret | [Set the admin password](#admin-password) |
| Metrics and pprof are not reachable from outside | [Keep metrics internal](#metrics-exposure) |
| Clients connect over TLS | [Terminate client TLS](#client-tls) |
| A rate limiter sits in front of the API | [Limit request rates](#rate-limiting) |
| Disk is sized for your retention | [Capacity and disk sizing](../reference/capacity.md#disk-sizing) |
| Compression is chosen on purpose | [Choose values](deploy-kubernetes.md#values) |
| The five alerts are configured | [Monitor and alert](monitoring.md#alerts) |
| Topics you cannot lose have a second copy | [Keep a second copy](#second-copy) |
| `initialClusterSize` is set and stays fixed | [Fix the bootstrap size](#initial-cluster-size) |
| Each service has its own user and grants | [Manage users and grants](users.md) |

## Secure the Raft port {#raft-tls}

Narad keeps its [metadata](../reference/glossary.md#metastore) (topics, users with their password hashes, grants, partition owners) in a Raft group on port 7943/tcp. Raft has no authentication of its own, and the cluster secret does not cover it: anything that reaches that port can force elections or rewrite the metadata as a fake leader.

A multi-node cluster with security on refuses to start unless one of two things is true:

- **Raft runs over mutual TLS.** Set `security.clusterTLS.enabled: true` in the chart. Steps: [Raft TLS certificates](raft-tls.md).
- **You state that the port is fenced another way.** Set `security.allowPlaintextRaft: true` and apply a NetworkPolicy ([next section](#network-policy)).

The chart sets `allowPlaintextRaft: true` by default, because `clusterTLS` is off by default. So a default install runs Raft in plaintext. Each node says at startup which transport it runs:

```bash
kubectl logs -n narad narad-0 \
  | grep -oE 'raft metadata transport (is plaintext|secured with mutual TLS)'
```

```text title="Output"
raft metadata transport is plaintext
```

How the two planes are secured is in [Networking and security](../understand/networking-and-security.md#raft-tls).

## Fence the cluster ports {#network-policy}

Two ports carry node-to-node traffic: Raft on 7943/tcp, and the node RPC plane, which uses QUIC on the API port number over UDP (7942/udp). Restrict both to the Narad pods, whether or not Raft uses TLS. This NetworkPolicy does that for a release called `narad` in the namespace `narad`:

```yaml title="narad-networkpolicy.yaml"
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: narad
  namespace: narad
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: narad
      app.kubernetes.io/instance: narad
  policyTypes:
    - Ingress
  ingress:
    # Raft and node RPC: other Narad pods only.
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: narad
              app.kubernetes.io/instance: narad
      ports:
        - protocol: TCP
          port: 7943
        - protocol: UDP
          port: 7942
    # Client API: any source that can reach the Service.
    - ports:
        - protocol: TCP
          port: 7942
    # Metrics and probe port: your Prometheus namespace only.
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: monitoring
      ports:
        - protocol: TCP
          port: 9100
```

```bash
kubectl apply -f narad-networkpolicy.yaml
```

Change `monitoring` to the namespace your Prometheus runs in. Kubernetes does not let a NetworkPolicy block traffic from the pod's own node, so kubelet probes still reach port 9100. The pprof port (6060) has no rule, so the policy closes it.

## Set the admin password {#admin-password}

Put `admin-password` in the security secret before the first start. The root user, `admin`, is created once, from that value, when the cluster has no users. Without it, one node generates a password and logs it once. Changing the secret later changes nothing; change the password through the API instead. Both cases: [Manage users and grants](users.md#root-admin).

## Keep metrics internal {#metrics-exposure}

`/metrics` names every topic with its partition count, lag, throughput and fan-out links. With the chart's default `metrics.enabled: true`, it is served on its own listener on port 9100, without authentication, for Prometheus to scrape inside the cluster, and the API port no longer serves it.

- Do not route port 9100 through an ingress or a LoadBalancer. The chart's LoadBalancer Service and Traefik route expose only the API port.
- With `metrics.enabled: false`, `/metrics` moves to the API port behind the same Basic auth as the API. `NARAD_HTTP_METRICS_UNAUTHENTICATED=true` removes that check; leave it unset.
- pprof is off by default. If you turn it on (`narad.pprof.enabled`), it is unauthenticated too; keep port 6060 internal ([pprof](monitoring.md#pprof)).

The listener settings are in the [Metrics reference](../reference/metrics.md).

## Terminate client TLS {#client-tls}

Narad serves plain HTTP and expects client TLS to terminate in front of it, at your ingress or load balancer. Clients send Basic credentials on every request, so never expose the API port without TLS. The chart can create a Traefik route or a LoadBalancer Service, or you can bring your own ingress: see the [Helm values reference](../reference/helm-values.md#values).

## Limit request rates {#rate-limiting}

Narad caps request sizes and concurrency, but it has no request rate limiting. A buggy or hostile client can send requests as fast as the cluster accepts them. Put a rate limiter at your ingress, where client TLS terminates.

The limits Narad applies on its own, per node:

| Limit | Default | Setting |
|---|---|---|
| Request body | 1 MiB | fixed |
| Request headers | 64 KiB | `NARAD_HTTP_MAX_HEADER_BYTES` |
| Open client connections | 4096 | `NARAD_HTTP_MAX_CONNECTIONS` |
| Concurrent consumes per user | 1024 | `NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY` |
| Concurrent produces per user (unreleased) | off | `NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY` |

A request over a per-user cap gets `429` ([Troubleshooting](troubleshooting.md#status-429)). With security off, the caps count per client IP instead of per user. Every setting is in the [Configuration reference](../reference/configuration.md#http).

## Keep a second copy {#second-copy}

--8<-- "contract/one-copy.md"

Decide which topics need a second copy before the first disk fails: [Back up and replicate topics](backups.md).

## Fix the bootstrap size {#initial-cluster-size}

`initialClusterSize` is the set of pods (`narad-0` up to `narad-<n-1>`) allowed to create a new Raft cluster; every pod beyond it joins the existing one. The chart requires an odd number. It is read only when a pod starts on an empty disk, so set it at the first install and never change it. To grow the cluster, raise `replicaCount` instead ([Scale out and in](scaling.md#scale-out)).

## Next steps

- [Raft TLS certificates](raft-tls.md): turn on mutual TLS for the Raft port.
- [Monitor and alert](monitoring.md): set up the five alerts from the checklist.
- [Back up and replicate topics](backups.md): add a second copy for the topics that need one.
