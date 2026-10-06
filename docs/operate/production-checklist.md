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
| Default retention is chosen on purpose (the chart's is 12 hours) | [Decide before you install](deploy-kubernetes.md#values) |
| Compression is chosen on purpose | [Decide before you install](deploy-kubernetes.md#values) |
| The seven alerts are configured | [Monitor and alert](monitoring.md#alerts) |
| Topics you cannot lose have a second copy | [Keep a second copy](#second-copy) |
| `initialClusterSize` is set and stays fixed | [Fix the bootstrap size](#initial-cluster-size) |
| Each service has its own user and grants | [Manage users and grants](users.md) |

## Secure the Raft port {#raft-tls}

Narad keeps its [metadata](../reference/glossary.md#metastore) (topics, users with their password hashes, grants, partition owners) in a Raft group on port 7943/tcp. Raft has no authentication of its own, and the cluster secret does not cover it: anything that reaches that port can force elections or rewrite the metadata as a fake leader.

A node with security on refuses to start unless one of two things is true (a node with no peers is exempt only while its `cluster.addr` is a loopback address, unreleased):

- **Raft runs over mutual TLS.** Set `security.clusterTLS.enabled: true` in the chart. Steps: [Raft TLS certificates](raft-tls.md).
- **The port is fenced.** The chart's NetworkPolicy fences it, and it is on by default (`networkPolicy.enabled`, [next section](#network-policy)); `security.allowPlaintextRaft: true` says you fence it some other way.

The chart tells the node that the port is fenced (`NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT`) only when one of those two is set and `clusterTLS` is off (unreleased). With security on, Raft TLS off, the policy turned off and no `allowPlaintextRaft`, the install fails and names the three fixes. The v3.0.1 chart set `allowPlaintextRaft: true` by default instead, while shipping nothing that fenced the port, so a default v3.0.1 install runs Raft in plaintext with nothing in front of it. Each node says at startup which transport it runs:

```bash
kubectl logs -n narad narad-0 \
  | grep -oE 'raft metadata transport (is plaintext|secured with mutual TLS)'
```

```text title="Output"
raft metadata transport is plaintext
```

How the two planes are secured is in [Networking and security](../understand/networking-and-security.md#raft-tls).

## Fence the cluster ports {#network-policy}

Two ports carry node-to-node traffic: Raft on 7943/tcp, and the node RPC plane, which uses QUIC on the API port number over UDP (7942/udp). Restrict both to the Narad pods, whether or not Raft uses TLS. The chart does it with its NetworkPolicy, on by default (`networkPolicy.enabled`, unreleased); these values keep the metrics port to your Prometheus namespace too:

```yaml title="narad-values.yaml"
networkPolicy:
  enabled: true
  metricsFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: monitoring
```

The policy admits 7943/tcp and 7942/udp from the release's own pods only, and leaves the API (7942/tcp) open to any source that can reach the Service; `networkPolicy.apiFrom` narrows it. Change `monitoring` to the namespace your Prometheus runs in. Kubernetes does not let a NetworkPolicy block traffic from the pod's own node, so kubelet probes still reach their ports. The pprof port (6060) is closed unless `networkPolicy.pprofFrom` names a source. Every value is in the [Helm values reference](../reference/helm-values.md#network-policy).

A NetworkPolicy is only enforced by a CNI that supports it (Calico, Cilium and most managed offerings do). On one that does not, the policy is accepted and changes nothing, and the ports stay open. Check yours before you rely on it for Raft; Raft TLS does not depend on the CNI.

??? note "The v3.0.1 chart: apply the policy by hand"
    The v3.0.1 chart has no `networkPolicy` values. This NetworkPolicy does the same for a release called `narad` in the namespace `narad`:

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

## Set the admin password {#admin-password}

Put `admin-password` in the security secret before the first start. The root user, `admin`, is created once, from that value, when the cluster has no users. Without it, one node generates a password and writes it to a file on its own volume. Changing the secret later changes nothing; change the password through the API instead. Both cases: [Manage users and grants](users.md#root-admin). The chart's scale-in guard signs in with that key (unreleased), so after changing the password, put the new one in the secret too ([Scale in](scaling.md#scale-in)).

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
- [Monitor and alert](monitoring.md): set up the seven alerts from the checklist.
- [Back up and replicate topics](backups.md): add a second copy for the topics that need one.
