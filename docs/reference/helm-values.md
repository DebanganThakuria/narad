---
description: "Look up every Narad Helm chart value, the objects the chart creates, its ports and probes, and the settings each value controls."
search:
  boost: 2
---

# Helm values reference

Look up every Narad Helm chart value, the objects the chart creates, its ports and probes, and the settings each value controls.

```sh title="Command"
helm template narad ./charts/narad \
  --set narad.defaultPartitions=6 \
  | grep -A1 NARAD_TOPIC_DEFAULT_PARTITIONS
```

```text title="Output"
            - name: NARAD_TOPIC_DEFAULT_PARTITIONS
              value: "6"
```

The chart lives in the repository at [`charts/narad`](https://github.com/DebanganThakuria/narad/tree/master/charts/narad): one chart, no dependencies, no chart repository to add. Run the commands on this page from a clone of the repository. The steps to install it are in [Deploy on Kubernetes](../operate/deploy-kubernetes.md), and moving to a new release is in [Upgrade Narad](../operate/upgrade.md). The comments in `values.yaml` are the final word.

## What the chart creates {#what-the-chart-creates}

| Template | Created | Purpose |
|---|---|---|
| `statefulset.yaml`<br>StatefulSet | always | The nodes. The pod name is the node ID. Pods start in parallel, each with its own volume. |
| `service-headless.yaml`<br>headless Service | always | A stable DNS name per pod, for the node-to-node port and Raft. The Raft peer list is built from these names. |
| `service-internal.yaml`<br>ClusterIP Service | `service.internal.enabled` | The API, and the metrics port, for clients inside the cluster. |
| `service-loadbalancer.yaml`<br>LoadBalancer Service `<name>-lb` | `service.loadBalancer.enabled` | The API for clients outside the cluster, without an ingress controller. |
| `ingressroute.yaml`<br>Traefik IngressRoute | `traefik.enabled` | A public host route that leaves out `traefik.blockedPathPrefixes`. |
| `configmap.yaml`<br>ConfigMap | `narad.config` is not empty | The config file passed with `--config`. |
| `servicemonitor.yaml`<br>ServiceMonitor | `serviceMonitor.enabled` | Scraping by the Prometheus Operator. |
| `pdb.yaml`<br>PodDisruptionBudget | `podDisruptionBudget.enabled` | At most one pod down at a time during voluntary evictions, whatever the cluster size. |
| `serviceaccount.yaml`<br>ServiceAccount | `serviceAccount.create` | A service account that mounts no API token. |
| `networkpolicy.yaml`<br>NetworkPolicy | `networkPolicy.enabled`, on by default | Admits Raft and the node RPC plane from the release's own pods only (unreleased); see [Network policy](#network-policy). |
| `scalein-guard-job.yaml`<br>Job `<name>-scale-in-guard`, a hook | `scaleInGuard.enabled` | Runs before every `helm upgrade` and `helm rollback`, and refuses one that would delete pods that are still cluster members (unreleased); see [Scale in](../operate/scaling.md#scale-in). |
| `validate.yaml`<br>none | always | Stops a render that would break the cluster; see below. |

`validate.yaml` fails the install or upgrade when:

- `initialClusterSize` is less than 1 or even;
- `replicaCount` is less than `initialClusterSize`;
- `clusterPeerCount` (when set) is less than `initialClusterSize`;
- `security.enabled` is on, `security.clusterTLS.enabled` is off, `networkPolicy.enabled` is turned off, and `security.allowPlaintextRaft` does not say the Raft port is fenced some other way (unreleased). The message names the three fixes; see [Production checklist](../operate/production-checklist.md#raft-tls).
- `replicaCount` is lower than the running StatefulSet's replicas and `allowScaleInTo` is not that new `replicaCount` (unreleased; it used to be `allowScaleIn: true`, which is no longer read). `helm template` cannot see the running StatefulSet, so this check runs only on a real install or upgrade, and `helm rollback` never runs it: the scale-in guard Job covers rollbacks.

For example, an even `initialClusterSize`:

```sh title="Command"
helm template narad ./charts/narad --set initialClusterSize=2
```

```text title="Output"
Error: execution error at (narad/templates/validate.yaml:11:4): initialClusterSize must be odd (Raft quorum).

Use --debug flag to render out invalid YAML
```

## Values {#values}

Each table lists the value with its default in `values.yaml` under it, and what it does. Values that become an environment variable of the broker link to the setting in the [Configuration reference](configuration.md).

### Cluster size {#size}

| Value | What it does |
|---|---|
| `replicaCount`<br>default `3` | The number of nodes. The only number you change to scale. Raising it adds nodes that join the existing cluster. Lowering it is a scale-in, and so is a rollback to a revision with fewer replicas: decommission the highest-numbered pods first, then set `allowScaleInTo`. See [Scale out and in](../operate/scaling.md). |
| `initialClusterSize`<br>default `3` | The pods (`narad-0` up to `narad-N-1`) that may bootstrap a new Raft cluster; the rest join it. Odd, at least 1. Set it once at the first install and never change it. Sets [`cluster.initial_members`](configuration.md#cluster). |
| `clusterPeerCount`<br>default `0` | The size of the peer list every pod gets. `0` means `initialClusterSize`. It is part of the pod template, so changing it rolls every pod; each pod also advertises its own address, so pods beyond the list work normally. Sets [`cluster.peers`](configuration.md#cluster). |
| `allowScaleInTo`<br>default `0` | Allows lowering a running StatefulSet's replicas to this one `replicaCount` (unreleased). Set it to the new size once `narad cluster members` no longer lists the pods being removed. It approves that size only, so a value kept by `--reuse-values` does not approve a later scale-in to another size. It replaces `allowScaleIn`, which is no longer read. |
| `scaleInGuard.enabled`<br>default `true` | Renders the scale-in guard, a hook Job that runs before every `helm upgrade` and `helm rollback` (unreleased). A change that deletes no pod passes without calling the API. On a scale-in the guard reads `narad cluster members` as `admin` with the security Secret's `admin-password` key, and refuses while a pod being deleted is still listed, when it cannot read the list (a list with no members counts as unreadable), or when DNS lookups fail so it cannot tell which pods exist (it checks DNS with the API server's Service, `kubernetes.default.svc`, so a release with no pod yet, as on a first Argo CD sync, deletes none and passes). It needs no Kubernetes API access and runs as the namespace's `default` ServiceAccount with no token mounted (the account `serviceAccount.name` names when `serviceAccount.create=false`), so a first Argo CD sync does not wait for the ServiceAccount this chart creates. `--no-hooks` skips it for one command. See [Scale in](../operate/scaling.md#scale-in). |
| `clusterDomain`<br>default `cluster.local` | The Kubernetes cluster domain, used in the pods' DNS names. |
| `nameOverride`, `fullnameOverride`<br>default `""` | Rename the chart's objects. |

The three sizes are separate on purpose. A fresh install with `replicaCount: 7` and `initialClusterSize: 3` needs 2 of 3 votes for its first election, not 4 of 7. And the peer list does not follow `replicaCount`, so a scale operation never rolls every existing pod at the same moment as a node joins or leaves.

### Image {#image}

| Value | What it does |
|---|---|
| `image.repository`<br>default `ghcr.io/debanganthakuria/narad` | The image. |
| `image.tag`<br>default `latest` | The image tag. Pin a release, such as `v3.0.1`, so every pod runs the same build. |
| `image.pullPolicy`<br>default `IfNotPresent` |  |
| `imagePullSecrets`<br>default `[]` | Pull secrets for a private registry. |

### Broker settings {#broker}

| Value | What it does |
|---|---|
| `narad.logLevel`<br>default `info` | Sets [`log.level`](configuration.md#logging-and-security). |
| `narad.logFormat`<br>default `json` | Sets [`log.format`](configuration.md#logging-and-security). |
| `narad.defaultRetentionAgeMs`<br>default `43200000` (12 hours) | Sets [`topic.default_retention_age_ms`](configuration.md#topic-defaults). The binary's own default is 7 days; the chart keeps it short so a test cluster does not fill its disks. |
| `narad.defaultVisibilityTimeoutMs`<br>default `30000` | Sets [`topic.default_visibility_timeout_ms`](configuration.md#topic-defaults). |
| `narad.defaultPartitions`<br>default `3` | Sets [`topic.default_partitions`](configuration.md#topic-defaults). |
| `narad.maxPartitions`<br>default `108` | Sets [`topic.max_partitions`](configuration.md#topic-defaults). |
| `narad.defaultMaxInFlightPerPartition`<br>default `1024` | Sets [`topic.default_max_in_flight_per_partition`](configuration.md#topic-defaults). |
| `narad.defaultMaxAckedAheadPerPartition`<br>default `1024` | Sets [`topic.default_max_acked_ahead_per_partition`](configuration.md#topic-defaults). |
| `narad.maxConsumeWait`<br>default `10s` | Sets [`http.max_consume_wait`](configuration.md#http). A value above `10s` also needs a longer `NARAD_HTTP_SHUTDOWN_GRACE` in `extraEnv`, or the pods refuse to start. |
| `narad.pprof.enabled`<br>default `false` | Serves pprof on `service.ports.pprof` ([`http.pprof_addr`](configuration.md#http)). |
| `narad.config`<br>default `{}` | Written as JSON to the [config file](configuration.md#config-file), for settings with no environment variable, such as `storage.codec`. The pods read it only when they start. |
| `extraEnv`<br>default not set | A map of extra environment variables for the broker, such as `NARAD_HTTP_SHUTDOWN_GRACE` or `GOMEMLIMIT`. |

A values file that turns compression on:

```yaml title="values-prod.yaml"
image:
  tag: v3.0.1
narad:
  config:
    storage:
      codec: zstd
      compression_level: fastest
```

### Security {#security}

| Value | What it does |
|---|---|
| `security.enabled`<br>default `true` | Sets [`security.enabled`](configuration.md#logging-and-security): credentials and grants on the API, and the shared secret between nodes. |
| `security.existingSecret`<br>default `""` | The Secret holding the credentials; empty means `<name>-security`. See [Secrets](#secrets). |
| `security.adminPasswordKey`<br>default `admin-password` | The key of the root admin's password in that Secret. |
| `security.clusterSecretKey`<br>default `cluster-secret` | The key of the node-to-node secret in that Secret. |
| `security.clusterTLS.enabled`<br>default `false` | Mutual TLS on Raft. Turn it on for production; see [Raft TLS certificates](../operate/raft-tls.md). |
| `security.clusterTLS.secretName`<br>default `narad-cluster-tls` | The Secret holding the Raft CA and node certificate. |
| `security.clusterTLS.mountPath`<br>default `/etc/narad/cluster-tls` | Where that Secret is mounted. |
| `security.clusterTLS.certKey`, `.keyKey`, `.caKey`<br>default `tls.crt`, `tls.key`, `ca.crt` | The keys in that Secret. Set [the three `cluster_tls_*` files](configuration.md#logging-and-security). |
| `security.allowPlaintextRaft`<br>default `false` | Says that something outside the chart fences 7943/tcp and 7942/udp to the Narad pods: your own NetworkPolicy, a service mesh, a firewall. While `clusterTLS` is off, it or `networkPolicy.enabled` sets [`security.allow_plaintext_raft`](configuration.md#logging-and-security); with neither, a secured install fails. The default was `true` before (unreleased). |
| `security.allowInsecureCluster`<br>default `false` | With `security.enabled: false`, sets [`security.allow_insecure_cluster`](configuration.md#logging-and-security), without which several nodes refuse to start with security off. |
| `security.allowLegacyClusterAuth`<br>default `false` | Sets [`security.allow_legacy_cluster_auth`](configuration.md#logging-and-security), for one rolling upgrade across the change in node-to-node authentication. Not needed for a fresh install. |

What to set before production traffic is in the [Production checklist](../operate/production-checklist.md).

### Network policy {#network-policy}

**Unreleased:** in master, not in v3.0.1.

| Value | What it does |
|---|---|
| `networkPolicy.enabled`<br>default `true` | Creates a NetworkPolicy for the narad pods. It admits Raft (`service.ports.cluster`, TCP) and the node RPC plane (`service.ports.api`, UDP) from this release's pods only, and with `clusterTLS` off it is what lets the chart set [`security.allow_plaintext_raft`](configuration.md#logging-and-security); turning it off then needs `clusterTLS` or `security.allowPlaintextRaft`. It restricts ingress only. It needs a CNI that enforces NetworkPolicy; on one that does not, it is accepted and changes nothing. An upgrade with `--reuse-values` from v3.0.1, whose values have no `networkPolicy` key, renders without it, as before. |
| `networkPolicy.apiFrom`<br>default `[]` | NetworkPolicyPeer entries allowed to reach the API (`service.ports.api`, TCP). Empty means anywhere. When set, the scale-in guard's pod is admitted too. |
| `networkPolicy.metricsFrom`<br>default `[]` | Peers allowed to reach the metrics port, when `metrics.enabled`. Empty means anywhere. |
| `networkPolicy.pprofFrom`<br>default `[]` | Peers allowed to reach pprof, when `narad.pprof.enabled`. Empty keeps it closed. |
| `networkPolicy.extraIngress`<br>default `[]` | NetworkPolicyIngressRule entries appended as given. |

Kubelet probes come from the pod's own node, which a NetworkPolicy does not block, so narrowing the API or the metrics port does not fail the probes.

### Services and ports {#services}

| Value | What it does |
|---|---|
| `service.ports.api`<br>default `7942` | The API port (TCP), and the node-to-node port (UDP). |
| `service.ports.cluster`<br>default `7943` | The Raft port (TCP). |
| `service.ports.metrics`<br>default `9100` | The metrics listener, when `metrics.enabled`. |
| `service.ports.pprof`<br>default `6060` | pprof, when `narad.pprof.enabled`. |
| `service.headless.annotations`<br>default `{}` |  |
| `service.headless.publishNotReadyAddresses`<br>default `true` | Publishes pods' DNS names before they are ready, so a starting node can reach its peers. |
| `service.internal.enabled`<br>default `true` | Creates the ClusterIP Service. |
| `service.internal.type`<br>default `ClusterIP` |  |
| `service.internal.annotations`<br>default `{}` |  |
| `service.loadBalancer.enabled`<br>default `false` | Creates the LoadBalancer Service `<name>-lb`. |
| `service.loadBalancer.type`<br>default `LoadBalancer` |  |
| `service.loadBalancer.annotations`<br>default `{}` | For example, the annotations of an internal cloud load balancer. |
| `service.loadBalancer.externalTrafficPolicy`<br>default `Cluster` |  |
| `service.loadBalancer.loadBalancerSourceRanges`<br>default `[]` | Client address ranges allowed through. |

### Metrics {#metrics}

| Value | What it does |
|---|---|
| `metrics.enabled`<br>default `true` | Serves `/metrics`, `/healthz` and `/readyz` on `service.ports.metrics` without credentials ([`http.metrics_addr`](configuration.md#http)), takes `/metrics` off the API port, and moves the startup and liveness probes there. With `false`, `/metrics` stays on the API port behind the API's credentials. |
| `metrics.path`<br>default `/metrics` | The path in the scrape annotations and the ServiceMonitor. |
| `metrics.annotations`<br>default `{}` | Extra pod annotations, added next to `prometheus.io/scrape`, `prometheus.io/port` and `prometheus.io/path`. |
| `serviceMonitor.enabled`<br>default `false` | Creates a ServiceMonitor. |
| `serviceMonitor.labels`<br>default `{}` | Labels your Prometheus Operator selects on. |
| `serviceMonitor.interval`<br>default `15s` |  |
| `serviceMonitor.scrapeTimeout`<br>default `10s` |  |

Every series is in the [Metrics reference](metrics.md).

### Storage {#persistence}

| Value | What it does |
|---|---|
| `persistence.enabled`<br>default `true` | Gives each pod its own PersistentVolumeClaim. With `false`, data lives in an `emptyDir` and is lost when the pod goes. |
| `persistence.size`<br>default `10Gi` | The volume size per pod; see [Capacity and disk sizing](capacity.md#disk-sizing). |
| `persistence.storageClassName`<br>default `""` | The storage class; empty uses the cluster's default. |
| `persistence.accessModes`<br>default `[ReadWriteOnce]` |  |
| `persistence.annotations`<br>default `{}` |  |

Narad keeps one copy of each partition, on the volume of the node that owns it. Put the volumes on storage you trust; ways to keep a second copy are in [Back up and replicate topics](../operate/backups.md).

### Pods {#pods}

| Value | What it does |
|---|---|
| `resources`<br>default requests `cpu: 500m`, `memory: 512Mi`; no limits | Container resources. A memory limit also sets Go's soft memory limit to 90% of it (unreleased; see [Go runtime](configuration.md#go-runtime)). |
| `terminationGracePeriodSeconds`<br>default `30` | Time a stopping pod gets before it is killed. Give it enough to shut down cleanly: the HTTP drain (up to `http.shutdown_grace`, 10 s by default), then up to about 2 ms per partition written to since it was opened (measured on local disk under Linux; more on a network volume), so the default covers roughly 10,000 such partitions. |
| `podManagementPolicy`<br>default `Parallel` | Pods start and stop together rather than one by one. |
| `updateStrategy`<br>default `type: RollingUpdate` |  |
| `podDisruptionBudget.enabled`<br>default `true` | Creates the PodDisruptionBudget. |
| `podDisruptionBudget.maxUnavailable`<br>default `1` | Pods a voluntary eviction may take down at once. |
| `podDisruptionBudget.minAvailable`<br>default `""` | Used only when `maxUnavailable` is empty. |
| `commonLabels`<br>default `{}` | Labels added to every object's metadata, for admission policies that require them. They never reach the StatefulSet's selector or its volume claim template, which cannot change after creation. |
| `podLabels`<br>default `{}` | Labels added to the pods only. |
| `podAnnotations`<br>default `{}` | Annotations added to the pods. |
| `serviceAccount.create`<br>default `true` | Creates the ServiceAccount. |
| `serviceAccount.name`<br>default `""` | Its name; empty uses the chart's name. |
| `serviceAccount.annotations`<br>default `{}` |  |
| `serviceAccount.automountServiceAccountToken`<br>default `false` | Narad does not call the Kubernetes API. |
| `podSecurityContext`<br>default `fsGroup: 10001`, `fsGroupChangePolicy: OnRootMismatch` |  |
| `securityContext`<br>default user and group 10001, non-root, no privilege escalation, every capability dropped |  |
| `nodeSelector`, `tolerations`, `affinity`, `topologySpreadConstraints`<br>default empty | Pod placement. Spread the pods across zones if you have them. |

### Probes {#probes}

| Value | Default |
|---|---|
| `startupProbe` | `failureThreshold: 30`, `periodSeconds: 2`, `timeoutSeconds: 5` |
| `livenessProbe` | `failureThreshold: 6`, `periodSeconds: 10`, `timeoutSeconds: 5` |
| `readinessProbe` | `failureThreshold: 3`, `periodSeconds: 5`, `timeoutSeconds: 3` |

Which port each probe uses is in [Ports and probes](#ports-and-probes).

### Traefik route {#traefik}

| Value | What it does |
|---|---|
| `traefik.enabled`<br>default `false` | Creates a Traefik IngressRoute for the API. |
| `traefik.apiVersion`<br>default `traefik.containo.us/v1alpha1` |  |
| `traefik.host`<br>default `narad.example.com` | The public host name. |
| `traefik.ingressClass`<br>default `traefik` |  |
| `traefik.entryPoints`<br>default `[http]` |  |
| `traefik.middlewares`<br>default `[]` | For example, a rate limit. |
| `traefik.annotations`, `traefik.labels`<br>default `{}` |  |
| `traefik.blockedPathPrefixes`<br>default `[/metrics]` | Paths left out of the public route. Add `/healthz` and `/readyz` if nothing outside probes them. |

With another ingress controller, route the `api` port of the ClusterIP Service and deny the same paths at the controller; or keep the API off the public internet. TLS ends at the ingress, not at Narad.

## Ports and probes {#ports-and-probes}

| Port | Protocol | Serves |
|---|---|---|
| `7942` | TCP | The client API, `/healthz` and `/readyz`. `/metrics` too, behind the API's credentials, when `metrics.enabled` is `false`. |
| `7942` | UDP | Node-to-node requests over QUIC, authenticated with the cluster secret. |
| `7943` | TCP | Raft, which replicates the cluster metadata. Mutual TLS with `security.clusterTLS.enabled`. |
| `9100` | TCP | `/metrics`, `/healthz` and `/readyz`, without credentials, when `metrics.enabled`. |
| `6060` | TCP | pprof, when `narad.pprof.enabled`. |

The chart splits the probes across ports on purpose:

- **Startup and liveness** call `/healthz` on the metrics port when `metrics.enabled`, and on the API port otherwise. They ask whether the process is alive, which a busy node is. A probe on the API listener can answer late under load; a 1-second liveness timeout there once killed a healthy node, which then could not pass its startup probe while clients kept sending traffic.
- **Readiness** calls `/readyz` on the API port, always. It asks whether traffic should come here, so the listener that serves the traffic has to answer it. A stuck API listener takes the pod out of the Service, which sheds traffic and recovers on its own.

`/healthz` answers `200` from the moment the process starts, before the node has caught up, so a node recovering from a long outage is not killed by its own liveness probe, and `503` once it is shutting down. `/readyz` is checked on every request: it answers `200` only while the node's startup work is done, it has heard from a Raft leader within the last 5 seconds (or is the leader), and its copy of the metadata has caught up with the leader since the process started. A pod that loses its leader turns not ready and stops getting traffic instead of serving stale topics and users. Both are described in [Check liveness](http-api.md#healthz) and [Check readiness](http-api.md#readyz).

## Secrets {#secrets}

The chart reads credentials from a Secret named `<name>-security` (`narad-security` for a release named `narad`), or the one `security.existingSecret` names. Nothing secret goes in a values file.

| Key | Required | Meaning |
|---|---|---|
| `cluster-secret` | yes, with `security.enabled` | The secret nodes prove to each other on the node-to-node port ([`NARAD_CLUSTER_SECRET`](configuration.md#logging-and-security)). A pod does not start without it. |
| `admin-password` | no | The root admin's password ([`NARAD_ADMIN_PASSWORD`](configuration.md#logging-and-security)). Left out, the node that creates the root admin generates one and writes it to `/var/lib/narad/admin-password` on its own volume (unreleased; it used to be logged once). See [Manage the root user](../operate/users.md#root-admin). |

```sh title="Command"
kubectl create secret generic narad-security \
  --from-literal=cluster-secret="$(openssl rand -base64 32)" \
  --from-literal=admin-password="$(openssl rand -base64 24)"
```

The Raft TLS Secret (`security.clusterTLS.secretName`) is described in [Raft TLS certificates](../operate/raft-tls.md).

## Settings the chart sets {#env-mapping}

The chart passes these to every pod. Anything else goes through `extraEnv`.

| Chart value | Broker setting |
|---|---|
| `service.ports.api` | `--addr=:<port>`, [`http.addr`](configuration.md#http) |
| `service.ports.cluster` | `--cluster-port=<port>`, [`cluster.addr`](configuration.md#cluster) |
| the pod name | `--node-id`, [`cluster.node_id`](configuration.md#cluster) |
| fixed `/var/lib/narad` | `--data-dir`, [`storage.data_dir`](configuration.md#storage) |
| `narad.config` | `--config=/etc/narad/narad.json`, the [config file](configuration.md#config-file) |
| `clusterPeerCount`, `initialClusterSize`, `clusterDomain` | [`NARAD_CLUSTER_PEERS`](configuration.md#cluster) |
| the pod's headless DNS name | [`NARAD_CLUSTER_ADVERTISE_ADDR`](configuration.md#cluster) |
| `initialClusterSize` | [`NARAD_CLUSTER_INITIAL_MEMBERS`](configuration.md#cluster) |
| `narad.logLevel`, `narad.logFormat` | [`NARAD_LOG_LEVEL`, `NARAD_LOG_FORMAT`](configuration.md#logging-and-security) |
| `narad.maxConsumeWait` | [`NARAD_HTTP_MAX_CONSUME_WAIT`](configuration.md#http) |
| `narad.default*`, `narad.maxPartitions` | the six [`NARAD_TOPIC_*`](configuration.md#topic-defaults) variables |
| `narad.pprof.enabled` | [`NARAD_HTTP_PPROF_ADDR`](configuration.md#http) `=:<pprof port>` |
| `metrics.enabled` | [`NARAD_HTTP_METRICS_ADDR`](configuration.md#http) `=:<metrics port>` |
| `security.enabled` | [`NARAD_SECURITY_ENABLED`](configuration.md#logging-and-security) |
| `security.allowInsecureCluster` | `NARAD_SECURITY_ALLOW_INSECURE_CLUSTER`, only with security off |
| the Secret's two keys | `NARAD_CLUSTER_SECRET`, `NARAD_ADMIN_PASSWORD`, only with security on |
| `security.allowLegacyClusterAuth` | `NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH` |
| `security.allowPlaintextRaft`, `networkPolicy.enabled` | `NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT`, only while `clusterTLS` is off and either is set |
| `security.clusterTLS.*` | the three `NARAD_CLUSTER_TLS_*_FILE` variables, when enabled |
| `extraEnv` | one variable per entry |
