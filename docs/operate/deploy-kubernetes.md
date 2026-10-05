---
description: "Install a three-node Narad cluster on Kubernetes with the Helm chart, then check that it answers."
search:
  boost: 2
---

# Deploy on Kubernetes

Install a three-node Narad cluster on Kubernetes with the Helm chart, then check that it answers.

Before you start: a Kubernetes cluster with a default storage class, `kubectl`, Helm 3 or later, `git` and `openssl`.
{: #prerequisites }

## Decide before you install {#values}

Three choices are cheap before the install and costly after it: the disk size, the Raft transport and the founding cluster size. Keep them, and the rest of your settings, in a values file. This one covers the choices most production clusters make:

```yaml title="narad-values.yaml"
replicaCount: 3
initialClusterSize: 3        # set once, at the first install

image:
  tag: v3.0.1                # pin a release

persistence:
  size: 50Gi                 # per pod; fixed once installed

narad:
  defaultRetentionAgeMs: 604800000  # 7 days; the chart's default is 12 hours
  config:                    # rendered to the --config JSON file
    storage:
      codec: zstd            # compression is off by default
      compression_level: fastest

security:
  clusterTLS:
    enabled: true            # needs the narad-cluster-tls secret
  allowPlaintextRaft: false

resources:
  limits:
    memory: 2Gi

extraEnv:
  GOMEMLIMIT: 1800MiB        # about 90% of the memory limit
```

Save it as `narad-values.yaml` in the clone you make under [Install](#install), or pass its path to `-f`.

- **`persistence.size`** is the volume of each pod. The StatefulSet fixes it once installed, and a `helm upgrade` that changes it fails. Size it with [Capacity and disk sizing](../reference/capacity.md#disk-sizing).
- **`narad.defaultRetentionAgeMs`** is the retention of a topic created without `retention_ms`. The chart's default is 12 hours and the binary's is 7 days. Retention deletes unacked messages too, so pick a value longer than your longest consumer outage.
- **Raft TLS.** For production, keep `security.clusterTLS.enabled: true` and `security.allowPlaintextRaft: false` in the file, and create the TLS secret in [Install](#install) step 2. Turning TLS on later needs a pause of topic and user changes ([Enable on a running cluster](raft-tls.md#enable-running-cluster)).
- **`initialClusterSize`** is the set of pods allowed to create a new cluster. Pods beyond it join the existing one. It is read only on an empty disk, so set it at the first install and leave it.
- **`codec: zstd`** turns on compression of stored messages. Measure the saving on your own payloads; already compressed payloads gain little.
- **`GOMEMLIMIT`** gives the Go runtime a soft memory limit, so it collects garbage harder before the pod reaches its memory limit. Builds after v3.0.1 set it to 90% of the pod's memory limit on their own when it is unset (unreleased).

Every value, with its default, is in the [Helm values reference](../reference/helm-values.md#values).

## Install {#install}

The chart lives in the repository, so the install starts from a clone of the release you mean to run.

<figure class="nr-dia nr-dia--doc" id="fig-deploy-topology">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/deploy-topology.html"
</div>
<figcaption>For a release called <code>narad</code>: TLS ends at your ingress, clients reach the pods through the Service <code>narad</code> on <code>7942/tcp</code>, and the pods reach each other directly on <code>7942/udp</code> and <code>7943/tcp</code>. Each pod keeps its own volume, <code>data-narad-0</code> for <code>narad-0</code> and so on.</figcaption>
</figure>

1. Get the chart and create a namespace:

    ```bash
    git clone --branch v3.0.1 --depth 1 \
      https://github.com/DebanganThakuria/narad
    cd narad
    kubectl create namespace narad
    ```

2. Create the security secret. The chart reads a secret named `<release>-security`, so for a release called `narad` it is `narad-security`:

    ```bash
    kubectl create secret generic narad-security -n narad \
      --from-literal=cluster-secret="$(openssl rand -base64 32)" \
      --from-literal=admin-password="$(openssl rand -base64 24)"
    ```

    `cluster-secret` authenticates the nodes to each other and is required. `admin-password` becomes the password of the root user, `admin`. It is optional: without it, one node generates a password and writes it to a file on its own volume (see [Manage users and grants](users.md#root-admin)). With Raft TLS on, also create the `narad-cluster-tls` secret now ([Create the certificates](raft-tls.md#create-certificates)).

3. Install the chart with your values file:

    ```bash
    helm install narad ./charts/narad -n narad -f narad-values.yaml
    ```

    For a trial, skip the file and set the few values that matter on the command line. This runs the Raft port without TLS, fenced to the Narad pods by the chart's NetworkPolicy:

    ```bash
    helm install narad ./charts/narad -n narad \
      --set replicaCount=3 \
      --set persistence.size=50Gi \
      --set image.tag=v3.0.1 \
      --set networkPolicy.enabled=true
    ```

    The policy holds only on a CNI that enforces NetworkPolicy ([Fence the cluster ports](production-checklist.md#network-policy)). Without TLS or the policy, a chart after v3.0.1 refuses to install and names the alternatives (unreleased); the v3.0.1 chart has no policy, ignores `networkPolicy.enabled` and runs the Raft port unfenced.

    Pin `image.tag` to a release either way. The chart's default is `latest`, which follows `master`.

4. Wait for the rollout:

    ```bash
    kubectl rollout status statefulset/narad -n narad
    ```

    A pod reports ready once it has finished starting, is in contact with the Raft leader and has caught up with it. The rollout is done when all three pods are ready.

Work through the [Production checklist](production-checklist.md) before the cluster takes real traffic.

## Verify {#verify}

Forward the API port to your machine and keep the command running:

```bash
kubectl port-forward -n narad svc/narad 7942:7942
```

In a second terminal, read the admin password from the secret and call the cluster:

```bash
export NARAD=http://127.0.0.1:7942
export AUTH="admin:$(kubectl get secret narad-security -n narad \
  -o jsonpath='{.data.admin-password}' | base64 -d)"
curl -s "$NARAD/readyz"
curl -s -u "$AUTH" "$NARAD/v1/topics"
```

```text title="Output"
{"status":"ready"}
{"next_page_token":"","topics":[]}
```

- `$NARAD` is the base URL of any node or of the load balancer in front of them.
- `$AUTH` is `username:password` for a user with the grant the request needs. Here it is the root admin.

`/readyz` needs no credentials, and the topic list is empty on a new cluster. Next, create a user for each service ([Manage users and grants](users.md)) and point clients at the cluster ([Connect and authenticate](../build/connect.md)).

## Verify the image {#verify-image}

**Unreleased:** in master, not in v3.0.1.

Images are signed keyless with [cosign](https://github.com/sigstore/cosign) and carry an SBOM and build provenance. Signing starts with the first image built after the v3.0.1 release. Images up to and including v3.0.1 carry no signature, so the command below fails for them.

```bash
IDENTITY='^https://github\.com/DebanganThakuria/narad/'
IDENTITY+='\.github/workflows/container\.yml@refs/(heads/master|tags/v.*)$'
cosign verify ghcr.io/debanganthakuria/narad:v3.0.1 \
  --certificate-identity-regexp "$IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The identity is pinned to one workflow file, on `master` or on a release tag. Matching the repository alone would accept a signature from any workflow on any branch.

## Change values later {#change-values}

Edit the values file and upgrade:

```bash
helm upgrade narad ./charts/narad -n narad -f narad-values.yaml
```

Adding or removing `narad.config` as a whole changes the pods, so Helm rolls them. A change inside it only rewrites the ConfigMap, and a pod reads its config file only when it starts, so restart the pods after one:

```bash
kubectl rollout restart statefulset/narad -n narad
```

## Next steps

- [Production checklist](production-checklist.md): secure and size the cluster before it takes real traffic.
- [Monitor and alert](monitoring.md): scrape the metrics and set up the six alerts.
- [Helm values reference](../reference/helm-values.md): look up every chart value and the ports and probes.
