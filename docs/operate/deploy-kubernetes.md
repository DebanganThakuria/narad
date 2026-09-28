---
description: "Install a three-node Narad cluster on Kubernetes with the Helm chart, then check that it answers."
search:
  boost: 2
---

# Deploy on Kubernetes

Install a three-node Narad cluster on Kubernetes with the Helm chart, then check that it answers.

Before you start: a Kubernetes cluster with a default storage class, `kubectl`, Helm 3 or later, `git` and `openssl`.
{: #prerequisites }

## Install {#install}

The chart lives in the repository, so the install starts from a clone of the release you mean to run.

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

    `cluster-secret` authenticates the nodes to each other and is required. `admin-password` becomes the password of the root user, `admin`. It is optional: without it, one node generates a password and logs it once (see [Manage users and grants](users.md#root-admin)).

3. Install the chart:

    ```bash
    helm install narad ./charts/narad -n narad \
      --set replicaCount=3 \
      --set persistence.size=50Gi \
      --set image.tag=v3.0.1
    ```

    Pin `image.tag` to a release. The chart's default is `latest`, which follows `master`.

4. Wait for the rollout:

    ```bash
    kubectl rollout status statefulset/narad -n narad
    ```

    A pod reports ready once it has finished starting, is in contact with the Raft leader and has caught up with it. The rollout is done when all three pods are ready.

This install runs the Raft port without TLS, which the chart allows by default. Work through the [Production checklist](production-checklist.md) before the cluster takes real traffic.

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

## Choose values {#values}

For anything beyond a trial, keep the settings in a values file. This one covers the choices most clusters make:

```yaml title="narad-values.yaml"
replicaCount: 3
initialClusterSize: 3        # set once at the first install

image:
  tag: v3.0.1                # pin a release

persistence:
  size: 50Gi                 # per pod

narad:
  config:                    # rendered to the --config JSON file
    storage:
      codec: zstd            # compression is off by default
      compression_level: fastest

resources:
  limits:
    memory: 2Gi

extraEnv:
  GOMEMLIMIT: 1800MiB        # about 90% of the memory limit
```

```bash
helm upgrade narad ./charts/narad -n narad -f narad-values.yaml
```

- **`initialClusterSize`** is the set of pods allowed to create a new cluster. Pods beyond it join the existing one. It is read only on an empty disk, so set it at the first install and leave it.
- **`codec: zstd`** turns on compression of stored messages. Measure the saving on your own payloads; already compressed payloads gain little.
- **`GOMEMLIMIT`** gives the Go runtime a soft memory limit, so it collects garbage harder before the pod reaches its memory limit. Builds after v3.0.1 set it to 90% of the pod's memory limit on their own when it is unset (unreleased).

Adding or removing `narad.config` as a whole changes the pods, so Helm rolls them. A change inside it only rewrites the ConfigMap, and a pod reads its config file only when it starts, so restart the pods after one:

```bash
kubectl rollout restart statefulset/narad -n narad
```

Every value, with its default, is in the [Helm values reference](../reference/helm-values.md#values). For disk size, see [Capacity and disk sizing](../reference/capacity.md#disk-sizing).

## Next steps

- [Production checklist](production-checklist.md): secure and size the cluster before it takes real traffic.
- [Monitor and alert](monitoring.md): scrape the metrics and set up the five alerts.
- [Helm values reference](../reference/helm-values.md): look up every chart value and the ports and probes.
