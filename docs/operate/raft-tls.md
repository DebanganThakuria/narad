---
description: "Turn on mutual TLS for the Raft port, then renew and rotate its certificates without losing messages."
---

# Raft TLS certificates: enable and rotate

Turn on mutual TLS for the Raft port, then renew and rotate its certificates without losing messages.

Before you start: the Narad namespace ([Deploy on Kubernetes](deploy-kubernetes.md#install) step 1), OpenSSL 3, and permission to create secrets in it. Enabling TLS on a running cluster also needs that cluster.

Every node presents a certificate that carries the DNS name `narad-cluster.local`, and checks its peers' certificates against a CA bundle. The name is the same for every node: membership of the cluster is the identity, not the pod's hostname, so all nodes can share one certificate. A node reads the files once, at startup. Replacing them changes nothing until the node restarts, so every change below is a rolling restart. Why Raft needs this is in [Networking and security](../understand/networking-and-security.md#raft-tls).

## Create the certificates {#create-certificates}

Create a CA, then one node certificate signed by it. The node certificate needs the DNS name `narad-cluster.local`, and it is used both to serve and to dial, so it needs both TLS usages.

```bash
openssl req -x509 -new -nodes \
  -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout ca.key -out ca.crt -days 3650 \
  -subj "/CN=narad-cluster-ca"
openssl req -new -nodes \
  -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -keyout tls.key -out tls.csr \
  -subj "/CN=narad-cluster"
printf '%s\n' \
  'subjectAltName=DNS:narad-cluster.local' \
  'extendedKeyUsage=serverAuth,clientAuth' > node.ext
openssl x509 -req -in tls.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out tls.crt -days 365 -extfile node.ext
```

Check the result:

```bash
openssl verify -CAfile ca.crt tls.crt
openssl x509 -in tls.crt -noout -ext subjectAltName,extendedKeyUsage
```

```text title="Output"
tls.crt: OK
X509v3 Subject Alternative Name: 
    DNS:narad-cluster.local
X509v3 Extended Key Usage: 
    TLS Web Server Authentication, TLS Web Client Authentication
```

Store the three files in the secret the chart mounts. Its default name is `narad-cluster-tls`, with the keys `tls.crt`, `tls.key` and `ca.crt`:

```bash
kubectl create secret generic narad-cluster-tls -n narad \
  --from-file=tls.crt \
  --from-file=tls.key \
  --from-file=ca.crt
```

Keep `ca.key` somewhere safe and outside the cluster. You need it to issue the next certificate.

Installing from a values file? Go back to [Deploy on Kubernetes](deploy-kubernetes.md#install) step 3. Otherwise continue below.

## Enable on a new cluster {#enable-new-cluster}

Create both secrets (the security secret from [Deploy on Kubernetes](deploy-kubernetes.md#install) and the TLS secret above), then install with TLS on:

```bash
helm install narad ./charts/narad -n narad \
  --set replicaCount=3 \
  --set persistence.size=50Gi \
  --set image.tag=v3.0.1 \
  --set security.clusterTLS.enabled=true \
  --set networkPolicy.enabled=true
```

With TLS on, the chart never tells the node that plaintext Raft is fenced, so `security.allowPlaintextRaft` does not matter. `networkPolicy.enabled` still keeps the node RPC plane (7942/udp) and Raft to the Narad pods ([Fence the cluster ports](production-checklist.md#network-policy); unreleased, the v3.0.1 chart ignores it).

Each node logs which transport it runs. Check one:

```bash
kubectl logs -n narad narad-0 \
  | grep -oE 'raft metadata transport (is plaintext|secured with mutual TLS)'
```

```text title="Output"
raft metadata transport secured with mutual TLS
```

## Enable on a running cluster {#enable-running-cluster}

A node on TLS and a node on plaintext cannot talk Raft to each other, so the switch has a short pause in the middle.

!!! warning "Topic, user and schema changes pause"
    While neither side holds a majority of the nodes, the cluster has no Raft leader and refuses topic, user, grant and schema changes. In a local three-node run this lasted a few seconds. Produce and consume carry on, because the node-to-node data plane does not use Raft. Plan a quiet moment for the switch.

<figure class="nr-dia nr-dia--doc" id="fig-raft-tls-enable">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/raft-tls-enable.html"
</div>
<figcaption>Raft follows the majority. The rolling update stops at the first pod on TLS; deleting <code>narad-1</code> by hand gives TLS the majority, and the rollout then finishes by itself. Only the moment in between, with a majority on neither side, has no Raft leader.</figcaption>
</figure>

1. Create the certificates and the TLS secret as in [Create the certificates](#create-certificates).
2. Turn TLS on:

    ```bash
    helm upgrade narad ./charts/narad -n narad --reuse-values \
      --set security.clusterTLS.enabled=true \
      --set security.allowPlaintextRaft=false
    ```

    `allowPlaintextRaft=false` clears a `true` that `--reuse-values` carries over from an older chart's default, so that a later change that turns TLS off is refused unless it names a fence.

    The StatefulSet restarts the highest-numbered pod first. That pod runs TLS, cannot reach its plaintext peers and stays not ready. The rolling update waits for it to become ready, so it stops here. The other pods keep their leader and keep serving.

3. Restart the next pods down by hand until a majority runs TLS. A deleted pod comes back with the new settings. On three nodes, one deletion is enough:

    ```bash
    kubectl delete pod narad-1 -n narad
    ```

    On five nodes, delete `narad-3`, wait for it to start, then delete `narad-2`. Once a majority runs TLS, those pods elect a leader and turn ready, and the rolling update restarts the remaining pods on its own.

4. Wait for every pod to be ready, then check the log line on each pod as in [Enable on a new cluster](#enable-new-cluster):

    ```bash
    kubectl rollout status statefulset/narad -n narad
    ```

## Watch the expiry {#expiry}

**Unreleased:** in master, not in v3.0.1.

Each node exports when its certificate and the earliest-expiring CA in its bundle expire, as `narad_raft_tls_cert_not_after_seconds` with `kind="leaf"` and `kind="ca"` ([Metrics reference](../reference/metrics.md#metastore-raft)). Alert well ahead, for example on `narad_raft_tls_cert_not_after_seconds - time() < 7 * 86400` ([Monitor and alert](monitoring.md#node-health-alerts)). The node also checks the dates at startup and then every hour, and logs:

| When | Level | Line starts with |
|---|---|---|
| At startup | info | `raft TLS certificate in use`, with `not_before`, `not_after` and `ca_not_after` |
| At startup, before the certificate's `not_before` | error | `raft TLS certificate is not valid yet` |
| 30 days, then 7 days, before it expires | warning | `raft TLS certificate expires in less than 30 days` (or `7 days`) |
| 1 day before it expires | error | `raft TLS certificate expires in less than a day` |
| Once it has expired, then every 24 hours | error | `raft TLS certificate has expired` |

The CA's lines say `raft TLS CA certificate` instead. The expiry lines end with the same reminder: Narad reads the files only at startup, so a renewed certificate takes effect only after a [rolling restart](#renew).

Once the certificate, or every CA in the bundle, has expired, peers refuse the new Raft connections the node opens or accepts, and connections opened before the expiry carry on until they break. While the node is otherwise ready, `/readyz` keeps answering `200` and lists the expiry under `degraded`:

```text title="Output"
{"status":"ready","degraded":["raft_tls_certificate_expired"]}
```

`raft_tls_ca_expired` means every CA in the bundle has expired. The expiry does not fail readiness: one certificate usually serves every node and expires on all of them at once, and failing readiness would take every pod out of its Services at the same moment.

Renew before the expiry, as below. Once the certificate has expired on every node, a pod restarted with a new one and its peers on the old one refuse each other, and a rolling restart stops at its first pod; [Troubleshooting](troubleshooting.md#log-raft-tls-expired) says how to get past that.

## Renew node certificates {#renew}

A new certificate from the same CA needs no ordering. Issue it, replace the secret, and restart the pods one at a time:

```bash
kubectl create secret generic narad-cluster-tls -n narad \
  --from-file=tls.crt \
  --from-file=tls.key \
  --from-file=ca.crt \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart statefulset/narad -n narad
```

The rolling restart waits for each pod to be ready before it restarts the next, which is what a certificate change needs.

## Rotate the CA {#rotate-ca}

Rotating the CA takes three rolling restarts. Between them, every node trusts the certificate every other node presents, so Raft never loses a connection.

<figure class="nr-dia nr-dia--doc" id="fig-raft-tls-ca-rotation">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/raft-tls-ca-rotation.html"
</div>
<figcaption>Trust the new CA first, move the certificates second, drop the old CA last. At every roll boundary each pod trusts the certificate every other pod presents; skip roll 1 and the first pod on a new certificate cannot join.</figcaption>
</figure>

1. **Trust both CAs.** Make `ca.crt` the old CA followed by the new one (`cat old-ca.crt new-ca.crt > ca.crt`), replace the secret, and roll.
2. **Move to certificates from the new CA.** Issue a node certificate from the new CA, replace `tls.crt` and `tls.key` in the secret, and roll. Nodes still trust the old CA, so pods on the old and new certificates keep talking during this roll.
3. **Drop the old CA.** Make `ca.crt` the new CA alone, replace the secret, and roll. From here a certificate from the old CA is refused.

Do not skip the first roll. A node that starts on a new-CA certificate while its peers trust only the old CA cannot join ([below](#untrusted-cert)).

## Restart cost {#restart-cost}

Measured with the scripted scenarios in `tests/cluster` (`go test -tags cluster -run TestTLS ./tests/cluster/`), which run a three-node cluster under continuous produce and consume load and check that every accepted message is acked:

| Restart | Raft | Node ready again |
|---|---|---|
| A follower | No election; the leader keeps serving | under 1 s |
| The leader, hand-over succeeds | The leader logs `leadership transferred before shutdown`; the others go leaderless for milliseconds at most and hold one election | about 1 s |
| The leader, hand-over fails | The leader logs `leadership transfer on shutdown failed`; the others notice at their heartbeat timeout, about 1.2 s without a leader, and hold one election | about 1 to 2 s |

Renewal and the three-roll CA rotation each ran under load with no message lost. In a renewal run on 2026-09-29, 12,000 messages were produced and all 12,000 acked, with 114 delivered twice. Messages delivered twice, after a lease expired during a restart, are expected: that is what [idempotent handlers](../reference/glossary.md#idempotent-handler) are for.

## Untrusted certificate {#untrusted-cert}

A node started with a certificate signed by a CA its peers do not trust does not join and does not become ready. The logs say why:

- The node itself logs `failed to decode incoming command: error="remote error: tls: bad certificate"` with `component=raft`.
- The leader logs `raft: failed to heartbeat to: peer=<addr>` with `error="tls: failed to verify certificate: x509: certificate signed by unknown authority"`.
- The node's `/readyz` answers `503`, so a readiness-gated rollout stops at it.

The other nodes keep their leader and their clients. The misconfigured node still reaches its peers over the node RPC plane, so it is not marked dead and its partitions do not move. They are unavailable until it is fixed, and `GET /v1/topics/{topic}` for a topic with a partition on it answers `500` (`get topic failed`) from the other nodes. Give the node a certificate its peers trust and restart it; it rejoins within seconds.

## Next steps

- [Production checklist](production-checklist.md#network-policy): fence the cluster ports as well.
- [Upgrade Narad](upgrade.md): roll the cluster to a new release the same way.
- [Troubleshooting](troubleshooting.md): match other log lines to their cause.
