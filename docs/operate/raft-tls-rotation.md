# Rotating the Raft TLS certificates

The Raft metadata transport runs over mutual TLS when
`NARAD_CLUSTER_TLS_CERT_FILE`, `_KEY_FILE` and `_CA_FILE` are set (see
[Configuration](configuration.md)). Every voter presents a certificate
carrying the DNS SAN `narad-cluster.local` and verifies its peers against
the CA bundle; cluster membership is the identity, not the pod's hostname.

**The files are read once, at startup.** Replacing a certificate, key or
CA bundle on disk changes nothing for a running node; the node keeps
serving and dialling with what it loaded. There is no live reload,
deliberately: a restart is the reload, and a restart under load costs
nothing measurable (below), so a reload path would be code with no
benefit. Rotation is therefore a rolling restart, and the only rule is
that **every node must trust the certificate every other node presents at
every moment of the roll**, which is what the phases below arrange.

Restarts must be one node at a time. Wait for `/readyz` to answer 200 on
the restarted node before restarting the next one; on Kubernetes the
StatefulSet's rolling update does exactly that.

## Renewing node certificates (same CA)

Nothing to sequence: issue the new certificate from the same CA, replace
the files, restart the node. Repeat per node, in any order.

```bash
# per node, after the new cert.pem / key.pem are in place
kubectl rollout restart statefulset/narad   # one pod at a time, readiness-gated
```

## Rotating the CA

Three rolls. Between rolls every node trusts what every other node
presents, so the transport never breaks.

1. **Trust both CAs.** Put the old and the new CA certificates in the CA
   bundle file (concatenated PEM) on every node. Roll.
2. **Move to certificates from the new CA.** Replace each node's
   certificate and key with ones signed by the new CA. Roll. Peers still
   trust the old CA, so a node on the old certificate and one on the new
   certificate keep talking through this roll.
3. **Drop the old CA.** Reduce the bundle to the new CA on every node.
   Roll. From here a certificate signed by the old CA is refused.

Do not skip the first roll: a node started on a new-CA certificate while
its peers trust only the old CA cannot join (see the failure mode below).

## What a restart costs

Measured with the scripted scenarios in `tests/cluster`
(`go test -tags cluster -run TestTLS ./tests/cluster/`), which run the
three-node cluster under continuous produce/consume load with the
`tests/integration` chaos driver and check every accepted message is
acked exactly once:

| Roll | Consensus | Node ready again |
|---|---|---|
| A follower restarts | No election; the leader keeps serving | ~1s |
| The leader restarts (`SIGTERM`) | Leadership is handed over before exit (`leadership transferred before shutdown` in its log); survivors leaderless for milliseconds, one election | ~1-2s |
| The leader restarts and the hand-over fails | The leader logs `leadership transfer on shutdown failed` and exits; survivors notice at their heartbeat timeout and elect a leader: about 1.2s without a leader, one election | ~1-2s |

Certificate renewal and the full three-roll CA rotation each ran under
load with zero message loss; duplicates (redelivery after a lease
expired during a restart) are normal and are what the client's
idempotent handling is for.

## Failure mode: a certificate the peers do not trust

A node started with a certificate signed by a CA its peers do not trust
does not join and does not become ready, and it says why:

- the node itself logs `failed to decode incoming command:
  error="remote error: tls: bad certificate"` (its peers' handshake
  attempts are rejected) under `component=raft`;
- the leader logs `failed to heartbeat to: peer=<addr> ... error="tls:
  failed to verify certificate: x509: certificate signed by unknown
  authority"` and the same for `failed to appendEntries to`, naming the
  peer;
- the node's `/readyz` stays `503` (`replica has not caught up with the
  leader since start`), so a readiness-gated rollout stops at it.

The other two nodes are unaffected: the leader keeps its term, no
election happens, and the client load carries on. Partitions the
misconfigured node owns are unavailable until it is fixed (it still
heartbeats over the cluster RPC plane, so it is not marked dead and its
partitions are not moved); give it a trusted certificate and restart it
and it rejoins in a few seconds. One side effect to know about while it
is in that state: `GET /v1/topics/{topic}` for a topic with a partition
on the misconfigured node answers `421` from every node, because the
per-partition stats merge asks each owner and that owner refuses. The
topic list, users and the cluster members endpoint are unaffected.
