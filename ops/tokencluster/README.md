# Four-node token-protocol cluster

Correctness harness for narad's cross-node consume path. Throughput
belongs on devstack; this exists to prove the protocol behaves under
failure.

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath \
  -o ops/tokencluster/build/narad ./cmd/narad
cp ops/tokencluster/Dockerfile.local ops/tokencluster/build/Dockerfile

docker compose -f ops/tokencluster/docker-compose.yml up -d --build
python3 ops/tokencluster/edgecases.py              # edge cases + chaos
python3 ops/tokencluster/edgecases.py --edge-only  # skip the slow chaos runs
docker compose -f ops/tokencluster/docker-compose.yml down -v
```

`build/` is gitignored: the image is a thin wrapper over a host-built
binary, because the repo Dockerfile compiles from source in-image and is
too slow to iterate on.

## Two settings that are not incidental

`NARAD_CLUSTER_ADDR` is `:7943`, port-only. It has to match this node's
own entry in the peer list so bootstrap excludes self from the voter
set; a bind-all `0.0.0.0:7943` does not match `narad-N:7943` and seeds
raft with a duplicate ID.

`NARAD_CLUSTER_ADVERTISE_ADDR` is set per node. Only raft reads it here
— the token protocol derives its own return address through
`peerMemberAddr`, since claims travel on the node-RPC port (7942), not
the raft port. Getting that wrong is silent: registration succeeds,
notifications are sent to a port that does not serve them, and every
consumer waits out its full budget as if the feature were not there.

## What the suite covers

Edge cases: cross-node wake-up, cold backlog served without a
notification, exactly-one delivery among many waiting consumers, a
consumer that gives up at the budget edge, clients disconnecting
mid-wait, and a node that owns none of the topic.

Chaos: killing an owner while consumers hold tokens on it, restarting it
and confirming delivery resumes with no manual step, and a produce
stream across all four nodes while two of them restart.

Loss is never acceptable. Duplicates are, but only in the chaos tests: a
record reserved by a node that dies is redelivered after its visibility
timeout, which is at-least-once working as intended. In the steady-state
tests a duplicate is a bug.
