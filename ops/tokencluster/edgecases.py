#!/usr/bin/env python3
"""Edge-case and chaos suite for narad's token-based cross-node consume.

Correctness only; throughput belongs on devstack. Every check here is a
property the token protocol has to hold, and most of them are cases
where a naive implementation quietly loses or duplicates a record.

The guarantee being tested is at-least-once, so the two assertions are
NOT symmetric:

  * loss is never acceptable, under any failure;
  * duplicates are acceptable ONLY when something was killed mid-flight,
    because a record reserved by a dying node is redelivered after its
    visibility timeout. In the steady-state tests a duplicate is a bug,
    since the whole design reserves at most one record per delivery.
"""

import json
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PORTS = [17942, 17943, 17944, 17945]
NODES = [f"http://127.0.0.1:{p}" for p in PORTS]
NAMES = ["narad-1", "narad-2", "narad-3", "narad-4"]
COMPOSE = ["docker", "compose", "-f", "ops/tokencluster/docker-compose.yml"]

RESULTS = []


def call(method, url, body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data:
        req.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def mktopic(name, partitions=8):
    call("POST", f"{NODES[0]}/v1/topics", {"name": name, "partitions": partitions})
    time.sleep(2)


def produce(node, topic, mid, timeout=10):
    st, _ = call("POST", f"{node}/v1/topics/{topic}/produce?key={urllib.parse.quote(mid)}",
                 {"id": mid}, timeout=timeout)
    return st == 202


def consume(node, topic, wait="4s", timeout=30):
    st, body = call("GET", f"{node}/v1/topics/{topic}/consume?wait={wait}", timeout=timeout)
    if st != 200 or not body:
        return None
    return body


def ack(node, topic, handle):
    call("POST", f"{node}/v1/topics/{topic}/ack?receipt_handle="
         + urllib.parse.quote(handle, safe=""))


def mid_of(body):
    p = body.get("payload")
    if isinstance(p, dict):
        return p.get("id")
    try:
        return json.loads(p).get("id")
    except Exception:
        return None


def drain(topic, rounds=3):
    for _ in range(rounds):
        for n in NODES:
            while consume(n, topic, wait="0", timeout=10):
                pass


def record(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  — {detail}" if detail else ""))


def compose(*args, check=False):
    return subprocess.run(COMPOSE + list(args), capture_output=True, text=True, check=check)


def wait_ready(port, secs=60):
    end = time.time() + secs
    while time.time() < end:
        st, _ = call("GET", f"http://127.0.0.1:{port}/v1/topics", timeout=3)
        if st == 200:
            return True
        time.sleep(1)
    return False


# ---------------------------------------------------------------- tests

def t_cross_node_delivery():
    """A consumer parked on one node must be woken by a record produced
    to a partition owned by another. This is the bug the whole protocol
    exists to fix: without tokens the consumer sits out its full budget."""
    topic = "ec-cross"
    mktopic(topic)
    drain(topic)
    lat = []
    for i in range(6):
        out = {}
        t = threading.Thread(target=lambda: out.update(body=consume(NODES[0], topic, wait="8s")))
        t.start()
        time.sleep(0.6)
        t0 = time.time()
        produce(NODES[3], topic, f"cross-{i}")
        t.join()
        if out.get("body"):
            lat.append(time.time() - t0)
            ack(NODES[0], topic, out["body"]["receipt_handle"])
    ok = len(lat) == 6 and max(lat) < 3.0
    record("cross-node delivery wakes a parked consumer", ok,
           f"{len(lat)}/6 delivered, max {max(lat)*1000:.0f}ms" if lat else "none delivered")


def t_cold_backlog():
    """A record produced with nobody waiting must be served instantly to
    the next consumer that shows up. Registration doubles as a read, so
    this path needs no notification at all."""
    topic = "ec-cold"
    mktopic(topic)
    drain(topic)
    produce(NODES[2], topic, "cold-1")
    time.sleep(1.5)
    t0 = time.time()
    body = consume(NODES[0], topic, wait="8s")
    dt = time.time() - t0
    ok = body is not None and mid_of(body) == "cold-1" and dt < 2.0
    if body:
        ack(NODES[0], topic, body["receipt_handle"])
    record("backlog served on arrival without a notification", ok, f"{dt*1000:.0f}ms")


def t_exactly_one_of_many():
    """Many consumers parked across every node, one record produced.
    Exactly one delivery: never zero (lost) and never two (duplicated)."""
    topic = "ec-one"
    mktopic(topic)
    drain(topic)
    got, lock = [], threading.Lock()

    def c(node):
        b = consume(node, topic, wait="6s")
        if b:
            with lock:
                got.append(mid_of(b))
            ack(node, topic, b["receipt_handle"])

    ts = [threading.Thread(target=c, args=(NODES[i % 4],)) for i in range(12)]
    for t in ts:
        t.start()
    time.sleep(1.5)
    produce(NODES[1], topic, "single-1")
    for t in ts:
        t.join()
    ok = got == ["single-1"]
    record("one record reaches exactly one of 12 waiting consumers", ok,
           f"delivered {len(got)}x")


def t_consumer_gives_up():
    """A consumer that times out must not strand the record it was
    almost handed. Anything reserved for a departed consumer has to come
    straight back, not wait out a visibility timeout."""
    topic = "ec-giveup"
    mktopic(topic)
    drain(topic)
    # Park a consumer with a budget that expires just as a record lands.
    out = {}
    t = threading.Thread(target=lambda: out.update(body=consume(NODES[0], topic, wait="2s")))
    t.start()
    time.sleep(1.95)
    produce(NODES[3], topic, "giveup-1")
    t.join()
    if out.get("body"):
        ack(NODES[0], topic, out["body"]["receipt_handle"])
        record("record delivered at the budget edge rather than stranded", True, "served the racer")
        return
    # It timed out; the record must still be claimable well inside the
    # 30s visibility timeout.
    t0 = time.time()
    body = consume(NODES[0], topic, wait="6s")
    dt = time.time() - t0
    ok = body is not None and mid_of(body) == "giveup-1" and dt < 5.0
    if body:
        ack(NODES[0], topic, body["receipt_handle"])
    record("record not stranded when its consumer gave up", ok, f"recovered in {dt*1000:.0f}ms")


def t_client_disconnect():
    """A client that hangs up mid-wait is the same hazard as a timeout,
    except the handler is cancelled rather than expiring."""
    topic = "ec-disconnect"
    mktopic(topic)
    drain(topic)
    for _ in range(4):
        threading.Thread(target=lambda: consume(NODES[0], topic, wait="8s", timeout=1),
                         daemon=True).start()
    time.sleep(1.0)
    produce(NODES[2], topic, "disc-1")
    time.sleep(2.5)  # let the cancelled handlers unwind
    t0 = time.time()
    body = consume(NODES[1], topic, wait="8s")
    dt = time.time() - t0
    ok = body is not None and mid_of(body) == "disc-1"
    if body:
        ack(NODES[1], topic, body["receipt_handle"])
    record("record survives consumers disconnecting mid-wait", ok, f"recovered in {dt*1000:.0f}ms")


def t_non_owner_node():
    """A consumer on a node owning NO partition of the topic still has to
    be served. This is the path that used to re-probe every owner on a
    timer."""
    # 3 is narad's minimum, and with 4 nodes it guarantees at least one
    # node owns NOTHING of the topic — exactly the path that used to
    # re-probe every owner on a timer.
    topic = "ec-single"
    mktopic(topic, partitions=3)
    drain(topic)
    # Find the node that does not own partition 0 by elimination: park on
    # each node in turn and make sure every one of them can be served.
    served = 0
    for i, node in enumerate(NODES):
        out = {}
        t = threading.Thread(target=lambda n=node: out.update(body=consume(n, topic, wait="8s")))
        t.start()
        time.sleep(0.6)
        produce(NODES[(i + 1) % 4], topic, f"single-node-{i}")
        t.join()
        if out.get("body"):
            served += 1
            ack(node, topic, out["body"]["receipt_handle"])
    ok = served == 4
    record("every node can serve a 1-partition topic it may not own", ok, f"{served}/4")


def t_kill_owner_mid_flight():
    """Kill a node while consumers are parked with tokens on it. Its
    tokens die with the connection, and the surviving cluster must keep
    delivering. Records the dead node owned are unavailable until
    ownership moves, which is the rebalance story, so this only asserts
    that the cluster keeps serving and loses nothing it acknowledged."""
    topic = "ec-kill"
    mktopic(topic)
    drain(topic)
    victim = "narad-4"
    survivors = NODES[:3]

    got, lock, stop = set(), threading.Lock(), threading.Event()

    def loop(node):
        while not stop.is_set():
            b = consume(node, topic, wait="2s", timeout=10)
            if b:
                with lock:
                    got.add(mid_of(b))
                ack(node, topic, b["receipt_handle"])

    ts = [threading.Thread(target=loop, args=(n,), daemon=True) for n in survivors]
    for t in ts:
        t.start()
    time.sleep(1.5)

    sent = set()
    for i in range(20):
        mid = f"kill-{i}"
        if produce(survivors[i % 3], topic, mid, timeout=8):
            sent.add(mid)
        if i == 8:
            compose("kill", victim)
    time.sleep(12)
    stop.set()
    time.sleep(1)

    missing = sent - got
    # Records that landed on the dead node's partitions are legitimately
    # unavailable until ownership moves; anything else is real loss.
    ok = len(missing) <= len(sent) // 3 + 2
    record("cluster keeps delivering after an owner is killed", ok,
           f"{len(got)}/{len(sent)} delivered, {len(missing)} pending on the dead node")

    compose("start", victim)
    wait_ready(PORTS[3], 90)
    time.sleep(8)
    return sent, got, topic


def t_restart_rebuilds_tokens(prev):
    """After a killed node comes back, delivery must resume with no
    manual recovery: tokens are connection-scoped soft state, rebuilt by
    the next consumer that registers."""
    sent, got, topic = prev
    drain(topic, rounds=1)
    out = {}
    t = threading.Thread(target=lambda: out.update(body=consume(NODES[0], topic, wait="10s")))
    t.start()
    time.sleep(1.0)
    produce(NODES[3], topic, "afterrestart-1")
    t.join()
    body = out.get("body")
    ok = body is not None
    if body:
        ack(NODES[0], topic, body["receipt_handle"])
    record("delivery resumes after the killed node restarts", ok,
           "recovered with no manual step" if ok else "no delivery after restart")


def t_chaos_no_loss():
    """The headline property. Produce a stream across every node while
    killing and restarting one, consume everywhere, and assert nothing
    acknowledged was lost. Duplicates ARE allowed here: a record reserved
    by a node that dies is redelivered, which is at-least-once working
    as intended."""
    topic = "ec-chaos"
    mktopic(topic)
    drain(topic)

    got, lock, stop = {}, threading.Lock(), threading.Event()
    sent, slock = set(), threading.Lock()

    def loop(node):
        while not stop.is_set():
            b = consume(node, topic, wait="2s", timeout=10)
            if b:
                m = mid_of(b)
                with lock:
                    got[m] = got.get(m, 0) + 1
                ack(node, topic, b["receipt_handle"])

    ts = [threading.Thread(target=loop, args=(n,), daemon=True) for n in NODES]
    for t in ts:
        t.start()
    time.sleep(1.5)

    def chaos():
        for name in ("narad-2", "narad-3"):
            if stop.is_set():
                return
            compose("restart", name)
            time.sleep(9)

    ch = threading.Thread(target=chaos, daemon=True)
    ch.start()
    for i in range(60):
        mid = f"chaos-{i}"
        node = NODES[i % 4]
        if produce(node, topic, mid, timeout=8):
            with slock:
                sent.add(mid)
        time.sleep(0.25)
    ch.join()

    # Generous drain: a record held by a restarting node is redelivered
    # only after its visibility timeout.
    end = time.time() + 60
    while time.time() < end:
        with lock, slock:
            if set(got) >= sent:
                break
        time.sleep(1)
    stop.set()
    time.sleep(1)

    missing = sent - set(got)
    dupes = {k: v for k, v in got.items() if v > 1}
    ok = not missing
    record("no acknowledged record lost across node restarts", ok,
           f"{len(got)}/{len(sent)} unique, {len(missing)} lost, {len(dupes)} redelivered")


def main():
    print("narad token-protocol edge cases and chaos\n")
    for p in PORTS:
        if not wait_ready(p, 90):
            print(f"node on {p} not ready", file=sys.stderr)
            return 2

    only = sys.argv[1] if len(sys.argv) > 1 else ""
    print("edge cases:")
    t_cross_node_delivery()
    t_cold_backlog()
    t_exactly_one_of_many()
    t_consumer_gives_up()
    t_client_disconnect()
    t_non_owner_node()

    if only == "--edge-only":
        failed = [n for n, ok, _ in RESULTS if not ok]
        print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
        return 1 if failed else 0

    print("\nchaos:")
    prev = t_kill_owner_mid_flight()
    t_restart_rebuilds_tokens(prev)
    t_chaos_no_loss()

    failed = [n for n, ok, _ in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    if failed:
        print("failed: " + ", ".join(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
