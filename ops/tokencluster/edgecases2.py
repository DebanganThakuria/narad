#!/usr/bin/env python3
"""The edge cases the first suite missed.

Every test here corresponds to a specific hazard raised while designing
the token protocol. They are the ones a naive implementation passes by
accident, so several assert on TIMING as well as outcome: a record that
arrives after a claim deadline instead of a round trip is technically
delivered but the mechanism underneath is broken.
"""

import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PORTS = [17942, 17943, 17944, 17945]
NODES = [f"http://127.0.0.1:{p}" for p in PORTS]
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


def consume(node, topic, wait="4s", timeout=40):
    st, body = call("GET", f"{node}/v1/topics/{topic}/consume?wait={wait}", timeout=timeout)
    return body if st == 200 and body else None


def ack(node, topic, handle):
    call("POST", f"{node}/v1/topics/{topic}/ack?receipt_handle="
         + urllib.parse.quote(handle, safe=""))


def mid_of(b):
    p = b.get("payload")
    return p.get("id") if isinstance(p, dict) else None


def drain(topic, rounds=3):
    for _ in range(rounds):
        for n in NODES:
            while consume(n, topic, wait="0", timeout=10):
                pass


def record(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  — {detail}" if detail else ""))


def park(node, topic, wait, out, key="body"):
    """Park a consumer in a thread; returns the thread."""
    t = threading.Thread(target=lambda: out.update({key: consume(node, topic, wait=wait)}))
    t.start()
    return t


# --------------------------------------------------------------- tests

def t_second_waiter_gets_it():
    """'Will C ever get the message?'

    B is offered the record but is gone by the time the offer lands. C is
    waiting on another node. C must be served, and QUICKLY — the owner
    should discover B is gone via a pass and move on, not wait out a
    claim deadline. A slow pass here means the record sat idle while a
    healthy consumer waited."""
    topic = "e2-second"
    mktopic(topic)
    drain(topic)

    # B parks with a budget that expires before the record is produced.
    b_out = {}
    tb = park(NODES[1], topic, "1s", b_out)
    # C parks with a long budget.
    c_out = {}
    tc = park(NODES[2], topic, "15s", c_out)
    time.sleep(1.6)  # B has now given up; its token may still be live.

    t0 = time.time()
    produce(NODES[3], topic, "second-1")
    tc.join()
    tb.join()
    dt = time.time() - t0

    body = c_out.get("body")
    ok = body is not None and mid_of(body) == "second-1" and dt < 4.0
    if body:
        ack(NODES[2], topic, body["receipt_handle"])
    record("a still-waiting consumer is served after another has gone", ok,
           f"{dt*1000:.0f}ms (must not wait out a claim deadline)")


def t_all_tokens_stale():
    """'The message keeps circulating.'

    Every consumer is gone, so every token is stale. The owner burns
    through them and stops — it cannot manufacture more. The record must
    then still be sitting there, unreserved, for the next arrival."""
    topic = "e2-stale"
    mktopic(topic)
    drain(topic)

    threads = [park(NODES[i], topic, "1s", {}) for i in range(4)]
    time.sleep(1.5)          # all four gave up, all four tokens stale
    produce(NODES[0], topic, "stale-1")
    for t in threads:
        t.join()
    time.sleep(3)            # let the owner spend and exhaust every token

    t0 = time.time()
    body = consume(NODES[1], topic, wait="10s")
    dt = time.time() - t0
    ok = body is not None and mid_of(body) == "stale-1"
    if body:
        ack(NODES[1], topic, body["receipt_handle"])
    record("record survives every token going stale", ok,
           f"recovered in {dt*1000:.0f}ms, unreserved as expected" if ok else "record lost")


def t_concurrent_claims_no_duplicate():
    """Two consumers on different nodes, one record, both racing to
    claim. Reservation is atomic, so exactly one wins; the loser must
    re-park rather than answering empty, and pick up the NEXT record."""
    topic = "e2-race"
    mktopic(topic)
    drain(topic)

    outs = [{} for _ in range(2)]
    ts = [park(NODES[i], topic, "12s", outs[i]) for i in (0, 1)]
    time.sleep(1.5)
    produce(NODES[2], topic, "race-1")
    time.sleep(1.5)
    produce(NODES[3], topic, "race-2")   # the loser must get this one
    for t in ts:
        t.join()

    got = sorted(mid_of(o["body"]) for o in outs if o.get("body"))
    for i, o in enumerate(outs):
        if o.get("body"):
            ack(NODES[i], topic, o["body"]["receipt_handle"])
    ok = got == ["race-1", "race-2"]
    record("a losing claim re-parks and takes the next record", ok,
           f"delivered {got}")


def t_ttl_expiry_no_ghost():
    """A token whose TTL runs out must be discarded rather than spent. If
    expiry were ignored, the owner would waste a notification on a
    consumer that left minutes ago and hold the record for a claim
    deadline while a live consumer waited."""
    topic = "e2-ttl"
    mktopic(topic)
    drain(topic)

    # Consumer with a short budget leaves a short-TTL token behind.
    park(NODES[0], topic, "2s", {}).join()
    time.sleep(3)   # its TTL has certainly lapsed

    # A live consumer elsewhere must be served promptly, not behind a
    # ghost token's deadline.
    out = {}
    t = park(NODES[2], topic, "12s", out)
    time.sleep(1.0)
    t0 = time.time()
    produce(NODES[3], topic, "ttl-1")
    t.join()
    dt = time.time() - t0
    body = out.get("body")
    ok = body is not None and dt < 3.0
    if body:
        ack(NODES[2], topic, body["receipt_handle"])
    record("an expired token does not delay a live consumer", ok, f"{dt*1000:.0f}ms")


def t_inflight_cap_no_hot_loop():
    """consumable() is an O(1) estimate and can over-report: records that
    exist but cannot be reserved. If the pump kept notifying on that, it
    would spin. Hold a batch of records unacked (in flight) and confirm
    the cluster stays responsive and serves the rest."""
    topic = "e2-cap"
    mktopic(topic)
    drain(topic)

    for i in range(12):
        produce(NODES[i % 4], topic, f"cap-{i}")
    time.sleep(2)

    # Take several without acking, so they sit reserved.
    held = []
    for i in range(6):
        b = consume(NODES[i % 4], topic, wait="3s")
        if b:
            held.append(b)
    time.sleep(3)  # a spinning pump would burn CPU here

    # The remaining records must still be servable.
    got = 0
    for i in range(6):
        b = consume(NODES[i % 4], topic, wait="4s")
        if b:
            got += 1
            ack(NODES[i % 4], topic, b["receipt_handle"])
    ok = got >= 5 and len(held) == 6
    record("unacked in-flight records do not wedge delivery", ok,
           f"{len(held)} held, {got}/6 remaining served")


def t_rebalance_under_live_tokens():
    """Ownership moving while tokens are live. A token at a node that no
    longer owns any of the topic is dead weight; the consumer must be
    served by whoever holds the partitions now."""
    topic = "e2-rebalance"
    mktopic(topic)
    drain(topic)

    # Park consumers so tokens are spread everywhere, then move
    # ownership by taking a node out and bringing it back.
    outs = [{} for _ in range(3)]
    ts = [park(NODES[i], topic, "25s", outs[i]) for i in range(3)]
    time.sleep(1.5)
    subprocess.run(COMPOSE + ["restart", "narad-4"], capture_output=True)
    time.sleep(10)

    for i in range(3):
        produce(NODES[i], topic, f"rebal-{i}")
    for t in ts:
        t.join()

    got = [mid_of(o["body"]) for o in outs if o.get("body")]
    for i, o in enumerate(outs):
        if o.get("body"):
            ack(NODES[i], topic, o["body"]["receipt_handle"])
    ok = len(got) >= 2
    record("consumers still served while ownership churns", ok,
           f"{len(got)}/3 delivered during a node restart")


def t_delete_topic_with_live_tokens():
    """Deleting a topic while tokens are registered against it must not
    take the cluster down or wedge a node. The tokens are worthless the
    moment the topic is gone."""
    topic = "e2-delete"
    mktopic(topic)
    drain(topic)

    ts = [park(NODES[i], topic, "6s", {}) for i in range(4)]
    time.sleep(1.5)
    st, _ = call("DELETE", f"{NODES[0]}/v1/topics/{topic}", timeout=20)
    for t in ts:
        t.join()
    time.sleep(3)

    # Every node must still be healthy and serving.
    alive = sum(1 for n in NODES if call("GET", f"{n}/v1/topics", timeout=5)[0] == 200)
    probe = "e2-delete-after"
    mktopic(probe)
    produce(NODES[1], probe, "after-delete")
    time.sleep(1)
    body = consume(NODES[2], probe, wait="8s")
    if body:
        ack(NODES[2], probe, body["receipt_handle"])
    ok = alive == 4 and body is not None
    record("deleting a topic with live tokens leaves the cluster healthy", ok,
           f"{alive}/4 nodes healthy, follow-up delivery {'ok' if body else 'FAILED'}")


def t_repeated_poll_cycle():
    """A consumer that times out and immediately re-polls, over and over.
    This is the ordinary client loop, and it exercises token
    registration and retirement churn. Delivery must stay correct and
    prompt across many cycles rather than degrading."""
    topic = "e2-cycle"
    mktopic(topic)
    drain(topic)

    delivered, lat = 0, []
    for i in range(8):
        out = {}
        t = park(NODES[i % 4], topic, "3s", out)
        time.sleep(0.5)
        t0 = time.time()
        produce(NODES[(i + 2) % 4], topic, f"cycle-{i}")
        t.join()
        if out.get("body"):
            delivered += 1
            lat.append(time.time() - t0)
            ack(NODES[i % 4], topic, out["body"]["receipt_handle"])
    ok = delivered == 8 and (not lat or max(lat) < 2.5)
    record("repeated poll/timeout cycles keep delivering promptly", ok,
           f"{delivered}/8, max {max(lat)*1000:.0f}ms" if lat else "none")


def main():
    print("token protocol: the cases the first suite missed\n")
    t_second_waiter_gets_it()
    t_all_tokens_stale()
    t_concurrent_claims_no_duplicate()
    t_ttl_expiry_no_ghost()
    t_inflight_cap_no_hot_loop()
    t_repeated_poll_cycle()
    t_rebalance_under_live_tokens()
    t_delete_topic_with_live_tokens()

    failed = [n for n, ok, _ in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(failed)}/{len(RESULTS)} passed")
    if failed:
        print("failed:")
        for f in failed:
            print(f"  - {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
