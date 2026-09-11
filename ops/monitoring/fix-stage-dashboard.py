#!/usr/bin/env python3
"""Make the Narad Stage dashboard honest under a 60s scrape.

Root cause: the pods are scraped every 60s but the datasource declares no
scrape interval, so Grafana sizes $__rate_interval for 15s scrapes. A window
of 60s over 60s samples holds one point (gaps) and any burst shorter than the
window is averaged down. Fix: an explicit rate window variable (default 2m,
two scrapes), a 1m minimum step, and a row of run totals and peaks that do
not depend on the window at all.
"""
import json, re, sys

src, dst = sys.argv[1], sys.argv[2]
d = json.load(open(src))
db = d["dashboard"] if "dashboard" in d else d

SEL = 'cluster=~"$cluster",job=~"$job",k8s_pod=~"$pod",k8s_pod=~"narad-.*"'
DS = {"type": "prometheus", "uid": "$datasource"}

# 1. rate window variable + minimum step
opts = ["2m", "1m", "3m", "5m", "10m"]
window = {
    "type": "custom", "name": "window", "label": "Rate window",
    "description": "Window for every rate()/increase(). The pods are scraped every 60s, so keep this at two scrapes or more; 1m shows single-sample gaps.",
    "query": ",".join(opts),
    "current": {"selected": True, "text": "2m", "value": "2m"},
    "options": [{"selected": o == "2m", "text": o, "value": o} for o in opts],
    "hide": 0, "includeAll": False, "multi": False, "skipUrlSync": False,
}
tl = db["templating"]["list"]
tl[:] = [v for v in tl if v["name"] != "window"] + [window]
db["interval"] = "1m"

# 2. every $__rate_interval -> $window
n = 0
def walk(panels):
    global n
    for p in panels:
        for t in p.get("targets", []):
            e = t.get("expr")
            if e and "$__rate_interval" in e:
                t["expr"] = e.replace("[$__rate_interval]", "[$window]")
                n += e.count("[$__rate_interval]")
        if p.get("panels"):
            walk(p["panels"])
walk(db["panels"])

# 3. run totals row: counts over the picked range and the true peak rate,
#    both independent of how a burst lines up with the window.
def stat(pid, x, w, title, expr, unit, desc, decimals=0):
    return {
        "id": pid, "type": "stat", "title": title, "description": desc,
        "datasource": DS, "gridPos": {"h": 3, "w": w, "x": x, "y": 8},
        "targets": [{"datasource": DS, "expr": expr, "refId": "A", "instant": False, "range": True}],
        "fieldConfig": {"defaults": {"unit": unit, "decimals": decimals, "color": {"mode": "thresholds"},
                        "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "none", "graphMode": "none", "justifyMode": "auto", "orientation": "auto",
                    "textMode": "auto", "wideLayout": True},
    }

ack = SEL + ',route=~".*/ack",status="204"'
prod = SEL + ',route=~".*/produce",status="202"'
# The per-partition message counters are keyed by topic and partition. A topic
# deleted and recreated under the same name drops and climbs again inside one
# 60s scrape, which Prometheus reads as a small increase rather than a reset,
# so those counters undercount across test runs. The HTTP counters are
# long-lived series; totals and peaks come from them (one request is one
# message; there is no batching).
new = [
    stat(79, 0, 6, "Produced in range",
         f'sum(increase(narad_http_requests_total{{{prod}}}[$__range]))', "short",
         "Produce requests accepted (202) across the selected time range, from the long-lived HTTP counter. The per-topic message counters are keyed by topic and partition; a topic deleted and recreated under the same name drops and climbs again inside one 60s scrape, which Prometheus reads as a small increase, so those counters undercount across test runs."),
    stat(80, 6, 6, "Acked in range (logical PCA)",
         f'sum(increase(narad_http_requests_total{{{ack}}}[$__range]))', "short",
         "Messages that completed the whole produce, consume, ack flow (ack 204) in the selected range."),
    stat(81, 12, 6, "Peak produce/s",
         f'max_over_time(sum(rate(narad_http_requests_total{{{prod}}}[$window]))[$__range:1m])', "reqps",
         "Highest produce rate seen at any minute of the range, averaged over the rate window. This is the number a load test is looking for."),
    stat(82, 18, 6, "Peak ack/s",
         f'max_over_time(sum(rate(narad_http_requests_total{{{ack}}}[$window]))[$__range:1m])', "reqps",
         "Highest ack (204) rate at any minute of the range, averaged over the rate window: peak logical messages per second."),
]
ids = {79, 80, 81, 82}
def shift(panels):
    for p in panels:
        if p.get("id") in ids:
            continue
        g = p.get("gridPos")
        if g and g["y"] >= 8:
            g["y"] += 3
        if p.get("panels"):
            shift(p["panels"])
db["panels"] = [p for p in db["panels"] if p.get("id") not in ids]
shift(db["panels"])
# place after the Overview stats (y=5,h=3 -> next free y is 8)
insert_at = next(i for i, p in enumerate(db["panels"]) if p.get("type") == "row" and p.get("title") == "Traffic")
db["panels"][insert_at:insert_at] = new

# 4. say what the per-second stats are
for p in db["panels"]:
    if p.get("title") in ("Produce rate", "Consume rate", "Ack rate"):
        p["description"] = (p.get("description") or "") + " Averaged over the $window rate window (pods are scraped every 60s). For a short burst read the peak and totals below."
    if p.get("title") == "Messages per second":
        p["description"] = "rate() over the $window window; a burst shorter than the window is averaged down, see 'Peak produce/s' and 'Produced in range'."

json.dump(db, open(dst, "w"), indent=2)
print(f"replaced {n} rate windows; panels top-level {len(db['panels'])}; version {db.get('version')}")
