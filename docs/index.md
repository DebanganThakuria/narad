---
hide:
  - navigation
  - toc
---

<div class="narad-hero" markdown>

![Narad — durable messages, timeless connections](assets/narad-logo.png){ .narad-hero-logo }

<p class="narad-tagline">Durable messages. Timeless connections.</p>

<p class="narad-sub">A message broker that respects your weekend. Plain HTTP in, at-least-once out, and nothing to babysit in between.</p>

[Get started in 5 minutes](client/index.md){ .md-button .md-button--primary }
[See how it works](internals/index.md){ .md-button }

</div>

<div class="narad-section" markdown>

## Hit any pod. Narad does the rest.

Produce, consume, ack — send every request to the **load balancer** and stop thinking. There is no "find the right broker," no partition leader discovery, no client-side metadata protocol. Whatever pod your request lands on, it's the right pod.

```mermaid
sequenceDiagram
    autonumber
    participant You
    participant LB as Load balancer
    participant P as whichever pod
    participant O as the owning pod
    You->>LB: POST /produce
    LB->>P: any pod at all
    P->>P: fsync to disk — your message is safe
    P-->>You: 202 Accepted
    Note over P,O: the magic you never see
    P->>O: routed, committed, verified
    O-->>You: ready for consumers
```

Under the hood, the pod that catches your produce makes it **durable on disk before answering**, then finds the partition's owner, hands it over, and retries through failures until it's committed and verified. Consumes and acks route themselves the same way. You brought an HTTP client; that was your entire job.

</div>

<div class="narad-section" markdown>

## Consume without the ceremony

No consumer groups. No rebalancing storms. No partition assignment protocols, generation IDs, or "stop the world, someone joined." Run **one worker or a hundred** against the same topic — each message goes to exactly one of them, and if a worker dies mid-job, its messages quietly come back for the others.

```mermaid
flowchart LR
    T[("topic: orders")] --> W1[worker]
    T --> W2[worker]
    T --> W3["worker (just deployed)"]
    W1 -->|ack| T
    W2 -->|"crashed? message returns"| T
    W3 -->|ack| T
```

Scale your consumers by... starting more consumers. That's the whole runbook.

</div>

<div class="narad-section" markdown>

## Deploys like it's nothing

A load balancer, a StatefulSet, and persistent volumes. **That is the complete architecture.** No ZooKeeper. No BookKeeper. No keeper of any kind — no sidecar quorum service, no external metadata store, no six-component "getting started" diagram.

```mermaid
flowchart TB
    LB[Load balancer]
    subgraph ss["StatefulSet — Raft built into every pod"]
        direction TB
        N0[narad-0] --- V0[("PV<br/>EBS-backed")]
        N1[narad-1] --- V1[("PV<br/>EBS-backed")]
        N2[narad-2] --- V2[("PV<br/>EBS-backed")]
    end
    LB --> N0 & N1 & N2
```

Cluster metadata lives in **Raft, inside the same binary**. Scaling out is raising `replicaCount` — new pods find the cluster, join it, and start taking work. One Helm chart, one image, one thing to understand.

```bash
helm install narad ./charts/narad --set replicaCount=3
```

</div>

<div class="narad-section" markdown>

## Everything you actually need from a broker

<div class="grid cards" markdown>

- :material-shield-check: **Durable before acknowledged**

    A `202` means your message is fsynced to disk — not buffered, not "probably." Crash a millisecond later; the message survives. Delivery is at-least-once, ordered per key.

- :material-call-split: **Fan-out, built in**

    Attach child topics to a parent and every message is copied to each child automatically. Analytics, billing, and audit each get their own independent stream — producers change nothing.

- :material-clock-outline: **Delayed delivery, built in**

    A child with `delay_ms` delivers each message exactly N ms after the parent committed it. Retry queues and cool-downs without cron jobs, external schedulers, or sorted-set hacks.

- :material-timer-sand: **Leases, not black holes**

    Consumed messages are invisible while you work, redelivered if you vanish, extendable if you're slow, and returnable if you give up. `410 Gone` tells you honestly when you lost the race.

- :material-lock-outline: **Access control included**

    Users, bcrypt, per-action grants with prefix wildcards, topic ownership. Give billing `produce` on `invoices.*` and nothing else. No plugin, no gateway.

- :material-magnify: **Replay on demand**

    Everything is a retained log underneath. Point a consume at any offset within retention and re-read history — without disturbing the live queue.

</div>

</div>

<div class="narad-section" markdown>

## Paranoia, professionally applied

We didn't unit-test our way to confidence — we **force-killed live nodes under traffic, for days**, with a harness that catches every single lost message. Leader kills, double kills, quorum loss, kills mid-rolling-restart, kills during scale-out.

<div class="narad-stats" markdown>

<div class="narad-stat"><span class="narad-stat-num">0</span><span class="narad-stat-label">messages lost across the entire chaos matrix</span></div>
<div class="narad-stat"><span class="narad-stat-num">4</span><span class="narad-stat-label">data-loss bugs found by chaos testing — and fixed before you got here</span></div>
<div class="narad-stat"><span class="narad-stat-num">1</span><span class="narad-stat-label">rule everywhere: no node destroys data without the leader confirming it</span></div>

</div>

The full war stories — including the bug that only bit the node that *won* an election — are written up honestly in [Cluster Lifecycle](internals/cluster-lifecycle.md). We think a broker that shows you its scars is one you can trust.

</div>

<div class="narad-section narad-final" markdown>

## Convinced? Good. Here's the manual.

<div class="grid cards" markdown>

- :material-rocket-launch: **[Client Guide](client/index.md)**

    Zero to produce–consume–ack in five minutes with `curl`, then topics, fan-out, delay, access control, and the exact fine print of every guarantee.

- :material-cog: **[Internals](internals/index.md)**

    Every subsystem with diagrams: the WAL-first produce path, the storage engine, Raft metadata, fan-out cursors, and how crash recovery really works.

</div>

</div>
