---
template: home.html
description: "Narad is a message broker in one Go binary. Producers POST a message and get 202 once it is fsynced to disk; consumers pull it, work under a lease and ack."
hide:
  - navigation
  - toc
  - path
---

<div class="nr-hero" markdown>

<div class="nr-hero__intro" markdown>

# A small, sturdy message broker. HTTP in, <span class="nr-nowrap">at-least-once</span> out.

<p class="nr-lead">Narad is one Go binary. A producer POSTs a message and gets <code>202</code> once it is fsynced to disk. A consumer pulls it, works on it under a lease and acks it. Anything left unacked comes back.</p>

[Run it locally](client/cli.md#the-sixty-second-demo){ .md-button .md-button--primary }

<div class="nr-doors" markdown>

- [<span class="nr-doors__tab">Build<span class="nr-sr">:</span></span> <span class="nr-doors__what">Produce, consume and ack from your service.</span>](client/index.md)
- [<span class="nr-doors__tab">Operate<span class="nr-sr">:</span></span> <span class="nr-doors__what">Deploy with Helm, monitor, scale, upgrade.</span>](operate/index.md)
- [<span class="nr-doors__tab">Understand<span class="nr-sr">:</span></span> <span class="nr-doors__what">Storage, Raft, rebalance, the delivery contract.</span>](internals/index.md)

</div>

</div>

<figure class="nr-term" aria-labelledby="nr-term-caption">
<div class="nr-term__bar" aria-hidden="true"><span>Terminal</span><span>produce, consume, ack</span></div>
<pre tabindex="0"><code><span class="p">$</span> NARAD=http://127.0.0.1:7942
<span class="p">$</span> curl -i -X POST "$NARAD/v1/topics/orders/produce?key=customer-42" \
    -H 'Content-Type: application/json' \
    -d '<span class="payload">{"order_id":"ord_123","amount":4999}</span>'
<span class="s">HTTP/1.1 202 Accepted</span>
<span class="h">Date: Mon, 28 Sep 2026 10:49:16 GMT</span>
<span class="h">Content-Length: 0</span>

<span class="p">$</span> curl "$NARAD/v1/topics/orders/consume?wait=10s"
{"topic":"orders","partition":1,"offset":0,"key":"customer-42",
 "payload":<span class="payload">{"order_id":"ord_123","amount":4999}</span>,
 "timestamp":1790592556,"receipt_handle":"1:0:8874198393259169482"}

<span class="p">$</span> curl -i -X POST -H 'Content-Type: application/json' \
    "$NARAD/v1/topics/orders/ack?receipt_handle=1:0:8874198393259169482"
<span class="s">HTTP/1.1 204 No Content</span>
<span class="h">Date: Mon, 28 Sep 2026 10:49:21 GMT</span></code></pre>
<figcaption id="nr-term-caption">A recorded session against <code>narad server start --dev</code>, after creating the topic <code>orders</code>. The consume response is one line; it is wrapped here to fit.</figcaption>
</figure>

</div>

<div class="nr-sections" markdown>

<section class="nr-section" markdown>
<div class="nr-section__text" markdown>

## Install locally with Homebrew or Docker

The `narad` binary is both the broker and the CLI. `narad server start --dev` runs one node on `127.0.0.1:7942` with auth off, which is what the examples on this page talk to.

[The CLI: install, contexts and every command](client/cli.md){ .nr-more }

</div>
<div class="nr-section__example" markdown>

=== "Homebrew"

    ```sh
    brew install debanganthakuria/narad/narad
    narad server start --dev
    ```

=== "Docker"

    ```sh
    docker run -p 7942:7942 -v narad-data:/var/lib/narad \
      -e NARAD_SECURITY_ENABLED=false \
      ghcr.io/debanganthakuria/narad:v3.0.1
    ```

</div>
</section>

<section class="nr-section" markdown>
<div class="nr-section__text" markdown>

## Fan-out children copy every message

Create a topic with a `parent` and every message committed to the parent from then on is copied into it, with its own consumers and retention. Producers change nothing.

[Fan-out & delay](client/fanout-and-delay.md){ .nr-more }

</div>
<div class="nr-section__example" markdown>

```sh
curl -X POST "$NARAD/v1/topics" \
  -H 'Content-Type: application/json' \
  -d '{"name": "orders-analytics", "parent": "orders"}'
```

</div>
</section>

<section class="nr-section" markdown>
<div class="nr-section__text" markdown>

## Delayed delivery through a delay child

A child with `delay_ms` receives each message that long after the parent committed it. Retry queues and cool-downs need no scheduler.

[Delay children](client/fanout-and-delay.md#delay-children){ .nr-more }

</div>
<div class="nr-section__example" markdown>

```sh
curl -X POST "$NARAD/v1/topics/orders/children" \
  -H 'Content-Type: application/json' \
  -d '{"child": "orders-retry", "delay_ms": 3600000}'
```

</div>
</section>

<section class="nr-section" markdown>
<div class="nr-section__text" markdown>

## Schema validation at the broker

Give a topic a JSON Schema and Narad checks every produce before writing it. A body that does not fit gets `400` naming the field, and never reaches the log.

[Schemas](client/schemas.md){ .nr-more }

</div>
<div class="nr-section__example" markdown>

```sh
curl -X POST "$NARAD/v1/topics" -H 'Content-Type: application/json' \
  -d '{"name": "invoices", "schema": {"type": "object",
       "properties": {"id": {"type": "integer"}}, "required": ["id"]}}'

curl -X POST "$NARAD/v1/topics/invoices/produce" \
  -H 'Content-Type: application/json' -d '{"id": "one"}'
```

```json title="Response: 400 Bad Request"
{"error":"invalid argument: schema: jsonschema validation failed with 'narad://schema/invoices/1#'\n- at '/id': got string, want integer"}
```

</div>
</section>

<section class="nr-section" markdown>
<div class="nr-section__text" markdown>

## Helm deployment on Kubernetes

The chart runs a StatefulSet with Raft inside every pod: no ZooKeeper and no external metadata store. It installs from a clone of this repository after one secret.

[Deployment, step by step](operate/index.md){ .nr-more }

</div>
<div class="nr-section__example" markdown>

```sh
git clone https://github.com/DebanganThakuria/narad.git && cd narad
kubectl create namespace narad
kubectl create secret generic narad-security -n narad \
  --from-literal=cluster-secret="$(openssl rand -base64 32)"
helm install narad ./charts/narad -n narad --set replicaCount=3
```

</div>
</section>

<section class="nr-section" markdown>
<div class="nr-section__text" markdown>

## Delivery guarantees, stated plainly

A `202` means the message is on disk and will be delivered at least once, so handlers must be idempotent. Order is not guaranteed. Acking twice shows the lease at work: the second ack gets `410 Gone`.

[Guarantees & errors](client/guarantees-and-errors.md){ .nr-more } · [How Narad compares](compare.md){ .nr-more }

</div>
<div class="nr-section__example" markdown>

```sh
curl -i -X POST -H 'Content-Type: application/json' \
  "$NARAD/v1/topics/orders/ack?receipt_handle=1:0:350573132236924935"
```

```http title="Response to the second ack"
HTTP/1.1 410 Gone
Content-Length: 67
Content-Type: application/json
Date: Mon, 28 Sep 2026 10:48:01 GMT

{"error":"receipt handle no longer matches an active reservation"}
```

</div>
</section>

</div>
