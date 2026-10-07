---
description: "Run Narad on your machine, then create a topic and produce, consume and ack one message with curl, in about five minutes."
search:
  boost: 2
---

# Quickstart: send your first message

Run Narad on your machine, then create a topic and produce, consume and ack one message with curl, in about five minutes.

Before you start: curl, plus one of Docker, Homebrew, or Go 1.26 or later with git and make.

## Step 1: start Narad {#start}

Each option below runs one Narad node on your machine, serving its API on port 7942 with authentication off.

=== "Docker"

    ```bash
    docker run --rm -p 127.0.0.1:7942:7942 \
      -v narad-data:/var/lib/narad \
      -e NARAD_SECURITY_ENABLED=false \
      -e NARAD_CLUSTER_ADDR=127.0.0.1:7943 \
      ghcr.io/debanganthakuria/narad:v3.2.1
    ```

    The container logs one JSON line per event. Narad is serving once a line contains `"msg":"http listening"`.

    - `-p 127.0.0.1:7942:7942` publishes the API on your machine only, which matters because authentication is off.
    - `-v narad-data:/var/lib/narad` keeps your topics and messages in a Docker volume named `narad-data`.
    - `NARAD_CLUSTER_ADDR` gives the node's internal cluster port an address it can advertise. Without it the container exits at once with `local bind address is not advertisable`.

=== "Homebrew"

    ```bash
    brew install debanganthakuria/narad/narad
    narad server start --dev
    ```

    Homebrew builds Narad from source, so the install takes a minute or more. `--dev` binds the node to 127.0.0.1:7942, turns authentication off and keeps data in `~/.narad/data`. It prints this banner, then log lines:

    ```text title="Output"
      narad  dev mode: auth OFF, bound to loopback only

      Try it from another terminal:

        narad topic add demo
        narad sub demo --peek        # terminal B: watch messages flow
        narad pub demo '{"hello":"narad"}' --count 100 --rate 20

      Or plain curl:

        curl -X POST 'http://127.0.0.1:7942/v1/topics' -H 'Content-Type: application/json' -d '{"name":"demo"}'
    ```

    The `narad` commands in the banner are the CLI's own demo, described in [Narad CLI](../build/cli.md#watch-messages-flow).

=== "From source"

    ```bash
    git clone --branch v3.2.1 --depth 1 https://github.com/DebanganThakuria/narad
    cd narad
    make build
    ./bin/narad server start --dev
    ```

    `make build` writes the binary to `bin/narad`. Build from the release tag rather than with `go install`: `@latest` resolves to an old v1 release, because the module path has no `/v3` suffix. `--dev` binds the node to 127.0.0.1:7942, turns authentication off and keeps data in `~/.narad/data`. It prints this banner, then log lines:

    ```text title="Output"
      narad  dev mode: auth OFF, bound to loopback only

      Try it from another terminal:

        narad topic add demo
        narad sub demo --peek        # terminal B: watch messages flow
        narad pub demo '{"hello":"narad"}' --count 100 --rate 20

      Or plain curl:

        curl -X POST 'http://127.0.0.1:7942/v1/topics' -H 'Content-Type: application/json' -d '{"name":"demo"}'
    ```

    The `narad` commands in the banner are the CLI's own demo, described in [Narad CLI](../build/cli.md#watch-messages-flow).

Leave Narad running. Open a second terminal and check that the node is ready:

```bash
export NARAD=http://127.0.0.1:7942
curl -i "$NARAD/readyz"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 19
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:56 GMT

{"status":"ready"}
```

- `$NARAD` is the base URL of the node. Every command on this page uses it.

A `503` means the node is still starting; try again after a second or two. Authentication is off on this node, so the commands on this page send no credentials. A real cluster, such as one from [Deploy on Kubernetes](../operate/deploy-kubernetes.md), needs a username and password on every request: see [Connect and authenticate](../build/connect.md#credentials).

## Step 2: create a topic {#create-topic}

```bash
curl -i -X POST "$NARAD/v1/topics" \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "visibility_timeout_ms": 300000}'
```

```http title="Response"
HTTP/1.1 201 Created
Content-Length: 209
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:57 GMT

{"name":"orders","id":"74b7c37f76f5ae56","partitions":3,
"retention_ms":604800000,"visibility_timeout_ms":300000,
"max_in_flight_per_partition":1024,"max_acked_ahead_per_partition":1024,
"created_at":1790623916}
```

The [topic](../reference/glossary.md#topic) `orders` now exists, with the server's defaults for everything you did not set: 3 partitions, and messages kept for 7 days (`retention_ms`). `visibility_timeout_ms` gives a consumer 5 minutes to ack each message, instead of the default 30 seconds, so you have time to copy a value in step 5.

Every `POST` on this page sends `Content-Type: application/json`. Without it, Narad answers `415 Unsupported Media Type`.

## Step 3: produce a message {#produce}

```bash
curl -i -X POST "$NARAD/v1/topics/orders/produce?key=customer-42" \
  -H "Content-Type: application/json" \
  -d '{"order_id": "ord_123", "amount": 4999}'
```

```http title="Response"
HTTP/1.1 202 Accepted
Date: Mon, 28 Sep 2026 19:31:57 GMT
Content-Length: 0
```

The request body is the message: any bytes, up to 1 MiB. The [key](../reference/glossary.md#key) `customer-42` keeps messages about one customer on the same partition. [Produce messages](../build/producing.md) covers keys, binary payloads and every status code.

--8<-- "contract/produce-202.md"

## Step 4: consume it {#consume}

```bash
curl -i "$NARAD/v1/topics/orders/consume?wait=10s"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 180
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:57 GMT

{"topic":"orders","partition":1,"offset":0,"key":"customer-42",
"payload":{"order_id": "ord_123", "amount": 4999},
"timestamp":1790623917,"receipt_handle":"1:0:3742135424316369939"}
```

- `payload` is the JSON you produced, byte for byte.
- `wait=10s` makes the request wait up to 10 seconds for a message, then answer `204 No Content` if none arrived.
- The message is now leased to you: no other consumer gets it until you ack it or the topic's visibility timeout (5 minutes here) runs out.
- `receipt_handle` names this one delivery. You send it back to ack the message; treat it as an opaque string. See [receipt handle](../reference/glossary.md#receipt-handle).

If this answers `204 No Content` at once, run it again after a few seconds: a node that started moments ago may not have placed the topic's partitions yet. [Consume and acknowledge messages](../build/consuming.md) covers leases, extends and nacks in full.

## Step 5: ack it {#ack}

<figure class="nr-dia nr-dia--doc" id="fig-quickstart-handle">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--sky">
--8<-- "diagrams/quickstart-handle.html"
</div>
<figcaption>Copy the <code>receipt_handle</code> from the consume answer into the ack. It names this one delivery, so it settles the message only while your lease lasts.</figcaption>
</figure>

Put the `receipt_handle` from your step 4 response in a variable. Yours differs from the one shown here, so replace the value:

```bash
HANDLE='1:0:3742135424316369939'
```

Then ack the message:

```bash
curl -i -X POST \
  "$NARAD/v1/topics/orders/ack?receipt_handle=$HANDLE" \
  -H "Content-Type: application/json"
```

```http title="Response"
HTTP/1.1 204 No Content
Date: Mon, 28 Sep 2026 19:31:57 GMT
```

The message is settled. Run the step 4 command again: it waits 10 seconds and answers `204 No Content`, because `orders` has nothing left to deliver. An ack that comes after the lease ran out answers `410 Gone` instead, and the message goes to the next consume.

--8<-- "contract/at-least-once.md"

To stop Narad, press Ctrl+C in the first terminal. Your topic and message stay on disk for the next start: in the `narad-data` volume with Docker, or in `~/.narad/data` with `--dev`.

## Next steps {#next-steps}

- [Core concepts](concepts.md): the ideas behind topics, keys, leases and acks.
- [Produce messages](../build/producing.md): keys, binary payloads and what each status code means.
- [Go SDK](../build/go-sdk.md): produce and consume from a Go service.
