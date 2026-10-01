---
description: "Point any HTTP client at a Narad node and authenticate each request with a username and password."
search:
  boost: 2
---

# Connect and authenticate

Point any HTTP client at a Narad node and authenticate each request with a username and password.

Before you start: the address of a Narad node or of the load balancer in front of the nodes, and a username and password from your operator. No cluster yet? [Quickstart](../get-started/quickstart.md) runs one on your machine in a minute; set `AUTH` to `:` for it.

Set the base URL and your credentials once per shell:

```sh
export NARAD="http://127.0.0.1:7942"
export AUTH="billing-service:your-password"
```

- `$NARAD` is the base URL of any Narad node, or of the load balancer in front of them, for example `http://127.0.0.1:7942`.
- `$AUTH` is `username:password` for a user your operator created, here `billing-service`. Quote it, so a password with shell characters survives. A local node with authentication off, such as `narad server start --dev`{.nr-nowrap}, ignores credentials: `export AUTH=":"` (an empty user and password).

Then call the node:

```sh title="List the topics you can see"
curl -i -u "$AUTH" "$NARAD/v1/topics"
```

```http title="Response"
HTTP/1.1 200 OK
Content-Length: 288
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:30:07 GMT

{
  "next_page_token": "",
  "topics": [
    {
      "name": "orders",
      "id": "71f9b6869df02ef3",
      "partitions": 3,
      "retention_ms": 604800000,
      "visibility_timeout_ms": 30000,
      "max_in_flight_per_partition": 1024,
      "max_acked_ahead_per_partition": 1024,
      "created_at": 1790623535,
      "owner": "billing-service",
      "role": "standalone"
    }
  ]
}
```

The list holds only the topics you may read, so an empty list can mean there are none or that you hold no grant on any.

## Base URL {#base-url}

Every node serves the whole API, on port 7942 by default. A node serves what it holds and forwards the rest: a produce is accepted by whichever node you reach, a consume or an ack goes on to the node that owns the partition, and a topic change goes on to the cluster leader. So any node, or a load balancer over all of them, is a valid base URL.

- API paths start with `/v1`. The probes `/healthz` and `/readyz` sit outside it and need no credentials.
- Narad speaks plain HTTP. Anywhere off your own machine, terminate TLS in front of it and use an `https://` base URL, so passwords never cross the network in the clear.
- `narad server start --dev`{.nr-nowrap} binds `http://127.0.0.1:7942` and turns authentication off.

## Credentials {#credentials}

Narad uses HTTP Basic auth on every `/v1` request. Your operator creates one user per service and gives it [grants](../reference/glossary.md#grant): an action (`produce`, `consume`, `create` or `admin`) on topic names or prefixes such as `orders-*`. What each action allows is in [Access model and grants](../reference/access-model.md); how an admin creates users is in [Manage users and grants](../operate/users.md#create-user).

Pass the credentials the way your client expects them:

=== "curl"

    ```sh
    curl -u "$AUTH" "$NARAD/v1/topics"
    ```

=== "Go SDK"

    ```go
    client, err := narad.New(os.Getenv("NARAD_ADDR"),
        narad.WithAuth("billing-service", os.Getenv("NARAD_PASS")))
    ```

=== "CLI"

    ```sh
    export NARAD_ADDR="$NARAD"
    export NARAD_USER="billing-service"
    export NARAD_PASS="your-password"
    narad topic ls
    ```

Missing or wrong credentials get [`401`](../reference/status-codes.md#status-401) with a Basic challenge:

```http title="Response"
HTTP/1.1 401 Unauthorized
Content-Length: 36
Content-Type: application/json
Www-Authenticate: Basic realm="narad"
Date: Mon, 28 Sep 2026 19:18:44 GMT

{"error":"authentication required"}
```

- Valid credentials without the right grant get [`403`](../reference/status-codes.md#status-403), for example `{"error":"produce not allowed on this topic"}`.
- After five wrong passwords for one username, the node answers [`429`](../reference/status-codes.md#status-429) with `too many failed authentication attempts` and allows one more attempt every 12 seconds. Fix the password rather than retrying in a loop.

## Required headers {#required-headers}

A `POST`, `PUT` or `PATCH` must carry `Content-Type: application/json`{.nr-nowrap} or `application/octet-stream`{.nr-nowrap}, or a non-empty `X-Narad-Client`{.nr-nowrap} header. Anything else gets [`415`](../reference/status-codes.md#status-415). `curl -d` on its own sends a form content type, so it is refused:

```sh title="A produce without an accepted content type"
curl -i -u "$AUTH" -X POST \
  "$NARAD/v1/topics/orders/produce?key=customer-42" \
  -d '{"order_id": "ord_123"}'
```

```http title="Response"
HTTP/1.1 415 Unsupported Media Type
Content-Length: 134
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:18:45 GMT

{"error":"state-changing requests must send Content-Type: application/json or application/octet-stream, or an X-Narad-Client header"}
```

- The rule is a guard against cross-site requests from a browser, not a format check: a message body is stored as the same bytes whichever of the two types you send. [Networking and security](../understand/networking-and-security.md#cross-site-guard) explains the attack it stops.
- `GET` and `DELETE` requests are not checked, with one exception: a [batch consume](consuming.md#consume-batch) (`GET .../consume?max=N`) without `X-Narad-Client` gets [`400`](../reference/status-codes.md#status-400). A consume reserves messages, so a batch consume needs the header that forces a preflight. With `curl`, add `-H 'X-Narad-Client: curl'`.
- The Go SDK and the CLI set `X-Narad-Client` on every request, so they never see this `415`.

## Limits {#limits}

| Limit | Default | Over the limit |
|---|---|---|
| Request body | 1 MiB (1,048,576 bytes) | [`413`](../reference/status-codes.md#status-413) |
| Request headers | 64 KiB, `http.max_header_bytes` | `431`, with a plain-text body from Go's HTTP server |
| Concurrent consume requests per identity, per node | 1024, `http.max_consume_in_flight_per_identity` | [`429`](../reference/status-codes.md#status-429) |
| Concurrent produce requests per identity, per node (unreleased) | off, `http.max_produce_in_flight_per_identity` | [`429`](../reference/status-codes.md#status-429) |
| Long-poll `wait` on a consume | 10 s, `http.max_consume_wait` | clamped, with an `X-Narad-Wait-Clamped` response header |
| Open connections per node | 4096, `http.max_connections` | extra connections wait to be accepted |

- An identity is the authenticated user, or the client IP address when authentication is off.
- A long poll counts against the consume cap for as long as it waits, so a consumer with many workers can reach it.
- Operators change these values in the [configuration reference](../reference/configuration.md#http).

## Next steps

- [Manage topics](topics.md): create the topic your service will use.
- [Produce messages](producing.md): send your first message with these credentials.
- [Go SDK: produce and consume from Go](go-sdk.md): let the client handle credentials, headers and retries.
