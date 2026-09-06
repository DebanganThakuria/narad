# Narad

[![CI](https://github.com/DebanganThakuria/narad/actions/workflows/ci.yml/badge.svg)](https://github.com/DebanganThakuria/narad/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/DebanganThakuria/narad)](./go.mod)

<p align="center">
  <img src="./assets/narad.png" alt="Narad logo: durable messages, timeless connections" width="420">
</p>

Narad is a queue-first message broker in a single Go binary: plain HTTP in, at-least-once out.
A `202` means the message is fsynced before you hear back. Consumers pull, work under a
visibility lease, and ack; anything unacked comes back. Fan-out and delayed delivery are
child topics, retained logs make replay a read, and topics can enforce a JSON Schema at the
broker. Raft keeps the metadata, Prometheus gets the metrics, and the whole thing runs on a
laptop unchanged from how it runs in Kubernetes.

Current release: **[v2.2.0](https://github.com/DebanganThakuria/narad/releases/tag/v2.2.0)**.

## Documentation

**https://debanganthakuria.github.io/narad/**

| You want | Go to |
|---|---|
| Use it: produce, consume, retries, fan-out, delay, schemas, access control | [Client Guide](https://debanganthakuria.github.io/narad/client/) |
| Run it: Helm chart, configuration, monitoring, scaling, recovery | [Operate](https://debanganthakuria.github.io/narad/operate/) |
| Understand it: every subsystem, with the real function names | [Internals](https://debanganthakuria.github.io/narad/internals/) |
| Decide whether it fits: an honest matrix against Kafka, NATS, RabbitMQ, SQS, Redis, Pulsar | [Compare](https://debanganthakuria.github.io/narad/compare/) |

## Quickstart

```sh
brew install debanganthakuria/narad/narad
# or: go install github.com/debanganthakuria/narad/cmd/narad@latest

narad server start --dev          # local playground on loopback, auth off
```

In another terminal:

```sh
narad topic add demo
narad sub demo --peek                                   # live, read-only tail
narad pub demo '{"hello":"narad"}' --count 100 --rate 20
```

Security is on outside `--dev`: a root `admin` user is seeded at first start (set
`NARAD_ADMIN_PASSWORD` or read the one-time log line) and every call needs HTTP Basic auth.
Terminate TLS at an ingress in front of Narad. Details in
[Getting Started](https://debanganthakuria.github.io/narad/client/).

## Container image

```sh
docker run --rm -p 7942:7942 -p 7943:7943 ghcr.io/debanganthakuria/narad:v2.2.0
```

Port `7942` is the API, `7943` is cluster traffic, `/var/lib/narad` is the data directory.
Images are multi-arch, non-root, and published for every tag and every commit on `master`.
For Kubernetes use the [Helm chart](https://debanganthakuria.github.io/narad/operate/helm-chart/).

## Developing

```sh
make tools-install   # gofumpt + goimports, once
make check           # fmt-check + vet + test
make build           # bin/narad
```

The layout is under `cmd/narad` (CLI and server entry point) and `internal/` (broker, cluster,
persistence, transport). Start with
[Architecture](https://debanganthakuria.github.io/narad/internals/) before reading code.

## License

[Apache 2.0](./LICENSE). Logo and brand assets are under [`assets/`](./assets/).
