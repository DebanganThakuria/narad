# The CLI

Everything on this site can be done with `curl` — that's the point. But for humans at terminals, `narad` ships a full CLI: start a playground server, stream a topic live, benchmark your cluster, and switch between environments with one word.

## Install

```sh
brew install debanganthakuria/narad/narad     # macOS / Linuxbrew
go install github.com/debanganthakuria/narad/cmd/narad@latest
```

Or grab a binary from the [releases page](https://github.com/DebanganThakuria/narad/releases). Shell completions: `narad completion bash|zsh|fish`.

## The sixty-second demo

Terminal A — a real broker, zero config:

```sh
narad server start --dev
```

Terminal B — watch the topic live:

```sh
narad topic add demo
narad sub demo --peek
```

Terminal C — make messages flow:

```sh
narad pub demo '{"hello":"narad"}' --count 100 --rate 20
```

Messages stream into terminal B as they commit. That's the whole tour.

## Contexts: stop retyping the server

```sh
narad ctx add local    --server http://127.0.0.1:7942
narad ctx add staging  --server https://narad.stage.example --user admin --password ...
narad ctx select staging
narad topic ls                    # now talks to staging
```

Stored at `~/.config/narad/contexts.json` (mode 0600 — it can hold credentials). Precedence per field: `--server/--user/--password` flags → `NARAD_ADDR`/`NARAD_USER`/`NARAD_PASS` → the selected context → localhost.

## The two personalities of `narad sub`

Narad is a queue, so "subscribe" means a choice:

- **`narad sub jobs`** — a *real consumer*: long-polls, prints, acks. Messages it takes are settled; it competes with your production workers. (Ack retries are built in, per [the discipline](handling-retries.md).) `--no-ack` to let leases lapse instead.
- **`narad sub jobs --peek`** — a *bystander*: tails every partition with [replay reads](consuming.md) starting at the current tail. Nothing is reserved, nothing is acked, production consumers never notice. This is the "what is flowing through this topic right now?" debugging tool. `--from N --partition P` to start in history.

Payloads print as themselves — JSON verbatim, text as text, binary hex-dumped with a byte count. `--raw` emits payloads only, for pipes.

## Command reference

```text
narad server start [--dev]         run a broker (--dev: loopback, auth off, ~/.narad/data)
narad server report                every topic: partitions, messages, size, owner spread

narad topic add|ls|info|edit|rm    human units: --retention 12h, --visibility 30s
narad topic add replica --parent orders          # the replication pattern, one flag
narad topic attach|detach|children               # fan-out & delay management

narad pub <topic> [msg]            --key K, --file F, stdin; --count N --rate R
narad sub <topic> [--peek]         stream to your terminal
narad replay <topic> --partition P [--from N --to M]   bounded read-only history

narad bench <topic>                p50/p95/p99 produce latency; --consume drains after
narad user add|grant|ls|rm         grants as action:pattern — produce:orders-*
narad ctx add|select|ls|rm         named server+credential profiles
```

The original `narad serve` and `narad client ...` commands are unchanged for scripts that use them.
