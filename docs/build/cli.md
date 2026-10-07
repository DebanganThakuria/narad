---
description: "Use the narad command to run a local broker, watch a topic, send test messages and switch between clusters from a terminal."
---

# Narad CLI

Use the `narad` command to run a local broker, watch a topic, send test messages and switch between clusters from a terminal.

Before you start: Homebrew on macOS or Linux, or Git, Make and Go 1.26 or later to build from source.

Everything the CLI does is also plain HTTP, so you never need it in production code. The same binary also runs the server: `narad serve` is what the container image starts.

## Install the CLI {#install}

=== "Homebrew"

    ```sh
    brew install debanganthakuria/narad/narad
    ```

    The formula builds v3.2.0 from source, which takes a minute or more, and installs shell completions for bash, zsh and fish.

=== "From source"

    ```sh
    git clone --branch v3.2.0 --depth 1 https://github.com/DebanganThakuria/narad
    cd narad
    make build
    ./bin/narad version
    ```

    ```text title="Output of the last command"
    narad dev+9b0fbf1
    ```

    A source build reports the commit it was built from rather than the release number. Copy `bin/narad` onto your `PATH`, and run `narad completion bash`, `zsh` or `fish` to get a completion script.

Do not install with `go install github.com/debanganthakuria/narad/cmd/narad@latest`. Narad's module path has no `/v3` suffix, so Go resolves `@latest` to the old v1.3.3 release.

## Watch messages flow {#watch-messages-flow}

Three terminals show a real broker, a live view of a topic and a producer. In the first, start a local broker:

```sh
narad server start --dev
```

It prints a banner that starts with this line, then logs in plain text:

```text title="Output"
  narad  dev mode: auth OFF, bound to loopback only
```

`--dev` binds `127.0.0.1:7942`, turns authentication off and keeps data in `~/.narad/data`. It is for your own machine only.

In the second terminal, create a topic and watch it without consuming anything:

```sh
narad topic add demo
narad sub demo --peek
```

In the third, send 100 messages at 20 a second:

```sh
narad pub demo '{"hello":"narad"}' --count 100 --rate 20
```

```text title="Output"
accepted 100 messages in 5.008s (20 msg/s)
```

The messages appear in the second terminal as they are committed, one line each: partition, offset, the local time the message was committed (to the second) and the payload. The CLI of v3.0.1 prints a wrong time there, a time of day from January 1970 (fixed in v3.1.0).

```text title="Output in the second terminal, first lines"
[p0 @0] 03:07:33 {"hello":"narad"}
[p0 @1] 03:07:33 {"hello":"narad"}
[p1 @0] 03:07:32 {"hello":"narad"}
[p1 @1] 03:07:33 {"hello":"narad"}
[p1 @2] 03:07:33 {"hello":"narad"}
```

Stop it with Ctrl-C, and it prints how many messages it saw.

## Consume or peek with narad sub {#sub}

`narad sub` has two modes, and the difference matters on a shared topic:

- **`narad sub orders`** is a real consumer. It long-polls, prints each message and acks it, so it competes with your workers for messages, and what it prints is settled. It retries failed acks. Add `--no-ack` to let leases run out instead, so every message it prints is delivered again later.
- **`narad sub orders --peek`** only watches. It tails every partition with [replay reads](replay.md#peek) from the current end of the log, reserves and acks nothing, and your consumers never notice it. `--partition P --from N` starts in history instead.

Both print payloads as they are: JSON verbatim, other text as text, and binary data as a hex dump with its size. `--raw` prints payloads only, for piping into other tools.

**New in v3.1.0.** A key that is not valid UTF-8 prints in hex, marked `(binary)`. While a partition's owner is down, `--peek` refuses to start (unless `--partition` and `--from` give the start), and `narad replay` of that partition fails, both naming the partition and its owner, rather than starting it at offset 0. `narad server report` marks such a topic `[k of n partitions unavailable]` and leaves those partitions out of its totals.

## Switch clusters with contexts {#contexts}

A context stores a server address and credentials under a name, so you stop retyping them:

```sh
narad ctx add local --server http://127.0.0.1:7942
narad ctx add staging --server https://narad.stage.example \
  --user billing-service --password-stdin < ~/.narad-stage-pass
narad ctx select staging
narad ctx ls
```

```text title="Output"
context "local" saved (current: local)
context "staging" saved (current: local)
current context: staging
  local            http://127.0.0.1:7942                    user=-
* staging          https://narad.stage.example              user=billing-service
```

Every command now talks to `staging`, until you select another context or override it. For each setting, the first of these that is set wins:

1. the flags `--server`, `--user` and `--password` (or `--password-stdin`);
2. the environment variables `NARAD_ADDR`, `NARAD_USER` and `NARAD_PASS`;
3. the selected context;
4. `http://127.0.0.1:7942` with no credentials.

**New in v3.2.0:** `--ctx <name>` picks a context for one command, without selecting it: `narad --ctx staging topic ls`. With `--ctx`, the environment variables are not read at all (flags, then that context, then the default), and the CLI refuses `--ctx` while `NARAD_ADDR`, `NARAD_USER` or `NARAD_PASS` is set, so a command meant for one cluster never reaches another or carries its password there. Scripts that drive two clusters, such as the [remote children playbooks](../operate/playbooks/offload.md), use it on every command.

!!! warning "Contexts store passwords in clear text"
    Contexts live in `narad/contexts.json` in your user config directory (`~/.config` on Linux, `~/Library/Application Support` on macOS), or in `$NARAD_CONFIG_DIR` when it is set. The file is readable only by you, but anyone who gets a copy of it, from a backup or a synced dotfiles folder, has your passwords. Treat it like an SSH private key, or leave the password out and set `NARAD_PASS` instead.

- A password given with `--password` ends up in `ps`, your shell history and audit logs. Every password flag has a `-stdin` form that reads the first line of standard input instead: `--password-stdin` for the global and `ctx add` flags, and `--user-password-stdin` for `narad user add`.
- The CLI warns on standard error before it sends credentials over plain `http://` to a host other than your own machine, for example: `warning: sending credentials for "billing-service" over plain http to 10.0.0.5; use an https:// server URL`. Use an `https://` address for any remote cluster.

## Measure throughput with narad bench {#bench}

`narad bench orders` produces 10,000 messages of 256 bytes from this machine with 8 workers and reports the p50, p95 and p99 produce latency. `--consume` also drains them with consume and ack. It writes real messages, so run it against a test topic.

## Next steps

- [CLI command reference](../reference/cli.md): every command and flag.
- [Replay messages from an offset](replay.md): read history with `narad replay`.
- [Deploy on Kubernetes](../operate/deploy-kubernetes.md): run the same binary as a cluster.
