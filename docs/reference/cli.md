---
description: "Look up every narad command, its flags, and what it prints."
---

# CLI command reference

Look up every `narad` command, its flags, and what it prints.

```sh title="Command"
narad topic ls
```

```text title="Output"
NAME                             PARTITIONS    RETENTION  ROLE
orders                                    3      48h0m0s  -
payments                                  3     168h0m0s  -
```

How to install the CLI and use it day to day is in [Narad CLI](../build/cli.md). Every command that talks to a broker goes through the [HTTP API](http-api.md), so the same grants apply ([Access model and grants](access-model.md)).

## Global flags {#global-flags}

Every command that talks to a broker takes these flags.

| Flag | Environment variable | Default |
|---|---|---|
| `-s`, `--server <url>` | `NARAD_ADDR` | the selected context, else `http://127.0.0.1:7942` |
| `-u`, `--user <name>` | `NARAD_USER` | the selected context |
| `-p`, `--password <password>` | `NARAD_PASS` | the selected context |
| `--password-stdin` | none | read the password from the first line of standard input |
| `--ctx <name>` (unreleased) | none | use this [context](#ctx) for this command only |

For each setting, a flag wins over its environment variable, which wins over the selected [context](#ctx). With `--ctx`, the environment variables are not read at all, and the CLI refuses `--ctx` while `NARAD_ADDR`, `NARAD_USER` or `NARAD_PASS` is set, so a command meant for one cluster never reaches another or carries its password there. A password on the command line shows up in `ps` and in shell history; prefer `--password-stdin` or `NARAD_PASS`. The CLI warns on standard error when it is about to send credentials over plain `http://` to another machine: use an `https://` URL for anything remote.

## narad server {#server}

### narad server start {#server-start}

Start a broker node.

| Flag | Default | Meaning |
|---|---|---|
| `--dev` | off | A local playground; see below. |
| `--port <n>` | `7942` | The API port. |
| `--data-dir <path>` | `~/.narad/data` with `--dev` | Where the node stores its data. |

With `--dev`, the node:

- listens on `127.0.0.1:<port>` only, with security off;
- keeps its data in `~/.narad/data` unless `--data-dir` says otherwise;
- runs Raft on `127.0.0.1:<port+1>`, unless `NARAD_CLUSTER_ADDR` is set;
- logs as text and prints a banner that starts `dev mode: auth OFF, bound to loopback only`.

Without `--dev`, it is [`narad serve`](#serve) with the secure defaults, passing on `--port` and `--data-dir`.

### narad server report {#server-report}

One table of every topic: partitions, messages, size on disk, how many nodes own its partitions, and its fan-out role.

```sh title="Command"
narad server report
```

```text title="Output"
TOPIC                         PARTS     MESSAGES       SIZE  OWNERS  ROLE
orders                            3         2004   540.7KiB       1  -
payments                          3            0         0B       1  -
total                                       2004   540.7KiB
```

**New in v3.1.0.** When some of a topic's partitions have an owner that is down, the server answers with the rest ([partial topic details](http-api.md#get-topic)). The report leaves those partitions out of `MESSAGES`, `SIZE` and `OWNERS`, marks the topic `[k of n partitions unavailable]` after its role, and ends with a line counting the partial topics. Against v3.0.1 such a topic shows `(stats unavailable: ...)` instead.

## narad serve {#serve}

The production entry point; the container image runs it. Its flags, and how they combine with the config file and the environment, are in the [Configuration reference](configuration.md#precedence): `--config`, `--addr`, `--port`, `--cluster-port`, `--node-id`, `--data-dir`, `--log-level`, `--log-format` and `--pprof-addr`.

## narad topic {#topic}

Manage topics. `narad topics` works too.

| Command | What it does |
|---|---|
| `narad topic add <name>` | Create a topic, or a fan-out child with `--parent`. |
| `narad topic ls` | List every topic, all pages. `RETENTION` shows `forever` for a topic that keeps messages forever. With `--json`, print the raw list. |
| `narad topic info <name>` | Print the topic and its partition statistics, as [get a topic](http-api.md#get-topic) returns them. |
| `narad topic edit <name>` | Change retention or the partition count, or register a new schema version. |
| `narad topic schema <name>` | Print the schema history (version `0` and an empty list when there is none), or with `--current` only the current schema, which prints `no schema` when there is none. |
| `narad topic rm <name>` | Delete the topic and all its data. It asks `delete topic "<name>" and all its data? [y/N]` unless `-f` (`--force`) is given. `--force` only skips the question: it never abandons a remote child's unshipped records (unreleased). |
| `narad topic attach <parent> <child>` | Attach an existing topic as a child. With `--delay <duration>`, attach it as a delay child. With `--remote <name>` (unreleased), create `<child>` as a [remote child](glossary.md#remote-child) instead; flags below. |
| `narad topic detach <parent> <child>` | Detach a child. The child and its messages remain. A remote child's stub is deleted instead (unreleased), refused while records of the parent are unshipped unless `--force` abandons them. |
| `narad topic children <parent>` | List a parent's children with how far each is behind. With `--partitions` (unreleased), one row per parent partition for each remote child. |
| `narad topic pause <parent> <child>` (unreleased) | Stop a remote child sending; `--reason <text>`, at most 256 bytes, shows in the listing. |
| `narad topic resume <parent> <child>` (unreleased) | Check the target from every node, then let a paused remote child send. `--accept-target` accepts a target topic that was recreated (`target_replaced`). |
| `narad topic skip <parent> <child> --partition <p> --offset <o>` (unreleased) | Let a remote child drop the one record it is stuck on (`rejected_record` or `record_too_large`). |
| `narad topic wait <parent> <child>` (unreleased) | Poll the listing until a remote child has shipped everything (`--lag-zero`) or the parent's consumers passed its start (`--source-drained`); flags below. |

Flags of `narad topic add`:

| Flag | Meaning |
|---|---|
| `--partitions <n>` | Partition count. `0`, the default, uses the server's default. |
| `--retention <duration>` | Retention, such as `12h`. `0` uses the server's default. |
| `--visibility <duration>` | Visibility timeout, such as `30s`. `0` uses the server's default. |
| `--max-in-flight <n>` | Per-partition in-flight cap. |
| `--max-acked-ahead <n>` | Per-partition acked-ahead cap. |
| `--schema <schema>` | A JSON Schema: inline JSON, `@file`, or `-` for standard input. |
| `--parent <topic>` | Create the topic as a fan-out child of this topic. |
| `--delay <duration>` | With `--parent`, make it a delay child. |

Flags of `narad topic edit`:

| Flag | Meaning |
|---|---|
| `--retention <duration>` | New retention. |
| `--partitions <n>` | New partition count, larger than the current one. |
| `--schema <schema>` | A new schema version: inline JSON, `@file`, or `-` for standard input. |
| `--schema-base-version <n>` | Apply `--schema` only if the current version is exactly `n`; the server answers `409` otherwise. |
| `--visibility <duration>` | Refused: the visibility timeout is fixed when a topic is created. |

The per-partition caps have no `edit` flag; change them with [change a topic](http-api.md#alter-topic).

Flags of `narad topic attach` with `--remote` (unreleased):

| Flag | Meaning |
|---|---|
| `--remote <name>` | The remote to send to. |
| `--remote-topic <topic>` | The topic on the remote. Defaults to the parent's name. |
| `--from <mode>` | `attach` (default), `unconsumed` or `earliest`. |
| `--lanes <n>` | Ordered streams per parent partition, 1 to 8 (default 1). |
| `--delay <duration>` | Send each record no earlier than this after the parent committed it. |
| `--dry-run` | Run every check and resolve the start offsets; write nothing. |

`--remote-topic`, `--from`, `--lanes` and `--dry-run` without `--remote` are refused.

Flags of `narad topic wait` (unreleased):

| Flag | Default | Meaning |
|---|---|---|
| `--lag-zero` | | Wait until every record committed to the parent is on the remote. |
| `--source-drained` | | Wait until the parent's consumers have acked past the link's start on every partition. |
| `--stable <duration>` | `0` | The condition must hold this long. |
| `--timeout <duration>` | `30m` | Give up after this long. |
| `--interval <duration>` | `2s` | Poll interval. |

Give exactly one of `--lag-zero` and `--source-drained`. It exits `0` when the condition holds, `1` on `--timeout`, and `2` at once when the link is stalled in a state that needs a fix (every [link state](remote-children.md#link-states) but `unavailable`, `throttled`, `unknown` and `running`; `paused` included), printing the state and the stuck record.

## narad pub {#pub}

`narad pub <topic> [message]` produces a message. The body comes from the argument, from `--file`, or from standard input.

| Flag | Default | Meaning |
|---|---|---|
| `-k`, `--key <key>` | none | The message key. |
| `--partition <n>` | none | Pin the message to this partition. |
| `-f`, `--file <path>` | none | Read the body from a file. |
| `--count <n>` | `1` | Send the message this many times. |
| `--rate <n>` | `0` | Messages per second when `--count` is above 1; `0` sends as fast as possible. |

It prints `accepted (<n> bytes)` on standard error, or for several messages the count, the time taken and the rate.

## narad sub {#sub}

`narad sub <topic>` streams messages to the terminal until you stop it. It has two modes:

- **Queue mode** (the default) is a real consumer: it long-polls, prints each message and acks it, retrying a failed ack. Messages it takes are settled, so it competes with your consumers. With `--no-ack` it leaves the leases to run out, and the messages are delivered again after the topic's visibility timeout.
- **`--peek`** reads with replays from the current end of every partition. It takes no lease and acks nothing, so consumers never notice it.

| Flag | Meaning |
|---|---|
| `--peek` | Watch without consuming. |
| `--partition <n>` | Read one partition only. |
| `--from <offset>` | With `--peek` and `--partition`, start at this offset instead of the end. |
| `--no-ack` | Queue mode without acks. |
| `--raw` | Print payloads only, for pipes. |

**New in v3.1.0.** `--peek` refuses to start while a partition it would read has an owner that is down, naming the partition, its owner and why, instead of starting that partition at offset 0. Peek a live partition with `--partition`; with `--partition` and `--from` the start is given and the peek runs.

Each message prints as `[p<partition> @<offset>] key=<key> <time> <payload>`. JSON prints as it is, text as text, and binary as hex with its byte count; a key that is not valid UTF-8 prints in hex, marked `(binary)`. On exit it prints the number of messages on standard error.

## narad replay {#replay}

`narad replay <topic> --partition <n>` prints a range of a partition's retained messages. It takes no lease and acks nothing.

| Flag | Default | Meaning |
|---|---|---|
| `--partition <n>` | required | The partition to read. |
| `--from <offset>` | the oldest retained offset | First offset to read. |
| `--to <offset>` | the current high watermark | Offset to stop before. |
| `--raw` | off | Print payloads only. |

It ends with a line such as `2 message(s) replayed from p1 [0, 2)` on standard error. Replay over HTTP is in [Replay messages](../build/replay.md).

**New in v3.1.0.** While the partition's owner is down, replay fails with an error naming the partition, its owner and why, instead of printing an empty range.

## narad bench {#bench}

`narad bench <topic>` measures produce throughput from this machine, and with `--consume`, consume and ack throughput after it.

| Flag | Default | Meaning |
|---|---|---|
| `--count <n>` | `10000` | Messages to produce. |
| `--size <bytes>` | `256` | Payload size. |
| `--workers <n>` | `8` | Concurrent workers. |
| `--consume` | off | Consume and ack the messages afterwards. |

It prints the produce count, failures, rate, and p50, p95 and p99 latency, and with `--consume` the consume rate. What the numbers mean for sizing is in [Capacity and disk sizing](capacity.md).

## narad user {#user}

Manage users. Every command needs the `admin` grant ([Manage users and grants](../operate/users.md)).

| Command | What it does |
|---|---|
| `narad user add <username>` | Create a user with the grants given by `--grant <grant>`. The password comes from `--user-password <password>` or, better, the first line of standard input with `--user-password-stdin`; one of them is required. |
| `narad user grant <username>` | Replace all of the user's grants with the ones given by `--grant <grant>`. |
| `narad user ls` | List users as JSON. |
| `narad user rm <username>` | Delete a user. |

`--grant` is repeatable. Each value is `action:pattern`, with several patterns separated by commas, such as `produce:orders-*,invoices.*`. The actions are `produce`, `consume`, `create` and `admin`; `admin` takes no patterns ([Access model and grants](access-model.md#actions)).

## narad remote {#remote}

**Unreleased:** in master, not in v3.1.0.

Manage [remotes](glossary.md#remote), the other clusters this one may send remote children to. Every command needs the `admin` grant and a cluster with security on ([Manage remotes](../operate/remotes.md)).

| Command | What it does |
|---|---|
| `narad remote add <name>` | Register a remote: `--url <https url>`, `--username <name>`, optionally `--ca-file <pem>` and the limit flags below. The password comes from the first line of standard input with `--remote-password-stdin`, the only way to give it. |
| `narad remote set <name>` | Change the fields given. `--url`, `--username`, `--ca-file` and `--no-ca` (back to the system roots) need `--remote-password-stdin` too; the limit flags do not. |
| `narad remote rm <name>` | Delete a remote. Refused while remote children use it, unless `--force`; they then hold in `remote_missing`. |
| `narad remote ls` | List remotes, their remote children and what each node's credential cache holds, as JSON. `--no-nodes` skips asking the nodes. |
| `narad remote test <name> --topic <topic>` | Run the attach checks against a topic on the remote, writing nothing; `--source <parent>` adds the schema and loop checks. With `remotes.allowed_hosts` set, every node runs the checks and the command exits `0` only when every node passes; without it, only the node that took the request runs them, so a `0` says nothing about the other nodes' egress (the attach checks from every node either way). |
| `narad remote reencrypt` | After a cluster secret rotation, re-seal every password under the new key. Exits non-zero when any remote failed to re-seal; keep the previous secret until it exits 0. |

Limit flags of `add` and `set`: `--max-in-flight <n>` (1 to 256, default 16), `--request-timeout <duration>` (5s to 120s, default 30s), `--idle-conn-timeout <duration>` (default 30s), `--conn-max-age <duration>` (default 5m), `--check-interval <duration>` (default 60s) and `--compression none|zstd`. What each does is in [Remotes and remote children](remote-children.md#limits).

The CLI refuses to send a remote password over plain `http://` to a host other than this machine, and refuses `--remote-password-stdin` together with `--password-stdin`, which read the same input.

## narad ctx {#ctx}

A context is a named server URL with optional credentials, so you do not have to repeat them.

| Command | What it does |
|---|---|
| `narad ctx add <name>` | Add or update a context: its URL with `--server <url>`, and optionally `--user <name>` and `--password-stdin`. The first context added becomes the selected one. `--password <password>` also works but shows in `ps`. |
| `narad ctx select <name>` | Make a context the default for every command. |
| `narad ctx ls` | List contexts; the selected one is marked `*`. |
| `narad ctx rm <name>` | Remove a context. |

Contexts are stored in `contexts.json` in `$NARAD_CONFIG_DIR` if that is set, else in a `narad` folder in your user config directory (`~/.config/narad/` on Linux, `~/Library/Application Support/narad/` on macOS). The file has mode 0600 and holds passwords in clear text: treat it like a private key, or leave the password out and use `NARAD_PASS`.

## narad cluster {#cluster}

Inspect partition placement and drain nodes. Every command needs the `admin` grant.

| Command | What it does |
|---|---|
| `narad cluster members` | List members with their status, owned partitions and moves under way, as JSON. From v3.1.0, also each member's voter and leader flags, heartbeat age and, while it drains, why its decommission is blocked. |
| `narad cluster members --detail` (v3.1.0) | Also ask every member for its own status: dispatch backlog, quarantined partition copies and move workers. |
| `narad cluster moves` | List partitions moving between nodes, as JSON. From v3.1.0, also each side's status and why a move is blocked. |
| `narad cluster moves --detail` (v3.1.0) | Also ask each destination for its move worker's report. |
| `narad cluster moves abort <topic> <partition>` (v3.1.0) | Clear a move's target, so the partition stays with its owner. `--target <node-id>` refuses the abort when the move now targets another node. |
| `narad cluster decommission <node-id>` | Mark a node for decommission: its partitions move to the other nodes, then it leaves the Raft voters. From v3.1.0, refused, with every reason, when the node could never be removed safely. |
| `narad cluster decommission <node-id> --dry-run` (v3.1.0) | Report whether the node could be decommissioned, and why not, without changing anything. |
| `narad cluster decommission <node-id> --cancel` | Stop a decommission. The node keeps the partitions it still has and takes new ones again. |
| `narad cluster members forget <node-id>` | **New in v3.1.0.** Remove a Raft voter or non-voter that has no member record, such as a joiner that never registered. Refused for a server with a member record (decommission it instead), one a partition assignment names, or a voter whose removal could leave the cluster without a quorum. Prints `{"id":...,"voter":...}`. See [Troubleshooting](../operate/troubleshooting.md#raft-server-no-member-record). |

```sh title="Command"
narad cluster members
```

```text title="Output"
{
  "members": [
    {
      "id": "narad-1",
      "addr": "127.0.0.1:18181",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0,
      "voter": true,
      "leader": false,
      "heartbeat_age_seconds": 3
    },
    {
      "id": "narad-2",
      "addr": "127.0.0.1:18182",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0,
      "voter": true,
      "leader": false,
      "heartbeat_age_seconds": 4
    },
    {
      "id": "narad-3",
      "addr": "127.0.0.1:18183",
      "status": "alive",
      "draining": false,
      "owned_partitions": 4,
      "outbound_moves": 0,
      "voter": true,
      "leader": true,
      "heartbeat_age_seconds": 4
    }
  ]
}
```

This output comes from a three-node test cluster on one machine, built from master.

```sh title="Command"
narad cluster decommission narad-3 --dry-run
```

```text title="Output"
{
  "member": "narad-3",
  "would_decommission": false,
  "reasons": [
    {
      "code": "below_min_voters",
      "message": "removing it would leave 2 voters, fewer than the 3 a cluster keeps; add a node first"
    }
  ],
  "voter": true,
  "owned_partitions": 4,
  "inbound_moves": 0
}
```

When to run these, and in what order, is in [Scale out and in](../operate/scaling.md).

## Other commands {#other}

| Command | What it does |
|---|---|
| `narad version` | Print the build's version. A binary built with plain `go build` prints `narad dev (<commit>)`. |
| `narad completion <shell>` | Print a shell completion script for `bash`, `zsh`, `fish` or `powershell`. |
| `narad help` | Print help. `narad help <command>` and `narad <command> --help` print the help of one command. |
