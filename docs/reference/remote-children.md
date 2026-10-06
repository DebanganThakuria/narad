---
description: "Look up the fields, limits, link states and check classes of remotes and remote children."
---

# Remotes and remote children

Look up the fields, limits, link states and check classes of remotes and remote children.

**Unreleased:** in master, not in v3.1.0.

```sh title="Request"
curl -sS -u "$AUTH" "$NARAD/v1/topics/orders/children" \
  | jq -c '.children[0] | {state, lag_messages, blocked_at, target_verified_at}'
```

```json title="Output"
{"state":"auth_failed","lag_messages":2,"blocked_at":null,"target_verified_at":null}
```

- `$NARAD` is the base URL of any node; `$AUTH` is `username:password` of a user with any grant on the parent.
- This output is from a link attached moments before its replicator user was deleted on the remote, so no target check had succeeded yet.

How to attach and run a remote child is in [Replicate a topic to another cluster](../build/remote-children.md), and how to manage remotes in [Manage remotes](../operate/remotes.md). Every request and answer is in the [HTTP API reference](http-api.md#fan-out-children).

## A remote {#remote}

| Field | Rule |
|---|---|
| `name` | A lowercase letter, then up to 62 lowercase letters, digits or `-`. |
| `url` | `https://host[:port][/path]`, no user, query or fragment, at most 2,048 bytes. The host converts to IDNA ASCII or is an IP address without a zone. The port must be in `remotes.allowed_ports`, the host in `remotes.allowed_hosts` when that is set, and the address it resolves to must pass the address guard. Stored lowercased, without a trailing dot or the default port. |
| `username` | 1 to 64 characters from `A-Z a-z 0-9 . _ -`, the target's user rule. |
| `password` | 24 to 72 bytes. Write-only: no answer, log line or metric holds it. |
| `ca_pem` | 1 to 16 PEM certificates and nothing else, at most 64 KiB. Absent: the system roots verify the remote. |
| `limits` | Below. |

A new `url`, `username` or `ca_pem` needs the password again: the password is sealed to the remote's name, ID, URL, username and trust anchor, and opens for nothing else. A cluster holds at most 64 remotes.

## Limits {#limits}

Per remote, applied on each node, changed live with a `PATCH` ([Change a remote](../operate/remotes.md#change)).

| Limit | Default | Range | Meaning |
|---|---|---|---|
| `max_in_flight` | 16 | 1 to 256 | Requests in flight at once to the remote from one node, shared by every lane of every cursor that sends to it. |
| `request_timeout_ms` | 30000 | 5000 to 120000 | Timeout of one request. A timeout halves the lane's chunk size. |
| `idle_conn_timeout_ms` | 30000 | 1000 to 300000 | How long an idle connection is kept. |
| `conn_max_age_ms` | 300000 | 10000 to 3600000 | How often the connections are replaced, busy ones included (each closes once its request ends), so a DNS change or a load balancer scale-out is picked up. |
| `check_interval_ms` | 60000 | 10000 to 3600000 | How often each node checks the target of each link, with records to send or not, with 20% jitter. |
| `compression` | `none` | `none`, `zstd` | `zstd` compresses a request when that saves at least 10% and the target decodes zstd (it probes). A target that cannot decode one gets it again uncompressed at once. |

## A remote child {#remote-child}

Attach fields, with `remote` set on `POST /v1/topics/{parent}/children`:

| Field | Default | Rule |
|---|---|---|
| `child` | required | A topic name that does not exist yet: the attach creates the stub. |
| `remote` | required | A remote's name. |
| `remote_topic` | the parent's name | A topic name. |
| `from` | `attach` | `attach` (the parent's committed high watermark), `unconsumed` (the parent's consumer frontier) or `earliest` (the oldest retained offset), per parent partition. |
| `lanes` | 1 | 1 to 8 ordered streams per parent partition; a key always uses one. |
| `delay_ms` | 0 | 0 to one year, as for a [delay child](../build/fanout-and-delay.md#delay-children). |
| `dry_run` | `false` | Run the checks and resolve the start offsets; write nothing. |

The stub's `remote` object holds `name`, `topic`, `target_id` (the target topic's ID at the attach, or at the last resume with `accept_target`; absent when the target serves no IDs), `from`, `lanes`, `paused` with `pause_reason` (at most 256 bytes of printable text), `paused_by` and `paused_at_ms`, `skip` (per parent partition, the list of offsets an admin accepted to lose, ascending, at most 4000, always including the newest; for example `{"0":[2,7]}`) and `created_by`. `paused_by` and `created_by` are shown to admins only.

| Bound | Value |
|---|---|
| Remote children per parent | 16 |
| Parent retention while it has a remote child | at least 24 hours, or `0` (forever); the attach warns below 72 hours |
| Links from this cluster to one remote topic (same host and port) | 1 |
| Records per request | 1,000 to a target on this release, 100 to an older one |
| Request body | 960 KiB, halved after a timeout down to 64 KiB, doubled back after 20 accepted requests; a single larger record goes alone |
| Records held in memory across a failure, per node | `remotes.max_held_bytes`, 256 MiB by default; past it a cursor reads its records again later |
| A link with a node that has had no successful target check for | 10 minutes is flagged `unverified` |
| A stalled cursor retries | every 30 seconds, and at once when the remote changes |
| Unshipped checks per parent | 1 every 10 seconds |
| Remote child writes (attach, pause, resume, skip) per node | 60 a minute |
| Remote writes (create, change, delete, re-encrypt) per node | 10 a minute |
| Checks of one remote per node | 1 every 5 seconds |

## Link states {#link-states}

A remote child's `state` in the [children listing](http-api.md#list-children) is the worst of its partitions', in this order, worst first. `narad_fanout_remote_state` exports each partition's on the node that runs its cursor. None of them moves a cursor: each holds the link where it is, and the parent's retention keeps running.

| State | Meaning | What clears it |
|---|---|---|
| `remote_missing` | The remote the link names does not exist: it was deleted with `force`. | A remote of that name, created again. |
| `credential_unreadable` | This node cannot open the stored password: it lacks the cluster secret it was sealed under (a rotation without `NARAD_CLUSTER_SECRET_PREVIOUS`), or the record no longer matches what it was sealed for. | Set the previous secret back, or enter the password again ([Rotate the cluster secret](../operate/remotes.md#rotate-cluster-secret)). |
| `node_insecure` | This node's settings forbid remotes: security off, or legacy cluster authentication on. | Fix the node's settings and restart it. |
| `destination_refused` | The address guard, the port list or the host allowlist refused the dial, for example because the host now resolves to a loopback or metadata address. | Fix DNS, or the node's `remotes.*` settings. |
| `target_has_remote_children` | The target topic has a remote child of its own. Sending would make a chain or a loop. | Detach that remote child on the target. |
| `target_replaced` | The target topic's ID changed: it was deleted and created again, or the URL now reaches another cluster. Nothing is sent to it. | Resume with `accept_target` once you have checked it is the right topic. |
| `auth_failed` | The target answered `401`: the user or password is wrong, or the user was deleted. | Fix the user on the target, or the remote's password. |
| `forbidden` | The target answered `403`: the user lacks `produce` on the topic. | Grant it on the target. |
| `target_missing` | The target answered `404`: the topic does not exist there. | Create it on the target. |
| `no_batch_produce` | The target has no batch produce (a release before v3.1.0). | Upgrade the target. |
| `redirect_refused` | The target answered with a redirect, which is never followed. | Point the remote's URL at the address that answers. |
| `tls_failed` | The TLS handshake failed, or the certificate did not verify against the remote's CA or the system roots. | Fix the certificate, or the remote's `ca_pem`. |
| `rejected_record` | The target refused one record for good (its schema, for example); `blocked_at` names it. A target topic that became a delay child or a stub also stops the link here. | Fix the target, or [skip](../build/remote-children.md#skip) the record. |
| `record_too_large` | One record is too large for the target's batch produce; `blocked_at` names it. A target on v3.1.0 takes 1 MiB of body, so a payload near 1 MiB that travels as base64 does not fit. | Upgrade the target to this release, whose batch takes any record a single produce takes, or skip the record. |
| `unavailable` | The target did not answer, timed out, answered `5xx`, or something in front of it (a load balancer, a proxy) answered instead. | Retried on its own with backoff. |
| `throttled` | The target answered `429`. Its `Retry-After` is honored, up to 60 seconds. | Retried on its own. |
| `unknown` | A partition's owner did not report its cursor (`lag_complete` is `false`), or an answer that fits no other class. | Usually clears on the next listing. |
| `paused` | An admin paused the link. | Resume. |
| `running` | Sending, or nothing to send. | |

Retries pace per remote on each node. A remote-wide failure (`auth_failed`, `throttled`, `tls_failed`), or three `unavailable` answers in a row across the remote's lanes, closes the remote's gate on that node: it then lets one request through per backoff (250 ms, doubling with jitter to 30 seconds; 30 seconds at once for `auth_failed`, so a wrong password costs the target one failed login per node per 30 seconds) as the probe for every cursor, and opens at the first accepted request or a new password. A single `unavailable` answer backs off only its lane, by 250 ms to 2 seconds.

## Check classes {#check-classes}

The attach, resume and test checks run on every node and fail with one `class`, the most important among the nodes. Besides the link states above:

| Class | Status on attach or resume | Meaning |
|---|---|---|
| `old_release` | `412` | A node runs an older release, or has no remote plane. |
| `unreachable` | `412` | A node did not answer in time (each node's checks have 15 seconds). |
| `stale` | `412` | A node checked with an older credential version than the remote's: it has not applied the latest change yet. Retry. |
| `target_disagreement` | `412` | The nodes passed but saw different target topic IDs, as when one name resolves to two clusters. |
| `target_security_off` | `409` | The target answered without asking for credentials. |
| `admin_credential` | `409` | The credential can list the target's users: it is an admin there. Use a produce-only user. |
| `target_is_source` | `409` | The target topic is the parent itself: the same topic ID, or, for a parent created before topic IDs existed (v2.1 and earlier), the same name and creation time. |
| `target_is_delay_child` | `409` | The target topic is a delay child, which takes no produces. |
| `target_is_stub` | `409` | The target topic is another cluster's remote child stub. |
| `schema_mismatch` | `409` | Both topics have a schema and they differ. |
| `edge` | `502` | Something in front of the target answered instead of it. |

`remote_missing`, `credential_unreadable` and `node_insecure` answer `412`, `throttled` `429`, `redirect_refused` `502`, `unavailable` `503`, and every other state `409`. A test answers `200` with `result: fail` and the class instead, or `429` while a check of the remote is too recent.

## Credential cache states {#cache-states}

What each node holds for a remote, in [`narad remote ls`](../operate/remotes.md#list) and `narad_remote_credential_state`:

| State | Meaning |
|---|---|
| `ready` | The node holds the remote's current credential version. |
| `stale` | The node holds an older credential version than the remote: it has not applied the change yet. |
| `credential_unreadable` | The node cannot open the password. |
| `node_insecure` | The node's settings forbid remotes. |
| `missing` | The node has no entry for the remote yet. |
| `unknown` | The node did not answer (`last_error` `unreachable` or `old_release`). |

Each node decrypts a password once per credential version and keeps the ready `Authorization` header and HTTP client in memory; `narad_remote_credential_decrypts_total` moves only when a remote changes.
