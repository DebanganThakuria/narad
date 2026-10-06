---
description: "Register the other Narad clusters this one may copy topics to, rotate their passwords and the key that protects them, and know what that storage risks."
---

# Manage remotes

Register the other Narad clusters this one may copy topics to, rotate their passwords and the key that protects them, and know what that storage risks.

**Unreleased:** in master, not in v3.1.0.

Before you start: the `admin` grant on both clusters, the [CLI](../build/cli.md) with a [context](../build/cli.md#contexts) for each (`a` for this cluster, `b` for the other one here), and the checklist below.

A [remote](../reference/glossary.md#remote) is another Narad cluster this cluster may send to: a name, an `https` URL, a username, a password, an optional CA bundle and limits. Admins manage remotes through the API, with no redeploy and no restart. A [remote child](../reference/glossary.md#remote-child) then copies a topic to a topic on that remote ([Replicate a topic to another cluster](../build/remote-children.md)).

## Before you start {#before-you-start}

Check each of these first. Every one is enforced, and each refusal names what is missing.

- **Security on, on both clusters.** Every remotes route answers `403` (`remotes require security`) on a node with security off, and the attach checks refuse a target with security off. A node whose metadata holds a remote refuses to start with security off or without a cluster secret.
- **A strong cluster secret here.** Remote passwords are sealed under a key derived from `NARAD_CLUSTER_SECRET`, so a create, a password change and a re-encrypt answer [`412`](../reference/status-codes.md#status-412) unless the secret is at least 32 random bytes as `openssl rand -base64 32` (padded standard base64) or `openssl rand -hex 32` prints them. A passphrase is refused even when it happens to decode, because the key is only as strong as the secret and a snapshot holder can test guesses offline. A secret made the documented way passes as it is, except about once in ten thousand, when it looks like a passphrase (no digit, `+` or `/`); generate another. A secured single node started without `NARAD_CLUSTER_SECRET` generates a secret for node RPC that lasts only until it restarts; that secret never seals a remote password, so set `NARAD_CLUSTER_SECRET` before you create a remote on such a node (unreleased). Two clusters never share a secret ([operating condition 5](#operating-conditions)).
- **An encrypted API hop, on both clusters.** TLS from a client usually ends at an ingress or a load balancer, and the hop from there to the Narad pod is plain HTTP unless something encrypts it. Narad cannot see that hop, so you attest it:
    - *Here*, a create or a password change carries the password in its body. Each node answers those `412` until `remotes.api_hop_encrypted` is `true` on it (`remotes.apiHopEncrypted` in the chart). Set it only once the ingress-to-pod hop is encrypted: a service mesh with mutual TLS, or an ingress that re-encrypts to the pod.
    - *On the target*, every request from this cluster carries the replicator's `Authorization` header across the same kind of hop. Encrypt that hop on the target too. This cluster cannot check it for you; it is a prerequisite for any cluster you register as a remote.
- **Every member on this release, without legacy cluster authentication.** Remote writes, attaches and resumes answer `412`, naming the member, until every member of this cluster, dead ones and Raft servers without a member record included, runs a release that applies the remote [Raft entry types](../understand/metastore-and-raft.md#remote-entry-types), with security on and `security.allow_legacy_cluster_auth` off. Decommission or [forget](../reference/cli.md#cluster) a member that will not come back.
- **Egress from every pod to the remote.** Cursors run on whichever node owns a parent partition, partitions move, and the attach, resume and test checks run on every node, so every pod needs to reach the remote's ingress. The address must also pass this node's bounds, which are node config, not API fields, so an admin cannot widen them: the port must be in `remotes.allowed_ports` (443 by default), the host in `remotes.allowed_hosts` when that is set, and the resolved address must not be loopback, link-local, multicast or a cloud metadata address unless `remotes.allow_addresses` lists it ([Configuration reference](../reference/configuration.md#remotes)). Set `remotes.allowed_hosts` in production ([operating condition 4](#operating-conditions)).
- **The target** needs batch produce (v3.1.0 or later). A target on this release also takes batches of 1,000 and zstd bodies, and reports the topic IDs that let a remote child notice a recreated or looping target; an older target works with those checks off, and the attach warns.

## Prepare the target {#prepare-target}

On the target, create the topic, with the parent's schema if the parent has one, and a user for this cluster that may only produce to the replicated topics. Give each source cluster its own user, with a name nobody guesses:

```bash
openssl rand -base64 24 > repl-password
narad --ctx b topic add orders --partitions 3 --retention 72h
narad --ctx b user add repl-from-a-7f3k9q --grant produce:orders \
  --user-password-stdin < repl-password
```

The password must be 24 to 72 bytes. The attach checks refuse a credential that can list the target's users, which is an admin's.

## Register a remote {#register}

=== "CLI"

    ```bash
    narad --ctx a remote add b --url https://narad-b.example.com \
      --username repl-from-a-7f3k9q --ca-file narad-b-ca.pem \
      --remote-password-stdin < repl-password
    ```

    The password comes from the first line of standard input, never from the command line. The CLI refuses to send it over plain `http://` to a host other than this machine, and refuses `--remote-password-stdin` together with `--password-stdin`, which reads the same input.

=== "curl"

    ```bash
    jq -n --rawfile pw repl-password --rawfile ca narad-b-ca.pem \
      '{name: "b", url: "https://localhost:8443",
        username: "repl-from-a-7f3k9q",
        password: ($pw | rtrimstr("\n")), ca_pem: $ca}' |
    curl -i -u "$AUTH" -X POST "$NARAD/v1/remotes" \
      -H "Content-Type: application/json" -d @-
    ```

    ```http title="Response"
    HTTP/1.1 201 Created
    Cache-Control: no-store
    Content-Type: application/json
    X-Content-Type-Options: nosniff
    Date: Tue, 06 Oct 2026 13:06:22 GMT
    Content-Length: 622

    {
      "name": "b",
      "id": "8ccffc20e36f644c",
      "url": "https://localhost:8443",
      "username": "repl-from-a-7f3k9q",
      "password": {
        "fingerprint": "ced1ea5d18d1",
        "set_at": "2026-10-06T13:06:22Z",
        "set_by": "admin",
        "key_version": "b51c9412df29325d"
      },
      "credential_version": 1,
      "ca_pem_sha512": "a85bc5f06c550eb18ff2a2a29187fe3a290b1fc74c76d0ef2781531bd4df4f84187980295b5484a6e363a2ff24c52fd9cec309bd74405beb5c815fb48b1e80b0",
      "limits": {
        "max_in_flight": 16,
        "request_timeout_ms": 30000,
        "idle_conn_timeout_ms": 30000,
        "conn_max_age_ms": 300000,
        "check_interval_ms": 60000,
        "compression": "none"
      },
      "revision": 1,
      "created_at": "2026-10-06T13:06:22Z",
      "created_by": "admin"
    }
    ```

    This ran against a local pair of single-node clusters, the target behind a TLS proxy on `localhost:8443`.

- **The password is write-only.** The node that takes the request seals it and drops it; no answer, log line, metric or audit line ever holds it. The `fingerprint` is keyed with the cluster secret, so it tells two passwords apart without revealing either.
- **The URL** is `https://host[:port][/path]`, with no user, query or fragment, and is stored in a canonical form (lowercased, the default port dropped). Its host must resolve, and the address must pass the guard, when you register it. Redirects are never followed.
- **The CA bundle** (1 to 16 PEM certificates, at most 64 KiB) alone verifies the remote when you give one; otherwise the system roots do. Verification is always on.
- A cluster holds up to 64 remotes, and a node takes 10 remote writes a minute. Every field and answer is in the [HTTP API reference](../reference/http-api.md#create-remote).

## Test a remote {#test}

Run the attach checks against a topic on the remote, writing nothing on either cluster:

```bash
narad --ctx a remote test b --topic orders --source orders
```

```json title="Output"
{
  "remote": "b",
  "result": "pass",
  "checks": [
    {
      "node": "narad-0",
      "result": "pass",
      "credential_version": 1,
      "target_id": "128e63dd156ff568",
      "target_serves_ids": true,
      "rtt_ms": 0,
      "lane_capacity_per_s": 20000,
      "server_cert_not_after": "2026-11-05T13:04:25Z",
      "warnings": [],
      "posture": {
        "security_enabled": true,
        "legacy_cluster_auth": false,
        "raft_tls": true,
        "api_hop_encrypted": true
      }
    }
  ]
}
```

The command exits `0` only when `result` is `pass`; otherwise it ends with `narad: remote test failed: <class>`, such as `target_missing` for a topic the remote does not have. With `remotes.allowed_hosts` set, every node runs the checks and reports its connect time (`rtt_ms`) and an estimate of one [lane](../build/remote-children.md#throughput)'s records per second at that round trip; without it, only the node you asked runs them and reports no time. A check of one remote runs at most once every 5 seconds per node (`429` in between). What each failing `class` means is in [check classes](../reference/remote-children.md#check-classes).

## List remotes {#list}

`narad --ctx a remote ls` (`GET /v1/remotes`) shows every remote, the remote children that use it, and what each node's credential cache holds:

```json title="Output, the remote b, trimmed to its nodes"
{
  "allowlist": "set",
  "key": {
    "key_version": "b51c9412df29325d",
    "first_sealed_at": "2026-10-06T13:06:22Z",
    "age_seconds": 10
  },
  "remotes": [
    {
      "name": "b",
      "credential_version": 1,
      "nodes": [
        {
          "node": "narad-0",
          "state": "ready",
          "credential_version": 1,
          "key_version": "b51c9412df29325d",
          "fingerprint": "ced1ea5d18d1",
          "last_ok_at": "2026-10-06T13:06:32Z",
          "last_error": "none",
          "server_cert_not_after": "2026-11-05T13:04:25Z",
          "rtt_ms": 0
        }
      ]
    }
  ],
  "lingering": [],
  "not_answering": []
}
```

- A node's `state` is `ready`, `stale` while it still holds an older `credential_version` than the remote, `credential_unreadable` when it cannot open the password (it lacks the secret it was sealed under), `node_insecure` when its own settings forbid remotes, `missing` before it has built an entry, or `unknown` when it did not answer.
- `last_error` is a class, never text from the remote.
- `lingering` lists deleted remotes some node still holds, and the nodes that did not answer.
- `not_answering` lists every node that did not answer, even when no answering node holds a deleted remote, and `narad remote ls` prints a warning naming them (unreleased).
- `--no-nodes` (`?nodes=false`) skips asking the nodes.

## Change a remote {#change}

`narad remote set` (`PATCH /v1/remotes/{name}`) changes the fields you name. Limits change live:

```bash
narad --ctx a remote set b --max-in-flight 32 --compression zstd
```

| Limit | Default | Range | Meaning |
|---|---|---|---|
| `max_in_flight` (`--max-in-flight`) | 16 | 1 to 256 | Record requests to the remote in flight at once on each node, shared by every cursor that sends to it, each on its own connection. Target checks, listings and capability probes use 4 more connections of their own (unreleased), so they never wait behind record requests, nor these behind them. |
| `request_timeout_ms` (`--request-timeout`) | 30 s | 5 s to 120 s | Timeout of one request. |
| `idle_conn_timeout_ms` (`--idle-conn-timeout`) | 30 s | 1 s to 5 min | How long an idle connection is kept. |
| `conn_max_age_ms` (`--conn-max-age`) | 5 min | 10 s to 1 h | How often idle connections are recycled, so a DNS change is picked up. |
| `check_interval_ms` (`--check-interval`) | 60 s | 10 s to 1 h | How often each cursor checks its target, with 20% jitter. |
| `compression` (`--compression`) | `none` | `none`, `zstd` | `zstd` compresses a request when that saves at least 10% and the target decodes zstd. |

A new `--url`, `--username`, `--ca-file` or `--no-ca` needs the password again in the same request (`--remote-password-stdin`): a stored password is only ever sent to the URL and user it was entered with, verified against the CA it was entered with.

## Rotate a remote's password {#rotate-password}

Every 90 days, or on your organization's schedule; `narad_remote_credential_age_seconds{remote}` says how old each password is. With no stall:

1. On the target, create a second user, `repl-from-a-2`, with the same `produce` grant.
2. Point the remote at it: `narad --ctx a remote set b --username repl-from-a-2 --remote-password-stdin < repl-password-2`.
3. Run `narad --ctx a remote ls` until every node shows the new `fingerprint` and `credential_version` with state `ready`, and check that `narad_remote_errors_total{class="auth_failed"}` stays flat.
4. Delete `repl-from-a-1` on the target.

Changing the password in place on the target, then `narad remote set b --remote-password-stdin` here, also works: the links hold in `auth_failed` in between and lose nothing, because a cursor never advances on a failure. Each node resumes its links as soon as it has the new password.

## Revoke a remote in an emergency {#emergency-revocation}

Revoking on the target is the fence, so it comes first.

1. **On the target, delete the user** (`narad --ctx b user rm repl-from-a-7f3k9q`). Every node of this cluster that still sends gets `401`, and its links hold in `auth_failed`.
2. **Here, delete the remote** even though links use it: `narad --ctx a remote rm b --force`. Each node drops its cached credential and closes its connections when it applies the delete, and the links hold in `remote_missing`. The delete checks only the cluster metadata, so it works while a node is down.
3. **Check every node let go:** `narad --ctx a remote ls` lists under `lingering` any node that still holds the deleted remote, and under `not_answering` (with a warning) any node that did not answer. Step 3 passes only when both are empty. A node cut off from the Raft leader but still able to reach the target keeps its cached credential until it catches up, which is why step 1 comes first.

Nothing is lost while the parent's retention lasts. Create a new user on the target and a new remote of the same name here, and the links resume after their target check.

## Delete a remote {#delete}

`narad remote rm b` is refused while remote children use the remote:

```http title="Response to DELETE /v1/remotes/b"
HTTP/1.1 409 Conflict
Cache-Control: no-store
Content-Type: application/json
X-Content-Type-Options: nosniff
Date: Tue, 06 Oct 2026 13:07:49 GMT
Content-Length: 79

{"error":"remote is used by 1 remote children","links":["orders/orders-to-b"]}
```

Detach those children first ([Detach a remote child](../build/remote-children.md#detach)), or delete with `--force` (`?force=true`): the links then hold, without loss while the parent's retention lasts, in `remote_missing` until a remote of that name exists again.

## Rotate the cluster secret {#rotate-cluster-secret}

Cluster RPC accepts exactly one secret, so this is a planned stop and start of the whole cluster, not a rolling restart.

1. Generate the new secret: `openssl rand -base64 32`.
2. Stop every node. Start them all with `NARAD_CLUSTER_SECRET` set to the new secret and `NARAD_CLUSTER_SECRET_PREVIOUS` set to the old one. In the chart, put the new secret under the security Secret's `cluster-secret` key and the old one under `cluster-secret-previous` ([Helm values reference](../reference/helm-values.md#secrets)). The previous secret only opens remote passwords sealed under it; cluster RPC never accepts it. Links keep running.
3. Re-seal every password under the new key:

    ```bash
    narad --ctx a remote reencrypt
    ```

    ```json title="Output"
    {
      "key_version": "b51c9412df29325d",
      "reencrypted": [],
      "already_current": ["b"],
      "failed": []
    }
    ```

    This output is from a cluster whose secret had not changed, so `b` was already current; after a rotation it is listed under `reencrypted`. A remote changed in between keeps its newer ciphertext, which is already under the new key. `failed` names any remote the leader could not open, with a reason (`key_unknown` when the leader lacks the previous secret). The command is safe to repeat.
4. Run `narad --ctx a remote ls` until every remote shows the new `key_version` on every node, and `narad_remote_credential_key_current` is 1 everywhere.
5. Remove `NARAD_CLUSTER_SECRET_PREVIOUS` (delete the key from the Secret) and restart; a rolling restart is fine now.

Without the previous secret, every node shows the remotes as `credential_unreadable` and every link stalls without loss: set it back, or enter each password again with `narad remote set`. Old ciphertexts stay in older Raft entries and snapshots until compaction replaces them, and nobody can open them once the old secret is destroyed.

## Audit lines {#audit}

Every remotes request writes one `component=audit` line on the node that took it, refusals included, with `event` (`remote.create`, `remote.update`, `remote.delete`, `remote.test`, `remote.reencrypt`, `remote.list`, `remote.get`), `actor`, `target`, `outcome`, `status` and a `request_id`. The leader writes the authoritative line for each write it proposes, with the same `request_id` and `outcome` `committed` or `refused`. A remote child's attach, pause, resume, skip and delete are audited the same way (`remote_child.create`, `.pause`, `.resume`, `.accept_target`, `.skip`, `.delete`). For example, the two lines of one create, with `time`, `level` and `source` left out:

```json
{"msg":"audit","component":"audit","event":"remote.create","actor":"admin","request_id":"9228e702f02e464c","target":"b","outcome":"committed","host":"localhost","fingerprint":"ced1ea5d18d1","credential_version":1}
{"msg":"audit","component":"audit","event":"remote.create","actor":"admin","target":"b","outcome":"ok","status":201,"request_id":"9228e702f02e464c","host":"localhost","fingerprint":"ced1ea5d18d1"}
```

A line carries the URL's host, never the URL, the username or the password. A forced delete of a remote child records what it abandoned (`abandoned_lag_messages`, `abandoned_dispatch_backlog`). A detach of a remote child is logged as `remote_child.delete` (target `<parent>/<child>`, with `force`) on the node the client called, and a delete of a stub or of a parent with remote children as `topic.delete`; both carry the `request_id` of the leader's line. How to read `outcome` is in [Read the audit log](monitoring.md#audit-log).

## Accepted risk: remote passwords in the metastore {#accepted-risk}

**Decided by the maintainer on 2026-09-29. Owner: the maintainer. Review by 2027-09-29, or sooner if Narad gains a KMS or secrets-manager integration.**

Remote passwords are stored in the Raft metastore, encrypted by Narad. This departs from three rules of the organization's security standard:

- secrets must be stored in a secrets manager, KMS or HSM-backed store with access controls;
- secrets must be read through the organization's secrets client, and a custom envelope scheme must not substitute for it;
- encryption keys must be stored in a dedicated KMS, HSM or vault, separate from the data they protect.

Why it was accepted: remotes change through the API with no redeploy, and an open-source broker should not embed one organization's secrets-manager client.

**What Narad does instead.** The node that receives a password encrypts it with AES-256-GCM (a random nonce for every seal) and drops the plaintext. The key is derived with HKDF from the cluster secret, a random per-cluster salt and a fixed purpose label, and is never stored. Each ciphertext records the key version it was sealed under, and is bound to the remote's name, ID, canonical URL, username and trust anchor (its CA bundle, or the system roots), so it opens only for the record it was sealed for. Raft holds only the ciphertext, the salt, the key version and a keyed fingerprint. Each node decrypts each password once per credential version into memory (a ready `Authorization` value and a prebuilt HTTP client), never on the send path. The cluster secret stays where it always was: `NARAD_CLUSTER_SECRET`, from a Kubernetes Secret, never in the data directory or the config file.

| Who or what | Protected? |
|---|---|
| A Raft snapshot copied off a node, `raft.db`, `fsm.db` | Yes: ciphertext only |
| Volume backups and snapshots | Yes, while the cluster secret is not in the same backup (condition 1) |
| A namespace backup that includes Secrets (Velero includes them unless told otherwise) | **No**: it holds the ciphertext and the cluster secret together, which amounts to an unencrypted backup of every remote password. Condition 1 |
| `pods/exec` or `pods/attach` on the namespace | **No**: a shell reads the secret from the environment and the ciphertext from the volume. Condition 2 |
| The Raft wire in plaintext | The password, yes: ciphertext only, and it cannot be redirected to another URL, user or CA. An attacker on the Raft port can still delete remotes and stall links, so run [Raft TLS](raft-tls.md) |
| The cluster RPC plane | Yes: ciphertext only, inside QUIC TLS |
| DNS or the network path to the target | Yes: TLS is verified against the trust anchor sealed with the password |
| Readers of metrics, logs and audit lines | Yes: no key version, ciphertext or username; the fingerprint is keyed |
| A Narad admin | Cannot read a stored password, or send it to another host, user or CA without typing it again |
| Someone with a node's disk **and** its cluster secret | **No**: they derive the key and decrypt |
| Process memory (a debugger, a core dump) | **No**: the cache holds the ready header for the process's lifetime |
| The ingress-to-pod hop, on a create or a password change | Only as far as the hop is encrypted: those requests answer `412` without `remotes.api_hop_encrypted` |

Controls the code enforces:

- A node whose metadata holds a remote refuses to start with security off or without a cluster secret.
- Every seal needs a cluster secret that decodes to at least 32 bytes.
- No remote create, change, re-encrypt, attach or resume while any member allows legacy cluster authentication or runs with security off.
- A create or a password change needs an attested encrypted API hop on the node that takes it.
- A random salt per cluster, a key version on every ciphertext, and a seal counter per key: a key seals at most 2^30 passwords, then a rotation is due (`narad_remote_key_seals`).
- The key's age and each password's age are exported as metrics.
- Raft TLS is not required. A node that holds remotes over plaintext Raft logs a warning at startup and exports `narad_remotes_plaintext_raft` 1; alert on it in production.

### Operating conditions {#operating-conditions}

These five conditions are part of the acceptance. A cluster that holds remotes must keep all five:

1. **The cluster secret is never in the same backup as a node's data directory.** Exclude the security Secret from namespace backups (for Velero, the label `velero.io/exclude-from-backup=true` on it).
2. **`pods/exec` and `pods/attach` on the namespace are granted only to the operators who may hold both halves**: the secret and the data.
3. **The cluster secret is rotated on the organization's key-rotation schedule**, followed by `narad remote reencrypt` ([above](#rotate-cluster-secret)). Alert on `narad_remote_key_age_seconds{key="current"}` 10 days before it is due.
4. **Production clusters set `remotes.allowed_hosts`.** Without it the node logs a warning and exports `narad_remotes_allowlist_configured` 0.
5. **Two clusters never share a cluster secret**, a disaster-recovery pair included.

## Upgrades and rollback {#upgrade}

- **Upgrade the whole cluster first.** Remote writes and attaches answer `412`, naming the member that holds them back, until every member runs this release; a member stopped for good must be decommissioned or forgotten. Nothing else needs a step.
- **The rollback boundary.** This release adds five Raft entry types, 33 to 37, all for remotes and remote children, and the leader writes them only for a remote write. Once any of them has committed (a remote created, a remote child attached), a rollback to v3.1.0 or earlier is unsupported: v3.1.0 stops applying at the first such entry, and refuses at startup a database that applied one ([`written by a newer Narad release`](troubleshooting.md#log-metastore-newer-database)). Deleting every remote and remote child does not undo it. As for any release that adds entry types, a rollback is unsupported once every member has reported this release ([Upgrade Narad](upgrade.md#roll-back-newer)); create no remote until you no longer need the way back.
- **Config keys.** v3.1.0 refuses to start with the file keys `http.max_batch_body_bytes_in_flight` or any `remotes.*` key. The chart passes the `remotes` values as `NARAD_REMOTES_*` environment variables, and `NARAD_CLUSTER_SECRET_PREVIOUS` from the Secret, which older binaries ignore.
- **Batch produce.** A v3.1.0 node refuses batches over 100 messages or 1 MiB, and compressed bodies. Move producers back first.

## Next steps

- [Replicate a topic to another cluster](../build/remote-children.md): attach, watch and detach a remote child.
- [Playbooks](playbooks/offload.md): move a topic to another cluster, and set up disaster recovery.
- [Monitor and alert](monitoring.md#remote-alerts): the alerts for links, credentials and keys.
