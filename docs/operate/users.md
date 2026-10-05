---
description: "Create a user for each service and give it only the grants it needs."
---

# Manage users and grants

Create a user for each service and give it only the grants it needs.

Before you start: credentials of a user with the `admin` grant (on a new cluster, the root user `admin`), a cluster with security on, and `jq` for readable output.

## Create a user {#create-user}

=== "curl"

    ```bash
    SERVICE_PASSWORD="$(openssl rand -base64 24)"
    curl -s -u "$AUTH" -X POST "$NARAD/v1/users" \
      -H "Content-Type: application/json" \
      -d @- <<EOF | jq .
    {
      "username": "billing-service",
      "password": "$SERVICE_PASSWORD",
      "grants": [
        {"action": "produce", "patterns": ["invoices.*"]},
        {"action": "consume", "patterns": ["payments.*"]}
      ]
    }
    EOF
    ```

=== "CLI"

    ```bash
    SERVICE_PASSWORD="$(openssl rand -base64 24)"
    printf '%s\n' "$SERVICE_PASSWORD" | narad user add billing-service \
      --user-password-stdin \
      --grant 'produce:invoices.*' \
      --grant 'consume:payments.*'
    ```

```json title="Response"
{
  "username": "billing-service",
  "grants": [
    {
      "action": "produce",
      "patterns": [
        "invoices.*"
      ]
    },
    {
      "action": "consume",
      "patterns": [
        "payments.*"
      ]
    }
  ],
  "created_at_ms": 1790625209680,
  "updated_at_ms": 1790625209680
}
```

- `$NARAD` is the base URL of any node or of the load balancer, for example `http://127.0.0.1:7942`.
- `$AUTH` is `username:password` of an admin, for example `admin:<root password>`.
- The CLI reads the broker URL and the admin's credentials from its active context, or from `NARAD_ADDR`, `NARAD_USER` and `NARAD_PASS` ([CLI command reference](../reference/cli.md#ctx)).

The request answers `201 Created` and never returns the password. Hand `billing-service` and its password to the service's owners; the service sends them as HTTP Basic credentials ([Connect and authenticate](../build/connect.md#credentials)).

Usernames are 1 to 64 characters from `A-Z a-z 0-9 . _ -`. Passwords are 1 to 72 bytes, counted in bytes, not characters. A longer password gets `400`.

## Choose the grants {#choose-grants}

A [grant](../reference/glossary.md#grant) is an action plus a list of topic-name patterns. The actions are `produce`, `consume` (which includes ack), `create` and `admin`. A pattern is an exact topic name or a prefix ending in `*`: `invoices.*` matches `invoices.eu` and `invoices.us`. The full model, including who owns a topic, is in [Access model and grants](../reference/access-model.md).

- **One user per service.** Scope it to exactly the topics that service touches.
- **Keep `produce` and `consume` apart.** A service that only produces to a topic cannot read or remove what it wrote.
- **Keep `admin` for people and deployment tooling.**

Three rules apply to every change:

- You cannot give another user a grant you do not hold yourself (`403 cannot grant permissions you do not hold`).
- Only the root user can give the `admin` grant.
- You cannot change your own grants (`403 cannot modify your own grants`).

## List users {#list-users}

=== "curl"

    ```bash
    curl -s -u "$AUTH" "$NARAD/v1/users" | jq .
    ```

=== "CLI"

    ```bash
    narad user ls
    ```

```json title="Response"
[
  {
    "username": "admin",
    "root": true,
    "created_at_ms": 1790623092462,
    "updated_at_ms": 1790623092462
  },
  {
    "username": "billing-service",
    "grants": [
      {
        "action": "produce",
        "patterns": [
          "invoices.*",
          "receipts.*"
        ]
      },
      {
        "action": "consume",
        "patterns": [
          "payments.*"
        ]
      }
    ],
    "created_at_ms": 1790625188042,
    "updated_at_ms": 1790625188098
  }
]
```

To read one user, `GET /v1/users/billing-service` returns the same shape for that user alone. Only admins can list or read users.

## Change a user's grants {#update-grants}

The new list replaces the old one, so send every grant the user should keep. Here `billing-service` also gets to produce to `receipts.*`:

=== "curl"

    ```bash
    curl -s -u "$AUTH" -X PUT "$NARAD/v1/users/billing-service/grants" \
      -H "Content-Type: application/json" \
      -d '{"grants": [
            {"action": "produce", "patterns": ["invoices.*", "receipts.*"]},
            {"action": "consume", "patterns": ["payments.*"]}
          ]}' | jq .grants
    ```

=== "CLI"

    ```bash
    narad user grant billing-service \
      --grant 'produce:invoices.*,receipts.*' \
      --grant 'consume:payments.*'
    ```

```json title="Response"
[
  {
    "action": "produce",
    "patterns": [
      "invoices.*",
      "receipts.*"
    ]
  },
  {
    "action": "consume",
    "patterns": [
      "payments.*"
    ]
  }
]
```

The CLI prints the whole user. Each node applies the change through Raft and checks it on the user's next request.

## Change a password {#change-password}

A user can change their own password by sending the current one. An admin can reset another user's password without it. The CLI has no command for this, so use the API:

```bash
NEW_PASSWORD="$(openssl rand -base64 24)"
curl -s -o /dev/null -w '%{http_code}\n' \
  -u "billing-service:$SERVICE_PASSWORD" -X PUT \
  "$NARAD/v1/users/billing-service/password" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{"current_password": "$SERVICE_PASSWORD", "new_password": "$NEW_PASSWORD"}
EOF
```

```text title="Output"
204
```

An admin resetting the password sends `-u "$AUTH"` and leaves out `current_password`. A wrong current password gets `403 current password is incorrect`. The old password stops working on the next request.

A password change touches only the password, and a grants change only the grants. Two admins changing different fields at the same moment cannot undo each other's change.

## Delete a user {#delete-user}

=== "curl"

    ```bash
    curl -s -o /dev/null -w '%{http_code}\n' \
      -u "$AUTH" -X DELETE "$NARAD/v1/users/billing-service"
    ```

=== "CLI"

    ```bash
    narad user rm billing-service
    ```

The API answers `204` and the CLI prints nothing. You cannot delete your own account or the root user.

**Unreleased:** deleting a user also releases the topics it owned: their owner is cleared, so only admins manage them, and a user created later under the same name does not inherit them. Earlier releases left the name on the topics, and whoever was created under it next owned them. While any node runs an older release, the delete removes only the user and the leader logs `user deleted, but its topics still name it as owner` at warning level (`component=audit`), with the `topics` (up to 20), `topic_count` and the `reason`, which names the node holding the change back. Until that node is upgraded, do not create a user with the deleted name; delete those topics, or recreate them as an admin, instead.

## Audit trail {#audit}

**Unreleased:** every user create, delete, password change and grants change, and every decommission, decommission cancel and [forget](troubleshooting.md#raft-server-no-member-record), writes one log line on the node the client called, also when that node forwarded the change to the Raft leader: message `audit`, attribute `component=audit`, with `event` (`user.create`, `user.delete`, `user.password`, `user.grants`, `cluster.decommission`, `cluster.decommission.cancel` or `cluster.forget`), `actor` (the caller, empty with security off), `target`, `status` (the HTTP status the client got) and `outcome`:

| `outcome` | Meaning |
|---|---|
| `ok` | Applied. |
| `denied` | Refused with `403`, for example a grant the caller does not hold or a wrong current password; logged at warning level. |
| `rejected` | Refused with another `4xx` by the node or the leader. Nothing changed. |
| `failed` | A `5xx` decided by the node or the leader. |
| `unknown` | The request ended without a decision the node knows: a forward to the leader whose answer never came back (`503`), or a client that went away mid-change (`499`). The change may have been applied; read the user or the members to find out. |

A request refused before these checks (a caller who is not an admin, a malformed body) writes no audit line. Route `component=audit` to its own sink if you keep an audit trail.

## Manage the root user {#root-admin}

The root user is called `admin`. The cluster creates it once, at its first start, when it has no users yet. It holds every permission, its grants cannot change, it cannot be deleted, and only the root user can change its password.

Its first password comes from the `admin-password` key of the chart's security secret (`NARAD_ADMIN_PASSWORD` outside the chart). If that key is missing, the node that creates the user generates a password. The password is never logged (unreleased; earlier releases logged it once, at warning level). That node writes it to the file `admin-password` in its data directory (`storage.data_dir`, `/var/lib/narad` under the chart), readable only by the Narad process user, and logs `seeded root admin with a generated password` at warning level with the file's `path` and its `node`. Find that node in the logs of the first pods, then read the file on it:

```bash
for pod in narad-0 narad-1 narad-2; do
  kubectl logs -n narad "$pod" | grep 'seeded root admin with a generated password'
done
kubectl exec -n narad narad-0 -- cat /var/lib/narad/admin-password  # the pod that logged the line
```

An existing file is never overwritten: if `admin-password` was already there, the log line names `admin-password.<digits>` instead. If the node cannot write the file, it does not create the root user yet: it logs `not seeding the root admin yet` at error level and tries again every 2 seconds.

Change a generated password straight away, then delete the file. The secret is read only when the root user is created, so editing the secret's `admin-password` key later changes nothing; a node that starts with `NARAD_ADMIN_PASSWORD` set on a cluster that already has users, and finds it is not root's password, logs `NARAD_ADMIN_PASSWORD is set but ignored` at warning level (unreleased). Change the password through the API instead, signed in as `admin`:

```bash
ROOT_PASSWORD="$(openssl rand -base64 24)"
curl -s -o /dev/null -w '%{http_code}\n' \
  -u "$AUTH" -X PUT "$NARAD/v1/users/admin/password" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{"new_password": "$ROOT_PASSWORD"}
EOF
```

Store the new password where your team keeps root credentials, and update the secret to match so it does not mislead the next reader. If the password was generated, delete the file it was read from:

```bash
kubectl exec -n narad narad-0 -- rm /var/lib/narad/admin-password
```

## Next steps

- [Access model and grants](../reference/access-model.md): see exactly what each action and pattern allows.
- [Connect and authenticate](../build/connect.md): what a service does with its username and password.
- [Monitor and alert](monitoring.md): set up the alerts every cluster needs.
