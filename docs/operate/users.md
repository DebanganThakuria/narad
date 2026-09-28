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

## Manage the root user {#root-admin}

The root user is called `admin`. The cluster creates it once, at its first start, when it has no users yet. It holds every permission, its grants cannot change, it cannot be deleted, and only the root user can change its password.

Its first password comes from the `admin-password` key of the chart's security secret (`NARAD_ADMIN_PASSWORD` outside the chart). If that key is missing, the node that creates the user generates a password and logs it once, at warning level. Find it in the logs of the first pods:

```bash
for pod in narad-0 narad-1 narad-2; do
  kubectl logs -n narad "$pod" | grep 'GENERATED password'
done
```

Change a generated password straight away. The secret is read only when the root user is created, so editing `admin-password` later changes nothing. Change the password through the API instead, signed in as `admin`:

```bash
ROOT_PASSWORD="$(openssl rand -base64 24)"
curl -s -o /dev/null -w '%{http_code}\n' \
  -u "$AUTH" -X PUT "$NARAD/v1/users/admin/password" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{"new_password": "$ROOT_PASSWORD"}
EOF
```

Store the new password where your team keeps root credentials, and update the secret to match so it does not mislead the next reader.

## Next steps

- [Access model and grants](../reference/access-model.md): see exactly what each action and pattern allows.
- [Connect and authenticate](../build/connect.md): what a service does with its username and password.
- [Monitor and alert](monitoring.md): set up the alerts every cluster needs.
