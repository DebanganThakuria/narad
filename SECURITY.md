# Security Policy

## Reporting a vulnerability

Please do not open a public GitHub issue for security vulnerabilities.

Use GitHub's private vulnerability reporting flow:

https://github.com/DebanganThakuria/narad/security/advisories/new

Include:

- a description of the vulnerability
- affected versions or commit SHAs
- reproduction steps or proof of concept
- impact assessment if known
- any suggested remediation

## Supported versions

Narad follows semantic versioning. Security fixes land on the most recent
minor line, and the fix ships as a patch release on that line. There are no
long-term support branches: upgrading to the current line is the supported
path.

| Version | Supported |
|---|---|
| 3.x | Yes |
| 2.x and earlier | No, upgrade to 3.x |

The `/v1` HTTP surface is stable across major versions, so upgrading is a
binary swap and a rolling restart rather than a client migration. See
[API stability](https://debanganthakuria.github.io/narad/client/guarantees-and-errors/#api-stability).

## Response targets

Narad is maintained by a single maintainer (see [MAINTAINERS.md](./MAINTAINERS.md)),
so these are goals rather than a contractual SLA:

| Stage | Target |
|---|---|
| Acknowledge a report | 5 business days |
| Initial assessment and severity | 10 business days |
| Fix released for a confirmed high-severity issue | 30 days |

## Disclosure process

The project acknowledges reports, assesses impact, develops a fix, and
coordinates disclosure once affected users have a reasonable opportunity to
upgrade. Fixes are published as a GitHub Security Advisory with a CVE where
one is warranted, and noted in [CHANGELOG.md](./CHANGELOG.md).

## Scope

The following are documented, deliberate configurations rather than
vulnerabilities, so please do not report them as such:

- **Running with security disabled.** `--dev` and `NARAD_SECURITY_ENABLED=false` turn authentication off by design, for local use.
- **The Raft port without TLS.** The cluster secret does not cover the Raft plane. Running it in plaintext requires `NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT=true`, and the risk is documented in [Deployment](https://debanganthakuria.github.io/narad/operate/#tls-story). A deployment that leaves 7943/tcp reachable is a misconfiguration we have warned about, not a product flaw.
- **The metrics listener served without credentials**, when `NARAD_HTTP_METRICS_UNAUTHENTICATED=true` is set explicitly.
- **No built-in request rate limiting.** Narad expects a rate limiter at the ingress, as documented.

A way to bypass any of these controls when they are configured on is in scope,
and so is anything that breaks the durability or access-control contract.

## Non-security issues

For crashes, correctness bugs, documentation issues, or operational questions
that do not expose a vulnerability, use public issues or GitHub Discussions
instead of the private security advisory flow.
