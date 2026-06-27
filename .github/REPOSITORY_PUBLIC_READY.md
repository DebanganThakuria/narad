# Repository public-readiness record

Narad is public as an Apache-2.0, pre-1.0 open-source project.

This file records the expected public repository posture. It is not a
production-readiness declaration for Narad deployments; production gates
remain documented in the README.

## In-repository OSS surface

- `LICENSE` — Apache License 2.0
- `README.md` — project overview, quickstart, status, benchmark notes,
  architecture, and production-readiness gates
- `CONTRIBUTING.md` — contributor workflow and local development commands
- `CODE_OF_CONDUCT.md` — community expectations
- `SECURITY.md` — private vulnerability reporting process
- `SUPPORT.md` — support, questions, and troubleshooting paths
- issue templates for bugs and feature requests
- pull request template
- `CODEOWNERS`
- Dependabot configuration for Go modules and GitHub Actions
- CI workflow: build/vet, unit, e2e, local cluster integration, chaos
- container workflow: multi-arch GHCR image publishing
- CodeQL workflow for static analysis
- `.github/settings.yml` documenting intended repository settings

## Live repository settings

Expected public settings:

- repository visibility: public
- default branch: `master`
- issues: enabled
- discussions: enabled
- projects and wiki: disabled
- squash merge: enabled
- merge commits and rebase merge: disabled
- auto-merge: enabled
- delete branches on merge: enabled
- vulnerability alerts and Dependabot security updates: enabled
- secret scanning and push protection: enabled

## Branch protection

The `master` branch should require:

- pull request before merge
- 1 approving review
- CODEOWNERS review
- stale approval dismissal on new commits
- conversation resolution
- branch up to date before merge
- linear history
- no force pushes
- no branch deletion

Required checks:

- `Build & vet`
- `Unit tests`
- `E2E tests`
- `Local cluster integration`
- `Local cluster chaos`

## Release posture

The first public alpha is `v0.1.0-alpha.1`.

Published image:

```text
ghcr.io/debanganthakuria/narad:v0.1.0-alpha.1
```

Future release tags should be immutable once published. Follow-up
documentation changes should land on `master` unless a replacement
release is intentionally planned.

## Remaining production gates

Narad is public-ready as an OSS project, but not yet ready for direct
production or externally exposed deployments. The README tracks the
current production gates, including API auth/rate limiting/TLS,
durability/DR, liveness behavior, partition rebalance, soak/SLOs, and
upgrade/rollback contracts.
