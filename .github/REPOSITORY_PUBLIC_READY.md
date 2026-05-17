# Repository public-readiness checklist

This repository has been prepared to be **public-ready** without actually
changing repository visibility yet.

## What is already added in-repo

- `LICENSE` — Apache License 2.0
- `CONTRIBUTING.md` — contributor workflow and expectations
- `CODE_OF_CONDUCT.md` — community expectations
- `SECURITY.md` — vulnerability reporting guidance
- issue templates for bugs and feature requests
- pull request template
- `dependabot.yml` for Go modules and GitHub Actions
- `CODEOWNERS`
- `.github/settings.yml` with intended repository settings

## Intended repository settings

These are declared in `.github/settings.yml` for a settings-sync tool or
manual application in GitHub.

### Merge strategy

- Allow **squash merge**
- Disable merge commits
- Disable rebase merge
- Delete head branches after merge
- Allow auto-merge

### Branch protection for `master`

- Require a pull request before merging
- Require 1 approving review
- Dismiss stale approvals on new commits
- Require CODEOWNERS review
- Require conversation resolution before merging
- Require branches to be up to date before merging
- Require these status checks:
  - `Build & vet`
  - `Unit tests`
  - `E2E tests`
- Disallow force pushes
- Disallow deletions
- Require linear history

### Security posture

- Enable Dependabot updates
- Enable vulnerability alerts
- Enable secret scanning
- Enable push protection

## Before making the repo public

Review these items manually:

- remove or redact any sensitive information in docs, examples, issues, and history
- verify badges and links resolve correctly from the public GitHub URL
- confirm the maintainer contact path for `SECURITY.md`
- decide whether `master` should be renamed to `main`
- review `README.md` language for claims about maturity and roadmap
- confirm whether GitHub Discussions should be enabled
- confirm whether Releases should be published and versioning policy documented

## How to apply settings

You can either:

1. apply `.github/settings.yml` with a settings-sync app such as Probot Settings, or
2. mirror the same settings manually in the GitHub repository settings UI

The repository should stay private until the validation checklist is done.
