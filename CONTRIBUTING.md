# Contributing to Narad

Thanks for contributing to Narad.

## Before you start

- Open an issue or start a discussion before large changes so the work can be scoped before implementation starts.
- Keep pull requests focused. Small, reviewable changes are easier to land than broad refactors.
- If the change affects behavior, tests are expected in the same pull request.
- Use `SUPPORT.md` for questions and troubleshooting paths.

## Development setup

```sh
make tools-install
make build
make test
```

Useful targeted commands:

```sh
go test ./cmd/narad
go test ./internal/...
go test ./tests/e2e/... -race
```

## Coding guidelines

- Follow existing code structure and naming.
- Prefer simple changes over new abstractions.
- Keep public behavior and CLI/HTTP output stable unless the change explicitly updates it.
- Update documentation when user-facing behavior, configuration, or operations change.

## Pull request checklist

Before opening a pull request, make sure you have:

- [ ] added or updated tests for the change
- [ ] run `make check` locally (format, vet, docs version check, tests)
- [ ] updated docs if behavior or configuration changed
- [ ] added an entry under `## [Unreleased]` in `CHANGELOG.md` if the change is user-visible
- [ ] described the motivation and scope clearly in the PR

## Commit style

There is no strict commit-message format requirement, but concise messages that explain the intent are preferred.

## Reporting bugs

When filing a bug, include:

- Narad version or commit SHA
- reproduction steps
- expected behavior
- actual behavior
- relevant logs, stack traces, or failing requests

## Releasing

Maintainers only, and the order matters because CI enforces part of it.

1. Move the `## [Unreleased]` entries in `CHANGELOG.md` under a new version heading with today's date, add the compare link at the bottom, and leave `## [Unreleased]` empty above it.
2. Update any pinned `ghcr.io/debanganthakuria/narad:vX.Y.Z` reference in `README.md` and `docs/` to the version about to ship. `make check-release-refs` fails until the tag exists, which is expected at this point.
3. Land both on `master`.
4. Tag the merge commit and push the tag. The container workflow builds, signs, and publishes the image, stamping the tag into `narad version`.
5. Write the GitHub release notes from the changelog entry.

Step 2 before step 4 is what keeps the documentation from advertising a
version that is one release behind, which is the drift
`scripts/check-release-refs.sh` exists to catch.

## Security issues

Please do not file public issues for vulnerabilities until a maintainer has had a chance to assess them. Follow the process in `SECURITY.md`.

## License

By submitting a contribution, you agree that your contributions will be licensed under the Apache License 2.0.
