# Contributing to Narad

Thanks for contributing to Narad.

## Before you start

- Open an issue or start a discussion before large changes so the work can be scoped before implementation starts.
- Keep pull requests focused. Small, reviewable changes are easier to land than broad refactors.
- If the change affects behavior, tests are expected in the same pull request.

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
- [ ] run `make test` locally
- [ ] updated docs if behavior or configuration changed
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

## Security issues

Please do not file public issues for vulnerabilities until a maintainer has had a chance to assess them. Follow the process in `SECURITY.md`.

## License

By submitting a contribution, you agree that your contributions will be licensed under the Apache License 2.0.
