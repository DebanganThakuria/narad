# Contributing to Narad

Thanks for contributing to Narad.

## Before you start

- Open an issue or start a discussion before large changes so the work can be scoped before implementation starts.
- Keep pull requests focused. Small, reviewable changes are easier to land than broad refactors.
- If the change affects behavior, tests are expected in the same pull request. See [Test policy](#test-policy).
- Every pull request is reviewed by the maintainer listed in [MAINTAINERS.md](./MAINTAINERS.md), which also says how decisions are made and how fast to expect an answer.
- Use `SUPPORT.md` for questions and troubleshooting paths.
- Participation is covered by the [Code of Conduct](./CODE_OF_CONDUCT.md).

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
go test ./tests/linearizability/...
```

One suite does not run under `make test`: the nightly delivery-contract
check. It starts a three-node cluster on loopback, kills and partitions
nodes under load, and checks the recorded history against a sequential
specification of at-least-once delivery.

```sh
./scripts/linearizability-nightly.sh
```

Partition faults need `iptables` and passwordless `sudo`; without them it
injects kills only, which is the normal case on a laptop. The verdicts, the
flags, and what the check does not catch are in
[Checking the Delivery Contract](https://debanganthakuria.github.io/narad/understand/linearizability/).

## Test policy

New functionality comes with tests in the automated suite, in the same pull
request. A bug fix should come with a test that fails without the fix. "Automated"
means a test CI runs: a Go test under `make test`, or a case in the e2e,
cluster integration or chaos suites. A pull request that changes behavior
without a test will be asked for one in review.

## Coding guidelines

- Follow existing code structure and naming.
- Prefer simple changes over new abstractions.
- Keep public behavior and CLI/HTTP output stable unless the change explicitly updates it.
- Update documentation when user-facing behavior, configuration, or operations change.

## Requirements for acceptance

A pull request is merged when:

- it is focused on one change, with the motivation and scope described
- it follows the [test policy](#test-policy)
- CI passes on it: build, `go vet`, the format check, unit tests under `-race`, e2e, the local cluster integration and chaos runs, govulncheck and CodeQL
- docs and `CHANGELOG.md` are updated where the checklist below asks for them
- the code owner has approved it
- it can be licensed under the Apache License 2.0 (see [License](#license))

## Pull request checklist

Before opening a pull request, make sure you have:

- [ ] added or updated tests for the change
- [ ] run `make check` locally (format, vet, docs version check, tests)
- [ ] updated docs if behavior or configuration changed, marking what is new as unreleased ([which release the docs describe](https://debanganthakuria.github.io/narad/reference/api-stability/#docs-version))
- [ ] added an entry under `## [Unreleased]` in `CHANGELOG.md` if the change is user-visible
- [ ] described the motivation and scope clearly in the PR

## Commit style

There is no strict commit-message format requirement, but concise messages that explain the intent are preferred.

## Reporting bugs and requesting features

File bugs with the bug report form and ideas with the feature request form,
both at https://github.com/DebanganThakuria/narad/issues/new/choose.

When filing a bug, include:

- Narad version or commit SHA
- reproduction steps
- expected behavior
- actual behavior
- relevant logs, stack traces, or failing requests

## Releasing

[Maintainers](./MAINTAINERS.md) only, and the order matters because CI enforces part of it.

1. Move the `## [Unreleased]` entries in `CHANGELOG.md` under a new version heading with today's date, add the compare link at the bottom, and leave `## [Unreleased]` empty above it.
2. Name the new version wherever the docs mark something unreleased: the line under a heading becomes `**New in vX.Y.Z.**`, and "(unreleased)" becomes "(vX.Y.Z)" or "(from vX.Y.Z)". Keep what the text says about older releases. In `docs/reference/openapi.yaml`, `x-narad-since: unreleased` becomes the version; in `scripts/gen_http_api.py`, `UNRELEASED_LINE` names the new latest release; then regenerate `docs/reference/http-api.md` with `python3 scripts/gen_http_api.py`.
3. Update any pinned `ghcr.io/debanganthakuria/narad:vX.Y.Z` reference in `README.md` and `docs/` to the version about to ship. `make check-release-refs` fails until the tag exists, which is expected at this point.
4. Land all three on `master`.
5. Tag the merge commit and push the tag. The container workflow builds, signs, and publishes the image, stamping the tag into `narad version`.
6. Write the GitHub release notes from the changelog entry. If the release fixes a vulnerability that has a CVE or a GitHub Security Advisory, the changelog entry names it under `### Security`, and so do the release notes.

Step 3 before step 5 is what keeps the documentation from advertising a
version that is one release behind, which is the drift
`scripts/check-release-refs.sh` exists to catch.

## Security issues

Please do not file public issues for vulnerabilities until a maintainer has had a chance to assess them. Follow the process in `SECURITY.md`.

## License

By submitting a contribution, you agree that your contributions will be licensed under the Apache License 2.0.
