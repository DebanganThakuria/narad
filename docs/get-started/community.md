---
description: "Ask a question, report a bug, request a feature, report a vulnerability privately, or contribute a change to Narad."
---

# Get help and contribute

Ask a question, report a bug, request a feature, report a vulnerability privately, or contribute a change to Narad.

Everything happens on GitHub, in public, except vulnerability reports. Narad has one maintainer, so answers are best effort; [MAINTAINERS.md](https://github.com/DebanganThakuria/narad/blob/master/MAINTAINERS.md) says what to expect.

## Ask a question {#ask}

Use [GitHub Discussions](https://github.com/DebanganThakuria/narad/discussions) for usage questions, deployment notes and design discussion. Include the Narad version or commit SHA (`narad version`), the deployment shape, the relevant configuration, and the exact command or request you ran.

## Report a bug {#report-a-bug}

Open an issue with the [bug report form](https://github.com/DebanganThakuria/narad/issues/new/choose). It asks for the version, the environment, what happened, the steps to reproduce it, what you expected, and any logs. Say whether the bug reproduces on a fresh data directory.

[Troubleshooting](../operate/troubleshooting.md) matches status codes, metrics and log lines to their causes, and may answer the question first.

## Request a feature {#request-a-feature}

Open an issue with the [feature request form](https://github.com/DebanganThakuria/narad/issues/new/choose). Describe the problem before the solution: what you are trying to do, and what you tried instead.

## Report a vulnerability {#report-a-vulnerability}

Do not open a public issue or discussion for a vulnerability. Report it privately through [GitHub private vulnerability reporting](https://github.com/DebanganThakuria/narad/security/advisories/new). Every report gets an initial response within 14 days.

The [security policy](https://github.com/DebanganThakuria/narad/blob/master/SECURITY.md) lists what to include, which versions get fixes, the response targets, and which configurations are deliberate rather than vulnerabilities.

## Contribute a change {#contribute}

Changes land through pull requests on [GitHub](https://github.com/DebanganThakuria/narad/pulls). Open an issue or a discussion first for anything that changes the delivery contract, the on-disk format, the HTTP API or the cluster protocol.

[CONTRIBUTING.md](https://github.com/DebanganThakuria/narad/blob/master/CONTRIBUTING.md) covers the development setup, the test policy, and what a pull request needs before it is merged. New behavior comes with tests in the same pull request.

Participation is covered by the [Code of Conduct](https://github.com/DebanganThakuria/narad/blob/master/CODE_OF_CONDUCT.md). Contributions are licensed under the [Apache License 2.0](https://github.com/DebanganThakuria/narad/blob/master/LICENSE).
