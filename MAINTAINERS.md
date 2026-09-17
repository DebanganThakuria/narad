# Maintainers

## Current maintainers

| Name | GitHub | Areas |
|---|---|---|
| Debangan Thakuria | [@DebanganThakuria](https://github.com/DebanganThakuria) | All of it |

`CODEOWNERS` routes every review to that one account, which is the accurate
picture rather than a formality.

## What a single maintainer means for you

Stated plainly, because it should affect your decision to depend on Narad:

- **The bus factor is one.** If the maintainer stops, the project stops until someone forks it. Apache 2.0 means you can, and the repository is deliberately free of anything that would make a fork hard: no CLA, no trademark restrictions on the code, no private build infrastructure.
- **Reviews are best effort.** Expect a first response within about a week. A pull request that sits longer has not been rejected silently; ping it.
- **Coverage has gaps.** There is no on-call rotation and no guaranteed response outside the security targets in [SECURITY.md](./SECURITY.md).

If the maintainer intends to stop, the plan is to say so in the README and in a
pinned discussion, look for a successor, and archive the repository rather than
leave it looking maintained. Silence is the failure mode this note exists to
prevent.

## How decisions get made

Small changes: open a pull request. Anything that changes the delivery
contract, the on-disk format, the HTTP surface, or the cluster protocol wants
an issue or a discussion first, so the design argument happens before the
implementation cost is sunk. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## Becoming a maintainer

There is no hidden bar, and growing this table is an explicit goal. The path:

1. Land a handful of non-trivial pull requests, with tests, that show you understand the subsystem you are touching.
2. Review other people's pull requests. Reviewing well is the skill the project is short of, more than writing code.
3. Stay around for a couple of release cycles.

At that point you get an invitation, commit access, and a row in this table.
Interested before then? Open a discussion and say which area you want to own.
