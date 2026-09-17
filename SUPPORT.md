# Support

Narad is an open-source project maintained by one person (see
[MAINTAINERS.md](./MAINTAINERS.md)). Support is best-effort and
community-driven.

## Questions and usage help

Use GitHub Discussions for questions, design discussion, deployment
notes, and general troubleshooting:

https://github.com/DebanganThakuria/narad/discussions

Before posting, include the Narad version or commit SHA, deployment
shape, relevant configuration, and the exact command or request you ran.

Most usage questions are answered in the
[documentation](https://debanganthakuria.github.io/narad/), which covers
the client API, operations, and the internals of every subsystem.

## Bugs

Use the bug report template for reproducible product issues:

https://github.com/DebanganThakuria/narad/issues/new/choose

Include logs, request/response examples, and whether the issue reproduces
on a fresh data directory.

## Security

Do not report vulnerabilities in public issues or discussions. Follow
the private process in [`SECURITY.md`](./SECURITY.md), which also lists
which versions receive fixes and which configurations are out of scope.

## Deciding whether to run it

[Project status](./README.md#project-status) states what is well covered
by tests and the three structural limits worth weighing: no ordering
guarantee, no synchronous replication, and months of track record rather
than years. [Compare](https://debanganthakuria.github.io/narad/compare/)
puts those against Kafka, NATS, RabbitMQ, SQS, Redis, and Pulsar, and
says which to pick instead when one of them is a hard requirement.
