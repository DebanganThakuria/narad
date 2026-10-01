---
description: "Look up what can change in Narad's /v1 HTTP API, how nodes on different releases work together, and which release these docs describe."
---

# API stability and versions

Look up what can change in Narad's `/v1` HTTP API, how nodes on different releases work together, and which release these docs describe.

## Changes within /v1 {#v1-changes}

Within `/v1`, the routes, parameters, status codes and JSON field names in the [HTTP API reference](http-api.md) do not change meaning and do not go away. What can appear:

- new optional request fields and parameters;
- new fields in responses;
- new endpoints.

So a client should ignore response fields it does not know. Request bodies stay strict: a field the server does not know gets `400`, which is how an older node refuses a field it cannot honour instead of dropping it silently.

A change that has to break the contract goes to a new `/v2` prefix, and both versions are served side by side for a deprecation period.

## Node-to-node protocol {#node-protocol}

Nodes talk to each other over a versioned protocol under the same rule. Operation codes are only ever added. New fields are optional and go at the end of a message. A node that does not know a new field answers with a clean `400`, and the sender retries in the older shape. That is what lets a cluster run two releases side by side during a rolling upgrade. Release-specific conditions, such as the config keys an older binary refuses, are in [Upgrade Narad](../operate/upgrade.md#version-notes).

## Which release these docs describe {#docs-version}

These docs follow `master`. The latest release is **v3.0.1**, tagged on 16 September 2026.

Anything in `master` but not in v3.0.1 carries this line under its heading:

**Unreleased:** in master, not in v3.0.1.

In a table, the item's name ends in "(unreleased)". The markers go when the next release ships. The [changelog](https://github.com/DebanganThakuria/narad/blob/master/CHANGELOG.md) lists every change by release.

What a v3.0.1 node does with the unreleased HTTP features, checked against a v3.0.1 build:

| Feature | A v3.0.1 node |
|---|---|
| Batch produce, `POST /v1/topics/{topic}/produce/batch` | answers `404` |
| Batch consume, `GET .../consume?max=N` | ignores `max` and answers with one message, in the single-message shape |
| Batch ack, a `receipt_handles` body | answers `400` (`receipt_handle required`) |
| A produce without a key | stores an invented key, `key-<n>`, which consumers see |
| `http.max_produce_in_flight_per_identity` in the config file | refuses to start with an `unknown field` error |
| `storage.consumer_offset_commit_interval_ms` or `storage.ingress_wal_prealloc` in the config file | refuses to start: the key is an internal setting there and cannot be configured |

A client that must work against both releases can send single produces, consumes and acks, and should always send a key when it relies on seeing one. To find the release a node runs, read the image tag it was deployed with: `narad version` on a v3.0.1 image prints a commit, not a version number.
