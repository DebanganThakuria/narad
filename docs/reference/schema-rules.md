---
description: "Look up exactly which payloads a topic's JSON Schema accepts, which schema documents Narad registers, and which changes between versions it allows."
---

# Schema validation rules

Look up exactly which payloads a topic's JSON Schema accepts, which schema documents Narad registers, and which changes between versions it allows.

```sh title="Request"
curl -i -u "$AUTH" -X POST "$NARAD/v1/topics/payments/produce" \
  -H "Content-Type: application/json" \
  -d '{"amount": 5}'
```

```http title="Response"
HTTP/1.1 400 Bad Request
Content-Length: 133
Content-Type: application/json
Date: Mon, 28 Sep 2026 19:31:44 GMT

{"error":"invalid argument: schema: jsonschema validation failed with 'narad://schema/payments/1#'\n- at '': missing property 'id'"}
```

- `$NARAD` is the base URL of any node or of the load balancer, for example `http://127.0.0.1:7942`.
- `$AUTH` is `username:password` of a user with `produce` on the topic.
- The topic `payments` was created with the schema `{"type": "object", "required": ["id"]}`.

How to add a schema to a topic and change it is in [Enforce schemas on a topic](../build/schemas.md). This page is the rulebook behind it.

## Validation on produce {#validation}

When a topic has a schema, every produce body must be one JSON text, in valid UTF-8, that the topic's current schema version accepts. Anything else gets `400`, and nothing is written. The error message names the failing keyword and where in the payload it failed, and is cut at 2 KiB, so a schema with a huge `enum` cannot turn one refused produce into a large response. A topic without a schema stores any bytes.

**Unreleased:** when a full report of every violation would be very large (a megabyte of failing values, or many items each missing a long `required` list), the payload is checked without building one, and the `400` says so instead of naming a violation: validate a smaller part of the payload to find one. The verdict is the same either way.

What "one JSON text" means:

- Text that is not JSON, binary data, an empty body, trailing data after the value (`{"id":1} {"id":2}`), a UTF-8 byte-order mark, a raw control character inside a string, and invalid UTF-8 inside a string all get `400`.
- Any JSON value can be the payload: `[1,2]` and `"text"` are checked the same way objects are. A schema of `true` or `{}` therefore means "must be JSON" and nothing more.
- When an object repeats a key, the last value wins, and that value is the one checked.
- Payloads nest at most 256 levels of objects and arrays (`[[1]]` nests two); deeper gets `400` (`payload nests deeper than 256 levels`) before validation (**Unreleased**; it was 10,000). The validator builds an error at every level above a failing value, each with a copy of the path to it, so one 20 KB payload nested 9,990 deep made a node hold close to a gigabyte, and real documents nest a handful of levels.

How keywords behave:

- **Numbers are checked exactly**, not as 64-bit floats: `9007199254740993` is odd under `multipleOf: 2`, `9223372036854775808` is over `maximum: 9223372036854775807`, and `1e400` is a valid `number`. `1.0` and `1e2` are integers; `1.5` is not. A number whose exponent is beyond ±1000 (`1e1001`) is refused as an invalid payload. The same bound applies to numbers in a schema at registration.
- **`maxLength` and `minLength`** count Unicode characters, not bytes.
- **`pattern` and `patternProperties`** use Go's RE2 syntax and run in linear time; a lookahead or a backreference is refused at registration. Linear time still pays, on every byte of the string, for each partial match the pattern keeps alive and each step it walks to reach them, so registration also refuses a pattern that can cost more than 32 such steps per byte (**Unreleased**): an unanchored `a.{1000}b` keeps a thousand partial matches alive, and `(?:\B){1000}x` walks a thousand assertions, either one taking seconds on a 1 MiB string. Anchoring the pattern with `^` or shortening its repetitions fixes it; an unanchored UUID pattern costs about a dozen.
- **`enum`** of 16 or more values is checked by hash (**Unreleased**), so a large allowlist costs the same per value as a small one. A 45,000-value allowlist of integer IDs used to cost 14 ms per value checked.
- **`format` is checked on every draft**, the default 2020-12 included. Known formats such as `email`, `date-time`, `uuid`, `ipv4` and `uri` refuse bad values; an unknown format name is ignored.

A batch produce (**Unreleased**) checks each message this way before it stores any of them; the first message that fails decides the answer.

### Validation capacity {#validation-capacity}

**Unreleased.** Small payloads validate in well under a millisecond and run as they come. A payload above 16 KiB, and any payload on a schema whose validation cost is high (one registered before the rules below existed, one listing more than 1,024 `required`, `dependentRequired` or `dependencies` names, or more than 64 `patternProperties` keys in all, or one that applies more than 64 subschemas to one value, such as a `oneOf` or `anyOf` union of 64 or more branches), is validated under a per-node bound of one validation per CPU core. A produce that finds no free slot within 5 seconds gets [`503`](status-codes.md#status-503) (`schema: validation capacity busy, retry`) and nothing is written: the payload was never checked, so retry it, preferably through another node. `narad_schema_validations_in_flight` shows how full the bound is.

## Schema documents {#registration}

A schema is registered when a topic is created with `schema`, or changed with `schema`. The document must meet these rules, or the request gets `400`:

- It is a JSON object or `true`. `false` (which would refuse every message), `null`, strings, numbers and arrays are refused.
- It is at most 256 KiB and nests at most 64 levels. A request body over 1 MiB gets `413` before this check.
- `$schema`, when present, names a built-in draft: 2020-12 (the default), 2019-09, 07, 06 or 04.
- `$ref` resolves only inside the document: `#/$defs/...`, `#anchor` and `$dynamicRef`. References to `file://`, `http://`, `https://` or a relative path are refused wherever they sit, including in a `$defs` entry nothing points to yet. An empty `$ref`, or an absolute URL equal to the schema's own `$id`, counts as relative. The error never repeats the reference. `$id` is accepted and loads nothing.

Three shapes are refused at registration, because of how the validator evaluates them:

- **The same value reached by more than one path at one level**, such as `"allOf": [{"items": {"$ref": "#"}}, {"items": {"$ref": "#"}}]`, or an `anyOf` that lists the same recursive `$ref` twice. The validator remembers nothing between paths, so the work grows exponentially with payload depth: a 21-byte payload nested ten deep against four such branches takes over a second. Ordinary recursive schemas, where the recursion goes through different property names, or through `items` and `additionalProperties`, reach each value once and are accepted. The error names the two paths.
- **One value checked against one subschema through more than 64 paths** (**Unreleased**), recursive or not. The validator pays every path in full: an acyclic chain of `$defs` entries that each apply the next one twice (through `allOf`, `anyOf` or `oneOf`) reaches its last entry through 2<sup>n</sup> paths, and 1.4 KB of such a chain took two seconds to check the string `"x"`. Paths are counted on the references as the validator resolves them, whatever their form: `#/$defs/...`, `$anchor`, `$dynamicAnchor`, an `$id` or draft-04 `id` fragment, a nested resource, a pointer into a member no keyword owns, and every target a `$dynamicRef` can pick. A shared definition used under many property names is one path per value, and a union of up to 64 branches that each reference one base is within the limit (a union of 64 or more branches is then validated under the per-node [validation capacity](#validation-capacity) bound, whatever the payload's size). The error names the subschema.
- **A `$ref` cycle that re-applies a schema to the value being checked**, without going into a child value, through `not`, `if` or `oneOf` (`"not": {"$ref": "#"}`). The validator fails such a cycle where it first revisits a schema, so under a negation the result would depend on where the cycle was entered. The same cycle through `allOf`, `anyOf`, `then` or `else` is accepted, because it can only fail that branch, and so is a loop of nothing but `$ref`, which accepts no value at all.

## Compatibility {#compatibility}

A topic's schema history only grows. Each schema change is checked against the latest version on the cluster leader and stored as the next version number. A version is never overwritten or removed, and a topic holds at most 1000 versions; the next change gets `409`.

**Unreleased:** histories are also bounded in bytes, because every node stores, snapshots and restores them, and a fan-out child keeps its own copy of its parent's. A topic's stored versions may hold at most 4 MiB together, and every schema in the cluster at most 256 MiB, counting each child's copy. A schema change, a create with a schema, or a create-as-child or attach that would adopt a parent's history past either budget gets `409` naming the budget and what is stored. A history already over 4 MiB, stored before the budgets applied, keeps working, but it takes no new version and no new schema-less child, and the `409` says so: smaller versions cannot help, so put the new schema on a new topic. A cluster whose schemas already hold more than 256 MiB takes no new schema version on any topic, a new topic included, until topics are deleted to bring the total under the budget. Schemas are stored compacted (insignificant whitespace removed), so a client's formatting does not reach the history; versions stored before stay as they were.

Four rules about the change request:

- **Idempotent.** A `schema` equal to the current version (the same JSON value; formatting and key order do not matter) registers nothing and answers `200`, so a retry after a lost response does not grow the history.
- **Annotations are not a change** (**Unreleased**). A `schema` that differs from the current version only in `title`, `description`, `examples`, `$comment`, `default`, `deprecated`, `readOnly` or `writeOnly`, wherever a subschema sits, accepts exactly what the current version accepts: it registers nothing and answers `200` with the current version. A property named `description` is a property, not an annotation. To change documentation, change it together with a real widening.
- **Conditional.** With `"schema_base_version": N`, the change applies only if the current version is exactly `N`; otherwise `409` (`schema version conflict`) and nothing changes. Without it, the change is checked against whatever version is current when the leader applies it.
- **Not removable.** `"schema": null` gets `400`. A schema can only be widened, and `{}` is not a widening of a schema with properties (see `properties` below).

The check enforces one rule: every message the previous version accepted stays valid. It compares the two versions structurally and fails closed: a construct it cannot reason about may only stay exactly as it was, or disappear where removing it can only widen the schema. Anything else gets `400`, with a message that names the keyword and where it sits (`at /properties/qty: type "integer" no longer allowed`).

| Keyword | Allowed change |
|---|---|
| `type` | Add types to the set; `integer` may become `number`; drop the keyword. |
| `enum`, `const` | Add values (`const` may become an `enum` that contains it); drop. Both on one schema count as their intersection. |
| `minimum`, `exclusiveMinimum`, `maximum`, `exclusiveMaximum` | Loosen or drop; never add. |
| `multipleOf` | Change to a divisor of the old value; drop. |
| `minLength`, `minItems`, `minProperties` | Decrease or drop; a new one may appear only as `0`. |
| `maxLength`, `maxItems`, `maxProperties` | Increase or drop; never add. |
| `pattern`, `format` | Keep identical or drop. |
| `uniqueItems` | `true` may become `false` or absent; never add. |
| `required` | Remove names; never add. |
| `properties` | Add optional properties; never remove one. Each existing property is checked with these same rules. A new property whose name matches a previous `patternProperties` pattern must accept everything that pattern's schema did. |
| `additionalProperties` | `false` may open up (to absent, `true`, or a schema); a schema may only widen; a closed model or a schema may not appear where there was none. |
| `items` | Widen or drop; never add. The array form (tuples) is not supported. |
| `anyOf` | Every old branch must be covered by some new branch. |
| `allOf` | Every new branch must be implied by some old branch. |
| `$ref` | Only `#/...` pointers into the same document, with no sibling keywords (annotations, `$schema` and a root `$id` aside). Both sides are resolved before they are compared. Recursive schemas are compared the way recursive types are: a pair already being compared further up counts as compatible. A loop of nothing but `$ref` accepts nothing, so anything may replace it, and it may replace nothing. |
| `$schema` | Must not change. |

The other keywords:

- `oneOf`, `not`, `if`/`then`/`else`, `contains`, `propertyNames`, `dependentRequired`, `dependentSchemas` and `patternProperties` may stay identical or be removed. `patternProperties` may be removed only while `additionalProperties` stays open, and `prefixItems` only together with `items`. "Identical" follows `$ref`: a `not` that points at `#/$defs/x` counts as changed when `x` changes.
- A nested schema may always become `true` or `{}`, which accepts everything at its position.
- A keyword the schema's draft does not define (`const` before draft-06, `if`/`then`/`else` before draft-07, `prefixItems` before 2020-12, `additionalItems` from 2020-12 on) is an annotation to the validator, and the check treats it as one too.
- `unevaluatedProperties`, `unevaluatedItems` and `$dynamicRef` are accepted only in a part of the schema that is unchanged byte for byte.
- `title`, `description`, `default`, `examples` and `$defs` can change freely.

Two limits of the check:

- **An optional property added under an open content model is allowed**, even though a message that already carried that key with another type was valid before. This is the one deliberate exception. Use `"additionalProperties": false` when you need exact semantics.
- **Each keyword is compared only with the same keyword** in the previous version. A change that is safe only because of a different keyword (dropping `const: 5` for `minimum: 3`) is refused: widen the schema keyword by keyword.

## Fan-out children {#fan-out-children}

A child created under, or attached to, a parent with a schema adopts the parent's whole history: the same version numbers and the same documents. While it is attached, its parent manages it: changing the child's schema gets `409`, and every change to the parent's schema reaches the child in the same metadata write. After a detach, the child keeps the history it has and manages it again.

- A child can be attached, or attached again, only to a parent whose history is identical to its own, version for version. (**Unreleased**) Once every member runs this release, identical means the same JSON value: key order, whitespace and the spelling of a number (`10`, `1e1`) do not matter, so a history copied from what the API serves matches one stored with the client's formatting. Before that, the stored bytes must match.
- A child with a schema cannot go under a parent without one. A child without a schema adopts the parent's on attach.
- To link a child whose history has drifted, first make the parent's history, or the detached child's, the same as the other.

Fan-out itself is described in [Fan out and delay messages](../build/fanout-and-delay.md).

## When a version takes effect {#when-a-version-takes-effect}

A schema change answers once the new version is applied on the leader and on the node that took the request, so a read or a produce sent to that same node afterwards sees the new version. Every other node applies it within replication latency, usually milliseconds, and checks the next produce it receives against it. No restart or cache expiry is involved. A produce that reaches one of those other nodes inside that window is checked against the previous version.

Adding partitions, or any other change to a topic, leaves its schema alone. Deleting a topic deletes its history; a topic created again under the same name starts with no schema, or at version 1 with the schema it is created with.
