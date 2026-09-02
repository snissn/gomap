# Query-independent query-ready generation open

M3 opens a caller-selected QRBG or consolidated QRDG base plus bounded QRDG
deltas into one immutable, generation-scoped set of encoded direct views. Open
is explicitly separate from query execution. It validates the complete file
set once and never calls `ColumnPartFromImage`, builds latest-row ranks, decodes
dictionaries, reconstructs offsets, or copies encoded payload bytes.

This is a derived data-plane object. Authoritative manifest selection remains
with the collection publication/recovery inventory; lifecycle roots and pins
remain with #1954; deletion safety remains with #3681. M3 does not scan a
directory, choose a root, publish files, or unlink them.

## Identity and collection ownership

`collections.collectionQueryReadyGenerationOpenKey` derives the open key from
the existing `ColumnStoreCacheIdentity`. The key includes the collection schema,
active manifest generation and checksum, recovery-authoritative identity and
applied command LSN, catalog root/commit, and manifest root/name. It has no
query kind, predicate, grouping, projection, or q1-q5 field.

The collection retains one current key. A key change marks the old entry stale
and installs a new generation scope. Reader leases keep old mappings alive
across invalidation; the final old lease release closes the stale mappings.
Collection-manager close detaches the cache and uses the same lease boundary.
This is a single current-generation cache, not a parallel history registry.

The low-level open cache returns a borrowed raw pointer. A direct caller owns
lifetime coordination: it keeps the cache open, quiesces all readers before
cache close, and does not close the published prepared generation directly.
The production collection seam always returns a reader lease; its prepared
pointer is valid only until lease close.

## States and failure atomicity

The observable states are:

- `absent_rebuildable`: at least one selected derived asset is missing; a later
  call may retry after the existing owner rebuilds it;
- `validating`: one opener is validating while concurrent callers wait;
- `ready`: one immutable prepared object was atomically published and cache
  hits return the same object;
- `unsupported_or_stale`: the schema, generation, manifest identity, or format
  version is incompatible;
- `corrupt`: checksums, bounds, structure, or another fail-closed invariant is
  invalid.

No prepared pointer is published until every base/delta mapping, QRBG/QRDG
validation, identity check, and M2 bound check succeeds. Partial failure closes
all mappings opened by the attempt. A corrupt or stale entry remains
fail-closed for that identity; repair is represented by a new authoritative
manifest identity. Missing assets alone are retryable.

## Segment ranges and direct views

`QueryReadyGenerationFile` carries `Path`, `Offset`, and `Length`, matching the
column asset manager's segment reference. Zero offset and length select a whole
standalone file for tests and tools. Otherwise both QRBG and QRDG open exactly
the supplied non-empty in-bounds range. The mmap owner retains the page-aligned
backing span and unmaps that owner, never the sliced logical view.

Open counters distinguish logical image bytes from actual page-aligned mapped
span bytes. They also report mapped files, validated structures/bytes, waits,
cache hits, rebuilds, and open/validation time. The payload decoded/copied,
whole-part decode, dictionary-domain, rank, and offset-construction counters are
required to remain zero after a successful M3 open. Query execution cannot
mutate them.

## Performance evidence boundary

Focused benchmarks cover cold open, warm cache hits, concurrent warm access,
live heap, and direct-view access orders with `-benchmem -count=5`. The
`shape-q1` through `shape-qexpr` access probes only permute M3 metadata/direct
view access. They are not JSONBench operators, canonical q1-q5 correctness
evidence, or q1/q3 CPU profiles. M4 owns encoded base-plus-delta operators and
M6 owns the canonical integrated query-order/profile matrix once production
runners consume these views.
