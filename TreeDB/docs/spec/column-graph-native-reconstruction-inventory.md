# Column Graph Native Reconstruction Inventory

This inventory is the V0 handoff for issue #1646. It records the clean
reconstruction base, the decoded vector PR stack disposition, and the carry
forward rules for building true TreeDB column-store-native vector graph search.

The target architecture is not the decoded `ColumnVectorGraph` path. The native
path must search through generic TreeDB column-store reader/cache APIs without
requiring a full decoded in-memory graph as the search substrate.

## Reconstruction Base

- Base branch: `codex/colgranule-m15c-column-asset-rewrite`
- Base PR: #1636
- Base commit: `3ed99fd05fa28687ed681b51a87e1bac2e10c402`
- Base commit subject: `collections: retain prepared column assets across reopen GC`
- Base issue: #1621, `Land TreeDB Column Store V1 physical execution engine`
- Post-V1 performance tracker: #1634, `Post-V1 TreeDB column store: close production-vs-experiment query performance gap`
- Native vector tracker: #1646, `Land TreeDB column-store-native vector graph search`

The V0 reconstruction branch starts from the #1621 top-of-stack physical column
asset implementation. It does not start from #1642, #1643, #1644, #1645, or any
local decoded-graph worktree.

## Current Architecture Boundary

The old vector stack proves this decoded path:

```text
physical column assets on disk
-> manifest/root lookup and physical scan
-> decode into Go slices
-> in-memory ColumnVectorGraph
-> ColumnVectorGraph.SearchCosine
```

That path is useful as persistence/load evidence and as an in-memory search
ceiling. It is not the product target for #1646.

The #1646 target is:

```text
collection column manifest/root snapshot
-> generic column-store graph reader/cache
-> fetch vector, invNorm, adjacency, and doc-id rows by ordinal/block
-> traverse/score/top-k without a full decoded in-memory graph copy
-> materialize documents only after top-k selection when public APIs require it
```

## #1621 And #1634 API Assumptions

The reconstruction assumes the #1621 physical column-store line provides or will
provide these generic capabilities:

- durable physical column asset publication through collection root/manifest state,
- reopen/recovery identity for typed column assets,
- serial physical scanning over typed column assets,
- planner/status surfaces that fail closed rather than silently falling back,
- column asset reachability, GC, and rewrite/remap integration,
- generic column-store visibility semantics for mutation-bearing manifests,
- benchmark/report plumbing that can distinguish physical paths from row-backed
  and decoded comparator paths.

The reconstruction assumes #1634 owns broad post-V1 production column-store
optimization. #1646 may require small local performance fixes for code it adds,
but it must not duplicate #1634 with vector-specific copies of generic reader,
cache, mark, locator, reducer, or prefetch work.

## Missing Generic Column-Store Primitives

These primitives block true native vector graph search unless supplied by the
base or added generically as part of #1646:

- ordinal-to-granule or ordinal-to-block lookup that does not scan prior
  granules,
- visible row-count and snapshot-generation APIs for one manifest/root
  generation,
- row and batch fetch for typed physical column rows,
- vector `[]float32` row fetch,
- scalar invNorm `float32` row fetch,
- adjacency `[]uint32` row fetch,
- explicit ordinal-to-document mapping row fetch: primary document ID, retained
  row locator, or both,
- graph metadata load and validation from the same manifest/root generation as
  the graph columns,
- mutation/tombstone visibility APIs that can safely answer graph reads or
  return rebuild-needed/unsupported status,
- bounded decoded-block cache semantics with eviction and resident-byte
  accounting,
- stats hooks for compressed bytes read, decoded bytes, row fetches, batch
  fetches, touched granules, cache hits/misses, resident bytes, and fallback
  reasons,
- corruption/mismatch/status error surfaces for missing, truncated, wrong-type,
  wrong-generation, and incompatible graph assets.

If a primitive is generally useful to the TreeDB column store, implement it as
generic column-store machinery first. Vector search is a consumer.

## Old PR Stack Disposition

| PR | Branch | Current role | Disposition | Notes |
| --- | --- | --- | --- | --- |
| #1642 | `codex/column-vector-graph-product-1621-integration` | Large integration base combining physical vector value support and the `column_graph` contract seam on an older #1636 head. | Source only / retarget generic pieces if needed. | Do not use as native-search ancestry. Harvest generic physical value support and status vocabulary only after reviewing the exact diff. |
| #1643 | `codex/column-vector-physical-loader-search` | Loads real physical column assets, fully decodes vector/invNorm/adjacency into Go slices, then searches an in-memory `ColumnVectorGraph`. | Retain as decoded evidence and oracle/comparator; do not present as native search. | Useful tests cover loader failure/status, rewrite/GC survival, schema mismatch, fallback, and mutation rebuild-needed status. |
| #1644 | `codex/column-vector-main-path-bench` | Benchmarks decoded physical-asset load, in-memory graph search, public search, and document materialization costs. | Retain as decoded benchmark evidence and ceiling comparator. | Useful benchmark discipline and labels should be adapted, but results are not native-reader product proof. |
| #1645 | `codex/column-graph-vector-user-docs` | User docs, demo, and scripts for the decoded physical-asset path, with corrected boundary language. | Adapt forward for docs/demo/benchmark scaffolding after relabeling to native-reader semantics. | Keep public-dataset script discipline and caveat wording; do not carry decoded path as product completion. |

External PR updates required after V0:

- If a PR remains open as decoded evidence, its title/body must say decoded
  full-graph behavior, not native column-store search.
- If a PR is source-only, add a comment pointing to #1646 and this inventory.
- If a PR is retargeted, narrow its title/body/scope to the generic support
  piece it still owns.

## Copy Forward

Copy these only after reviewing that they are generic and do not imply decoded
search as the final product path:

- physical column value support for vector `[]float32`,
- physical scalar `float32` support for invNorm,
- physical `[]uint32` adjacency-list value support,
- schema/type validation for those physical column values,
- durable manifest/root compatibility tests for generic physical column assets,
- generic status enums that do not label decoded search as native reader search,
- corruption/fallback tests that fail closed without requiring vector-specific
  storage shortcuts.

## Adapt Forward

Adapt these pieces only with explicit path identity and native-reader wording:

- explicit `column_graph` strategy selection,
- fallback/status plumbing,
- public collection/vector-index lifecycle tests,
- docs/demo examples,
- benchmark harnesses and scripts,
- old PR validation commands,
- benchmark result reporting and path-status proof.

Any adapted benchmark must distinguish setup/open/load/decode, native
reader/cache work, graph traversal/scoring, and post-top-k document
materialization.

## Keep As Oracle Or Comparator

Keep these paths only as labeled evidence, not as the #1646 product target:

- decoded physical-column loader into `ColumnVectorGraph`,
- `ColumnVectorGraph.SearchCosine` serial and parallel benchmarks,
- decoded load/search public API benchmarks,
- small deterministic decoded graph tests used as native-reader correctness
  oracles,
- old benchmark result tables used as in-memory ceiling comparisons.

Allowed labels include `column_graph_decoded`,
`column_graph_decoded_comparator`, or equivalent. They must not report
`column_graph_native_reader`.

## Quarantine

Keep these outside the V0-V4 product path unless a later issue explicitly
reintroduces them through generic column-store APIs:

- Deep1B and large-scale dataset experiments,
- geometric/JZip/spherical/vector-codec experiments,
- quantization and residual-sketch experiments,
- dynamic overlay/sealing prototypes,
- vector-specific cache implementations,
- custom vector sidecar lifecycle code,
- benchmark-only scripts that are not CI-safe.

## Do Not Copy

Do not copy these into the native-reader product implementation:

- public search code that reloads and fully decodes the graph per call,
- product-facing APIs that make decoded graph loading look like final
  `column_graph` behavior,
- a vector-only persistence format,
- vector-specific caches that are unaccounted full decoded graph copies,
- mutation/update/delete logic that bypasses generic column-store visibility,
- benchmarks or docs that claim native column-store search while searching a
  full decoded `ColumnVectorGraph`,
- any fallback from native search to decoded, exact, row-store, sidecar, or
  legacy/native vector-index behavior without explicit status.

## Required PR Body Controls

Each #1646 implementation PR must include the sections below, either through the
repository PR template or manually in the PR body:

- `Copied/Adapted From Old Stack`
- `Path Identity`
- `Base And Dependency State`
- `Evidence`
- `Test Plan Start`
- `Performance Plan Start`
- `Test Plan Close`
- `Performance Plan Close`
- `AI Review Loop`

Each stacked PR must also state:

- base PR/head,
- whether parent review fixes have been propagated,
- whether the PR is native reader, decoded comparator, legacy/native vector
  index, or unsupported/fail-closed work.

## V0 Test And Performance Plan

V0 changes docs, issue-driven process controls, and PR-template expectations. It
does not change runtime code and must not make product performance claims.

Required validation:

- docs test proves this inventory contains the exact base branch/commit, #1621
  and #1634 assumptions, missing primitive list, and #1642-#1645 dispositions,
- PR-template check proves required #1646 sections are present,
- docs/spec manifest test proves this document is linked from
  `TreeDB/docs/spec/README.md`,
- `git diff --check`,
- no benchmark is required for V0 unless runtime code changes; the performance
  statement is "no runtime code changed, no benchmark labels claim native reader
  speed."

## Next PRs

The next PR after V0 should start from this V0 branch or its merged equivalent
and target the generic carried-forward support slice:

- port only generic `[]float32`, scalar `float32`, and `[]uint32` physical
  column value support that the old stack proves useful,
- keep decoded graph loader/search as comparator only,
- add focused tests and microbenchmarks for the carried-forward generic support,
- request Codex, Copilot, and CodeRabbit latest-head reviews before claiming the
  PR is ready.
