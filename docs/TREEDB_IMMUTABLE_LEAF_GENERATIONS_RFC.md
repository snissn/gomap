# TreeDB RFC: Immutable Leaf Generations

Status: Draft

Owner: TreeDB

Date: 2026-04-14

## 1. Summary

This RFC changes the architectural model for outer leaves stored outside
`index.db`.

The current design treats outer-leaf storage as part of generic value-log
maintenance. That has led to the wrong maintenance shape: leaf pages are being
managed through rewrite-oriented logic that was designed for value records.

This RFC replaces that model with a stricter split:

- `leaf_vlog` becomes an immutable leaf-generation store.
- `value_vlog` remains the mutable append-only value log for value records.
- `wal` remains durability/recovery infrastructure only.

The key design decision is:

- steady-state leaf maintenance is generation lifecycle management, not
  stale-ratio-driven rewrite

The practical consequence is:

- delete leaf maintenance from generic `ValueLogRewriteOnline`
- stop treating outer-leaf storage as a generic rewrite target
- use sealing, pinning, generation GC, and rare explicit leaf-pack compaction
  for leaf storage

## 2. Problem

TreeDB currently stores outer leaf pages outside `index.db`, but the maintenance
model still largely follows value-log thinking.

That is wrong for two reasons.

First, the physical objects are different:

- `value_vlog` stores value records
- `leaf_vlog` stores page-shaped outer leaves

Second, the reclaim problems are different:

- value records become stale when pointers are overwritten or deleted
- outer leaves become stale when newer tree commits stop referencing older leaf
  pages

The current code mixes these domains:

- generic rewrite scans live tree state for stale value-log work
- leaf maintenance is attached as a special subphase inside generic value-log
  rewrite
- scheduler policy, stats, and execution flow now carry complexity from both
  models

This produces pathological behavior:

- leaf maintenance is selected via value-log-style rewrite triggers
- leaf maintenance is executed by tree walk and remap logic inside generic
  rewrite
- leaf storage retention is treated as a rewrite problem instead of a generation
  lifecycle problem

This is the wrong control plane.

## 3. Decision

TreeDB should model outer-leaf storage as immutable generations.

The preferred design is:

1. Writes append new leaf pages only to the current writable leaf generation.
2. Commits may leave the live tree referencing leaf pages from many sealed
   generations.
3. Sealed generations are immutable.
4. Snapshots pin generations.
5. Whole generations are garbage-collected when they become fully unreachable.
6. Rarely, a maintenance job may repack live pages from old sparse generations
   into a fresh compact generation. This is called `leaf-pack`.

This is a hard architectural preference, not one option among many.

### 3.1 Clarification: Generations Track Pages, Not Keys

This RFC is easy to misread if the reader thinks generations advance logical
keys. That is not the model.

The unit of storage and movement is the outer leaf page.

That means:

- a key that is not modified does not move merely because the writable
  generation rolls
- sealing a generation does not rewrite its pages
- a key remains in the generation that contains the leaf page where it last
  landed
- a key only moves when the leaf page containing it is rewritten by the normal
  write path, or when a rare `leaf-pack` copies that still-live page into a new
  compact generation

This also means the system is page-granular, not key-granular:

- if one key on a page changes and the page is rewritten, neighboring unchanged
  keys on that page may move with it
- if a sealed generation still has even one live page, the generation remains
  retained until that page dies or a pack operation copies it elsewhere

### 3.2 Pathologies This RFC Explicitly Rejects

This RFC is intended to prevent a specific set of pathological behaviors.

The design must not drift into any of the following:

- generation rollover that copies or rewrites untouched data
- background leaf maintenance that repeatedly scans the live tree just to
  rediscover stale leaf debt
- leaf maintenance implemented as a special case inside generic
  `ValueLogRewriteOnline`
- eager continuous pack activity that behaves like a hidden rewrite loop
- key-granular leaf movement outside normal page rewrites or explicit pack
- ordinary generation GC that tries to extract sparse live pages instead of
  either retaining the generation or invoking pack

## 4. Non-Goals

- This RFC does not redesign `value_vlog` into a generation store.
- This RFC does not attempt backward-compatible migration.
- This RFC does not require the final directory rename away from `leaf_vlog`.
- This RFC does not solve sync-path performance in the same sprint.

## 5. Why This Is Better

This design aligns the maintenance model with the physical object being stored.

For `leaf_vlog`, the important truths are:

- leaf pages are immutable once written
- roots and snapshots define reachability
- old generations can be retained safely until readers drain

That naturally suggests:

- manifest-like generation publication
- snapshot pinning
- whole-generation GC
- rare rebuild/pack operations

It does not naturally suggest:

- stale-ratio heuristics
- generic rewrite queues
- per-segment rediscovery scans in steady state

## 6. Storage Model

### 6.1 Terminology

- `leaf page`: a serialized outer leaf page stored outside `index.db`
- `leaf generation`: an immutable sealed set of one or more leaf-log files
- `writable generation`: the only generation that may receive new leaf pages
- `sealed generation`: immutable generation no longer accepting writes
- `retired generation`: generation no longer reachable from the current live
  state, but still pinned by readers/snapshots
- `deleted generation`: physically removed generation

### 6.2 Physical Layout

Semantically, `leaf_vlog` becomes a generation store.

To reduce rollout churn, the initial implementation may keep the existing
`leaf_vlog/` directory name even though its semantics are no longer “generic
value log”.

Preferred logical layout:

- `<maindb>/leaf_vlog/`
  - `manifest.json`
  - `gen-<id>/`
    - `leaf-l<lane>-<seq>.log`
    - optional per-generation metadata

The directory name may later be renamed to `leaf_store/`, but this RFC does not
require that rename in the first implementation.

### 6.3 Pointer Identity

Existing `LeafRef` addressing should be preserved if practical.

A `LeafRef` must continue to identify:

- the physical file
- the offset
- the encoded/compressed record length as needed for safe readback

Generation identity does not need to be embedded into the pointer if each file
belongs to exactly one generation and the manifest maps file IDs to generation
membership.

## 7. Manifest Model

The leaf store needs an explicit manifest.

The manifest is the authoritative description of:

- current writable generation ID
- sealed generations
- file membership by generation
- generation state
- generation commit sequence watermark

Suggested manifest-level fields:

- `format_version`
- `current_generation_id`
- `generations[]`
  - `generation_id`
  - `state`
  - `file_ids`
  - `created_commit_seq`
  - `sealed_commit_seq`
  - `published_commit_seq`
  - optional `stats`

The manifest is operational metadata. It is not the source of truth for page
reachability. Reachability comes from the published roots and any pinned
snapshots.

### 7.1 Generation States

The initial state machine should stay small.

Preferred states:

- `writable`: current append target for new leaf pages
- `sealed`: immutable generation still potentially referenced by live roots
- `retiring`: no longer required by current live roots, but still pinned by one
  or more snapshots/readers
- `deleted`: physically removed and absent from the manifest

The RFC intentionally does not require a large workflow state machine.

### 7.1.1 Preferred Lane Model For First Rollout

The first rollout should keep a single dedicated writable leaf lane and a single
writable leaf generation.

Reason:

- current TreeDB already routes outer-leaf writes through a dedicated reserved
  leaf lane
- multiple writable leaf lanes would add balancing and publish complexity before
  the lifecycle model is proven
- the main goal of this RFC is correctness and simpler maintenance semantics,
  not parallelizing leaf generation writes

So the preferred first-rollout shape is:

- one dedicated leaf writer
- one writable generation
- many sealed generations
- no leaf-lane fanout policy initially

### 7.1.2 First Code Rollout: One Segment Per Generation

To keep the first implementation slice small and mechanically obvious, the first
code rollout may map one writable leaf generation to one active leaf-log
segment.

That means:

- when the leaf writer rotates to a new leaf-log segment, the previous writable
  generation seals
- the new segment becomes the first and only file in a new writable generation
- a generation may contain more than one file in the long-term design, but that
  is not required for the first rollout

This is an implementation simplification, not a change in the architectural
model. It preserves the core invariant that generation rollover is a lifecycle
transition with no page-copy movement.

### 7.2 When The Manifest Changes

The manifest should not churn on every commit.

Normal commits that append new leaf pages to the current writable generation do
not need a manifest update as long as:

- the writable generation identity is unchanged
- file membership is unchanged
- no generation lifecycle transition occurred

Manifest writes should happen only on generation lifecycle events:

- create/open writable generation
- seal writable generation
- publish a new packed generation set
- mark generations retiring
- delete retired generations

This keeps steady-state write cost bounded and avoids turning the manifest into
a second commit log.

### 7.3 Publish And Recovery Rule

The required publish rule is:

1. leaf pages referenced by a commit must be flushed or synced before the root
   that references them becomes visible
2. normal root publication remains the commit-atomic visibility point
3. manifest updates are required only for lifecycle transitions, not for every
   commit that appended to the current writable generation

This aligns with the existing backend commit rule that value-log-backed leaf
pages are flushed before the index commit is published.

Recovery rule:

- treat the last durable index/meta state as authoritative for visible roots
- treat the manifest as authoritative for generation lifecycle and file
  membership
- if a writable generation contains additional durable leaf pages beyond the last
  committed root, those pages are harmless unreferenced tail and may be reused
  only according to the append format rules, not by in-place mutation

### 7.4 Manifest Durability Principle

Manifest durability should be strict only where lifecycle correctness depends on
it.

That means:

- sealing a generation must be durably recorded before later GC may rely on that
  seal
- deleting a generation must be reflected durably before the system forgets it
- pack publish must durably record the new generation set before old packed
  generations are eligible for deletion

### 7.5 Preferred Manifest Shape For First Rollout

The first rollout should use a single manifest file, not per-generation metadata
files as the primary source of truth.

Reason:

- lifecycle transitions should be infrequent
- the number of simultaneously retained generations should be intentionally
  bounded
- a single file is easier to reason about during crash recovery

If later evidence shows manifest size or update cost becoming material, the
design can split metadata by generation. That is not needed up front.

### 7.6 Preferred Pack Publish Sequence

Pack publish should use a conservative two-step manifest rule so recovery never
sees a root that depends on a generation missing from durable metadata.

Preferred sequence:

1. build and sync the new packed generation files
2. write a durable prepared manifest that includes the new generation and keeps
   the old generations retained
3. publish the new root commit that references the packed generation
4. write a durable follow-up manifest that marks the replaced old generations as
   `retiring`
5. delete retiring generations only after pins drain and deletion is durably
   recorded

This intentionally allows the manifest to get ahead of the root, but not the
other way around.

That is the conservative recovery rule:

- extra manifest-listed generations are harmless
- a root that references an unknown generation is not acceptable

### 7.7 Transitional Rollout Rule

The rollout into the existing pre-alpha codebase may temporarily tolerate old
`leaf_vlog` directories that do not yet have `manifest.json`.

For the first implementation slice:

- read-write open with `IndexOuterLeavesInValueLog=true` must create and persist
  the manifest if it is missing
- read-only open may synthesize the default single writable generation in memory
  when the manifest is missing
- once generation lifecycle state becomes actively mutated by maintenance, this
  compatibility escape hatch should be removed and the manifest should become
  hard-required for both read-write and read-only open

This rollout rule is narrow and temporary. It exists only to let saved homes and
older local fixtures remain inspectable while the generation-store control plane
lands incrementally.

## 8. Write Path

### 8.1 Steady-State Writes

Steady-state writes behave as follows:

1. Dirty tree work creates new leaf pages as needed.
2. New leaf pages are appended to the current writable leaf generation.
3. Internal pages in `index.db` reference those new `LeafRef`s.
4. Commit publication makes those references visible atomically.

Old leaf pages remain valid as long as any committed root or pinned snapshot
still references them.

Important clarification:

- a commit that does not touch a key does not rewrite that key just because a
  newer writable generation exists
- generation rollover is a lifecycle event, not a data-copy event
- the write path rewrites pages only when normal B-tree/page maintenance would
  have rewritten those pages anyway

### 8.2 Generation Sealing

The writable generation is sealed when a policy threshold is reached.

Preferred default policy:

- primary trigger: bytes written or page count
- optional secondary trigger: max age, but only once the generation has reached
  a meaningful minimum size
- checkpoint boundary should not force a seal by default

Recommended initial rule shape:

- seal when `writable_bytes >= seal_target_bytes`
- optionally also seal when `writable_pages >= seal_target_pages`
- disable automatic age-based sealing by default in the first rollout
- never force an age-based seal on a trivially small generation unless the
  operator requested it explicitly

Preferred first-rollout defaults:

- `seal_target_bytes = 256 MiB`
- `seal_target_pages = 65536` pages (4 KiB pages)
- automatic age-based sealing disabled by default

These values intentionally align with current TreeDB segment-sizing tendencies
for hot/warm value-log work while avoiding tiny leaf generations.

The reason for this default is to avoid pathological tiny generations.

Only one generation is writable at a time.

After sealing:

- no new leaf pages are appended to that generation
- a fresh writable generation is opened
- existing pages in sealed generations are not rewritten or copied merely
  because sealing occurred

### 8.3 Why Sealing Matters

Sealing gives the system a simple lifecycle:

- writable generation accumulates new leaves
- old generations become immutable and easy to pin
- retention and GC become generation-level decisions instead of rewrite-ledger
  decisions

## 9. Read Path

Reads resolve `LeafRef` pointers by reading the referenced file and offset from
the leaf-generation store.

Requirements:

- reads against sealed generations are always valid while that generation is
  pinned or reachable
- reads against the current writable generation are valid after commit
  publication
- read code should not need rewrite-ledger knowledge

The read path must be able to locate files by file ID even across multiple live
generations.

## 10. Snapshot And Pinning Model

Snapshots must pin the leaf generations needed to safely serve their referenced
`LeafRef`s.

That implies:

- snapshots pin the active leaf-store manifest view
- generation deletion is deferred until all pins are released

This is the same core idea as index-generation pinning today, but applied to
leaf generations.

Important consequence:

- successful maintenance and actual disk reclamation may be separated in time if
  readers are long-lived

This is correct behavior and must be observable.

### 10.1 First-Rollout Pin Implementation

The first implementation should pin leaf generations in the same phase where
`Snapshot` already pins the published `ValueLogSet` and index generation.

Preferred first-rollout shape:

- add an immutable in-memory leaf-generation view to published DB state
- store manifest file IDs in the raw leaf-log segment namespace used by `LeafRef`
  payloads, not the high-bit-marked `ValuePtr.FileID` namespace
- on `AcquireSnapshot`, increment pin counts for the generation IDs present in
  that published view
- on `Snapshot.Close`, decrement those pin counts
- omit `retiring` and `deleted` generations from newly published views so a
  generation already scheduled for removal cannot repin itself
- treat the pin set as generation-granular, not page-granular

The implementation should not try to infer pins from filesystem activity or from
 generic value-log current-set metadata. Pinning must be explicit and tied to
 snapshot lifetime.

For the first rollout, pin accounting may be process-local and rebuilt on open.
That is sufficient because pinned snapshots are process-local runtime objects,
not durable state.

## 11. GC Model

### 11.1 Regular GC

Regular `leaf_vlog` GC should be cheap and generation-oriented.

A sealed generation is eligible for deletion only when:

- the current live roots no longer reference any leaf pages from it
- no pinned snapshot requires it

If both are true, delete the whole generation.

This is intentionally coarse:

- a generation with one remaining live page is still retained as a whole
- generation GC does not rewrite or extract that last page
- reclaiming sparse tails is the job of `leaf-pack`, not ordinary GC

### 11.2 Reachability Source Of Truth

The source of truth for generation retention is:

- current published roots
- pinned snapshots

Not:

- stale-ratio heuristics
- generic value-log rewrite planning

### 11.3 Operational Simplicity

This GC model is intentionally coarse.

If a generation still contains even a small amount of live data, it remains
retained until a pack operation occurs.

That is acceptable because pack is a separate maintenance operation, not the
default reclaim path.

### 11.4 First-Rollout Whole-Generation GC Implementation

The first whole-generation GC implementation should be explicit and boring.

Preferred first-rollout algorithm:

1. acquire one stable snapshot of the current published root
2. walk only outer-leaf references and collect the live leaf-log file IDs they
   reference
3. map those file IDs to generation IDs through the manifest
4. classify each sealed generation as:
   - `live`: at least one referenced file is still reachable from the current
     root
   - `retiring`: not live from the current root, but still snapshot-pinned
   - `delete-eligible`: not live and pin count is zero
5. durably update the manifest before deletion, then mark deleted files zombie in
   the read manager and republish state so new snapshots cannot acquire them
6. prune `deleted` manifest records only after their files are actually gone so
   recovery never needs to infer whether a half-finished delete completed

The root walk should use the existing outer-leaf traversal machinery already in
 the backend, not the generic value-log rewrite planner. The point of this RFC
 is to stop treating `leaf_vlog` cleanup as a rewrite-discovery problem.

For the first rollout, generation GC should run only on sealed generations and
 should never touch the writable generation.

## 12. Leaf-Pack Compaction

### 12.1 Purpose

Leaf-pack is the rare operation that reclaims space from sparse sealed
generations that are not fully dead.

Leaf-pack is not the steady-state maintenance model. It is the exceptional,
heavy maintenance tool.

### 12.2 Shape

Leaf-pack should:

1. take a stable snapshot
2. identify live leaf pages reachable from that snapshot
3. copy those live leaf pages into a fresh compact generation
4. rebuild the internal tree above the new `LeafRef`s
5. publish the new roots and manifest atomically
6. retire old generations after readers drain

Leaf-pack is page-granular, not key-granular.

That means:

- it copies live leaf pages, not selected keys
- an unchanged key may move during pack if it resides on a page that is being
  copied
- if a page is still live, the entire page is treated as live payload for pack

This is operationally similar to vacuum, but it is a generation-publish
operation, not generic value-log rewrite.

### 12.3 Triggering

Leaf-pack should be rare and explicit.

Examples of valid triggers:

- too many sealed generations retained
- retained leaf bytes above threshold
- live density of sealed generations falls below threshold
- operator-invoked maintenance

Leaf-pack should not be a tiny high-frequency background nibble.
Leaf-pack should also not run merely because generations rolled. It should run
only when there is a clear retained-space problem that ordinary whole-generation
GC cannot solve.

Initial policy constraints:

- start operator-invoked or tightly gated
- require substantial expected reclaim before running
- require cooldown / hysteresis so pack does not oscillate
- prefer generation-count and retained-bytes thresholds over scan-driven
  "estimated stale debt" heuristics

Preferred first-rollout default:

- pack is manual or explicitly operator-invoked only
- there is no always-on automatic pack loop in the first rollout

Initial non-eligibility rules:

- do not run pack when only one or very few sealed generations exist
- do not run pack when expected reclaim is small
- do not run pack simply because a new writable generation was opened
- do not run pack on every checkpoint

Recommended first-rollout admission shape:

- candidate set excludes the current writable generation
- candidate set excludes freshly sealed generations younger than a cooldown age
- candidate set should normally contain at least 3 sealed generations, unless an
  operator forces the run
- candidate retained bytes should exceed a configured floor before analysis is
  attempted
- one explicit snapshot-based candidate analysis is acceptable at pack admission
  time because pack is rare and heavy; what this RFC rejects is repeated
  background scan-driven rediscovery in steady state
- run pack only if `expected_reclaim_bytes` exceeds a configured floor and a
  configured fraction of candidate bytes

Correctness note:

- if pack never runs, the system should still remain correct
- the failure mode without pack is retained space, not logical corruption

## 13. Why This Replaces Current Leaf Rewrite

Current leaf rewrite is the wrong abstraction because it makes leaf storage look
like partially stale record storage.

The existing leaf path in `ValueLogRewriteOnline`:

- selects source segments
- recursively walks the live tree
- copies matching leaf pages
- remaps changed internal ancestors
- performs post-rewrite reachability cleanup

That design should be deleted once immutable leaf generations exist.

Specifically:

- `rewriteLeafRefsOnline`
- `leafRefRewriteCtx`
- leaf-specific rewrite counters in generic value-log maintenance
- leaf rewrite planning as a subphase of generic `ValueLogRewriteOnline`

should all become transitional code, then be removed.

## 14. Relationship To `value_vlog`

This RFC makes `value_vlog` easier to reason about.

Once leaf pages are out of scope, `value_vlog` becomes a clean value-record
subsystem again.

Preferred `value_vlog` model after this split:

- append-only record log
- per-file live/stale accounting
- cheap GC for fully dead files
- targeted rewrite for partially stale files
- offline full rewrite for deep cleanup

This RFC does not require the final `value_vlog` redesign to land in the same
change, but it does require that leaf maintenance stop being embedded in the
value path.

## 15. Relationship To Index Vacuum

The current `index.db` vacuum path is useful as an orchestration reference, but
it should not remain the primary answer for outer-leaf maintenance.

After this RFC lands:

- leaf generation sealing and GC handle most outer-leaf lifecycle needs
- leaf-pack handles rare deep compaction of old sparse generations
- standalone index vacuum should be reassessed and possibly narrowed

In particular, if leaf-pack rebuilds the user internal tree as part of publish,
then ordinary index vacuum may only remain necessary for:

- pager/freelist cleanup
- system-tree-specific compaction
- exceptional fragmentation repair

Preferred first-rollout answer:

- do not immediately subsume ordinary index vacuum into leaf-pack
- keep index vacuum as a separate tool/policy until leaf generations and pack
  are validated on real datasets

This is a rollout-discipline choice. It avoids coupling two large maintenance
changes into one correctness boundary.

## 16. Large-Dataset Requirements

This RFC is intended to improve large-dataset behavior, but only if the
implementation respects a few constraints.

### 16.1 No Unbounded In-Memory Delta Growth

An online leaf-pack or generation-publish flow must not rely on an unbounded
in-memory map of replayed writes.

Acceptable approaches:

- strict lag ceiling with deferral
- spillable delta log
- bounded catch-up with retry policy

### 16.2 Streaming Build

The common build path should be streaming.

It must not require:

- whole-tree remap maps for all leaf pages
- whole-dataset in-memory state beyond bounded builders and manifests

### 16.3 Free-Space Admission

Before starting a heavy leaf-pack operation, TreeDB should estimate temporary
disk demand and refuse the operation when headroom is insufficient.

### 16.4 Reader-Pin Observability

Operators must be able to see:

- which generations are still pinned
- whether retention is due to live roots or long-lived readers

Without that, the system will look broken even when it is correct.

## 17. Crash And Recovery Invariants

The leaf-generation store needs crash rules as explicit as `index.db`.

Required invariants:

1. New leaf pages are durable before a root is published that references them.
2. A manifest update is never considered committed unless its referenced files
   are durable.
3. Recovery can identify the last fully published manifest/state pair.
4. A crashed pack/build never causes silent leaf loss or dangling `LeafRef`s.

Initial implementation guidance:

- keep crash handling simple and explicit
- prefer generation swap artifacts and manifest sequence checks over implicit
  recovery heuristics

## 18. Metrics And Observability

Primary operator-facing metrics should become physical and generation-oriented.

Needed metrics:

- current writable generation bytes/pages
- sealed generation count
- retained generation count
- retired generation count
- bytes by generation state
- generations pinned by snapshots

First-rollout implementation note:

- export initial backend metrics through existing `Stats()` output under the
  `treedb.leaf_generation.*` prefix
- include current generation ID, generation counts by state, file counts by
  state, bytes by state, and snapshot pin counts

Longer-term metrics still needed:

- leaf-pack runs
- leaf-pack bytes in/out
- leaf-pack wall time
- leaf-pack max RSS
- leaf-pack temporary disk high-water
- generation GC deletions and bytes reclaimed

Metrics that should stop being primary for leaf maintenance:

- stale-ratio-driven rewrite counters
- generic rewrite queue statistics for leaf work

## 19. Authoritative Milestone Plan

This section supersedes the earlier lighter-weight rollout framing.

As of 2026-04-14, the saved-home estimator results are strong enough to make
this implementation plan concrete.

The key validation result is:

- large Celestia homes have substantial dead `leaf_vlog` bytes
- almost none of those dead bytes are recoverable by whole-generation deletion
  alone at current or moderately smaller generation sizes
- therefore whole-generation GC is necessary but not sufficient
- explicit `leaf-pack` is not optional for good reclaim on real workloads

This means the milestone is now defined as:

- make `leaf_vlog` generation-based
- make ordinary maintenance whole-generation GC only
- make sparse reclaim an explicit `leaf-pack` operation
- make `ValueLogRewriteOnline` value-only
- validate the design on saved Celestia homes before claiming the milestone is
  complete

## 20. Milestone Structure

The full milestone should be executed as 6 phases grouped into 4
implementation sprints.

### Phase Summary

- Phase 0: baseline and saved-home estimator validation
- Phase 1: generation metadata and write ownership
- Phase 2: whole-generation GC and snapshot pinning
- Phase 3: offline `leaf-pack`
- Phase 4: online bounded `leaf-pack`
- Phase 5: split control planes and delete mixed leaf/value maintenance logic
- Phase 6: hardening, docs, metrics, and final Celestia validation

### Sprint Mapping

- Sprint 1: phases 1 and 2
- Sprint 2: phase 3
- Sprint 3: phase 4
- Sprint 4: phases 5 and 6

Phase 0 is already underway via the saved-home estimator and should remain the
baseline reference for later validation.

### Git Strategy

The milestone should not be implemented on one long-lived branch with mixed
work. Use a narrow stacked-PR discipline.

Preferred rules:

- every implementation slice gets its own branch
- every branch has one behavioral theme
- each branch starts either from `main` or from the immediately preceding green
  branch in the stack
- do not implement directly on `main`
- keep RFC/docs commits separate from code unless the docs are tightly coupled
  to that code slice

Preferred branch naming:

- `leafgen/s1-<slice>`
- `leafgen/s2-<slice>`
- `leafgen/s3-<slice>`
- `leafgen/s4-<slice>`

Example:

- `leafgen/s1-manifest-skeleton`
- `leafgen/s1-writable-generation-routing`
- `leafgen/s2-offline-pack-planner`
- `leafgen/s4-value-only-rewrite`

### PR And Review Strategy

The milestone should move as a sequence of active PRs, not as hidden local work.

Required rules:

- open each coherent slice as a real PR as soon as local tests are green
- set each PR to active or ready-for-review, not draft, once it is ready for CI
  and review
- explicitly request AI review from Copilot and Codex when those review agents
  are available in the repository
- resolve or explicitly answer every actionable AI review comment before moving
  on
- do not stack new behavioral work on top of a PR whose CI is still red or
  whose review comments are still unresolved

The reason for active PRs is operational, not cosmetic:

- active PRs trigger CI and AI review flows
- draft PRs are acceptable only for very short-lived scaffolding, not for
  milestone slices that are awaiting review

### CI Gate

Every slice must pass both local and remote gates before it is treated as
complete.

Required rules:

- run targeted local tests before every push
- run broader local tests before marking a PR ready for review
- wait for remote CI to finish and confirm green status before declaring the
  slice done
- if CI fails, fix the issue on the same branch before starting the next slice
- if CI flakes, inspect first and rerun only with a stated reason

The milestone is not complete if the branch stack only "usually passes".

### Git Hygiene

Required hygiene rules:

- begin each slice from a clean branch tip
- keep commits scoped to one behavioral theme each
- do not mix benchmark outputs, saved-home copies, temp files, or unrelated
  experiments into implementation commits
- if an experiment is useful but not ready, keep it on a scratch branch instead
  of polluting the implementation branch
- prefer rebasing and stacked linear history over merge-commit clutter while
  the stack is in flight
- before requesting review, inspect `git diff` and `git status` and remove
  unrelated edits

### Test-Driven Development Protocol

This milestone should be implemented test-first, not benchmark-first.

Required protocol for every slice:

1. write or extend the narrowest failing unit or integration test that captures
   the intended behavior
2. implement the minimum code needed to make that test pass
3. run the targeted test file/package locally
4. run the broader package or subsystem tests for the touched surface
5. only after local tests pass, push and use saved-home or Celestia validation
   as a higher-level confirmation

Rules:

- no slice should rely on Celestia runs as the first signal of correctness
- large-home and Celestia validation confirm the design; they do not replace
  unit and integration tests
- every sprint exit criterion must be represented by explicit tests where
  practical
- every bug found in saved-home or Celestia validation should first be reduced
  to a smaller deterministic test before the fix is considered complete

## 21. Sprint 1 Punch List

Sprint 1 goal:

- make `leaf_vlog` an explicit generation store with one writable generation,
  sealed generations, and whole-generation GC
- do not introduce page-copy pack behavior yet

### Sprint 1 Deliverables

- manifest/state model for leaf generations
- explicit writable generation ownership for new outer-leaf appends
- generation rollover without page movement
- whole-generation GC for fully unreachable, unpinned generations
- snapshot pin accounting for generation retention
- initial leaf-generation metrics

### Sprint 1 Recommended PR Stack

- `leafgen/s1-manifest-skeleton`
- `leafgen/s1-writable-generation-routing`
- `leafgen/s1-rollover-and-sealing`
- `leafgen/s1-snapshot-pins-and-whole-generation-gc`

### Sprint 1 Current Local Status

- manifest skeleton: landed locally
- writable generation routing: landed locally
- rollover and sealing: landed locally
- snapshot pins and backend whole-generation GC: landed locally
- initial backend `treedb.leaf_generation.*` stats: landed locally
- public strict `LeafGenerationGC` wrapper: current slice

### Sprint 1 Code Work

- add manifest/state files under [TreeDB/db](../TreeDB/db)
  for generation metadata, load/save, and validation
- wire DB open/load/save in [TreeDB/db/db.go](../TreeDB/db/db.go)
- extend [TreeDB/db/leaf_page_log.go](../TreeDB/db/leaf_page_log.go)
  and [TreeDB/caching/leaf_page_log.go](../TreeDB/caching/leaf_page_log.go)
  so outer-leaf appends belong to one explicit writable generation
- introduce generation rollover thresholds and sealing logic
- add generation liveness tracking from live roots and pinned snapshots
- add whole-generation deletion logic for fully dead sealed generations
- land backend generation-state and GC semantics first in [TreeDB/db](../TreeDB/db)
- wire public and cached-mode generation stats only after the backend semantics
  and file-ID namespace are settled

### Sprint 1 Unit Tests

- manifest create/load/validate
- manifest rejects unknown version
- normal commits do not churn manifest state
- sealing does not move untouched keys/pages
- reopen preserves readable leaf refs across rollover
- pinned snapshots prevent generation delete
- fully dead generation deletes
- generation with one live page remains retained

### Sprint 1 Integration / Large-Home Validation

- small rollover/reopen parity runs
- saved-home estimator still runs and reports generation geometry correctly
- large saved-home analysis confirms explainable retained generation counts

### Sprint 1 Exit Criteria

- `leaf_vlog` files belong to explicit generations
- ordinary writes append only to the current writable generation
- sealing is a lifecycle event, not a page-copy event
- whole-generation GC works for fully dead generations
- sparse generations remain retained as a whole

## 22. Sprint 2 Punch List

Sprint 2 goal:

- implement offline `leaf-pack` so sparse retained generations can actually be
  compacted on real Celestia homes

### Sprint 2 Deliverables

- offline `leaf-pack` planner
- offline `leaf-pack` executor
- tree/root publish path for packed generations
- pack metrics and operator tooling
- saved-home reclaim proof

### Sprint 2 Recommended PR Stack

- `leafgen/s2-offline-pack-planner`
- `leafgen/s2-offline-pack-executor`
- `leafgen/s2-pack-publish-and-metrics`

### Sprint 2 Current Local Status

- landed backend `LeafGenerationPlan` with exact live-byte accounting by generation
- landed backend `LeafGenerationPack` for explicit sealed-generation leaf compaction
- public `DB.LeafGenerationPlan` and `DB.LeafGenerationPack` now checkpoint cached state before backend execution
- `treemap` now exposes `leafgen-plan` and `leafgen-pack` operator entrypoints
- planner and GC now share the same `LeafRef` reachability scan shape
- rewrite-created leaf files are now registered into the leaf-generation manifest before publish
- legacy split-leaf homes that predate `leaf_vlog/manifest.json` now bootstrap generation state from existing `leaf_vlog` segment files instead of fabricating an empty synthetic manifest
- saved-home validation on saved-home fixture `splitouterleaf-20260414090917` now reconstructs 9 generations correctly and proves the compatibility path works on a real pre-manifest Celestia home
- unit coverage now includes ranking, dead-vs-live geometry, age-gate force override, snapshot-pin non-interference, explicit pack, reopen, post-pack GC, operator-surface parsing/checkpoint tests, and pre-manifest saved-home manifest bootstrap

### Sprint 2 Code Work

- add reusable planner helpers that consume the leaf-generation state plus live
  `LeafRef` reachability and rank sparse generations by reclaim ratio
- add offline `leaf-pack` implementation under [TreeDB/db](../TreeDB/db)
- add CLI/tooling entrypoints under [TreeDB/cmd/treemap](../TreeDB/cmd/treemap)
- reuse or extract the leaf-ref remap/publish mechanics currently living in
  [TreeDB/db/vlog_rewrite.go](../TreeDB/db/vlog_rewrite.go)
- reuse rebuild/swap ideas from [TreeDB/db/vacuum_online.go](../TreeDB/db/vacuum_online.go)
  and [TreeDB/db/vacuum_offline.go](../TreeDB/db/vacuum_offline.go)
- expose pack metrics: bytes copied, bytes reclaimed, wall time, max RSS,
  temporary disk high-water

### Sprint 2 Unit Tests

- planner ranks sparse generations by dead-bytes-per-live-byte-to-copy
- page-granular pack can move a live leaf page without corrupting neighboring
  unchanged keys
- packed root no longer references source generations
- reopen preserves packed data
- old generations become retiring and later deletable

### Sprint 2 Integration / Large-Home Validation

- run offline `leaf-pack` on saved Celestia homes
- compare bytes copied versus bytes reclaimed
- confirm reclaim on homes where whole-generation delete alone produced zero or
  near-zero reclaim
- repeatable saved-home validation harness now lives at [scripts/leafgen_saved_home_validate.sh](../scripts/leafgen_saved_home_validate.sh)
- the harness now records both the default planner view and the forced experimental view, plus explicit split-layout sizes for `maindb`, `index.db`, `leaf_vlog`, `value_vlog`, and `wal`, so dense homes can be rejected by normal operator policy without losing the ability to study them under explicit force
- `leafgen-pack` now enforces the same default reclaim-per-copy safety on explicit generation sets, and `-force` is the deliberate override for research or one-off experiments
- `LeafGenerationPackStats` now returns the selected-generation economics directly: source generation ids, source live/dead/copy bytes, expected reclaim metrics, and wall time, so operator tooling no longer has to reconstruct that from a separate plan after the fact
- latest saved-home result on saved-home fixture `splitouterleaf-20260414090917`:
  planner reconstructs 9 generations, default admission rejects pack as `reclaim_per_copy_too_low`, and the forced view still exposes the same 8 sealed generations with only `4,585,857` reclaimable bytes across about `2.1 GiB` of `leaf_vlog`
- the same plan reports about `2,142,908,926` candidate bytes to copy and only `2,140` reclaimable ppm per byte copied, which is the right high-level signal for why this home is a bad pack candidate
- the updated harness measured `2,425,593,709` bytes total / `46,923,776` bytes `index.db` / `2,237,194,549` bytes `leaf_vlog` / `141,249,172` bytes `value_vlog` before pack, `4,602,701,220` / `77,332,480` / `4,383,893,448` / `141,249,172` after pack, and `2,455,204,749` / `77,332,480` / `2,236,396,977` / `141,249,172` after GC pass 1; the second GC pass is now an idempotent no-op on the same home
- same saved-home `leafgen-pack` validation copied about `3,878,699,008` live bytes into one new generation; after GC pass 1 the copied home returned to essentially its starting size, `value_vlog` stayed flat across the whole run, and a second immediate GC pass stayed flat, confirming correctness but also confirming that whole-generation delete plus naive pack will not materially shrink this Celestia snapshot because the sealed generations are almost entirely live
- operationally, the saved-home flow no longer needs a second pruning pass: once no external snapshot is pinning the old generations, a single GC call now marks, deletes, and prunes them; an immediate second call reports no further work
- target-geometry sweeps now justify a real default instead of leaving `leaf_vlog` on the generic hot target: the small sweep in `leafgen_target_sweep_20260415_smalltargets` shows the first reclaim cliff below `8 MiB`, while the larger sweep in `leafgen_target_sweep_20260415_large` shows `64 MiB` already collapsing to one generation (`0` reclaim), `16 MiB` yielding `3` generations with about `20.8 MiB` reclaimable for about `12.8 MiB` copied, and `32 MiB` yielding essentially the same reclaim/copy economics with only `2` generations
- offline validation on the large synthetic rebuilds in `leafgen_validate_16m_20260415` and `leafgen_validate_32m_20260415` then confirms both targets shrink from about `47.1 MB` to about `26.8 MB` after `leafgen-pack` plus one GC pass, but `32 MiB` does it with one source generation instead of two; that is the current rationale for making `32 MiB` the runtime default `leaf_vlog` target while keeping the explicit override knob for further Celestia-scale tuning
- a fresh `run_celestia` against this branch with `LOCAL_GOMAP_DIR=<repo-root>`, `TREEDB_OPEN_PROFILE=fast`, and the new default leaf target produced Celestia home `20260415093638`; that run froze remote height at `10,678,682`, finished in `344` seconds, peaked at `9,805,084` KiB RSS, and wrote a `leaf_vlog` made of `126` sealed `~33 MiB` generation files plus one current writable generation instead of the older `~268 MiB` dense-file geometry
- offline validation on that exact fresh home now lives in `leafgen_validate_celestia_32m_20260415`: the default `leafgen-plan` is now `eligible` without `-force`, with `1,946,127,025` reclaimable bytes against `2,281,854,413` bytes to copy (`852,870` reclaim-per-byte-copied ppm) across `126` candidate sealed generations
- the same validation then packs and GCs successfully: `application.db` shrinks from `4,531,609,589` bytes to `2,614,622,989` bytes after one GC pass, `leaf_vlog` shrinks from `4,280,689,658` bytes to `2,351,382,242` bytes, and the second GC pass is again an idempotent no-op; this is the first real Celestia-shaped proof in the branch that Sprint 2's offline `leaf-pack` path is not just correct but materially useful once generation geometry is sane
- a follow-on real run with `TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES=16777216` on Celestia home `20260415094512` did not beat the new default: sync slowed to `428` seconds, the run produced `254` candidate sealed generations instead of `126`, and offline validation in `leafgen_validate_celestia_16m_20260415` measured `1,957,798,037` reclaimable bytes against `2,303,889,235` bytes to copy (`849,779` reclaim-per-byte-copied ppm); the final packed+GC size still landed around `2,620,857,595` bytes, so `16 MiB` adds churn without improving the real Celestia economics over `32 MiB`

### Sprint 2 Exit Criteria

- offline `leaf-pack` materially shrinks sparse Celestia homes
- copied live bytes are much smaller than reclaimed bytes
- correctness is preserved across reopen and verification

## 23. Sprint 3 Punch List

Sprint 3 goal:

- add bounded online `leaf-pack` so steady-state maintenance can make progress
  without pathological churn

### Sprint 3 Deliverables

- bounded online `leaf-pack` runner
- strict admission policy and budget controls
- manual trigger first, scheduler second
- restore/catch-up suppression
- online pack metrics and observability

### Sprint 3 Recommended PR Stack

- `leafgen/s3-online-pack-admission`
- `leafgen/s3-online-pack-runner`
- `leafgen/s3-pack-observability-and-dwell-validation`

### Sprint 3 Current Local Status

- manual `leafgen-pack -from-plan` now supports bounded prefix selection with `-max-generations` and `-max-bytes-to-copy`, so the first online-style admission surface is available to operators before any background scheduler exists
- the bounded prefix selector now lives in reusable TreeDB code (`SelectLeafGenerationPackCandidates`) instead of only in the CLI, so future backend online-pack runners can share the exact same ranking and budget semantics
- manual `leafgen-pack -from-plan` now routes through `LeafGenerationPackFromPlan`, so the plan/select/pack composition exists as a DB API surface instead of a CLI-only orchestration path
- `LeafGenerationPackRunOnce` now exists as a bounded skip-or-run backend surface: it returns the computed plan, selected prefix, pack stats when it runs, and an explicit skip reason when it declines work
- the cached maintenance layer now has an env-gated, periodic-only `LeafGenerationPackRunOnce` hook that inherits the existing quiet-window, maintenance-phase suppression, and backend-maintenance barrier instead of introducing a parallel scheduler path
- the first cached maintenance hook is intentionally conservative: it only runs for `IndexOuterLeavesInValueLog`, only on non-GC periodic passes, only when the quiet window is satisfied, and it defaults to a one-generation / 64 MiB copy cap plus a conservative min-interval guard until dwell validation proves the policy
- repeatable cached dwell validation now lives at [scripts/leafgen_cached_dwell_validate.sh](../scripts/leafgen_cached_dwell_validate.sh); it copies a saved home, opens it through `treedb.Open` with a real profile, emits periodic JSONL stats, records RSS/HWM and split-layout sizes, captures pre/post `leafgen-plan`, and now promotes `sample_count`, `max_rss_kb`, `max_hwm_kb`, and the final sample into `summary.json`
- latest cached dwell result on saved-home fixture `splitouterleaf-20260414090917` with `TREEDB_PROFILE=fast` over a 60-second dwell: `sample_count=5`, `max_rss_kb=90,280`, `max_hwm_kb=90,292`, final `scheduler_state=idle`, `leaf_pack_attempts=2`, `leaf_pack_runs=0`, and `leaf_pack_skips=2`; the forced plan still remained roughly `4,585,857` reclaimable bytes against `2,142,908,926` bytes to copy, so the hook is bounded and explainable on this home but not yet useful
- the same `fast` dwell stayed bounded in disk behavior: the final sample kept `leaf_vlog` at `2,237,192,421` bytes and `value_vlog` at `141,245,076` bytes, while the copied home’s end-of-run size only moved by a few KiB of metadata noise rather than by pack churn
- latest `TREEDB_PROFILE=wal_on_fast` dwell over the same 60-second saved-home run now reports `sample_count=5`, `max_rss_kb=90,984`, `max_hwm_kb=91,004`, final `scheduler_state=idle`, `leaf_pack_attempts=2`, `leaf_pack_runs=0`, and `leaf_pack_skips=2`; that closes the liveness gap from the earlier hard-disabled scheduler without the previous planner-memory blow-up, and the branch now holds `fast` and `wal_on_fast` to essentially the same steady-state leaf-pack cost on this dense home
- the dwell harness now respects `TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE`, and the first `wal_on_fast` control run with that flag forced to `0` reports `max_rss_kb=76,200`, final `scheduler_state=idle`, and zero leaf-pack attempts/runs/skips; after adding the published-state live-scan cache and sealed-generation offset/length indexing, the remaining steady-state delta on this home is now only about `14 MiB` in both `fast` and `wal_on_fast`, which means the earlier planner-memory blow-up was largely removed and the residual cost of a skipped leaf-pack attempt is now much smaller
- repeatable planner profiling now lives at [TreeDB/db/leaf_generation_plan_bench_test.go](../TreeDB/db/leaf_generation_plan_bench_test.go) and [scripts/leafgen_plan_profile.sh](../scripts/leafgen_plan_profile.sh); the cold saved-home benchmark now explicitly clears the planner cache before each run and, after adding a sealed-generation offset/length index, measures about `352 ms/op`, `16,198,368 B/op`, and `31,326 allocs/op`, down from the earlier `582 ms/op`, `24,062,888 B/op`, and `978,230 allocs/op`
- the same benchmark file still carries a cached steady-state benchmark: after one cold plan populates the published-state cache, repeated `LeafGenerationPlan` calls on the same saved Celestia home stay around `5,633 ns/op`, `5,160 B/op`, and `33 allocs/op`; for leaf-generation reachability, that cache is now keyed by the published tree roots `(rootID, systemRoot)` rather than generic commit churn, so same-root publishes reuse the cached scan and the result remains shared by both `LeafGenerationPlan` and whole-generation `LeafGenerationGC` reachability collection
- the saved-home duplicate scan benchmark still reports `978,081` total leaf refs, `978,081` unique leaf refs, and `0` duplicates, which justified removing the planner's per-`LeafLogPtr` seen map; the new sealed-generation index then removed most of the remaining per-leaf header churn, cutting cold-path alloc objects from about `978k` to about `31k` and reducing the dwell RSS peak further in both `fast` and `wal_on_fast`
- persisted sealed-generation length metadata is now landed as advisory `.lenidx` sidecars beside sealed `leaf_vlog` files: cached `AppendLeafPage` forwards each leaf-page `ValuePtr` record-length hint into the backend, generation rollover persists the just-sealed file's offset/length index, pack-created leaf files persist the same sidecar on registration, and planner reload tests now prove that after clearing in-memory caches the sealed lookup path can reopen from disk with `0` value-log header reads and `0` sealed-file rescans
- repeatable planner profiling still lives at [TreeDB/db/leaf_generation_plan_bench_test.go](../TreeDB/db/leaf_generation_plan_bench_test.go) and [scripts/leafgen_plan_profile.sh](../scripts/leafgen_plan_profile.sh), but the harness now also carries a persisted-index reopen benchmark (`BenchmarkLeafGenerationPlanPersistedIndexReopen_SavedHome`) plus an isolated live-stats benchmark (`BenchmarkLeafGenerationLiveStatsPersistedIndex_SavedHome`) on a copied work fixture; the latest raw output is captured in `leafgen_live_stats_bench_20260414_phase4.txt`
- on saved-home fixture `splitouterleaf-20260414090917`, the persisted-index reopen benchmark now lands around `40.5` to `42.7 ms/op`, `9.93` to `10.47 MiB/op`, and `191` to `276 allocs/op`; the isolated live-stats scan lands around `40.0` to `42.9 ms/op`, `9.92` to `10.46 MiB/op`, and `150` to `238 allocs/op`
- the mechanism for that additional improvement is explicit in the sidecar loader: `loadLeafGenerationRecordLengthIndexFile` now decodes `.lenidx` files as a streaming chunked read into final offset/length slices instead of allocating a full raw-file buffer and then decoding from that temporary blob
- this leaves the planner in a materially different place than the earlier sealed-sidecar-only state: first the writable-file in-memory offset/length index removed the old `~31k allocs/op` header-read churn, then per-scan file-state preload plus slice accumulators cut cold saved-home wall time by roughly `40%`, the per-file lookup hint cut another roughly `35%`, and the streaming sidecar decoder roughly halved cold allocation pressure while preserving the same `~40 ms` wall-time class
- the saved-home page-stat benchmark still reports `9,560` total pager pages, `9,560` unique pager pages, `0` duplicate pager pages across user+system roots, and `978,081` leaf refs; that remains the reason not to carry a riskier acyclic shortcut for little additional gain
- latest isolated profile evidence from `cpu_top.txt`, `alloc_space_top.txt`, and `alloc_objects_top.txt` now shows the remaining cold-path cost concentrated in hinted index lookup/search, the shared-root pager walk, checksum verification, and cold `.lenidx` file decoding; planner bookkeeping itself is no longer the dominant issue
- `scanLeafGenerationLiveStats` now also honors the pager verified-page cache instead of forcing checksum verification on every planner walk; the new warmed benchmark (`BenchmarkLeafGenerationLiveStatsVerifiedPagesPersistedIndex_SavedHome`) is captured in `leafgen_live_stats_bench_20260414_phase5.txt` and lands around `37.6` to `38.9 ms/op` on the same saved home, versus roughly `40.3` to `43.3 ms/op` for the cold unverified-path benchmark in the same run
- a new steady-state benchmark (`BenchmarkLeafGenerationLiveStatsWarmIndexesVerifiedPages_SavedHome`) now isolates the post-invalidation recompute path with both verified pages and sealed-file length indexes already warm; the latest raw output lives in `leafgen_live_stats_bench_20260414_phase6.txt` and lands around `23.0` to `23.4 ms/op` with only `20 allocs/op`, which is the current real planner tax once reopen/cold-index churn is removed
- `lookupWithHint` now probes a tiny local window around the previous hit before falling back to binary search, which matches the saved-home scan locality much better than immediately paying a closure-heavy `sort.Search`; the latest raw output in `leafgen_live_stats_bench_20260414_phase7.txt` lands the verified-page persisted-index path around `27.7` to `28.3 ms/op` and the true warm steady-state recompute path around `13.0` to `13.5 ms/op`, still at `20 allocs/op`
- `leafrefscan` now walks internal-page children through a dedicated child-ID iterator instead of repeatedly calling `GetInternalChildID` per slot, so base-delta metadata and directory decoding are amortized once per page; the latest raw output in `leafgen_live_stats_bench_20260414_phase8.txt` lands the verified-page persisted-index path around `24.8` to `25.8 ms/op` and the true warm steady-state recompute path around `11.2` to `12.4 ms/op`, still at `20 allocs/op`
- `scanLeafGenerationLiveStats` now also uses a dense pager-page visited bitmap for the common in-range page IDs instead of a hash map in the planner/GC shared walk path, with a sparse overflow map only for out-of-range IDs; the latest raw output in `leafgen_live_stats_bench_20260414_phase9.txt` lands the verified-page persisted-index path around `24.9` to `26.0 ms/op` and the true warm steady-state recompute path around `11.0` to `11.2 ms/op`, again with `20 allocs/op`
- the live-scan visit path now also caches direct per-file state pointers instead of re-resolving file indexes and totals slices on every reachable `LeafLogPtr`; the latest raw output in `leafgen_live_stats_bench_20260414_phase11.txt` lands the verified-page persisted-index path around `24.0` to `24.6 ms/op` and the true warm steady-state recompute path around `9.7` to `10.4 ms/op`, while also trimming the steady-state path to `2752 B/op` and `19 allocs/op`
- internal-page walking for the planner/GC shared live-scan path now uses a dedicated `WalkInternalChildren` helper that decodes child IDs and routes `LeafRef` children directly without the extra generic per-child callback layer; the latest raw output in `leafgen_live_stats_bench_20260414_phase12.txt` lands the verified-page persisted-index path around `23.2` to `23.7 ms/op` and the true warm steady-state recompute path around `9.1` to `9.4 ms/op`, still at `2752 B/op` and `19 allocs/op`
- the same hot path now also splits `LeafRef` marker checks from pointer decode, so internal child walking and root handling can test `IsLeafRefID` first and only materialize the `LeafLogPtr` on the taken branch; the latest raw output in `leafgen_live_stats_bench_20260414_phase13.txt` lands the verified-page persisted-index path around `22.8` to `23.7 ms/op` and the true warm steady-state recompute path around `9.18` to `9.33 ms/op`, still at `2752 B/op` and `19 allocs/op`
- a dedicated steady-state CPU profile from `leafgen_phase13_hotsteady_uixB4S/cpu.pprof` now shows the remaining warm-path cost dominated by actual tree traversal and required I/O rather than planner bookkeeping: `scanLeafGenerationLiveStats` visit work, `page.IsLeafRefID`, `node.(*Node).WalkInternalChildren`, `pread`-backed sealed-index reads, and a small checksum tail; that is the current evidence that further large wins are likely structural (fewer repeated scans or less reopen/index churn), not another round of tiny branch-level cleanups
- that next structural step is now landed in this branch: `scanLeafGenerationLiveStats` memoizes pager-page subtree generation totals keyed by page ID, so root churn only rescans new path pages while unchanged subtrees reuse cached reachability totals; `TestLeafGenerationPlan_ReusesCachedSubtreesAcrossRootChanges` now proves the second plan after a small write misses fewer cached pages than the initial full scan, and the cache is hardened by invalidating entries for newly allocated index pages and clearing on index-generation swaps so freelist page-ID reuse cannot return stale subtree data; the latest raw output in `leafgen_live_stats_bench_20260415_phase14.txt` and the post-hardening confirmation run in `leafgen_live_stats_bench_20260415_phase14b.txt` land the verified-page persisted-index path around `13.5` to `14.3 ms/op` and the true warm repeated-scan path around `5.6` to `9.0 us/op`, with the steady-state allocs trimmed to `15 allocs/op`
- the benchmark harness now also measures the exact root-churn case this cache was added for: `BenchmarkLeafGenerationPlanAfterSingleKeyRootChange_Local` rewrites one hot key between iterations and then times `LeafGenerationPlan`; the latest raw output in `leafgen_root_churn_bench_20260415.txt` lands around `118` to `159 us/op`, with `1.000 subtree_misses/op`, about `267 KiB/op`, `25` to `26 allocs/op`, and a steady `12 subtree_pages` cache footprint, which is the strongest current evidence that small publishes now pay only path-local rescans instead of another full-tree walk
- a follow-on fix then closed the writable-file record-length gap that the first root-churn profile exposed: `LeafLogPtr` intentionally drops record-length hints, so the DB now wraps the installed leaf-page log and consumes the real compressed record length reported by the underlying writer on each append, keeping the writable generation's in-memory offset/length index hot across commits; the new regression `TestLeafGenerationPlan_SmallWriteKeepsWritableRecordLengthIndexWarm` proves the next plan after a small write no longer rescans the writable leaf file, and the updated raw output in `leafgen_root_churn_bench_20260415_postlenfix.txt` now lands around `17.9` to `20.7 us/op`, `3912` to `3940 B/op`, and `20 allocs/op`, still at `1.000 subtree_misses/op` and `12 subtree_pages`
- for `leaf_vlog`, the current planner is already measuring the right thing for pack economics: each reachable `LeafLogPtr` accounts for one whole compressed outer-leaf page record, so `BytesLive`/`BytesDead` at generation scope are exact record-level pack costs rather than a fuzzy estimator; on this home, every sealed generation is a single ~268 MiB file with only about `0.4` to `0.8` MiB dead, which means the main remaining problem is write-time generation geometry, not a missing finer-grained pack opportunity inside the existing files
- that geometry conclusion is now backed by a direct forced-plan run on a copied saved Celestia-style home in `leafgen_geometry_8bOiDN`: `leafgen-plan -force` reports `4,585,857` reclaimable bytes against `2,142,908,926` bytes to copy, with eight sealed candidate generations (`1` through `8`) each still around `268 MiB` total and only `428,792` to `826,791` dead bytes; once the root-churn planner tax fell into the ~`20 us/op` class, this plan output became the decisive evidence that the next sprint should target generation rollover geometry and pack economics, not another planner micro-optimization loop
- generation geometry is still leaf-log file-roll geometry, but this branch now exposes an explicit leaf-only sizing override: `ValueLog.Generational.LeafSegmentTargetBytes` (and `TREEDB_VLOG_GENERATION_LEAF_SEGMENT_TARGET_BYTES`) can seal `leaf_vlog` generations more aggressively without changing generic hot/warm/cold `value_vlog` targets; when unset it still falls back to the generic hot target (`256 << 20` bytes by default), which is why the saved Celestia home still presents ~268 MiB sealed generations today
- the bounded manual path is intentionally fail-closed when the top-ranked candidate alone exceeds `-max-bytes-to-copy`; it does not silently blow the requested copy budget
- cached leaf-pack policy now has explicit `TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MIN_CANDIDATE_GENERATIONS` and `TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_TIMEOUT_SECONDS` knobs, defaulting to `2` candidates and `30s`; the scheduler also exports dedicated `canceled`, `deadline`, and `last_skip_reason` stats so bounded background failures are visible without poisoning generic maintenance error state
- focused scheduler coverage now includes policy propagation, skip-on-too-few-candidates, deadline expiry, and foreground cancellation; the new tests prove deadline/cancel outcomes return the scheduler to `idle` and increment leaf-pack-specific counters instead of surfacing as generic scheduler errors
- on the true `32 MiB` Celestia sync home at `Celestia home 20260415093638`, repeatable `95s` cached dwells in both `fast` and `wal_on_fast` now show the hook is alive and bounded: `3` runs, `0` errors, `0` cancels, `0` deadlines, only `200,704` bytes copied, and plan reclaim moving from about `1,946,127,025` bytes to about `1,866,385,786` bytes; this is measurable progress, but it is intentionally backlog-limited by the current one-generation / `64 MiB` budget
- a fresh end-to-end `run_celestia` on this branch with `TREEDB_OPEN_PROFILE=fast`, leaf-pack maintenance enabled, and a `95s` post-sync dwell completed successfully in `350s` sync / `446s` total at `10,528,336 kB` max RSS; a follow-up copied-home `leafgen-plan` on Celestia home `20260415102153` still reports `133` candidate generations, about `2,103,358,517` reclaimable bytes, and about `2,359,513,641` bytes to copy, which is the clearest current evidence that Sprint 3 solved the control-plane safety problem but not the backlog-drain throughput problem

### Sprint 3 Code Work

- add a bounded online pack path in [TreeDB/db](../TreeDB/db)
  that reuses offline planner/executor components where possible
- add admission checks based on retained bytes, sparse generation count,
  copy-budget, and cooldown
- exclude the writable generation and freshly sealed generations from pack
  candidates
- add restore/catch-up suppression in the cached maintenance layer
- add operator-facing stats for pack admissions, skips, bytes copied, bytes
  reclaimed, wall time, RSS, temp disk, and cancellations

### Sprint 3 Unit Tests

- online pack does not run when candidate set is too small
- online pack does not run during restore/catch-up
- online pack respects copied-bytes and time budgets
- online pack never touches the current writable generation
- online pack can be canceled cleanly without corrupting state

### Sprint 3 Integration / Large-Home Validation

- bounded dwell-style runs against saved homes or local replay proxies; current harness coverage now includes `fast` and `wal_on_fast` opens against the large split-outer-leaf Celestia home, with periodic stats, RSS/HWM, and size snapshots
- if safe and practical, controlled Celestia dwell validation with pack enabled
  behind explicit limits
- completed one fresh `run_celestia` on branch with `TREEDB_OPEN_PROFILE=fast`, pack enabled, `TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MAX_BYTES_TO_COPY=67108864`, `TREEDB_LEAF_GENERATION_PACK_MAINTENANCE_MAX_GENERATIONS=1`, and a `95s` post-sync dwell; sync finished cleanly and the copied-home post-run plan quantified the remaining pack backlog
- verify no obvious sync-path regression from pack guards alone

### Sprint 3 Exit Criteria

- bounded online pack can make useful progress when sparse generations exist
- online maintenance no longer depends on generic leaf rewrite activity
- pack activity is explainable, budgeted, and non-pathological

### Sprint 3 Follow-On Experiment: Leaf Generation Target Sweep

The first saved-home dwell result shows that current Celestia generation geometry
is dominated by the default `256 MiB` hot segment target, and that the resulting
sealed generations are too dense for bounded online pack to matter. The next
experiment should therefore focus on generation creation, not more scheduler
policy.

Completed follow-on:

- ran controlled sweeps across the leaf-only target knob using [scripts/leafgen_target_sweep.sh](../scripts/leafgen_target_sweep.sh)
- recorded generation count, forced-plan reclaim bytes, candidate bytes to copy, and reclaim-per-copy ppm
- validated the best candidates with offline `leafgen-pack` plus GC on copied rebuild outputs

Current readout:

- the small sweep in `leafgen_target_sweep_20260415_smalltargets` shows the structural cliff directly: below `8 MiB` the workload starts to create multiple sealed generations and non-zero reclaim structure, while `8 MiB` already collapses back to one generation and zero candidates
- the larger sweep in `leafgen_target_sweep_20260415_large` is the more useful default-setting signal: `4 MiB` yields `11` generations and only about `327k` ppm reclaim/copy, `8 MiB` yields `6` generations and pack-friendly economics, `16 MiB` yields `3` generations with about `20.8 MiB` reclaimable for about `12.8 MiB` copied, `32 MiB` yields `2` generations with essentially the same reclaim/copy economics, and `64 MiB` already collapses to one generation with zero reclaim candidates
- offline validation on `leafgen_validate_16m_20260415` and `leafgen_validate_32m_20260415` shows both `16 MiB` and `32 MiB` shrinking the rebuilt synthetic DB from about `47.1 MB` to about `26.8 MB` after `leafgen-pack` plus one GC pass, but `32 MiB` achieves the same shrink while packing one source generation instead of two
- current conclusion: `32 MiB` is the best default candidate so far because it preserves reclaim structure while avoiding the generation-count inflation of smaller targets; the override knob remains important for further Celestia-scale tuning

This remains synthetic evidence rather than full Celestia replay, but it is now strong enough to set a leaf-specific default and move the next validation step to larger replay-proxy or dwell-style runs instead of continuing blind planner work.

## 24. Sprint 4 Punch List

Sprint 4 goal:

- finish the control-plane split, delete mixed logic, harden crash behavior, and
  produce the final milestone validation package

### Sprint 4 Deliverables

- `ValueLogRewriteOnline` becomes value-only
- leaf-specific rewrite planning/execution paths are deleted from the generic
  value maintenance flow
- crash-point coverage around manifest/publish/delete and pack transitions
- tuned online leaf-pack policy that can retire meaningful Celestia backlog
  during the standard post-sync dwell instead of merely proving liveness
- final docs and operator guidance
- final Celestia validation readout

### Sprint 4 Recommended PR Stack

- `leafgen/s4-value-only-rewrite`
- `leafgen/s4-delete-mixed-leaf-maintenance`
- `leafgen/s4-crash-hardening-docs-and-final-validation`

### Sprint 4 PR-By-PR Execution Plan

`leafgen/s4-value-only-rewrite`

- remove leaf-page rewrite execution from [TreeDB/db/vlog_rewrite.go](../TreeDB/db/vlog_rewrite.go): delete `rewriteLeafRefsOnline`, `leafRefRewriteCtx`, `LeafRefRecordsCopied`, and `LeafRefBytesCopied`
- remove leaf-page live-byte planning from [TreeDB/db/vlog_rewrite.go](../TreeDB/db/vlog_rewrite.go) and [TreeDB/db/vlog_rewrite_chunk_plan.go](../TreeDB/db/vlog_rewrite_chunk_plan.go): delete `collectLeafRefValueLogLiveBytes`, `collectLeafRefPtrLiveBytes`, `collectLeafRefValueLogLiveBytesByChunk`, and `collectLeafRefPtrLiveBytesByChunk`
- keep `LeafRef` read semantics intact; only delete generic rewrite ownership of outer-leaf pages
- migrate or delete rewrite tests that assume leafref compaction still lives in generic rewrite: [TreeDB/db/vlog_leafref_maintenance_test.go](../TreeDB/db/vlog_leafref_maintenance_test.go), the leafref-specific cases in [TreeDB/db/vlog_rewrite_test.go](../TreeDB/db/vlog_rewrite_test.go), and the leafref portions of [TreeDB/db/vlog_rewrite_bench_test.go](../TreeDB/db/vlog_rewrite_bench_test.go)
- required proof before PR merge:
  - unit: `ValueLogRewritePlan` on a split-outer-leaf DB with no true value pointers returns no leaf-driven candidates
  - unit: `ValueLogRewriteOnline` copies only value records and never increments leaf-specific counters because those counters no longer exist
  - saved-home: planner/executor on a copied split-outer-leaf home show value-only behavior while `leafgen-plan` still reports leaf reclaim opportunities separately

`leafgen/s4-delete-mixed-leaf-maintenance`

- remove leaf reachability merges from [TreeDB/db/vlog_gc_incremental.go](../TreeDB/db/vlog_gc_incremental.go): delete `mergeLeafRefValueLogRefs`, `collectLeafRefValueLogRefCounts`, `shouldScanLeafRefValueLogRefs`, and related state-tracking helpers
- remove cached mixed counters from [TreeDB/caching/db.go](../TreeDB/caching/db.go): delete `vlogGenerationRewriteLeafRefRecordsCopied`, `vlogGenerationRewriteLeafRefBytesCopied`, and their exported stats
- make the split explicit in operator stats: `leaf_vlog` lifecycle comes only from leaf-generation stats, and `value_vlog` stats describe only value records
- required proof before PR merge:
  - unit: `value_vlog` GC no longer scans leaf pages or merges leaf refs into value refcounts in split mode
  - unit: leaf-generation GC/pack tests still cover leaf reclaim after mixed GC logic is deleted
  - saved-home: `value_vlog` retained bytes remain stable when only leaf pages churn, and `leafgen-plan` remains the only source of leaf reclaim accounting

`leafgen/s4-crash-hardening-docs-and-final-validation`

- harden generation state transitions around [TreeDB/db/leaf_generation_manifest.go](../TreeDB/db/leaf_generation_manifest.go), publish paths, and generation deletion so crash ordering is explicit and testable
- document final operator model: `leaf_vlog` = generation GC + optional bounded pack, `value_vlog` = value-only GC/rewrite, `wal` = durability log only
- assemble the milestone validation package from saved-home and Celestia runs after the control-plane deletion PRs are green
- required proof before PR merge:
  - crash-point tests cover seal, publish, retire, delete ordering
  - saved-home validation package is updated after the control-plane split
  - final Celestia validation reports separate leaf/value metrics and shows no background page-by-page leaf rewrite activity

### Sprint 4 Code Work

- remove leaf-ref rewrite from the common path in
  [TreeDB/db/vlog_rewrite.go](../TreeDB/db/vlog_rewrite.go)
- remove leaf-ref live-byte planning from common rewrite selection and chunk
  planning paths
- remove leaf-ref reachability merges from common `value_vlog` GC paths
- split leaf metrics from value metrics in [TreeDB/db/api.go](../TreeDB/db/api.go)
  and [TreeDB/caching/db.go](../TreeDB/caching/db.go)
- harden manifest/publish/delete crash semantics and cleanup any obsolete
  counters or dead code
- update docs so the generation model is the only documented leaf-maintenance
  architecture

### Sprint 4 Unit Tests

- crash after sealing old generation but before new root publish
- crash after publishing packed generation but before old generations are marked
  retiring
- crash after old generations are marked retiring but before deletion completes
- generic value rewrite no longer rewrites or scans leaf pages in the common
  path
- generic value rewrite tests fail if they observe leafref-specific stats or
  leaf-page copy activity
- value-log incremental GC no longer walks leafref pages in split mode
- leaf maintenance metrics are separate from value maintenance metrics
- legacy leafref rewrite tests/benches are either deleted or replaced with
  leaf-generation pack tests/benches

### Sprint 4 Integration / Large-Home Validation

- saved-home regression suite
- final Celestia sync + dwell validation package
- standard post-sync Celestia dwell must show material backlog reduction, not
  only successful bounded runs
- document final retained bytes, pack reclaim, wall time, RSS, and temp-disk
  behavior

Current saved-home dwell result on `2026-04-15` using
Celestia home `20260415102153`
and `scripts/leafgen_cached_dwell_validate.sh` with the new default leaf-pack
window (`256 MiB`, `8` generations, `180s` dwell, `fast` profile):

- `application.db`: `4,736,992,671` bytes -> `3,431,477,665` bytes
- `leaf_vlog`: `4,491,474,477` bytes -> `3,191,178,215` bytes
- forced `leafgen-plan` expected reclaim: `2,103,358,517` bytes ->
  `806,745,850` bytes
- runtime pack maintenance: `5` successful windows, `214,790,144` bytes copied,
  `40` generations deleted, `40` files deleted
- dwell RSS/HWM: `407,904 KiB` / `471,340 KiB`

This is the first saved-home proof on the branch that Sprint 4 is no longer
just "bounded and correct"; the standard Celestia-shaped dwell now removes a
material portion of the leaf backlog during steady-state maintenance.

Required final runtime readout:

- pre-dwell and post-dwell `leafgen-plan` on the same copied home
- pre-dwell and post-dwell `leaf_vlog` / `application.db` sizes
- number of generations retired and deleted during dwell
- bytes copied by online pack during dwell
- remaining expected reclaim bytes after dwell
- sync wall time, dwell RSS/HWM, and any observable sync regression

Sprint 4 runtime success requires all of:

- no control-plane regression versus Sprint 3: bounded, explainable, and
  non-pathological behavior still holds
- the standard Celestia dwell materially reduces leaf backlog, not just by a
  token amount
- the reduction is visible in either retired/deleted generations or on-disk
  `leaf_vlog` size after GC, not only in "runs happened" counters
- post-dwell remaining reclaim backlog is materially lower than the current
  Sprint 3 baseline

### Sprint 4 Follow-On: Reclaimed-Bytes-Per-Byte-Copied Efficiency

The current branch now has live proof that online `leaf-pack` engages and makes
material progress under real Celestia dwell. The next PR must not revisit the
control-plane question. It must improve maintenance efficiency on the exact same
workload.

Current live baseline from the fresh full `wal_on_fast` run at
Celestia home `20260415150245`:

- `application.db`: `4.303 GiB` at sync complete -> `2.474 GiB` after the
  standard `15m` dwell
- `leaf_pack_runs`: `25`
- `leaf_pack_bytes_copied`: about `7.462 GiB`
- final gap versus the current recorded offline TreeDB compacted floor:
  `2.474 GiB - 2.092 GiB = 0.382 GiB`

That means the remaining problem is no longer "maintenance does not run." The
remaining problem is "maintenance copies too many mostly-live bytes per unit of
reclaim."

The immediate follow-on PR should therefore target:

- better reclaimed-bytes-per-byte-copied on the standard post-sync Celestia
  dwell
- fewer low-yield pack windows against mostly-live generations
- improved end-of-dwell `leaf_vlog` / `application.db` size without reopening
  the mixed generic rewrite architecture

Preferred scope for the follow-on efficiency PR:

- tighten pack candidate ranking to favor higher dead-byte density and avoid
  spending window budget on near-live generations too early
- add explicit per-window yield accounting so the scheduler can stop or defer
  when a candidate set is real but low-value
- bias online pack toward deleting more whole generations per copied byte, even
  if that means fewer total windows run
- preserve the current active-safe admission model; do not reintroduce global
  quiet-window dependence as the main control

Required proof for that follow-on PR:

- same validation shape as the current live run: full `run_celestia` +
  `15m` dwell
- compare against the current baseline, not against an older broken scheduler
  baseline
- report:
  - sync wall time
  - dwell RSS/HWM
  - pre-dwell and post-dwell `application.db` and `leaf_vlog`
  - `leaf_pack_runs`
  - `leaf_pack_bytes_copied`
  - deleted generations/files
  - final gap versus the `2.092 GiB` offline compacted TreeDB floor

### Sprint 4 Exit Criteria

- `leaf_vlog` maintenance is generation GC plus pack only
- `value_vlog` maintenance is value-only
- crash behavior is explicit and tested
- standard Celestia dwell shows substantial leaf cleanup rather than merely safe
  low-throughput liveness
- docs, tests, and validation results are consistent with the final design

## 25. Validation Framework

The milestone should not be judged by unit tests alone. It requires three
validation levels.

### Unit / Integration Validation

- lifecycle correctness
- reopen correctness
- crash-point ordering correctness
- snapshot pin behavior
- pack publish correctness

### Saved-Home Validation

Use the saved Celestia homes and similar large homes as the main architecture
proof before enabling or claiming success on online behavior.

Required saved-home checks:

- estimator output is explainable and stable
- whole-generation GC reclaims fully dead generations when they exist
- offline `leaf-pack` reclaims sparse retained space that ordinary GC cannot
- reclaimed-bytes-per-byte-copied is favorable on real homes

### Celestia Validation

Celestia validation should happen in two steps:

1. after Sprint 2, validate offline `leaf-pack` against saved homes and fresh
   copied homes
2. after Sprint 3 and Sprint 4, validate bounded online behavior during real or
   realistic dwell runs

Required final checks:

- no background page-by-page leaf rewrite activity remains
- retained generation counts and retained bytes are explainable
- online pack, if enabled, is bounded and useful rather than churn-heavy
- online pack materially reduces the standard post-sync Celestia leaf backlog
  during dwell
- sync and dwell behavior remain operationally acceptable

## 26. Authoritative Acceptance Criteria

The milestone is complete only when all of the following are true:

- `leaf_vlog` is managed as immutable generations
- ordinary leaf maintenance is whole-generation GC only
- sparse retained generations can be compacted by explicit `leaf-pack`
- `ValueLogRewriteOnline` is value-only in the common path
- leaf metrics and value metrics are separate and operator-readable
- saved-home validation proves that `leaf-pack` solves reclaim cases that
  whole-generation deletion alone cannot solve
- final Celestia validation shows the resulting maintenance model is bounded,
  correct, explainable, and materially effective on the standard dwell

The milestone is not complete if TreeDB still requires repeated generic
full-tree rediscovery to keep `leaf_vlog` healthy in steady state.

## 27. Transitional Code And Deletion Targets

The current codebase still has a large mixed surface where outer-leaf storage is
handled as generic value-log rewrite/GC work. That surface should be treated as
transitional.

The main deletion targets are:

- [TreeDB/db/vlog_rewrite.go](../TreeDB/db/vlog_rewrite.go)
  `LeafRefRecordsCopied`, `LeafRefBytesCopied`, `rewriteLeafRefsOnline`, and the
  `leafRefRewriteCtx` machinery. These are symptoms of the wrong abstraction for
  `leaf_vlog` steady-state maintenance.
- [TreeDB/db/vlog_rewrite.go](../TreeDB/db/vlog_rewrite.go)
  `collectLeafRefValueLogLiveBytes` and `collectLeafRefPtrLiveBytes`. These are
  tree-scan rediscovery paths and should not remain on the common leaf
  maintenance path.
- [TreeDB/db/vlog_rewrite_chunk_plan.go](../TreeDB/db/vlog_rewrite_chunk_plan.go)
  `collectLeafRefValueLogLiveBytesByChunk` and related chunk planning for leaf
  refs. Chunking does not fix the architectural problem; it only slices the same
  rediscovery model more finely.
- [TreeDB/db/vlog_gc_incremental.go](../TreeDB/db/vlog_gc_incremental.go)
  `mergeLeafRefValueLogRefs`, `collectLeafRefValueLogRefCounts`, and related
  leaf-ref reachability merge paths. These should not stay on the common
  `value_vlog` GC path once leaf generations exist.

Transitional code that should remain, but only temporarily:

- current `LeafRef` encoding and read path, because the first rollout should
  preserve leaf read semantics while the storage lifecycle changes underneath
- current split on-disk directories `leaf_vlog/`, `value_vlog/`, and `wal/`
- existing offline tooling as a fallback or audit mechanism until manual
  leaf-pack exists

The cleanup principle is:

- first land generation lifecycle correctness
- then delete mixed rewrite logic
- only then decide whether any shared utility code is still justified

## 28. What Success Looks Like

A correct first rollout should make leaf maintenance boring.

That means:

- normal writes append leaf pages into one writable generation
- sealed generations just sit there until they are either fully dead or
  explicitly packed
- whole-generation GC is cheap and obvious
- `value_vlog` rewrite/GC is free to focus only on actual value records
- operators can reason about retained leaf bytes using generation counts and
  states, not by interpreting rewrite counters

If TreeDB still needs repeated full-tree discovery to keep `leaf_vlog` healthy,
then this RFC has not been implemented correctly.


## 29. What This Means For `value_vlog`

Once outer-leaf pages are no longer maintained through generic rewrite logic,
`value_vlog` becomes a much simpler subsystem again.

Its job should be:

- store actual large value records
- support pointer durability across reopen
- support reachability-based GC
- support explicit offline rewrite/compaction when operators want denser storage
- optionally support bounded online value rewrite later, but only for value
  records

Its job should not be:

- carry outer-leaf page lifecycle
- participate in leaf-generation sealing or deletion decisions
- scan the tree for leaf-ref rediscovery on behalf of `leaf_vlog`

That split matters because the physical economics are different.

`value_vlog` is a record store. Rewriting selected value records is still a
reasonable abstraction there.

`leaf_vlog` is a page store. Rewriting pages one record at a time to maintain
steady-state health is the wrong abstraction there.

In practical terms, after the leaf-generation rollout:

- `ValueLogRewriteOnline` should become value-only or be removed entirely if its
  cost/benefit remains poor
- `ValueLogRewriteOffline` can remain as an operator tool for dense rebuild of
  value records
- value-log GC should reason only about value pointers and segment reachability
- leaf-pack and generation GC should own outer-leaf reclaim

This is also why dead-code cleanup matters.

As long as `value_vlog` still contains leaf-specific accounting, planning, or
rewrite branches, the implementation will keep pulling the two subsystems back
together and the maintenance model will drift back toward the current confused
state.
