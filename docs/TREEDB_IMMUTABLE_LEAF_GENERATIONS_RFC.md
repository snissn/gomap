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
- omit `deleted` generations from newly published views so a generation already
  scheduled for removal cannot repin itself
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
- snapshot pins and backend whole-generation GC: current slice

### Sprint 1 Code Work

- add manifest/state files under [TreeDB/db](/home/mikers/dev/snissn/gomap/TreeDB/db)
  for generation metadata, load/save, and validation
- wire DB open/load/save in [TreeDB/db/db.go](/home/mikers/dev/snissn/gomap/TreeDB/db/db.go)
- extend [TreeDB/db/leaf_page_log.go](/home/mikers/dev/snissn/gomap/TreeDB/db/leaf_page_log.go)
  and [TreeDB/caching/leaf_page_log.go](/home/mikers/dev/snissn/gomap/TreeDB/caching/leaf_page_log.go)
  so outer-leaf appends belong to one explicit writable generation
- introduce generation rollover thresholds and sealing logic
- add generation liveness tracking from live roots and pinned snapshots
- add whole-generation deletion logic for fully dead sealed generations
- land backend generation-state and GC semantics first in [TreeDB/db](/home/mikers/dev/snissn/gomap/TreeDB/db)
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

### Sprint 2 Code Work

- add reusable planner helpers that consume the leaf-generation state plus live
  `LeafRef` reachability and rank sparse generations by reclaim ratio
- add offline `leaf-pack` implementation under [TreeDB/db](/home/mikers/dev/snissn/gomap/TreeDB/db)
- add CLI/tooling entrypoints under [TreeDB/cmd/treemap](/home/mikers/dev/snissn/gomap/TreeDB/cmd/treemap)
- reuse or extract the leaf-ref remap/publish mechanics currently living in
  [TreeDB/db/vlog_rewrite.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vlog_rewrite.go)
- reuse rebuild/swap ideas from [TreeDB/db/vacuum_online.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vacuum_online.go)
  and [TreeDB/db/vacuum_offline.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vacuum_offline.go)
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

### Sprint 3 Code Work

- add a bounded online pack path in [TreeDB/db](/home/mikers/dev/snissn/gomap/TreeDB/db)
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

- bounded dwell-style runs against saved homes or local replay proxies
- if safe and practical, controlled Celestia dwell validation with pack enabled
  behind explicit limits
- verify no obvious sync-path regression from pack guards alone

### Sprint 3 Exit Criteria

- bounded online pack can make useful progress when sparse generations exist
- online maintenance no longer depends on generic leaf rewrite activity
- pack activity is explainable, budgeted, and non-pathological

## 24. Sprint 4 Punch List

Sprint 4 goal:

- finish the control-plane split, delete mixed logic, harden crash behavior, and
  produce the final milestone validation package

### Sprint 4 Deliverables

- `ValueLogRewriteOnline` becomes value-only
- leaf-specific rewrite planning/execution paths are deleted from the generic
  value maintenance flow
- crash-point coverage around manifest/publish/delete and pack transitions
- final docs and operator guidance
- final Celestia validation readout

### Sprint 4 Recommended PR Stack

- `leafgen/s4-value-only-rewrite`
- `leafgen/s4-delete-mixed-leaf-maintenance`
- `leafgen/s4-crash-hardening-docs-and-final-validation`

### Sprint 4 Code Work

- remove leaf-ref rewrite from the common path in
  [TreeDB/db/vlog_rewrite.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vlog_rewrite.go)
- remove leaf-ref live-byte planning from common rewrite selection and chunk
  planning paths
- remove leaf-ref reachability merges from common `value_vlog` GC paths
- split leaf metrics from value metrics in [TreeDB/db/api.go](/home/mikers/dev/snissn/gomap/TreeDB/db/api.go)
  and [TreeDB/caching/db.go](/home/mikers/dev/snissn/gomap/TreeDB/caching/db.go)
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
- leaf maintenance metrics are separate from value maintenance metrics

### Sprint 4 Integration / Large-Home Validation

- saved-home regression suite
- final Celestia sync + dwell validation package
- document final retained bytes, pack reclaim, wall time, RSS, and temp-disk
  behavior

### Sprint 4 Exit Criteria

- `leaf_vlog` maintenance is generation GC plus pack only
- `value_vlog` maintenance is value-only
- crash behavior is explicit and tested
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
  correct, and explainable

The milestone is not complete if TreeDB still requires repeated generic
full-tree rediscovery to keep `leaf_vlog` healthy in steady state.

## 27. Transitional Code And Deletion Targets

The current codebase still has a large mixed surface where outer-leaf storage is
handled as generic value-log rewrite/GC work. That surface should be treated as
transitional.

The main deletion targets are:

- [TreeDB/db/vlog_rewrite.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vlog_rewrite.go)
  `LeafRefRecordsCopied`, `LeafRefBytesCopied`, `rewriteLeafRefsOnline`, and the
  `leafRefRewriteCtx` machinery. These are symptoms of the wrong abstraction for
  `leaf_vlog` steady-state maintenance.
- [TreeDB/db/vlog_rewrite.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vlog_rewrite.go)
  `collectLeafRefValueLogLiveBytes` and `collectLeafRefPtrLiveBytes`. These are
  tree-scan rediscovery paths and should not remain on the common leaf
  maintenance path.
- [TreeDB/db/vlog_rewrite_chunk_plan.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vlog_rewrite_chunk_plan.go)
  `collectLeafRefValueLogLiveBytesByChunk` and related chunk planning for leaf
  refs. Chunking does not fix the architectural problem; it only slices the same
  rediscovery model more finely.
- [TreeDB/db/vlog_gc_incremental.go](/home/mikers/dev/snissn/gomap/TreeDB/db/vlog_gc_incremental.go)
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
