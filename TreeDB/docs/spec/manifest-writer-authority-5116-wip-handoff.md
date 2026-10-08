# #5116 unvalidated writer manifest repair handoff

This is source preservation in user-requested HANDOFF MODE, not an accepted
implementation or runtime result. The source base is
`3c33dd77d2457a8e477ec0e7cd50bb8fde80fcc3`. No Go compilation, tests, race tests,
benchmark, remote execution, review or acceptance gate ran for this candidate.
Only gofmt and Git whitespace checks were authorized locally. A fresh native
baseline RED is still required before resuming the implementation fix loop.

## Selected defect and retained evidence

The meaningful earlier native baseline reached genuinely exact both-slot
manifest coverage after real vacuum for serialized, queued and build-group
ascending writers, then acknowledged a new producer whose root referenced raw
FileID 2139095051 while manifest revision 3 omitted it. The read-only census was
UNKNOWN; all 258 values verified after reopen. This is not current-main runtime
evidence. Root's compiler-bound observation SHA256 is
`05475a05a6f4e43b06775500b045743c88838d27c8bb83973beb4a9836284ede`.

The committed external fixture is the corrected V4 source, SHA256
`17d9c641b0746b1f02b26ccb29f4221e522c40daaa5bd2c80bc61e2fc46f7f90`.
Its nine auxiliary file-ID checks use canonical `ptr.ValueLogFileID()`; the
meaningful ascending invariant is unchanged. Parent-coordinated current-main
native baseline attempts failed in setup/window admission before copy or Go;
they do not constitute RED or compile results.

Retained original packets live under
`/Users/michaelseiler/dev/snissn/gomap/tmp/quicksilver-maintenance-5028-20261005`:

- `qualification-v13-current-checkpoint-publication-v94/root-writer-repair-selection.json`
  and `writer-current-block.md` select #5116.
- `qualification-v13-writer-invariant-native-preparation-v3` retains the earlier
  raw/compiler-bound observation.
- `qualification-v13-external-manifest-writer-invariant-v4` retains the corrected
  fixture and its provenance.
- `qualification-v13-manifest-writer-repair-design-v1/REPORT.md`,
  `qualification-v13-writer-wal-admission-design-v1/REPORT.md` and
  `qualification-v13-actual-physical-intent-callers-v1/REPORT.md` retain the
  selected design and narrowed WAL caller audit.

These designs were B-based. B/R-only apply-leaf projection APIs and the adjacent
R1 owned-manifest ABI are absent from this source base and were not adopted.

## What the draft changes

`writer_manifest_preparation.go` consolidates optimistic, serialized and group
writer publication. It retains exact index/base/producer handles, stages stable
manifest semantics before capture, releases serialization for durability and
immutable replacement preparation, revalidates roots/sequence/index/manifest/
pending/conditional state, and transfers the exact returned token to the same
finalizer. Membership reuse requires existing exact manifest authority and
avoids replacement revision/sync. Selected pending attribution is consumed at
successful visibility, rather than indiscriminately in late post-work.

Serialized apply uses a private allocator tracker and clone. Group finalization
has a single owner while detached; Close waits for that owner before cleanup.
Existing compatibility-mode late staging remains separate from stable-token
preparation. Ordinary unassigned WAL retains its existing outer guard while
append and dependency synchronization release root serialization. Physical
writers reject invented unassigned/replay/group intents. Assigned publication
failure returns recovery-required and prevents output rollback or a second
append. Accepted publication output is retained after later wait errors.

These statements describe intended source behavior, not validated outcomes.
No durable format change, cap change, integrity relaxation, lower-sequence
filter change, early allocator preparation barrier, directory absence permission
or B/R optimization extraction was made. #5004 / PR5006 remain held.

## Finite affected surfaces

- `TreeDB/db/batch.go`
- `TreeDB/db/db.go`
- `TreeDB/db/command_wal_publish.go`
- `TreeDB/db/root_publication_activation.go`
- `TreeDB/db/root_publication_build_group.go`
- new `TreeDB/db/writer_manifest_preparation.go`
- new `TreeDB/db/writer_manifest_preparation_test.go`
- corrected `TreeDB/db/external_manifest_writer_invariant_test.go`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`
- this handoff document

No benchmark harness surfaces owned by #5117 were edited.

## Open gates and known issues

1. Preserve actual current-main native RED from immutable base plus exact V4
   fixture before treating the candidate as a repair. The meaningful selector is
   `TestExternalManifestWriterACKCoverage/(serialized|queued|build-group)/ascending$`.
2. Compile this exact WIP source, then run `TestWriterManifest` and ascending
   fixture cases normally and under race. The draft has not been type-checked.
3. The unchanged controlled-present-lower-sequence fixture currently requires
   ACK, but the selected design deliberately preserves the lower-sequence
   registration filter. The draft intends to reject unmapped required producer
   membership before capture. After preserved RED, narrowly convert the
   controlled case to expected fail-closed rejection with unchanged-root and
   pending-ownership assertions, preserving its causal history and ascending
   positive invariant. Its expectations have NOT been changed in this commit.
4. Native risk coverage must include both slots/reopen, index/root/manifest/
   pending drift, exact abandoned token cleanup, competing prepared revisions,
   private allocator abandonment, group Close, ordinary and preassigned WAL
   failure/recovery/no-double-append, accepted wait errors, handle lifetimes,
   snapshots and GC fail-closed behavior. Existing tests and new tests need
   actual execution; source reasoning alone is insufficient.
5. New focused tests currently cover manifest/pending/root drift and exact
   revision abandonment, unchanged membership sync counters, group finalizer
   versus Close, failed publication retaining pending attribution, and an
   ordinary assigned-WAL finalize failure without a second append. They do not
   by themselves complete the preceding risk packet.
6. Measure equivalent public-path allocation, throughput, process memory and
   content/namespace sync controls. The draft reuses one selected base clone for
   final capture and skips empty producer pins, but it adds tracked serialized
   ownership and producer retention. No performance or memory claim is made.
7. Audit compatibility-mode behavior, post-work ordering, accepted error cleanup,
   and stale retry/assigned-WAL classification against actual main callers.
   Broad normal/race/lifecycle/WAL/GC suites remain required. Native selectors
   include `TestRootPublicationPathsPinTeardownThroughPostWork` and the existing
   root publication, command-WAL, snapshot and value-log GC tests.

The local draft and read-only audit copies remain at
`/tmp/gomap-5116-writer-draft-20261008`. Its `transform.py` is an initial generator,
not a resumable script: rerunning it would overwrite subsequent draft fixes.
The isolated worktree is
`/Volumes/FlashDrive/gomap-5028-owned-worktrees-20261008/manifest-writer-authority`
on `codex/5116-manifest-writer-authority`. Root owns push, draft PR creation,
issue/graph synchronization, future native scheduling, reviews and merges.
