# Recoverable-root maintenance authority

Issue #3681 makes destructive TreeDB maintenance consume one DB-minted
`RecoverableRootSet`. The capability is a bounded snapshot of every root and
stable resource that can still become recovery-selectable. It includes both
validated durable meta slots, the visible and queued publication frontiers,
publication resource manifests, snapshot/history pins, the oldest protected
commit sequence, and the applied command-WAL frontier of each root.

A destructive plan MUST capture the capability after maintenance admission and
MUST revalidate that same instance immediately before its first mutation. A
stale capability deletes nothing. Callers MAY release exact resource pins after
successful revalidation only while holding a publication fence that prevents a
new visible root from appearing before the mutation completes.

Capture performs no tree scan. Its scalar work is bounded by the two durable
slots and publication debt; resource work is bounded by retained manifests and
pins. Maintenance operations walk the captured roots only when they need a
resource-specific reachability projection.

## Checked destructive-call-site inventory

| Storage class | Destructive owner | Capability integration | Status |
|---|---|---|---|
| Persistent value-log segments | `TreeDB/db/vlog_gc.go` | scans value pointers from every captured root, unions exact referenced segment IDs, then revalidates under `publishPrepareMu` before `MarkZombie`; full GC only retires segments absent from the whole union | active |
| Value-log rewrite sources | `TreeDB/db/vlog_rewrite.go` | publishes rewritten pointers first; source retirement delegates to capability-backed value-log GC. A source remains in the current manager topology while any recoverable root still references it. Once absent from every recoverable root, zombie publication removes it from the current topology while snapshot/resource pins defer physical unlink; a stale cleanup capability records retained debt rather than failing the committed rewrite | active |
| Exported/cached value-log zombie requests | `TreeDB/db/db.go` (`MarkValueLogZombie`) and cached retention callers | delegates to capability-backed observed-source GC instead of directly marking a manager file zombie | active |
| Zero-byte value-log cleanup | `TreeDB/db/compact_storage.go` (`pruneZeroByteValueLogFiles`) | captures and revalidates under the publication fence, then keeps that fence through stable deletion and directory durability | active |
| Raw and packed outer-leaf generations | `TreeDB/db/leaf_generation_gc.go` | scans raw leaf FileIDs from every captured root, resolves them through every retained leaf-generation manifest, preserves FileID reuse/ABA generations, and revalidates before zombie publication | active |
| Leaf pack/rewrite sources | `TreeDB/db/leaf_generation_pack.go`, `TreeDB/db/compact_storage.go` | replacement output is stabilized and published first; physical retirement is exclusively owned by capability-backed leaf-generation GC | active |
| Column, dictionary, template, typed-column, vector-graph, text, and query-ready assets | `TreeDB/collections/column_asset_gc.go` and `column_asset_rewrite.go` | scans each captured collection catalog/manifest, pins exact referenced segment identities, protects published generations replayable from an older durable WAL frontier, and revalidates before `BeginDeleteAt`. Current query-ready base/delta/consolidated assets remain rebuildable and non-authoritative, but share the same exact-identity retirement gate | active |
| Online index vacuum/replacement | `TreeDB/db/vacuum_online.go`, `TreeDB/public.go`, `TreeDB/bg_vacuum.go`, and `TreeDB/db/compact_storage.go` | the production entry captures a DB-minted capability, rebuilds both durable slots and their per-root resource closure, revalidates before namespace mutation, and atomically rebinds the live publication runtime; public and CompactStorage wrappers checkpoint cached state around successful replacement and reconcile it afterward | active on supported writable opens; CompactStorage reports typed policy outcomes |
| Offline `CompactIndex` | `TreeDB/db/db.go` | appends and publishes new pages in the same index namespace; it does not unlink or replace the index file. Page eligibility is governed by the page rule below | no external unlink |
| Graveyard extraction and page/freelist reuse | COW allocator and root-reuse paths from #3678 | `RecoverableRootSet` registers the oldest captured commit sequence and holds the root-reuse read fence; actual page eligibility remains the #3678 generation rule | delegated to #3678 |
| Stable unlink/rename instrumentation | `TreeDB/db/namespace_mutation.go`, `TreeDB/internal/valuelog/stable_resource.go`, and collection stable deleters | exact identity, namespace lease, unlink observation, and directory-sync failure handling remain the PR #3706 contract; #3681 adds root authority before those operations | inherited from PR #3706 |
| Command-WAL segment deletion | `TreeDB/db/command_wal_publish.go` and cached WAL cleanup | ordinary WAL deletion is intentionally owned by #3682. #3681 only supplies the applied/replayable frontier used by resource retirement | adjacent #3682 |

The following direct filesystem operations are not deletion of published
recovery state and therefore do not consume the capability:

- rollback truncation/removal of unpublished prepared column-asset tails, after
  proving no later writer appended to the shared segment;
- growth/materialization truncates that extend an index generation to its
  prepared high-water mark;
- temporary-file and temporary-directory cleanup before publication;
- rebuildable legacy vector-index epochs, which never select recovery state and
  retain exact-search fallback;
- Raft transport snapshot staging/cleanup, which is outside the local TreeDB
  root-selection namespace.

## Failure and convergence contract

- Candidate enqueue, durable-meta rotation, system-root publication, index
  replacement, or publication-resource debt changes invalidate the capability.
- An invalid capability returns `ErrRecoverableRootSetStale`; destructive paths
  retain their candidates and expose retryable debt/diagnostics.
- A durable fallback behind the visible command-WAL frontier protects
  format-valid authoritative typed-row/typed-column generations that replay can
  recreate. Newer or malformed unpublished candidates are not replay roots.
  Query-ready assets remain rebuildable and non-authoritative unless a later
  contract explicitly promotes them.
- Once both durable slots advance beyond the retired root and explicit
  snapshots/iterators/`KeepRecent` pins drain, a fresh maintenance pass can
  reclaim the retained resource.
- Namespace deletion is successful only after its owning directory has been
  made durable. An unlink followed by directory-sync failure is recovery
  required, never reported as clean success.

## Online index replacement contract

The production `db.DB.VacuumIndexOnline` backend is authorized only by a
`RecoverableRootSet` minted by the same live DB. It rebuilds the older durable
slot's user, system, and collection closure first, then rebuilds the latest
visible closure as its consecutive successor. Each replacement slot carries
the original root's stable-resource manifest and command-WAL frontier; the
replacement index is therefore independently complete for either recovery
selection.

The replacement pager, durable-root state, snapshot view, allocator/freelist,
and root-publication runtime are constructed and stabilized before namespace
publication. Final publication drains pending root debt outside `writeMu`,
revalidates the capability, writes the ready marker, and performs this ordered
namespace transition:

1. rename `index.db` to `index.db.bak`;
2. rename the stabilized replacement to `index.db`;
3. remove the ready marker and obsolete backup;
4. sync the parent directory.

An ambiguous rename or directory-sync result poisons publication and returns
`ErrRecoveryRequired`; destructive maintenance remains blocked until reopen
reconciles the marker and namespace. Before the first rename, cancellation or
revalidation failure leaves `index.db` authoritative and removes only
unpublished replacement artifacts. After the first rename, recovery rather
than cancellation owns convergence.

Once namespace publication is durable, the DB installs the replacement pager,
durable slots, state token, snapshot, and publication runtime as one coherent
in-process generation. A writer that released `writeMu` while waiting for the
cutover rechecks the runtime and acquires a builder from the replacement
generation. The retired coordinator is stopped, drained, and its recovery
handoff released. Existing snapshots, iterators, and stable-resource captures
continue to pin old-generation handles until they close; physical cleanup is
deferred until those references drain.

The public `DB.VacuumIndexOnline` entry checkpoints cached state before calling
the production backend and reconciles the cached runtime after a successful
replacement. Writable non-Windows opens start the background worker when the
normalized interval is positive (`0` selects the 30-second default and a
negative value disables it). Read-only and Windows opens do not start it.
Windows remains explicitly unsupported because the required open-file rename
and generation-retirement semantics are not implemented there.

The background worker preserves unchanged-commit probe suppression and the
user-page, freelist, collection-root, and bounded-backlog triggers. Concurrent
mutation, stale recoverable-root-set results, and stale command-WAL cleanup
proofs are retry outcomes and do not invoke `NotifyError`. An unsupported result quiesces the worker without a retry
loop. Permanent failures invoke `NotifyError` once for the unchanged state and
remain in `treedb.bg_vacuum.last_err`; retry class and terminal outcome are
separately exposed by `last_retry_reason`, `last_outcome`, and their cumulative
counters. `Close` cancels an active pass and waits for the worker before closing
maintenance or storage resources.

`CompactStorage` consumes this production path when its bounded index-debt
planner selects work. Transient capability invalidation is reported as deferred
debt; it is never converted into successful compaction.

## Verification map

- `TreeDB/db/recoverable_root_set_test.go`: both durable slots, stale candidate
  enqueue, stale durable advance, root-bound snapshots, and exact identity pins.
- `TreeDB/db/vlog_gc_test.go`: full and observed-source GC both retain an older
  durable root's value-log pointer after a newer visible root drops it.
- `TreeDB/db/leaf_generation_gc_test.go`: stale scans delete nothing, durable
  fallback generations remain live, FileID reuse is conservative, and debt
  converges after fallback/snapshot advance.
- `TreeDB/collections/column_asset_gc_test.go` and
  `column_asset_rewrite_test.go`: snapshot and replay-frontier retention,
  replacement-before-retirement, exact stale-plan checks, and post-advance
  convergence.
- PR #3706 stable-resource tests remain the namespace identity, unlink, and
  directory-durability regression suite.
