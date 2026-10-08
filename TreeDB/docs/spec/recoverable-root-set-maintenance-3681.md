# Recoverable-root maintenance authority

Issue #3681 makes destructive TreeDB maintenance consume one DB-minted
`RecoverableRootSet`. The capability is a bounded snapshot of every root and
stable resource that can still become recovery-selectable. It includes both
validated durable meta slots, the visible and queued publication frontiers,
publication resource manifests, snapshot/history pins, the oldest protected
commit sequence, and the applied command-WAL frontier of each root.

For continuously managed collections under `command_wal_durable`, column GC
computes a minimum generation during the existing validated per-root manifest
pass. Only candidates in the matching namespace with generation strictly below
that minimum omit the *additional replay-candidate* pin. Each captured root's
actual refs remain pinned independently, including older physical generations.
Equality remains conservative because maintenance may replace assets without
advancing generation. The existing per-visible-root LSN/generation/schema tuples
remain paired; global LSN ordering is not used to infer generation ordering.

This optimization is disabled if any captured root lacks a collection or valid
nonzero active/recovery manifest, or has incompatible metadata, schema hash, or
namespace, and for non-durable or WAL-off profiles. Supported managed APIs do
not provide collection drop/recreate; incompatible create is rejected, and a
persisted profile cannot silently switch to WAL-off and back. This restricted
continuity contract does not make schema hash an incarnation ID or certify
unsupported catalog replacement. Future drop/recreate support must revisit the
predicate. No new manifest scan, registry, or on-disk identity is introduced.

A destructive plan MUST capture the capability after maintenance admission and
MUST revalidate that same instance immediately before its first mutation. A
stale capability deletes nothing. Callers MAY release exact resource pins after
successful revalidation only while holding a publication fence that prevents a
new visible root from appearing before the mutation completes.

Typed graph work-epoch renewal and vector-partition reclamation may retry a
column-asset GC pass on `ErrRecoverableRootSetStale`, with at most eight attempts,
only when that pass deleted no segments. Each retry releases the stale capture
and rebuilds the full plan from fresh roots. Exhaustion returns the stale error;
it does not advance the work epoch. Partial deletion or unrelated errors are not
retryable by this rule. No checkpoint or sleep is introduced inside the held
schema/storage/mutation locks.

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

Publication metadata and auxiliary output can reuse capability-certified
contiguous free intervals (#4627); observing old auxiliary IDs allocated again
is not by itself a retention leak. Placement examines at most four 256-page
chunks and otherwise falls back to the existing tail path. It avoids repeatedly
inspecting a fragmented prefix by advancing a transient ledger hint past the
last observed chunk after a failed search; success keeps its chunk for locality.
Updates apply only if the initial hint remains unchanged. Metadata
and auxiliary searches share this hint; each attempt wraps at most once and
does not repeat chunks. This changes placement only, never reclamation authority.
This is not a full free-extent search, physical shrink, or a replacement
for vacuum when fragmentation or oversized requests require it. Recovery and
snapshot eligibility are unchanged.

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

## Intrinsic pager-owned leaf manifests

In the [pager-owned format](../design/owned-leaf-manifest-v1.md), the system root
itself owns the canonical inventory. Its retention follows the existing exact
index generation, both selectable durable slots, visible/queued roots, recovery
handoff and held-view pins. Intrinsic metadata is not an external
`ResourceOuterLeafManifest` file token; real leaf/value-log dependencies retain
their existing resource closure and stable-deletion authority.

A whole owned-mode leaf GC validates each distinct intrinsic object against one
pinned recoverable-root basis. Publication, relocation or epoch changes
invalidate that basis. Every destructive allocator step freshly fences the
root/index identity, publication, snapshot admission, held FD identity and
opaque page-reuse capability. A scheduling cursor, old digest or counter cannot
replace those checks. Stale captures defer or return the existing stale error;
failures retain existing recovery-required and quarantine semantics.

The format relies on TreeDB's exclusive index writer and immutable COW pages.
Visited content receives integrity validation; unsupported external writes to
unvisited unrelated index pages have no immediate whole-inventory detection
guarantee. The standalone layout retains its whole-inventory corruption,
namespace/rebind, exact child-handle, pin and directory-sync safeguards.

### Transaction-private allocator preparation (#5105 and #5108)

The allocator uses the canonical typed 16-way Patricia topology described in
[storage format](storage-format.md#freelist-patricia-v2-active-durable-root-allocator-format).
Its sole owning edge is a value reference to either a branch or a 256-entry
chunk. A branch records the first differing nibble and has at least two
children; deletion immediately collapses unary branches and removes empty
chunks. A single nonempty chunk is the root directly. The only empty physical
state is the exact generation-root sentinel; empty or unary descendants do not
remain as history.

Before private edits, a prepared allocator transaction detaches unmaterialized
aliases reachable before an immutable boundary from its rollback transaction.
The first private copy of a durable branch isolates each dirty immediate-child
subtree through the next durable boundary; later durable-child first copies
repeat that rule. A copied branch retains its fixed child references exactly
once. Only these isolation boundaries permit in-place editing; zero page
identity alone is insufficient. Materialized objects remain immutable. Creating
a persistent transaction clone first revokes the original's private permission;
the private clone re-enables it after its own detachment. No owner token or
identity table enters an immutable generation or retained page view.

Existing allocator admission precedes isolation and allocation. For C nonempty
chunks, a canonical tree has at most C-1 branches because every branch has at
least two children. The selected allocation-class geometry is 352 bytes per
branch and 2304 bytes per chunk. State backing is therefore at most
2304*C + 352*(C-1) bytes for C>0; a materialized empty root uses one 2304-byte
chunk sentinel. A valid high-water H bounds C by ceil(H/256). Two complete rollback
and private representations require at most twice the corresponding state
bound, while shared immutable backing may reduce actual overlap. This bound
excludes separately admitted creator receipts, sets, maps, slices, reservation
and metadata output, and other retained generations and candidates. All births,
boundary copies and isolation visits remain charged; each whole branch or
chunk class is prepaid before birth, and the original creator retains its
backing charge until the final owning reference releases it.

The current transaction and generation raw sizes are 368 and 320 bytes,
respectively, with selected allocation classes 384 and 320. These are explicit
layout expectations rather than a padding-based incremental charge claim;
pinned Linux allocation-class witnesses remain required. Existing input/output
caps, publisher reserves and defaults remain unchanged. Full caller byte
preflight remains unproved: the state-overlap bound does not account for all
simultaneous trees, sets, slices, plans, creator receipts and retained
candidates, and cannot establish complete reservation sufficiency.

Materialization consumes the transaction and clears private permission before
sink callbacks on success or partial-write failure; validation-error returns
also revoke permission. Abort restores the original transaction and preserves
conservative reservation burns. Dirty state emits one page per actual chunk or
canonical branch, with one root sentinel for empty state, plus the actual
coalesced reservation chain and generation header. Reservation sizing must
match those emitted pages and normalized extents, including skipped abandoned
append ranges at a reservation-page boundary.

Visible and durable-seal generations remain separate generations with their
existing publication authority and opaque root, pin and recovery horizons.
Their allocator and owned-manifest work joins the existing finalization path;
this topology change does not add a separate publication round. New canonical
physical page counts, identities, CRCs and bytes are verified independently of
the historical V1 identical-byte evidence. Logical free/retired contents,
rollback and recovery behavior, slot-overwrite retirement, and the existing
horizon checks remain the equivalence contract. Full visible/seal caller
emission, creator-class and fixed-capacity acceptance remains open until the
actual production-path witnesses pass. The accepted bounded
parent-lifetime correction additionally protects overwritten root records
through any surviving fixed slot's immediate-parent reference, while manifest
pages retain their own commit horizon. Direct preparation and queued seal use
one fixed-slot validation and inventory-partition helper. The physical metadata
reuse fixtures now retain 88 pages in the steady case and 37 in the multipage
case (previously 86 and 34); earlier physical inventories and fit forecasts do
not apply to this candidate. These fixture counts are not general plateau
bounds. The [accounting contract](allocator-patricia-v2-accounting.md) records
all additional caller/backing obligations and preserves the 128 MiB gate.

Credited transaction origin survives epoch completion in the existing bool
padding. Future births require both the synchronously borrowed current request
and the exact newly admitted mutable resident creator edge. Ending an epoch
revokes the fixed live, rollback, and preborn activation roles together; ordinary
uncredited wrappers retain their behavior. Re-admission loans the real existing
closure to the new request and creates only missing mutable edges, preserving
all intrinsic historical creators. This origin bit is expected to leave the
368-byte transaction geometry and 384-byte Linux class unchanged; actual
compiled class and whole-caller fit remain required before finite activation.
