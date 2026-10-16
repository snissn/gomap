# COW integration follow-on: source preparation

Root recovered the two exact provisional #5047 tests from commit `72d7dcda2ad4922157e61ba8229f283c106579e9` and refreshed the live issue. `source-preparation-receipt.json` binds both blobs and ten inspected implementation files to immutable minimum-source01 manifest `b9d1baa055e9b821106d09cae9f562537610c40e75e2596efd9a18eda4759c91`. No product source, branch, index or test runtime changed. This preparation does not open a competing writer or an integration PR.

The preserved tests characterize clean-close/checkpoint and acknowledged process-exit WAL recovery. In particular, the WAL test deliberately expects COW prune refusal before loading the floor, then checks that rejection leaves WAL/publication/capture counters unchanged. It is not a positive native-prune test. Future integration must reconcile this explicitly with the supported public route; deleting the refusal oracle alone supplies no replacement coverage. The original read/floor/group/tombstone/pointer/reopen assertions remain useful.

## Current source boundary

| Existing seam | What it provides | Missing native connection |
| --- | --- | --- |
| `mvcc/versions.go:PruneVersions` | Work limits/cursor select `pruneNativeV6` | The selected production path still needs the COW source |
| `caching/native_mvcc_prune_v6.go:StepNativeMVCCCaptureV6` | Bounded empty adaptive-cache frontier capture plus original backend Snapshot | Explicitly rejects `db.cow != nil` and nonempty overlays before backend birth |
| `db/native_mvcc_current_delete_v6.go:NativeMVCCSelectionSourceV6` | An original composite producer can supply and validate its stationary selected record | Inspected source has the interface/bind method, with no COW implementation/caller |
| `caching/cow_cut.go:cowReadCut` | One original immutable shards/frozen/backend-basis cut, refs, budget and physical custody | Native capture must retain this same cut, with actual bounded close/retirement |
| `internal/memtable/cow_traversal.go` | Maintained charged resumable immutable traversal, original view and close receipt | Composite precedence/merge and original DATA projection must share one selection/custody contract |
| `caching/cow_rollover.go` | Original frozen-prefix/successor ownership transfer | Current prepare/install/drain is synchronous; it is not already a native work continuation |
| `caching/cow_flush.go` | Exact accepted frozen-prefix coverage, original BuildGroup, successor-basis handoff preserving newer sources | Current flush/refresh/retirement cannot be invoked as an uncharged bounded-prune child |
| `internal/memtable/cow_tree.go:COWWriter.Prepare` | Existing private COW writer and once-only pending publication | A pending original prepare excludes a later prepare; holding it across public yields does not prove continuing-write progress |

`COWTraversal.CloseStep` returns an original retirement; its documentation explicitly excludes physical drain. `COWRetirement.Drain`, `cowReadCut.drain` and the current basis handoff still call synchronous original cleanup. Their presence is not a complete finite native Finish proof. These findings preserve the already selected existing-authority design; they do not select a second publisher, registry, WAL or reclamation mechanism.

## Next dependency boundary

Complete and review the active #4878 ledger/minimum and general native work contract first. Then reconcile the exact provisional tests and source map with that usable contract. The first COW production control must use a nonempty real command-WAL COW cut, retain an old iterator, interleave legal newer durable grouped/point writes, and require actual selected deletion, same-authority publication, full original Finish before ACK and floor/tombstone/pointer/reopen behavior. Its allocation/work and continuing-write proof must include the captured cache sources; a backend-only certificate or synchronous checkpoint does not supply it.

No COW positive activation, Q32 fit, all19, performance, native physical retirement, #5047 completion or Dgraph activation is claimed. This read-only preparation removes source-discovery work from the next stage while the native implementation remains under one writer.
