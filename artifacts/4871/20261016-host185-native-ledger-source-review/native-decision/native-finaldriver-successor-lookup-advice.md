# Selected successor root-table lookup bound

Disposition: **source-derived conservative bound available for the named single-successor fixture**. This is design advice, not a numeric work allowance, execution grant, or native publication acceptance. Reviewer: independent internal subagent, explicitly routed gpt-6.1-sol/high; candidate edits: false. No Go/runtime or mutable source inspection.

## Bound and exact scalar

Take `N0 = bank.PagerTable.TableNodeCount` from a successful `NativeMVCCCurrentDeleteV6.MaintenanceNativeBankGeometryForTestV6(role, w)` observation **after the native candidate/root/seal preparation is ready and before Fence/ordinary successor publication**. This forwards through `Arena.MaintenanceNativeBankGeometryForTestV6` to `Pager.MaintenanceNativeReadRootTableGeometryForTestV6`; that last function reads `uint64(t.count)` under the SAME table RLock (`pager/primary_read_root_v6.go:374-380`). Require `ready`, valid bank, applicable table, exact owner/image matches and the SAME retained source pager used by the subsequent lookup. Use a single successful same-pager row; do not sum source/parent/member aliases or sample different times and call them atomic.

For the specified fresh-key `Store.CommitAt(34, ..., CommitDurable)` and no other root producers:

```
A = optimisticWriteMaxAttempts + 1 = 3 + 1 = 4
N(t) <= N0 + A = N0 + 4
L_non_nil <= N0 + 4
L_all = N0 + 5    // includes one final nil-link inspection for an absent root
```

`L_all` is the conservative per-lookup link-reservation bound, not the number of live root references. A successful present lookup needs no terminal nil inspection and is bounded by `N0+4`. All additions/byte multiplications must use checked unsigned arithmetic. The actual next frozen observation must supply N0; this advice does not infer its value from timestamp 33, old observer totals, or timing.

## Why count covers every traversable node

`primaryReadRootTableV6` contains bucket heads of actual linked `primaryReadRootEntryV6` objects; it has no tombstone array or separate inactive-node list (`primary_read_root_v6.go:18-40`). Both ordinary and finite constructors link exactly one node and increment `t.count` once (`:132-133`, `:187-188`). Nodes already installed but not yet sealed/registered count too; sealing merely fills the existing node's image (`:239-276`). Held, queued, failed-cleanup or zero-ref-but-not-yet-unlinked nodes remain included. Refcount changes do not decrement this counter.

Removal unlinks the node, decrements count, then scrubs its aliases under the same table lock (`:291-324`). Whole-table Close also unlinks one node and decrements count per iteration (`primary_read_root_close_work.go:11-35`). There are no reachable tombstones excluded from count. Ordinary rehash relinks existing nodes without adding nodes (`primary_read_root_v6.go:107-130`). Thus every bucket-chain length is <= count at the actual lookup, including nodes that are not current logical roots.

## Real route and maximum additions

1. The named fixture explicitly selects ProfileNoWALFast, outer leaves false, pointer threshold 2048, background checkpoint interval -1, seeds/checkpoints/closes and reopens before the native cursor (`mvcc/native_prune_backend_route_v6_test.go:30-61`). The original reopen yields no pending foreground cache mutations. Native preparation retains private controls, not a cache key. The proposed successful Fence has completed publication before the single ordinary write, and the genuine Finish is paused; it performs no additional directory construction while paused. The ready-boundary count must already include the prepared native directory.
2. One Mutation is encoded as one physical MVCC point record (the value is one tag plus `durable`), and CommitGroupAt dispatches the admitted singleton through `SetPointWithMVCCInput(..., true, ...)` (`mvcc.go:169-281`). Public NoWALFast is non-command-WAL; its non-COW route delegates to cached admission (`public.go:2124-2161`, `profiles.go:258-263`). Cached insertion affects one shard/key; explicit sync invokes Checkpoint (`caching/mvcc_admission.go:15-21`, `caching/db.go:26984-27017,27135-27196`). The tiny value is inline at threshold 2048, and outer-leaf vlog output is disabled; no pointer-generation producer is introduced.
3. The fresh cache has only this one point op. Mutable rotation/frontier capture plus serialized lane claims cannot create a second distinct nonempty unit for that same op (`caching/db.go:25704-25804,29481-29548`). Canonical chunking covers the point-op interval; a one-op run has only one nonempty chunk, hence one backend batch write; successful units are removed from the queue (`caching/flush_run_planner.go:870-929,953-1049,1361-1367`). The streamed alternative also emits at most one nonempty chunk for one source op (`:1053-1187`). The one-chunk path does not start a chunked RootPublicationBuildGroup (`:33-41,1005-1025`). Dirty cached-root publication updates cache snapshots, not the pager root constructor (`caching/root_domain.go:695-710`).
4. That backend batch allows three optimistic attempts and one serialized fallback (`db/batch.go:27,350-365`). Each attempt calls Zipper Apply at most once (`:404-429,723-747`). Counting all four attempts is deliberately conservative even if no competing writer makes the first attempt sufficient. Errors stop the path; this bound does not require aborted table nodes to have been reclaimed immediately.
5. On the PRIMARY root, ApplyWithOptions disables span-native execution/parallel apply before its single canonical apply (`zipper/read_only_prepare.go:1043-1061,1145`); applyWithConfig dispatches to applyPrimaryDirectory (`zipper/zipper.go:1877-1901`). Each applyPrimaryDirectory calls the original constructor once (`zipper/primary_directory.go:352-378`). The constructor calls `PrepareConstructedReadRootV6` once (`internal/rootpublication/primary_transaction_construction_v6.go:49-86`), and the ordinary arena route installs one pager node (`internal/primaryarena/read_root_v6.go:96-146`). The subsequent directory write reuses that supplied constructor (`zipper/primary_directory.go:640-659`). Component claims are in the component-bank namespace and do not install RAM read-root table nodes. Materialized fallback sets `materializedBase=true`, avoiding recursive PRIMARY-directory construction, and returns through the same supplied constructor (`primary_directory.go:425-466`).
6. Durable capsule promotion acquires existing parent/root edges and constructs an image; it does not allocate another RAM read-root entry (`db/durable_primary_runtime_v5.go:509-619`, `db/primary_capsule_runtime_v6.go:179-233`, transaction promotion). The independent copyCapsuleReadImageV6 root constructor is used by capsule recovery, not this commit (`db/primary_capsule_recovery_v6.go:243,253`). Backend checkpoint waits the current root-publication prefix, without another Zipper Apply (`db/db.go:4829-4910`).
7. OuterLeaves=false makes checkpoint-time sparse vacuum potentially relevant, so it must not be ignored. This reopened fixture's first explicit cached checkpoint has `checkpointRuns+1 == 1`, and the source skips sparse vacuum until the interval of 8 (`caching/db.go:25237,25259-25291`). Require that counter precondition at the proposed boundary (no intervening cached checkpoints). No background/manual vacuum, CompactIndex or reopen is permitted during this interval. BackgroundCheckpointInterval=-1 alone is not a universal exclusion of all maintenance.

Consequently there are at most four newly linked entries before native Finish/Close completes. Release and failed-attempt cleanup can only reduce the count. A changed pager, extra writer/checkpoint workload, fault-injected replay/retry outside this one commit, recovery, or maintenance invalidates the selected-route proof and must refuse this diagnostic shape rather than silently reuse L.

## Exact lookup traffic and sentinel

Let `T=sizeof(primaryReadRootTableV6)`, `E=sizeof(primaryReadRootEntryV6)`, `P=sizeof(*link)` (the pointer value). `PrimaryReadRootOwnerWithWorkV6` reserves one head/table operation `(1,T)`, then reserves `(1,P+E)` **before every link inspection**, including the terminal nil (`primary_read_root_v6.go:205-226`). Therefore:

```
Lookup_max = (1 + L_all, T + L_all*(P + E))
```

The lookup holds the table RLock across that traversal; the count bound is about the table geometry at each traversal, not a lock kept across I/O. With an empty head array it returns after the head reservation. `removePrimaryReadRootV6` has a distinct prefix `(1,128)` and `(1,P+2E)` per visited link; its conservative bound is `(1+L_all,128+L_all*(P+2E))` (`:291-324`). Use the right expression for the actual caller and add the replayed Arena/transaction/Close prefixes identified in the prior unit-envelope report. These expressions do not establish a complete G or a complete Finish total.

## Remaining inputs and limits

No missing per-commit constructor cardinality remains for this precise route: four is a checked conservative maximum. Still missing for an executable selected grant are the new frozen scalar N0 and verified route/exclusion predicates above, as well as the other independent envelope/retention inputs. If the intended diagnostic cannot establish the clean-cache/one-op/no-extra-producer/first-checkpoint predicates, the first unresolved input is the number of additional nonempty backend apply attempts (and any maintenance RAM-root constructors); N0 or head capacity alone cannot bound arbitrary future writers. The table's load-factor relation is not a constant bucket-chain bound and does not replace this route proof.

This report does not select a numeric budget, approve final-driver execution, claim Close convergence as a whole, change Q32/1MiB or ACK0, or accept native publication.
