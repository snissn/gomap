# Native finaldriver bootstrap root-table bound

The selected native prefix can add **at most one RAM read-root table node**. For the fresh original cursor, B1/one original frame and its first genuine ready/Fence boundary:

```
Nready_hat = checked_add(Npre, 1)
Nafter_successor <= checked_add(Npre, 5)
L_with_terminal_nil <= checked_add(Npre, 6)
```

The latter two expressions compose the unchanged prior proof for ONE selected `Store.CommitAt(34, fresh key, "durable", CommitDurable)` (+4 nodes) and the one terminal nil link. They are not a general writer bound or a numeric work grant.

## Exact counting basis

`Npre` is the SAME pager's `immutableReadRoots.count`, sampled after the fixture's seed/checkpoint/reopen and before this fresh cursor. `MaintenanceNativeReadRootTableGeometryForTestV6` reads that scalar under the table read lock (`TreeDB/pager/primary_read_root_v6.go:373-380`). It counts every linked entry, including entries without a published image and failed/abandoned roots still linked. The table has no separately excluded tombstone class. Ref acquisitions do not increment it; removal unlinks and decrements it (291-324). Rehash relinks existing entries without creating additional entries (60-134).

## Why retries do not add more nodes

1. `StepNativeMVCCCurrentDeleteV6` creates its transaction only while `c.transaction == nil` (320-322), retains the concrete returned transaction **before** inspecting `ready/error` (323-334), and otherwise resumes that same transaction (330-331). The native constructor returns that original transaction even after a partial refusal (`primary_transaction_construction_v6.go:73-79`). A refusal before allocating a transaction has installed no root node.
2. `PrepareNativeMVCCConstructionRootV6` returns immediately once the original `construction.root` is nonzero (93-113). The arena delegates to the original pager constructor and, after success, only advances its serial and returns the reference (`primaryarena/read_root_v6.go:102-148`). The transaction stores that returned reference without a further fallible step.
3. The pager constructor performs capability, capacity, duplicate lookup, overflow and aggregate admission checks **before** installing a node (`primary_read_root_v6.go:149-182`). The remaining constructor/install tail is nonfallible: one owner, one entry link, one `t.count++`, then `true,nil` (183-189). Thus there is no installed-but-unreported node that a later retry could duplicate. Pre-install failed attempts add zero; an installed but abandoned node remains the same one counted node until genuine removal.

The existing native capacity guard is `t.count < 2*len(t.heads)` before insertion (159). A successful insertion may make count equal that threshold. This capability refusal does not add another node or permit silently substituting a capacity bound for actual count.

## Sibling route closure

- Root preparation borrows/acquires the **existing** current root and uses `transaction.PrimaryConstructionReferenceV6()` for the new root (`db/native_mvcc_root_preparation_v6.go:79-100`); sealing fills that same construction root (153-159).
- Promotion storage is an image/promotion allocation, not another RAM root. Promotion custody acquires the same constructed root and parent (`primary_transaction_construction_v6.go:275-315,332-397`). Component/group refs and Set clones do not register RAM roots.
- Materialized output uses the captured backend snapshot/input and `NewMaterializedWriteWork` to create DATA pages (`db/native_mvcc_materialized_write_v6.go:112-164`, `zipper/materialized_write_work.go:92-115,175+`). This selected route does not call the ordinary PRIMARY directory constructor.
- Initial cached capture explicitly requires empty mutable/queue/publish state and acquires a snapshot; it does not flush/checkpoint (`caching/native_mvcc_prune_v6.go:86-90,116-180`). The embedded cursor preparation is reused through phases 15/16/24/25/26 (`mvcc/prune_cursor_v6.go:522-598`).
- `RegisterPrimaryReadRootV6` populates an existing entry's image rather than allocating a node (`pager/primary_read_root_v6.go:239-272`). Recovery's `copyCapsuleReadImageV6` does create a node (`db/primary_capsule_runtime_v6.go:117-122`), so all initial recovery must precede Npre; no later reopen is included.

These observations cover the selected first-ready prefix only. A later batch, replacement cursor, extra ordinary publisher, maintenance/recovery producer or second preparation is outside this +1 proof. Cancellation/cleanup may retain the one node or remove it; it cannot increase this prefix's node maximum.

## Disposition and binding

**SOURCE_BOUND_AVAILABLE_FOR_SELECTED_PREFIX.** Use the checked conservative Npre+1 bootstrap, then verify actual ready table count does not exceed it. Preserve all original refusal/custody assertions. No Go command, runtime, source change, Git/GitHub operation, numeric allowance, full G/Finish proof, native publication or Q32-fit acceptance was performed or granted.

Source manifest: `5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d` (10,389 files); archive: `0bcaee68f1b6a058bd358b5096265771d35ba503143ac9f8e255b2c215fad8f4`; tree: `e523999e97e0af545da0072359cd1ac2fc57b5eb`. Immutable tar members were read; mutable integrated source was not inspected. The paired JSON records each inspected source hash and full selection/current-plan/prior-successor advice hashes.

Reviewer: `/root/ordinary_review`, explicitly routed `gpt-6.1-sol/high`, internal read-only advice, no children, candidate edits false. Hosted manual review cannot pin the required model and was not used.
