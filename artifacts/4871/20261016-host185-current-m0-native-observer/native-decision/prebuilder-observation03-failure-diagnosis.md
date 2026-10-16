# Actual33 observation03 failure: canonical parent placement mismatch

The source contains a native prerequisite defect: the final basis compares the capsule's relocated embedded-parent record directly with the base capsule's current record. Those records necessarily have different physical `UserRootPageID` values, even when they describe the SAME unchanged parent. The resulting pre-effect refusal protects authority, but it is a false stale diagnosis. A sibling authority gate repeats the error and must be repaired in the same batch. No source change or runtime command was performed for this diagnosis.

## Evidence and causal path

Frozen source03 manifest is `9e3596eb05d4222ba4acf747a4045452efb8210a55a74201f5923da1fef0ad88`; archive is `a2e7deb7559d66138abe6eaed5cc9d0760498469b9a1fc2c4300f7a641530c12`. All sixteen inspected source paths match the frozen hashes; their identities and raw evidence hashes are in the companion JSON.

The retained run log records calls159, phase26, `durable-root candidate base changed`, test failure0.18s. The full Go command took1.721s, exit1, with unchanged source and a joined/empty process group. No `native-observer` row was printed. This is failed safety-control/observation evidence, not an observed F or retained total.

The original promoted fixture opens/reopens the original33 history, disables background checkpointing and uses unchanged Q32/1MiB. In promoted mode it makes no newer foreground write until after the expected refusal and observer (`TreeDB/mvcc/native_prune_backend_route_v6_test.go:30–81,158–176`). The unexpected error fails at160; the observer helper is first called at176. The changed ForTest observation hooks and OBS1 scalar expression therefore do not execute before this failure. There is no demonstrated fixture misuse or observer-induced mutation. Parent cancellation/stale modes use their own explicit foreground writes; the executed command selected only the promoted test.

The original phase26 production route is candidate preparation -> seal preparation -> private coordinator binding -> `FenceNativeMVCCPreparedV6` (`TreeDB/mvcc/prune_cursor_v6.go:581–608`). The final driver validates the seal installation before Cut, composite allowance, builder acquisition or enqueue (`TreeDB/db/native_mvcc_final_publication_v6.go:46–77`). Its full predicate at `native_mvcc_final_basis_v6.go:92` includes:

```
seal.primary.promotion.View.Parent().Record != base.record
```

The canonical constructor targets `1-parent.Slot()` (`internal/rootpublication/primary_transaction_construction_v6.go:332`). Its SAME codec writes current at capsulepage+1 and embedded parent at capsulepage+2 (`internal/rootpublication/primary_capsule_v6.go:124,128`). `encodeCapsuleOperandV6` changes `UserRootPageID` to that directory page and zeroes the three external ancestry fields at166–169. All other record fields are preserved. This is intentional encoding, not stale ownership.

| Existing base slot | Base current UserRootPageID | New target | Embedded parent UserRootPageID |
|---|---:|---:|---:|
| 0 | 3 | 1 | 7 |
| 1 | 6 | 0 | 4 |

Thus the quoted raw equality always fails for a valid opposite-slot promotion. The function returns the exact observed error at97. Raw logs do not print the individual predicate values or final candidate/seal phases, so they alone cannot rule out every earlier stale return. The source nevertheless proves this unconditional gate blocker; fixing it does not predict that a later run cannot expose another problem.

## Required coherent sibling audit

`TreeDB/db/primary_authority_scope_v6.go:73` repeats the same raw `View.Parent().Record != db.durableRoot.record` comparison. It would later return `ErrDurableRootOwnership` after activation and index preparation, before the authority write. It was not reached by this failed run. Leaving it unchanged would move the same defect past adoption and is not a coherent repair.

The existing current-record normalization at `native_mvcc_seal_preparation_v6.go:555–558` is correct: expected current uses target capsulepage+1 and zero ancestry fields before full equality at559. The parent must use target capsulepage+2. No current-record check should be weakened or switched to the parent location.

## Smallest coherent repair proposal — not implemented

Add one narrow same-package pure canonical parent-record projection/comparison and reuse it in BOTH final-basis and authority gates. Start from the complete saved/current base record and change only the four format-defined fields:

```
UserRootPageID = PrimaryCapsulePageV6(target) + 2
ParentRecordPageID = 0
ParentCommitSeq = 0
ParentRecordDigest = [32]byte{}
```

Require target0/1 and preserve full equality of the resulting record. Do not compare only commit sequence or omit any other field. Preserve all existing saved base slot/meta/full record, generation-tagged parent Ref, exact current root/certificate/transaction, active seal, prefix, sequence and capacity checks. The current-record projection remains target+1. No codec change, owner, registry, private authority or fallback is needed.

Admit the real fixed local copy/initialization/complete comparison traffic before it executes. Audit both actual gate reservations and their source-bound F/observer pricing together; any added record/byte work must change the expressions honestly. This report grants no numeric allowance and does not assume F281 survives new operations unchanged. Unsupported budget must still refuse before first effect. All public caps and error distinctions remain.

## Focused proposed validation — all UNEXECUTED

A new pure `TestNativeCapsuleParentRecordProjectionV6` should use the original capsule codec and SAME helper to cover both slot directions; raw inequality versus projected full equality; target0/1 and invalid target; wrong physical parent placement; all other record fields mismatched one at a time (including ancestry bytes in the actual encoded record); current-versus-parent +1/+2 confusion; unchanged source operands after pure comparison. Preserve exact full-record comparisons rather than selected-field assertions. The SAME helper's use in both gates is part of source review; where synthetic gate controls are added, assert no adoption/authority effect and exact expected failure.

With pinned Go1.26.3, GOMAXPROCS2, task-local paths, full source/archive/compiler/PID/PGID/subreaper/join receipts and outer bound, propose these after root's source/runtime selection:

```
go test -p=1 ./TreeDB/db ./TreeDB/internal/rootpublication -run '^(TestNativeCapsuleParentRecordProjectionV6|TestPrimaryCapsuleV6CompleteIndependentSlots)$' -count=1 -v -timeout=3m
go test -race -p=1 ./TreeDB/db ./TreeDB/internal/rootpublication -run '^(TestNativeCapsuleParentRecordProjectionV6|TestPrimaryCapsuleV6CompleteIndependentSlots)$' -count=1 -v -timeout=3m
```

Then separately grant the original promoted observation and both-slot stale/cancel controls on the exact reviewed successor, without changing fixture errors or limits:

```
go test -p=1 ./TreeDB/mvcc -run '^(TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6|TestNativeMVCCCopiedParentAcrossBothDurableSlotsV6)$' -count=1 -v -timeout=3m
```

Those existing controls must still reach the original pre-effect refusal with no ACK; preserve newer durable writer/stale refusal/cancellation/old-reader/reopen checks. Do not reinterpret the observed stale error as the expected missing-work error. No native publication, fence, Consume, Finish, ACK, cap increase, benchmark or performance qualification is proposed here.

Read-only diagnosis lease released. Original Q32 RED/ACK0.
