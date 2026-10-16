# Core registry/API source review: REJECT

Independent internal review by `/root/ordinary_review`, explicitly routed **gpt-6.1-sol/high**. Candidate edits by reviewer: **false**. Hosted manual review cannot pin the required exact model.

Bound to handoff `6bf01e024e0d225d155f9547e1b2c48019e512b93e0a5456b5ad83c9947eb4c3`, owned manifest `e27c30ac4113aa2ffdae6d746afbff75375d1134227b6f276bfd602a27e0069c`, archive `884fdfb148166a67ea765f044db67abfbaf530f353efd6a3cfc6b45f7d23a104`, and patch `222b897ecb46b2312f1566e384a092805dced7f797fb802bc6e0ec8d2176a34c`. The immutable owned partition contains 501 files, with 120 changes: 31 production and 89 tests. The accompanying JSON records individual production and selected dependency/test hashes.

## Blocking finding C1: containing leaf is refunded before its final use

In `TreeDB/lifecycle/registry.go:243–270`, `readerLeaf.refund` uses a `RemovalWork` embedded in the independently admitted leaf to return a charge that can include that same leaf. `ContinueOriginal` at line 259 reaches `retainedalloc/owner_work.go:277–279`, debits the owner, then completes governing shrink at line 284. On success it still writes the embedded receipt at lines 286–287 and returns through the embedded mutex unlock (`removeStep`, lines 246–248). The caller then writes `l.debt = 0` at `registry.go:269`.

A successful positive-handle ordinary `Unregister` detaches the leaf and passes its full containing charge to this path (`registry.go:374–377`). Full speculative refund after a successful locked fast recheck (`used == 0`, lines 205–229) and first-hole failure cleanup (lines 208–212) have the same issue. The new containing admission ends before the actual last receipt/leaf access. This is an accounting/lifetime violation; it is not a Go memory use-after-free claim.

The partial unused-branch refund with a still-admitted registered leaf is different. A failed governing shrink also retains the SAME leaf/error and governing lease debt; I do **not** claim that failed cleanup loses custody. The distinct DATA detach path already moves accounting-only debt into an original surviving parent.

Required correction: use an original surviving caller or an appropriate same-owner last-access teardown seam for full-leaf completion. Keep the leaf and embedded receipt admitted through their final use and retain the original failed-cleanup receipt if refund fails. Removing only the final debt write is insufficient because the embedded receipt/mutex also outlives the shrink. Add a control that detects this interval; existing positive/peak/end-total controls cannot.

## Coverage and checks

Verified all handoff artifact hashes, the full owned archive's 501 hashes/modes, and the exact 120-path delta against immutable source04. Read every production delta, full registry/unregister/generation/DATA release source, affected checked snapshot/publication/recovery/Close/build-group callers, both new core control files, and selected existing test deltas. Existing benchmark timer directives remain unchanged. The proposed top-name inventories contain 16 lifecycle and 24 DB tests per normal/race invocation; no commands were executed.

Otherwise reviewed without an additional source blocker: fixed-width Patricia lookup/insertion/deletion and summary maintenance; fast first/last cohort transitions; maximum leaf/branch preadmission outside registry lock; same metadata-owner header/node enrollment; physical completion plus last-wrapper generation retirement; DATA same-owner debt transfer; truthful checked API errors; Snapshot/recoverable-root failure custody; and final build-group failed unregister retention after lock release. New controls meaningfully distinguish arbitrary sequence/duplicate/handle reuse, callback lock order, actual capacity and closed-binding failures, failed speculation, completion owner matching, and header last-wrapper lifetime. This does not establish bounded lock/CAS contention or ordinary owner-binding traversal.

No Go/compiler/test/benchmark/runtime or Git/GitHub mutation was performed. No integrated compile/race/CI/performance, native publication, Q32 fit, numeric allowance, or full graph acceptance follows. Prior outside-partition source reviews remain separately attributed. Source readiness is **REJECT pending C1**; the review lease is released.
