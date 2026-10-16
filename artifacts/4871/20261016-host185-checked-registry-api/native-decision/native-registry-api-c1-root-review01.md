# C1 root reset seam: ACCEPT

Independent source-only review by `/root/ordinary_review`, explicitly routed **gpt-6.1-sol/high**; candidate edits: false. Hosted manual review cannot pin the required exact model.

Bound to freeze `ca5ceb03e0b8d7b662568e7edb23838f20c125ecce8ee7d4b38a7a09f476291a`, source archive `96a9813cd0225fa5978efbd40ae44a84c4f4216b5c19c4da58faab9bf4ad2f5a`, and patch `80246cfa4a356445b67647f2ffeaba9cdc972c03293eab32c6e76c9598dc939c`. Verified every declared artifact hash, exactly two archived source members, and current hashes of these two owned paths only. The production preimage matches the immutable original archive; the production diff was reproduced. No mutable db/lifecycle source was inspected.

`ResetCompletedOriginal` (`owner_work.go:362`) locks the existing receipt mutex, rejects native receipts before inspecting completion, rejects unfinished receipts, and clears completed original fields without copying/resetting the mutex. The factored field clearing matches the original bounded reset: its completion/unused checks and one-record/twice-receipt Reserve remain in place, with no effect on refusal. `shrinkWork` contains no separate mutex. Original `done` is reached only after governing shrink completion, so failed/unfinished removal cannot use this reset to discard original debt or first cause.

Read all 140 lines of added controls and the actual original Bind/Continue/remove/shrink/reset and budget/lease behavior. The tests discriminate exact repeated deltas and replay, refusal before completion, sticky failed shrink with retained original governor, and refusal to reset both unfinished and completed native receipts through ordinary policy. They do not claim runtime results; failed-shrink coverage deliberately preserves the unresolved graph.

No source blocker in this two-file seam. Caller lifetime remains a prerequisite: the receipt must live in an admitted surviving parent and disposed-child aliases must end before completion/reset. This helper alone does **not** establish that the pending db/lifecycle repair resolves C1.

No compiler/Go/test/runtime, source edits, or Git/GitHub mutations. No combined-source, CI, performance, native-publication, Q32 or numeric allowance claim. Review lease released.
