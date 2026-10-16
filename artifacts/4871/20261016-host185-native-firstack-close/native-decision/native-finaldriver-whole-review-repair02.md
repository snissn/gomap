# Finaldriver fixture repair02

**ACCEPT, source-only selected diagnostic readiness.** F2 is resolved by an exact one-file fixture correction; no production or formula change is involved. Independent gpt-6.1-sol/high; no children, source edits, Go/runtime or Git/GitHub operations.

Independently verified full10,431-file archive against manifest `a7f05aa0a824de9f37664f1504636453133ad9b4d6d14a86826abeedd6bded2a`, archive `aa282fb7a4892023781dd9862a9f2000e92cccacc36edf42329cd0106a4f0dd9`, patch `fa54ed9e6b06fdbdf1b002cb46066099a8dd09576f9f9c0b670533bef35e6315`. Complete manifest comparison against30e0 proves only native_finaldriver_diagnostic_v6_test.go changed (7bc58df8→cf8018fd); all10,430 others match.

F2: original diagnostic line82 required COWMode through `!cache.COWMode`. The actual observer defines that flag as `db.cow != nil` (caching/native_mvcc_prune_v6.go403), while original capture rejects precisely that state (129–130). The observed `{false false false 0 4}` was compatible with the intended original non-COW capture route; the test rejected it before constructing G or a cursor. This fixture contradiction was missed by prior source review and is now explicitly recorded rather than dismissed by its historical acceptance.

The successor changes only the predicate to reject COWMode and adds its source-authority comment. Readiness/Closing guards, production options, original empty-frontier proof and all actual caller/Finish/Close assertions remain. It does not force an unsupported production configuration or manufacture a capture.

Inspected the failed first-arm raw and result: exit1, no G/cursor, no signals, joined empty groups, frozen source before/after. This is not a successful publication result. All previous production/formula/custody/envelope review coverage composes by byte identity, including repair01. Worker-reported focused-pass reuse is not independently accepted by this narrow task.

No successor runtime is claimed or authorized; no numeric allowance/G evaluation, Q32 fit, native publication/Finish/ACK/Close/reopen or parent qualification follows. Q32 remains RED/ACK0. Exact-source real runtime still requires separate root selection. Lease released.
