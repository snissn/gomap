# Origin repair: metadata and stopped-control addendum

**REJECT control acceptance for frozen source e434542a; metadata ACCEPT.** The independently accepted product-source review remains preserved, unchanged, at `origin-repair-independent-review.md/json`. The observed failure establishes a stale synthetic test boundary; it does not establish a production custody regression. Source acceptance does not make the failed control packet acceptable.

Reviewer: `/root/ordinary_review`, actual explicitly routed **gpt-6.1-sol/high**, INTERNAL_SUBAGENT_REVIEW. Hosted manual review cannot pin the required exact model and was not requested. No construction role, candidate edits, child agents, Go execution, Git/GitHub mutation, or runtime campaign by reviewer. This is an additive exact-source/control review, not native publication qualification.

## Exact binding and metadata

Independent read-only hashing verified all **10,375** final archive members, live source paths, and staged Git blobs against the final manifest; all agree. Relative to the accepted 787445cb manifest, the only changed path is `.github/ci/ci_impact.json`, and its only changed JSON field is `discovery_source_sha256`. No paths were deleted. The final policy hash is `2f6227e5a3f0fa3016d06b6f010ab982e22c86e0d6a859a85fafb6e7cf5672b9`.

The source-contract check and all four retained Python suites exit zero; their raw log hashes match receipts (21+15+6+16 = **58 tests**). The inventory refresh also exits zero. No duplicate checks were run. The staged count is **202**. The retained initial 201-count assertion was an incorrect coordinator expectation: predecessor 198 union the 24-path repair adds four staged paths (three new files plus previously ordinary-owned `release_queue_test.go`). It was not a Go/product-test failure or a source mutation. Final index bytes match the final archive.

## Authorized and observed controls

The root selection binds **19 commands**, ordered ten normal then nine race, **66 normal / 63 race** top-level names and **34 source-bound test files**. Independent hashes agree with all 34 current files. Actual argv for both executed commands matches the authoritative selection. The old proposal's stale top-name/status fields are superseded by this root selection; no result is inferred from them.

| Executed call | Outcome | Top RUN/PASS | All RUN/PASS | Elapsed |
|---|---|---|---|---|
| normal-TreeDB-db | PASS, exit 0 | 31/31 | 75/75 | 12.343 s |
| normal-TreeDB-internal-rootpublication | FAIL, exit 1 | 17/16 | 32/29 | 1.205 s |

Raw RUN/PASS/FAIL counts and hashes were independently recomputed. DB evidence includes all four original accepted-cache refusal children and the real ordinary held-root/table-rehash control. Rootpublication has one failed parent and two failed children, `TestPrimaryFinishResourceRefusalAndFailureCustodyV6/{retry,failure}`. All other selected top-level tests in that call passed. There were no SKIPs, timeouts, signals, races/panics reported, or source changes. Both actual leaders were joined; retained process/subreaper records show no remaining group members or adopted/direct children. All before/after source manifests equal e434542a. The evidence manifest's 20 entries independently match their bytes/hashes; the archive digest also matches its receipt.

**The remaining 17 commands were not executed**, including all race calls and the M4 leaf. First-failure stop was honored. No later-stage success is inferred.

## C1 — stale finite-work fixture boundaries

Location: `TreeDB/internal/rootpublication/durable_root_transaction_work_callbacks_v6_test.go:313` (first call/assertion at 315), later queue/final calls at 325 and 331. Both children fail with `ready=false err=nil calls=0 effects=0`. The test file is byte-identical to the accepted source packet; this is a newly exercised inherited control.

The actual fresh constructor root is a pager immutable-read-root node. `finishPrimaryStepWithWorkV6` (`primary_transaction_construction_v6.go:435`) reserves one descriptor record before calling `DropToReleaseQueue`. The latter (`primaryarena/arena.go:766`) reserves two records and calls `validWithWorkV6` before decrementing the root. `slotWithWorkV6` delegates to `PrimaryReadRootOwnerWithWorkV6` (`pager/primary_read_root_v6.go:205`), which charges one table header and one matching link for this single-node fixture. Therefore the current **3** limit exhausts exactly before lookup; it safely returns false before root drop, release dispatch, physical effects, or accounting change. `finishBusy` is reset and original construction/root custody remains. The failure is an invalid expectation that the callback already ran, not a source failure to retain ownership.

Precise source-derived boundaries for this fixture, with its existing 1 MiB byte cap and no owner bindings:

| Stage | Record arithmetic | Consequence |
|---|---|---|
| Existing 3 | descriptor 1 + Drop dispatch 2 | Table-header admission refuses; no root decrement or callback. Preserve this as a distinct pre-effect control. |
| Intended first callback refusal: 5 | descriptor 1 + Drop 2 + table header/link 2 | Root drops once into SAME native queue; callback is entered, its Reserve(1,8) refuses; no resource effect or containing refund. |
| Existing later 11 | descriptor 1 + callback 1; 9 remain < queue dispatch 10 | Valid pre-physical queue refusal. Keep it and assert callback/count/custody explicitly. |
| Retry cleanup call 32 | descriptor/callback 2 + bank retirement 14 + accounting settlement 15 = 31 | Bank/node aliases are scrubbed and their own charge legitimately settled; final empty-queue observation needs 10 more and yields. SAME constructor/terminal remains charged; Set effect has not run. |
| Same-original retry 12 | descriptor 1 + callback 1 + now-empty queue observation 10 | Callback can release the Set exactly once and return; terminal may then detach/complete under the original assertions. |

The bank-retirement 14 is queue dispatch 10 + table header/link 2 + pager removal dispatch/link 2. Accounting settlement 15 is queue dispatch 10 + SAME RemovalWork bind 1 + removal 1 + owner-shrink header 1 + initial empty binding-cursor 1 + receipt reset 1. Fresh capsule initialization and constructor create no owner enrollment bindings in this fixture. The final empty test is a real admitted operation; it must not be bypassed.

A hypothetical single-call full success would consume 41 records for this tiny geometry, but **no larger native turn is proposed or authorized**. The minimal repair is fixture-only: retain the original 3-record no-drop refusal, add the exact 5-record callback refusal, retain 11, then assert the truthful 32-record progress/yield and resume the SAME transaction/queue with the bounded 12-record remainder. Derive constants from those named source operations and assert consumed work/stage as appropriate. Keep byte limits and all original success, first-error, no-replay, no-ordinary-assistance, and terminal-completion assertions.

The `failure` arm's funded 32-record call remains sufficient: descriptor + callback costs two records before the callback records the original physical cause; it does not enter queue traversal. Its zero-work sticky retry and Abort error-identity assertions remain meaningful and unchanged. Recalculate expected callback counts to include distinct newly asserted stages, never weaken their equality or allow unexpected effects. For retry, record the one root drop and assert it never replays; preserve constructor and containing admission across both refusals and the progress yield. The bank/node refund after alias scrub is different from containing transaction/Set refund: do not forbid legitimate physical progress or permit early containing completion.

No production change is justified by this failure. In particular, do not remove lookup/empty-check pricing, grant ordinary assistance, replace native work by nil/unlimited work, or refund the containing parent before callback return and real Set completion. A repaired fixture and separately authorized rerun are required before this control gate can pass.

## Scope and limits

Named native callback/Finish tests use deliberately synthetic callbacks, empty Sets, or tiny owner/queue primitives. Their passes are dispatch/custody checks. DB controls exercise real ordinary publication and finite capture/cancel primitives; containing ACK/Stop tests use an ordinary coordinator/synthetic publisher. None establishes genuine native DB activation, publication, fence, Consume, final Finish, or full Close qualification. Positive actual-native cache activation remains a future-route gap. The unexecuted M4/race controls remain unavailable.

Original **Q32 / 1 MiB / actual33 / 80 / 4096 remain RED, ACK0; no allowance chosen**. No full retained/private-backing total, complete future Finish bound, C1/C3, or public native qualification is accepted. This addendum leaves the prior source-only ACCEPT intact and records **control REJECT pending C1 fixture correction and new root-authorized evidence**. Review lease released on delivery.

## Hashes

- `preserved_source_review_md`: `919013501fdbdd0e75ff74c7cbbeb395da5f8e81558463273755f7acf5726d3f`
- `preserved_source_review_json`: `7f68dc548dceb43d05e5cb5b81b00da3fefbbb45a64a78a7c5131b94018093a3`
- `base_source_manifest`: `787445cb4cbdda78d329b9c927c6e77fbe2ce48cd669214509790c3e9305b118`
- `final_source_manifest`: `e434542a59d55cf67c90d6a16c5787abeb908efda4c1c705033658b9ba8e0577`
- `final_source_archive`: `9d60ef1a4d1960aa63a0fabbb99b55b57c61262ca9bdaa867160303994cbf7ed`
- `final_staged_tree`: `7354d5a24c03ea69f39489d33c2013ba0907f9d6`
- `selection_md`: `d86a94d86b13cac9efeddad57cb4b72b22fd774aa8797cbbbbdd28f0bb27eff9`
- `selection_json`: `9aa62ed39b31d92fb238fa1251facf85d567d16c84c9e5781350000710eb7331`
- `evidence_manifest`: `9bbb05aab3bbd83167498f86048ee64183b52edbff48f69088810005ba2b416c`
- `evidence_archive`: `3fdc9555184bd235bb7736380b60c9050739c192c03d03b9b185591396361de0`
- `db_raw`: `71988855c0f5befa833bd78ca142c1676efa4e67897d4a3d4a99c42607df5e97`
- `rootpublication_raw`: `1d9511ff810681455444b7e4c767ba3c12a99bea8a15aab34976db81fee6662b`
