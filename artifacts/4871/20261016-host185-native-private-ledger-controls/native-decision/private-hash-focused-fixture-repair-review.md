# Focused fixture successor review

**ACCEPT the one-file C1–C3 repair. REJECT overall d0cba snapshot/control readiness for unchanged sibling fixture D1.** No production correction is demonstrated or selected by these failures.

Independent reviewer `/root/ordinary_review`, **gpt-6.1-sol/high**, internal immutable source/evidence review. Candidate edits: **false**. Hosted manual review cannot pin the required exact model and was not requested. No Go/runtime, mutable-source access, source/Git/GitHub edits or children.

## Exact binding and source disposition

Manifest **d0cba17d5cef5666f2856cc9bf0278b0ec7eedcd9946b2aadf5ffaac344d3247**; full archive **4ca1cb5dbd75b3ddcf40c10c4f99dd273572716032ebe4ace2e1240cffd34e51**; delta **37272a6bc03539c622ad2a50b978b8c68cf060d5885021a7a62984af10145e67**.

Independently verified all **10,389** archived files against the manifest and compared the complete baseline 6c525 manifest: exactly one modified file, zero additions/deletions. Its hash is **e83201279fc824bebc60f60b9cf1736cb010492680500ce389feb9924beddf90** (`TreeDB/freelist/private_control_capacity_v6_test.go`). Read the entire file, exact patch and relevant immutable hash/owner/budget/cancel callers. Earlier accepted production and corrected observer source carries by byte identity; its scope limits remain intact.

**C1 resolved.** Peak setup includes actual governing `Stats().TotalBytes` and the rounded additional external lease header. Checked old/new array charges derive `headroom=new-old`; post-fill total/in-flight/retired checks prove replacement alone fits and old+new overlap does not. Refusal must be `ErrCOWCapacity`. Original backing pointer, count, version, owner bytes, next-array absence and key absence must all survive. Caps remain 4096.

**C2 resolved.** Checked live charge includes clone header/vectors, both scratch buffers and all four actual private controls. Detached debt is summed separately, including original private-debt custody. The assertion admits only `live <= Owner.Bytes <= live+debt`, with exact equality when debt is zero. It preserves every cut0..32 and SAME bounded cancellation with final owner zero. This matches original debit-before-reset semantics without excluding live backing.

**C3 resolved.** Phase3 now gets exactly three remaining records, permitting entry + allocation + no-binding Owner admission. Other phases still get two, except the original three-record phase7/10 commit boundary. This reaches the genuine phase11 pause without changing Q32, cursor phase or operation identity. Exact committed-key/debt/no-next, continuation/duplicate/sink and final-zero assertions remain.

## New blocking sibling fixture D1

`TreeDB/internal/rootpublication/selected_manifest_ledger_test.go:137,142` creates ordinary callback directories with `NewOwnedLocalPrimaryDependencyDirectoryV5`. Later, line211 invokes `ReleaseFromWithWorkV6`; line213 receives `ErrWorkRequired`.

The constructor initializes `ownedRelease`, leaving `ownedReleaseWork=nil` (`dependency_directory_reader_v2.go:88`). `DependencyDirectoryReleaseWork.bind` rejects that missing bounded capability before dropping any **directory** ref (`dependency_directory_release_work.go:76–93`), even if the directory is nonlast. More Q32 cannot make this collaborator supported.

The containing Set release can already have claimed `ResourceOwnerReleased` and dropped its wrapper allocation ref before reaching this directory (`resource_set_graph_release_work_v6.go:19`). Therefore this is not a globally effect-free Set abort: its original frame/charge remains responsible, and no private refund is justified.

Root selected a test-only bounded-capability correction in `root-private-directory-fixture-repair.md` (**0b59848d273c6c2f57ac16009946f7bc190182237388e9c0a65ee66cb9e0af6b**): use the original Work constructor with an admitted bounded counter producer, plus finite final directory-edge cleanup. Preserve real constructor/pair equality and unsupported-shape assertions, physical counter exactly two and final owner zero. Keep original caps and production strictness. This selection is not yet reviewed source or executed success.

## Exact controls and reuse

Independently checked both raw logs, command/result receipts, raw hashes and source-before/source-after equality to d0cba:

- `normal-freelist-private-capacity`: **18 top-level / 79 child PASS**, no failure/skip. Raw **38be0632fc119724e47792a2d821d5c179f85813f1d16f0c0b171019e3b27965**.
- `normal-rootpublication-selected-ledger`: three top-level RUN, two PASS, one FAIL (`TestSelectedSetAllocationPairsV6/empty-directory`); kinds passes. Raw **7abd3b7d6bfed56b20a7958ba74fbb4dfcce02352ee2ac64d5da2b725786d1d3**.

No timeout/source drift occurred. Both leaders joined; final group/direct/adopted child lists are empty. The batch stopped at the second command; **10 calls and every race remain unrun**. No previous failure is erased or relabeled.

A strictly rootpublication-test-only successor can reuse normal freelist18/79 after confirming unchanged relevant source/compiler/argv bindings, with original d0cba attribution. It need not repeat that unaffected command; remaining selected controls require root authorization and exact successor evidence.

No actual33 successor observation, complete physical retained union, fixed F/Finish/Close allowance, native finaldriver/publication or qualification acceptance follows. Original Q32 RED/ACK0/no allowance remains. Review lease released.
