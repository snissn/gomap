This draft preserves the in-progress ordinary DATA/PRIMARY implementation for the user-requested host migration. Refs #5111 and parent #4871; #4878 remains blocked by this prerequisite. **WIP: not dependency-ready, reviewed, qualified, or mergeable.**

Ordinary publication uses immutable logical PRIMARY roots, separately owned promotion roots and the same DurableRootTransaction/coordinator lifecycle. The packet includes complete capsules and physical closures, identity-bound joint DATA/PRIMARY vacuum/recovery, held-reader/cut and backup/Raft consumers, typed ordinary Store/cache/backend admission, and added retained-metadata ownership. Persistent ValuePtrs and command-WAL DURABLE/RELAXED ACK boundaries remain part of the intended contract.

Frozen source:
- Head: `d4cbf23f9333eed79718845d3ac253c5d7fc5513`
- Parent/runtime baseline: `2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c`
- Tree: `e76c23955124cc4cc10ef59ad6b8490bb4066f2a`
- Branch: `codex/5111-ordinary-data-primary-publication`
- 246 intended source/doc paths plus the advisory CI discovery fingerprint. All source/doc SHA-256 bindings match the pre-commit candidate; checkout clean after push.

Validation is component evidence. The final consolidated allocation-core command **failed**:
```
go test -p=2 -v ./TreeDB/internal/retainedalloc ./TreeDB/internal/rootpublication ./TreeDB/internal/primaryarena -count=1 -timeout=180s
```
Go 1.26.4, GOMAXPROCS=2, explicit matching GOROOT and offline module/toolchain environment. retainedalloc and primaryarena pass; rootpublication has three retained failures:
- TestResourceKindBackingSharedSetAndScopedBorrow: `set release retired a scoped borrower`
- TestResourceOwnedKindMergeBuilderTransferAndLastBorrow: `public release stole borrowed closure`
- TestOwnedBuilderAddCoalescesAndRefusesBeforeTransfer: `actual repeated identity did not coalesce`

Raw SHA-256 `06c394861466b760eecace3481b077e857b20c38ca73de3e37c6f956a5ef85c3`; result `b27746dde8bdd93bc0f8ca876590f3233a1b92e71bdfa2867f7b9448b2465f51`; equal 10040-path source maps `7ad592208817c1c3959c6296aae75fa49b5d2c931a8029c0a9118108a72f5203`. No repairs or tests followed the spin-down instruction. Earlier six-case child-seal/V5 vacuum/rebind race packet passes at its own recorded source; it is not whole-candidate acceptance.

CONTRIBUTING's CI inventory refresh/check passed against staged source, preserving all 14 workflows/56 original members and omission_authority=false. Contract suites remain owed. Canonical format/recovery/lifecycle/write/verification docs and pre-alpha notes are included.

Resume by resolving the three failures causally, completing the source-bound 19-group allocation/lifetime inventory, then affected normal/race/recovery/GC/Close/consumer validation and one final-base reconciliation. #5118 must land its reviewed harness selector before expensive retained performance collection. Existing-path matched controls and enabled incremental/scaling/retention evidence are still missing. #5080 is unmerged foreign work, not adopted by this PR; preserve its owner and coordinate any actual ABI dependency.

No native public glue, original 33/80/4096 runtime, 32R/1MiB qualification, once-ACK/finite-Finish acceptance, performance acceptance, AI review, or merge is claimed. Root owns acceptance/merge. Exact retained host paths and remaining phase inventory are in the #5111 migration handoff comment.
