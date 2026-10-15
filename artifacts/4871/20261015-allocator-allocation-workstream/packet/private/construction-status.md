# Private allocator preparation construction

Issue: snissn/gomap#5132, parent #5130 / #4871.
Base: 177fb0c8db1d19c79d3d975126b4b0b64019568e.
Source lane: allocation-private-20261015, branch codex/4871-allocation-private-20261015.
Source supplier inspected: preserved #5123 at 46e5c249e5714be644a406c52addcae312ad372f.
Policy inspected: root AGENTS.md, TreeDB/AGENTS.md; no deeper policy.
Helper setup (coordinator validated): ordinary Git explicitly selected, clean exact-base seed ordinary-5111; no CoW seed claim.

Changed owned files: TreeDB/freelist/generation_v1_txn.go,
generation_v1_metadata_reuse.go, allocator_cow.go; new generation_v1_private_test.go.
Topology contract dependency: compressed canonical *stateNode, terminal chunk != nil,
branch routing n.depth/n.prefix, direct chunk page root; unified
mutatePrivateChunkWithStats(...,*FreelistTxnStats) from #5131 owner.

Explicit privatePreparation permission is granted after dirty graph detachment,
revoked before sharing transaction roots, and frozen before any sink callback.
The staged rollback clone uses an isolated dirty graph. Durable nodes are copied
on first mutation; their zero-ID children must be detached before private access.
Page ID zero alone confers no authority. All materializer exits revoke permission.
Candidate byte isolation for generic retaining/mutating sinks remains unchanged.
No R1 capacity-credit system, global pools, output representation or native Set/binding/Finish changes.

Representation exception: public allocation/recovery semantics remain equivalent;
internal representation RED directly proves the repeated-copy mechanism. Baseline
RED captured in representation-red.txt before production edits. Public route/profile
and matched B/op evidence remain required, and construction is not accepted performance.
Counters are transaction-owned PrivateMutationPaths and terminal PrivateChunkReuses.
No mutable global callback or extra lookup traversals are added for measurement.
Count detachUnmaterialized plus node/chunk clones in allocation attribution.

New controls: repeated mutation identity+counter activation; original/clone alias
revocation; private prepare rollback isolation; retained candidate and reopen;
first durable-parent copy with dirty child aliases; validation-error revocation;
sink failure consumption/revocation. Existing general-sink, reservation/reused-ID,
ambiguous-publication and retry tests remain required.

Integrated focused validation completed: full freelist suite and targeted DB Patricia/format
controls passed; exact source/argv/output in ../repair-batch-focused.{json,log}.
Public combined and topology-only counterfactual diagnostics completed; see handoff.md.
Final post-review source formatting/P1 repair was rebound; see final-freeze.md.
No data paths are released for deletion. Binaries/profiles/source variants remain evidence.
Original uncommitted source remains unique and frozen for coordinator composition.
