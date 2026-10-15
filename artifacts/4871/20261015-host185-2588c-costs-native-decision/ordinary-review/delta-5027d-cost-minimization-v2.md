# Independent three-file cost delta review against5027d

**SOURCE ACCEPT; qualification HOLD.** No source correctness blocker was found in the exact delta. Root owns implementation and all runtime execution. This source disposition does not waive the original5027d raw GetVersioned failure or qualify successor performance/CI.

Predecessor `5027dc559f83dee04317910169c48d277fcce72f`. Exact full diff SHA256 `622bd6c91cea4a6fe7f8eaa61c20fef7e0cb99f0a7b2942ebe7dbfe1ffd7f718` matches root's frozen `ordinary-capture-cost-pager/source.patch`. The JSON companion binds each production file and generated CI hash by SHA256 and records controls/limitations.

- Capture exit preserves original rollback, epoch-before-inflight-decrement and rootReuse unlock, including panic cleanup. Existing containing wrapper and physical ownership are unchanged.
- Empty/singleton kind traversal uses the same authoritative logical-index walker and stop signal; canonical order is intrinsic for at most1 kind. Multi-kind generic sorting is unchanged.
- `IsVerified` routes retained metadata through the same lock over one shared body. The original verification body after its lock block and the new `isVerifiedPage` body are byte-identical, SHA256 `bc24d45eb603433bc4f851515470aedc495edee605e820208c8a55609bb88455`. Companion/fallback recursion still invokes public `IsVerified`; namespace/bounds/atomic behavior and lock span remain unchanged.
- Generated CI metadata changes only its discovery hash.

The first two-edit batch passed all7 commands. DB normal/race each passed56 top-level+14 subtests; full rootpublication normal/race each passed352+243, without failures or skips. Rootpublication source hashes are unchanged in this three-edit freeze. Reviewer reparsed original rootpublication JSON logs and checked ELF/assembly hashes. Actual local Go1.26.3 disassembly shows no stack-defer registration in the capture or its exit helpers; exceptional `deferreturn` landing pads remain. This is distinct from hosted retained ELFs built with Go1.26.9.

The active root batch covers selected DB and entire pager normal/race plus build under complete frozen source hashes. It includes the proposed minimum existing controls, so no duplicate run is requested. Final compiled pager mechanism and bounded Get measurement remain pending; source factoring alone does not prove a speedup. No further optimization is proposed in this review.
