CLEAN: exact committed `45d2cf6828c50491f82ac00403f5d5328ba0c67e`, compared with `e4e7a3032405bef141010b1c80eb0f0db1cd6f74`.

The complete commit delta is one existing RF4 test file: the post-rejoin all-voter wait adds `status.VectorPhase != "active"` to its refusal condition and three explanatory comment lines. Removing precisely those additions restores the entire original file. The canonical CI inventory is byte-identical; `git diff --check` passes.

The new condition strengthens the readiness precondition. Status uses the existing phase function, which requires a prepared runtime bound to the current DATA FSM DB, matching applied base catalog and the exact ACTIVE lifecycle record. It publishes no authority. Missing, permanently stale or mismatched state remains a bounded failure under the original 90-second child / 120-second parent contexts and 20ms polling. The wait checks no mutation result and retries no capture or mutation.

Every strict oracle remains: independent DATA/CATALOG election; original reply recovery; DATA rejoin prefix; mutation/idempotency/no-op/refusal assertions; live/document/logical witnesses; provider snapshots; fresh insertion and exact retry; each voter's actual backend capture; root/live/document/original-result equality; final snapshot-tail recovery; deliberately stale-FSM binding rejection. Source review identifies no correctness or scope finding. This is not proof of absence of every possible product defect.

The original auxiliary FAIL is preserved. Its log identified owner-3 and catalog-meta-unavailable, but did not retain actual catalog/phase state or retired voter identity. The missing fixture precondition is source-demonstrated; the exact failing schedule remains untraced. Later same-packet passes do not erase that FAIL.

Source applicability, independently computed from Git objects:

- Runtime: 1122 canonical inputs; exact map unchanged, SHA256 `cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9`. Scope includes `.go`, assembly/native inputs and module pins, excluding tests.
- A/C: all 7 ordered harness inputs unchanged, SHA256 `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822`.
- D: all 458 committed root-package collection test blobs unchanged, as are all lifecycle Python/shell tools and the shared source helper. Actual target-specific `go list` selection/compiler/binary identity was not observed. The complete one-path delta excludes every D capture input.
- CI inventory: SHA256 `28e0ffca6160a65173abb05c1b2d6f31668547d5c8070f9eae77da2ded0480fb`; no discovery/shard digest change.

The source-only change preserves applicability from e4e. It does not rehabilitate older different-runtime captures or supply future landed/source/build/environment/noise acceptance. Root owns focused validation, exact-head auxiliary qualification, required gates and merge. No Go, remote, GitHub, CI actions, source edits or delegation occurred.

Original log: 509158 bytes, SHA256 `cc85d081d4365c002b524c2f2854144a592418e87f6020375354d34814f539d6`. Exact source paths/blobs, patch hash and retained diagnosis hashes are recorded in `review.json`. Worktree was clean at the reviewed exact head.
