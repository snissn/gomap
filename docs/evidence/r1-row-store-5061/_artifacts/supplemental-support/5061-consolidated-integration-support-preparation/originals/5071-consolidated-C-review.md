Consolidated C integration review: CLEAN.

Reviewed source `ebe414b6d6a22a7f616c108157a01f8ed109e7cc`, parents `8506a498346d35c635bbd0f59d99a135d6c6785d` and accepted C `90fa3dc6819dfcd6823bf68d3287252b87c1b8d0`. Checkout observed clean. This review certifies integration and source/workload semantics; it claims no runtime or performance equivalence, landing, or retained acceptance.

The exact nine-file C delta from 8506 preserves every accepted C code/test/capture blob, including the original CLI entry point. All seven A/C harness blobs and bytes match accepted 90fa and the earlier source-semantic review. Four A blobs remain baseline-identical; all three baseline-to-selected raw diff bytes and SHA256s remain exact. Module bytes remain baseline-identical. The earlier A fixture, route, timing, SQLite durability, denominator and noise-policy review therefore transfers at source/workload scope.

Each benchmark README has exactly the accepted C paragraph inserted into the 8506 text. Removing it reconstructs the entire parent README byte-for-byte, preserving all current-main documentation. C command README/mutation spec and final guide/lifecycle spec remain accepted 90fa-exact, including the corrected 18-child-descriptor scope. Canonical inventory passes: 14 workflows, 56 members. Only discovery hash changes from 8506: `984e49ecfaf1131bbbb1b8803cf472cf05378d941b39fc982f0c83040bdf3093`. Raw tree-inventory SHA256: `6f0dbcad267ee634bbe2c9ba2b4dd37d20b0dfe78db0586de376d066a4c99e24`. Applicable AGENTS/CONTRIBUTING were enumerated and bound in the JSON; no explicit local hard review cap was found.

Current runtime SHA256 `cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9` covers 1122 inputs and equals 8506 exactly. Harness SHA256 `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822` equals 90fa exactly. Runtime changed from 90fa; this review deliberately leaves shared runtime seam qualification to the separately assigned audit. Original source-bound C tests remain valid as historical evidence, while current-runtime C checks and required exact-head CI are separate gates.

The six external acceptance pins remain required, including exact selected source = independently verified landed source, actual executing-binary hash and original packet-byte hash. Producer semantic-only replay stays UNQUALIFIED. Physical sync calls must cover the serial requests and WAL write bytes must be positive; measured before/ACK/Flush deltas retain aggregate background attribution. No old performance numbers or packets can be rebound to the new runtime.

Minimal existing Linux185 checks, after the root runner releases the shared correctness lock, using root's configured Go 1.26.4 environment:

```sh
GOWORK=off "$GO" test ./cmd/collection_workload_bench -run "^TestR1MutationSweepSmokeAndCorruption$" -count=1 -v -timeout=3m
GOWORK=off "$GO" test -race ./cmd/collection_workload_bench -run "^TestR1MutationSweepSmokeAndCorruption$" -count=1 -v -timeout=3m
GOWORK=off "$GO" vet ./cmd/collection_workload_bench
```

The single meaningful test creates the real 16-cell diagnostic and checks complete rows/postings after Flush/reopen, internally consistent counter corruptions, missing/wrong independent pins and original-byte changes. A duplicate CLI rehearsal or broad suite is unnecessary for this source integration unless these checks reveal a concern. These are recommendations, not newly executed tests.

Root owns hosted review/CI/actual 5071 merge and eventual 5072 disposition. 5059 criterion6 stays OPEN pending actual retained generic UpdateBatch width/request-size measurements from reviewed landed tooling. Larger populations/concurrency remain deferred; no typed-replace or metadata-reference numerical claim is introduced. Fresh A/D numerical evidence also requires the new frozen runtime and independently observed receipts.

Exact commit/blob/byte/inventory/raw-diff/original-proof bindings are in `5071-consolidated-C-review.json`. No source edits, Go, captures, GitHub operations, polling or delegation occurred; original proofs and packets are untouched.
