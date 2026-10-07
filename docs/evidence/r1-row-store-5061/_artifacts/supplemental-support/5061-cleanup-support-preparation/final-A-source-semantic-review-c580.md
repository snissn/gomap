A source/workload semantic applicability: CLEAN at c580 and final 90fa.

Observed source: `c5803b93caca11825cb826a54a568ff522b80373`. Final candidate: `90fa3dc6819dfcd6823bf68d3287252b87c1b8d0`. Neither is asserted landed. Baseline: `0216e2e9ee701dee0eda96c569dc0c8facb37ca9`; prior reviewed source: `b29a3747a99681963e1799c9c303b5a502667c92`.

All seven harness blobs and bytes match the prior b29 review. Four A implementation/analysis blobs remain byte-identical to baseline. All three exact baseline-to-candidate raw diff SHA256s and bytes match the original b29 patches. Modules remain unchanged. The prior fixture, route, SQLite durability, timing/allocation scope, denominator and noise-policy observations therefore transfer as source/workload semantics. The companion JSON preserves their detailed observations and denominator map.

Current runtime SHA256 is `9bae22b48fd75c2cc38ba5b016970b6d5104f9b5ee7eae04267bbd2e49734873`; harness SHA256 is `706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822`. Runtime differs from prior b29 (`31b744906a95c46b124913edb42e29df6350ec0bdcb431311f8d6cc2e364b183`); prior runtime-equality statements do not transfer. The current runtime map equals both 46 and 3182. Runtime changed paths since b29: `TreeDB/db/leaf_manifest_revision_gc.go`. This certificate establishes no generated-code, compiler, runtime or performance equivalence.

The final 90fa descendant changes only the two R1 docs, correcting the static GC child handle bound to 18. All harness/runtime blobs and all three reviewed baseline diff hashes remain exact. This correction does not rebound any original packet or receipt.

Fresh current-source measurements and actual landing/source/compiler/build/environment/host/filesystem/binary/packet receipts remain required. Preserve the original fixture/count/config, denominator, noise and complete-run gates. Historical noisy measurements remain historical. Root owns hosted review/CI/landing and retained acceptance.

Independent checks were read-only Git object inspection and Python binding/inventory verification; no Go, tests, captures, polling, GitHub operations, delegation or source edits. Exact per-file, diff and original-proof SHA256 bindings are in `final-A-source-semantic-review-c580.json`.
