# M0 observation harness: reviewed source, no collection

Harness PR [#5136](https://github.com/snissn/gomap/pull/5136), head `939b10c22e54caf56dc84553b65e2435862052fb`, base `5bca87ffe6e1bfba2d693264d2dc82936adc234f`. The separately fixed measured product is `2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49` (ordinary PR #5127). The harness is not yet merged or executed in this packet.

Independent explicitly routed gpt-6.1-sol/high review ACCEPT with zero findings binds all 9,942 committed source files. Six focused root suites pass (68 tests plus inventory verification); the reviewer independently ran the ten pure diagnostic controls. Pristine/overlay Go1.26.9 builds are compile-only. Both executables and the official SDK archive are retained locally and identified in `binary-bindings.json`; this Git packet publishes hashes/provenance, not binary contents.

The original two fixtures and 20-call interleaved schedule, actual Go invocation semantics, timed benchmark body/writer, selectors, workload, 10% stability limits, and workflow timeout are preserved. One preflight may refuse with zero calls. The diagnostic adds untimed GC observation, process/host/cgroup polling, and actual-ELF retention; these can perturb runtime behavior. It cannot replace the failed uninstrumented M0 gate or establish environmental causality by itself.

The initial integration test cardinality failure is retained alongside the corrected inventory and passing final controls. No existing execution selector or required gate was weakened. No runtime grant is inferred: the exact harness must land and receive root's separate one-run authorization before dispatch. Native construction and ordinary acceptance remain open.

`manifest.json` binds each original and stored file; gzip files preserve source hashes after decompression. Caches, temporary build trees and module copies are excluded. The current ordinary raw/PRIMARY PASS, M0 FAIL, maintenance residual allocation cost, and native prerequisite state remain in the preceding evidence packets.
