# Bounded MVCC early performance inquiry

This packet records an early failing performance signal, not final product qualification. Candidate production is c3ba82a5ef6d6590f6c70734045c8d640c4c1255; baseline main is 3f87d74b98e8b1e809d151611be03785f85e2283. Original issue #4871, #5111 and #4878 gates remain open.

Twenty-one invocations used existing identical benchmark harnesses, two balanced short AB/BA pairs of five rows, and four candidate-only COW diagnostic rows. All benchmark commands exited 0. The landed checker exited 1: GetVersioned added112B/1alloc and paired46.14%time; BatchWrite added2215.5B/5alloc and paired4.40%time. Three other matched rows showed no threshold crossing in these samples. Low sample count prevents statistical timing acceptance. COW rows have no equivalent enabled-PRIMARY baseline and are diagnostics only.

The source-only hypothesis is an escaped112-byte OrdinalScanWork from newly unconditional synchronous Manager wrapper release; compiler/profile proof and equivalent cleanup semantics are required before accepting a repair. The implementation priorities are that read allocation, deterministic batch allocations, and finishing in-flight original caller migration, before expensive full cost collection.

See early-report.md, measurement-receipts.json, root-verification-receipt.json and landed-checker-summary.json. Exact argv/environment/raw rows/toolchain/binary/source hashes and failed initial GOROOT attempt are retained. Source-callchain copies retain the measured actual API route. The two protected diagnostic test overlays are separately attributed; production and prerequisites are committed.

Only the exact owned .185 baseline/candidate worktrees, clone repo and fresh tmp root were removed after inventory/consumer review. Approximately1.91GiB allocation was released. Root independently read back all four absent paths and all six unchanged measured executable hashes. Privileged infrastructure process exclusions are explicitly recorded in cleanup-receipt.json.

All six actual measured executables remain on192.168.0.185 under /mnt/fast4tb/mvcc-4871-earlyperf-20261014-sol61/measured-binaries; their294483244 bytes are omitted from this Git artifact. Raw results, source receipts and exact reconstruction remain retained there and in this packet. The next short recheck should reuse original baseline binaries and transfer only a newly source-bound candidate executable, avoiding another source clone.
