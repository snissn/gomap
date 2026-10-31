# Public control scope retained for the final comparison

The public A/B controls were actually built and executed at
[`79220385186080e77898fcb99c78a8ea15480241`](https://github.com/snissn/gomap/commit/79220385186080e77898fcb99c78a8ea15480241),
tree `ed39075798b6b5797854a6851ece3996d865a606`. The accepted map binds **22 unique
files** across A/B. Those are not 8bea executions or the current gateway ELF.
The [original reviewed receipt](https://github.com/snissn/gomap/blob/8bea490ad5a2d89a14f02f428b4283579a6dd864/docs/benchmarks/ycsb_2026_10_30_evidence/final-ab-review.md)
is revision SHA256 `92a8322d5c12cd20509de0469958e518236f2459d5e85c9f022166e92259a3a8`:
**ACCEPT for the original scoped public correctness contracts**, not performance.

| Control | Exact accepted public proof | Result and binding | Explicit limits |
| --- | --- | --- | --- |
| A: same-owner public sentinel / pointer-bearing prefix | Four clients each insert 16 documents through the public official Mongo driver with explicit w:1/j absent; all clients join before a same-owner w:1,j:true sentinel. Both durable and relaxed profiles complete the sentinel, then the process exits 97 without server Close. All 64 expected raw BSON documents match byte-for-byte after public reopen, including ten binary 100-byte fields and a selected 64KiB binary value. Selected actual persistent leaf root 10/Flags 1/ValuePtr survives across the abrupt exit; this is a concrete checkpointed pointer witness. | Positive exit 0; both profile children exit 97 as expected. Negative duplicate-client case joins all four clients and exits 2 **without attempting the sentinel**. Control ELF `5396263f2f52eea75109d243e433fbe06ad65025b12cbdcc3eee1a58425aaaf4`; source SHA `103dc743…`; positive stdout SHA `795542e9…`, negative stderr `9ed00e5d…`. | A witnesses one selected pointer and its actual dependencies through public sentinel success/readback. It does not fault-inject newly-created value-log sync, exercise all pointer/GC cases, or establish failure durability. Its negative case is failed-client prefix admission, not a storage sync-failure injection. |
| B: ordinary durable ACK / sync / unapplied replay | Public BSON insert/update requests use explicit w:1/j absent under command_wal_durable. Five requests are held 100ms at the actual physical command-WAL BeforeDependencyFileSync seam; no ACK escapes the hold, and AfterDependencyFileSync precedes success. Assigned/durable endpoints 3–7 remain applied 2. Four concurrent writers then insert/change-update. Before abrupt exit 97: durable 15, accepted target 15, applied 2, next 16. No postmutation checkpoint or Close is manufactured. Reopen advances applied 15 and exactly verifies all 7 IDs and 128KiB binary bytes 0x72. | B suite exit 0. Separate existing-collection insert and update cases inject CONTROL_B_SYNC_FAILURE at retained physical WAL sync: public ShutdownInProgress error, never success; poisoned Close returns reopen-required, Disconnect succeeds. ELF `881f10a6cfabf7fdcb8006b0c0bea32721b718edd0c1f7d2a011f50a35d4cc01`; source SHA `9d2dacb1…`; stdout SHA `b7a049ed…`. | Setup includes one public pointer seed and a checked standalone durability barrier. B proves acknowledged-but-unapplied WAL replay and specific required command-WAL sync/error behavior. It does not independently witness its selected persistent leaf pointer; A supplies the separate checkpointed-pointer proof. No new value-log sync-failure proof or full Mongo crash semantics follows. |

These were process-crash correctness controls, not a clean-close substitute:
the success fixtures exit abruptly before owner Close; subsequent recovered owners
have checked cleanup. They are also not hardware power-loss/controller/filesystem
failure certification. Runtime GOMAXPROCS was absent, sampled effective runtime 1
under CPU 0/nice 10; these settings and administrative elapsed times are not final
throughput evidence. Sampled PID/start/ELF coverage does not imply every ephemeral
child was observed or a quiet host. All original preparatory failures remain
retained; no threshold, cost FAIL, CI or comparison acceptance is waived.

## Applicability to 8bea and current lifecycle separation

The actual 792 control source remains unchanged in the map. The
[scoped source proof](/home/mikers/gomap-5335-wire-shell-final-source-20261031/SCOPED-CONTROL-APPLICABILITY.json)
(SHA256 `97c0d57c1b2a5fb33a51efd62622d0b88337d174ab431c8f9f00ee0df465d13b`)
retains seven public ACK-route source checks and the accepted dependency scope.
It admits the direct standalone-server BSON sentinel/ordinary-ACK mechanisms
across the listed deltas, without blanket whole-product equivalence. The controls
do not enter the CLI private checkpoint callback or the changed shell/wire helpers.

Root's [8bea scoped acceptance](/home/mikers/gomap-5335-wire-shell-final-source-20261031/ROOT-PACKET-SCOPED-ACCEPT.json)
SHA256 `15054d268da7ed9aaf0ccc9df03db39d7f710fcc38857e87050906c5eccf7017`
records actual control source 792 and separate required current build/script/final
arm evidence. Current private schema 2 owner CheckpointStorage receipts, original
producer Close outside TOTAL and final actual request/runtime/state/reopen checks
remain separate; historical schema 1 backend-only receipts are not current proof.
Neither the public controls nor clean reopen establish full MongoDB durability
equivalence. Durable-B's timed differing-request label remains mandatory.

## Prospective compact evidence identifiers

The final public destination is planned as
`docs/benchmarks/ycsb_5335_final_evidence/` with `index.json` and
`raw-phase-evidence.tar.gz`. The accepted packager deterministically assigns these
member names from the exact retained original paths. **These are prospective
identifiers, not a claim that the final archive already exists; the actual checked
index is authoritative.** ELF bytes are omitted with original build/map digest
authorities retained. The current control map may itself be represented by the
packet's `accepted-controls.json`; individual 22 control-map file entries are
explicit external-reference seeds.

- `REVIEW-RECEIPT.md` → `support/referenced/70755a8259736f0b/REVIEW-RECEIPT.md`
- `RESULT-SUMMARY.json` → `support/referenced/b20f470bc9c0381e/RESULT-SUMMARY.json`
- `sources/control-a.go` → `support/referenced/a4095bf1a643caae/control-a.go`
- `worktree/TreeDB/mongo_gateway/control_b_public_durable_test.go` → `support/referenced/8c6eec21413e830b/control_b_public_durable_test.go`
- `live-output/a-positive/stdout.txt` → `support/referenced/5d8b1e3b8d9c83d9/stdout.txt`
- `live-output/a-negative/stderr.txt` → `support/referenced/d83b89e2f4e742e4/stderr.txt`
- `live-output/b/stdout.txt` → `support/referenced/6be367b8d24cc7ab/stdout.txt`

Original map: [reviewed-controls.json](/home/mikers/gomap-5335-final-ab-controls-20261030/reviewed-controls.json)
SHA256 `c88c4f3514ade98668325f18b0de7a31162985ccd00c6b684a8952e4aaa67633`.
[RESULT-SUMMARY.json](/home/mikers/gomap-5335-final-ab-controls-20261030/RESULT-SUMMARY.json)
and [REVIEW-RECEIPT.md](/home/mikers/gomap-5335-final-ab-controls-20261030/REVIEW-RECEIPT.md)
supply full original log/source/build identities and precise limits. This note
reuses that accepted evidence; it creates no new execution, review gate or claim
about the active twelve-arm result.
