# #5091 architecture and diagnostic evidence

This packet supports the [proposed architecture decision](../../design/r1-row-execution-architecture.md), refs #5090/#5091. It contains a **frozen landed rehearsal baseline** and **short nonqualifying characterization**, not runtime candidates or product qualification. The source-bound decision needs coordinator acceptance. The near-linear reclamation (#5095), native leaf retention (#5098) and checkpoint materialization (#5099) gates remain open under their sole owners.

Runtime: `797d783b21b36e7a0e540cd8190b4ec5921711fe`, runtime blob SHA256 `eab40aaf77ed307d92a7a38adf0dd80be07a7e12bdda8a2ec3118baf9cad33ff`. Landed `r1.go` blob `fec4f76df23af2cfa04522fa3e6d07a6fbb43a68`; capture script `abc2008715641c2133fe8958295913782f42c5a3`; source script `16e6bd4bacaf53b0f49f70e2789d0a47b3b42496`. Source manifests retain the complete production blob list. Added test files do not change production source or the baseline harness. Go 1.26.3, Linux amd64, CGO SQLite; GOMAXPROCS=16, GOWORK=off.

Raw artifacts reside on `mikers@192.168.0.185` at `/mnt/fast4tb/gomap-r1-arch-5090-20261007/artifacts/5091`. [Raw inventory](raw-artifacts.json) records bytes and SHA256. Keep every original baseline binary/packet/source-before/source-after/toolchain/CGO/host/validation file, profiler binary/profile/log/source, failed attempt and noisy cell until the parent evidence owner releases it. This checked-in packet contains summaries and immutable identity receipts; it is not a replacement for raw files.

[Baseline summary](baseline-summary.json) retains each population/engine/phase median, raw sample and CV, including inconclusive timing cells. [Attribution summary](attribution-summary.json) retains public route costs and counter deltas. [Rollover summary](rollover-summary.json) separates physical leaf assets from main-value segments. [Characterization identities](characterization-identity.json) bind each archived test source and original binary. [v2 recovery receipt](v2-reconstructed-identity.json) proves the recovered v2 source reproduces the original binary byte for byte. The original v1/v2/v3 binaries are distinct; do not substitute the final PR test source for their source identities.

The [protocol](protocol-before.md) was recorded before collection. The initial `base-4096-r1` failed compilation because inherited GOROOT pointed at Go 1.25 while GO was 1.26.3. It remains retained; no measurements came from it. Subsequent capture explicitly set GOROOT. Shared-host observations were recorded by each baseline process (load about 1.3 initially; later about 2.7, around 23GiB available). No absolute-quiet requirement or cross-host engine claim is made. SQLite B/op excludes C allocation; process RSS is unavailable.

The baseline used this exact order and command from a clean frozen worktree; the six output directories are `base-fixed-{4096,16384}-r{1,2,3}`:

```sh
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
export PATH="$GOROOT/bin:$PATH" GOWORK=off GOMAXPROCS=16
OUT=/mnt/fast4tb/gomap-r1-arch-5090-20261007/artifacts/5091
for rep in 1 2 3; do
  for docs in 4096 16384; do
    R1_OUT="$OUT/base-fixed-${docs}-r${rep}" TMPDIR="$OUT/tmp" R1_GO=go \
      scripts/r1_collection_capture.sh -documents "$docs" -operations 50 \
      -repetitions 1 -engines typed-row,json,sqlite-row -qualification rehearsal
  done
 done
```

Setup/first-use/warmup/oracle boundaries are described in the decision. Point precedes its oracle; held first-use follows it, so it is not cold. Held warm request cost retains oracle/fixture/backend heap and asset handles. Ordinary complete range is coherent. IDs plus held materialization is a quiescent composition. Ordinary structural counters are unavailable, not observed zero. Three processes cannot pass final median-of-five qualification; necessary cells with CV>.10 remain inconclusive.

Characterization reused the existing R1 opener/fixture and public `GetInto`, complete range, captured fetch, `UpdateBatch`, `ReplaceTypedBatch` and FlushAll+Checkpoint. It did not modify production instrumentation. Reproduce using the archived source for the corresponding original binary, on an isolated frozen worktree; never overwrite a user checkout. The final opt-in tests are Linux-only and skip unless their output environment is present. The complete command family was:

```sh
go test -c -o "$OUT/attribution.test" ./cmd/collection_workload_bench
GOMAP_R1_ATTR_OUT="$OUT/attr-generic_update-off" \
 GOMAP_R1_ATTR_ROUTE=generic_update GOMAP_R1_ATTR_CALLS=64 \
 GOMAP_R1_ATTR_DETAIL=off "$OUT/attribution.test" \
 -test.run '^TestR1Attribution5091$' -test.v
```

v1 has serial generic/native and four generic callers; its six detail off/on outputs are `attr-{generic_update,typed_replace,concurrent_update}-{off,on}`. Each is 64 requests. v2 adds the four native caller route `concurrent_typed`; `attr-concurrent_typed-on` is its one 64-request output. v3 adds only the reduced-target rollover probe. Matching inputs are preencoded complete email/city/revision replacements. Native replacement is not a field-patch control. Setup and primary-row oracle are outside timing; these characterizations do not replace the baseline full posting/ownership/reopen oracles.

CPU routes used `GOMAP_R1_ATTR_PROFILE=cpu` and output `cpu-ROUTE`: point=20000, range/prepared=5000, generic_update/typed_replace/checkpoint=64. Alloc routes used `GOMAP_R1_ATTR_PROFILE=alloc` and 16 calls under `alloc-ROUTE`. Checkpoint means n setup mutations then one timed FlushAll+Checkpoint. The 64-update CPU checkpoint and 16-update allocation checkpoint are **unmatched**, so they cannot explain an allocation increase. Detailed stats were enabled except the labeled off controls. Raw report files include every per-request sample, fixture hash, MemStats/Rusage and before/after counters.

CPU profiles cover the timed route, excluding setup/oracle. `go tool pprof -top ORIGINAL_BINARY cpu.pprof` produced each top file. Allocation profiles use rate one and `go tool pprof -top -alloc_space -base allocs-before.pprof ORIGINAL_BINARY allocs-after.pprof`; they contain profile-writing/statistics bookkeeping. Public-stack focus removes synchronous bookkeeping and misses asynchronous coordinator work. Separate `flush-top.txt` and `publication-traces.txt` retain coordinator stacks. Use timed-region MemStats for totals; profile percentages and nested timers are not exclusive partitions. Process wall minus CPU is not request wait. CurrentRead does not enclose typed reconstruction. Exclusive queue/prepare/wait remains unavailable until #5096 propagates the existing diagnostic.

GC source/binary is separate, and the short public characterization excludes generation setup:

```sh
go test -c -o "$OUT/gc-attribution.test" ./TreeDB/db
GOMAP_R1_GC_REVISIONS=32 "$OUT/gc-attribution.test" \
 -test.run '^TestR1ManifestAttribution5091$' -test.v
# Same binary, n=128 and n=512; exact raw logs gc-{32,128,512}.log.
"$OUT/gc-attribution.test" \
 -test.run '^TestR1ParentGenerationDoesNotCertifyInventory5091$' -test.v
```

All three GC calls deleted exactly n released revisions. They are single observations, not comparative acceptance. `authority-probe.log` is a counterexample to parent-identity certification, not a replacement authority. Root performed one fs-verity feasibility probe; [receipt](fsverity-capability-probe.json) records errno95/EOPNOTSUPP and exact inode deletion. No filesystem feature/configuration changes occurred. The [kernel contract](https://docs.kernel.org/filesystems/fsverity.html) explains enforceable immutable content and unsupported features; namespace authority is a separate requirement. No equivalent probe should be rerun to seek a pass.

The single v3 rollover probe used `GOMAP_R1_ROLLOVER_OUT="$OUT/rollover-probe" "$OUT/attribution-v3.test" -test.run '^TestR1RolloverAttribution5091$' -test.v`. Existing leaf/hot knobs were both 65536 bytes only in that test. It checks held/current primary rows through 64 native replacements, checkpoints and backend main-value GC before/after reader release. It does not call leaf revision GC, rewrite or reopen and does not qualify default 32MiB/256MiB economics. Main-value GC did not retire these leaf files; this establishes distinct ownership routing and multiple leaf boundaries.

Validation logs record focused normal R1 harness, public read/mutation/lifecycle and typed-metadata coverage, revision-GC gates/corruption/rebound/held/recovery/cancellation, focused read/GC races and vet. Existing `TestR1PublicPathRehearsalAndRejectPackets` and mutation-sweep corruption tests deliberately reject malformed/stale identity and wrong oracle/counter evidence. No new production API, storage format or ACK behavior is implemented here. The added parent identity counterexample is deterministic on the Linux contract; opt-in characterization tests have explicit short input limits.

Fixture databases were owned by the existing R1 opener or Go test TempDir and removed on close/test cleanup. Retained original baseline and characterization binaries are evidence consumers, not released garbage. The only manually released file was `attribution-v2-identity.test`, a completed duplicate rebuild with identical SHA to original v2, after the comparison receipt was saved. Persistent user Go caches and unrelated primaries were untouched. Root must release unique raw evidence/binaries after graph review consumers finish; no broad temporary-directory cleanup is authorized by this packet.
