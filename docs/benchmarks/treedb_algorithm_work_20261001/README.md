# TreeDB physical-work decision harness (#4893)

This is an instrumentation-only harness for [#4893](https://github.com/snissn/gomap/issues/4893), under #4888. It changes no production source, default, storage format, or durability boundary. The issue remains open until measured decisions and every activated implementation child complete. A reviewed, landed harness precedes expensive retained collection; bounded pilots only qualify the constructor.

Initial provisional source is the clean union `6fffa5412db4ec70a5db614bd52f9cf7d255b210`: V `ab2e9124fcca72710b896cf78577fa6bd37a11b0` plus N `a95ffc65cf4020b41cdbf90774473a6a469d0215`. Final acceptance must synchronize merged predecessors and audit the actual diff before freezing runtime and harness identities. Root/TreeDB AGENTS.md and CONTRIBUTING.md were read at this union. This bounded benchmark/instrumentation change uses the issue's test-first exception; public behavior checks protect its constructor.

The next provisional synchronization merged V `9ab02b48ce2397410940a7e42cc22bbf95a96c5a`, N `37fb160c1ebfd21710cbce68da53d883c1e18eff`, and main `c9a00bb631d34b62669ec1d3f4437e1c8c5df845` without conflict, producing `a0827b8818b0af728d5ac7b63443922d3651f534`. This preserves the original constructor evidence at `b80d4c09bc78086ef5e7fb216786109f8907fb74`; it does not relabel it as acceptance of the newer runtime.

## Fixed write workflow

`BenchmarkAlgorithmSparseUpdates` uses ordinary public `Open`, `NewBatchWithSize`, `Batch.Set`, synchronous `Batch.WriteSync`, and `Checkpoint`. It loads 250,000 even keys, updates exactly 40,000 keys with `(i*7919)%keys`, and verifies every byte and every interleaved odd-key miss after close/reopen. Both fixture sizes are coprime to 7919; updates do not repeat a key. Keys are 32 bytes with shared prefixes. Values are compressible 256 bytes with an explicit key ID and generation. One update generation is permitted: the concurrent reader accepts exactly generation 0 or 1 and checks all remaining bytes.

The pilot uses 8,192 keys and 8,000 updates. Every synchronous acknowledgement has 1,000 updates. Four checkpoints occur every 10,000 updates (pilot: every 2,000); one checkpoint occurs after all updates. All acknowledgement, batch close, and final checkpoint work stays inside the shared write interval. Worker preparation precedes the release barrier. Writer failure stops and joins the reader before database teardown; a reader validation/sample failure cancels subsequent writer batches. Every operation/checkpoint count and final Close must pass before a JSON packet is emitted. Process-crash recovery separately exits immediately after `WriteSync` without Close/checkpoint and verifies replay after reopening.

The controls are inline placement (`PointerThreshold=16384`) versus persistent pointers (`PointerThreshold=1`), and existing coalescing defaults (64 memtables, 2,097,152 operations, 512 MiB) versus widened existing budgets (128 memtables, 4,194,304 operations, 1 GiB). These are admission limits, not fixture sizes or actual allocations. `FlushThreshold=64 MiB` is fixed within the primary comparison; `TREEDB_ALGORITHM_SMALL_FLUSH=1` selects a separately labeled 1 MiB cohort if missing backlog eligibility prevents the decision (other nonempty values except 0 fail closed). Automatic checkpoints, idle checkpoints, maximum-WAL checkpoints, background index vacuum, and background pruning are disabled. Existing native/adaptive flush admission remains selected normally. Its effective routing, admission, background flush, and coalescing counters are retained. The widened budget may admit no extra work for this shape: zero admission is an eligibility result, not proof that coalescing or a larger algorithm cannot help. Flush-threshold comparisons change an explicit operational control and are not engine-only wins.

Different checkpoint cadence changes amortization, publication latency, and residency; it is not an engine-only optimization. Widened budgets can spend memory. Comparison requires identical logical bytes, acknowledgement cadence, RNG, generations, host, and toolchain. Existing #2922 and #2771 implementations are reused; their historical 10MM acceptance gates are neither inherited nor waived.

## Measurements and units

Go `ns/op` is one complete fixed-work write interval including stopping/joining the reader after the final checkpoint. `updates/s` and `reads/s` use that same interval. The reader measures ordinary owned `Get`, checks all bytes outside each individual read timer, and samples every 16th completed read across the entire interval. Full read count/max and sample count/stride are retained. Sample overflow fails closed instead of truncating the end. p99/p99.9 are sampled owned-read latencies; disclose sample support and do not claim precise p99.9 from tiny pilots. Full-value validation and reader scheduling influence workflow throughput. A single reader is a bounded workload, not a capacity claim.

Go B/op and allocs/op include timed foreground/reader work. The explicit process allocation delta additionally covers reader setup immediately before release. They are not engine-only allocation figures. Post-reopen one-GC heap is a diagnostic with the live database; the stats include memory/cache/mapping owners. RSS and mmap virtual bytes must be reported separately, never summed as independent physical owners. Peaks exposed by Stats are sampled engine/process peaks, not a continuous phase maximum.

Before/after complete flush/checkpoint/command-WAL/cache/memory stats bind old leaf loads, old node bytes, operations/spans, output index/leaf bytes, WAL bytes, stages, LSNs, and background/coalescing routing. Counters must be differenced, not read as update-only cumulative totals. Engine page/node bytes are logical physical-work counters, not disk-device I/O. `/proc/self/io` is labeled **kernel process I/O**: `write_bytes` counts kernel storage-layer accounting, `wchar` counts write syscall bytes, and `cancelled_write_bytes` remains separate. None alone proves total device writes. Missing Linux I/O is an unavailable observation, not zero. File sizes are logical retained bytes keyed by filename; redo WAL and persistent value/leaf logs must remain separately classified. Allocated filesystem bytes and recoverable/pinned-generation debt require the native debt audit or external runner manifest before a final storage claim.

## Naturally supplied batches and independent descent diagnostics

`BenchmarkAlgorithmGetMany` supplies sorted contiguous, unsorted clustered (128-key neighborhood), and uniform batches of 64 directly to public owned `GetMany` and callback `GetManyView`. There are 128 deterministic batches (seed 4893), duplicate inputs, and interleaved misses. The entire fixture and every supplied batch are verified before timing. Owned results preserve input indexes; callbacks identify inputs by index and may arrive out of order/concurrently. Both modes check identical found/missing/full-value semantics; their different ownership costs are reported separately. The reused per-batch validator requires each input index exactly once, the corresponding original key, found/missing/full-byte values, and sticky callback errors. A mutex permits out-of-order/concurrent callbacks while detecting omitted, repeated, out-of-range or wrong-key callbacks, including duplicate keys and misses. Owned warmup/timing, callback warmup/timing, constructor checks and the independent diagnostic share this validator. Batch timing includes **API plus full consumer validation**, including its mutex, completeness scan and per-batch allocations, not engine-only time; a traversal ceiling requires separate work/CPU attribution. The constructor test additionally proves old snapshot generation stability and that mutating owned output cannot alter storage. Existing contract tests continue to own callback-error, empty-value, and closed-snapshot behavior.

Existing leaf-group counters do not count internal descents. `scripts/treedb_algorithm_work_overlay.py` creates a disposable Go overlay that counts actual successful internal-page loads at `loadNodeViewWithLoadKindInto`, the common point/batch traversal seam, after integrity checks. The separately generated public-path diagnostic resets before each stable-root batch and reports actual visits and unique `(root,page)` identities. The latter is **potential union visits**, a work lower bound; it is not an implemented traversal and proves no throughput improvement. It covers fallback work too. The mutex and map overhead are isolated from timing. Do not run benchmarks through the overlay.

The script records original source SHA-256/Git blob, runtime HEAD/TreeDB tree, and generated source hashes. It fails if the instrumented function marker changes. Full collection compares the claimed runtime HEAD to actual checkout HEAD, rejects dirty/untracked relevant sources, and checks an external freeze JSON binding `runtime_head`, `runtime_tree` (the TreeDB Git tree identity, which binds its blobs), `harness_sha256`, and `binary_sha256`. The executable is hashed before timing. Build/freeze the executable from the reviewed clean source before collection and preserve that external manifest; recording an environment variable alone is insufficient. Pilot mode is explicitly unretained and does not require this production freeze.

`capture.py` provides the operational freeze around that runtime check. Preparation builds one test binary without running it. Its external `freeze.json` records the exact build command, executable SHA-256 and `go version -m` output, actual `go env` settings (including architecture feature controls such as GOAMD64/GOARM64), resolved module graph, and SHA-256 inventory of the actual `go list -deps -test` package inputs (including local replacements, stdlib, cgo/assembly and embedded files), plus Go compiler/linker/assembler inputs. The same identity is checked before and after preparation and every capture; package files in a dependency cache are bound independently of Git HEAD. Clean tracked/untracked checkout, changed source/dependency/toolchain/binary, and changed overlay inputs fail closed. C compiler settings are recorded; system C headers and filesystem/device state still require the external host manifest and are not inferred from Go package inputs. Inspect build-info output alongside the build command/input inventory: a test binary may omit module details from build info, so that field alone is insufficient.

Process environments admit only known build/path inputs. Runtime settings are normalized and frozen: GOGC=100, GODEBUG empty, GOMAXPROCS=2, GOMEMLIMIT=2GiB, GOTRACEBACK=single and GORACE empty. Arbitrary inherited variables/secrets and TREEDB overrides do not cross the process boundary. Revalidation requires the complete eight-field source identity exactly equal to the external freeze, including nonempty input/module/build-environment inventories; empty or subset identities fail closed.

Each process writes exclusively created `stdout.log` and `stderr.log`, then records their independent hashes, exact command/environment and exit status in `execution.json`. JSON and benchmark rows are parsed only from stdout; engine stderr is retained without repair or splicing. A failed process or validation leaves an incomplete packet and both raw streams. Completed raw logs/records are made read-only and can be revalidated against their hashes; permissions do not replace hashes. Eight write cells require eight unique control packets, exact fixture/ack/checkpoint counts, complete stride samples, monotonic required work counters, and checked closure. Twelve batch cells require 1,000 public calls of 64 inputs each. Twelve independent internal diagnostic cells require all 128 batches and consistent actual/union/repeated visit counts. These checks establish packet completeness, not sample precision or noise acceptance. Synthetic records in `test_capture.py` are gate tests, never measured evidence.

## Reproduction

On the exclusively granted Linux runner, use Go 1.26.3, `GOWORK=off`, persistent build cache, and a dedicated source/tmp/log directory. No other timed run may overlap. Keep raw successful and failed logs. Package test profiles are ordinary Go profiles, not benchprof inputs.

```sh
GOWORK=off go test ./TreeDB -run '^TestAlgorithmWork(BatchConsumer|Fixture|AcknowledgedReplay)$' -count=1

# Bounded constructor pilot, only after runner grant and source preflight.
TREEDB_ALGORITHM_PILOT=1 GOWORK=off go test ./TreeDB -run '^$' -bench '^BenchmarkAlgorithmSparseUpdates$' -benchtime=1x -count=1 -benchmem -v
TREEDB_ALGORITHM_PILOT=1 GOWORK=off go test ./TreeDB -run '^$' -bench '^BenchmarkAlgorithmGetMany$' -benchtime=1000x -count=1 -benchmem -v

# Counter-only pass; use a fresh output directory and bind its manifest.
python3 scripts/treedb_algorithm_work_overlay.py "$PWD" /tmp/algorithm-overlay
TREEDB_ALGORITHM_PILOT=1 GOWORK=off go test -overlay /tmp/algorithm-overlay/overlay.json ./TreeDB -run '^TestAlgorithmWorkInternalVisits$' -count=1 -v

# After review/landing: build only, then collect under the coordinator's grant.
# Output directories must be new; prepared paths/source remain stable.
CAPTURE=docs/benchmarks/treedb_algorithm_work_20261001/capture.py
# Pin the actual toolchain; the helper disables automatic toolchain downloads.
export GOROOT=/home/mikers/.gvm/gos/go1.26.3
python3 "$CAPTURE" prepare --source "$PWD" --output /tmp/algorithm-prepared
python3 "$CAPTURE" capture --source "$PWD" --prepared /tmp/algorithm-prepared --output /tmp/algorithm-writes-r1 --family writes --grant COORDINATOR_EXCLUSIVE_GRANT
python3 "$CAPTURE" capture --source "$PWD" --prepared /tmp/algorithm-prepared --output /tmp/algorithm-many-r1 --family many --grant COORDINATOR_EXCLUSIVE_GRANT
python3 "$CAPTURE" validate --source "$PWD" --output /tmp/algorithm-writes-r1
# Add --pilot for an explicitly unretained 8k qualification;
# add --small-flush only for the separate 1 MiB write cohort.

# Diagnostic binary is prepared independently and never used for timing.
python3 "$CAPTURE" prepare --source "$PWD" --output /tmp/algorithm-counter-prepared --family internal --overlay /tmp/algorithm-overlay/overlay.json
python3 "$CAPTURE" capture --source "$PWD" --prepared /tmp/algorithm-counter-prepared --output /tmp/algorithm-counters --family internal --overlay /tmp/algorithm-overlay/overlay.json --grant COORDINATOR_EXCLUSIVE_GRANT

PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s docs/benchmarks/treedb_algorithm_work_20261001 -p test_capture.py -v
```

Final decision collection needs repeated fresh-process serial cohorts, independently frozen profiles for internal traversal share, and all requested closure/debt observations. Report the actual repetition/noise policy before collection. Unmeasured larger-than-memory capacity stays open.

## Conditional implementation gate

Persistent deltas need reproducible whole-leaf materialization dominance after eligible existing checkpoint/coalescing controls, plus a design preserving overlay reads/scans, captured base/delta generation pins, acknowledged-LSN replay, sealed checkpoint closure, and reachability-based GC. Shared traversal needs actual internal visit reduction on naturally supplied batches and profiles showing that repeated internal work materially limits the equivalent owned/callback route. Both require a stable measured improvement target and allocation/read-tail guardrails before the coordinator activates a separate durable implementation child and updates the parent edges. Eligible children execute in this graph.

A negative decision requires measured cause/cost and a concrete revisit threshold. Counter opportunity alone, zero eligible backlog, noisy timing, or unmeasured capacity cannot close the decision as no-go. This harness makes no present go/no-go or speedup claim.
