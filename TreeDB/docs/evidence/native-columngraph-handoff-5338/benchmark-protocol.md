# Ordinary ColumnGraph lifecycle measurement protocol (#5338)

This is a successful public lifecycle cost control. It does not force the captured-prefix overlap and does not establish a speedup. The separate public handoff witness and original lifecycle tests establish correctness. No measured result is claimed by this protocol.

## Frozen inputs

Use baseline commit `d7db5409e20412f4dac4e1a3f902484b05e014e1`, tree `39a87e47f3740e4056f25196da54a9f4e35eb159`, with only the identical lifecycle benchmark source overlay. The common `shared_cow_write_bench_test.go` helper remains exact baseline SHA-256 `45d2186bbc6ece189f56ee4fd44226250ed2a9e458d739d3dd4eb34ecaec6826`. Supply the actual candidate commit/tree returned by the authenticated GitHub adapter after staging, discovery refresh and source validation. The freezer requires the local staged candidate tree to match that exact tree and refuses unstaged or untracked source changes.

`native_graph_lifecycle_benchmark.py freeze` records every repository Go/build input's size and SHA-256, aggregate production/test/source hashes, the two harness hashes, HEAD/tree/status observations and the authenticated identities. The Git trees additionally bind tracked assets; `go.mod` and `go.sum` bind dependencies. The runner and Python identities are recorded. HEAD alone does not describe the baseline overlay or a locally staged candidate.

From the evidence directory, after the parent supplies the actual authenticated candidate IDs:

```sh
python3 native_graph_lifecycle_benchmark.py freeze \
  --baseline-root /workspace/scratch/0d9ab3f5d72e/gomap-5338-baseline \
  --candidate-root /workspace/scratch/0d9ab3f5d72e/gomap-5338 \
  --candidate-commit ACTUAL_FULL_CANDIDATE_COMMIT \
  --candidate-tree ACTUAL_FULL_CANDIDATE_TREE \
  --tmpdir /workspace/scratch/0d9ab3f5d72e/evidence/lifecycle-runtime-tmp \
  --output lifecycle-source-freeze.json
```

The parent builds normal benchmark binaries sequentially from those frozen sources with the same Go toolchain. Keep each exact build command, working directory, relevant environment, toolchain/build-info output, source-freeze hash, original stdout/stderr and exit status in a build receipt. The runner never builds Go binaries and requires both build receipts. It validates that frozen source bytes still match before binding the supplied executable hashes. A binary hash identifies the executable; the retained build receipt establishes its relationship to the frozen source.

Each build receipt is JSON with at least `command` (an argv array), `cwd`, `exit_status` (zero), `go_version`, `source_freeze_sha256` and `binary_sha256`. Keep the original build output paths/hashes and environment in additional fields. The runner verifies these required bindings, the exact build directory and equal Go versions; it rejects a race build for this normal-binary comparison.

The separate `build_native_graph_lifecycle_benchmark.py` helper creates those receipts. After all other test/build work ends, invoke it for baseline and wait for completion before invoking candidate. It verifies frozen source bytes before and after compilation, uses the explicit local toolchain, retains original compiler output/status and records binary build information. Optional `--gocache` and `--gomodcache` select the same existing task-owned caches used for validation; otherwise it inherits those environment settings.

```sh
python3 build_native_graph_lifecycle_benchmark.py \
  --freeze lifecycle-source-freeze.json --arm baseline \
  --go /workspace/scratch/0d9ab3f5d72e/toolchain/go/bin/go \
  --output lifecycle-build-baseline
python3 build_native_graph_lifecycle_benchmark.py \
  --freeze lifecycle-source-freeze.json --arm candidate \
  --go /workspace/scratch/0d9ab3f5d72e/toolchain/go/bin/go \
  --output lifecycle-build-candidate
```

The exact compiler operation is `go test -c -p 1 -o OUTPUT/lifecycle.test ./TreeDB/documentservice`. Neither helper executes a benchmark during compilation. The receipt preserves `compiler_exit_status`; a source change or missing output also makes the overall build receipt fail, even if the compiler returned zero.

## Fixed execution

The runtime environment is `GOWORK=off`, `GOMEMLIMIT=4GiB`, `GOMAXPROCS=2` and the same explicit owned `TMPDIR`. Other inherited Go/runtime settings are recorded at freeze and must still match when measurement begins. Product options remain native `command_wal_durable`, `cow_btree`, 4096 rows, 128 rows per request, one writer, original finite limits and disabled periodic/idle checkpoint triggers. Each benchmark invocation owns a fresh database directory internally.

Run six sequential processes in the predeclared order baseline, candidate, baseline, candidate, baseline, candidate. Each binary invocation selects only `BenchmarkOrdinaryColumnGraphNativeLifecycle4096`, uses `-test.benchtime=1x -test.count=1 -test.benchmem -test.v`, and has a 180-second external deadline. Do not overlap measurement with builds, tests, another sample or profiling. Do not replace failed samples or use successful retries to erase an original failure.

```sh
python3 native_graph_lifecycle_benchmark.py run \
  --freeze lifecycle-source-freeze.json \
  --baseline-binary /ABSOLUTE/PATH/TO/baseline-lifecycle.test \
  --candidate-binary /ABSOLUTE/PATH/TO/candidate-lifecycle.test \
  --baseline-build-receipt /ABSOLUTE/PATH/TO/baseline-build-receipt.json \
  --candidate-build-receipt /ABSOLUTE/PATH/TO/candidate-build-receipt.json \
  --output lifecycle-measurements
```

Output directories must be fresh. Every sample runs in a fresh Python worker that launches exactly one benchmark binary and performs no other subprocess operation. Its initial `resource.getrusage(resource.RUSAGE_CHILDREN)` must be zero. Its final child usage records CPU time and peak RSS; Linux `ru_maxrss` is normalized from KiB to bytes, while Darwin uses bytes. This avoids carrying the maximum of an earlier sample into a later one. The measurement does not require `/usr/bin/time`.

Each sample retains original benchmark stdout/stderr and hashes, exact command/cwd/runtime environment, wall duration, CPU usage, initial/final rusage, peak RSS, original exit status, timeout status, source/benchmark/helper/binary identities and parsed metrics. Worker stdout/stderr and status are retained separately. A timeout kills only the spawned benchmark process group and remains a failed sample. Exit zero without the expected benchmark result, all three phase records, the completed graph observation and the post-Close record is invalid. The coordinator continues through the predeclared samples without retry, retains every failed/invalid result and returns nonzero unless all six complete.

## Interpretation

Standard `ns/op`, `B/op` and `allocs/op` cover seed, Optimize and explicit Checkpoint only. The per-phase log records elapsed time and process-wide allocation deltas, unforced start/end heap samples and the original COW publication/handoff/checkpoint/accounting counters. `cow_peak_B` is the largest owner `peak_bytes` value observed at these three phase ends. Graph status/manifest, all document/vector/metadata values, serving, original Close, physical-pin cleanup and fresh reopen are checked outside the timer.

Process wall time, CPU and peak RSS include setup, observations, full validation, Close and reopen. `LIFECYCLE_CLOSED` is a process heap sample after both owners close and an untimed GC; expected inputs remain alive and shared/runtime caches are included. Neither process RSS nor this retained heap sample is graph-only memory. These scopes must not be substituted for one another.

`summary.json` reports median/minimum/maximum only when all three planned samples for an arm complete. Every original sample remains in `samples.json`. No percentage improvement, speedup acceptance threshold, original #5332 incident attribution or power-loss qualification follows from these measurements. Keep the benchmark and source/binary/runtime bindings with the final evidence report; the standalone logs are not benchprof inputs.
