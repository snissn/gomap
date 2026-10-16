# First-ACK cost plan: conditional design ACCEPT

Independent read-only advice by gpt-6.1-sol/high. No source construction, execution, numeric allowance or native qualification is approved by this report.

The proposed measurement is useful as an initial cost baseline for the selected genuine public route. Its prerequisite must reference the newly selected exact successor and successful focused plus genuine once-ACK/paused-Finish Close controls, rather than repair07, whose successor fixtures failed. Repair08 source acceptance alone does not establish those runtime prerequisites.

## Smallest shared-fixture extraction

Retain the existing `TestNativeSelectedOriginalSplitDiagnosticV6` table wrapper and all arms. Change its inner diagnostic runner, backend census, ready-envelope validator and selected-constructor-ledger helper to accept `testing.TB`. The ledger refusal helper already accepts only `Fatalf`, so needs no wider interface. Have the benchmark invoke the same inner runner's once-ACK arm with an optional test-only call meter; tests use a nil meter. This shares setup, prebirth checked G, ready-envelope/retention observations, genuine detached/accounting-incomplete receipt boundary, actual Commit34, held-reader checks, original Finish, once-only ACK, cursor Close, public DB.Close and reopen assertions. Do not fork the approximately 380-line oracle into a second benchmark implementation.

Place the optional meter around the one existing `s.PruneVersions(options)` invocation. Capture the starting phase before entering; a nil cursor has a distinct birth bucket. Stop the wall and allocation windows immediately on return, before assigning the returned cursor, checking stats, logging, appending samples or performing observations. Accumulate call count, sum/max wall time and actual per-call work records/bytes; the original cursor method resets the work object for each public invocation and returns that invocation's consumption. A call can span several phases, so starting-phase buckets are attribution labels only. Stop the measurement at the first return with exactly one pruned record and one batch; preserve all earlier zero-ACK checks. Subsequent Close/reopen stays in the shared oracle and outside public-call sums.

Use a fixed-size phase accumulator or append raw samples only after the window closes. Avoid meter bookkeeping or log formatting inside the public-call window. Keep the whole birth-to-first-ACK wall interval separately: it includes the writer, observer calls, meter gaps and scheduling. Do not subtract these gaps from that whole-interval metric or relabel it native-only time.

## Timing and allocation boundaries

The benchmark begins stopped. After setup, ResetTimer once while stopped, then explicitly StartTimer/StopTimer for each public invocation; ResetTimer does not start a stopped timer and resetting between calls would erase previous windows. Report the manually summed invocation wall time explicitly, since ordinary benchmark elapsed time includes small adapter/clock overhead at its boundaries. The benchmark must perform one operation (`b.N == 1`, checked before fixture effects); three fresh prebuilt processes with exact selector, `-test.run=^$`, `-test.benchtime=1x` and `-test.count=1` supply three samples, not three histories in one process. No RunParallel, calibration run, discarded sample or retry-to-pass.

Pinned Go's StartTimer and StopTimer read process-wide runtime memory statistics. These reads are outside the elapsed timer at their respective edges, can perturb GC/cache/scheduling, and allocation edges are not perfectly identical to the invocation wall-time edges. Allocation counts include other goroutines in the process during the windows. Report them as process allocation observed during these probe-conditioned public-call windows, not goroutine allocation, an uninstrumented latency estimate, or retained memory. Observers and Commit34 remain outside the primary sum but can affect later GC/cache state; their cost is included in the whole interval. Retained P keeps its existing selected constructor-envelope domain and cannot be inferred from allocation deltas.

## Required validation and limits

Before measurement, source review should confirm that extraction preserves all six test arms and exact failure/ACK/held-reader/floor/encoded-value/reopen oracles, and that a meter adds no product callback or changed scheduling. Focused source-bound normal/race controls must continue to exercise the shared tests. The eventual benchmark must fail on every semantic error, retain all raw counters and per-process identity, use the same prebirth formula and selected predicates, and stop the sequence on first failure. Source/toolchain/compiler/ELF/argv/environment provenance and quiet-host evidence belong to the separately selected runtime packet.

No timing, allocation sample, profile, benchmark build or new test was run for this advice. There is no old-route equivalence or speedup claim. Original Q32 remains blocked; continuing-write progress, physical-Finish generality, concurrent containing-admission limits, C1/C3/all19 and other native graph gates are unchanged. This is an implementable test-only design recommendation, not acceptance of a benchmark source which does not yet exist.

## Inspected bindings

- plan: `80b39a30aaf0f8a96d3231b3876a534caed701f4c9d3c6a3c6480feeeeaca1f7` — `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-decision/root-native-firstack-cost-plan.md`
- diagnostic: `7b2d04223f54617e3adfa29467671c2819a8d1a9cd00ee923303d9568b8dd396` — `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-decision/repair05-review-source/TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go`
- envelope: `c84d8e74682e0024f963c29283f613e32189e4cdaa5488940bbc063a9d6a7e4f` — `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-decision/repair05-review-source/TreeDB/mvcc/native_finaldriver_envelope_test.go`
- precursor: `02ecc538748a09db4afbb34df7c06c7523772bb1f9c88fed00cf797b5ab925c1` — `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-decision/repair05-review-source/TreeDB/mvcc/native_finaldriver_precursor_test.go`
- ledger: `3353e402cf3545339346061775dcba60b03bc85174c86690748572e267dfc41a` — `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-decision/repair05-review-source/TreeDB/mvcc/native_selected_ledger_test.go`
