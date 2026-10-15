# Caller and timer audit

Fixed product: 2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49. Harness base: 5bca87ffe6e1bfba2d693264d2dc82936adc234f. All six implementation paths are new; existing tracked product, M0 workflow/capture/summarizer files remain unchanged.

| Path/caller | Actual boundary and disposition |
| --- | --- |
| workflow manual dispatch | Separate exact harness checkout at github.sha and fixed pristine product checkout. Ephemeral ubuntu24.04, contents:read, no inputs/secrets/PR/push/selfhosted trigger. 20min cap unchanged. |
| fixed candidate observer import | SHA256 checked58c58c… file imported from PRODUCT, not assumed available on main. Existing ProcessObservation/allowed_processes/tick accounting/2% rule reused. Python3.10.12 import+observe+finish control passes. |
| module preparation | Normal download+verify before freeze; exact candidate source equality required. GOPROXY/GOSUMDB off afterward. Runtime SDK official archive+actual executable verified; complete SDK hash manifest retained. |
| quiet preflight | One nominal250ms sampled10s/2% window; no wait/retry until quiet. Refusal starts zero fixture/benchmark calls; compilation/preparation already occurred and is recorded. |
| original two fixtures | Exact names, count1 and original -v on artifact fixture. Fresh go test and actual -exec ELF preserved. No overlay body edit. Offline128doc fixture remains distinct from1024doc timed workload. |
| BenchmarkVacuumIndexOnlineCollectionForegroundChurn | Delegates original helper with legacy=true. Same bytes64x selector,1x/count1/benchmem. Original setup1024docs+large catalog and coordinator drain/Stop occur before initial observation. |
| benchmarkVacuumIndexOnlineCollectionForegroundChurn | Exactly two inserted calls: m0ObservationBegin immediately BEFORE ResetTimer; m0ObservationEnd immediately AFTER StopTimer. Entire ResetTimer→StopTimer bytes unchanged. Foreground goroutine is created only inside subsequent timed loop, so initial ReadMemStats cannot interrupt a prestarted foreground operation. Existing setup/runtime goroutines and subsequent GC scheduling may still be perturbed. |
| vacuumCollectionForegroundWriter | Entire function/suffix byte-identical. Original start/warmed/precutover barriers,4096 alternating operations, time.Now→time.Since interval, Set/batch/Close, allocations and error behavior unchanged. No new per-operation logging/timestamps/work. |
| m0ObservationEnd | After timer stop, preserves original latencies slice order/contents. Exports zero-based ordinal/parity type and all durations, call wall span and before/after MemStats counters/PauseEnd+PauseNs ring. Existing original exposure/p95/p99 logic still executes. |
| PL06 public benchmark | Separate original body untouched. Shared overlay helper has no init side effects; no calls inserted on this path. |
| Go -exec wrapper | Receives actual temp ELF path+args, copies/hashes it before setup, then os.execv same original path/argv. Positive toy exit7 and ELF hash control passes. Copy/hash creates diagnostic I/O/cache effects. |
| Runner.command | New session/group; original fresh Go driver is retained. Default original10min Go timeout, outer cap min600s/remaining19min budget. SIGTERM/SIGINT raise into kill-group/join, Linux subreaper joins adopted descendants; toy fork timeout control passes. |
| summarizer | Exact original candidate script and script tests; original nonzero result propagated. No metric/threshold/row removal. Telemetry busy/unavailable is diagnostic labeling, never waiver. |
| finally/upload | Before/after source manifests and equality, errors, raw attempts, SDK/source archives, actual binaries, group receipts, hashes always attempted. Upload explicitly excludes only task cache/modcache/tmp. Runner loss/job timeout can prevent final upload; this limitation is stated. |

The pure control restores the two inserted call-site lines and proves byte-for-byte equality to the checked benchmark preimage. A separate assertion proves original writer bytes unchanged. The local compile-only overlay maps to the exact same helper bytes retained in final freeze. No Go fixture/test body/benchmark/profile was executed. No M0 diagnostic dispatched.

Telemetry samples actual wall+monotonic time; nominal250ms sampling may overrun. Raw proc stat/vmstat/diskstats/pressure plus process stat/status/io and visible unified-cgroup leaf/ancestor quota/throttle counters preserve units. Explicit availability/reason flags cover missing/denied/v1 sources. Entirely short-lived processes, hidden ancestors/hypervisor activity, scheduler waits and GC assists remain blind spots. Pause ring may wrap; durations/ordinals cannot reconstruct exact operation wall starts across unmeasured gaps.
