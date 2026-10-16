# First-ACK wall-time/work measurement

This is a source-only packet. No Go compiler, test body, benchmark build, sample, profile, Git or index mutation ran. Pinned gofmt formatted the four selected test files. Root independently accepted repair08 focused and actual controls; those historical results are prerequisites, not results for this new extraction.

The benchmark runs `nativeSelectedOriginalSplitDiagnosticV6(b, "once-ack", &meter)`. The six-arm test wrapper still supplies nil. The same backend census, ready-envelope validation and constructor ledger now accept testing.TB. All original setup/33 versions/floor32, physical41/9 inputs, checked prebirth G, ready/promotion/retained21-row and named-root oracles, actual detached Finish boundary/receipt, Commit34, held reader after native publication and after writer, exact one-record/one-batch ACK, repeated cursor Close, DB.Close and reopened deletion/anchors/floor/newer assertions remain in their original shared runner. No oracle is copied. No production file or G expression changes. F/G/P/retention and Q32 unresolved status are unchanged. The optional meter is test infrastructure outside the selected constructor P domain; this packet makes no aggregate heap/retained measurement claim.

Each ordinary call closure sets options.Cursor first. Nil meter follows the original call. A nonnil meter reads starting phase before the window (-1 denotes nil-cursor birth), obtains monotonic start immediately before public PruneVersions and end immediately after return, then records counters/raw sample before assigning the returned cursor or checking stats. Two time.Now calls are the only new per-call timing probes. No per-call StartTimer, StopTimer, ReadMemStats, logs, allocation probes, observer or callback hook is inserted. A call may cross phases: entry phase is only a label, never a phase-cost decomposition.

The benchmark checks b.N==1 before fixture effects, calls StopTimer and ResetTimer once before setup and never restarts the framework timer. Pinned testing source proves ReportMetric overrides normal ns/op and ResetTimer preserves stopped state. Initial framework timer/probe activity is outside fixture setup. No ReportAllocs or benchmem is requested. No B/op or allocs/op claim is permitted even if unrelated external settings print those fields.

Per-return WorkRecords/WorkBytes are the actual PruneStats counters, reset by the production public method per invocation. Integer totals have overflow checks. Raw samples and exact integer aggregate totals are logged only after the complete original oracle succeeds. Their allocation/bookkeeping is outside individual windows but can influence later cache/GC/scheduling; no correction or overhead subtraction is made. Setup/census is excluded from both wall metrics. Writer/observations/meter gaps/scheduling between first public call and first exact ACK are excluded from public-call sum and included in whole birth-to-ACK interval. The whole endpoint is the captured ACK return, before ACK bookkeeping, Close or reopen. Every post-ACK cleanup/reopen oracle still must succeed before a metric is reported.

Metrics (one selected operation, b.N==1):

| Metric | Meaning |
|---|---|
| ns/op | Sum of monotonic public PruneVersions call windows from birth through exact first ACK |
| birth-to-ack-ns/op | Whole first public entry to exact ACK return, including all designated intercall gaps |
| max-call-ns/op | Maximum single public-call window |
| public-calls/op | Actual number of public invocations through ACK |
| work-records/op / work-bytes/op | Actual sum of per-return work counters |

Three samples require three separate executions of the SAME prebuilt nonrace ELF, exact benchmark selector, run=^$, benchtime=1x, count=1, and no benchmem. There is no calibration fixture, retry, discarded sample, speedup claim or full-prune throughput claim. Root must bind exact source/compiler/ELF/argv/env, fresh TMPDIR checks, quiet-host preflight and real process/subreaper/group joins before selecting runtime. Any semantic failure invalidates its sample and stops the sequence. Build and both full six-arm normal/race controls are proposed only. Existing repair08 focused tests outside these shared files remain source evidence; no additional runtime applicability is asserted here.
