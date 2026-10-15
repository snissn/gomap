# #5133 final-main qualification

Base 3f87d74b98e8b1e809d151611be03785f85e2283; candidate cb5960399df0c23e320193b64d8b6a5db5844c3e. The candidate source hashes matched the independently reviewed four-file freeze. Root committed/pushed byte-equivalent inputs during build setup; -buildvcs=false means that metadata transition does not change the compiled source. Before/after SHA256 census (3471 Go/module inputs) is identical; candidate worktree is clean afterward.

The cleanmain baseline uses a Go build overlay replacing only modified zipper.go with exact git-show base3f87 source bytes. Other tracked product and canonical db benchmark inputs are exact cleanmain. New zipper dependency test files do not participate in a db test binary. Saved overlay/original source, binaries/hashes, every argv/env, and raw output bind this comparison. No product code or fixture was edited during qualification.

Full zipper, pointer checkpoint/write-sync reopen and value-log GC removal, and focused race checks passed. The full zipper run includes actual public Batch.Write retained-snapshot/owned-Get checks plus clone selection, concurrent borrowers, retention bounds, repeated split/delete/overwrite, and error/retry tests.

Three alternating fixed1000x public pairs use taskset CPU0, GOMAXPROCS=1, Go1.26.3, CGO_ENABLED=1, GOWORK=off, GOTOOLCHAIN=local. The canonical harness seeds10000 keys, measures groups of8 public writes, and drains Checkpoint outside the timer. All asynchronous-publication boundary guards passed. No profiling observer was active.

| Median | Baseline | Candidate | Delta |
|---|---:|---:|---:|
| B/op | 152734 | 141606 | -7.286% |
| allocs/op | 281 | 269 | -4.270% |
| ns/op | 45653.0 | 44701.0 | -2.085% |


These three pairs meet the early directional allocation/no-regression gate. Short latency samples show cold/host variation; no qualified speedup is claimed and this does not replace final graph eight-repeat gates/current-head CI. The original rate1 exact-public Write profile established recurring scratch attribution, separately bound to ordinary177/v1; it is not relabeled as final-main profile evidence.

Existing cache limits, operation-local owning persisted-leaf bytes, no added global pool, and oversized metadata trimming remain documented in scratch-reuse.md. Final-main source passes this packet; root owns remaining CI/review and graph acceptance.

The run wrote receipt.json successfully before report generation hit a Python dictionary-key quoting error; only report generation was repaired afterward, with no measurement rerun or source change.

Runtime lease released with no Go/test/benchmark process. No generated paths were deleted or released; source/evidence remain under root custody.
