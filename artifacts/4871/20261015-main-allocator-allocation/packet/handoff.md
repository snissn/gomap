# Main allocator projection handoff

Base: `3f87d74b98e8b1e809d151611be03785f85e2283`.
Frozen source reused: `7a6fd35f56dc6ddb70ae76e91c36656e7055a8df`.
Worktree: `/home/mikers/benchmarks/mvcc-4871-repair-20261013/allocation-main-20261015`.
Branch: `codex/5130-allocator-main-20261015` (uncommitted; root owns Git/GH).

18 changed paths are bound by `main-projection-source.json` SHA256
`2ed6e8ace61a4da3fb16edbf4b48be7d370b20759659ed5bf67cbb8bbece39f8`.
`main-projection.patch` includes all tracked edits and all three new test files.
Main snapshot production source is byte-identical to base; its existing
`lookupChunk` call uses the new canonical Patricia implementation. Main inline
append logic is retained. Readonly gates are inserted directly into main's two
entrypoints; no native recovery/registry functions were imported. Recovery docs
append only the allocator-format section. The new direct-leaf test uses main's
public `SnapshotPageUnusedV1` and covers wrong chunk, matching free page, and
matching live page. No OrdinalScanWork API/import is introduced.

## Validation

Full freelist passed (0.125s); focused DB Patricia/fresh/readonly/reopen/both-slot
fallback and format controls passed (0.028s). `main-focused.json/log` retain exact
commands, output, elapsed time and before/after owned-source census. Gofmt and
`git diff --check` passed. No full DB/race reruns were performed in this lease.

Public benchmark: unchanged main `BenchmarkConditionalTxnBaselineBatchWrite`,
10000-key seed, fixed1000x, foreground8 and stopped-timer Checkpoint, CPU0,
GOMAXPROCS1, three alternating fresh-process pairs. Baseline overlay restores
all changed Go inputs to exact base (new test files become package-only stubs),
with unchanged harness. Product inputs, overlay and binaries are hashed.

| median | main baseline | allocator candidate | change |
| --- | ---: | ---: | ---: |
| B/op | 152734 | 43074 | -71.80% |
| allocs/op | 281 | 139 | -50.53% |
| ns/op | 46297 | 22473 | -51.46% |

All three pairs improve. These are short paired public-path measurements;
latency remains directional. `summary.json`, `rows.json`, `public-commands.json`,
`binaries.json`, pair raw logs, `source-before.json` and `source-after.json`
retain exact evidence. Source census remains unchanged after all runtime.

Separate diagnostic-only allocator overlay prints existing txn stats after
successful materialization, through the same real public benchmark entrypoint.
It proves 295 private mutation paths / 235 private chunk reuses across Open,
seed, warmup and fixed8x, plus189 COW pages/774144 bytes. These aggregate counters
are activation proof, not the timed1000x denominator. Diagnostic printing never
enters either timed binary; overlay/source/binary/raw are separately identified
in `activation-*` evidence.

## Remaining gates

Root owns independent projection review, current-head CI/PR/merge and any
required rebinding after zipper lands. Main's encoding-flat-union profile has
not been recaptured here; prior measured allocator encoding routines are reused,
but prior ordinary/native profiles are not relabeled as main evidence. The
measured main source is exact3f87 plus this patch; a later zipper integration
needs its own source/baseline identity and affected validation. No Git/GH
mutation, cleanup, recursive delegation or production instrumentation occurred.
All generated evidence/binaries/overlays are retained; no paths released for
cleanup. Runtime slot released to root after the activation diagnostic.
