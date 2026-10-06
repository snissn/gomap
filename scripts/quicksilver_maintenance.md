Use `quicksilver_maintenance.py` with the same reviewed native build manifest as
`unified_bench_quicksilver_capture.py`. It captures independently copied, closed
TreeDB fixtures under the existing `treemap compact` API. It changes no database
defaults, memory budget, durability policy, or maintenance scheduler.

Both this offline collector and `unified_bench_quicksilver_capture.py` use
`GOMEMLIMIT=off`, normal `GOGC=100`, `GOMAXPROCS=12`, and the existing
`TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES=1073741824` mapping-domain setting. They
retain these actual controls in environment receipts and override conflicting
ambient/build-manifest heap limits. The unified collector uses the same controls
for fresh/online/churn and retained final-read cells; the maintenance collector
uses them for every offline endpoint. Adapter defaults and the scheduler budget
remain unchanged. Every new baseline calibration and A/B cell must use the same
reviewed landed collectors and environment. Earlier 2 GiB heap-limit diagnostics
retain their original receipts; they supply no new calibration or numerical
comparison for this campaign.

Both collectors construct the same sanitized child environment and record its
complete contents as `env` with `environment_policy: sanitized-full-v1` in every
`run.json`. Only ambient `PATH`, `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ`,
`GLIBC_TUNABLES` and `LD_*` variables are inherited. Existing native checks still
reject unsupported loader injections. Other controls, including
`MALLOC_ARENA_MAX`, must be declared in the reviewed manifest `build_env`;
undeclared allocator settings and ambient credentials are excluded. Explicit
manifest variables are recorded in full, so do not put credentials there.
The frozen Go/mapping controls above and root-owned `TMPDIR` override manifest
values. Restore, native validation, `/usr/bin/time` and the measured child receive
this identical environment. Comparisons bind every recorded variable, and
calibration/analysis reject old allowlist-only receipts, missing markers,
undeclared recorded variables and drift from explicit manifest controls.
The original unified report validator and offline loader both check this
contract against the captured manifest, including the original executable-root
`TMPDIR`; replay does not inherit the analyzer's process environment.

Each cell supplies `label`, manifest `source`, absolute `fixture`,
`fixture_receipt: {path, sha256}`, `mode: full|exhaustive`, `batch_size`, and an
optional `timeout_seconds` (default 1800) and `endpoint` (default
`single-compact-v1`). The fixture receipt has `closed: true`,
`verified: true`, and `fixture_sha256` from the module's `fingerprint` function.
The coordinator reviews the attached full-value/miss/census evidence and source,
build, native-library and runner receipts; hash validation does not establish
their truth. Freeze exact product and landed harness/observer identities before
collection. A baseline may contain the identical landed instrumentation overlay;
record that composition in the source/build receipts.
Every comparison shares identical source, build, native-library and runner
receipt digests. Source/build receipts inventory all frozen product variants,
so A/B product entries differ within one common campaign inventory. Separate
capture directories may share this inventory; separately valid campaigns with
different runner or build receipts cannot be combined into one qualification.

The manifest also names `snapshot_restore`, a fixed source entry for
`cmd/quicksilver_snapshot_restore`. Build it from the frozen baseline plus landed
harness and use the same binary for all cells. A raw copy is byte-identical but
has different physical file identities. Before timing, the collector invokes
the existing explicit snapshot restore API, side stores first, then main. It
checks unchanged original bytes, unchanged payload files and unchanged file
counts/extents, retaining the rebound index fingerprint and restore receipts.
Ordinary recovery retains its identity checks. Restore, copying and hashing are
outside maintenance time and RSS; the final full logical oracle remains required.

The opt-in `endpoint: "compact-command-wal-settle-v1"` adds
`-command-wal-settle` to the native compact command. Its fixed sequence is one
applied Full/Exhaustive `CompactStorage`, capture/release of the exact state
token, next command LSN and recoverable-root summaries, the existing
`RefreshCommandWALCheckpointFallback`, `Checkpoint`, capture/release of the
post-refresh summaries, one existing `LeafGenerationGC`, and a dedicated
`CompactStoragePlan` final dry-run audit, followed by cleanup. It does not call
`CompactStorage` with `DryRun: true`: that API forces an applying operation.
There is no second applying compact, retry/settle loop, synthetic key, unlogged
root commit, new command frame, profile override or changed maintenance budget.

The endpoint requires an actual persisted command-WAL required feature and a
compatible persisted `command_wal_durable` or `command_wal_relaxed` profile before
writable Open. Non-command-WAL stores, absent/incompatible persisted profiles
and conflicting explicit profiles reject before replay or maintenance. The
capture campaign keeps its original durable acknowledgement contract. Refresh
may advance CommitSeq by one or lawfully do nothing; user/system roots, applied
command LSN, maximum entry revision and the next command LSN must remain equal.
Root-summary leases end before mutations. Cancellation is checked between
operations; the existing refresh/checkpoint APIs do not accept a context and
therefore cannot provide a mid-operation cancellation guarantee.

Raw stdout has `schema: 1`, `endpoint: "compact-command-wal-settle-v1"`, operation
`status`, and two original `reports`: the initial applied report and the final
`dry_run: true` audit. `refresh` retains `basis`/`result`, `next_lsn_before`/
`next_lsn_after`, `roots_before`/`roots_after`, refresh `status`,
`checkpoint_status` and `summary_before_status`/`summary_after_status`.
`initial_status`, `leaf_gc.status` with the original `leaf_gc.stats`,
`audit_status` and `cleanup_status` record actual execution. Scalar fields retain
their exported Go names. Errors preserve partial reports/counters/statuses and
an `error`; failed receipts remain diagnostic. Envelope `completed` means the
fixed operations and cleanup succeeded, not that final maintenance debt vanished.

The analyzer derives endpoint identity from raw stdout and the bound command,
validates token/root/WAL authority and every executed operation, and uses final
audit debt/completion flags for policy eligibility. It retains all initial
applied phase dispositions in `phases` and all final audit dispositions separately
in `audit_phases`; planned work is never reported as executed. Deferred or
unsupported phases in either view cannot qualify. Initial incomplete debt stays
visible in the original applied report. Whole-endpoint storage accounting uses
the initial `before` census and final audit `after` census. Legacy single-call
raw output and command behavior remain unchanged.

The baseline causal probe supports this bounded horizon-refresh hypothesis,
including unchanged decoded values and roots/RID/LSN across refresh. The landed
CLI still requires a native rehearsal on an independent verified baseline copy
with the full unified bytes/misses/live-key oracle and census. If final debt
remains, retain the failure and diagnose actual root references, resource closure
and pin ownership; do not add passes or weaken completion gates. After acceptance
and landing, collect three fresh completed baseline calibrations before any
candidate run, using the identical endpoint for every A/B cell. Earlier
single-call or incomplete smoke packets cannot supply this endpoint's calibration.

```sh
python3 scripts/quicksilver_maintenance.py capture /abs/manifest.json /abs/plan.json
python3 scripts/quicksilver_maintenance.py calibrate --metric elapsed_seconds \
  --out /abs/noise.json /abs/C1/run.json /abs/C2/run.json /abs/C3/run.json
python3 scripts/quicksilver_maintenance.py analyze /abs/pairs.json --out /abs/result.json
python3 -m unittest discover -s scripts -p test_quicksilver_maintenance.py
```

Plan: `{"output":"new-capture-dir","cells":[...]}`. Run three baseline
characterizations first, write the immutable noise packet, then execute
`A1/B1, B2/A2, A3/B3`. Pairs input has `calibration`, `calibration_sha256`, and
`pairs: [{"A":"/abs/A1/run.json","A_sha256":"...","B":"/abs/B1/run.json","B_sha256":"..."}, ...]`.
Freeze all six expected run packet SHA256 digests in the reviewed pair bundle
after collection; metadata changes cannot use their own raw-artifact hashes as
acceptance authority.
All three characterizations must be policy-completed with no `deferred` or
`unsupported` phase before a noise packet can be written. Analysis revalidates
their completion too; incomplete baselines cannot define the qualifying noise
threshold even when all subsequent pairs complete.
The analyzer recomputes `E=(max A-min A)/median A` and reports every pair's
fractional reduction. Material improvement requires all three favorable signs,
median reduction greater than `2E`, and all six policy-completed runs with equal
truthful completion flags (exhaustive also requires byte minimization). Equally
incomplete runs remain diagnostics even when faster. A `deferred` or `unsupported`
phase is also nonqualifying even when the reported debt and completion flags do
not represent it; retain the original report as diagnostic evidence.
`peak_rss_bytes` is also supported with its own pre-comparison noise packet.
Negative or noisy evidence leaves the parent gate open. Changed fixture contents,
mode, endpoint, fixed batch size, environment, source, harness, raw artifacts, RSS identity,
or execution order reject the comparison. Separate cells are needed per fixture,
mode and batch size. Full's policy completion never means byte minimization.

RSS observation runs in the waiting Python caller at 200ms. Co-timed anonymous,
file-backed and shared-memory components accompany the largest sampled RSS row;
they are separate from `/usr/bin/time -v` process HWM. These are whole-command
maintenance measurements, not online quiet-window residency. Cloning and hashing
are outside the compact command's timing. Cancellation reaps the owned process
group; failures retain raw output, samples, metadata and the writable copy.
For the command-WAL settle endpoint the measurement includes the applied
compact, summary captures, refresh, checkpoint, leaf GC, dedicated audit and
backend/side-store cleanup in one native process. It never reports only the
audit or GC as whole-command performance.
Peak temporary disk and per-phase CRC/decode byte counts remain unavailable.
Use the landed unified capture's `measure_dir` mode on a released successful copy
for the full bytes/misses/live-key oracle and final service-read guard. Do not
normally reopen failed originals. This single-metric analysis cannot replace
correctness, online eligibility/progress, read regression, or source-equality gates.

Online churn receipts from the unified collector include actual per-round
`pause_started_unix_nano` and `pause_finished_unix_nano` alongside monotonic
`pause_seconds`. Capture validates positive integer boundaries, snapshot and
round ordering, and wall/monotonic agreement within 1 millisecond. Quiet RSS
analysis must use the frozen 200 ms exclusion at both boundaries and actual
interior samples; absent interior observations cannot establish residency.
The fixed 1 ms sanity tolerance is 0.5% of the 200 ms boundary trim; wall and
monotonic clocks have no exact cross-platform equality contract. It is frozen
before calibration and does not excuse unordered stamps or larger clock drift.
Whole-command timing/RSS still includes all work. These markers do not force
maintenance or alter the workload or profile artifacts.
