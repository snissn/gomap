Use `quicksilver_maintenance.py` with the same reviewed native build manifest as
`unified_bench_quicksilver_capture.py`. It captures independently copied, closed
TreeDB fixtures under the existing `treemap compact` API. It changes no database
defaults, memory budget, durability policy, or maintenance scheduler.

Each cell supplies `label`, manifest `source`, absolute `fixture`,
`fixture_receipt: {path, sha256}`, `mode: full|exhaustive`, `batch_size`, and an
optional `timeout_seconds` (default 1800). The fixture receipt has `closed: true`,
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
mode, fixed batch size, environment, source, harness, raw artifacts, RSS identity,
or execution order reject the comparison. Separate cells are needed per fixture,
mode and batch size. Full's policy completion never means byte minimization.

RSS observation runs in the waiting Python caller at 200ms. Co-timed anonymous,
file-backed and shared-memory components accompany the largest sampled RSS row;
they are separate from `/usr/bin/time -v` process HWM. These are whole-command
maintenance measurements, not online quiet-window residency. Cloning and hashing
are outside the compact command's timing. Cancellation reaps the owned process
group; failures retain raw output, samples, metadata and the writable copy.
Peak temporary disk and per-phase CRC/decode byte counts remain unavailable.
Use the landed unified capture's `measure_dir` mode on a released successful copy
for the full bytes/misses/live-key oracle and final service-read guard. Do not
normally reopen failed originals. This single-metric analysis cannot replace
correctness, online eligibility/progress, read regression, or source-equality gates.
