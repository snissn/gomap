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
`pairs: [{"A":"/abs/A1/run.json","B":"/abs/B1/run.json"}, ...]`.
The analyzer recomputes `E=(max A-min A)/median A` and reports every pair's
fractional reduction. Material improvement requires all three favorable signs,
median reduction greater than `2E`, and equal truthful completion flags.
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
