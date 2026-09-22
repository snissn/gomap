# Celestia Vlog Compression Notes (2026-03-26)

This note captures the 43k codec-shape rerun and the follow-up `run_celestia`
fast-profile measurements focused on end-run on-disk compression.

## 1) 43k codec-shape rerun

Command:

```bash
OUT_SUFFIX=oversized_refresh_20260326 scripts/rerun_43k_codec_shape_sweep.sh
```

Inputs:

- `/tmp/fast_43k_band.jsonl`
- `/tmp/wal_43k_band.jsonl`

Outputs:

- `/tmp/fast_43k_codec_shape_sweep_oversized_refresh_20260326.json`
- `/tmp/wal_43k_codec_shape_sweep_oversized_refresh_20260326.json`

Top results by `total_ratio`:

- fast:
  - `dict_h256k_s256k_noent k=32 ratio=0.0028739696`
- wal:
  - `dict_h256k_s256k_noent k=32 ratio=0.0028759450`

Observed trend:

- For this 43k band dataset, ratio continued improving with larger grouped
  frame K values up to `k=32`.
- Larger dict/history variants (especially `h256k/s256k`) remained top-ranked.

## 2) Celestia run (profile fast, auto+size)

Command:

```bash
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active \
TREEDB_OPEN_PROFILE=fast \
TREEDB_VLOG_COMPRESSION=auto \
TREEDB_VLOG_AUTO_POLICY=size \
STOP_AT_LOCAL_HEIGHT=10402350 \
~/run_celestia.sh
```

Artifacts:

- launcher log: `/tmp/cel_fast_size_policy_20260326224358.log`
- run home: `/home/mikers/.celestia-app-mainnet-treedb-20260326224423`
- node log: `/home/mikers/.celestia-app-mainnet-treedb-20260326224423/sync/node.log`

Run summary:

- duration: `328s`
- final local height: `10402026`
- dict publish line:
  - `treedb: dict training trained dict ... dict_bytes=40960 ...`
  - `treedb: value-log dict published ... k=8 ... total_ratio=0.005`

Pre-rewrite measurements:

- app DB bytes (`du -sb .../data/application.db`): `5436529252`
- main vlog bytes (`du -sb .../maindb/wal`): `5127859796`
- gzip sanity (`tar -C <home>/data -cf - application.db | gzip -1 | wc -c`): `3868527038`

Rewrite command:

```bash
/home/mikers/dev/snissn/celestia-app-p4/build/treemap-local \
  vlog-rewrite /home/mikers/.celestia-app-mainnet-treedb-20260326224423/data/application.db -rw
```

Rewrite output:

- `segments_before=22 segments_after=16`
- `bytes_before=5127855700 bytes_after=2146238755`
- `records=1003254`

Post-rewrite measurements:

- app DB bytes: `2185953906`
- main vlog bytes: `2146242851`
- gzip sanity: `1787216021`

Derived reduction:

- app dir reduction: `~59.79%` (`5.436G -> 2.186G`)
- gzip reduction: `~53.80%` (`3.869G -> 1.787G`)

## 3) Branch-side code changes tied to this note

- Added/updated sweep tooling:
  - `TreeDB/cmd/vlog_codec_shape_sweep/main.go`
  - `scripts/rerun_43k_codec_shape_sweep.sh`
- Expanded autotune dict candidate grids:
  - `TreeDB/internal/valuelog/autotune_options.go`
- Made size policy dict selection ratio-first:
  - `TreeDB/caching/vlog_autotune.go`
  - `TreeDB/caching/vlog_dict.go`

Validation:

```bash
go test ./TreeDB/caching -run 'TestValueLogAutotuneShouldSwitch|TestValueLogDictTrainerIOCost|TestValueLogDictCandidateK' -count=1
```

## 4) Bootstrap-K follow-up (2026-03-26 late pass)

To reduce bootstrap bias on large-value streams, we added:

- `TreeDB/internal/compression/trainer.go`
  - `DefaultTrainBootstrapMinRecords` raised to `32` (from `8`)
  - after first bootstrap profile accept, trainer resumes collecting for upgrade-followup

Validation:

```bash
go test ./TreeDB/internal/compression -count=1
```

Celestia run used:

```bash
LOCAL_GOMAP_DIR=/home/mikers/dev/snissn/gomap-phasehook-active \
TREEDB_OPEN_PROFILE=fast \
TREEDB_VLOG_COMPRESSION=auto \
TREEDB_VLOG_AUTO_POLICY=size \
STOP_AT_LOCAL_HEIGHT=10402026 \
~/run_celestia.sh
```

Run artifacts:

- launcher log: `/tmp/cel_fast_size_bootstrap32_20260326232729.log`
- run home: `/home/mikers/.celestia-app-mainnet-treedb-20260326232754`

Observed dict behavior:

- `dict training trained dict ... dict_bytes=65536 samples=32 ...`
- `value-log dict published ... k=16 ...`

Measured bytes:

- pre-rewrite app dir: `5610552827`
- pre-rewrite gzip: `4000189977`
- rewrite:
  - `segments_before=23 segments_after=17`
  - `bytes_before=5268562411 bytes_after=2227827818`
- post-rewrite app dir: `2268092050`
- post-rewrite gzip: `1815227655`

Near-height comparison reference (older run, pre bootstrap-K tweak):

- run home: `/home/mikers/.celestia-app-mainnet-treedb-20260326230729`
- dict behavior there: `dict_bytes=40960`, `samples=8`, `k=8`
- post-rewrite app/gzip: `2219558390` / `1811956666`

Takeaway:

- bootstrap behavior moved from `k=8` to `k=16` with larger first-pass sample windows.
- post-rewrite gzip remained in the same band despite height drift; fixed-height
  freezing is still recommended for strict A/B comparisons.

## 5) Notes

- A one-time forced second-stage retrain hook was tested during this session and
  reverted due noisy/inconclusive run-to-run behavior under varying final
  heights.
- For future A/Bs, prefer fixed-height or frozen-remote runs to reduce drift in
  pre-rewrite byte comparisons.

## 6) Template Lab Sweep (2026-03-27)

Goal:

- Evaluate template pre-transform opportunities with class-isolated corpora:
  - pointer-backed value payloads
  - outer-leaf page payloads
- Compare baseline (`off`) vs template configs and `header_v1` outer-leaf pre-transform.

Corpus extraction:

```bash
go run ./TreeDB/cmd/template_corpus_extract \
  -app-dir /home/mikers/.celestia-app-mainnet-treedb-20260326232754/data/application.db \
  -out-dir /tmp/template_corpus_run_20260327_120046 \
  -pointer-limit 0 \
  -outer-leaf-limit 50000 \
  -pointer-stride 1 \
  -outer-leaf-stride 1 \
  -overwrite
```

Extracted corpus summary:

- pointer records: `21401`
- pointer bytes: `880,146,915`
- outer-leaf records: `50000`
- outer-leaf bytes: `204,800,000`

Sweep commands:

```bash
go run ./TreeDB/cmd/template_lab \
  -corpus-dir /tmp/template_corpus_run_20260327_120046 \
  -dataset both \
  -outer-leaf-pretransform off \
  -disable-mask-templates true \
  -warmup-passes 1 \
  -measure-passes 1 \
  -sweep-min-savings 1,4,8 \
  -sweep-fingerprint-k 8 \
  -sweep-max-fetch 8,16 \
  -include-off=true \
  -out-json /tmp/template_lab_run_off_20260327.json \
  -out-md /tmp/template_lab_run_off_20260327.md

go run ./TreeDB/cmd/template_lab \
  -corpus-dir /tmp/template_corpus_run_20260327_120046 \
  -dataset both \
  -outer-leaf-pretransform header_v1 \
  -disable-mask-templates true \
  -warmup-passes 1 \
  -measure-passes 1 \
  -sweep-min-savings 1,4,8 \
  -sweep-fingerprint-k 8 \
  -sweep-max-fetch 8,16 \
  -include-off=true \
  -out-json /tmp/template_lab_run_header_v1_20260327.json \
  -out-md /tmp/template_lab_run_header_v1_20260327.md
```

Results:

- pointer corpus:
  - template keeps: `20092 / 42802 attempted`
  - encoded bytes: `876,711,183` vs raw `880,146,915`
  - byte savings: `0.3904%`
  - gzip sanity: `427,462,007` vs raw gzip `428,482,010`
- outer-leaf corpus:
  - template keeps: `0 / 100000 attempted` (all tested configs)
  - encoded bytes: no change from raw
  - `header_v1` pre-transform: no keep increase and no encoded-byte reduction

Takeaways:

- Current template path yields a small but measurable gain on pointer values.
- Outer-leaf payloads currently do not admit profitable template frames under this prototype/config set.
- `header_v1` pre-transform did not improve outer-leaf template effectiveness.
- For reproducible lab runs we used `-disable-mask-templates=true` (anchor-only), while mask-template behavior remains a separate correctness/perf investigation thread.

## 7) Mask Template Correctness Fix + Rerun (2026-03-27)

Issue found:

- Sparse mask encoding reused the input value slice and mutated it in place.
- This caused deterministic decode-mismatch failures in the lab harness and could corrupt caller-owned buffers.

Fix:

- `TreeDB/template/match.go`: removed in-place sparse mask payload construction; sparse payload encoding is now non-mutating.
- Added regression test:
  - `TreeDB/template/engine_test.go::TestEncodeMaskTemplateSparse_DoesNotMutateInput`

Validation:

```bash
go test ./TreeDB/template -count=1
```

Mask-enabled rerun commands:

```bash
go run ./TreeDB/cmd/template_lab \
  -corpus-dir /tmp/template_corpus_run_20260327_120046 \
  -dataset both \
  -outer-leaf-pretransform off \
  -disable-mask-templates=false \
  -warmup-passes 1 \
  -measure-passes 1 \
  -sweep-min-savings 1,4,8 \
  -sweep-fingerprint-k 8 \
  -sweep-max-fetch 8,16 \
  -include-off=true \
  -out-json /tmp/template_lab_run_mask_on_off_20260327.json \
  -out-md /tmp/template_lab_run_mask_on_off_20260327.md

go run ./TreeDB/cmd/template_lab \
  -corpus-dir /tmp/template_corpus_run_20260327_120046 \
  -dataset both \
  -outer-leaf-pretransform header_v1 \
  -disable-mask-templates=false \
  -warmup-passes 1 \
  -measure-passes 1 \
  -sweep-min-savings 1,4,8 \
  -sweep-fingerprint-k 8 \
  -sweep-max-fetch 8,16 \
  -include-off=true \
  -out-json /tmp/template_lab_run_mask_on_header_v1_20260327.json \
  -out-md /tmp/template_lab_run_mask_on_header_v1_20260327.md
```

Results:

- Pointer corpus (best row from mask-enabled off run):
  - `tmpl_ms4_fk8_fetch16`
  - encoded bytes: `132,634,951` vs raw `880,146,915`
  - encoded savings: `84.93%`
  - encoded gzip: `19,144,812` vs raw gzip `428,482,010`
- Outer-leaf corpus:
  - template keeps remained `0` across tested configs
  - encoded and gzip bytes unchanged from raw
- `header_v1` still did not improve outer-leaf keep rate in this pass.

Takeaway:

- Mask templates are now usable/correct in this harness and dramatically improve pointer-value compression on this corpus.
- The outer-leaf side remains the active gap and still requires a different transform/routing strategy.

## 8) Outer-Leaf Transform Follow-up (2026-03-27)

Prototype:

- Added `header_dir_delta_v1` in `template_lab`:
  - carries `PageID+Checksum` as side bytes (same as `header_v1`)
  - delta-encodes outer-leaf columnar-prefix directory metadata in-place (key dir, value dir, prefix dir)
  - reverse path restores original bytes exactly (lossless round-trip tests added in `TreeDB/cmd/template_lab/main_test.go`)

Focused run (20k outer-leaf pages):

```bash
go run ./TreeDB/cmd/template_lab \
  -corpus-dir /tmp/template_corpus_run_20260327_120046 \
  -dataset outer_leaf \
  -max-records 20000 \
  -outer-leaf-pretransform header_dir_delta_v1 \
  -disable-mask-templates=false \
  -warmup-passes 1 \
  -measure-passes 1 \
  -sweep-min-savings 1,4,8 \
  -sweep-fingerprint-k 8 \
  -sweep-max-fetch 8,16 \
  -include-off=true \
  -out-json /tmp/template_lab_outer_header_dir_delta_20k.json \
  -out-md /tmp/template_lab_outer_header_dir_delta_20k.md
```

Aggressive follow-up (same corpus/run size):

```bash
go run ./TreeDB/cmd/template_lab \
  -corpus-dir /tmp/template_corpus_run_20260327_120046 \
  -dataset outer_leaf \
  -max-records 20000 \
  -outer-leaf-pretransform header_dir_delta_v1 \
  -disable-mask-templates=false \
  -warmup-passes 1 \
  -measure-passes 1 \
  -wait-after-warmup-ms 10000 \
  -sweep-min-savings 1,4,8 \
  -sweep-fingerprint-k 8 \
  -sweep-max-fetch 8,16 \
  -template-train-sample-stride 1 \
  -template-synthesize-every 8 \
  -template-min-anchor-freq 2 \
  -template-min-presence-ratio 0.6 \
  -template-min-publish-savings 1 \
  -template-min-publish-ratio 0.7 \
  -template-cold-search-after 1000000000 \
  -template-cold-search-probe-every 1 \
  -include-off=true \
  -out-json /tmp/template_lab_outer_header_dir_delta_20k_aggr.json \
  -out-md /tmp/template_lab_outer_header_dir_delta_20k_aggr.md
```

Observed (both baseline and aggressive):

- outer-leaf keeps: `0` across all tested rows
- encoded bytes: unchanged from raw
- reason counters (aggressive): `tmpl_no_candidates=40000`, `templates_published=0`

Conclusion:

- Even with directory normalization and permissive training/cold-search settings, this corpus did not synthesize usable outer-leaf templates.
- Next optimization effort should prioritize non-template paths for outer-leaf pages (block/dict strategy and/or outer-leaf-specific codec work), while retaining template focus primarily for pointer-value payloads.
