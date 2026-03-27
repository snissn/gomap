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

## 4) Notes

- A one-time forced second-stage retrain hook was tested during this session and
  reverted due noisy/inconclusive run-to-run behavior under varying final
  heights.
- For future A/Bs, prefer fixed-height or frozen-remote runs to reduce drift in
  pre-rewrite byte comparisons.
