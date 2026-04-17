## Leaf-Pack Maintenance: Sealed Outer-Leaf Dict Transcode

This note captures the first maintenance-only attempt to close the gap between:

- post-`run_celestia + 900s dwell` on-disk bytes
- post-offline-`treemap vlog-rewrite -rw` bytes

The branch keeps the hot first-write path cheap and only allows outer-leaf dict
compression from sealed leaf-pack maintenance.

### Control Runs

Recent `main` controls from `/tmp/leaf_rewrite_gap_analysis`:

| Profile | Dwell-end `application.db` | Post-rewrite `application.db` | Gap |
| --- | ---: | ---: | ---: |
| `fast` | `2,536,171,948` | `2,096,276,342` | `439,895,606` |
| `wal_on_fast` | `2,585,213,462` | `2,117,928,675` | `467,284,787` |

### Candidate Runs

Candidate branch: maintenance-only sealed outer-leaf transcode.

`wal_on_fast`

- sync: `317s`
- total: `1217s`
- max RSS: `12,053,748 kB`
- dwell-end `application.db`: `2,406,264,420`
- dwell-end `maindb/index.db`: `66,584,576`
- dwell-end `maindb/leaf_vlog`: `2,173,456,227`
- dwell-end `maindb/value_vlog`: `166,024,395`
- post-rewrite `application.db`: `2,135,297,434`
- post-rewrite `maindb/index.db`: `38,535,168`
- post-rewrite `maindb/leaf_vlog`: `2,086,182,002`
- post-rewrite `maindb/value_vlog`: `10,183,468`
- rewrite elapsed: `42.40s`
- rewrite max RSS: `200,332 kB`

`fast`

- sync: `305s`
- total: `1205s`
- max RSS: `10,349,620 kB`
- dwell-end `application.db`: `2,462,549,256`
- dwell-end `maindb/index.db`: `63,963,136`
- dwell-end `maindb/leaf_vlog`: `2,229,472,757`
- dwell-end `maindb/value_vlog`: `168,906,592`
- post-rewrite `application.db`: `2,155,783,009`
- post-rewrite `maindb/index.db`: `38,797,312`
- post-rewrite `maindb/leaf_vlog`: `2,106,225,474`
- post-rewrite `maindb/value_vlog`: `10,363,427`
- rewrite elapsed: `43.10s`
- rewrite max RSS: `202,240 kB`

### Gap Reduction

| Profile | Control Gap | Candidate Gap | Reduction |
| --- | ---: | ---: | ---: |
| `fast` | `439,895,606` | `306,766,247` | `133,129,359` |
| `wal_on_fast` | `467,284,787` | `270,966,986` | `196,317,801` |

### Notes

- `wal_on_fast` is the clearest signal because it finished at the same height as
  the recent `main` control.
- The `fast` candidate finished `222` blocks higher than the earlier `main`
  control, so its absolute end-state comparison is directionally useful but not
  perfectly apples-to-apples.
- Late dwell samples show leaf-pack switching into transcode mode:
  - `wal_on_fast`: `debug_vars_8..12` show `last_selection.mode=transcode`
  - `fast`: `debug_vars_11..13` show `last_selection.mode=transcode`
- Final live dict-frame share increased materially during dwell:
  - `wal_on_fast`: `treedb.cache.vlog_auto.frames_frac.dict=0.419404`
  - `fast`: `treedb.cache.vlog_auto.frames_frac.dict=0.675012`

### Interpretation

- This branch materially closes the dwell-to-rewrite gap, especially for
  `wal_on_fast`.
- The remaining gap is still large, so this is not a full replacement for
  offline rewrite.
- `wal_on_fast` is already a strong candidate for merge review.
- `fast` is also directionally positive, but its absolute comparison should be
  read with the height mismatch caveat above.
