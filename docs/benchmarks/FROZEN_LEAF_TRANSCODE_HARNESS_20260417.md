# Frozen Leaf Transcode Harness

## Goal

Close the `leaf_vlog` portion of the post-dwell to offline-rewrite size gap
without paying the cost of running outer-leaf dict compression on the live
first-write path.

The intended workflow is:

1. take a recent post-dwell `application.db`
2. clone it
3. run a focused sealed-generation outer-leaf transcode loop
4. compare the result to an offline rewrite floor on a second clone

This is intentionally a frozen-copy harness, not a live maintenance result.

## Reference Gaps On `main`

Archived `main` controls show the real production gap we ultimately want to
close.

### `main fast`

| stage | application.db | index.db | leaf_vlog | value_vlog |
|---|---:|---:|---:|---:|
| post-dwell | 2,536,171,948 | 67,895,296 | 2,325,334,197 | 142,744,416 |
| post-rewrite | 2,096,276,342 | 38,273,024 | 2,048,095,816 | 9,510,710 |
| gap | 439,895,606 | 29,622,272 | 277,238,381 | 133,233,706 |

Rewrite elapsed: `47.02s`

### `main wal_on_fast`

| stage | application.db | index.db | leaf_vlog | value_vlog |
|---|---:|---:|---:|---:|
| post-dwell | 2,585,213,462 | 61,865,984 | 2,351,834,076 | 171,297,961 |
| post-rewrite | 2,117,928,675 | 38,535,168 | 2,068,758,838 | 10,237,873 |
| gap | 467,284,787 | 23,330,816 | 283,075,238 | 161,060,088 |

Rewrite elapsed: `43.84s`

Interpretation:

- leaf-only work cannot close the whole `application.db` gap
- the leaf-specific ceiling is roughly the `277-283 MB` `leaf_vlog` gap
- `value_vlog` and `index.db` need separate follow-on work

## Frozen Harness Source

Frozen source used for iteration:

- home: `/home/mikers/.celestia-app-mainnet-treedb-20260417111043`
- profile shape: `wal_on_fast`
- source `application.db`:
  - `application.db`: `2,576,203,003`
  - `index.db`: `61,341,696`
  - `leaf_vlog`: `2,344,984,993`
  - `value_vlog`: `169,695,667`

This source is not the archived `main` control. It was kept because it is a
recent post-dwell Celestia-shaped DB with the same remaining leaf-compression
problem, and it lets us iterate cheaply on a frozen copy.

## Harness Command

```bash
treemap leafgen-transcode-bench <application.db> -rw \
  -json \
  -force \
  -max-seconds 600 \
  -max-passes 5 \
  -max-generations-per-pass 128 \
  -max-bytes-to-copy-per-pass 4294967296
```

The harness:

- opens the DB read-write
- builds a dedicated sealed-generation transcode plan
- estimates actual dict-transcoded bytes for sampled leaf pages
- selects candidates by expected saved bytes and saved-per-byte-copied
- rewrites the selected generations through the existing pack copy engine with
  outer-leaf dict mode enabled
- closes and reopens between passes so on-disk measurements reflect deleted
  files, not just open-file handles

## Frozen Harness Result

### Before vs After

| stage | application.db | index.db | leaf_vlog | value_vlog |
|---|---:|---:|---:|---:|
| before | 2,576,203,003 | 61,341,696 | 2,344,984,993 | 169,695,667 |
| after | 2,506,390,975 | 111,411,200 | 2,225,103,449 | 169,695,667 |
| delta | -69,812,028 | +50,069,504 | -119,881,544 | 0 |

Elapsed: `61.28s`

Runner peak RSS from `/usr/bin/time`: `684,500 kB`

### Offline Rewrite Floor On The Same Source

| stage | application.db | index.db | leaf_vlog | value_vlog |
|---|---:|---:|---:|---:|
| post-rewrite | 2,190,083,914 | 39,321,600 | 2,139,453,969 | 10,944,315 |

Rewrite report:

- `segments_before=76`
- `segments_after=17`
- `bytes_before=2490028712`
- `bytes_after=2125746336`
- elapsed: `44.08s`
- peak RSS: `193,120 kB`

### Gap Closure

Against the rewrite floor on this same frozen source:

- total `application.db` gap before rewrite:
  - `2,576,203,003 - 2,190,083,914 = 386,119,089`
- total `application.db` gap after transcode:
  - `2,506,390,975 - 2,190,083,914 = 316,307,061`
- total gap closed:
  - `69,812,028`
  - `18.1%`

Leaf-only:

- `leaf_vlog` gap before rewrite:
  - `2,344,984,993 - 2,139,453,969 = 205,531,024`
- `leaf_vlog` gap after transcode:
  - `2,225,103,449 - 2,139,453,969 = 85,649,480`
- `leaf_vlog` gap closed:
  - `119,881,544`
  - `58.3%`

This is the main success signal for the harness.

## Pass Breakdown

### Pass 1

- plan admission: `eligible`
- candidates: `69`
- estimated bytes saved: `78,096,344`
- expected saved per byte copied: `34,555 ppm`
- selected bytes to copy: `2,260,034,554`
- actual bytes copied by pack engine: `5,893,971,968`
- GC deleted generations: `69`
- GC deleted bytes: `2,317,726,562`

Disk movement:

- `application.db`: `2,576,203,003 -> 2,506,845,623`
- `leaf_vlog`: `2,344,984,993 -> 2,226,344,529`

### Pass 2

- plan admission: `eligible`
- candidates: `1`
- estimated bytes saved: `34,133`
- selected bytes to copy: `1,458,247`
- actual bytes copied by pack engine: `1,916,928`
- GC deleted generations: `3`
- GC deleted bytes: `2,606,483`

Disk movement:

- `application.db`: `2,506,845,623 -> 2,506,390,975`
- `leaf_vlog`: `2,226,344,529 -> 2,225,103,449`

### Pass 3

- plan admission: `no_estimated_savings`
- no transcode run
- converged

## Interpretation

This harness is doing something meaningfully different from the rejected live
maintenance attempt:

- it is dedicated to sealed outer-leaf transcode, not reclaim-driven leaf-pack
- it estimates actual dict-transcoded output size before selecting candidates
- it can run large, concentrated batches against a frozen DB copy
- it closed most of the leaf-only gap in about one minute with sub-`700 MB` RSS

The remaining gap is also informative:

- the harness is strong on `leaf_vlog`
- it does nothing yet for `value_vlog`
- `index.db` grew in the frozen run, so index-side behavior is not yet aligned

So the next standard for success should be:

- keep using this frozen harness for fast iteration
- close the remaining `leaf_vlog` gap further if possible
- only then port the proven policy back into live dwell maintenance
