# Leaf VLog Compact Payload Validation

## Format

Reserved `leaf_vlog` records no longer need to persist the whole raw `4096`-byte
leaf page image.

For a slotted leaf page, the live bytes are already split into two contiguous
regions:

- a live prefix at the front of the page: header + key/value offset directories +
  top-level leaf metadata
- a live suffix at the back of the page: the packed heap payloads

The middle gap is free space. The compact payload stores:

- `prefix_len`: number of live bytes from the start of the page up to the first
  free-gap byte
- `suffix_len`: number of live bytes from the end of the page back to the first
  heap byte
- `prefix bytes`
- `suffix bytes`

On read, TreeDB reconstructs a full `4096`-byte page by copying the prefix,
copying the suffix at the end, and zero-filling the middle gap.

This is intentionally scoped to the reserved split `leaf_vlog` path only. Raw
lane-`0` leaf-page logging remains readable and unchanged.

## Local Validation

Targeted local coverage passed:

- compact payload encode/decode round-trips
- aliased decode buffers do not corrupt compact payloads
- `Manager`, `Set`, and caching protected read paths all reconstruct full pages
- reopen / rewrite / maintenance regression tests for leaf refs remain green
- lane-`0` rewrite-writer leaf pages remain readable without the compact format

## Celestia Control vs Candidate

Control: `main` at `c77306fb`
Candidate: `codex/leaf-vlog-compact-pages` on top of `f6648019`

Each run used:

- `run_celestia.sh`
- `POST_SYNC_DWELL_SECONDS=900`
- profile `fast` or `wal_on_fast`
- offline `treemap vlog-rewrite -rw` on the resulting `application.db`

### Raw Results

| profile | variant | final height | sync s | dwell s | total s | max RSS GiB | dwell app bytes | dwell leaf bytes | dwell value bytes | rewrite s | post-rewrite app bytes | post-rewrite leaf bytes | post-rewrite value bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fast | control | 10700468 | 300 | 901 | 1201 | 13.125 | 2770968832 | 2511152494 | 194377257 | 46.43 | 2205846052 | 2154880692 | 11185567 |
| fast | candidate | 10700687 | 316 | 900 | 1216 | 9.775 | 2692090902 | 2433055884 | 195660321 | 32.88 | 2314709750 | 2264568499 | 10852821 |
| wal_on_fast | control | 10700906 | 353 | 900 | 1253 | 13.744 | 2749236665 | 2477668157 | 207651640 | 43.97 | 2211808519 | 2160776000 | 11219923 |
| wal_on_fast | candidate | 10701119 | 308 | 901 | 1209 | 9.891 | 2568728819 | 2338264533 | 162876471 | 29.96 | 2225966375 | 2177699867 | 9764478 |

### Direct Deltas

`fast` candidate vs control:

- height: `+219`
- sync time: `+16s`
- dwell-end `application.db`: `-78,877,930 B`
- dwell-end `leaf_vlog`: `-78,096,610 B`
- max RSS: `-3.35 GiB`
- rewrite time: `-13.55s`
- post-rewrite `application.db`: `+108,863,698 B`

`wal_on_fast` candidate vs control:

- height: `+213`
- sync time: `-45s`
- dwell-end `application.db`: `-180,507,846 B`
- dwell-end `leaf_vlog`: `-139,403,624 B`
- dwell-end `value_vlog`: `-44,775,169 B`
- max RSS: `-3.85 GiB`
- rewrite time: `-14.01s`
- post-rewrite `application.db`: `+14,157,856 B`

## Interpretation

The compact leaf payload clearly improves the live / post-dwell shape.

- Both profiles ended the `900s` dwell with materially smaller `application.db`
  and `leaf_vlog`.
- Both profiles also cut peak RSS by roughly `3.3` to `3.9 GiB`.
- `wal_on_fast` improved both dwell-end size and total runtime meaningfully.

The offline rewrite comparison is more nuanced.

- The candidate rewrites faster in both profiles.
- The post-rewrite sizes are close, with candidate slightly larger:
  - `fast`: about `+109 MB`
  - `wal_on_fast`: about `+14 MB`
- Those rewrite runs are not at identical final heights; candidate ended
  `~200` blocks higher in both profiles.

That shape is still coherent with the design:

- this change removes persisted free-gap bytes from the live split `leaf_vlog`
  format
- offline rewrite already rebuilds and densifies pages aggressively, so the
  additional benefit after a full rewrite is expected to shrink

So the main success condition for this sprint is live / dwell storage shape, and
the candidate wins there in both profiles, especially `wal_on_fast`.
