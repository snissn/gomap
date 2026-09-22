# Cold Value-Log Compression: Analysis + Design Sketch

Date: 2026-01-25

This document summarizes recent compression experiments on TreeDB value logs and proposes a **cold-storage, block-compressed** design. It captures:

- The current in-system compression formats (dict, template, and prepass combination)
- Offline compressibility results (whole-file and block-based)
- A matrix of block sizes × zstd levels across three log variants
- A proposed cold-storage approach and next experiments

## 1) Scope & goals

We want to understand the gap between:

- **Current in-system compression** (dict, template, or template→dict)
- **Theoretical upper bound** (whole-file zstd)

and determine whether a **cold storage tier** can safely exploit larger block sizes to approach the theoretical ratio while keeping acceptable access latency for cold data.

## 2) Terminology / log variants

These are the three log variants used in the analysis:

1) **Template-off log**
   - Raw value-log with no template compression.

2) **Template-on log**
   - Values encoded with template compression (TemplateOnly).

3) **Dict log**
   - Values written using zstd dict compression in TreeDB (fastest settings).

### Raw log sizes (from the runs used in the sweep)

- **Template-off**: 77,567,118 bytes
- **Template-on**: 44,422,788 bytes
- **Dict log**: 43,164,763 bytes

These sizes are from the actual value-log files used in the sweep.

## 3) Current in-system compression formats

### 3.1 Raw (template off, dict off)
- Per-record bytes as written, no compression.

### 3.2 Dict compression (TreeDB zstd + dict)
- Value-log frames compressed with zstd and a trained dictionary.
- Frame-sized context; no long-range redundancy between frames.

### 3.3 Template-only
- Each value is reduced to a template payload with anchors/mask + var bytes.
- Still per-record; no cross-record entropy sharing.

### 3.4 Template → dict (prepass)
- Template payloads produced first, then fed into dict compression.
- Can reduce size further, but lowers throughput.

## 4) Empirical results (in-system)

**Bench summary (celestia-db.head.jsonl, mode3, batch=1024, raw_mib=64, pointer_threshold=1, train=20000, eval=5000)**

| Mode | Template | Dict | steady_raw_MBps | value_log_bytes |
|---|---|---|---:|---:|
| Baseline | off | off | ~264 | 65.1 MiB |
| TemplateOnly | on | off | ~114 | 37.2 MiB |
| DictOnly | off | on | ~116 | 36.4 MiB |
| Template→Dict | prepass | on | ~67 | 32.7 MiB |

Observations:
- TemplateOnly and DictOnly are close in size.
- Prepass yields better ratio but significant throughput drop.

## 5) Whole-file zstd (theoretical upper bound)

Compressing the entire log as a single stream gives a ceiling (not random-access):

| Variant | zstd -19 size | Ratio vs raw off |
|---|---:|---:|
| Template-off | 2,785,759 bytes | 0.03591 |
| Template-on | 2,730,418 bytes | 0.06146 (vs off raw) |
| Dict log | 2,577,372 bytes | 0.03323 (vs off raw) |

This shows the **global redundancy** in the data, but it is **not directly viable** for random-access storage.

## 6) Block-zstd sweep (levels × block sizes)

Ratios below are **total compressed bytes / template-off raw bytes** (77,567,118). This makes all three variants directly comparable on the same baseline.

### log=off (ratio vs template-off raw)
| level | 1MiB | 2MiB | 4MiB | 8MiB | 16MiB | 32MiB | 64MiB |
|---|---|---|---|---|---|---|---|
| -1 | 0.389453 | 0.389378 | 0.389346 | 0.389329 | 0.389317 | 0.389312 | 0.389310 |
| -3 | 0.344585 | 0.189709 | 0.124744 | 0.094700 | 0.088304 | 0.085728 | 0.085709 |
| -5 | 0.344842 | 0.189822 | 0.123637 | 0.092667 | 0.086283 | 0.083722 | 0.083568 |
| -9 | 0.338387 | 0.188890 | 0.116351 | 0.079816 | 0.058453 | 0.049906 | 0.045633 |
| -15 | 0.337228 | 0.186737 | 0.113615 | 0.076765 | 0.055235 | 0.046611 | 0.042305 |
| -19 | 0.336592 | 0.185252 | 0.111839 | 0.074869 | 0.053228 | 0.044569 | 0.040240 |

### log=on (ratio vs template-off raw)
| level | 1MiB | 2MiB | 4MiB | 8MiB | 16MiB | 32MiB | 64MiB |
|---|---|---|---|---|---|---|---|
| -1 | 0.377940 | 0.377906 | 0.377893 | 0.377885 | 0.377879 | 0.377876 | 0.377875 |
| -3 | 0.205673 | 0.120527 | 0.093878 | 0.078646 | 0.073673 | 0.071494 | 0.069328 |
| -5 | 0.206215 | 0.120889 | 0.079288 | 0.058017 | 0.051491 | 0.049321 | 0.047156 |
| -9 | 0.204400 | 0.121702 | 0.081407 | 0.060822 | 0.048465 | 0.044325 | 0.040185 |
| -15 | 0.202798 | 0.119373 | 0.078693 | 0.057816 | 0.045276 | 0.041101 | 0.036949 |
| -19 | 0.205478 | 0.119797 | 0.078087 | 0.056620 | 0.043755 | 0.039505 | 0.035201 |

### log=dict (ratio vs template-off raw)
| level | 1MiB | 2MiB | 4MiB | 8MiB | 16MiB | 32MiB | 64MiB |
|---|---|---|---|---|---|---|---|
| -1 | 0.405692 | 0.405625 | 0.405574 | 0.405555 | 0.405550 | 0.405546 | 0.405533 |
| -3 | 0.229166 | 0.134199 | 0.095473 | 0.082632 | 0.076586 | 0.074081 | 0.071576 |
| -5 | 0.225194 | 0.131581 | 0.086897 | 0.065579 | 0.058464 | 0.055993 | 0.053532 |
| -9 | 0.221180 | 0.128358 | 0.082846 | 0.058061 | 0.042878 | 0.037808 | 0.032745 |
| -15 | 0.220728 | 0.128706 | 0.083618 | 0.059015 | 0.043950 | 0.038926 | 0.033909 |
| -19 | 0.221798 | 0.129000 | 0.083557 | 0.058652 | 0.043406 | 0.038320 | 0.033228 |


**Quick read:**
- At small blocks (1–2 MiB), template-on wins vs dict log.
- At large blocks (16–64 MiB), dict log catches up and slightly wins.
- Higher levels (-9, -15, -19) benefit much more from larger blocks.
- Level -1 is essentially flat across block sizes (short window, fast mode).

## 7) Block decompression with disk access (template-off log)

This test reads all compressed blocks from disk and streams them through `zstd -d` to `/dev/null`.

**Note:** Likely OS cache is hot; these numbers are optimistic for cold reads.

| block_mib | raw_bytes | comp_bytes | comp_ratio | decode_seconds | decoded_MBps |
|---:|---:|---:|---:|---:|---:|
| 1 | 77,567,118 | 26,108,468 | 0.336592 | 0.06 | 1232.90 |
| 2 | 77,567,118 | 14,369,473 | 0.185252 | 0.03 | 2465.79 |
| 4 | 77,567,118 | 8,675,054 | 0.111839 | 0.02 | 3698.69 |
| 8 | 77,567,118 | 5,807,386 | 0.074869 | 0.02 | 3698.69 |
| 16 | 77,567,118 | 4,128,722 | 0.053228 | 0.02 | 3698.69 |
| 32 | 77,567,118 | 3,457,082 | 0.044569 | 0.02 | 3698.69 |
| 64 | 77,567,118 | 3,121,303 | 0.040240 | 0.02 | 3698.69 |

Interpretation: once blocks are ≥4 MiB, total decode time is dominated by IO + overhead and becomes flat. Per-block latency still matters for random reads.

## 8) Key insights

1) **Whole-file zstd is an upper bound.**
   It captures cross-record redundancy that per-frame compression can’t exploit.

2) **Block size is the main lever.**
   At -9 to -19, block sizes ≥8–16 MiB yield large gains.

3) **Template vs dict tradeoffs shift with block size.**
   Template-on looks better for small blocks; dict log looks better at large blocks.

4) **Cold storage is the right use case.**
   If we accept block-level random access (not per-record), the ratios approach the theoretical ceiling.

## 9) Proposed cold storage design (high-level)

### 9.1 Architecture

- Keep current value-log format for **hot data**.
- Periodically **repack old segments** into cold format:
  - Large block compression (8–64 MiB)
  - Optional per-block dictionary / or global dict
- New pointer format:
  - `{segment_id, block_id, record_offset, record_len}`

### 9.2 Block format (draft)

```
block_header:
  magic, version
  block_raw_len
  block_comp_len
  block_crc
block_payload:
  zstd-compressed bytes
```

We need a **block index** that maps record offsets → block_id + offset.

### 9.3 Access path

- For cold record lookup:
  1) locate block via index
  2) read compressed block
  3) decompress block
  4) slice record bytes

### 9.4 Background job

- Identify cold segments by last-access time / age.
- Repack into cold blocks and update pointers.
- Garbage collect old segments when safe.

### 9.5 Caching

- Optional in-memory block cache for hot-cold crossover.

## 10) Next experiments

1) **Cold-cache random block decode** (latency per block size)
2) **Template-on + dict log decode test** (same decode benchmark)
3) **Block size sweet spot** analysis by workload type
4) **Small prototype cold segment** format to test read amplification

---

Appendix: raw matrix data is saved at `/tmp/zstd_matrix.csv`.
