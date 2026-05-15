# Wildcat Unified Bench Smoke Comparison - 2026-05-01

Small deterministic smoke comparison after adding the Wildcat adapter to
`cmd/unified_bench`.

Command:

```bash
./bin/unified-bench \
  -dbs treedb,wildcat \
  -profile fast \
  -keys 20000 \
  -valsize 128 \
  -batchsize 1000 \
  -test sequential_write,random_write,random_read,random_read_batch,full_scan,prefix_scan \
  -read-require-hit \
  -format markdown \
  -progress=false \
  -seed 1
```

Notes:

- `profile=fast` is an unsafe throughput profile. TreeDB ran with WAL off and
  relaxed read integrity. Wildcat ran with `-wildcat-sync=none`.
- The adapter uses Wildcat's unversioned Go module,
  `github.com/wildcatdb/wildcat` at v1.0.13. The current `/v2` module line
  requires Go 1.26, which is newer than this repository's current toolchain.
- The Wildcat adapter stores benchmark keys as order-preserving hex strings
  internally because Wildcat v1.0.13 serializes WAL transaction maps through
  BSON, whose map keys cannot contain NUL bytes. The public benchmark key/value
  contract remains byte-slice based.
- This is a small smoke run, not a full performance study.

Results:

```text
               Test         TreeDB        Wildcat
-------------------  -------------  -------------
   Sequential Write      3,585,434        116,776
       Random Write      4,299,997        123,433
        Random Read      5,910,165      1,265,082
Random Read (Batch)        228,664      1,650,721
          Full Scan      5,119,545      4,494,029
        Prefix Scan      7,352,844      4,985,865
```

End-of-run disk usage:

```text
TreeDB:
  maindb/index.db: 256 KiB
  maindb/value_vlog: total=0 B files=3
  maindb/leaf_vlog: total=533 KiB files=2 value=532 KiB other=244 B
  dictdb/index.db: 64 KiB

Other DBs:
  Wildcat: total=39 MiB files=2
```
