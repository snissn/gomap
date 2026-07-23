# TreeDB #3852 `wal_on_fast` write-path closeout

This report records the benchmark evidence and deliberately narrow disposition
for #3852. The implementation coalesces retry-free, empty command-WAL
dependency debt into logical LSN ranges; it does not change the command-WAL
format or checkpoint protocol.

## Method

The baseline was `origin/main` at
`11b95c696e0594ba8aed17634d09cb7aa24dfdd7`. The candidate was built from the
#3852 change. Each database was run twice in alternating TreeDB/LevelDB order
on the same host (Intel Core i5-11400F, six physical cores, `GOMAXPROCS=12`).
The unified-bench profile is `wal_on_fast`, which selects TreeDB's
`command_wal_relaxed` mode with `read_integrity=verify`.

```sh
BIN=/path/to/unified-bench
TMPDIR=/mnt/fast4tb/tmp "$BIN" -dbs treedb -keys 500000 \
  -profile wal_on_fast \
  -checkpoint-between-tests -test sequential_write,random_write \
  -profile-dir <treedb-artifact-dir>
TMPDIR=/mnt/fast4tb/tmp "$BIN" -dbs leveldb -keys 500000 \
  -profile wal_on_fast \
  -checkpoint-between-tests -test sequential_write,random_write \
  -profile-dir <leveldb-artifact-dir>
```

The full alternating matrix below is retained because this workload is subject
to host-level variation; it is directional evidence, not a statistical
certification.

## Alternating matrix

All figures are operations/second. Ratios are TreeDB divided by LevelDB.

| Build and order | TreeDB sequential | LevelDB sequential | Sequential ratio | TreeDB random | LevelDB random | Random ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Baseline, TreeDB-A / LevelDB-A | 333,150 | 557,600 | 59.75% | 334,536 | 299,610 | 111.66% |
| Baseline, TreeDB-B / LevelDB-B | 330,731 | 565,365 | 58.50% | 329,318 | 301,220 | 109.33% |
| #3852, TreeDB-A / LevelDB-A | 349,421 | 540,146 | 64.69% | 332,902 | 282,722 | 117.75% |
| #3852, LevelDB-B / TreeDB-B | 356,928 | 517,747 | 68.94% | 334,618 | 311,040 | 107.58% |

The mean TreeDB sequential result rose from 331,941 to 353,175 operations/s
(+6.4%). The normalized sequential mean rose from 59.1% to 66.8% (+7.7
percentage points, +13.1% relative). TreeDB random writes were effectively
flat, from 331,927 to 333,760 operations/s (+0.6%), while remaining ahead of
LevelDB in every run. LevelDB's sequential mean varied from 561,483 to
528,947 operations/s, so absolute cross-build comparator changes are treated
as host noise rather than attributed to #3852.

## Allocation attribution and disposition

The baseline sequential-write allocation profile contained 531.69 MB total;
`CommandWALDependencyDebt.add` accounted for 237.09 MB (44.59%). The same
profile also attributed 119.01 MB cumulatively to payload encoding, including
53.50 MB in `writeRawKVBatchPayloadTo`. Its CPU profile showed
`setDirectAfterCommandWALAppendWithPreparedRevision` at 85.80% cumulative and
commitlog journal append at 25.44% cumulative.

After #3852, the sequential-write allocation profile was 301.00 MB total and
no longer listed `CommandWALDependencyDebt.add`: a 43.4% total-allocation
reduction. The focused debt benchmark also measured 15.35--15.53 ns/op with
0 B/op and 0 allocs/op across three runs. This supports the scoped conclusion:
eliminating one physical debt entry per ordinary relaxed command improves the
write path without a material random-write regression.

## Compressed-WAL disposition

Generic `Options.JournalCompression` is not a candidate fix for this strict
V2 command-WAL workload. V2 frame reads reject compressed segment storage
(`ErrCommandWALV2CompressedRecordUnsupported`), while V2 writers deliberately
use raw, checksummed segments so tail inspection can establish frame identity,
LSN, and durability class. Existing coverage includes
`TestCommandFrameV2RejectsCompressedSegmentStorage` and
`TestCommandJournalV2CompressionOptionWritesStrictlyReopenableRawFrames`.

A compressed command WAL would require a dedicated payload-aware format and
new replay, corruption/torn-tail, reopen, storage, and CPU validation. It was
therefore explicitly out of scope for #3852 rather than silently benchmarked
through the generic compression switch.

## Residuals and validation

TreeDB's pre-random checkpoint was about 227--252 ms both before and after the
change. Its post-run checkpoint remained about 6.7--7.0 s, versus about
1.2--1.3 s for LevelDB. That residual checkpoint gap is not addressed by this
write-path-only change.

The benchmark retained TreeDB's post-cleanup storage shape (`maindb/wal`: 11 B
across two files; index: 4 MiB; leaf value log: 16 MiB). Focused validation:

```sh
GOWORK=off go test ./TreeDB/db -run '^TestCommandWALDependencyDebt' -count=1
GOWORK=off go test -race ./TreeDB/db -run '^TestCommandWALDependencyDebt' -count=1
GOWORK=off go test ./TreeDB/internal/commitlog -run '^(TestCommandFrameV2RejectsCompressedSegmentStorage|TestCommandJournalV2CompressionOptionWritesStrictlyReopenableRawFrames|TestCommandJournal.*)' -count=1
GOWORK=off go test ./TreeDB -run '^(TestPublicCommandWALCheckpointCleansCoveredCommandJournalSegment|TestPublicCommandWALEmptyCheckpointReclaimsCoveredBenchmarkEpochs|TestPublicCommandWALGroupCommitFastRelaxedPublicationsOverlapWithoutCoordinator)$' -count=1
GOWORK=off go test ./cmd/unified_bench -run '^TestApplyProfile_FastAndWALOnFastEnableIndexOptimizations$' -count=1
```
