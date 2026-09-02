# TreeDB Benchmark Report Evidence, 2026-06-25

This is the M4 closeout artifact for the benchmark-report graph in
`snissn/gomap#3026`. It is a complete smoke-scale validation run for the report
and harness changes, not a throughput-regression comparison against the larger
June 24 artifact.

## Artifact

- Report: `/tmp/gomap_m4_final_smoke_goworkoff_20260625_105430/deep_report.html`
- Current-head rerender: `/tmp/gomap_m4_final_smoke_goworkoff_20260625_105430/deep_report_current_head.html`
- Run root: `/tmp/gomap_m4_final_smoke_goworkoff_20260625_105430`
- Commit: `0927decca6c846e1c9a42686cd2363f37b7fbeaf`
- Current render commit: `2fbd3987c459fd5b23ad3f7f3fd11717d9feb471`
- Branch: `codex/3031-final-report-evidence`
- Go: `go version go1.25.7 linux/amd64`
- Host: `Linux mikers-B560-DS3H-AC-Y1 6.8.0-124-generic x86_64`
- Mongo mode: Docker, image `mongo:8`

Run status from the rendered report:

```text
Complete run: all 20 recorded commands exited 0.
```

The current-head rerender used the completed run root above after this branch
was retargeted through M3 and validated the same complete run status after the
command-log parser stopped counting in-progress commands as successful.

All required report sections were present: raw TreeDB engine, collections vs
SQLite, Mongo API full sweep, Mongo client-mode load matrix, Mongo InsertMany
producer scaling, Mongo reader/writer scaling, and optional profile manifests.

## Command

```sh
RUN_ROOT=/tmp/gomap_m4_final_smoke_goworkoff_20260625_105430
GOWORK=off \
MONGO_BATCH_SIZE=1000 \
INSERT_PRODUCERS=8 \
MONGO_READS=200 \
MONGO_RANGE_READS=50 \
MONGO_UPDATES=50 \
MONGO_CONCURRENT_READS=200 \
MONGO_CONCURRENT_WRITES=100 \
MONGO_READERS=1,2 \
MONGO_WRITERS=1,2 \
scripts/treedb_benchmark_run_report.sh \
  --out "$RUN_ROOT" \
  --tier smoke \
  --raw-keys 10000 \
  --collection-docs 1000 \
  --mongo-docs 32000 \
  --mongo-load-docs 32000 \
  --mongo-load-batch-size 1000 \
  --mongo-load-producers "1,2,4,8,16,32" \
  --indexes "0 1 2" \
  --mongo-mode docker \
  --mongo-image mongo:8 \
  --timeout 10m \
  --title "TreeDB Benchmark Report M4 Complete Smoke"
```

`GOWORK=off` is included because this local checkout's ambient `go.work`
references a sibling module that requires a newer Go version than the workspace
declares.

## Fixed-Producer Index Count

The full-sweep load chart used 32,000 documents, batch size 1,000, requested
insert producers 8, effective producers 8, and 32 load batches. The run used
`range_index=true`, so the 0-secondary-index row still maintained the additional
range index during insert.

| secondary indexes | TreeDB docs/sec | MongoDB docs/sec | TreeDB/MongoDB |
| ---: | ---: | ---: | ---: |
| 0 | 439,212 | 487,549 | 0.90x |
| 1 | 323,459 | 294,195 | 1.10x |
| 2 | 303,069 | 241,194 | 1.26x |

## InsertMany Producer Scaling Peaks

The producer-scaling sweep used 32,000 documents and batch size 1,000, so the
1/2/4/8/16/32 producer rows all had enough load batches to avoid producer-count
cap artifacts.

| secondary indexes | TreeDB peak docs/sec | TreeDB producers | MongoDB peak docs/sec | MongoDB producers |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 537,154 | 4 effective | 248,362 | 2 effective |
| 1 | 438,909 | 4 effective | 350,992 | 8 effective |
| 2 | 365,509 | 32 effective | 290,705 | 8 effective |

## Notes

- This artifact validates final report storytelling, metadata propagation,
  load-scaling charts, index-retention tables, run-status visibility, and
  profile-manifest rendering.
- Treat throughput numbers as smoke-scale evidence only. The June 24 larger
  artifact remains the scale comparison until a deliberate PR or large-tier run
  is selected.
