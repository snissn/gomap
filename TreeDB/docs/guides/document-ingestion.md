# One-call document ingestion

`Collection.IngestSources` composes chunking, embedding, and collection indexing for a batch of source documents. It is the smallest path from long text to searchable RAG children.

```go
import (
    "github.com/snissn/gomap/TreeDB/collections/chunking"
    "github.com/snissn/gomap/TreeDB/collections/embedding"
)

cfg := collections.IngestSourcesConfig{
    Chunking: chunking.Config{
        Strategy: chunking.StrategyFixedWindow,
        SizeUnit: chunking.SizeUnitRunes,
        Size: 512,
        Overlap: 64,
    },
    Embedding: embedding.Config{
        Provider: embedding.ProviderHashing,
        Dimensions: 256,
    },
    VectorIndexName: "embedding",
    TextField: "body",
    Concurrency: 4,
}
result, err := collection.IngestSources(ctx, sources, cfg)
```

The configured vector index must exist and its dimensions must exactly match the embedder configuration. Every source is validated and chunk-planned before mutation. Embedding also completes before that source is changed, so chunk or embed errors fail closed. A successful result contains one `Ingested` outcome per source, including deterministic child IDs (`<sourceID>#<ordinal>`).

## Failure and retry behavior

`*collections.IngestError` identifies the source ID, input index, and stage (`chunk`, `embed`, or `storage`); use `errors.As` and `errors.Is` to inspect it. The worker pool is bounded by `Concurrency` (zero defaults to four), and cancellation stops unstarted work while preserving completed sources.

Child deletion and insertion are separate collection mutation boundaries. A storage error after one boundary is therefore commit-ambiguous for that source; the source may still have its old children, its new children, or be between those states. Retry the same source to converge. Child IDs are deterministic, so retries do not duplicate children. The built-in test fault boundary runs before either mutation and leaves the source unchanged.

## Quick smoke path

Use the hashing embedder for deterministic local checks, then verify text, scalar, and vector queries against `result.Ingested[*].ChildIDs`. For durable evidence, call `Flush` or `Checkpoint`, close the database, reopen it, and repeat the index checks.

The focused benchmark is bounded to 10,000 documents and 256 dimensions:

```sh
go test ./TreeDB/collections -run '^$' -bench '^BenchmarkIngestSources10K$' -benchtime=1x -count=1
```

Measured on an Apple M3 (one iteration, hashing embedder, 256 dimensions):

| docs/sec | chunk wall share | embed wall share | index wall share | B/op | allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 34.35 | 0.005753% | 0.008030% | 99.99% | 176,072,008,224 | 295,077,995 |

The storage/index share includes collection mutation and index maintenance; this is a before-state measurement, not a production capacity claim.

# C8 ingestion limiter classification

The C8 characterization used the same fixture and command as the C4 before-state benchmark:

```sh
PATH="$HOME/.gvm/gos/go1.26.0/bin:$PATH" \
GOROOT="$HOME/.gvm/gos/go1.26.0" CGO_ENABLED=1 \
GOCACHE="$HOME/orca/workspaces/gomap/.gocache-go126" \
go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkIngestSources10K$' -benchtime=1x -count=1 -benchmem \
  -cpuprofile=/tmp/c8-before.cpu -memprofile=/tmp/c8-before.mem
```

Host: Apple M3, darwin/arm64, 8 CPUs, Go 1.26.0. Fixture: 10,000 source
documents, one fixed-window chunk per source, hashing embedder, 256
dimensions, four workers, collection scalar + text-v1 + vector indexes.
The benchmark timer covers `IngestSources` only; embedding and chunking are
included in the stage counters, while setup is excluded.

Two identical profiled baseline repetitions measured 31.01 and 31.21
docs/sec, 322.50 s and 320.37 s wall time, 176,532,381,152 and
175,967,961,112 B/op, and 296,096,807 and 295,444,436 allocs/op. Stage
shares were 0.01281% and 0.01175% chunk, 0.00886% and 0.01133% embed, and
99.98% index in both runs.

An identical resource-wrapped repetition measured 36.58 docs/sec, 273.35 s,
175,860,713,496 B/op, and 295,345,710 allocs/op; peak resident set size was
2,652,635,136 bytes (2.47 GiB). The wider 31.01--36.58 docs/sec spread is
measurement noise from the durable maintenance worker, not evidence of an
ingestion-stage improvement. The existing C4 one-run result (34.35 docs/sec)
is within the same single-run noise envelope; the limiter classification is
unchanged.

The CPU profile (`/tmp/c8-before.cpu`) attributes the measured wall time to
the durable storage boundary rather than the ingestion handoff: `InsertBatch`
is 59.79% cumulative, durable value-log reference capture is 53.19%
cumulative, and `maintenanceReachabilityScan` is 51.33% cumulative. The
profile's direct hot symbols are `Node.GetLeafValueView` (14.07% flat),
`pthread_cond_signal` (12.13%), and `syscall.rawsyscalln` (11.15%). This
classifies the limiter as **checkpoint/value-log and durable root-publication
interaction**, with per-source mutation serialization, not embedding or
chunking allocation churn. The 0.008--0.011% embed share also rules out an
embed batching/concurrency lane for this fixture.

The heap profile (`/tmp/c8-before.mem`) corroborates the same boundary:
`maintenanceReachabilityScan` accounts for 60.52% cumulative allocation
space, `tree.Iterator.ValueCopy` for 24.22%, and leaf-reference walking for
4.03%. This is storage-maintenance allocation churn, not chunk/embed handoff
allocation churn.

No ingestion-path code change is safe within C8 ownership: reducing durable
root publications requires a new atomic batch publication contract spanning
collection and storage, and bypassing it would violate C4 durability and
reopen guarantees. Text-v2 delta staging is likewise a spec-first blocker.
The measured follow-up is tracked in [#4284](https://github.com/snissn/gomap/issues/4284).

The retrieval cross-check remains the C1 medium fixture's hybrid
`score_only`/`none_100pct` row: recall@10 `0.0222`, p50 `0.671 ms`, p99
`1.333 ms`; no search-path code was changed by C8.
