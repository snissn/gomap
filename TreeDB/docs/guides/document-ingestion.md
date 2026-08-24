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

The configured vector index must exist and its dimensions must exactly match
both the embedder configuration and the created provider. Every source is
validated and chunk-planned before mutation. Parent IDs must be non-empty
valid UTF-8 without `#`; reserved chunk linkage roots cannot be text/vector
destinations. Provider output count, width, finiteness, and cosine magnitude
are checked before that source is changed, so chunk, provider, or output errors
fail closed. A successful result contains one `Ingested` outcome per source,
including deterministic child IDs (`<sourceID>#<ordinal>`).

## Failure and retry behavior

`*collections.IngestError` identifies the source ID, input index, and stage
(`chunk`, `embed`, or `storage`); parent-ID rejection unwraps a typed
`*chunking.ParentIDError`, while provider/index failures preserve typed causes
for `errors.As`/`errors.Is`. The worker pool is bounded by `Concurrency` (zero
defaults to four), but calls to its one shared provider instance are serialized
because providers need not be thread-safe. Cancellation prevents queued
provider calls and unstarted source mutations while preserving completed
sources.

The shared per-parent lock covers the complete plan through replacement across
concurrent calls and collection handles; independent parents do not share that
lifecycle lock. A short collection-wide lock protects enumeration, candidate
construction, and publication, so `Concurrency` parallelizes planning and
embedding while root publication remains serialized. Parent lifecycle locks are
context-aware and release after a source commits, before its progress callback.

For each source, stale child and old-parent removals plus the new parent,
children, text/scalar/vector index entries, typed-column assets, row locators,
and catalog descriptors are one dependency-closed durable root publication.
Failures before publication leave the old source. A recoverable command-WAL
frame may complete the whole new source during reopen; post-publication failures
are commit-ambiguous only between complete old and complete new states. No
intermediate parent/child/index state is certified. Retrying the same source
converges because child IDs are deterministic.

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

# C8 ingestion limiter classification and optimization

The C8 characterization used the C4 fixture on the current mainline before
the prefix-scan change. Run from the repository root:

```sh
PATH="$HOME/.gvm/gos/go1.26.0/bin:$PATH" \
GOROOT="$HOME/.gvm/gos/go1.26.0" CGO_ENABLED=1 \
GOCACHE="${GOCACHE:-$HOME/.cache/gomap-go126}" \
go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkIngestSources10K$' -benchtime=1x -count=1 -benchmem \
  -cpuprofile=/tmp/c8-prefix.cpu -memprofile=/tmp/c8-prefix.mem
```

Host: Apple M3, darwin/arm64, 8 CPUs, Go 1.26.0. Fixture: 10,000 source
documents, one fixed-window chunk per source, hashing embedder, 256
dimensions, four workers, collection scalar + text-v1 + vector indexes.
The benchmark timer covers `IngestSources` only; embedding and chunking are
included in the stage counters, while setup is excluded.

The pre-change current-main run was head `5516aaba2` (mainline already included
the native vector-insert work from #4282): 27.32 docs/sec, 366.06 s wall time,
177,905,962,160 B/op, 296,268,940 allocs/op, with 0.01210% chunk,
0.008471% embed, and 99.98% index stage share. The optimized run at
`efe5d4488` measured 37.59 docs/sec, 266.02 s wall time,
132,071,674,336 B/op, 95,534,267 allocs/op, with 0.01514% chunk,
0.007082% embed, and 99.98% index stage share:

| revision | docs/sec | B/op | allocs/op | chunk % | embed % | index % |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pre-change `5516aaba2` | 27.32 | 177,905,962,160 | 296,268,940 | 0.01210 | 0.008471 | 99.98 |
| prefix-scan `efe5d4488` | 37.59 | 132,071,674,336 | 95,534,267 | 0.01514 | 0.007082 | 99.98 |

This is a +37.6% docs/sec improvement, -25.8% B/op, and -67.8% allocs/op
under identical fixture/host/command conditions. The implementation changed
ordinary JSON `ChunkChildren` from a full primary-collection scan per source to
a bounded primary-key `parent#` prefix range. #4293 extends that same bounded
range to reconstructable column-store layouts, reconstructs only matched rows,
and exposes `ChunkChildrenWithStats` structural counters. Truncation and
linkage validation remain fail closed; no storage format changes.

The optimized CPU profile (`/tmp/c8-prefix.cpu`) identifies the remaining
limiter as durable root publication rather than chunking or embedding:
`InsertBatch` is 58.27% cumulative, durable value-log reference capture is
54.74% cumulative, and `maintenanceReachabilityScan` is 53.10% cumulative.
These are on-CPU samples and do not by themselves attribute blocked wall time.
The heap profile (`/tmp/c8-prefix.mem`) attributes 81.40% cumulative allocation
space to `maintenanceReachabilityScan`, with durable value-log capture at
86.95% cumulative. The remaining root-publication optimization requires the
atomic durable batch-publication contract tracked in
[#4284](https://github.com/snissn/gomap/issues/4284); C8 does not weaken
durability or absorb that storage-contract work.

The retrieval cross-check remains the C1 medium fixture's hybrid
`score_only`/`none_100pct` row: recall@10 `0.0222`, p50 `0.671 ms`, p99
`1.333 ms`; no search-path code changed by C8.
