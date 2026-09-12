# Opt-in 768D diagnostic harness

This harness measures the public Go Collections interface, not native transport,
HTTP, document materialization, or an end-to-end Minima workload. It introduces
no runtime changes and carries no performance improvement claim.

`prepare_cohere_scale.py` exports finite, nonzero 768D vectors from caller-supplied
parquet files using the existing NumPy/PyArrow environment. It computes exhaustive
float64 cosine top-10 truth with row-ID tie-breaking and rejects byte-identical
float32 query/train overlap. Record the external dataset revision and split
provenance separately: filenames and absence of exact overlap do not establish
a genuine Cohere held-out split. The output directory must not already exist.

```sh
python benchmarks/vector_db_compare/prepare_cohere_scale.py \
  --train /data/train.parquet --queries /data/test.parquet \
  --out /mnt/fast4tb/task/cohere-export --rows 500000 --query-count 100
```

Run the wrapper from a clean committed repository root, with output and temporary
files outside the checkout. Each invocation exclusively reserves
`RESULTS/LABEL-PHASE/` before compilation; failed runs remain there and must not
be reused. It records the source commit/tree, complete tracked path/blob inventory,
runtime subtree and harness blob identities, binary SHA256, Go build information,
dataset manifest digest, and command output. The Go fixture validates payload
hashes/shapes and truth cardinality/IDs before measured searches.

```sh
export TMPDIR=/mnt/fast4tb/task/tmp GOMAXPROCS=6
export TREEDB_SCALE_DATA=/mnt/fast4tb/task/cohere-export
export TREEDB_SCALE_DB=/mnt/fast4tb/task/pristine-db
export TREEDB_SCALE_RESULTS=/mnt/fast4tb/task/evidence
export TREEDB_SCALE_LABEL=candidate TREEDB_SCALE_CPUS=0-5
TREEDB_SCALE_PHASE=build benchmarks/vector_db_compare/run_cohere_scale.sh
# After construction, in a separate quiet window:
TREEDB_SCALE_PHASE=measure benchmarks/vector_db_compare/run_cohere_scale.sh
```

The caller creates TMPDIR, confirms capacity, freezes inputs, and excludes other
CPU/I/O-heavy work. Compilation and full dataset hashing precede measured search
intervals: “cold” means process/application-cache cold, **not** OS-page-cache cold.
Report cold preparation separately from warm per-query p50/p95 and recall at each
EF. The prepared-global helper is a diagnostic comparator, not the public path.

Write phases require `TREEDB_SCALE_WRITE_COPY=1` and a separately copied, cleanly
closed DB fixture. This acknowledgement does not create or prove the copy: retain
the copy command and source lineage, and never point a write phase at the pristine
search fixture. `write` and `write_schema` perform eight 256-ID changed replacements;
the latter first creates unrelated metadata-only schema. They preserve vector and
scalar truth. Same IDs bound the live replacement set, not physical mutation
history: eight batches consume all 4096 publication rows (replacement plus delete).
`write_schema_roots` names a prior run via `TREEDB_SCALE_PRECEDING_RUN` and creates
unrelated text roots; reuse of that exhausted fixture is a fold-budget negative
diagnostic, not successful write-throughput evidence. All phases fail on write
errors; rejected attempts must not be reported as completed durable writes.

`TestTypedWrite768Diagnostic` is a separate opt-in synthetic-vector diagnostic:
`GOMAP_WRITE_768_DIAGNOSTIC=1 go test ./TreeDB/collections -run
'^TestTypedWrite768Diagnostic$' -count=1 -v`. It times 16 durable upsert calls of
previously absent IDs at 64/256/1024 rows and 1/4 writers, includes two scalar
indexes and one text index, and checks sampled IDs after clean reopen. Optional
`GOMAP_WRITE_768_PRELOAD` is setup outside timing. This is neither ANN quality
evidence nor crash/power-loss certification. Direct Go runs need their own clean
source/binary provenance capture; the scale wrapper runs only the scale fixture.

## Retained evidence gate

Land the reviewed harness before expensive retained collection. Freeze the exact
measured runtime commit and harness commit/blobs, and identify their dependency
order. A runtime change, harness change, changed dataset digest, or materially
different environment invalidates affected comparisons. Old diagnostic results
remain historical; never relabel them as measurements of a new runtime head.

An artifact-only descendant may carry retained evidence only after verifying its
diff contains exclusively the declared evidence paths, no runtime/harness changes,
and that every recorded implementation/harness blob still matches the frozen
source. Keep the frozen commit/tree and `source-blobs.txt` as the authority, not
the descendant's different tree hash. Explicitly report missing dedicated runner,
persistent cache, or durable artifact storage as
`INFRASTRUCTURE_UNAVAILABLE: <runner|cache|storage>: <reason>` with the actual
fallback. Local focused smoke tests are not retained performance acceptance.
