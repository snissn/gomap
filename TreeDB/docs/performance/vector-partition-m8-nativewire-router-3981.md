# #3981 nativewire router-session matched evidence

Status: measured, local single-host evidence for #3981. The exact-head refresh
immediately below is the authoritative matched comparison after #3980 merged.
It is not an M8 enablement claim and does not replace the broader M8 evidence
index or its gate ledger.

## Exact-head refresh after #3980 merge

The final matched pair compares merged #3980 base
`464aa721aef3743893615440b20dc91195f86afb` with #3981 head
`2fe04482d807d4fb680030581c68b967f8d7612f`. Both trees were clean and
used Go `go1.26.0`, Linux/amd64, `GOMAXPROCS=16`, the same 10k fixture,
topology, query order, concurrency, and `/mnt/fast4tb` mount. Binaries were
built with `GOWORK=off go build -buildvcs=false`; the benchmark itself records
the correct base/head revisions from its clean worktree.

The matched shell invocations differed only in binary and capture directory:

```sh
GOMAXPROCS=16 <base-bench> -mode production_multi_group \
  -dataset /mnt/fast4tb/tmp/gomap-3981-nativewire-router/testdata/vector_partition_10k \
  -out /mnt/fast4tb/tmp/treedb_3981_final_g9LD0i/base/out \
  -raft-groups 2 -raft-nodes-per-group 3 -partitions 4 -probes 4 -overlap 0 \
  -top-k 10 -recall-target .90 -concurrency 16 -warmup 1 -ef-search 4096 \
  -seed 1 -profiles /mnt/fast4tb/tmp/treedb_3981_final_g9LD0i/base/profiles \
  -format json

GOMAXPROCS=16 <head-bench> -mode production_multi_group \
  -dataset /mnt/fast4tb/tmp/gomap-3981-nativewire-router/testdata/vector_partition_10k \
  -out /mnt/fast4tb/tmp/treedb_3981_final_g9LD0i/head/out \
  -raft-groups 2 -raft-nodes-per-group 3 -partitions 4 -probes 4 -overlap 0 \
  -top-k 10 -recall-target .90 -concurrency 16 -warmup 1 -ef-search 4096 \
  -seed 1 -profiles /mnt/fast4tb/tmp/treedb_3981_final_g9LD0i/head/profiles \
  -format json
```

| Metric | merged #3980 base | #3981 head | head vs base |
| --- | ---: | ---: | ---: |
| QPS | 50.59 | 155.89 | 3.08x, +208.2% |
| p50 | 301.49 ms | 92.39 ms | -69.4% |
| p95 | 377.17 ms | 134.79 ms | -64.3% |
| p99 | 384.44 ms | 156.29 ms | -59.3% |
| Recall@10 | 1.0 | 1.0 | unchanged |
| Differential allocation | 1,789.92 MB | 389.97 MB | -78.2% |
| Differential allocation/query | 13.984 MB | 3.047 MB | -78.2% |
| End `inuse_space` heap | 42.61 MB | 41.07 MB | -3.6% |
| Measured peak RSS | 133,316,608 | 142,479,360 | +6.9% |
| Request / response / candidate bytes per query | 1,600 / 2,064 / 640,000 | 1,600 / 2,064 / 640,000 | unchanged |
| Persistent asset bytes | 3,686,024 | 3,686,024 | unchanged |

Both revisions pass the exhaustive union, exact all-partition parity, recall,
balance, and unavailable-endpoint all-or-error checks. The base allocation
profile attributes 966.12 MB flat to `encoding/json.Marshal` and 970.49 MB
cumulative to manifest integrity reconstruction. Those request-fixed costs are
absent from the head top; its 389.97 MB differential allocation is dominated
by the unchanged partition-local HNSW search scratch. Base CPU includes 690 ms
flat SHA-256 and 1.11 s cumulative JSON struct encoding, while head CPU is
dominated by the expected HNSW candidate selection/search path.

The head records one bounded accepted placement identity. Warmup ended at one
cold open, one manifest-open attempt, one miss, one reader pin, two balanced
leases, and one hit. After 128 measured requests it still had one open/miss/
reader pin, with 129 hits and 130 balanced leases. Thus every measured request
was a hit and no measured request opened a manifest, missed, pinned a reader,
invalidated, or closed a session. Reopen accounting is aggregated by the one
immutable placement key; a changed ready-set or model digest for that key now
fails closed instead of adding retained history.

Authoritative retained artifacts are under
`/mnt/fast4tb/tmp/treedb_3981_final_g9LD0i`:

| Capture | File | SHA-256 |
| --- | --- | --- |
| base JSON | `base/out/vector_partition_m8_464aa721aef3_5f85ec14f3cd.json` | `148622375d43a8638546f7795ac59ebc584d0f509efea3a51b03cd09e95ba959` |
| head JSON | `head/out/vector_partition_m8_2fe04482d807_5f85ec14f3cd.json` | `0c44672c449a56d5a1ef89222c6d4f0f113de485516dc894675fa7be7e0b4591` |
| base alloc baseline/final | `base/profiles/{allocs_baseline,allocs}.pprof` | `fc3149cc76d458749b95f11af191b8e4ebc3513a55794f0a0f76bdb38025ffc9` / `8facf89b295f0c178564355c18d4c445b9710d96430792f262fc63ed1bc9dd8a` |
| head alloc baseline/final | `head/profiles/{allocs_baseline,allocs}.pprof` | `7aaeac940584040593e4e4a61d8e4662de944f42e157eeb7f0f977084c8e16a8` / `93dd9a331c56c26712978b409ac0670f03e66b457367e68f5594972fa40f98b5` |
| base CPU / heap / block / mutex / trace | `base/profiles/{cpu,heap,block,mutex}.pprof`, `trace.out` | `5f6a16af225135e5ecc7db8853f5a854e53cbfa13288909c9463c156e10c71a7` / `25abaddce6f155f4f2f5629c8461a4f994193d333c4d32fd73dae58b20696599` / `768201aedc011f85262d8834ebd566649d9851f8a03f85617105a6e1e39c68d3` / `601314d2976d1ba8e5fee92ebc75968b4fc0d38eb578426cad6cf2961ea391a5` / `b447f9c6ac9157fbae0ff6a52a27ba9f09563da56053e054a30d4543a1427ff9` |
| head CPU / heap / block / mutex / trace | `head/profiles/{cpu,heap,block,mutex}.pprof`, `trace.out` | `15c7a63b63f5244d554c95061666405553db855b5a1d8040a9a57d22038010dc` / `9ecb406d8fa56af3338e5e3b4159a62e3adcc13fd500b2e942a3ea8851e38033` / `9b284a9014774abc6ff391c71d0bab2950623092e2b2e17428aadc7c30255a54` / `416755363bc1ae3795c92b3ab3303764244c88c527e21de334165a1686ca317b` / `8bde8930a19f4ea407daee125343cbcfddc6334e842fc4a6d0c7aa117b9b9d20` |

This is one sequential local pair, not a statistical campaign. The head's
single-capture peak RSS is 9.16 MB higher while end live heap is 1.54 MB lower;
the retained generation pin is bounded, persistent asset bytes are unchanged,
and no configured resource limit was tested. This scoped tradeoff is accepted
for #3981's request-session objective; the M8 resource-bounds gate remains
`measured_not_bounded` and stays owned by the follow-on gate work.

## Earlier pre-rebase capture

The remainder of this document preserves the earlier matched capture for
history. Its performance numbers are superseded by the exact-head refresh
above.

## Decision and scope

The measured head removes the per-request manifest/asset validation-open work
from the warmed coordinator request path. On the one matched 128-query cell
below, it increases throughput 3.20x (+219.6%) and reduces cumulative allocation 94.9%.
The head router-session counters directly show that all 128 measured requests
reused the warmed generation-pinned router; no measured request opened a
manifest, missed the cache, pinned a reader, invalidated, or closed a session.

This is local loopback TCP evidence with real serialized M5 messages and
in-memory HashiCorp Raft consensus. It is not multi-host deployment evidence,
an exact-HNSW correctness pass, a resource-bounds pass, or a general M8
throughput claim.

## Exact revisions and environment

| Role | Revision | Meaning |
| --- | --- | --- |
| base | `f2f47a63c3806cd154bfad3a20ea1b75fea713a0` | parent before `nativewire: reuse generation-pinned coordinator routers` |
| head | `eaac9b7039bcd16e859a40b17a3b8818af02a50c` | #3981 branch head, including lifecycle evidence retention |

Both source trees were clean. Both runs used Go `go1.26.0`, Linux/amd64,
`GOMAXPROCS=16`, and the same host: Intel i5-11400F, 12 logical CPUs, 31 GiB
memory, Linux `6.8.0-124-generic`. The fixture, persistent assets, and
artifacts were all on the same `ext4 rw,noatime` `/mnt/fast4tb` mount.

## Matched workload and boundary

Fixture: `vector_partition_10k`, generated by
`treedb_vector_partition_fixture_v2`; 10,000 vectors, 128 queries, 64
dimensions, cosine metric, seed 1, checksum
`2413ef7c2f65a4b5ce8ecc3846f473fd85d337a87511538f962af7cdf6aec291`.

The two commands were identical except for their output/profile capture
directories:

```sh
# base f2f47a63c3806cd154bfad3a20ea1b75fea713a0
GOWORK=off GOMAXPROCS=16 go run ./cmd/treedb_vector_partition_bench \
  -mode production_multi_group \
  -dataset /mnt/fast4tb/tmp/gomap-3981-nativewire-router/testdata/vector_partition_10k \
  -out /mnt/fast4tb/tmp/treedb_3981_matched_oCFOpo/pre3981_run/out \
  -raft-groups 2 -raft-nodes-per-group 3 -partitions 4 -probes 4 -overlap 0 \
  -top-k 10 -recall-target .90 -concurrency 16 -warmup 1 -ef-search 4096 \
  -seed 1 -profiles /mnt/fast4tb/tmp/treedb_3981_matched_oCFOpo/pre3981_run/profiles \
  -format json

# head eaac9b7039bcd16e859a40b17a3b8818af02a50c
GOWORK=off GOMAXPROCS=16 go run ./cmd/treedb_vector_partition_bench \
  -mode production_multi_group \
  -dataset /mnt/fast4tb/tmp/gomap-3981-nativewire-router/testdata/vector_partition_10k \
  -out /mnt/fast4tb/tmp/treedb_3981_matched_oCFOpo/active/out \
  -raft-groups 2 -raft-nodes-per-group 3 -partitions 4 -probes 4 -overlap 0 \
  -top-k 10 -recall-target .90 -concurrency 16 -warmup 1 -ef-search 4096 \
  -seed 1 -profiles /mnt/fast4tb/tmp/treedb_3981_matched_oCFOpo/active/profiles \
  -format json
```

The topology was two three-node data Raft groups, four partitions, exact router
mode with a 64-candidate budget, four probes, `ef_search=4096`, concurrency
16, `top_k=10`, overlap 0, and one warmup request. The timed boundary is the
runner's wall-clock query cells after topology creation, exhaustive endpoint
preflight, and generation warmup. It includes the coordinator/router, TCP M5
serialization, Raft read-index/apply, persistent HNSW search, merge, and caller
scheduling; it excludes topology construction, exact truth, preflight, warmup,
artifact encoding, and shutdown.

## Result

| Metric | base | head | head vs base |
| --- | ---: | ---: | ---: |
| QPS | 46.36 | 148.14 | +219.6% |
| p50 | 340.12 ms | 102.65 ms | -69.8% |
| p95 | 366.11 ms | 130.16 ms | -64.4% |
| p99 | 376.34 ms | 131.94 ms | -64.9% |
| Recall@10 | 0.96953125 | 0.96953125 | unchanged |
| Request bytes/query | 1,600 | 1,600 | unchanged |
| Response bytes/query | 2,064 | 2,064 | unchanged |
| Candidate bytes/query | 640,000 | 640,000 | unchanged |
| RPCs/query | 2 | 2 | unchanged |
| Persistent asset bytes | 3,686,024 | 3,686,024 | unchanged |
| Measured peak RSS | 132,820,992 | 131,362,816 | -1.1% |

The request/response/candidate byte totals above are the raw 128-request totals
divided by 128. Both runs had two groups and two RPCs per query, passed the
unavailable-endpoint all-or-error check (zero returned neighbors/groups), and
had the same balance values: 2,500 maximum partition load under the 2,625
hard cap.

## Allocation, heap, and CPU profiles

`allocs.pprof` is cumulative, so the allocation figures below are the
difference from each run's `allocs_baseline.pprof`:

```sh
go tool pprof -top -sample_index=alloc_space \
  -base <profiles>/allocs_baseline.pprof <profiles>/allocs.pprof
```

| Profile observation | base | head |
| --- | ---: | ---: |
| Differential allocation | 1,447.95 MB | 74.05 MB |
| Differential allocation/query (128 samples) | 11.312 MB | 0.579 MB |
| End `inuse_space` heap snapshot | 42.83 MB | 43.31 MB |
| Peak RSS (runner measurement) | 132.82 MB | 131.36 MB |

This is a 94.9% allocation reduction. The base allocation profile attributes
941.24 MB flat (65.00%) to `encoding/json.Marshal`, 702.84 MB cumulative to
`encodeVectorPartitionManifestWithContextV1`, and 940.43 MB cumulative to
`VectorPartitionManifestV1.integrityDigestWithContextV1`. Its CPU profile has
530 ms flat in SHA-256 and 1.39 s cumulative in JSON structure encoding.
The head allocation profile attributes 5.16 MB flat to `encoding/json.Marshal`
and 3.16 MB cumulative to manifest encoding; its CPU top is instead the
expected HNSW candidate selection/search work. CPU sample totals are not used
as a percentage-comparison metric because profile durations differ.

The end heap snapshot is not a peak measurement. The runner's peak RSS value is
the appropriate captured process-peak metric; it remains effectively flat in
this single paired capture.

## Head router-session evidence

The base predates these counters. The head records one warmed identity:
`default/default/m8_production_vectors/embedding_graph`, source generation 2,
partition generation 3, with its exact ready-set and router-model digests in
the retained JSON.

| Counter | after warmup | after 128 measured queries | measured delta |
| --- | ---: | ---: | ---: |
| cold opens | 1 | 1 | 0 |
| manifest open attempts | 1 | 1 | 0 |
| misses | 1 | 1 | 0 |
| hits | 1 | 129 | +128 |
| open failures | 0 | 0 | 0 |
| reader pins / releases | 1 / 0 | 1 / 0 | 0 / 0 |
| lease pins / releases | 2 / 2 | 130 / 130 | +128 / +128 |
| invalidations / closes | 0 / 0 | 0 / 0 | 0 / 0 |

Thus the measured steady-state rate is one cache hit and one balanced
lease/release per request, and zero manifest opens, misses, cold opens, reader
pins, invalidations, or closes per request. There is no literal base-side
counter series because the instrumentation was added by #3981; the base-side
cost attribution above comes from its matched allocation and CPU profiles.

## Retained artifacts

Artifact root: `/mnt/fast4tb/tmp/treedb_3981_matched_oCFOpo`.

| Capture | Retained file | SHA-256 |
| --- | --- | --- |
| base JSON | `pre3981_artifacts/out/vector_partition_m8_f2f47a63c380_074789700c38.json` | `115abb6d4323a60388c52afecadc2ce5edde0435a395c8bdba6d8c5b0a0ff309` |
| head JSON | `head_artifacts/out/vector_partition_m8_eaac9b7039bc_074789700c38.json` | `fe540416aaaef307b8875a7394ef559eb4b190c702cfc9ee22248a674d78e259` |
| base alloc baseline/final | `pre3981_artifacts/profiles/{allocs_baseline,allocs}.pprof` | `2fb88b14f4592397f2bec77f24154efd69a29cc7e359ab223b23709b709f5478` / `610178a83a64493ea44c850ae24cc0c57fc88c6d17255b8fc3b4207304db5b8d` |
| head alloc baseline/final | `head_artifacts/profiles/{allocs_baseline,allocs}.pprof` | `3999a8c9b38997987d9a68ca8729cccb4931c1ab5249829b210cf313ba90c4ab` / `1bfdccdad93e9ba11a83f5ad73e0d5daa5fb2bb4e1b6a46f05433900094a650a` |
| base CPU / heap / block / mutex / trace | `pre3981_artifacts/profiles/{cpu,heap,block,mutex}.pprof`, `trace.out` | `411d9acf7fcea8b763d9ad6fb1794e8c63e6f939aa64f4f0e4c4ee1faa6fa41e` / `8b02739898edcad1fa9c9f7b1647382eb76f81867f1865380653b9446d5ab9ed` / `bad73a84a5c3edc23579c24570c2d94cb2418389f288a6fb1803d49ac668a20e` / `d500d8e1d31aef4a2b3d58993015b1189eb075cb60a686d676a8ac89a8510cf1` / `6953e6f7989815e68faf3241640667cf094eb58e62a38746a5f9549932cb08f5` |
| head CPU / heap / block / mutex / trace | `head_artifacts/profiles/{cpu,heap,block,mutex}.pprof`, `trace.out` | `3e86181cd92132739b63b94a4fbe6d99ea2e89e72d595bf69154c30d1170fe38` / `c0628aab971e83f18d4ccc67f5fa5fbbd079d15353199c3139f14409bbb9ad24` / `5978e27436b55c9f77ded305faac0c6bcc136482af1abbf31221eef16373bab2` / `4e4b4ee4e929b76b5ebf3b1a5eeb64dbaec8da898bae97ed9358cc5a35191da4` / `d157cce8cdd966e3d064ec85fd9f8df54c0ae18956a56c69d450c6510ee191ed` |

The report intentionally names the direct retained base profile set above; an
earlier interrupted capture also left nested duplicate files under the artifact
root and is not used for any number in this report.

## Caveats and interpretation

- This is one sequential base/head pair, not a repeated randomized or
  statistically significant campaign. The large allocation and latency changes
  are direct observations for this fixed host and cell, not a universal
  throughput promise.
- CPU, block, mutex, and trace profiles cover the measured query cells **plus**
  the endpoint-loss fault boundary. Allocation values therefore remain profile
  evidence for that documented scope, rather than isolated microbenchmark
  `B/op` values.
- The M8 result status remains `experimental_gate_failures`: exact all-partition
  HNSW parity is checked but false. Failure honesty passes and returns no
  partial response, but this pre-existing parity gate is unrelated to the
  router-session reuse result and still blocks M8 enablement claims.
- The base has no router-session counter schema. Its per-request
  marshal/hash/open work is demonstrated through matched profile attribution;
  only the head supplies literal open/miss/hit/lease counter deltas.
- No configured process-resource limit was compared, so the unchanged asset
  bytes and near-flat RSS do not constitute a resource-bounds pass. Mmap
  residency is not sampled by this runner.
