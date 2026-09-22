# Vector-partition recall attribution: retained 100k evidence

Date: 2026-07-30

Status: **the attribution gate is complete; experimental enablement remains off.**

This report closes the measurement contract in
[#4023](https://github.com/snissn/gomap/issues/4023). It identifies which
stage owns recall loss without changing partition assignment, overlap,
routing, HNSW, Raft, or serving behavior. The run is a single-host,
multi-daemon loopback experiment on 100,000 vectors, so it is not the
multi-host qualification owned by #3983.

## Retained experiment

All three variants use the same deterministic 100,000-vector,
1,000-held-out-query, 128-dimensional cosine corpus, seed 4017, 16 partitions,
four three-node Raft groups, probes `1,2,4,8,16`, `top_k=10`, `ef_search=64`,
and concurrency 1. The fixture checksum is
`ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95`.
Canonical FP32 truth has identity
`accdb76c693e2da99333b9327efc0e3d83ba630b25a8ba2b6820f5a6f6e38937`
and SHA-256
`0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c`.

The measured implementation head is
`6d69bb99ab2e30ff33045fc39e3041780fc63be3`, based on
`765616d3d20e5fa612376711fd9628e3d8f7c9ee`. Exact M3 and M8 commands are
embedded in the retained JSON artifacts.

| Probes | Graph/disjoint | Graph/overlap .20 | Stable-ID-hash/disjoint |
| ---: | ---: | ---: | ---: |
| 1 | .2323 | .3972 | .0730 |
| 2 | .4072 | .6266 | .1332 |
| 4 | .6211 | .8220 | .2578 |
| 8 | .8316 | .9192 | .4979 |
| 16 | .9971 | .9894 | 1.0000 |

These are production-shaped end-to-end recall@10 values. Exhaustive
partition-union ID and score parity (`exhaustive_partition_union_id_parity`,
`exhaustive_partition_union_score_parity`) pass for every variant.

## Quarter-probe ownership

At four probes, the graph/overlap row gives the clearest adjacent-stage
attribution:

| Stage | Recall@10 | Adjacent effect |
| --- | ---: | ---: |
| Global exact | 1.0000 | - |
| Best primary-home oracle | .8303 | -.1697 |
| Best final home+overlap oracle | .9637 | +.1334 from overlap |
| Exact representative routing + exhaustive local exact | .8342 | -.1295 |
| Approximate representative routing + exhaustive local exact | .8342 | .0000 |
| Partition-local HNSW | .8220 | -.0122 |
| Production end to end | .8220 | .0000 |

The largest active loss after overlap materialization is exact representative
routing, followed by the remaining placement/membership ceiling. Local HNSW
is a smaller secondary loss, and this candidate budget introduces no
approximate-router loss at four probes. The graph/overlap membership ceiling
is .9637, above the .90 target, but the served result remains .8220; therefore
this is a diagnostic success, not an enablement pass.

Overlap accounts for .2107 exact-route truth coverage at four probes, with
.2206 duplicate-membership coverage. It materializes 20,000 positive-gain replicas, no
filler or rejected replicas, and no unused capacity. Directed graph cut falls
from 1,122,051 to 773,414 edges, or 17.43185 cut edges per useful replica. The
persistent asset ratio is 1.20181x.

Graph assignment retains 17.6822% of exact truth-neighbor pairs versus 6.1644%
for stable-ID hash. Its directed cut ratio is .70128 versus .93754, and its
four-probe primary-home oracle is .8303 versus .6379. This demonstrates useful
graph locality while also showing that the present representative router does
not convert that full ceiling into queried-partition coverage.

## Decision and follow-up ownership

The matrix correctly reports `experimental_gate_failures` with disposition
`enablement_off_follow_up_required`. Exactness, failure honesty, reachability,
recall, balance, overlap storage, and resource gates pass. Probe reduction,
matched-recall QPS/tail, and coupled graph acceptance fail; existing-behavior
CI is recorded separately at the final pull-request head.

The measured owners map directly to the existing follow-ups:

- #4024: primary placement and graph-locality ceiling.
- #4025: overlap selection and the remaining membership ceiling.
- #4026: exact representative-routing loss, the dominant active four-probe gap.
- #4027: the smaller partition-local HNSW loss.

No policy is selected here. The retained evidence makes those tickets
independently testable and prevents router, overlap, and HNSW effects from
being pooled into one end-to-end number.

## Retained artifacts

The complete machine-readable matrix and compact M2/M3 reports are committed
under `TreeDB/docs/spec/artifacts/vector-partition-attribution-4023/`. The
adjacent `vector-partition-attribution-4023.json` ledger is the authoritative
path and SHA-256 index; its linkage test verifies all six published artifacts.

The matrix records the host, resource limits and observations, variant
identities, topology, exact commands, all probe rows, stage ledger, gate
ledger, limitations, and the concise decision report. The uncommitted
11 MB partition maps and persistent databases remain local because the compact
reports identify them cryptographically and are sufficient to reproduce the
run.
