# TreeDB construction-policy history for #4587

This mutable, non-authoritative inventory records the measured predecessors that
bound the #4587 construction-policy decision. It is provenance only: it does not
freeze the C0 protocol, authorize a retained run, or carry a GO/NO-GO verdict.

The accepted production control is TreeDB
`fcc1d79696a9b7c6655b793f6224f6937e698ce5`: Cohere-10M, 768-dimensional
cosine, M16, efConstruction300, with a 5,774.242 s adjacency build, 0.9335
Recall@100, 0.9425 NDCG@100, 21,986.664 QPS, 2.1638 ms concurrent p99, and
75.480 GiB peak service RSS. The original #4587 definition base
`6beea3ace082eee8afe5dccf629cc1a533823bfc` changed only four text-index files
under the inspected `TreeDB/collections`, `scripts`, `cmd`, and
`docs/benchmarks` scope relative to that control; no vector-construction or
benchmark-harness path changed. The repaired protocol branch incorporates the
merged observer prerequisite at
`8cf3a6a8d19cf8615b5301795937048857464085`; its final runtime identities are
pinned separately in the contract. That integration does not authorize
execution: after exact-head review, the external coordinator authorization flow
must build the service binary from the clean reviewed commit with the contract's
complete non-inherited Go environment, then bind the build argv, environment,
toolchain identity, and resulting binary checksum.

| Candidate family | Exact design tested | Integrated wall result | Quality result | Memory result | Disposition | Why C0 is causally different |
| --- | --- | ---: | ---: | ---: | --- | --- |
| [#4432](https://github.com/snissn/gomap/issues/4432#issuecomment-5461367877) exact four-row indexed AVX-512 | Interleave four independent random FP32 rows while preserving each row's arithmetic and graph bytes. | Cohere-250K adjacency 97.949 s -> 100.462 s (+2.6%) despite 22-25% single-thread micro gains. | Recall/NDCG stayed 0.2436/0.2443; reopen and route passed. | No claimed material win; aggregate workers were already bandwidth-saturating. | NO-PROMOTE; do not reopen multi-row FP32 or worker-level MLP. | C0 changes efConstruction and therefore the topology/quality contract; it is not an exact-byte kernel substitution. |
| [#4443](https://github.com/snissn/gomap/issues/4443#issuecomment-5462565405) packed scalar-u8 ordinary diversity | Packed offline arena plus scalar-u8 traversal, reciprocal scoring, and ordinary diversity. | Adjacency 96.818 s -> 7.335 s (-92.42%). | Recall 0.2436 -> 0.0003; NDCG 0.2443 -> 0.0002. | Optimize current RSS -1.36%, lifecycle peak RSS +6.36%. | NO-PROMOTE; approximate ordinary diversity destroyed navigability. | C0 retains the production exact-FP32 builder and only calibrates its declared search width. |
| [#4444](https://github.com/snissn/gomap/issues/4444#issuecomment-5462735285) packed/scalar navigation with exact ordinary diversity | Restore exact FP32 final ordinary-diversity decisions on the #4443 packed/scalar path. | Adjacency 96.818 s -> 99.322 s (+2.59%). | Recall/NDCG restored to 0.2436/0.2443. | Optimize current RSS -0.06%, lifecycle peak RSS +7.31%. | NO-PROMOTE; restoring exact decisions restored the wall. | C0 does not combine scalar navigation with an exact decision stage and introduces no new score plane. |
| [#4447](https://github.com/snissn/gomap/issues/4447#issuecomment-5462844107) exact four-row diversity chunks | Evaluate selected neighbors in fixed exact-FP32 chunks and stop after the first exact rejection. | Adjacency 96.818 s -> 100.132 s (+3.42%). | Recall/NDCG parity at 0.2436/0.2443. | Peak RSS +6.69%; total-sys +10.00%. | NO-PROMOTE; work reduction was unproven at product scale and wall/RSS independently failed. | C0 reduces the builder's candidate breadth prospectively instead of preserving every exact decision with a chunking micro-optimization. |
| [#4452](https://github.com/snissn/gomap/issues/4452#issuecomment-5463349976) bulk Vamana | Deterministic private bulk graph with seed/refinement/backbone and existing persisted format. | Adjacency 96.818 s -> 57.425 s (-40.69%). | Recall/NDCG collapsed to 0.0066/0.0066. | No terminal memory failure reported. | NO-PROMOTE; structural speed ceiling passed but graph quality failed. | C0 remains production HNSW; it is not a renamed bulk/Vamana topology. |
| [#4453](https://github.com/snissn/gomap/pull/4453) evidence serialization | Bound Minima per-batch diagnostic serialization; no product optimization or performance run. | Not run. | Not run. | 10,000-correlation synthetic evidence stayed bounded. | MERGED as qualification infrastructure, not a construction candidate. | C0 consumes lifecycle evidence; it does not claim this infrastructure change as a candidate. |
| [#4454](https://github.com/snissn/gomap/issues/4454#issuecomment-5463452936) completed frozen-epoch Vamana | Add all-row beam refinement, reciprocal proposal union, RobustPrune, and deterministic component repair. | Build exceeded the 77.454 s ceiling and was stopped before completion. | No valid product quality result. | No terminal memory result. | NO-PROMOTE; wall gate failed irreversibly. | C0 neither adds Vamana refinement epochs nor changes the graph family. |
| [#4455](https://github.com/snissn/gomap/issues/4455#issuecomment-5463561033) landmark HNSW skeleton | Ordinary HNSW on deterministic anchors plus parallel local attachment and one final exact prune. | Adjacency 93.734 s, above the 77.454 s gate. | Not inspected after the irreversible wall miss. | 3,514,756 KiB observed RSS, within its gate. | NO-PROMOTE; landmark structure did not remove enough work. | C0 has no anchors, buckets, bridge repair, or restricted attachment candidates. |
| [#4456](https://github.com/snissn/gomap/issues/4456#issuecomment-5463678267) parallel landmark finalization | Parallelize row-owned landmark attachment and exact final pruning. | Adjacency 96.818 s -> 73.296 s (-24.3%). | Recall 0.2436 -> 0.2117; NDCG 0.2443 -> 0.2140. | Peak RSS +1.0%, within its gate. | NO-PROMOTE; scheduling cleared time but not the landmark topology's quality ceiling. | C0 uses the unrestricted production HNSW candidate mechanism and tests a bounded quality/speed trade-off. |
| [#4457](https://github.com/snissn/gomap/issues/4457#issuecomment-5463896573) widened exact HNSW epochs | Widen frozen-prefix planning with exact intra-epoch closure; corrected candidate staged future peers and proved activation. | Correctly activated run stopped before optimize completion. | Product quality not reached. | 4,196,950,016 B crossed the 4,188,420,096 B hard cap. | NO-PROMOTE; terminal RSS failure. | C0 leaves epoch width, scheduling, reciprocal grouping, and commit dependencies untouched. |
| [#4461](https://github.com/snissn/gomap/issues/4461#issuecomment-5464329711) exact-decision observer | Measure planning/reciprocal rows, groups, rejection positions, repeats, and a staged exact-dot ceiling without changing topology. | Diagnostic only: 3.213B scored rows; ideal staged ceiling 19.8220%, below its 20.0004% wall gate. | Observer proved exact topology/search parity; no product candidate. | Observer bounded below 70 KiB; diagnostic process peaked at 3,176,852 KiB including the FP32 matrix. | Terminal NO-GO for replay, coalescing, staged indexed-dot, worker-count, or batch-width successors. | C0 explicitly changes the construction quality/breadth contract; it does not claim an exact-topology micro ceiling can now clear the gate. |

## Preserved local evidence inspected

The predecessor artifact roots remain present at
`/mnt/fast4tb/treedb-{4432,4440,4443,4444,4447,4454,4455,4456,4457}-evidence`
and `/mnt/fast4tb/treedb-4452-bulk-vamana-valid`. Completed roots retain
manifests, lifecycle journals, diagnostics, route/count proofs, results, and
artifact-owned databases; stopped runs retain their partial journals and
logs. #4456 additionally retains its search-only recovery proof. The terminal
metrics and causal dispositions above come from the linked issue comments,
not from reinterpretation of partial files.

## Current local Cohere assets

- Cohere-250K: `/mnt/fast4tb/treedb-4568-dataset-cohere250k`; 250,000 ordered
  rows in `train.parquet` (SHA-256
  `27c6144b8ab65e24015775f18943690c30ac17df4e052745877f3663d350db0b`),
  1,000 queries in `test.parquet` (SHA-256
  `a2ec76c907da22e5533ce31c1105e4fd4d3c4597915a7771d5e41897a95ecbed`),
  and matching ground truth in `neighbors.parquet` (SHA-256
  `30e7467766be3e2a6b167df16388a4a35e38fa6cc9e593fb659cce81ffde0940`).
- Cohere-1M: `/mnt/fast4tb/treedb-4384-local-qualification-20260828/datasets/cohere-1m`;
  1,000,000 ordered rows in `train.parquet` (SHA-256
  `023b16f7abafecda7c790a25f6eb3837367070f6ea8837be1f986fbd302ccf21`).
  Its query and ground-truth files are byte-identical to the 250K files.
- The local 10M retained archive records canonical file checksums, but it does
  not contain the 10M Parquet corpus. No 10M input is needed for #4587 C0.
