# #4096 strict search proof evidence

This compact packet is the exact-product-head evidence for #4096. It proves
that ordinary strict `OperationsV1.Search` uses one fresh ingress catalog proof
and one immutable serving-snapshot pin per query, with no per-request data-group
proof, generation pin, partition open, or manifest/router reconstruction. It
does not rerun or replace the frozen five-system gold matrix.

The 29 GiB logical scratch root containing the rebuilt database, binaries,
logs, raw rows, and 189 profiles remains outside git at
`/mnt/fast4tb/gomap-4096-strict-proof-evidence-a640b522-static-vcs`.
`RAW_EXTERNAL_SHA256SUMS` binds the omitted evidence. The committed packet is
under 100 KiB.

## Exact identity

| Item | Value |
| --- | --- |
| Base | `977557884d1c0e991a95e01c8824b25df5308c28` |
| Instrumented product head | `a640b522fcfd90ef6d3c97bdd54058f04fc38ced` |
| Benchmark binary SHA-256 | `fdcdfd53ce468a8398cfdb9199850e2bd42792fca479d48818787140be49b05f` |
| Capability key SHA-256 | `a6a1f34d6b3ff84362770b55e48fd59b8d84af30505a6298fa72ca2b0d9c1555` |
| Container image | `sha256:703637f37f9d45e70151a8c21bfe0f5b0c5cb68897f578df59dff7ba62e17c56` |
| M3 artifact SHA-256 | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| M3 descriptor SHA-256 | `038a7835a00654612a9269b8d4798fc59ec7ef30706e0f9d194d524c462c25c6` |
| Fixture checksum | `d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69` |
| Truth artifact SHA-256 | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |

The exact 250k graph-overlap M3 rebuild passed once in 13:51.34 with peak RSS
2,153,120 KiB. Nine search rows then ran serially: single, native four-daemon,
and container four-daemon, each repeated three times at p2, EF128, c1 and c32.

## Qualified matched-recall result

Every cell completed 1,000/1,000 queries with recall 0.9247, generation 1,
zero errors, zero timeouts, zero retries, and zero redirects.

| Topology | c1 QPS / p95 | c32 QPS / p95 |
| --- | ---: | ---: |
| Single daemon | 751.20 / 1.53 ms | 3,474.31 / 15.85 ms |
| Native four-daemon | 745.80 / 1.57 ms | 2,226.79 / 23.71 ms |
| Container four-daemon | 750.45 / 1.56 ms | 2,168.71 / 23.79 ms |

Relative to the exact #4091 runtime-ownership baseline, every predeclared cell
passes the >=1.05x QPS and <=1.05x p95 gates:

| Topology | c1 QPS / p95 ratio | c32 QPS / p95 ratio |
| --- | ---: | ---: |
| Single daemon | 1.123 / 0.828 | 1.141 / 0.973 |
| Native four-daemon | 1.397 / 0.597 | 1.298 / 0.855 |
| Container four-daemon | 1.355 / 0.613 | 1.342 / 0.788 |

Native and container are effectively equal: their median QPS ratios are 0.994
at c1 and 1.027 at c32, while p95 ratios are 1.004 and 0.997. Strict proof
propagation therefore removes the former c1 topology tax. At c32, native still
retains only 64.1% of single QPS and has 1.50x p95; #4098 and #4097 own the
remaining pinned-session and wire/allocation work.

## Proof and payload attribution

Each cell retains exactly 1,000 strict ingress proofs and 1,000 snapshot pins.
Request-side data-group proofs, generation pins, partition opens, and unknown
proofs are all zero. The separately retained background serving refresh is
quorum-backed and no-log; its variable process-lifetime count is not charged
to foreground requests. All catalog stage totals are independently recomputed.

Compared with the #4090 on-ramp profile, median catalog lifecycle work per
query falls by 99.88-99.90% at c1 and 98.30-99.23% at c32. Selected
partitions/groups, RPCs, candidates, edges, query bytes, candidate bytes,
response bytes, generation, and recall are exact across all rows.

The strict capability intentionally carries topology-bound authority floors.
Service request bytes are 2,560,928 per single cell and 2,573,766 per
four-daemon cell: a retained 0.50% increase. The reducer requires native and
container bytes to match exactly and caps the topology delta at 1%; it does
not hide this protocol cost behind a false byte-parity claim.

## Validation and reproduction

`benchmarks/vector_db_compare/strict_proof.py` rejects stale topology/config,
binary, dataset, truth, M3, client, capability-key, profile, raw timing,
semantic-work, proof-attribution, recall, performance, or payload-boundary
evidence. `comparison.json` is its machine-readable qualified output.

Validate this committed packet with:

```sh
(cd TreeDB/docs/evidence/vector-partition-strict-proof-4096/a640b522 && sha256sum -c SHA256SUMS)
(cd benchmarks/vector_db_compare && python3 -m unittest test_runtime_ownership.py test_topology_tax.py test_strict_proof.py)
```

On the evidence host, validate and reduce the omitted artifacts with:

```sh
(cd /mnt/fast4tb/gomap-4096-strict-proof-evidence-a640b522-static-vcs && sha256sum -c /mnt/fast4tb/gomap-4096-strict-vector-proof/TreeDB/docs/evidence/vector-partition-strict-proof-4096/a640b522/RAW_EXTERNAL_SHA256SUMS)
python3 benchmarks/vector_db_compare/strict_proof.py \
  --root /mnt/fast4tb/gomap-4096-strict-proof-evidence-a640b522-static-vcs \
  --out /tmp/strict-proof-comparison.json
```

## Claim boundary

#4096 keeps ordinary Search strict while propagating one server-authenticated
ingress proof over one immutable serving snapshot. It does not add relaxed or
pinned APIs, change routing/index/search/topology, replace the bounded framed
JSON wire codec, or claim that the remaining c32 distributed tax is solved.
