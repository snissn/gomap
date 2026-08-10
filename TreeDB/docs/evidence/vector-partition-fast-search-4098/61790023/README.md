# #4098 bounded fast-search evidence

This compact packet is the exact-product-head reference for #4098. It proves
that strict, bounded-staleness, and pinned-session searches return the same
truth-bound results through the native four-daemon production route, while
fast and pinned searches perform no per-query strict catalog read. It does not
rerun or replace the frozen five-system gold matrix.

The 205 MiB scratch root containing the detached source, static binaries, raw
result JSON, commands, and process logs remains outside git at
`/mnt/fast4tb/gomap-4098-fast-search-evidence-61790023d`. Successful cleanup
removed the copied databases. `RAW_EXTERNAL_SHA256SUMS` binds the omitted
artifacts; this committed packet is only a few KiB.

## Exact identity

| Item | Value |
| --- | --- |
| #4096 base | `50f6ab6a2ed4b4c9f2cfc8b6720b07d7a55c9151` |
| Instrumented product head | `61790023df00a0050c8f35aa58f67850565a80c7` |
| Benchmark binary SHA-256 | `11186d2a948937314409d9da748dbb47be858b01a93212057caaf10f2eab62a2` |
| Rebind helper SHA-256 | `9a933ffef41bead05352cdbc8baa89612713ec12363481b707aa63613b4a91d7` |
| M3 artifact SHA-256 | `57ad36d923c5fdb701a082727fd24efdcf0c6ac0e24efeda28ca11f232a65f1d` |
| M3 descriptor SHA-256 | `038a7835a00654612a9269b8d4798fc59ec7ef30706e0f9d194d524c462c25c6` |
| Fixture manifest SHA-256 | `14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025` |
| Truth artifact SHA-256 | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` |
| Reference runner SHA-256 | `bb1c69dbed36b2af040c5c48e84f44e4b1fb0a064281309a114a71fe647344ad` |

The binary is a static `go1.26.0 linux/amd64` executable whose build info binds
the product head above with `vcs.modified=false`.

## Reference result

One native four-daemon topology ran strict, fast, then pinned mode serially.
Each cell used the retained 250k graph-overlap assets, p2, EF128, router
candidate budget 256, top-k 10, 1,000 warmup queries, and 1,000 measured
queries. Every cell completed with recall 0.9247, generation 1, and zero
errors or timeouts.

| Mode | c1 QPS / p95 | c32 QPS / p95 | Strict reads | Snapshot/session pins |
| --- | ---: | ---: | ---: | ---: |
| Strict | 766.22 / 1.52 ms | 2,226.35 / 22.90 ms | 1,000/cell | 1,000 / 0 |
| Fast | 755.12 / 1.60 ms | 2,336.24 / 22.93 ms | 0 | 1,000 / 0 |
| Pinned | 724.80 / 1.67 ms | 2,132.71 / 25.99 ms | 0 | 0 / c1:1, c32:32 |

The only catalog reads in fast and pinned cells are 1-5 background serving
refreshes for the whole cell. Pinned requests reuse a bounded session while
validating the refreshed authority proof before capability construction.

## Directional gold-rubric context

The frozen 250k native TreeDB row was 22.2 QPS / 71 ms at c1 and 60.5 QPS /
694 ms at c32. The current strict reference is 34.5x and 36.8x higher QPS,
with p95 reduced to 2.1% and 3.3% of those values. Against the frozen external
rows, current strict QPS is 1.89x/1.68x Milvus and 2.60x/1.40x pgvector at
c1/c32. This is directional only: the external systems were not rerun, this is
one reference repetition, and their frozen recalls differ slightly.

## Validation

Validate the committed packet with:

```sh
(cd TreeDB/docs/evidence/vector-partition-fast-search-4098/61790023 && sha256sum -c SHA256SUMS)
```

On the evidence host, validate the retained raw boundary with:

```sh
(cd /mnt/fast4tb/gomap-4098-fast-search-evidence-61790023d && sha256sum -c /mnt/fast4tb/gomap-4098-fast-search/TreeDB/docs/evidence/vector-partition-fast-search-4098/61790023/RAW_EXTERNAL_SHA256SUMS)
```

## Claim boundary

#4098 adds explicit strict, fast, and pinned query shapes without weakening
authorization, revocation, generation, integrity, or partial-result handling.
This packet proves correctness and foreground proof removal; it does not claim
that fast or pinned must beat strict in a one-shot timing row. #4097 owns the
remaining low-allocation wire work, and #4093 owns final focused revalidation.
