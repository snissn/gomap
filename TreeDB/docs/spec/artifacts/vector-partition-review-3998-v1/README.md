# Vector-partition review 3998 v1

This directory retains the machine-readable review ledger for the
fixture-scoped duplicate-aware graph correction. Exact duplicate normalized
vectors receive deterministic zero-distance links that remain connected after
degree pruning, including classes larger than the configured graph degree. The
measured implementation is
`866e08035bdc7b98c34ab31641f0d794d937dd9d` (binary SHA-256
`2f0896113893eecfb4936f28d0ab8b14d67e8d94c3d47aed060b247bb332e9e0`)
on base `53cc1c338636ba8168aa13cc70c2aa167257f361`. All semantic review fixes are
included in that measured SHA: truth-oracle query/byte/work preflight, one
cached and memory-modeled M8 truth-home mapping, attribution-inclusive peak
RSS, and the large duplicate-class connectivity regression.

On fixture checksum
`71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f`,
exact representative routing recall@10 is `1.0` at four of sixteen probes.
Disjoint end-to-end recall is `0.96875`; its only residual loss owner is
partition-local HNSW. Qualified graph overlap `0.20` realizes all `200,000`
memberships, improves four-probe end-to-end recall to `0.99375`, and reaches
`1.0` at sixteen probes with exact exhaustive ID/score parity.

The overlap treatment costs `1.200236359428x` derived M3 bytes and
`1.200253419554x` persistent M8 bytes relative to graph/disjoint, below the
issue's `1.35x` limit. All overlap partition loads are exactly `75,000`, equal
to the hard cap, and the measured resource comparisons pass. M8 peak RSS is
captured through post-measurement attribution and includes the cached
truth-home map: `2,247,274,496` bytes for disjoint and `2,003,255,296` bytes for
overlap, both below the `4,294,967,296`-byte cap.

[`evidence.json`](./evidence.json) records exact commands, source and binary
SHAs, report/time-log checksums, profile checksums, hardware context, routing
attribution, storage arithmetic, negative experiments, gate dispositions, and
limitations. The corpus contains only 124 unique normalized vector bit
patterns and queries copy corpus vectors. This evidence therefore closes the
declared fixture gate; it is not a general population-level or multi-host
enablement claim. The broad disjoint measured-HNSW exhaustive gate remains red
at `0.96875`; #3998 explicitly assigns that loss class to #3999, while its
issue-scoped exact representative routing and exhaustive exact-union parity
both pass at `1.0`.
