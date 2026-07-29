# Vector-partition review 3998 v1

This directory retains the machine-readable review ledger for the
fixture-scoped duplicate-aware graph correction. Exact duplicate normalized
vectors receive zero-distance, degree-bounded links before partitioning. The
measured implementation is `60fa60efbd5497433444f46a519c7751ba7108ca`
(binary SHA-256
`f7f27cd0bf25fd10c0d499ec7e6d9df982abad0d8b155ab1d6271bf56e4b9a2d`)
on base `53cc1c338636ba8168aa13cc70c2aa167257f361`.

On fixture checksum
`71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f`,
exact representative routing recall@10 is `1.0` at four of sixteen probes.
Disjoint end-to-end recall is `0.90625`; its only residual loss owner is
partition-local HNSW. Qualified graph overlap `0.20` realizes all `200,000`
memberships, improves four-probe end-to-end recall to `0.996875`, and reaches
`1.0` at sixteen probes with exact exhaustive ID/score parity.

The overlap treatment costs `1.200089958633x` derived M3 bytes and
`1.200100789171x` persistent M8 bytes relative to graph/disjoint, below the
issue's `1.35x` limit. All overlap partition loads are exactly `75,000`, equal
to the hard cap, and the measured resource comparisons pass.

[`evidence.json`](./evidence.json) records exact commands, source and binary
SHAs, report/time-log checksums, profile checksums, hardware context, routing
attribution, storage arithmetic, negative experiments, gate dispositions, and
limitations. The corpus contains only 124 unique normalized vector bit
patterns and queries copy corpus vectors. This evidence therefore closes the
declared fixture gate; it is not a general population-level or multi-host
enablement claim.
