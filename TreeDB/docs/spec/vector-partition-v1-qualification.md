# Vector-partition V1 qualification

This is the local-evidence contract for issue #4015.  It supplements, and does
not change, the frozen V1 correctness contract.  In particular, canonical M8
exact truth and exact-union parity remain FP32 semantics; generated fixture
matrices are FP64 only as deterministic benchmark input.

## Dataset identities

The live structured qualification owned by #4027 uses the retained 100,000-row
embedding-mixture calibration and
`testdata/vector_partition_qualification_embedding_mixture_250k`: 250,000
vectors and 1,000 held-out queries generated with
`treedb_vector_partition_embedding_mixture_v1`. It is an embedding-shaped
mixture of directional topics with continuous noise, explicitly not a claim
about a licensed external corpus. The committed 250k manifest is the required
second structured corpus for this node; the retained 100k corpus is
calibration, not a substitute for it.

`testdata/vector_partition_qualification_high_entropy_1m` remains a committed
high-entropy routing-stress fixture. Its p8/p16 work is separately owned by
`#4030` and is neither pooled with nor a blocker for the #4027 structured
qualification claim.

## Required artifact rows

Each retained result must bind: fixture manifest/checksum, base/head SHA,
hardware and topology, variant descriptor and asset digest, probes, ef-search,
concurrency, samples, timing boundary, profiles, and exact command.  A
comparable matrix includes graph-disjoint, graph-overlap-020, stable-ID-hash
disjoint, and exhaustive all-partition rows under the same topology and local
search settings. For #4027 repeat one records the p1/p2/p4/p8/p16 ladder;
repeats two and three retain p4/p16 at `ef_search=64`, concurrency `1`, and
approximate router candidate budget `64`; the retained matrix continues to
bind the exhaustive control.

Repeat the p4/p16 comparison three times per structured corpus. The campaign
index hashes every matrix, requires one exact head and corpus/truth/variant
identity across the repeats, and derives median/min/max QPS plus p95 spread;
it must not imply that unlisted sweep rows were repeated.

The graph and offline router builders have separate scalar-work envelopes from
M3's exact-oracle visit cap: the retained 100k corpus uses 20B and the 250k
corpus uses 50B, a linear 2.5x scaling of the same per-source-row allowance.
Every M3 build command binds its corpus-specific
`-partition-max-distance-work` and `-router-max-scalar-work` alongside its
independent M3 visit cap. The router's default remains 20B and the explicit
offline ceiling is 50B.

## Gates and disposition

The executable M8 matrix gates exact exhaustive parity, failure honesty,
recall, probe reduction, matched-recall QPS, p95 tail, balance, overlap
storage, resource bounds, and coupled graph acceptance.  The north-star target
is recall@k >= .90 (stretch .95), median probes <= 25% of partitions at target
recall, QPS >= 1.15x exhaustive at matched recall, p95 no worse, and overlap
assets < 1.35x disjoint.  A failed, unsupported, or resource-bounded row is a
qualification failure or limitation, never a silent gate relaxation.

Apply these gates separately to each declared corpus and then state the
aggregate disposition. A clustered-distribution success does not erase a
uniform-data routing failure; it can support only an explicitly narrower
experimental or opt-in disposition.

The M8 request-work guard does not count exact source-query vector visits. One
250k x 1,000 child charges 500M visits for fixture checksum binding plus
canonical exact truth; its strict three-variant matrix therefore requires a
1.5B explicit `-m8-max-exact-truth-visits` bound. Record its calibrated
runtime/resource envelope before the full repeated campaign.

An external exact-truth cache is never self-authenticating. A cache hit requires
`-m8-truth-cache-sha256` to match the exact streamed cache artifact; the digest
must come from an independently trusted, source-computed run. In a strict
three-variant matrix with a new cache, the first child computes truth from the
authoritative source and the parent forces its reported artifact digest into
every later child. A pre-existing cache without that supplied digest fails
closed, as does a newly computed artifact that disagrees with a supplied
digest.
