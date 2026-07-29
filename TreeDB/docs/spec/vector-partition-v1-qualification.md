# Vector-partition V1 qualification

This is the local-evidence contract for issue #4015.  It supplements, and does
not change, the frozen V1 correctness contract.  In particular, canonical M8
exact truth and exact-union parity remain FP32 semantics; generated fixture
matrices are FP64 only as deterministic benchmark input.

## Dataset identities

`testdata/vector_partition_qualification_high_entropy_1m` is a committed,
procedurally generated 1,000,000-vector, 128-dimensional high-entropy cosine
corpus with 1,000 independently generated held-out queries.  Its generator
uses distinct corpus and query domains, so queries are not copied corpus rows.
It is intentionally uniform/unclustered after normalization: it is a
generalization and routing-stress corpus, not a semantic-cluster proxy.

`testdata/vector_partition_qualification_embedding_mixture_250k` is the
second distribution: 250,000 vectors and 1,000 held-out queries generated with
`treedb_vector_partition_embedding_mixture_v1`. It is an embedding-shaped
mixture of directional topics with continuous noise, explicitly not a claim
about a licensed external corpus. The smaller corpus makes the second,
diversifying distribution practical while the high-entropy corpus supplies the
required 1M qualification shape.

## Required artifact rows

Each retained result must bind: fixture manifest/checksum, base/head SHA,
hardware and topology, variant descriptor and asset digest, probes, ef-search,
concurrency, samples, timing boundary, profiles, and exact command.  A
comparable matrix includes graph-disjoint, graph-overlap-020, stable-ID-hash
disjoint, and exhaustive all-partition rows under the same topology and local
search settings.  Sweep probes `1,2,4,8,16`; include an ef sweep and
concurrency `1,16,64` when the host supports it.

Repeat the important target-recall and exhaustive comparison rows three times;
the report must identify repetitions rather than implying that every sweep row
was repeated.

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

The M8 request-work guard does not count exact source-query vector visits. The
required 1M x 1,000 shape performs both fixture checksum binding and canonical
exact truth, so one retained-asset run charges 2B visits outside that request
count. The strict three-variant matrix charges 6B. Any such execution requires
an explicit `-m8-max-exact-truth-visits` override and must record its calibrated
runtime before the full retained-asset sweep.
