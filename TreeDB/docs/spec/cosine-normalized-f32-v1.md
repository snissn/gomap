# Canonical normalized FP32 cosine representation v1 (#4722)

Status: normative target contract for the opt-in
`cosine_normalized_f32_v1` representation. Q1 freezes the contract and the
engine gates below; the representation is unavailable until Q2 implements it.
Legacy and omitted representations keep their existing behavior.

## Declaration and admission

`cosine_normalized_f32_v1` is valid only for a `column_graph` vector index with
`metric="cosine"` and exactly one non-null typed-column-part field: the selected
float32 vector. Sibling row-asset fields and non-column retained JSON remain
allowed. Other column-part layouts are rejected at create or reset. The
representation is selected when the index is created. It is not inferred from
data, enabled by a query flag, or applied to a populated legacy index in place.
TreeDB is pre-alpha, so rebuilding the database is the migration when the format
changes.

The canonical asset replaces the selected typed vector column's FP32 payload;
it is not an auxiliary graph cache. Generic typed-column and document readers
resolve the column through an explicit durable row-to-graph-ordinal mapping.
Version 1 rejects a second vector index over the same typed vector column, an
incompatible metric or representation, and in-place conversion of a populated
legacy column.

Admission MUST reject a zero-norm vector, a non-finite component, or a dimension
mismatch before encoding a durable command. It makes a caller-independent copy,
computes the sum of squared float32 inputs from left to right in a float64
accumulator, computes the reciprocal square root in float64, and stores each
component as the float32-rounded product. It MUST NOT mutate caller memory.
Query normalization uses the same arithmetic once per request. Replay, reopen,
fold, rebuild, and suffix-to-base publication preserve the canonical bytes and
MUST NOT renormalize them.

A metadata-only or document-only update preserves the existing canonical
embedding bytes without renormalization. It may copy those identical bytes into
the bounded mutable suffix and therefore may replace the physical row mapping.
Supplying an embedding is a new vector admission and follows the rules above.

## Score and ordering

The public score is the packed float32 dot product of the normalized query and
stored row, clamped to `[-1, 1]` after the dot product. Scalar and accelerated
packed-dot implementations MUST satisfy the same numeric and ordered-result
fixtures; version 1 does not promise bitwise identity across SIMD reductions.
Ranking is descending finite float32 score, then ascending document-ID bytes for
equal scores. No epsilon tie bucket is permitted. Base and mutable-suffix rows
use the same score and ordering contract.

Conformance accepts bitwise-equal scores, absolute error no greater than
`2e-6 + 2e-6 * abs(reference)`, or at most 32 ordered-float32 ULPs. NaN and
infinity never match. This numeric tolerance cannot excuse a changed result
order: hostile near-tie fixtures MUST return identical ordered IDs.

This score supersedes the legacy close-angle stable cosine / FP64 inverse-norm
requirement only for `cosine_normalized_f32_v1`. The legacy per-candidate scorer
is not a fallback for this representation.

## One permanent FP32 plane

There is exactly one permanent O(rows x dimensions) normalized FP32 serving
asset per live base generation. It is aligned, row-major, and in graph-ordinal
order. A declared physical stride MAY exceed the dimensions; padding is
zero-filled and included in storage/RSS accounting. The asset is authoritative
for exact traversal, packed rerank, fold/rebuild input, generic embedding reads,
and explicit embedding return. HNSW topology references its durable identity
and MUST NOT contain another complete FP32 corpus.

Pinned superseded generations and a bounded mutable suffix are lifecycle state,
not a second representation, but inventory reports them separately. A full-size
heap preparation, search-pack vector section, or raw-vector shadow makes a run
ineligible.

Named scalar-u8 codes are additive derived state. They generate candidate
ordinals. After visibility, filtering, and shadow handling, retained base
candidates are scored exactly once by indexed/batched packed FP32 scoring in
one call or bounded chunks of that kernel. A token packed call followed by
scalar or stable rescoring is non-conforming.

## Result projection

`return_embedding` defaults to false. False permits FP32 reads for scoring but
causes zero additional output-materialization vector reads, embedding encodes,
or embedding response bytes. True returns the stored canonical normalized
vector. The caller's original magnitude and bit pattern are not retained;
applications that need them must store a separate field.

IDs, scores, document fetches, and optional embedding fetches remain bound to
one captured read owner. Projection cannot reopen a newer owner.

## Native capability boundary

Native command64/v4 identifies this score contract. Its production envelope
validates framing, bounds, negotiated representation, owner/generation, and
result shape without constructing or serializing a full score-plane proof.
Proof is explicit diagnostic work. Production clients do not reconstruct
vectors or re-score results. Older command versions fail closed for this
representation rather than translating it to legacy semantics.

Q1 records Go/Python v4 as `NOT_IMPLEMENTED`. Q2 keeps those stages unavailable
while implementing storage and collection/service behavior. Q3 owns the native
wire and Python seam. An unsupported stage is never filled with a legacy
measurement.

## Phase gates and qualification ownership

The retained Q1 diagnostic baseline is summarized in
[`minima-sq8-q1-baseline.md`](../benchmarks/minima-sq8-q1-baseline.md). It is
engineering evidence, not a qualifying run.

Q2 MUST preserve these engine guardrails at E=R=64 on the same-shortlist seam:

- packed rerank: at most 10 us, at least 100,000 QPS, and at least 80% of the
  historical 144,421 QPS receipt;
- SQ8 candidate generation: no more than 20% slower than 83.4 us;
- public collection SQ8: at least 10% higher median QPS and lower p50 than
  same-build exact, with p95 no worse than exact;
- same-build exact collection p50 no more than 5% above the retained 305.303 us
  reference and p95 no more than 10% above the retained 367.344 us reference;
- normalized-dot and original-cosine recall@10 at least 0.90, with SQ8 no more
  than 0.01 absolute below same-build FP32;
- positive scalar-u8 and packed counters, zero stable-scorer fallback, and exact
  packed accounting for all retained eligible base candidates;
- one canonical FP32 inventory and scalar-u8 total-owned-storage increment no
  greater than 10% over the same representation without scalar-u8 codes.
- mutation, checkpoint, fold, reopen, GC, suffix/base merge, and same-owner
  materialization preserve the contract and fail closed on identity mismatch.

Q4 owns the next fresh 500,000 x 768, 200-query integrated qualification after
Q2 and Q3 are complete. It compares exact and SQ8 on the same owner, projection,
query order, construction, runtime, and document shape; includes collection,
service, Go native, and Python native; and fails closed on duplicate FP32 planes,
fallback scoring, proof or output-embedding work, identity mismatch, or post-hoc
tuning. Q1 does not add or run a second full-matrix framework.
