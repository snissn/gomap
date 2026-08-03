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
search settings. For #4027 every repeat records the same p1/p2/p4/p8/p16
ladder at `ef_search=128`, concurrency `1`, and approximate router candidate
budget `256`; the retained matrix continues to bind the exhaustive control.

Repeat the full probe ladder, including the p2/p16 comparison, three times per
structured corpus. The v2 campaign index contains exactly one retained 100k
campaign and one retained 250k campaign; each hashes every matrix and records
`publication_completed_at` only after its foreground matrix child exits, with
that timestamp strictly after the matrix's retained execution completion. It
requires frozen base `03e7a26e56100964f14f603f0248a1a6ccc50a68`, one exact head,
and corpus/truth/variant identity across its repeats,
and derives median/min/max QPS plus p95 spread. Qualification is reported only
after both corpus campaigns validate.

Each child also retains a runner-written, hashed measurement transcript (schema
v5) bound to its execution ID, immutable candidate/config identity, measured
rows, and each normal row's ordered per-query returned top-k document IDs,
parallel float32 score-bit arrays, and request-total nanoseconds. Qualification
bounded-decodes the frozen exact-truth cache, recomputes retained row recall,
p50/p95/p99, and maximum request time from those samples before trusting recall
or tail-latency gates. It requires elapsed wall time to be at least the
overflow-checked sum of retained request totals divided by configured
concurrency, and replays offline membership-oracle/routing attribution from
retained M3 assets and frozen truth before attribution gates. That replay
compares its local IDs and float32 score bits directly with the transcript,
deriving coordinator parity rather than trusting report flags. The bounded
transcript reader admits at most 2 MiB, with a frozen 5x1000x10 serialization
test guarding the retained shape. It also retains the positive raw process
peak-RSS observation and recomputes the report peak before resource gates. This
detects retained-bundle mismatch, reuse,
or a relabeled aggregate; it does not claim to authenticate an attacker that
can regenerate an entire evidence bundle.

The graph and offline router builders have separate scalar-work envelopes from
M3's exact-oracle visit cap: the retained 100k corpus uses 20B and the 250k
corpus uses 50B, a linear 2.5x scaling of the same per-source-row allowance.
Their persisted M3 descriptors also bind the corresponding visit caps: 400M
for 100k and 900M for 250k, plus the canonical benchmark executable SHA-256
used to construct the retained candidate. Qualification requires that digest to
match every child/matrix report and both corpus campaigns.
Every M3 build command binds its corpus-specific
`-partition-max-distance-work` and `-router-max-scalar-work` alongside its
independent M3 visit cap. The router's default remains 20B and the explicit
offline ceiling is 50B. The immutable M3 descriptor records both parsed build
caps and the complete parsed partition-builder configuration, so campaign
validation rejects a retained candidate built above its corpus-specific envelope
even when its artifact bytes otherwise match.

For graph-disjoint and graph-overlap builds, the same schema-v5 descriptor also
binds the selected KaHIP Python executable SHA-256 and the pinned adapter
SHA-256. Qualification requires Python
`7d51cd6b48b521277f5caa4610a82126e315fa2be4df069823a8b1eeb5bd4a86` and
adapter `ae4ca8f5f26bd510a507a0f4ba50adaf1e5514ee9e20340cb9d494aba8f54825`;
the stable-ID-hash baseline carries neither external-execution identity.

Each retained M3 and M8 command also explicitly sets `-max-vectors` to the
authoritative corpus size (100k or 250k) and `-max-fixture-bytes` to 4 GiB.
Qualification re-applies the runner's fixture and pre-measurement work
admission checks to those recorded values, rejecting omitted defaults as well
as tighter or looser substitutions. The same per-corpus vector cap is parsed
into the expected M3 partition and router configurations, including dependent
edge limits, before retained descriptor comparison.

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
must come from an independently trusted, source-computed run. The frozen 100k
anchor is artifact `0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c`,
truth `6e17b00a04ad86ad4b13507e6afc1ae38d323280b3a0aa8405bce88b222fa1bc`, and
identity `accdb76c693e2da99333b9327efc0e3d83ba630b25a8ba2b6820f5a6f6e38937`;
the 250k anchor is artifact `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e`,
truth `89b84125e518f33cc30bc1e4e9defcc0639378d7108fb180f56ec2dc91d6f254`, and
identity `f1fab20b88cd3dcdd6e95a284400983230b1432b36bd4d73e321e251159795ab`.
Qualification bounded-decodes these corpus-pinned caches before accepting
report-supplied corroboration. In a strict
three-variant matrix with a new cache, the first child computes truth from the
authoritative source and the parent forces its reported artifact digest into
every later child. A pre-existing cache without that supplied digest fails
closed, as does a newly computed artifact that disagrees with a supplied
digest.

`generate-truth-cache` is the bounded source-only producer for a frozen cache:
it regenerates the deterministic fixture from its manifest, writes the
canonical cache format, and emits its artifact and semantic truth SHA-256s.
It requires explicit `-max-vectors`, `-max-fixture-bytes`, and
`-max-exact-truth-visits` values before it allocates source rows or scores a
query.

Before a qualification run, copy each authoritative fixture manifest and any
reused truth-cache artifact into that corpus's campaign directory. The retained
validator rejects external or linked dataset, truth-cache, M3, report, profile,
and transcript inputs so the final campaign index remains a self-contained
replay bundle.

Before any M3 or M8 work, the campaign makes an ordinary detached local clone
at `<campaign-root>/source` from the explicit repository source and exact head.
It stages the committed 250k fixture and generates its pinned truth cache from
`<campaign-root>/source/testdata/vector_partition_qualification_embedding_mixture_250k`
only after that source stage, so those data paths do not inherit the caller cwd.
The clone must retain a `.git` directory (a linked-worktree `.git` file is not
enough for Go's VCS stamp discovery) and no `objects/info/alternates` link to
an external object store. It builds one clean-head
`<campaign-root>/bin/treedb_vector_partition_bench` binary from that source,
creating the retained `bin` directory first, with
`GOWORK=off go -C <campaign-root>/source build -buildvcs=true`, records
its SHA-256, and verifies `go version -m` contains the exact head revision and
`vcs.modified=false`. Every retained M3, M8, and final validation command
invokes that same canonical path. The graph M3 commands likewise use the
cloned adapter at `<campaign-root>/source/scripts/treedb_kahip_partition.py`.
Child and matrix evidence bind the byte digest as well as Go build metadata
(benchmark main package, recorded head revision, and an unmodified build); the
aggregate requires that one digest for both corpora and all repeats. Ephemeral
`go run` executables are not final replay evidence.
Every retained M3 and M8 command also carries exactly one explicit canonical
`-base-sha`/`-head-sha` pair and `-source-checkout` matching its evidence. The
checkout must be the clean Git toplevel at that head, rather than inheriting
provenance from the replay process cwd or CI environment.

`-max-vectors` remains the source-fixture admission cap. Each frozen M3 command
also carries `-router-max-vectors`: 120000 for 100k and 300000 for 250k, which
reserves the full 0.20-overlap final-membership shape for all three variants.
