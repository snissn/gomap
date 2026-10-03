# Bounded native query-under-write checkpoint

This standalone client supports one authenticated RF4 initialized data group, four resident servers (two per host), and either the existing 2D cosine bootstrap corpus or a qualified dataset corpus. It uses public VectorSearchStrictV1 and VectorInsertV1. It does not create stores, initialize or prepare a cluster, alter a graph, change membership, or supply substitute proofs.

Run it only after the existing fixed-peer initialize / clean restart / qualify sequence succeeds, with the retained qualify JSON and that driver's original initialization config. The config's public endpoint is loopback; run the driver on its configured host. Reader and writer have independently dialed connections. For the 2D fixture, the previous qualified corpus must be exactly the three seeds plus the qualified fresh-y row, with live revision 1. A dataset corpus instead requires the matching retained dataset identity/count/eligibility proof described below, also with live revision 1. Use fresh owned cluster roots for each campaign and never reuse a run ID after a mutation attempt. The old qualification receipt is an input binding, not a fresh route capability; current all-voter readiness is checked before workload. Bootstrap owner must equal both the configured sole data group and initialization source group before any networking.

Example after root-approved build and resource allocation:

    treedb-query-under-write \
      -config /inputs/node-c.json \
      -bootstrap-receipt /receipts/qualify.json \
      -run-id checkpoint01 \
      -timeout 120s -rpc-timeout 10s \
      > /receipts/query-under-write.jsonl

Root must bind the driver source/executable, daemon source/executables/images, shared/local configuration, bootstrap receipt, physical hosts, owned roots/labels and resource receipts. Each existing daemon retains its approved 2 CPU / 2 GiB / zero extra swap limits; use one bounded driver and existing admission/cleanup seams. This command does not inspect Docker limits or stop daemons. External owned teardown and container-event/peak receipts are necessary runtime evidence. FD data that cannot be inspected remains unavailable.

## Fixed population and output

The default 2D fixture run has 132 planned API attempts: two preflight strict anchor searches, 32 unique inserts concurrent with 64 strict anchor searches, one explicitly scheduled identical retry of the last insert, 32 post-write self searches and one final anchor search. That is 99 searches and 33 mutation attempts, only 32 new IDs. Including the existing bootstrap fresh-y ID, this fixture campaign has 33 fresh IDs and 36 documents, within the earlier64-ID qualification population. Preparation input admission remains distinct from the fresh-write campaign population.

Before network activity the command emits a "planned" JSON event containing generation, frozen requests/vector/document bytes, stable request hashes, input/executable SHA256s, timeouts and every initially unissued operation. Logical request hashes omit the later operation deadline. A "result" event retains the final report with each attempted operation's actual deadline, monotonic start/end nanoseconds, complete response, outcome and typed error code/message. Unissued operations remain in the same plan. JSON byte fields use the standard Go base64 encoding. Setup/dial/Hello/readiness are outside workload latency timestamps; readiness observations are retained separately with requested node, round, response and error classification. There are no percentile or QPS summaries.

The anchor [1,0] must return seed-x, cosine score 1 within the declared 1e-5 float32 tolerance, unchanged generation, positive selected partitions, native HNSW for every selected partition, zero exact scans and a positive read proof. New vectors are frozen float32 directions strictly inside the negative-x/positive-y quadrant. Their self-query winner margins are checked against the known corpus before any work. Post-write self searches are separate ANN quality evidence. Exact-ID visibility comes from the validated mutation response, not appearance in ANN top-K.

Every fresh insert is checked immediately, before the next insert: complete shared public response proof, frozen owner, no split token, next live revision and increasing commit index. Explicit retry changes only its deadline and must preserve the original exact-ID/owner/partition/live revision while proving a later commit, with no split visibility token. A split-shaped retry response may pass the shared general validator but remains UNKNOWN in this single-owner checkpoint; post-write searches stay unissued. Its original response is retained. Highest acknowledged commit includes the planned explicit retry. Final acceptance also requires all four actual voters active, ready, nondraining, and locally applied through that prefix. Read-only final readiness observations have at most 64 rounds (50ms between rounds) and remain bounded by the overall and per-call deadline. They never replace workload search attempts.

## Failure and overlap semantics

After both concurrent streams complete successfully, the driver closes the
original writer and dials/handshakes one fresh writer before the scheduled
identical retry. After that retry succeeds, it closes the original reader and
prepares one fresh reader before post-write searches. A finished stream may
otherwise leave its socket idle beyond the server's configured timeout while
waiting for the other stream. Each connection preparation fits the remaining
overall deadline and per-call budget, remains outside API operation timestamps,
and does not issue a mutation. Preparation or close failure stops the checkpoint
with the next operations still unissued. No failed operation is redialed or
retried; close errors are retained in the failed result.

Any failed operation cancels the workload context and joins both workers before reporting. No new writes continue after failure; no automatic retry occurs. A commit-ambiguous or unclassified mutation error is UNKNOWN. Malformed insert responses share invalid_request classification with some local refusals in the existing native API; the runner conservatively retains all mutation invalid_request outcomes as UNKNOWN rather than assert noncommit. Its original typed code remains recorded. Invalid successful receipt/sequence is also unknown. Unknown IDs are neither counted as absent nor used to construct a supposedly complete corpus. Failed/canceled/unknown/unissued counts reconcile with every planned attempt.

Successful independent client calls are timed by one driver's monotonic clock. Acceptance requires a successful concurrent strict query to complete while an insert API call remains outstanding; every intersecting pair and whether the query finished before the insert are retained. If all operations succeed but that witness is absent, verdict is INCONCLUSIVE_OVERLAP and exit is nonzero. No samples are replaced.

The exported client API exposes no command-dispatch event. Therefore the report explicitly labels this as independent client API-call overlap; exact socket-dispatch overlap is unproved. This does not prove simultaneous server graph-mutation critical sections: an insert may commit before its reply is observed, and safe read/publication serialization is allowed. A stronger requirement needs separate authorized tracing, not a hidden socket observer or production hook.

Successful report verdict is ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP. It is not stable tail latency, saturation, sustained capacity/QPS, comparative speedup, host-loss tolerance, or support for batches/upsert/replace/delete/optimize/cross-group transactions. No such operation is accepted by this command. RF4 quorum three cannot survive loss of either two-voter host.

Focused validation for root:

    GOWORK=off go test ./cmd/treedb-query-under-write -count=1
    GOWORK=off go test -race ./cmd/treedb-query-under-write -count=1

The deterministic client fakes exercise operation populations, exact retry identity, invalid fresh sequence stopping before another insert, ambiguity/full accounting, cancellation, overlap boundaries and refusal of native/exact-fallback oracle mismatches. They are not actual distributed runtime evidence. A subsequent bounded two-host packet must use the integrated predecessor stack and all actual public API/daemon receipts. Construction on the provisional #4944/#4945 snapshot does not make this branch mergeable.


## Dataset checkpoint

A successful dataset qualification receipt may bind a non-2D corpus from #4956.
Use its same configuration and retained dataset identity/count/eligibility proof.
`-fresh-inserts 65` plans 65 distinct ordinary public inserts, one explicit retry,
and 132 strict native searches (198 total attempts), in the configured dimensions
and EfSearch. Default flags still plan the original132 operations. The dataset
must have passed the first-two-coordinate norm fraction<=0.9 eligibility before
loading; anchors/new probes use that plane, with zeros in remaining coordinates.
Raw planned/result responses, ambiguity accounting, final all-voter prefix checks,
client-overlap semantics and no automatic retry remain unchanged. This is a
functional population probe, not sustained performance or general mutation proof.


## Explicit probe deadlines and retained corpus failure (#4958)

The defaults remain `-timeout 120s -rpc-timeout 10s`, with the default 132-operation
population unchanged. Explicit total deadlines admit 1s..600s; explicit per-call
deadlines admit 1ms..60s and must fit the total. Invalid bounds refuse before
input loading, planning or network activity. Both admitted budgets and each
attempt's actual deadline remain in the raw report. These are harness limits;
they do not change runtime request limits or imply a latency guarantee.

For the optional unchanged 10,000-row/128D corpus plus three anchors, retained
trial08's unpaced 65-new-ID probe failed: 198 planned, 10 attempted, 8 succeeded,
one search deadline failure, one UNKNOWN canceled mutation, and 188 unissued.
Four acknowledged ordinary writes took 3.8..3.91s each; at that rate 65 serial
writes would exceed 250s. The preceding successful search spent about 7.18s in
the service adapter and about 8ms in the coordinator. A later search exhausted
its 10s RPC deadline. Larger explicit budgets permit a bounded followup but do
not themselves fix admission contention. The failed packet and ambiguous
outcome remain retained; there is no automatic retry or paced reinterpretation.

The reader-intent admission regression and fix belong to #4958. Representative
unpaced >64-write reconciliation, write-phase cost attribution and resource
qualification remain pending fresh frozen collection. A sequential/alternating
65-write control would prove only the growth behavior it actually exercises.
#4250 still owns sustained throughput, p99, representative recall and resource
claims; RF4 on two hosts with two voters each cannot survive either host loss.

## Quiescent representative recall (#4959 first checkpoint)

The optional `-mode quiescent-recall` is read-only. It admits the unchanged
10,000-row/128D export with sixteen canonical queries and topK10, then makes
exactly sixteen serial public `VectorSearchStrictV1` calls. Default workload
flags and populations remain unchanged. Supported phases are `pre` (after
successful dataset qualification, before any probe writes) and `post-only`
(after a complete successful probe). This mode does not compare paired packets,
issue mutations, retry searches, compute concurrent recall, or qualify sustained
latency/QPS/p99. Low recall is an observed quality value, not an invented gate.

    treedb-query-under-write -mode quiescent-recall \
      -config /inputs/node-c.json -bootstrap-receipt /receipts/qualify.json \
      -dataset /inputs/dataset -provenance /receipts/recall-provenance.json \
      -phase post-only -probe-receipt /receipts/probe.jsonl \
      -run-id recall01 -timeout 120s -rpc-timeout 10s

For `pre`, omit `-probe-receipt`. Explicit `-fresh-inserts` refuses in this
mode. Configured EfSearch must be at least10. Queries retain configured EfSearch,
probes1, generation_snapshot, request/response1MiB, candidates8MiB and merge32
limits; setup and all-voter readiness stay outside per-call intervals.

The manifest, every file length/SHA, finite normalized FP32 bytes, IDs and
sixteen imported truth rows are frozen before networking. Input caps are:
config8MiB, qualifier4MiB, manifest/provenance/truth64KiB each, probe16MiB,
aggregate32MiB. Only the exact 10k/128D/16/top10 schema is supported. The oracle
uses `collections.VectorPartitionCanonicalScoreContractV1` and its exported
scorer: FP32 normalization, binary64 dot accumulation, FP32 score rounding,
score-descending/stable-ID-ascending ordering. Corpus-only exact IDs must match
the unchanged export. Augmented truth adds the three seed anchors, qualification
fresh-y and every proven unique ordinary probe insert; exact retry adds no row.

Root supplies strict JSON provenance with these case-sensitive exported keys:

    {
      "Version": 1, "Phase": "post-only",
      "CampaignID": "rf4trial10", "BootstrapRequestID": "rf4trial10",
      "ConfigSHA256": "<raw config SHA256>",
      "BootstrapSHA256": "<raw qualify JSON SHA256>",
      "ManifestSHA256": "<raw manifest SHA256>",
      "RuntimeSourceHead": "<40 lowercase hex>",
      "ServerBinarySHA256": "<64 lowercase hex>",
      "QualificationSourceSHA256": "750dde6bc3866287e3201e62be9b88dc6f5e0d7b2a947d16da0e5efba074e769",
      "InitializationReceiptSHA256": "<complete successful initialize receipt>",
      "CleanReopenReceiptSHA256": "<successful all-voter clean-reopen receipt>",
      "ProbeSHA256": "<raw successful planned/result JSONL SHA256>",
      "ProbeBinarySHA256": "<that probe executable SHA256>",
      "ProbeRunID": "<that mutation run ID>",
      "Roots": {"node-a":"/owned/a","node-b":"/owned/b","node-c":"/owned/c","node-d":"/owned/d"},
      "Hosts": {"node-a":"host1","node-b":"host1","node-c":"host2","node-d":"host2"},
      "RootAccepted": true, "InitializationSucceeded": true,
      "CleanReopenSucceeded": true, "ExclusiveWriterStopped": true
    }

Pre provenance has Phase=pre and empty/omitted ProbeSHA256, ProbeBinarySHA256,
ProbeRunID. Root must inspect and accept the referenced complete create/seed/
chunk/prepare/clean-reopen receipts, exact source/ELF/config/owned roots and writer
inventory before authoring this attestation. References and booleans are operator
evidence bindings, not independent cryptographic proof or serving capabilities.
The initialization source tuple/completion and actual qualifier dataset identity
are cross-checked. Qualification responses omit original request/vector bytes,
so this slice reconstructs only the explicitly pinned qualifier source algorithm
and exact namespace `<BootstrapRequestID>/dataset-<ManifestSHA256>-fresh-y`,
with vector [0,1,0,...]. Changed qualifier source contracts require a reviewed
harness update; arbitrary missing insert-vector provenance refuses.

Post-only requires exactly the existing successful planned/result probe events,
matching frozen config/bootstrap/executable/run, every supported planned request,
logical hashes excluding operation deadlines, all succeeded counts, valid
single-owner next-revision/increasing-commit receipts, byte-identical logical
retry, overlap accounting and final all-voter ACTIVE/applied-prefix observations.
Any UNKNOWN, failed, canceled, unissued, missing, extra or conflicting write
population refuses. The probe executable identity is separate from this new
recall executable identity. The actual document embedding must match its public
insert vector. There is no prefix-only acceptance of a failed probe.

Root stops/joins the prior driver and excludes every other writer for the whole
recall observation. Current authenticated all-four ACTIVE readiness is captured
before and after through the highest acknowledged commit (including retry).
Those observations prove prefix readiness; they do not prove writer exclusion
or attach an applied-index/live-revision watermark to an individual search.
The public response has no such watermark. Quiescence is operationally enforced
and retained in root's campaign evidence. Running trial10 cannot produce a
retrospective pre receipt; its successful ledger may support post-only evidence.

Distinct `fixed_cluster_quiescent_recall_v1` planned/result events retain source/
input/runtime/provenance identities, deterministic live-population digest/count,
corpus and augmented scored truth, full bounded responses/counters/timings,
request hashes/deadlines, monotonic call intervals, per-query recall and counts.
Generation, exactly10 distinct known IDs, canonical scores/order, native HNSW
for every selected partition, zero exact scans and positive read proofs are
correctness requirements. Returning an approximate set with recall<1 is allowed
and recorded. No exact service fallback exists; exact oracle work is client-side
setup. Failures stop at the first call, retain its response when bounded, and
leave subsequent queries unissued. Mean recall is supplied only for sixteen
successful calls. Each event contains at most 512 KiB of JSON plus one newline
(pair <= 1 MiB + 2 bytes); each retained
response at32KiB and error summaries at2KiB plus a full-error SHA256.

Root validation: existing driver normal/race checks plus tests named
TestRecall*. Source-only construction does not establish runtime qualification.
The full #4959 sustained windows and #4250 serving/capacity gates remain open.
