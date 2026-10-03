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
