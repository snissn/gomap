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

Successful report verdict is ACCEPT_BOUNDED_FUNCTIONAL_CLIENT_OVERLAP. It is not stable tail latency, saturation, sustained capacity/QPS, comparative speedup, host-loss tolerance, or support for batches/upsert/replace/delete/optimize/cross-group transactions. No such operation is accepted by the default query-under-write mode. RF4 quorum three cannot survive loss of either two-voter host.

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

The reader-intent admission regression and fix belong to #4958. Fresh trial10
completed the unchanged 198-operation plan: 65 distinct ordinary inserts, one
identical retry and 132 native searches, with verified client-call overlap and
all four voters applied through commit 154. All attempts succeeded; all four
servers stopped cleanly without OOM and their stores remain preserved. Its
independently reviewed sealed archive is
`91333f496872113c8b1942c173a43218810eaf0322921677e6cd53086814aa17`.
The driver renews idle connections before the planned retry and post-write
phases; failed mutations never retry. This proves bounded ordinary growth and
native overlap. Write-phase cost attribution and broader resource qualification
remain pending; no sustained performance or whole-lifetime peak is claimed.
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

The overall recall timeout starts at mode entry, before input/executable reads,
JSON admission, FP32 conversion, oracle construction, planned output, readiness
and native searches. One context is shared across those stages; synchronous read
checks and per-row/operation/query oracle checks stop subsequent work after
cancellation. No goroutine is created to race an I/O call against a timer.
An already blocking OS open/read/write/close, bounded buffered JSON decode,
hash or sort must return before the next cancellation check; the flag is not a
hard process wall-time/preemption guarantee. Root retains an outer process
wall-time limit. Failure-result output and cleanup remain best-effort after
cancellation so partial evidence can be retained, and can themselves block on
the output/filesystem; root must preserve an outer-timeout failure packet.

Root validation: existing driver normal/race checks plus tests named
TestRecall*. Source-only construction does not establish runtime qualification.
The full #4959 sustained windows and #4250 serving/capacity gates remain open.


## Representative read-only window (source checkpoint)

`-mode read-window` reuses the exact quiescent-recall input admission,
provenance, bootstrap/probe population, and canonical oracle above. `-phase pre`
or `post-only` carries the same meaning and requires the same root-attested
writer exclusion for the entire observation. This checkpoint issues no writes;
concurrent paced writes remain OPEN under #4959. The existing default workload
and sixteen-call quiescent-recall mode remain available.

    treedb-query-under-write -mode read-window \
      -config /inputs/node-c.json -bootstrap-receipt /receipts/qualify.json \
      -dataset /inputs/dataset -provenance /receipts/recall-provenance.json \
      -phase post-only -probe-receipt /receipts/probe.jsonl \
      -run-id readwindow01 -read-concurrency 4 \
      -timeout 120s -rpc-timeout 10s > /receipts/read-window.jsonl

`-read-concurrency` is mandatory and accepts only 1 or 4. Each worker owns one
independently dialed persistent strict native client across warmup and measurement.
Default warmup is 64 requests total, assigned by ordinal=worker+n*concurrency;
with four workers each receives 16. Warmup must finish successfully before the
measured clock starts. `-read-warmup` accepts 0..1024. Every phase cycles query
ordinal%16 through the frozen TopK=10/probes=1/configured-EfSearch requests. No
socket is shared across workers, no search is retried, and no fallback occurs.

Default admission window is 60s; `-read-window` accepts 1s..60s for bounded
checks. The closed loop admits another call after each worker returns, validates,
and retains its previous response. At normal cutoff, admission stops and calls
already issued drain under their remaining per-call and overall timeout. The
actual duration includes that drain. A genuine call/validation failure cancels
all workers, joins them, and retains every returned attempt, including sibling
cancellations. Setup, input reads, oracle scoring, executable hashing, all-voter
readiness, connection Hello and warmup are outside measured latency/QPS, and
inside the total mode-entry timeout. Root must still impose an outer process
wall timeout for blocking output/OS I/O and preserve an outer-timeout packet.

`fixed_cluster_read_window_v1` planned/result events include the admitted
sixteen request hashes, vectors, corpus/augmented truth and population identity
once in `Admission.Queries`. Per-attempt records refer to those immutable query
IDs/hashes and retain actual deadline, worker, ordinal, phase, monotonic call
start/return nanoseconds, outcome/error, recall and a private bounded response
copy with generation/neighbors/native counters/server stages. Query scorer
normalization is prepared once and read-only across workers; validation still
checks canonical score bits for every returned neighbor. Client API latency
excludes client validation/retention; loop QPS includes those costs and drain.
This is an observed closed-loop rate at stated concurrency, not saturation or
capacity. Server stage totals are nested/per-shard sums as defined by the public
response; they must not be added into a purported exclusive latency breakdown.

Measured and warmup counts are separate. `Planned` describes the finite maximum
possible attempts, `Unissued` includes budget slots unused at normal cutoff,
and `Completions` counts every issued API return regardless of outcome.
Attempted=succeeded+failed+canceled+unknown; Planned=Attempted+Unissued.
The report retains per-query measured coverage, actual elapsed duration,
attempt/success QPS, nearest-rank p50/p95/p99 nanoseconds for successful calls
and non-success calls separately, and mean recall over measured successes when
untruncated. Empty latency populations have Samples=0. Warmup and oracle are
excluded from these summaries; low recall is observed, not an invented gate.

Default measured cap is 65536 attempts (`-read-max-attempts` accepts 1..65536).
Default encoded pair budget is 128 MiB (`-read-output-bytes` accepts 1 MiB..256 MiB,
including both event newlines); each response is bounded at 32 KiB and errors
reuse the 2 KiB/full-hash summary. Retention charges actual encoded attempt bytes
and reserves space for readiness/errors and at most four response-free terminal
records. On aggregate byte exhaustion the terminal record retains response
byte length/SHA but omits its response, marks Truncated and stops the workers.
Both final events are checked against the actual encoded pair budget. Hitting
an attempt/byte cap before normal cutoff refuses sustained-window acceptance.
A successful verdict also requires successful measured coverage of all sixteen
query IDs, zero measured failures/cancellations/unknowns, complete returned
attempt accounting, and authenticated all-four ACTIVE readiness before/after
through the same pinned highest acknowledged prefix. Prefix readiness does not
prove writer exclusion or individual search applied-index/live-revision.

No additional diagnostics endpoint or resource sampler is introduced. Root
collects existing authenticated diagnostics and external voter/client cgroup
CPU/RSS/swap/FD/I/O/network/event evidence separately, bound to the admitted
runtime/config/source/ELF and the measured interval. Client socket ownership,
request response copies, JSON marshaling, canonical neighbor validation and the
short admission/retention lock are harness costs; retention can consume the
stated byte budget plus Go object/slice overhead, transient JSON buffers and
final serialization buffers. Limits and elapsed time do not prove whole-lifetime
peaks. Preserve raw failed/capped packets; never turn a cap into a capacity claim.

Root validation: normal/race checks of this package, especially `TestWindow*`
and unchanged `TestRecall*`. Source-only tests do not establish runtime window
qualification. This checkpoint leaves concurrent writes and the broader #4250
serving/capacity gates OPEN.

### Optional resource boundary handshake

`-read-resource-gate-dir /run-resource-gate` enables a trusted fresh run-local
read/write directory mounted into the driver. The default empty flag preserves
the existing flow and emits no gate files. Root must precreate an empty regular
directory, retain it after every outcome, and never reuse it. A symlink directory,
nonempty directory, unexpected entry, changed receipt, malformed/stale token or
wrong run/phase/nonce fails closed. The driver creates exclusive `claim.json`,
then `ready.json` after successful warmup and before measured origin. It waits
for `ready.ack`; after all voters and the client are sampled, the collector must
atomically rename an exact byte copy of `ready.json` to `ready.ack`. Place any
collector temporary file outside the gate directory. A partially published
acknowledgment is invalid, rather than a signal to retry a workload.

Measurement starts only after ready acknowledgment. After measured admission
and in-flight drain fix ActualDurationNS/QPS, the driver publishes `done.json`
and waits for an exact-byte `done.ack` while its native clients and process stay
alive. Root samples the same client/voter cgroups, retains the raw counters and
sample times, then acknowledges done. Normal after-readiness/result/close follow.
A measured failure also reaches done when the overall context still permits it;
a total timeout/cancellation refuses the wait and retains ordinary failure output.
Missing acknowledgments use the existing total timeout; there is no independent
extension. Both waits, receipt I/O and polling are outside measured QPS and call
latency. Polling is only acknowledgment discovery, never a resource guarantee.

Each regular token is bounded to2048 bytes. Receipts bind Version1, RunID, phase,
a fresh128-bit nonce, publish UTC, measured origin/duration and stop reason;
acknowledgments copy those bytes exactly, including newline. The report retains
small ResourceBoundaries with acknowledgment UTC and wait durations; it never
writes another full result to the gate. Directory inventory is bounded to six
entries (five allowed). Root retains all tokens and raw resource samples. The
boundary counter interval encloses actual measurement plus release/publication
and sampling overhead; it is not an exact measured-only resource delta or a
whole-lifetime resource peak. Wall-clock alignment and external collection remain
root's responsibility. This trusted local filesystem protocol is not hardened
against hostile path replacement or a blocking filesystem; existing outer
process timeout remains required. No server/public API or endpoint is changed.


## Paced ordinary writes with representative reads (provisional source checkpoint)

`-mode paced-window` is a separate admitted observation. Existing default,
quiescent-recall and read-window behavior stays available. This first paced mode
requires a complete accepted post65 input chain: population10069, live revision66
and the highest acknowledged prefix recomputed from that campaign's validated
write ledger. Raft entries may separate acknowledgments by more than one index.
Root must exclude other
writers before setup/pre-recall/warmup and after this mode's writer drains.
During measurement this mode alone owns the serial ordinary writer. Existing
provenance binds the original server source/ELF independently of this driver's
source/ELF; building the driver does not relabel the accepted server runtime.

    treedb-query-under-write -mode paced-window \
      -config /inputs/node-c.json -bootstrap-receipt /receipts/qualify.json \
      -dataset /inputs/dataset -provenance /receipts/recall-provenance.json \
      -phase post-only -probe-receipt /receipts/probe.jsonl \
      -run-id unique-paced01 -read-concurrency 1 \
      -paced-inserts 6 -paced-interval 5s \
      -read-resource-gate-dir /fresh-resource-gate \
      -timeout 120s -rpc-timeout 10s > /receipts/unique-paced01.jsonl

The unchanged sixteen topK10/probes1/configured-EfSearch queries run after64
warmup calls, at explicit reader concurrency1 or4, for60s measured admission.
The existing read attempt/output bounds apply; defaults remain65536 and128MiB.
This mode requires warmup64 and window60s; shorter private phase tests do not
qualify the actual checkpoint. Each reader owns its persistent native socket across warmup and measurement.
Setup connections are deliberately renewed after serial pre-recall and before
warmup so an unused worker socket cannot inherit a long idle pre-phase.
One additional persistent native socket belongs solely to the writer. There is
no client mutation/search retry, catch-up burst, exact fallback, or serving/API
change. `-fresh-inserts` refuses here; `-paced-*` flags refuse in other modes.

`-paced-inserts` admits1..10 candidates (default6); `-paced-interval` admits
1s..60s (default5s). All candidate IDs must be absent from the complete frozen
baseline and from earlier candidates. The existing angular candidate generator
is reused, but its anchor-only guard is insufficient here: the full possible
population must prove each declared post-ack visibility query's canonical winner.
Six candidates use a different direction denominator from trial11's65 candidates;
that observation does not itself establish visibility or recall invariance.
Exact ties use the same FP32 score bits and ID order as the existing oracle.
Any failed candidate/visibility/invariance admission refuses before networking.

The exact full-baseline Top10 for every query must remain unchanged at every
possible planned serial insert prefix. For insertion only, merging each
canonically scored new candidate into the previously proven full-baseline exact
Top10 is equivalent to rescoring all rows; ordered IDs and score bits must match.
The report retains baseline population identity, all candidate requests/hashes,
per-candidate sixteen scores and each prefix's Top10 digest. This is an explicit
workload-specific invariant proof, not a search response watermark. General
changing-population concurrent recall remains unqualified. This mode fails closed
when the invariant cannot be proved, rather than issuing writes without that
qualification or reporting invented baseline recall.

A frozen baseline-plus-all-planned-vector map permits canonical validation of
approximate native neighbors. Its membership is not visibility evidence. Every
returned planned ID must additionally have an insert invocation interval start
at or before the search API completion, on the same monotonic phase clock.
InvocationStartNS is recorded at the local client-interface method entry,
separately from the outer call interval; it proves local dispatch began and
does not claim wire submission, commit completion or a server prefix.
This rejects never-invoked/future candidates but does not invent an applied-index
or claim that the search saw an exact acknowledged prefix. Native generation,
HNSW/read proof, no exact scan, canonical score/order, zero retry/redirect counters
and complete bounded response checks remain required.

The writer declares offsets0,interval,2*interval,... and executes serially. Its
next invocation is no earlier than both that offset and one full pace interval
from the previous invocation start. A complete configured RPC budget must fit
both the remaining measured admission and overall budget. A future offset that
cannot fit is marked unissued immediately, without waiting beyond the cutoff.
Shorter successful prefixes are retained honestly; at least one successful write
and a successful native search completing during an outstanding insert API call
are necessary for this mode's overlap observation. That is client interval
evidence, not a server critical-section, latency guarantee, saturation or capacity.
Any read-call/validation/cap or write/proof failure cancels both roles immediately,
joins every worker, retains actual failures/UNKNOWN and leaves a complete unissued
suffix. Duration is fixed only after both read and writer drain. Read QPS excludes
setup, oracle, pre-recall, warmup, resource waits, retry, visibility and post-recall.
Successful/non-success read and ordinary-write call latency populations stay
separate; explicit retry and post-ack probe intervals are individually retained.

After a successful measured phase and drain, the resource done handshake keeps
all client sockets/process alive for root's closing five-role samples. One
explicit identical logical retry of the last acknowledged fresh write then runs
outside read QPS; an idle writer connection is deliberately renewed before that
planned phase, never after a failed call. The new consensus commit must leave
live revision/ID/owner/partition/generation unchanged. Final all-voter readiness
uses that new prefix. Post-ack TopK1 visibility probes and paired sixteen-call
quiescent recall use a deliberately renewed idle reader outside measurement.
The post oracle adds only acknowledged distinct vectors, excludes retry as a row,
records its exact population digest, and rechecks all-four final readiness.
UNKNOWN never becomes an assumed committed/uncommitted population. Low recall
is recorded, not an invented threshold; external root gates decide agreed quality
and service thresholds independently of this harness observation verdict.

`fixed_cluster_paced_window_v1` planned/result records preserve all existing
window fields plus explicit write slots, receipts, skipped reasons, starting/final
revision/prefix, prefix proof scores/digests, retry, bounded overlap counts and
paired recall. Read retention charges actual encoded attempt bytes; a3MiB reserve
covers the bounded write, visibility, paired recall and terminal evidence. Small
otherwise valid byte caps can refuse if the full planned/result evidence cannot
fit. The final pair is checked against its actual encoding; a cap or encoding
failure refuses qualification. Retained JSON bytes are not a Go heap bound:
response copies, maps/slices, transient encodings and final output buffers add
harness memory. No full-result duplicate is written to the resource directory.

The initial admission deliberately cannot chain a later paced population.
After any invocation, root must preserve its raw ledger/UNKNOWN history and never
rerun the admitted inputs on the changed store. This source scope supports an
initial C1 observation; a second C4 run needs a separately qualified fresh cluster
or reviewed chained population admission. Limits and elapsed time do not erase this
sequencing restriction. Runtime collection, native/resource review, source/ELF
freeze and predecessor integration remain root-owned gates; source construction
alone does not establish a qualifying actual paced window or close #4959/#4250.

Root validation commands (not run during source construction):

    GOWORK=off go test ./cmd/treedb-query-under-write
    GOWORK=off go test -race ./cmd/treedb-query-under-write

Focused additions: TestPacedFullBaselinePrefixInvariantAndCanonicalTie,
TestPacedInvocationEvidenceAndFullRPCBudget,
TestPacedSuccessfulSharedQueryOverlapAndExplicitRetry,
TestPacedReadFailureImmediatelyCancelsWriterAndRetainsUnknown,
TestPacedLateWriteSlotsStayUnissuedWithoutWaiting,
TestPacedModeBoundsRefuseBeforeInputOrNetwork,
TestPacedReceiptFenceMismatchRemainsUnknown. Existing TestWindow*, TestRecall*
and mixed-workload tests remain required. Channel rendezvous establishes tested
API overlap/cancellation; private short clocks are not distributed runtime proof.

## Mixed colocated replace/delete window

`-mode mixed-window` reuses the fixed RF4 read-window admission, connections,
resource gates and retained attempts. Existing insert/read modes are unchanged.
This mode admits exactly six serial slots in a 60-second window: replace A,
supersede A, delete B, replace C, delete C, delete D. A/B/C/D are four existing
corpus IDs outside every query's baseline and exported-corpus canonical top10.
The minimum invocation interval is `-mixed-interval` (default 5s, 1s..8s), with
full write and untimed visibility RPC budgets required before the cutoff.
Warmup is 64 calls; read concurrency/caps use existing read-window limits.

```sh
treedb-query-under-write -mode mixed-window \
  -config fixed-peer.json -bootstrap-receipt fresh-bootstrap.json \
  -dataset frozen-dataset -provenance fresh-provenance.json -phase pre \
  -run-id mixed-one -read-concurrency 4 -read-window 60s \
  -read-warmup 64 -mixed-interval 5s -rpc-timeout 3s -timeout 5m \
  -read-resource-gate-dir fresh-owned-gate > mixed.jsonl
```

Admission scores the **entire frozen population** at all seven planned prefixes
before client networking, using canonical FP32 cosine and stable ID ordering.
The 10,000-row/128D/16-query corpus is not the entire baseline: the authenticated
bootstrap's fixture anchors and prior explicitly proven rows are counted too.
`Admission.PopulationRows`/`PopulationSHA256`, `Anchors` and each `Prefixes` row
retain that actual reconstructed identity. Never reuse a prior campaign's final
population, revisions or commit positions. Root must freeze/export the actual
fresh source and verify this admission population before starting this mode.
Any planned canonical top10 ID or score-bit change refuses admission.

Each prefix also retains all four changed IDs' canonical `ScoreBits` and
`Present` values for each query in `Prefixes[].Changed`. A measured response must
match **one** permitted prefix in its entirety, including changed-ID absence;
unchanged IDs retain baseline canonical scores. The permitted interval is bounded
by original mutation ACKs before call entry and issued mutations before call
return. Online validation is tightened after both reader and writer join against
actual retained `StartNS`/`EndNS`; `ReadPrefixes` records lower/upper/matched
prefix per measured ordinal. This prevents mixing two postimages or accepting
an arbitrary old state. Approximate HNSW results remain allowed: recall uses the
existing invariant full-population top10 and existing external acceptance
threshold, not an added recall=1 gate. Each ACK gets an untimed strict query with
its visibility token, validated against exactly that acknowledged prefix before
the next writer invocation.

After drain, the driver explicitly retries original slot0 after supersession and
original slot2 after deletion. It retains their original request identity and
requires identical original term/index/counts/coverage/live revision/token, with
returned applied index covering that outcome. `HighestNewCommitIndex` excludes
these retries; `RequiredAppliedIndex` additionally covers their observed apply
positions. A complete acknowledged ledger reconstructs the final expected
population; quiescent canonical recall and all-voter catchup follow. Any failed,
UNKNOWN, malformed, truncated, mismatched or missing sample consumes the run:
no automatic retry, discarded sample, exact fallback or fast-read substitution.

The final `AuditPlan` is a bounded six-original-request/ACK attachment to the
**existing** fixed-peer diagnostics operation. The driver acquires `Audits` from
all four live voters before any shutdown. Root may also extract that exact plan
from the final result and independently collect the same existing operation:

```sh
treedb-fixed-peer -mode diagnostics -config voter.json \
  -colocated-audit-plan extracted-plan.json > voter-audit.json
```

The plan is at most 524288 encoded bytes, version1, six unique original attempts,
zero logical deadlines and at most six final unique IDs (this mode has four).
`treedb-fixed-peer` loads/validates it before opening any stores or client
networking. Ordinary diagnostics omit `ColocatedAudit`. No endpoint, offline
DB opener, scheduler or shutdown hook is added.

Every audit proves the actual current-FSM DB binding, ACTIVE/catalog/router
scope, applied floor, exact original command digests/outcomes and physical WAL
coverage. It verifies the retained ordinal/count/byte/SHA chain and performs
six exact witness lookups, then uses the prepared source owner to prove final
canonical content/absence and exact per-domain live membership for the known
IDs. The receipt carries per-voter applied term/index, physical root state,
next WAL LSN, retained count/bytes/chain, six witnesses and final-ID proofs.
Current DB, root, applied state, summary and next LSN are rechecked; any torn
state or pending publication refuses the audit. The owner wrapper can flush
pending work; this is **not** an inherently read-only operation. Pending gauges
are a conservative precheck; unchanged physical state/covered WAL and fenced
proofs establish the accepted observation. A stale handle with matching bytes,
forged digest/chain, incomplete witnesses or source/live disagreement cannot be
successful evidence. Root-owned quiescence is required; these checks do not
create a distributed stop-the-world barrier. Keep all voters live until all
four receipts and all-voter catchup have been verified, then retain clean-stop
receipts separately.

`mixed-report-v1.schema.json` describes the JSONL envelope and audit-plan shape;
semantic validators are authoritative beyond schema shape. Root collectors must
bind binary/config/bootstrap/dataset/provenance hashes, planned/result pair,
seven full-prefix truths and changed-ID scores, every issued/unissued original
and retry, probe response hashes, query coverage/recall/error counters,
client-call overlap and causal-prefix rows. `Writes[].LogicalSHA256` binds the
zero-deadline request; `RequestSHA256` binds its actual deadline-bearing call.
Search attempts retain their logical query hash plus actual deadline; token
probes retain their actual request hash. No socket dispatch or server critical
section overlap is claimed. Verify six original ACKs, two original retries,
six token probes, full final-ledger identity and four matching audit summaries;
per-voter physical LSNs/roots are local and need not equal.

Root resource brackets reuse the ready/done nonce-bound gates. Report per-role
process/cgroup/disk/network availability and sampling limits honestly. Search
QPS includes online validation/retention and call drain, excluding setup, full
oracle, warmup, resource waits, post-join causal recheck, retry, recall and audit.
Writer latency is public client submit-to-visible ACK; six raw samples do not
justify p99, sustained capacity, speedup or request-attributed process allocation.
The driver verdict remains pending root shutdown verification. The audits prove
only six original witnesses and four known IDs, **not** the entire 10K source
population. Cross-group projection, general changing-topK attribution, unlimited
retention, movement/splitting, host loss and capacity remain open. RF4 quorum
three cannot survive either two-voter host's loss.

Allocation ownership: corpus/query vectors and prefix truths are immutable after
setup. Seven full-population maps/oracles are built outside measurement; retained
prefix evidence contains only 16x10 truths and 16x4 changed-ID scalar scores.
Measured calls copy response strings/neighbors/tokens before client reuse. The
writer retains six responses and two explicit retries; the final plan borrows
that immutable ledger until synchronous serialization, and receivers own decoded
plan bytes. Four audit receipts own six scalar witnesses/final proofs each.
The existing aggregate JSONL byte cap covers the complete pair. Audit chain
verification and manager gauges are untimed observation costs, not a hot serving
path. `BenchmarkMixedRetainedCallV1` measures attributable fake-client retention
cost; process samples include clients/background/audits according to their
brackets and are not B/op.

Validation selectors: `TestMixed*`, `TestColocatedAudit*`,
`TestFixedPeerColocatedAuditCurrentAuthorityV1`, plus unchanged `TestWindow*`,
`TestPaced*` and `TestRecall*`. Existing
`TestVectorPartitionColocatedOutcomeSummaryVerificationV1` covers forged chains,
ordinal/count/bytes/version/uncovered records; existing
`TestVectorPartitionColocatedMutationProofV1` covers source/live disagreement.
The new mixed/audit API was absent at the provisional predecessor head: focused
fake tests were authored before implementation, but there is no runnable
pre-implementation API test. This capability-absence exception is not a compiler
failure presented as product-red evidence. Linux Go1.26 normal/race, matched
existing-mode allocation guards, exact-head CI/review and a landed independently
verified two-host campaign remain required for acceptance.
