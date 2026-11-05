# One exact M0 ordinal trace (manual diagnostic)

This branch replaces only the registered manual maintenance entrypoint. It is
not a required CI predicate or an M0 CV acceptance run. Dispatch is restricted to
`diagnostic/m0-combiner-operation-capture-01`, ROOT actor ID `1981537`, attempt 1, and an
independently reviewed full commit equal to both `GITHUB_SHA` and
`github.workflow_sha`. ROOT alone publishes, admits and dispatches it. Both workflow pre-helper and
worker admission require immutable ROOT authority thread
`01a18e7c-8c23-7fd2-8d55-b0486d938cd3`, also required in the controls receipt.

## Source and prerequisite

Product4934bd6d51d9047ba0187d05c1d211cbe419811a/tree7f30f92674110a4544a381468677183ab3133406 remains fixed. The selected private diagnostic bridge is indivisible: exactly seven replacements in `.github/m0-combiner-fixture/overlay-map.json`, including api.go, commit_combiner.go, the private bridge, its controls, service exporter and the original ordinal benchmark/helper. The original4934 preimage/absence of every replacement and all seven preserved dependencies are checked before compiling the full package. No old two-overlay map, PRIMARY overlay, partial method transplant or product optimization is admitted.

Owner manifest ca51c90ae96fcb04ed5764e9acd3d79a01983d0bfe99d2107a9ee7912b5938b5 and independent source review405124015cb24de0d4efcdd583f0c45e2ef3b51521efced31cdcc9c54cd757c0 are immutable. Every active helper/overlay/metadata input is size/SHA bound before imports through m0-trace-inputs.json and the reviewed exact Git commit. Dormant old observer files are not applied or imported.

The actual bridge controls producer eaf4e6ad6688dcb1ddb0748243f1f1609c1c3764 / tree5fd41b42289a16d1391def5d3fdbc4780bdd17b4 ran41969672803/job127054731875. Independent original-artifact review958811fd4b5c0a25ada2595567b1c70f5fa2fbf90cff67c1e6b76da1dd39b9e4 admitted normal AND race82names/24tops/74leaves, all15stages normal/joined/zero and positive maintained guard. Artifact13389282854/SHA19ec98a2106180a3ee38c9206153f8fe91ed1af66d585ed21cbb9e332865ec1d is controls-only, not a capture or timing certificate. ROOT canonical controls receipt ed5c3761d40fb3319b8303eaf62a3546ab9d6c4e4c631464de23b0be6f0cfd2b is mandatory in workflow and worker before any helper; ROOT must also bind the final independent binding review before a separate one-capture admission. Source review and matching self-declared strings do not provide runtime authority.

## Private service receipt

The existing benchmark's checked Close defer invokes export only after original foreground join, timer stop, Stats/exposure checks and nonblocking observation of the borrowed original worker done. Missing join/done/Close prevents complete rows; no new wait or worker is added. Caller/worker mutate disjoint slots and original channel/join/done edges publish them. The retained process-lifetime owner is never recycled. See the accepted owner's decision for exact source/lifetime limits.

The original ordinal/trace bijection remains mandatory. `m0-combiner-service.json` additionally supplies all4096 matching original regions/durations:2048 even Set and2048 odd DeleteRange NON_COMBINER rows. The consumer validates caller/worker fields, explicit timestamp validity, original error/branch categories, selected batch positions/counts/shared-time equality and lifecycle. The complete per-operation output and deduplicated shared batches are retained in combiner-service-validation.json. Zero valid timestamps and cross-owner enqueue-after>dequeue / receive<send-after are legal. A successful ordinal receipt precludes errors, stopped/drained or setup-error service rows; actual fallback stays explicit, with no fabricated service.

Caller select, worker apply, Write/WriteSync and checked batch Close are inclusive wall brackets with observer/scheduler overlap. Shared batch rows are not exclusive per-request service and nested spans must not be summed. WriteSync can include meta sync; this receipt cannot isolate meta-sync service. No positive queue-residence lower bound, CPU, lock holder, exact phase or causal-repair inference is available. The diagnostic ELF has extra fields/clocks/scans/retained state even when a selected request is nil; the original unoverlaid product remains unchanged, with no same-ELF/zero-cost claim. Panic/Goexit evidence remains an outer TEST MODEL; real nonnil concrete batch-Close injection remains UNAVAILABLE. OriginalM0 CV.164849 and every previous failure/HOLD remain unchanged.

## Fixed command and lifecycle

Hosted Go is pinned to 1.26.10. The actual Go, gofmt, Git, taskset and enabled CGO
compiler identities/context are retained; unsupported inherited GOFLAGS or
GOEXPERIMENT refuse rather than being overridden. Trusted hosted module/build
caches are reused and recorded. TMP/GOTMPDIR and output are task-specific;
GOENV=off, GOTOOLCHAIN=local, GOWORK=off. No HOME is reassigned.

A normal, optimized test ELF is constructed once with:

```
<authenticated-go> test -c -p 1 -mod=readonly -overlay=<seven-entry-map> -o <bin>/db.test ./TreeDB/db
```

The ELF/source/tree/overlay/buildinfo and actual first two allowed CPUs are
recorded. Exactly one fixed capture follows:

```
<authenticated-taskset> -c <first-two-allowed> <bin>/db.test -test.run=^$ -test.bench=^BenchmarkVacuumIndexOnlineCollectionProductionForegroundChurn$/^bytes_1x$ -test.benchtime=1x -test.count=1 -test.benchmem -test.trace=<arm>/trace.out -test.outputdir=<arm> -m0-ordinal-observer-dir=<arm>
```

This is the original production bytes_1x/N1 operation, 4,096 ordinals, default
options, original timers/phase hooks/ACKs/join/Stats/exposure/checked Close.
Only the selected observer is opt-in. It brackets the existing foreground
operations, so the trace perturbs scheduling. GOMAXPROCS=2, GOMEMLIMIT=8GiB
(soft Go target), and the first two actual allowed CPUs match the selected
4934 route. This is a newly authenticated hosted context, not a claim of
historical environment identity. There is no warmup, retry or CV computation.

## Checked standard decoder and consumer

Pinned Go1.26.10 `cmd/trace/main.go`, `internal/trace/event.go` and Go tool-driver
sources are authenticated. One `go tool -n trace` materializes/binds the actual
standard tool; hash, size and buildinfo are retained before use. Then only:

```
<authenticated-standard-trace> -d=parsed <arm>/trace.out
```

The exact debug grammar is a pinned implementation detail. There is no HTTP
viewer or alternate decoder. Standard `debugProcessedEvents` reads trace EOF but
ignores `fmt.Println` write errors. The fixed runner therefore polls both child
pipes, checks every retained byte write, flush/fsync/close, both EOFs, normal
zero exit and whole/stage/output limits. A write/read/close error or truncated
line refuses, even if the decoder exits zero. Partial files and exits remain.
The pattern reuses the maintained bounded CLI pipe approach and portable guard;
there is no shell redirection acceptance or posthoc unbounded trace join.

The fixed consumer validates the authenticated successful owner lifecycle and
all 4,096 identity/duration rows. `OriginalDurationsMatch` is the owner's
**in-process** comparison to its original latency array. That other array is
not separately serialized, so the consumer cannot independently recompare it.
It requires precisely one ordered begin/end pair per expected region, the
same positive goroutine and same task (task 0 is valid for context.Background), no overlap or timestamp regression, no unknown
selected form, no extra/missing/duplicate ordinal. Nonselected ordinary trace
events remain in the full raw text. The bounded alignment receipt retains each
ordinal's original owner duration and same-trace begin/end timestamps. Region
duration includes observer work; it is not the original operation duration,
a queue/execute decomposition, historical alignment or CV acceptance.

The original fifteen finite Python control methods are preserved: one complete fixture, twelve
adverse populations/grammar/lifecycle/decoder cases, checked I/O failure, and
fixed-entry source contract. They run once hosted before product capture; pure synthetic methods were also checked during source preparation, without Go or original payloads. Six additional authority methods cover matching
thread/review bytes, missing/foreign admission thread, missing/foreign controls
thread, and altered review bytes/head. The consumer source/data methods are pure fixtures; actual hosted capture remains UNRUN for this successor.
Ten additional service methods cover complete shared batches/overlap, valid zero timestamps, lifecycle refusals, original identity/duration/population, odd-row refusal, invalid/regressing stamps, unknown categories/errors, shared-batch conflicts/duplicates/missing members, legal direct fallbacks and exact export/one-capture source contracts. All 31 pure source/data methods are required; actual Go/capture qualification is separate. The original Go controls are a separate prerequisite.

## Time, resources and output ledger

The hosted job is 20 minutes. The shared worker/portable-guard deadline is
1,020 seconds from the first authenticated step's monotonic start, including
checkout/setup elapsed. Checkout is at most 60 seconds, setup 120 seconds;
these ceilings consume that same clock and are not extra entitlement. The
worker refuses exhausted time before starting any child. The unchanged guard
samples owned descendants every .2 seconds, RSS cap 24GiB, free floor 10GiB,
cleanup 10 seconds. The step/worker cannot certify descendant cleanup itself;
positive maintained outer guard result and independent ROOT output review are
required. Cancellation, live descendants or missing finish/cleanup stay adverse.

Fixed worker stage ceilings are source fetch 120s/checkout 60s; other source
metadata/config commands 10s each; formatting 30s; consumer controls 30s;
trace-tool construction 90s; normal build 300s; capture 240s; decode 90s;
small version/context/buildinfo commands 10s each. These are per-stage upper
bounds, **not additive reservations**: each is clamped to the remaining shared
1,020 seconds. The initial recipe's 960-second broad-group estimate does not
promise completion; trusted hosted bootstrap/metadata overhead can cause a
terminal timeout, never a reset, extension or repeated arm. Up to 20 seconds
is reserved in the 1,200-second job for guard cleanup and 120 for artifact
upload, leaving at least 40 seconds outer margin beyond a full 1,020-second
worker clock. Upload uses a two-minute action deadline.

The diagnostic's output refusal bounds are raw trace 32MiB, full parsed text
128MiB, every other retained log/receipt 32MiB, and aggregate capture/decoded/
receipts 256MiB. The census includes `output/**`, `guard/**`, and root top-level
JSON/logs from creation, including partial files/control logs/receipt bytes.
Checked pipe writes reserve the next chunk before writing. Go-written trace,
owner JSON and guard files are sampled while the child runs; the first observed
breach is retained before refusal. It is neither a hard instantaneous storage
limit nor a measured peak. Small adverse receipts may be retained after breach,
counted and refused; they do not restore within-cap success. EOF/fsync/close or
final census failure precludes a successful worker receipt. A fixed always-run
post-guard census also checks guard/entry files written after the worker exits,
counts its own finite receipt prospectively and refuses on any earlier sticky
output breach. It must complete inside the same 1,020-second clock; no clock
reset or extra execution allowance is supplied. Its one-minute action ceiling is
also bounded by the 20-minute outer job.

Source/Git objects, SDK, module/build caches, test ELF, temporary DB/build work
are ordinary hosted construction/input bytes; they have **no invented 1GiB cap**.
They remain subject to free-space/RSS/whole-time controls. This output ledger
is diagnostic-only and changes no M0 CV gate, PRIMARY 1GiB cap or platform contract.
No raw trace payload is printed to the console; all fixed raw/parsed/ordinal/
source/tool/ELF/stage/limit/guard evidence is uploaded, even on failure. Trace
contains runtime/source metadata; no new document/body/credential logging is
added. Child environments remove fetch credentials and ROOT admission inputs.

Reproduction requires ROOT's exact-head review plus the fixed manual dispatch
inputs `expected_head`, `root_admission_b64`, `controls_pass_b64`, and
`binding_review_b64`. The latter transports the actual final independent review
bytes (bounded 16KiB decoded). Their SHA must equal the ROOT admission's
`independent_binding_review_sha256`, with its matching
`independent_binding_review_head` and `independent_binding_review_status` set to
`READY_SOURCE_ONLY`. The exact supplied review bytes are retained reversibly in
`binding-review.json`. ROOT must inspect and authenticate that actual frozen
independent review before writing admission. The script verifies byte binding;
a hash, self-declared receipt or matching strings do not prove review authorship
or readiness. There is no signature/cryptographic-authority claim; a source review
or completed ordinal receipt alone provides no dispatch/admission authority.
All old CV/PRIMARY/CPU/GetVersioned failures and logical holds remain unchanged.
