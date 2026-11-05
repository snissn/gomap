# One exact M0 ordinal trace (manual diagnostic)

This branch replaces only the registered manual maintenance entrypoint. It is
not a required CI predicate or an M0 CV acceptance run. Dispatch is restricted to
`diagnostic/m0-operation-trace-01`, ROOT actor ID `1981537`, attempt 1, and an
independently reviewed full commit equal to both `GITHUB_SHA` and
`github.workflow_sha`. ROOT alone publishes, admits and dispatches it. Both workflow pre-helper and
worker admission require immutable ROOT authority thread
`01a18e7c-8c23-7fd2-8d55-b0486d938cd3`, also required in the controls receipt.

## Source and prerequisite

Product source is exactly `4934bd6d51d9047ba0187d05c1d211cbe419811a`, tree
`7f30f92674110a4544a381468677183ab3133406`. Only the corrected owner's `m0.C`
map applies: the original collection vacuum benchmark and its test-only ordinal
observer. No PRIMARY, caching, DB production, race or test-fixture overlay is
used. The source is a verified sparse Git checkout, with only the four established
large evidence/benchmark documentation directories excluded, and no archive
fallback. The original benchmark preimage is authenticated before overlaying it.

Owner manifest `86f4ee083aba9e2f2dbdb87abf9ec5be9eb13d416e1bafd25f7b61139e317aa3`
and format review `863afc8d80a3f68f767b8be0f8f810f5ab646a5c1e256ea674b7894a97ef883a`
are fixed inputs. Every active helper/overlay input in `m0-trace-inputs.json` is
hashed before helper import. Its own identity is bound by the reviewed clean
Git commit, without a circular self hash. Existing unused controls/PRIMARY files
in that tree are not executed or applied.

**Not launchable from source review alone.** A separate ROOT-admitted actual
controls receipt must bind producer `ee6c3880e1b0ec6d7e490232f3a3de45643a455e`,
tree `ffc790f080c04708d9363d375f9b380d7430350a`, both normal and race exact
6 tops/26 outcomes/24 leaves, literal exits zero and positive outer guard
cleanup. It must include actual run/artifact identities and independent review.
The earlier 42b6 normal timeout, unentered DB/race controls, and outer guard
failure remain adverse. Empty signal arrays were not a drain certificate.
The workflow checks a SHA-bound base64 ROOT receipt before checkout/setup; the
entrypoint rechecks it before imports, source acquisition or Go. The actual ROOT-admitted controls receipt is now fixed at SHA
`5fbc92847359e709fe9ef7812786ff469f9892dedf9e4c81d27a3183e67245c5`
(run 41940783159, job 126960744638, artifact 13377165673), with normal/race
6/26/24 and positive maintained guard cleanup. Its bytes must match that exact
SHA in workflow and worker, as well as ROOT admission. The receipt is supplied
as an input, not copied into a circular source/review seal. It admits controls
only; this source still requires its own final independent READY review and
separate ROOT capture admission.

## Fixed command and lifecycle

Hosted Go is pinned to 1.26.10. The actual Go, gofmt, Git, taskset and enabled CGO
compiler identities/context are retained; unsupported inherited GOFLAGS or
GOEXPERIMENT refuse rather than being overridden. Trusted hosted module/build
caches are reused and recorded. TMP/GOTMPDIR and output are task-specific;
GOENV=off, GOTOOLCHAIN=local, GOWORK=off. No HOME is reassigned.

A normal, optimized test ELF is constructed once with:

```
<authenticated-go> test -c -p 1 -mod=readonly -overlay=<m0.C-map> -o <bin>/db.test ./TreeDB/db
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
fixed-entry source contract. They run once hosted before product capture;
locally they remain UNRUN. Six additional authority methods cover matching
thread/review bytes, missing/foreign admission thread, missing/foreign controls
thread, and altered review bytes/head. All twenty-one methods are UNRUN here.
The original Go controls are a separate prerequisite.

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
