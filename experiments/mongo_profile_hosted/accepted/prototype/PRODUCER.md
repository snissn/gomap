# Standalone phase-profile producer: UNBUILT

This is an out-of-repository diagnostic prototype against exact base
`95c6ff3de473786bd98b692375cfdb9cef951668`. It is not runtime qualification,
benchmark evidence, a production edit, or evidence of a performance improvement.
Only pinned Go 1.26.10 `gofmt` parsing/formatting has run. No import resolution,
typechecking, Go build/test, server/client launch, worktree creation, or runtime
negative test has run. The host's unchanged 8 GiB build and 10 GiB public-run
free-space floors and exclusive measurement custody still gate those actions.

`make_overlay.py` checks the frozen source manifest and original server SHA-256
`2b19dfec2741a5dfaacfdd50cf31cccf95e0228b565420c9992d9fe4e6a73490`, patches
three seams, and appends `profile_control.go.txt`. It writes a generated
`diagnostic_server.go.txt`, `overlay.json`, and `source.patch` here, plus
`../evidence/producer-syntax.json` and `producer-manifest.json`. The latter binds
raw source, source manifest, helper, generator, generated entry, overlay, and raw
patch hashes. Existing output may only be reused when byte-identical. A reviewed
changed producer requires a fresh packet or explicit replacement of its own
previous generated outputs; the generator refuses silent overwrites.

The first generated evidence is preserved byte-for-byte under
`../evidence/producer-batch1/`. During batch 1, the coordinator completed the
maintained validator fixture/dependency exports in `source/manifest.json`.
Coherent batch 2 regenerated only producer evidence against that finalized
source manifest; its generated source, overlay, patch, and syntax result were
unchanged. Independent source review rejected that candidate for buffered HTTP
response errors and request paths bypassing application validation. The
coordinator preserved the rejected exact bytes and review under
`../evidence/rejected-candidate-1/`. This first functional repair batch adds
direct dispatch and checked framed response flushing together. Only owned
generated outputs/evidence are explicitly replaced; rejection history is not
edited. The generator's no-overwrite rule remains in force.

The default overlay maps the actual absolute build-as-file entry
`/mnt/fast4tb/gomap-profile-producer-95c6ff3/TreeDB/mongo_gateway/server.go`.
No directory at that future path is created. `--worktree` selects a different
future path; if it already exists, HEAD and the actual original entry must match
the pinned source before output generation. The generator also requires empty
`git status --porcelain=v1 --untracked-files=all --ignored=matching` output,
rejecting staged, modified, untracked, and ignored content. The dirty primary
checkout is never the overlay target.

## Lifecycle preservation

The original `OpenStandaloneServer`, `ValidateListener`,
`ycsbevidence.OpenFromEnv`, `evidence.StartCheckpointControl`, signal context,
`standalone.Serve`, and original `evidence.Close(standalone.Close)` are retained.
No configuration, batches, flags, checkpoint callbacks, storage operation, forced
GC, or ownership observation is replaced. Startup adds the diagnostic control
only after successful original owner/checkpoint initialization. Its startup
failure closes the client listener, original checkpoint control, and original
observed owner, joining their errors before returning 1. Normal shutdown first
closes/joins the diagnostic control, then calls the original checkpoint close
and observed original owner close, joining all returns. Diagnostic failure cannot
skip either original close. The existing earlier error paths are unchanged.

When `GOMAP_DIAGNOSTIC_PROFILE_DIR` is unset, the helper returns nil and nil-safe
Close returns nil. When set, it requires an absolute nonexistent output path;
`Mkdir` and `Chmod` establish 0700. It reads, decodes into the original
`ycsbevidence.Observation`, and retains `YCSB_SERVER_RECEIPT_DIR/owner.open.json`,
checking the schema/kind, owner ID, and current PID. It does not construct or
replace that receipt or schema. The owner evidence hook remains responsible for
actual owner/backend observation.

The control binds only AF_UNIX `phase.sock`, chmod 0600, using direct
`http.HandlerFunc(c.admit)` dispatch and
`DisableGeneralOptionsHandler: true`. ServeMux canonical-path redirects and the
general OPTIONS shortcut therefore cannot bypass application validation. It has
bounded HTTP read/header/idle/write timeouts and no TCP or default pprof server.
The Serve goroutine is joined; admitted handlers are counted and joined;
Close prevents new admissions and closes the HTTP server/listener. All
application responses set the exact Content-Length before Write, check short
writes/write errors, and explicitly check `http.NewResponseController(w).Flush()`
under the state mutex; admitted handlers retain their lifetime until that local
flush completes. Failed response writes/flushes for requests admitted before
Close marks the control closed are sticky and included in its return. Post-close
rejections use the same checked framing under the mutex without admitting new
work; their errors are best effort while the object remains live and cannot
retroactively change a Close return that already completed. No goroutine calls
`os.Exit`.

## One fixed phase

After the original gateway is ready, a controller may send exactly:

```sh
curl --fail --unix-socket "$PROFILE_DIR/phase.sock" -X POST \
  'http://localhost/start?mode=cpu_wait'
# Execute the already frozen diagnostic workload; then:
curl --fail --unix-socket "$PROFILE_DIR/phase.sock" -X POST \
  'http://localhost/stop'
```

`mode=off` uses identical phase/owner/receipt control without enabling samplers or
creating profile files. Both modes have exactly one lifetime phase, `run_1`.
There is no reset/retry, phase-name, GC, batching, or runtime tuning option.
For parsed requests reaching the application handler, unknown routes (including
canonicalization candidates such as `/x/../start`), non-POST/body requests
(including `OPTIONS *`), invalid/extra query arguments, duplicate start/stop,
stop-before-start, and Close while active or never-started fail closed. net/http
may reject malformed framing before application validation runs; those framework
rejections do not promise a sticky application error.
For admitted requests, control/file errors are sticky: HTTP reports failure and
the gateway Close joins those errors and returns nonzero. This propagation covers
requests admitted before the shutdown boundary, not later rejected requests or
framing rejected by the HTTP framework. A failed startup capture is physically finalized for cleanup and
cannot be accepted as a successful phase. Failure receipts may be incomplete and
are never valid success evidence.

`cpu_wait` opens exactly `run_1.cpu.pprof`, `run_1.block.pprof`, and
`run_1.mutex.pprof` with O_EXCL/0600. It starts CPU and mutex/block sampling at the
start boundary, stops CPU with `StopCPUProfile`, disables block sampling, and
restores the prior mutex fraction at finalization. This assumes the fresh
standalone entry's ordinary initial block-sampling state (disabled); Go exposes
no getter for the previous block rate. No other profiler may share this process.
Wait profiles are process-global, single-phase diagnostic samples; they do not
establish operation attribution, exact per-stage latency, exact CPU fractions,
or portable timing. Pprof decoding and symbolization remain unqualified.

One shared bounded writer enforces 67,108,864 total profile bytes, rejects any
chunk exceeding the remaining budget, and retains write/short-write errors.
The CPU profiler can discard writer return errors internally, so the sticky error
is checked only after `StopCPUProfile` joins its writer. Each block/mutex
`WriteTo`, each profile and receipt Write/Sync/Close, and each listener close is
checked. Every opened profile is synced and closed even after failure. Profile
hashes and byte totals are calculated from the actual bytes accepted by writes.
Receipt file bytes are outside the declared profile-only cap. A successful local
HTTP flush does not prove client receipt: the controller must independently veto
any failed start/stop exchange, even if on-disk profile/owner receipts succeed.

`phase.start.json` uses schema 1, kind
`standalone_diagnostic_profile_start`, the full original owner observation,
mode/phase, `begin_utc`, and `ready_utc`. `phase.stop.json` uses kind
`standalone_diagnostic_profile_stop`, the same full owner, `start_utc` equal to
the start receipt's `ready_utc`, requested/finished timestamps,
`ycsbevidence.Result` finalization, artifact name/bytes/SHA-256 entries, and exact
limit/total bytes. `ycsbevidence.Call` brackets the physical stop, profile writes,
Sync, and Close; `finished_utc` follows that result. Receipts use O_EXCL with
checked Write/Sync/Close. A failed stop-receipt write or locally detected HTTP
response failure within a request admitted before Close may leave
a physically completed profile result on disk, but its sticky error still vetoes
a successful gateway exit and must veto integration acceptance. The original
storage-only owner-close receipt can still record a successful storage close;
acceptance requires the actual zero gateway exit and successful controller
exchange as well. The controller's own failed exchange independently vetoes a
capture even when local flush reports success.

## Future build recipe; do not execute until gates reopen

After the owner records sufficient free space and exclusive custody, create a
fresh, clean detached worktree at the overlay's frozen absolute path and HEAD.
Do not reuse the dirty primary checkout or a different candidate. The recipe
below deliberately binds the actual entry, toolchain, offline dependencies,
ordinary optimization settings, and task-specific writable cache/output paths.
No commands in this section have been executed.

```sh
# Preparation after authorization: verify writable /mnt/fast4tb, free-space floors,
# clean pinned worktree, complete existing module cache, and measurement custody.
TASK=/mnt/fast4tb/gomap-profile-producer-95c6ff3-artifacts
WORKTREE=/mnt/fast4tb/gomap-profile-producer-95c6ff3
PACKET=/home/mikers/gomap-mongo-performance-exec-20261031-5z3yyej9/profiling-prototype-followup
PINNED_GOROOT=/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.10.linux-amd64
mkdir -p "$TASK/tmp" "$TASK/gocache" "$TASK/bin"
cd "$WORKTREE"
test "$(git rev-parse HEAD)" = 95c6ff3de473786bd98b692375cfdb9cef951668
test -z "$(git status --porcelain=v1 --untracked-files=all --ignored=matching)"
# Independently verify all producer-manifest hashes and overlay source before build.
env -u GOMAXPROCS -u GOEXPERIMENT \
  GOROOT="$PINNED_GOROOT" GOTOOLCHAIN=local GOWORK=off GOFLAGS= \
  GOPROXY=off GOSUMDB=off GOMODCACHE=/home/mikers/go/pkg/mod \
  TMPDIR="$TASK/tmp" GOTMPDIR="$TASK/tmp" GOCACHE="$TASK/gocache" \
  "$PINNED_GOROOT/bin/go" build -mod=readonly \
  -overlay "$PACKET/prototype/overlay.json" \
  -o "$TASK/bin/treedb-mongo-profile" TreeDB/mongo_gateway/server.go
```

No special build tags, race instrumentation, disabled optimizations, custom
allocator flags, or forced CPU-count setting are added. Capture the Go version,
GOROOT/go executable hash, source/overlay/binary hashes, actual resolved backend,
full environment, gateway flags, process identity, and original checked owner
close in the future run packet. A build failure is not runtime evidence.

Required future integration gates include typecheck/build against the exact
entry; env-unset equivalence; fresh `cpu_wait` and `off` process runs; receipt
schema/time/owner/hash/cap validation; CPU artifact readability and block/mutex
artifact readability; sampling activation and cessation; and original checkpoint
and checked owner-close proof in every shutdown path. Negative integration must
cover existing/symlink/relative output path, missing/mismatched owner receipt,
invalid and duplicate controls, canonical-path POST `/x/../start?mode=off`,
`OPTIONS *` (preserve the original request-target when sending these controls),
socket disconnect during start/stop responses, concurrent stop/Close,
active/never-started Close,
listener failure, and injected Open/Write/short-write/Sync/Close/receipt failures,
including CPU sticky-writer overflow. Exercise response write, short-write, and
flush failures while retaining exact Content-Length and handler synchronization;
verify that the controller rejects failed exchanges even when the server's local
flush succeeds. The actual gateway must retain a nonzero
exit while still reaching both original close calls. No profile may qualify
until these checks pass and the coordinator's validator independently accepts
the retained exact-source packet. Public performance qualification remains a
separate unmet gate.
