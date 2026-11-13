# README YCSB and MongoDB comparison recipe

This is the portable version of PR #5408's direct CLI recipe. The timed
producers remain `cmd/mongo_gateway_bench` and external
`snissn/go-ycsb@3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`. It preserves the
48 fixed slots, workload arguments, repetition order, fresh directories,
public full-data census, graceful stop and independent reopen checks.
It does not qualify the separate Linux runtime-attested YCSB matrix.

Build before collection with Go 1.26.10, `GOWORK=off`, CGO enabled and persistent
Go caches. Use a clean product checkout and a separately pinned, reviewed and
landed harness checkout. Copy this directory's seven Python/Go recipe sources
listed in `freeze.py` to a fresh task's `recipe` directory. Build the census
with `go build -o "$TASK/bin/census" "$TASK/recipe/census.go"` from the product
module root. The other required binaries are `treedb-mongo-gateway`
(`TreeDB/mongo_gateway/server.go`), `mongo_gateway_bench`,
`mongo_gateway_compare_report`, `ycsb-correctness-fixture`
(`scripts/ycsb_correctness_fixture.go`) and `go-ycsb`. Build the external
client with `-tags libsqlite3,sqlite_omit_load_extension`; do not patch sources.
Place its clean source worktree at `$TASK/go-ycsb-source`.

```sh
python3 "$TASK/recipe/freeze.py" --root "$TASK" --repo "$PRODUCT" \
  --harness-repo "$HARNESS" --go "$GO" --mongo-image-id "$IMAGE_ID"
shasum -a 256 "$TASK/freeze.json"
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite smoke
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite ycsb
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite shape
python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite sweep
python3 "$TASK/recipe/aggregate.py" --root "$TASK" \
  --expected-freeze-sha256 "$RECORDED_FREEZE_SHA256"
python3 "$TASK/recipe/archive.py" --root "$TASK" --output "$EVIDENCE"
python3 "$TASK/recipe/verify_archive.py" --directory "$EVIDENCE"
```

Record the freeze digest outside the mutable task before running. An exclusive
task lock prevents simultaneous suites. A failure stops the suite, leaves its
failure receipt and database, and must never be retried as a replacement sample.
A reviewed continuation may execute only never-started slots. Builds, tests,
compression and cleanup of unrelated outputs stay outside timed collection.
The freeze records the actual user-selected host as shared infrastructure;
unrelated services are preserved. MongoDB uses a frozen image ID and a fresh bind
mount with an ephemeral loopback port. On Linux, MongoDB runs with the host
user/group IDs so its bind-mounted files remain readable and releasable without
privileged cleanup. Read the host and filesystem metadata
before interpreting results.

The census proves observed key/typed field state and selected query behavior,
not every acknowledged update or physical index activation. YCSB original Close
receipts are checked. The macrobenchmark's original deferred Close return is
unobserved; independent reopen does not recover it. The durable YCSB label uses
different durability requests and does not prove crash or power-loss equivalence.
Storage is stopped filesystem allocation (`st_blocks * 512`) of the complete DB,
including redo/WAL and persistent value logs, excluding image/runtime overhead.
Summaries require all three valid repetitions; tails are medians of per-run
percentiles, not pooled requests. Reports must use the actual frozen host and
attempt ledger; the historical Mac renderer is deliberately excluded.

Offline controls, from this directory:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_recipe.py
"$TASK/bin/census" -self-test
```

## Idle-checkpoint A/B diagnostic

`idle_checkpoint_ab.py` is an opt-in GoMap-only timing comparison for #5415;
it does not change the 48-slot recipe above. Prepare two independently frozen
roots with distinct clean product commits and the same recipe, Go environment,
external client and validator binaries. Reuse the exact prebuilt client/census/
correctness-fixture binaries in both roots. Record both manifest digests outside
the tasks. Review and land the diagnostic driver before expensive retained
collection, and retain its commit, source digest and complete output directory.

```sh
python3 "$HARNESS/scripts/readme_mongodb_bench/idle_checkpoint_ab.py" \
  --baseline-root "$BASELINE_TASK" --candidate-root "$CANDIDATE_TASK" \
  --baseline-freeze-sha256 "$BASELINE_FREEZE_SHA256" \
  --candidate-freeze-sha256 "$CANDIDATE_FREEZE_SHA256" \
  --out "$EVIDENCE/idle-5415-attempt-01" \
  --lane-lock "$SHARED_MEASUREMENT_LOCK" --long-ops 100000 --order AB,BA,AB
```

Choose the longer fixed operation count from a baseline pilot before collection;
100000 above is an example, not a claim that it lasts 30 seconds on every host.
The plan freezes counts, order, source manifests and the default timing rules
(median paired throughput ratio at least 0.95 and update p99 ratio at most 1.10).
The first long baseline runs before any candidate and each long baseline must
last at least 30 seconds. A short baseline invalidates that attempt; preserve it
and prepare a fresh plan instead of replacing its samples. Each length has three
mixed-order pairs, with the original short cell fixed at 10000 operations.
Every case uses `run_case`/`ycsb_phase`, a fresh DB, full census, checked client
and server shutdown, deterministic fixture, and independent reopen checks.

The driver holds both original campaign locks plus the supplied shared host lane
lock until exit. All measurement owners must cooperate with that same host lock;
this is not host-wide process, disk or network attestation. Keep other builds,
benchmarks, profiles and artifact transfers outside the exclusive measurement
window. No failed sample is retried; original failures, cases and the frozen plan
remain. Hash drift aborts collection. Profile sampling is excluded from this
timing driver. Its summary deliberately reports **incomplete acceptance** even
when timing guardrails pass: collect actual write activity, maintenance reasons
and overlap, and normalized allocated bytes/objects separately without confusing
profile-perturbed diagnostics with timing evidence. Scheduling/liveness tests,
those allocation/attribution checks, review and current-head CI are still required.

Offline driver controls (no services or benchmark workload):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_idle_checkpoint_ab.py
```

## Separate allocation and maintenance diagnostics

Run `idle_checkpoint_diagnostic.py` with the same two frozen roots and manifest
digests as the timing driver, plus `--out`, `--lane-lock`, `--go /absolute/path/to/go`
and `--operations` (the predeclared long operation count). It builds separate Go
overlay binaries outside the product checkouts. The overlay observes actual
automatic maintenance attempts after eligibility and cumulative process allocation
bytes/objects at existing read-only phase boundaries. Exact overlay sources,
original source hashes, build argv, executable hashes, RPC receipts, client Close
receipts, census, shutdown, reopen and failed attempts are retained.

This is instrumented attribution, never timing acceptance. No profile sampling,
forced GC, checkpoint commands, polling or foreground-write instrumentation is
added. The private owner control is enabled because the existing protocol requires
it; only `phase_stats` read-only requests are sent. Allocation deltas include
background maintenance and observer overhead, normalized by validated total YCSB
operations. They are not update-only allocations or retained heap. Event intersection
uses a bracket containing observations and the full client process (startup and
Close included), not exact request timing. Different foreground-write timestamps
at maintenance begin/end prove publication during that attempt; elapsed time
includes coordination and cleanup and does not measure time holding writer admission.
A genuinely quiet eligibility decision can still precede a new write.

Offline controls: `python3 -B -m unittest -v test_idle_checkpoint_diagnostic.py`.

## Opt-in Gomap comparison case

`--suite ycsb-single` runs one original `run_case`: a fresh 100,000-row
load, 16 YCSB workers, uniform 95/5 READ/UPDATE, ten 100-byte binary fields,
`w=1`, and `command_wal_relaxed`. Select either 10,000 or 1,000,000 mixed
operations. No checkpoint, settle wait, warmup, profiler or statistics observer
is inserted before the timed mixed phase. Existing suite defaults above are
unchanged. This selector is a prerequisite harness, not an accepted performance
result or a replacement for the original complete-suite aggregator.

Prepare a separate side root for each clean pinned product and operation count
(for example, `base-short`, `candidate-short`, `base-long`, `candidate-long`).
Copy the same prebuilt product binary and honest build receipt into the short
and long roots; rebuilding for each operation count is unnecessary. Creating an
existing `comparison-freeze.json` fails; it is never overwritten.
Put the product gateway at `$SIDE/bin/treedb-mongo-gateway`; copy the **same
original frozen** `go-ycsb`, `census`, and `ycsb-correctness-fixture` binaries to
`$SIDE/bin/`. Keep the original task, its `freeze.json`, recipes, binaries and
clean external client source available and unchanged. On another host, pass
`--client-repo` to select its clean transferred checkout; its commit/tree must
match the original freeze while the original manifest bytes remain unchanged. No MongoDB image or
unused macrobenchmark binary is required by this explicit Gomap-only mode.
The original manifest is never rewritten to assign its product identity to a
new product.

Build outside collection, from the clean product module root, with an explicit
Go 1.26.10 driver and matching GOROOT. Retain a JSON build receipt containing:

- `schema_version: 1`, `source_commit`, `source_tree`, and `binary_sha256`;
- `argv`: the absolute Go executable followed by `build`, optionally
  `-buildvcs=true`, `-o`, the absolute original build output path, and
  `./TreeDB/mongo_gateway/server.go`;
- `environment`: explicit `GOROOT`, `GOTOOLCHAIN` (`local` or `go1.26.10`),
  `GOWORK: "off"`, and `GOFLAGS: ""`;
- exact `go_version` (`go version`), `go_env` (JSON from `go env -json
  GOVERSION GOROOT GOTOOLDIR`), `compiler_version` (`go tool compile -V`),
  and `binary_build_info` (`go version -m "$SIDE/bin/treedb-mongo-gateway"`)
  output, with trailing whitespace stripped.

The verifier checks live toolchain identity and receipt hashes. Any embedded
VCS identity must match the clean pinned product; file-list builds that omit
VCS settings rely on the explicit source/tree/build receipt and binary hash.
The binary may be copied to another side root: its hash and build information
must match, allowing only the filename prefix of `go version -m` to differ.
The receipt must describe the actual build, not be reconstructed from an
unrelated binary. Overlays, race builds, custom build flags and observer/runtime
overrides are excluded, including `GOGC`, `GOMEMLIMIT`, `GODEBUG`,
`GOEXPERIMENT`, `GOMAP_READ_*` and all `TREEDB_*` settings. Both the parent
environment and the effective child environment are checked before every case.
The merged receipt build environment receives the same check. Embedded experiment,
instrumentation and custom Go/C compiler flag settings are rejected as well.
Run the reviewed selector from its clean landed harness
checkout, with no generated files in that checkout:

```sh
python3 "$HARNESS/scripts/readme_mongodb_bench/comparison.py" \
  --root "$SIDE" --repo "$PRODUCT" --original-freeze "$ORIGINAL/freeze.json" \
  --expected-original-sha256 "$ORIGINAL_FREEZE_SHA256" \
  --build-receipt "$SIDE/build-receipt.json" --operations 10000
# Record the printed comparison manifest hash outside the mutable side root.
python3 "$HARNESS/scripts/readme_mongodb_bench/run.py" \
  --root "$SIDE" --repo "$PRODUCT" --suite ycsb-single \
  --operations 10000 --attempt-label pair01-base-short \
  --expected-comparison-sha256 "$SIDE_FREEZE_SHA256"
```

Use a new attempt label for every planned slot, including failures; existing
attempt directories cannot be reused. Every command/Close receipt, loaded and
preclose/reopened full typed census, exact-value fixture, cleanup and failure
record retains the original lifecycle checks. A ten-minute work deadline raises
into the existing owned-process cleanup; bounded cleanup may extend past that
deadline. Partial startup registrations are retained. Failed DB directories
remain for diagnosis and a failed slot is never automatically retried.

A host-wide `/tmp/gomap-ycsb-qualification.lock` serializes these comparison
selectors across side roots. The measurement owner must also exclude other
workloads, builds, transfers and observers on that host. Compare pair members
on the same host and record counterbalanced order before collection. CPU count
is recorded; GOMAXPROCS remains unset as in the original recipe, with its runtime
value unobserved. Pre/post process snapshots and original process launch PIDs
are retained. These snapshots cannot establish peak/resident/retained memory,
allocation rates or pending/settled maintenance; collect those separately as
attribution evidence and report their absence from accepted timing cases.

Run the offline controls with:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_recipe.py test_comparison.py
```

When sharing a host with the single-case comparison selector, also hold its
`/tmp/gomap-ycsb-qualification.lock`. On Linux, wrap either idle driver with
`flock --nonblock --no-fork /tmp/gomap-ycsb-qualification.lock python3 ...`
while retaining the shared `--lane-lock` used by other measurement owners.
The no-fork form keeps both locks owned by the driver through its cleanup.
