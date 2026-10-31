# MVCC raw-path gate executable retention

After the original paired measurements and checker, the raw-path gate retains
  all six actual measured executables under `measured-binaries/`, on passing
  and failing gates. `measured-binaries.json` binds their SHA-256, size, mode,
  source commit/tree, package, exact build argv and explicit `GOWORK`, plus the
  recorded environment and initial/checker digest evidence. Copies must match
  both the premeasurement digests and any available checker digest evidence.
  This is executable retention, not a complete compiler/toolchain closure or
  new performance evidence. A retention failure leaves an explicit diagnostic
  log and preserves the original checker exit status; an absent manifest or
  partial directory does not establish complete executable retention. Existing
  iterator diagnostics and all original gate thresholds remain unchanged.

Run `python3 -B .github/scripts/test_mvcc_raw_path_equivalence.py` for the
no-Go digest, retention and exit-status controls. CI uploads the gate output
directory, including these exact binaries, with its existing artifact step.

## Public PRIMARY runtime attribution

The separate `scripts/mvcc_primary_cost_gate.py` comparison retains every raw
sample, median, gate and `measurement_pass`. Raw timing remains limited to +5%
median paired growth, and raw bytes/allocations permit no median growth. Timing
and bytes acceptance may report `EQUIVALENT_NON_ATTRIBUTABLE` only for identical
independently bound runtime and input identities. A different executable, loader,
library or input retains the original strict timing and bytes gates, with no
numerical allowance. This attribution does not turn a raw failure into a measured
PASS or establish a speedup. Allocation count, candidate PRIMARY activation, the
public lifecycle control, quiet-host checks, ordered complete samples and all
source/input/binary drift checks remain mandatory even for identical runtimes.

Before and after measurement, `runtime-inputs-before.json` and
`runtime-inputs-after.json` bind independently hashed actual executable contents
and sizes, the ELF interpreter, and every filesystem library resolved by that
interpreter's `--list` under the measurement environment. Resolved loader/library
paths, SHA-256, sizes and loader enumeration order must remain identical. The kernel VDSO has no filesystem
input and is excluded. Missing/unresolved inputs or drift fail closed; `LD_AUDIT`
is unsupported because loader enumeration does not establish its complete inputs.
Static ELF executables are bound explicitly with no interpreter or libraries only
when valid ELF load headers contain neither INTERP nor DYNAMIC. Their runtime is
contained in the hashed executable; all invocation/input bindings still apply.
Malformed headers or a dynamic segment without an interpreter fail closed.
This Linux gate requires `readelf` and, for dynamic executables, their loader.

The binding also covers the fixed benchmark argv, effective environment digest
(values are not published), working directory, harness and runner. The workload
uses compiled fixed data and fresh temporary databases, with no external dataset
or configuration. The source commits remain distinct and recorded. Executable
identity alone or caller-supplied identity claims never establish equivalence.
The separate final `accepted` and `acceptance_gates` fields report disposition;
`gates.timing`, `gates.bytes` and `measurement_pass` preserve the actual measured
result. `timing_disposition` and `bytes_disposition` distinguish measured PASS,
FAIL and equivalent-runtime attribution. Identical runtime bindings establish
that timing variation cannot be attributed to candidate code; they do not make
host scheduling or cache/process state immutable. The original timing-only
failure at PR #5149 head `ab2bbeb5d` (job 118411039651, artifact 12235870827)
measured +5.258179% and remains raw FAIL. Its independently hashed actual
executables, loader/libraries and before/after input bindings were identical.
This case is resolved by explicit attribution under #5150, without a rerun or
threshold change; the earlier #5152 bytes-only evidence remains preserved.

Run `python3 -B .github/scripts/test_mvcc_primary_cost_gate.py` for the no-Go
measurement, attribution, actual ELF resolution and drift/refusal controls.

## Bounded PRIMARY quiet-window acquisition (#5185)

After the unchanged candidate public lifecycle and before any timed PRIMARY
sample, the runner acquires the **first** qualifying five-second process-CPU
window within a fixed 60-second setup allowance. Each window uses the existing
`ProcessObservation`: the current [external-cpu-v2 policy](../docs/diagnostics/quiet-host-policy.md)
rejects **10% per process or 50% observed aggregate** of one logical CPU, warns
at the historical 2% threshold, and retains the original
0.25-second sampling, ancestry/descendant handling and exited-process accounting.
There are no new process or kernel-thread exclusions. A failed window starts
another observation only when at least five seconds remain; there is no separate
cooldown or unconditional delay. An incomplete window, or one completing after
the deadline, cannot qualify. Synchronous process inspection/artifact writes can
return after the deadline; a final post-write clock check prevents this from
authorizing timing or another window. A late write records `exhausted` and the
quiet window's rejected authorization in one final receipt write; that failure
receipt does not restart acquisition or recursively check/rewrite itself.
Exhaustion is a setup refusal with zero timed samples, not a measured gate result.

`preflight-acquisition.json` records the cap, unchanged predicate, ordered window
results, observation filenames and explicit `acquired`/`exhausted` disposition.
Every busy window remains retained: the first raw observation is
`preflight-processes.jsonl`, subsequent files are numbered, and the acquisition
receipt contains every complete classifier result. `preflight.json` preserves
the original final-window schema; the acquisition receipt distinguishes a
qualifying final window from exhaustion. Already-quiet setup starts timing after
its first complete window. No public lifecycle or timed sample is repeated to
obtain acceptance. Once timing begins, all eight alternating pairs of 128 durable
replacements, measured process observations, +5% paired-time/no-byte-or-object-growth
rules, source/runtime bindings and drift checks remain unchanged.

This is an explicit **setup acquisition protocol change**, not a measured
performance improvement or a scientific inference. It requires source/protocol
review and ordinary harness qualification before retained collection under the
updated runner; expensive retained collection uses a reviewed, landed harness
and frozen product/runner/harness/runtime identities. Old single-window refusals,
failed acquisition windows and measured failures remain source-bound evidence;
none is retroactively accepted, and a passing refresh does not identify or repair
the cause of prior external CPU activity. In particular, the #5168 head
`9a44fd73cbffb9e89c62f7e12bd7539eec66c2a8` job `118762421078`
refusal retains zero PRIMARY samples and no paired measurement verdict.

The existing no-Go Python command above also exercises busy-then-quiet,
already-quiet, deadline/exhaustion and original measured-acceptance controls.
The exhaustion integration controls stub setup commands and prove the measured
observer and benchmark are never launched, including when either final artifact
write crosses the cap after a quiet observation; they run no Go/build/benchmark.

### Isolated public PRIMARY cost boundary

`mvcc_primary_cost_gate.py` copies the maintained
`TreeDB/mvcc/primary_generation_routing_v6_test.go` from the candidate
checkout into the baseline build with an overlay; the candidate build uses
the fixture directly from that checkout. Both builds therefore use the same
fixture bytes.
The fixture explicitly disables periodic background index vacuum and makes its
already-disabled periodic/idle checkpoint and WAL-size checkpoint triggers
explicit. Pruning retains the default background scheduling; it is not moved
onto foreground commits. Its actual public
`CommitAt(..., CommitDurable)` publication, PRIMARY activation witness, DATA
fixture, timer and clean-reopen lifecycle remain the same. Setup and configuration
assertions precede the timer. Go allocation counters are process-global, so
the incidental vacuum ticker must not enter this narrow boundary. The hypothesis
is a 128-byte ticker allocation crossing the timer in a 128-iteration run; this
does not prove all remaining process-global allocation noise is removed.
This is a harness isolation change, not a storage optimization or a relaxation of
the gate: eight alternating pairs, 128 iterations, the 5% paired timing limit,
no median bytes/allocation growth, identity binding and quiet-host qualification
remain unchanged. Old failed/noisy measurements remain failed retained evidence;
new-harness rows cannot be silently pooled with them. Short diagnostics do not
establish that disabling workers resolves every observed byte difference.
