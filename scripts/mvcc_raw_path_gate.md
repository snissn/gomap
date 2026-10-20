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

## Public durable replacement cost and runtime attribution

`scripts/mvcc_primary_cost_gate.py` always rebuilds the exact workflow-supplied
base and head with one maintained fixture,
`TreeDB/mvcc/durable_replacement_cost_test.go`. It does not substitute a historical
control for the ordinary base/head comparison. The 128 timed `CommitAt(100, ...,
CommitDurable)` replacements, seed commits, `ProfileNoWALFast`, disabled side
stores/background checkpoint, pointer threshold 4096 and timer boundaries retain
the original public PRIMARY workload. Setup, lifecycle verification and database
close remain outside timing; the independent raw-path gate is unchanged.

Each arm's actual source selects one explicit compiled setup: the maintained
PRIMARY attachment implementation enables `IndexPrimaryDirectory`, whereas the
DATA retirement validator selects DATA without referring to the retired option.
Missing, ambiguous or unrecognized source capabilities fail closed, with no
runtime route retry. `bindings.json` records each capability path/hash and each
generated arm fixture hash. Both arms run public lifecycle controls before any
timed samples, and every sample proves its actual public snapshot root namespace,
PRIMARY-file presence/absence, resolved profile, mode and disabled redo log.
The DATA lifecycle additionally decodes both recovery slots and their exact
parent records, checks both slots advance, and verifies values and authority
after close/reopen without PRIMARY files. All source/fixture identities are
checked again after measurement.

Thus PRIMARY/DATA retirement and future DATA/DATA comparisons use the same public
write/durability contract and strict cost limits. `generation_advances/op` remains
raw telemetry: DATA generation advancement is an architectural difference,
not a PRIMARY activation failure. Every PRIMARY lifecycle retains its original
zero-generation replacement assertion; timed candidate PRIMARY samples also
require zero advancement. Both arm activation gates are mandatory. Historical
PRIMARY controls such as `7385e231632ef30189b12c20df12a9f4705c9f50` provide
diagnostic provenance only; they do not replace the supplied accepted main base
or establish an architecture verdict. Both measured binaries must be rebuilt
under the recorded common toolchain; saved binaries from another toolchain do
not constitute this comparison.

The separate `scripts/mvcc_primary_cost_gate.py` comparison retains every raw
sample, median, gate and `measurement_pass`. Raw timing remains limited to +5%
median paired growth, and raw bytes/allocations permit no median growth. Timing
and bytes acceptance may report `EQUIVALENT_NON_ATTRIBUTABLE` only for identical
independently bound runtime and input identities. A different executable, loader,
library or input retains the original strict timing and bytes gates, with no
numerical allowance. This attribution does not turn a raw failure into a measured
PASS or establish a speedup. Allocation count, source-bound route activation, both
public lifecycle controls, quiet-host checks, ordered complete samples and all
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
