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

## Public PRIMARY bytes attribution

The separate `scripts/mvcc_primary_cost_gate.py` comparison retains every raw
sample, median, gate and `measurement_pass`. Its no-growth bytes measurement is
strict: a changed executable receives no numerical allowance. When raw bytes
alone fail, final bytes acceptance may instead report
`EQUIVALENT_NON_ATTRIBUTABLE` for identical bound runtime and input identities.
This does not turn the raw result into a measured PASS. Timing, allocation count,
candidate PRIMARY activation, the public lifecycle control, quiet-host checks,
ordered complete samples and all source/input/binary drift checks still apply.

Before and after measurement, `runtime-inputs-before.json` and
`runtime-inputs-after.json` bind independently hashed actual executable contents
and sizes, the ELF interpreter, and every filesystem library resolved by that
interpreter's `--list` under the measurement environment. Resolved loader/library
paths, SHA-256 and sizes must remain identical. The kernel VDSO has no filesystem
input and is excluded. Missing/unresolved inputs or drift fail closed; `LD_AUDIT`
is unsupported because loader enumeration does not establish its complete inputs.
This Linux gate requires `readelf` and the executable's dynamic loader.

The binding also covers the fixed benchmark argv, effective environment digest
(values are not published), working directory, harness and runner. The workload
uses compiled fixed data and fresh temporary databases, with no external dataset
or configuration. The source commits remain distinct and recorded. Executable
identity alone or caller-supplied identity claims never establish equivalence.
The separate final `accepted` and `acceptance_gates` fields report disposition;
`gates.bytes` and `measurement_pass` preserve the actual measured result.

Run `python3 -B .github/scripts/test_mvcc_primary_cost_gate.py` for the no-Go
measurement, attribution, actual ELF resolution and drift/refusal controls.
