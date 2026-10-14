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
