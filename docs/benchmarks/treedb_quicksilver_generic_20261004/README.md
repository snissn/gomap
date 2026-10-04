# Generic Quicksilver final evidence construction

This directory contains provisional report machinery for #4983 under #4978.
The historical random4k report remains separate. No optimization is accepted by
this construction commit, and no final performance measurements are invented.

`assemble.py` uses the landed #4979/#4981 capture validator and the accepted
full baseline packet. It verifies actual raw hashes, receipt/build/native/loader
binding, frozen compiled project bytes, unchanged benchmark/capture code,
declared plan cells, exact retained argv and absolute built/loaded executable,
executed overrides, owned-value/reopen/request contracts,
and matched fixture/configuration before pairing observations. Assertions in
the reused validator remain enabled even with `python -O`.

```sh
python3 docs/benchmarks/treedb_quicksilver_generic_20261004/assemble.py \
  /absolute/quicksilver-authoritative-20261003 /absolute/report-output \
  --repo /absolute/gomap
```

That command emits `REPORT.md` and `RESULTS.json` with final columns `PENDING`.
The baseline packet SHA is pinned; copied baseline capture inputs are rehashed.
Mutable campaign amendments and later optimization diagnostics are not presented
as new baseline inputs. To consume final frozen captures, add one `--bundle`
for each copied capture directory and `--landed-final` with the coordinator's
landed main SHA. Candidate source receipts retain their original SHAs: compiled
project inputs and the entire TreeDB subtree must equal that frozen landed
source. Every mode/type/blob entry in `cmd/unified_bench`, `cmd/benchprof` and
the production capture producer/test must match the frozen baseline, except
the two explicit README transitions below. Earlier qualifying candidate captures
may supply final cells only for exact predeclared configurations after strict
binding to actually landed runtime, harness and compiled inputs. Their original
SHAs remain recorded; this grants no implicit reuse of other candidate captures.

The [candidate applicability receipt](DOC_REPAIR_APPLICABILITY.json) is pinned to tooling repair
`8229183f61d5d93e612e9adb79ace3514fddd50c`. The permitted README blob transitions
are from `6137db44b66e0323daba05ae891db44a8055c0fd`; both modes remain `100644`:

| Path | Frozen original blob | Frozen repaired blob |
|---|---|---|
| `cmd/benchprof/README.md` | `fc1ca832dc2f250002e4a6c6858bad977893f9d9` | `f6cd1c62a46c8a4b7a0d93befa86fb327d449ce1` |
| `cmd/unified_bench/README.md` | `309cf9c59c67967abba9256684ec67de8dc45753` | `3a40d2b73cc6fd5484b1ecc7f380223c4f797ea3` |

These describe the standalone cached-owned microprofile, whose helper is
`scripts/treedb_point_lookup_profile.sh`. That helper is outside the production
capture path. When its original hash is present in a source receipt, the assembler
checks it against the original captured head; the helper is not relabeled as the
producer of earlier data. Every final record includes `source_applicability`
with the original captured SHA, supplied landed SHA, exact README entries,
compiled-input count and original helper hash. Whole-TreeDB, all compiled-input,
production producer/test and all other harness entry checks remain strict.
Identical old/new README entries and the pinned old-to-new transition are
accepted; other documentation changes and reverse transitions fail closed.
This candidate receipt supplies no final landed-main authority.

`plans.json` freezes all 43 required cells. Scaling reuses the matching four-reader
held-out cell and adds 8/16/32/64 readers; 10M capacity uses holdout seed173.
No profile capture is accepted into these unprofiled cells. Partial or failed
captures remain pending, and `--finalize` fails until all cells pass. Completed
evidence still requires coordinator review, current CI/reviews and performance,
checkpoint, storage, noise, correctness and graph acceptance gates.

The bounded selfcheck reads the actual accepted baseline packet and original O1
source receipt to verify the documented repair; it does not
execute benchmark binaries or rewrite evidence. Run both commands:

```sh
python3 docs/benchmarks/treedb_quicksilver_generic_20261004/selfcheck.py /absolute/evidence
python3 -O docs/benchmarks/treedb_quicksilver_generic_20261004/selfcheck.py /absolute/evidence
```

Final capture receipts must use the existing wrapper's directory/file schema and
the same runtime environment and native library inventory as the baseline.
The coordinator supplies authority that `--landed-final` is landed main; the
assembler checks Git object equality without contacting GitHub or the runner.
