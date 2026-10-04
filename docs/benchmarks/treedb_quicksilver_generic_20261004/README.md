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
source. Documentation changes outside TreeDB, the protected harness/capture
contract and compiled inputs do not invalidate this equality. Earlier candidate
qualification captures are not substitutes for fresh final frozen captures.

`plans.json` freezes all 43 required cells. Scaling reuses the matching four-reader
held-out cell and adds 8/16/32/64 readers; 10M capacity uses holdout seed173.
No profile capture is accepted into these unprofiled cells. Partial or failed
captures remain pending, and `--finalize` fails until all cells pass. Completed
evidence still requires coordinator review, current CI/reviews and performance,
checkpoint, storage, noise, correctness and graph acceptance gates.

The bounded selfcheck reads the actual accepted baseline packet; it does not
execute benchmark binaries or rewrite evidence. Run both commands:

```sh
python3 docs/benchmarks/treedb_quicksilver_generic_20261004/selfcheck.py /absolute/evidence
python3 -O docs/benchmarks/treedb_quicksilver_generic_20261004/selfcheck.py /absolute/evidence
```

Final capture receipts must use the existing wrapper's directory/file schema and
the same runtime environment and native library inventory as the baseline.
The coordinator supplies authority that `--landed-final` is landed main; the
assembler checks Git object equality without contacting GitHub or the runner.
