# Independent maintenance harness refresh review

**SOURCE ACCEPT / READY for final committed harness binding.** Accepted harness base `25ebe4d8c775a10274c360f2ff80aa49b5601599`, tree `de8bae5a9e5f299a1702815b6f3eaf9fcea4936c`. Exactly6 dirty paths change only the fixed candidate5027→2588, outputprefix5027d→2588c and displaylabel. Reversing those literals reproduces each accepted base blob exactly. The10251-file manifest matches the root freeze; full patch SHA256 `96d45cffa19065da472da0e2cd8689ff356b2d77be941060cc1137af9e865987`.

Selectors,14-call schedule, metric/error/fixed-work controls, compilation, quietness threshold/sampling, caps, process-group kill/join, source/binary verification and manual-only pinned ephemeral workflow remain unchanged. Source review acceptance does not authorize dispatch. Root must bind the final harness commit before execution.

Reviewer executed only `python3 -B .github/scripts/mvcc_maintenance_test.py`: **7 tests PASS**, plus the parser positive/six negative controls. Exact argv, stdout, stderr and10251-file before/after manifests are retained in `harness-2588c-pure-controls/`; source is unchanged. No Go, benchmark, fetch or dispatch was executed.

A new14-call packet must identify product `2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49` and its original binaries. Prior5027d maintenance evidence cannot qualify this candidate. There is no successor Get timing claim or current cost acceptance.
