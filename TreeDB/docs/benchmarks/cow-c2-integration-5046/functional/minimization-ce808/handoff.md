# ce808 minimization functional handoff

Exact candidate: `ce808d9d523e6ec7c32fe4c773e87d7ed8dcc2a1`; Git tree `3a5ebe8c320723bfd4a3da9aa285d9c18f889b1e`; full6462-file source tree SHA256 `c50cf00ef056bd1cca78e80c02cf5df79deb961915290d161d139b5aa98a0d1f`.

Immutable Linux185 source: `/mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-minimization-ce808`. Full remote packet including Git archive: `/mnt/fast4tb/gomap-cow-execution-o2nauuzm/artifacts/c2/minimization-ce808`. Local packet keeps scripts, identities, raw JSON, receipts and checks; no archive/build binary.

- Initial meaningful new pressure/owner/canonical-finalizer tests: 10 test/subcase pass events +3 packages, zero failures.
- Selected affected seven-package normal:565 test/subcase pass events +7 packages, zero failures.
- Race:564 test/subcase pass events +7 packages, zero failures.
- Safe build:565 test/subcase pass events +7 packages, zero failures.
- Vet:exit0; raw log retained.

These are selected affected gates, not the entire repository. Exact runner commands and regular expressions are in `run-initial.sh` and `run-gates.sh`; the actual changed durability-profile inventory is explicitly included. Separate package events are not counted as tests. Before and after all gates, every frozen relative path/hash was verified, including extra-file detection:6462 files, zero mismatch. Actual Go environment and script/source/raw/receipt SHA256 bindings are retained.

New source removes eager COW decoder/page construction, omits disk cursors only for an exact retained empty normal index leaf, reuses preadmitted prediction backing and contiguous file/dictionary ID scratch, skips journal dependency custody only in the explicitly WAL-off cached helper, and chooses bounded native owners for actual Snappy/LZ4 frames. The planner/RID cache, actual frame identity, CRC/length validation, live physical/dictionary pins, pre-visibility flush, retirement and sync/checkpoint boundaries remain authoritative. Fixed finite admission still refuses before allocation. New empty/nonempty old/current cut and budget-pressure tests supplement actual public pointer/dictionary lifetime, sync, crash, power-loss, close and maintenance tests in the selected gates.

The benchmark fixtures remain byte-identical to b9. Source applicability and inherited diagnostic close-race limitation are in `source-applicability.md` and `retained-source-applicability.json`. Old frozen runtime/cost packets are preserved. No replacement benchmark values or material-cost acceptance is claimed here: coordinator must recollect and disposition actual costs on this candidate.

All writer Go/test jobs ended before runner release; final process-name-only probe returned an empty list. Linux185 is released to coordinator for exclusive matched cost collection. Runtime/harness remains frozen at ce808 during that collection.
