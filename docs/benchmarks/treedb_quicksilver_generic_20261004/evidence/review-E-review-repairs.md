# Independent E review-repair assessment

**Disposition: ACCEPT_REVIEW_REPAIRS.** No remaining actionable finding in the reviewed validator, plan, report, digest-label, or test-only allowlist repairs. This accepts the working repair implementation and exact reassembled evidence below. Root-owned publication applicability metadata is still to be appended; current-head CI, hosted review, publication and merge remain separate gates.

Reviewed the unpublished diff over `57d5bebdc6d78cbb15e11ac7142a1e34fce2c681` in `/private/tmp/gomap-qs-4983-evidence`. Root `AGENTS.md` was read; there is no `TreeDB/AGENTS.md` in this checkout. The implementation diff contains only the benchmark documents/validator/selfcheck and one existing naming-test allowlist entry. No production, benchmark producer, runtime or workload change is present. `git diff --check` passes.

## Exact examined identities

| File | SHA256 |
|---|---|
| `assemble.py` | `a7b850cabb1707d09b787b22ebf226d0b50e9058e75b014c992bdae397d6a7c1` |
| `selfcheck.py` | `54696e1d381b2f55510d1c0ee20b13a0a01ad122f0a33f2bb68b699f9fb6bb23` |
| `plans.json` | `0e02daae758d39ee282c7fb9ddc7dad096151885d5ddd86cf52673ef239326ab` |
| `RESULTS.json` | `51f351db11b8872000b0503ab255a34bb1c9952e001e28a2a0bd1c349f0e3268` |
| `REPORT.md` | `f38c1008f7430fdf2514a18940a390cdd3e6b7726689fbcdb9f90dc6a13e3b2e` |
| `FINAL_FINDINGS.md` | `f9b1661eae69b965dd89896204168b60f7f9e67b645368027581cdff403ddeb2` |
| `README.md` | `e7fbd8fb9d0c4e732818efc9e854977b5d19a610f4baab1895776e624465b36d` |
| `TreeDB/collections/typed_storage_naming_test.go` | `c6bd4714458759d74ad5554e29cef0522a84b394c56f7d5ed23282e91980ed3f` |
| Assessment before root's applicability addendum | `3b26d0d6378175bc8f2f2fdc1bbc81f67e98e63c98179eeefb979eda69c66339` |

The whole observed working diff hashes to `1c4ff6640fd33f831049de61a1504225a551160b962ed38336dea837751560ba`. That identity includes root's README clarification and precedes root's planned metadata-only addendum. Supplemental independently computed comparison/pin identities are retained in `E-review-repairs-independent-checks.json`.

## Findings and repair checks

The complete sorted project-path inventory is now pinned by captured head using an accepted count plus SHA256 of compact JSON of sorted paths. Independent recomputation matches all three original receipts: baseline `c6c9aa07…` has 2120 inputs, O1 `63794357…` has 2130, and composed final `87eb3445…` has 2131; each includes 133 paths outside `TreeDB/`. This is a trusted campaign constant, not a new receipt-declared authority. Both receipt maps can no longer omit a dependency together, add a path, or replace a path while preserving count. Subsequent checks still verify every expected value hash against captured and landed Git blobs, whole TreeDB equality and the protected harness including only the two explicit README transitions.

During this narrow review, the coordinator raised the remaining possibility of an arbitrary landed head adding a compiled file outside the captured inventory. Confirmed: checking only old paths would miss that addition. The final shared guard now pins the exact accepted source/landed pairs: `c6c9aa07…` to `6137db44…`, `63794357…` to `8229183f…`, and `87eb3445…` to `17712b9c…`. Unknown targets fail before the first Git lookup. The bounded check substitutes a Git function that fails if invoked and confirms rejection for all three unaccepted targets. This closes the gap without a new enumeration framework. Current campaign source authority remains literal `17712b9cfcef2b90516419a34ccf3d464984d473`, never the publication head.

The shared `accepted_baseline` helper hashes the packet against `BASE_PACKET` before parsing its fields, then checks the collector identity before either caller uses the returned packet. Copied packet-byte and collector mutations are rejected. Explicit `require` and optimize=0 compilation preserve those checks under `-O`. The missing-RSS match now fails with a clear `ValueError` identifying the relevant `stderr.log`; successful RSS extraction is otherwise unchanged.

The four canonical 1% working-set labels now name `ws1-s24`. All 43 semantic benchmark configurations remain exact unchanged. Complete reports omit the obsolete PENDING sentence and empty missing-cells summary; incomplete-report handling remains conditional. `final_findings_sha256` authenticates the unchanged FINAL_FINDINGS bytes, while `report_sha256` independently matches the actual regenerated REPORT.

The publication README clarification accurately documents the fixed campaign inventory and accepted source/landed pins, including the final frozen product SHA. It changes no protected harness README or executable input.

The one allowlist entry uses the existing `typedStorageLegacyCompatibility` classification for retained public configuration/Stats names. Independently scanning the repaired RESULTS with the actual naming pattern confirms 559 matching lines and 559 occurrences. The native validation receipt matches the current test-file hash; retained log hashes match actual bytes. The naming test has one top-level PASS and the unchanged vector test has three top-level PASS observations. The naming log exercised original RESULTS; its reuse applies because the only JSON changes are labels and validator/plan hashes, with the relevant field occurrences exact unchanged. The native vector repeats do not erase or explain the retained original Ubuntu CI failure and do not replace new exact-head CI.

## Exact evidence reuse

Latest strict assembly is retained at `E-landed-identity-reassembly-me3__ym4`, including exact command, stdout/stderr, normal and `-O` selfcheck results and delta check. Both selfchecks have rc0/empty stderr. Strict assembly reports COMPLETE_EVIDENCE, 43 required, zero remaining; stderr is empty. Independently verified that its RESULTS and REPORT equal the working publication copies byte for byte.

An independent recursive comparison against original `final-assembly-complete/RESULTS.json` (`97a04839a57e523992087c772de3ed6fe0dcc1f8a2d138799eaba18c7ba7116c`) finds exactly six JSON leaf changes: four canonical plan labels and the assembler/plan hash bindings. All final records, baseline records, matched pairs, original baseline hash map, metrics, workload configuration, source applicability, receipt projections and original raw identities remain exactly equal. All 492 map entries remain present; the 490 other hash bindings are unchanged, and the two updated bindings match actual source bytes.

Therefore `review-E-final43-numeric.md` (`15e786e7e3eb343ec5133c9998900dd398e346085e796f04720a510619f2fa4c`) remains applicable to its previously audited 492 raw identities, 43-cell numerics and source/native/profile proof, with the two explicitly replaced publication-tool hashes authenticated here. FINAL_FINDINGS remains byte exact. No original full raw audit, native campaign, remote toolchain rehash, broad Go test run, GitHub write, implementation edit, commit or push was performed by this reviewer.

Root will update the assessment's current assembler/result identities and append a transparent publication applicability addendum binding these reviewed bytes. Its existing historical format applicability block and original reviews must remain interpretable as historical evidence; they are not authentication of the new publication hashes. This known metadata follow-up is outside the exact assessment identity accepted above and should receive a small consistency check after it is concrete.
