Independent focused-controls verdict: **ACCEPT**, scoped to correction03 manifest `d6a090b25f8df158f0f3e830c009cabe6959b65339f9528eedbae63e8e036798` (10,389 files; archive `3f65af587c0df94cd89fa82c98196d5d1da997782ca882b3ca8dfb279e97effa`). The existing source ACCEPT is preserved; this is its additive control-evidence review.

Reviewer `/root/ordinary_review`, explicitly routed `gpt-6.1-sol/high`, internal independent review. Hosted manual review cannot pin the required model. No candidate edits, child agents, Go commands or runtime executed by reviewer.

All eleven authorized calls passed. Independently rehashed all 98 payloads; checked exact proposal/argv/environment/grant, pinned Go/compiler hashes, selected source-file bindings, complete per-call source manifests, raw top/child RUN-to-PASS inventories and process receipts. No skipped/failed cases, race reports, timeouts, drift, unexpected names, residual process intervention or unjoined groups. Recorded calls are sequential.

| Evidence | Top-level PASS | Child PASS |
|---|---:|---:|
| Five new normal calls | 10 | 46 |
| Prior normal freelist, explicitly reused | 18 | 79 |
| Six new race calls | 28 | 125 |
| Normal including reuse | 28 | 125 |

Normal freelist remains attributed to correction02 manifest `d0cba17d5cef5666f2856cc9bf0278b0ec7eedcd9946b2aadf5ffaac344d3247`, result `3a234e01fce895f44163563db1cb13af3321f8cde8d72d760d37387857a0030b`. Its copied receipts equal the originals. Complete manifest comparison proves correction03 changes only `TreeDB/internal/rootpublication/selected_manifest_ledger_test.go`; all freelist and production bytes are unchanged. This was not a rerun.

One reporting defect is confirmed: the final runner summary uses `len(results)==12`, while authorization, launch assertion and actual schedule all correctly use eleven. That expression alone yields the retained false `all_authorized_pass` flag. Per-call checks remain fail closed and all eleven PASS. The separately derived receipt correctly reconciles them. Original runner/summary and previous failure packets remain preserved; no rerun or weakened gate is required.

Bindings:

- Artifact manifest: `0b06aebf6e9367589f5c95e0244afbbd803a0fa833003ec1f77b40d46351bba6` (98/98 match).
- Runner: `42529cf1b87238af834819c32201638636cf6d203dc4b9e9cebff8ba43253337`.
- Raw summary: `7e8fef1752925b98929978b451985af5783e63b4f7dd74021e8055b96e395347`.
- Derived receipt: `f07d70cd961207b5ef10eefe81237dd0ed12f65100426a238148e794007cbd39`.
- Root full verifier: `63ca0d127819125998f3d57987fdc139c3dabd2188b89641c186ebe9e156201d`.

These controls cover the selected constructors, admission/refusal/cancellation, private allocator primitives, finite cleanup and ledger arithmetic/holder/dedup predicates. DB controls exercise real original captures with synthetic holder geometry; they do not perform genuine native publication. No actual33 run on this source, complete real selected retained-union total, aggregate F/Finish bound, numeric allowance, Q32 fit, performance, platform or publication qualification follows. Index was not bound at launch. **Q32 RED/ACK0; no allowance chosen.**

Read-only review lease released.
