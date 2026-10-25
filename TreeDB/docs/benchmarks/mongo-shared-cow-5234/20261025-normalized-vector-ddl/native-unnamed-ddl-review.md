# Native unnamed-vector DDL repair review

**ACCEPT** the two-file working delta from `2235461290dc60e150267ac8abe9ec3ae651480f` identified below. Source review only: no runtime/build/test, source mutation or delegation. Broader collections/CI/promotion remains root-owned.

| Reviewed file | Git blob | SHA256 |
| --- | --- | --- |
| `TreeDB/collections/native_index_ddl.go` | `b6d56e2a5bcec593f42b0e88c7f3af862a46954a` | `0f84cf84c1cace6aee83640587b7e8f9b305faa66a798da21ef782267468ab86` |
| `TreeDB/collections/native_hnsw_public_test.go` | `a2da30bd80be10137b139886973e4245bd531d99` | `813be73dac4aec899ed7ed61e0dcd041fb11bb09e9b24662bfb2f51c6ebf48ed` |

## Correctness

`mutateIndexFamilies` now retains the normalized definition returned by `addVectorIndexToCollectionMeta` (`native_index_ddl.go:274–279`) and uses it for representation eligibility and the new graph-domain name (`:454–463`). The existing helper (`api.go:26656`) normalizes the complete proposed metadata, then explicitly resolves the generated default name when the request name is empty. Its success guarantees this definition is present in the proposed metadata. The old ignored-error lookup by the original request name could return a zero definition, creating `vector-index/` while the catalog declared the normalized name. The corrected source uses one canonical helper result rather than adding another normalization/lookup authority.

The subsequent `nativeJSONMeta` revalidation is idempotent for the already-normalized vector definition; no post-helper transformation changes its name or representation. Named requests therefore retain the same definition as the old successful lookup. Unnamed normalized-representation requests now also receive the intended existing nonempty-primary eligibility guard. Input/schema errors still return before this result is used. No publication/dependency/WAL/custody, temporary admission, runtime preparation/rollback, mutation/manager lock or assigned/installed outcome flow changes.

The fixture retains all twelve original profile×format×declared/later-create cases and adds six unnamed later-create cases (three profiles×JSON/BSON), for eighteen total. It checks both presence of the canonical graph domain and absence of the empty-name domain before proceeding through the same insert/build/search/save/checkpoint/reopen/old-cut/replacement/stale/drop/recreate body. This directly addresses the omission rather than merely renaming an assertion. No existing lifetime/refusal checks were relaxed.

Retained old-production red is the intended `normalized=false empty=true` failure (0.190s). The complete eighteen-case green passes 8.250s; changed-risk race passes 11.553s. This review read those terminal receipts; it did not run them. Raw SHA256:

- `native-unnamed-ddl-red1.log`: `dfc353293b584073bd2298e4bcaebeda2fb8ed014c77b5c74ba3c425b3d509fc`
- `native-unnamed-ddl-green1.log`: `ab6b38e08273843da053db46fa9663357b567e758a6ec3e7aa6e412f6c8fdde2`
- `native-unnamed-ddl-race1.log`: `b6fe1be8e7e03ea542e8d336d1d59ad17e60d123790804eacf329018ae84f655`

No material residual finding in this bounded delta.

## Applicability of the retained d040 qualification

**ACCEPT source-level applicability of the original 106 d040 observations to the unaffected measured workloads; a replacement unchanged-cost campaign is not required by this repair.** This is not an assertion that a newly compiled binary was measured, or a relabeling of old receipts. The observed candidate remains `d0407930ef138915306e1a89c2c80522174c50ad`, binary SHA256 `52bc3afbc46088218e0e8f8d0b59598b434e69d343d9c861d52cb5f1553a9455`; the baseline and all original raw/control/disposition records remain unchanged.

The measurement spec invalidates *affected* evidence when runtime/harness code changes. All seven recorded harness/spec/driver blobs were independently checked against the d040 manifest and remain identical. The source-level affected-path analysis is:

| Retained workload | Relationship to this repair |
| --- | --- |
| Primary 64 indexed Mongo updates | Scalar `n`/`email` setup and updates; never creates vector indexes and never executes either changed vector branch. |
| Held-reader resource 24 updates | Same scalar fixture and vector-free churn; held-read, ACK, maintenance, idle and Close paths unchanged. |
| Ordinary vector 18 | Shared fixture explicitly creates named `embedding` before every benchmark timer. For this named request the helper-returned normalized definition is exactly what the old successful name lookup returned, giving the same metadata, eligibility decision and domain. Timed Build/Save/hot Load, physical Close/reopen Load and warmed direct idx.Search callgraphs are unchanged. The saved/reopened catalog is semantically the same; reopen does not rerun vector DDL. |

`git diff d040..2235461290dc60e150267ac8abe9ec3ae651480f` contains publication artifacts only; the two-file repair is the sole additional runtime/test delta examined here. The changed cold unnamed-DDL behavior is outside these retained workloads and is covered by the new correctness receipts, without pretending it received performance qualification. Source equivalence does not prove identical compiler layout or byte-for-byte binary performance; it is a bounded applicability decision under the affected-evidence rule. It grants no numerical waiver, no new performance claim for unnamed DDL, and no inheritance of evidence by the separate service/hybrid/default-adoption work. Keep this review and final committed blob binding adjacent to any new-head promotion explanation, with the original d040 provenance visible.
