# Independent ordinary/main integration review

**ACCEPT** the frozen source integration for root commit/push and fresh current-head CI. No integration blocker found. This does not establish PR merge readiness, performance or M0 acceptance.

`INTERNAL_SUBAGENT_REVIEW` by `/root/ordinary_review`, explicitly routed `gpt-6.1-sol/high`; `candidate_edits_by_reviewer:false`. Manual hosted review cannot pin the required exact model and was not requested.

The exact candidate is staged tree `dd50b7ea3ca04193f4fa00decf15cbc98edb68d1`, with intended parents `a17febafb5353897c59a4e17f992e34c47a2585b` and landed main `637129789378c33027a234997b6cdd57f20fd0fe`. The source manifest is `8f32bf3ea51c2b082f5c9cfce2b8066e6202ae89ea6e696ef44899ffb5d09476` (10,258 files); retained exact staged patch SHA256 `34831b024edf0b6714ffde81b4f5b9238931fa1f95aef87f8534fc0ed8dfac20`.

I independently verified the current HEAD/MERGE_HEAD, every staged index mode/object against the frozen tree, and every manifest path/hash against both staged Git blobs and worktree bytes. There are no unresolved index stages or byte mismatches. The actual cached binary diff equals the retained patch exactly. The final read-only status still has precisely the expected 15 staged paths.

All 15 differences from a17 are tooling/documentation. Fourteen are byte-for-byte landed main; only `.github/ci/ci_impact.json` differs from main. Every other candidate path is preserved. No production `.go` file, TreeDB/HashDB path, go.mod/go.sum, scripts path, original raw/PRIMARY/M0 harness or original execution workflow changes. TreeDB and HashDB tree identities and original harness/workflow blobs are retained in the JSON report.

The generated inventory independently equals the exact main policy with only these source-required substitutions: candidate go.mod/go.sum harness descriptors; the complete candidate treedb-tests workflow metadata; and the new discovery digest. All other fields, reviewed owners, rules, members and dynamic-consumer definitions equal main. It inventories 17 workflows and 59 members. The updated pure-test cardinalities and documentation agree. New manual diagnostics retain fixed historical product references and diagnostic-only scope; importing them does not redirect ordinary candidate CI or certify the integrated product.

I also invoked the existing generator's read-only `check_source_contracts` over frozen-tree inputs with Python bytecode disabled. Actual workflow commands, checkout contexts, hashes, harness descriptors/modes and discovery digest all validate. The independently recomputed discovery digest is `f7f11621d2e7166736c3dd7abce082655dabae46ec4aa62c6844caa6f9a3caed`. I did not invoke its snapshot/write-tree or refresh functions. Root's retained 21 pure CI controls, generated inventory check and diff check all pass; their raw logs were inspected.

This is an integration/provenance review, reusing exact landed source identity rather than claiming another complete diagnostic-harness architecture review. There is no new Go build, test-body execution, benchmark, performance/M0 result, hosted current-head CI result, diagnostic dispatch authorization, native integration acceptance or PR merge-readiness finding. No native source was inspected. Root must commit the reviewed tree unchanged and bind the resulting commit separately. Reviewer/source/runtime lease released.
