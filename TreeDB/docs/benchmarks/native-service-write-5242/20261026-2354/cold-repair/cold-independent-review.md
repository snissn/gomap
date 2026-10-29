# Independent cold Hybrid source review

Disposition: **ACCEPT, source component only.** No material correctness or ownership blocker found in the selected delta. This is not runtime, allocation-performance, original-public-fixture, writer-pressure, or product acceptance.

Reviewed canonical `consumer-canonical`, base HEAD `0a8323f7ca6f6128d53dfca43a408c7e443d39ab`, against HEAD for the three production paths and the new test. Verified SHA-256 bindings:

| Path | SHA-256 |
| --- | --- |
| `TreeDB/collections/hybrid_executor.go` | `54992c68aae2668d9d0fc62a9755e466a5214b29b83139b017f0c33ad0790c1e` |
| `TreeDB/collections/hybrid_vector_candidates.go` | `aad9db30162adf137b4ad06a911ba9b6801b78534a14fd0159632049b35b3827` |
| `TreeDB/collections/native_ordinary_columngraph_search.go` | `066f011f1e35a52b32175291b0ae7c47de1710042ee59fa75ee17391dd1655f4` |
| `TreeDB/documentservice/ordinary_columngraph_native_cold_hybrid_test.go` | `19f5e1ef1505e1919793e9e80d4d0834e52fdbb91af34f4d9de8e27708a51613` |
| `evidence/native-write-failure-repair1/cold-integrated-manifest.json` | `c9064576003d592fca5dc655cb95df0e21229059709b3600b440f957c2602b1e` |
| `evidence/native-columngraph-cold-hybrid-repair1/proof.md` | `0b232c9206fddb48782bd47135fb8f8b55f02280ab2f268effeaa244c51dd806` |

## Findings

- `hybrid_executor.go:1082` orders only the existing ordinary ColumnGraph acquisition before the default mixed-source goroutine phase. It uses the plan's original `nativeRead`, query options, context and Production stats. Native plan construction establishes the read view and cumulative scratch before setting this eligibility flag. Explicit TextFirst/VectorFirst and single-source cases retain their previous execution branches. Text candidate evaluation and actual vector traversal still overlap in the default mixed branch; even preparation failures allow text evaluation and retain its existing first-error precedence.
- The new handoff does not create another prepared owner, cache, capture or lease. Its embedded response/definition fields are borrowed headers, held within the already-live original read scope. The cumulative operation receipt admits the handoff's allocation before construction and remains through candidate join and final same-cut document fetch. Original constructor admission and temporary-lease cleanup are unchanged. This source review does not claim a compiler-level absence of additional escaping closure/control allocations or an exact per-operation allocation result.
- `native_ordinary_columngraph_search.go:451` preserves the original buffered validation, buffer reset/error cleanup, view authority, declared-filter restriction, prepared-owner RLock, closed/pack guard, query-capacity admission and traversal. A cache replacement or Close between preparation and use still fails closed under that lock. The handoff cannot bypass validation through an external caller: its sole constructing caller supplies the same original view and options subsequently used by traversal. Its lifetime does not extend the outer capture.
- The old search entry point delegates with a nil handoff and retains its original acquisition path. The mixed branch passes the result of one acquisition into traversal; it does not look up the cache a second time. Route/work generation, result conversion and output admission remain at their original sites. Acquisition counters are applied once at the existing post-traversal site; the existing error-path reporting behavior is preserved. Allow-set cutoff and source merge/filter/fetch logic are unchanged.
- The new public test is a meaningful cold-lifecycle discriminator: durable omitted-strategy ordinary ColumnGraph, 4096 distinct documents, Optimize, verified post-Optimize query, checked complete Close, new owner/service, then Hybrid as the first reopened query. It checks native/source identity, filtered owned document content, unique canonical IDs, meaningful text/vector/scalar work, no full-scan fallback, and subsequent cache retention and stable cold/warm rank on the same identity. The cache-warm inspection occurs after the successful first request. It does not independently prove concurrent scheduling, all cancellation/error combinations, hard-exit recovery, or exact acquisition-counter multiplicity.

## Limits

Read-only source inspection only; no Go, build, formatting, tests, profiles, runtime or GitHub operations were performed. The supplied manifest records original test RED log `4c722de34b40649c98ae9b1c67769c47a5d18d123df7d65187a57161f090b8f8` and 4.22052713483572 seconds; that is root evidence, not a newly executed or independently validated result here. Root owns current GREEN, race, original 4096 lifecycle and public/performance acceptance. Separate concurrent-writer pressure work is outside this review.
