# Template-mode preflight review

**DISMISS the claimed late-refusal/storage-effects defect under existing public normalization.** No production or test change is required. The coordinator chose to preserve that policy rather than add COW-only refusal of caller template requests.

I read live thread `PRRT_kwDOIdn7C86pY8UV` at [discussion_r4193380442](https://github.com/snissn/gomap/pull/5069#discussion_r4193380442), observed unresolved. Its exact causal claim is: “The later `caching.Open` call receives the actual template mode and rejects it.” That path is contradicted by the inspected `61fbd36655a9a8641f1cacee013c8f2bf2dc1553` source.

`TreeDB/public.go:567–576` defines forceTemplateCompressionOff: it sets TemplateMode to Off and clears template callbacks/options. `openResolved:819` calls it before its layout resolution and any backend/side-store opening; `caching.Open` later receives that normalized mode (`1207`). The initial omitted option is also zero/Off. It therefore does not disagree with later effective caching options. Profile resolution can perform earlier read-only layout selection; `dir_layout.go:18` uses path/Stat operations and does not create storage. Direct caching validation still rejects effective nonzero template mode (`caching/cow_open.go:28`); this is distinct from the accepted normalized public request. Existing `TestSideStoreChunkSize_TemplateModeRequestIsIgnored` explicitly requires successful public Open with a requested Prepass and no template side store.

I verified the literal13-file packet/no extras, every manifest hash, unchanged before/after source against current bytes and immutable61 Git blobs, actual commands/exits and probe source. Its six actual rows cover all three COW profiles × TemplateOnly/TemplatePrepass. Each uses a unique absent child database directory, confirms actual cow_btree dispatch and no templatedb, then succeeds through real SetSync/Get/Close and cleanup. The existing ignored-policy test passes count3 with one package pass, no skip/fail and empty stderr. Both receipts exit0. This is local Darwin/arm64 Go1.26.0 evidence; it is not new Windows or final-head CI evidence.

For these accepted effective-Off public Opens, ordinary database files are expected. The invalid-open no-storage-effect contract is not violated by an accepted normalized request. Forwarding the original caller mode into early validation would intentionally introduce a new COW-only public policy instead of repairing the alleged later disagreement.

No production/test/fixture code changed for this triage, so it adds no compiled source delta and requires no automatic45-run recollection. Historical974 costs keep their original identity. The separately accepted Close test changes binary inputs; strict final-head hosted/default performance, actual Windows, requiredCI and hosted review remain authoritative. Root owns the thread reply/resolution; no reviewer GitHub mutation, jobs or source edits were performed.

Exact source SHA-256 bindings:

- `TreeDB/public.go`: `626b7d2f1ab12545d1d8ace49c89b81da09821d588819cdbb95ae2f170278c65`
- `TreeDB/caching/cow_open.go`: `439cdb487f7941ff8279845ee0b5c807b702119e725b8b2d3750b8cb8e68b213`
- `TreeDB/side_store_chunk_size_test.go`: `6fd4943645ca45cae6a448c4fbe20211c68d52973b3ff4dffef4eb9952bd250d`
- `TreeDB/profiles.go`: `60e637ca9ad0493f5ebac36bf49ed18bbc2b4fe04c93aadbca25004e675c9742`
- `TreeDB/dir_layout.go`: `7bbeba773c434e885af025471d5fd8e8c07750736a93cdcc6dad36f31e539e3f`
- `TreeDB/template/mode.go`: `72873ca6827c50ed06d919b9d934010693c8cdc246e3955bd7943795e1910581`

Sealed packet-manifest.json SHA-256: `86a443ca855a07a9c702e7cd866750e572380d799b4274bc7d7ab50dffa7301e`. Detailed raw/artifact bindings are in the accompanying JSON.
