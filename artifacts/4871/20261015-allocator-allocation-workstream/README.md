# Allocator allocation workstream: issues 5130, 5131, 5132

Construction source 7a6fd35f56dc6ddb70ae76e91c36656e7055a8df, parent 177fb0c8db1d19c79d3d975126b4b0b64019568e; exact19-file reviewed manifest in packet/review-repair-composition-source.json. Source is pushed; independent review ACCEPT in review.json. Main projection and per-PR gates remain open.

Frozen report: packet/private/final-freeze.md. Full freelist and focused private/snapshot race pass on final source. Prior selected DB format/reopen checks are separately pinned in packet/repair-batch-focused.*. Exact commands, raw output, source input census, binaries' identities, overlays and Rate1 profiles retained; generated binaries omitted by explicit recipe/hash.

Three fixed1000x alternating public Batch.Write pairs, same10000-key seed and stopped-timer Checkpoint every8 writes, Go1.26.3 CPU0/GOMAXPROCS1:
- Ordinary:46678ns/op (21423ops/s),154838B/op,286allocs/op.
- Canonical topology plus private mutation:23542ns/op (42477ops/s),45181B/op,144allocs/op.
- Directional medians:-49.57%time,-70.82%bytes,-49.65%objects. No zipper change used.

Matched exact-Write Rate1 profiles show disjoint named flat page encoding plus reservation digest sites87462120->16457352B (-81.18%); index+chunk subset75137024->8232960B (-89.04%). This is a flat-site metric, not a recomputation of original57.23% stack-union attribution. Do not add cumulative percentages or combine separate speedups.

Historical fair topology-only counterfactual isolates private mutation:24299->22352ns/op (41154->44739ops/s),56005->45130B/op,156->144objects; clone bytes INCLUDING mandatory detachment16277536->5468048B (-66.41%). Historical measured overlay remains unchanged; final-source reproduction overlay is explicitly unmeasured. No retained-heap claim.

These are bounded construction diagnostics, not final sprint/Dgraph or native ACK qualification. Public main delivery, exact-head CI, eight-repeat micro, M0/Dgraph, bounded native ACK/Q32 and graph acceptance remain open. No source or evidence directory is released for deletion by this publication.
