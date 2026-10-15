# Original public vacuum structural observation

Exact unchanged TestPublicVacuumIndexOnlineHighDebtShrinkAndReopen. External compiler overlay only; all3763 live Go/module inputs unchanged. Exit1 is the existing40%shrink failure (25.6579% observed), not a setup or timeout failure.

Required slot/proof CommitSeqs7,6,6,5 deduplicate to three effective versions. All three captured the SAME index generation1/object/file dev47 inode9384545; identical PRIMARY DATA base1049, sequence5, digest2cfec5c5530f6078e66802c0c4628e8e32adad40e8c67102f03e3c999942e8e2. Sequence5 has no components; sequences6/7 each have one distinct immutable revision of public-vacuum/0000.

Source DATA page SUM891 versus UNION297. Each version rebuild emitted297 user DATA pages; output SUM891/UNION891: three disjoint complete copies of almost entirely shared source. Two repeated full copies represent594 pages/2,433,024 raw bytes. Before4,980,736B after3,702,784B;40% gate permits2,988,441.6B. Sharing ceiling is ample, but this observation does not prove a safe implementation or its final file size.

The observer records both retained system outputs (one page each); visible system output was not observed at the early user-build hook. Metadata/PRIMARY/freelist pages are not included in the891 user-page total. Full required slot/proof records remain in normal.raw. No physical generation/page lifetime policy was changed.

Selected candidate for source design: construction-local exact-base rebuild once, then existing fresh-allocation zipper COW materialization for each version's independent PRIMARY delta. Retain shared DATA pages across every output and all catchup/recapture branches; returned retired pages cannot enter newAlloc while reachable by any retained output. No old-arena component reference may enter the new arena.
