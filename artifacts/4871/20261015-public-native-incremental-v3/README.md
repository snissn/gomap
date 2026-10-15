# Public incremental DATA activation diagnostic

Source 7df5c8277ace70d8551417d92e8fe4a84842f472, parent 8c54df0.
Four exact owned paths; protected original fixture77f14 and foreign2 unchanged.
The same public multileaf CWALDurable/COW4shards persistent-pointer fixture
fails before on unrelated ChildRef preservation while actual ACK, old cut,
post-ACK durable writer, pointer values and reopen pass. Bulk replaces all37
children. New integration passes all lifecycle checks and retains36of37 children.
The original captured prefix is applied oldest-to-newest with a selected actual
physical delete through existing primaryMaterialize/applyWithConfig DATA logic.
SAME native transaction, real retirements, apply-fed pointer refs, negative filter,
producer/capsule/Consume/physical Finish and accepted-prefix handoff are retained.

Single functional samples: bulk5420881ns/1990048B/3479alloc;
incremental5299492ns/2405640B/3418alloc. Both2276 intervening writes/prepared1.
Total public time about-2.2% is inconclusive; allocation bytes about+20.9% regress.
No statistically qualified speedup or viability claim. Costs include all public
prune calls, synchronous publication and Finish, excluding fixture and intervening
ordinary writes. Process-wide MemStats only; no phase profile was captured here.
The new delta follows captured-prefix size plus touched DATA paths rather than
rebuilding untouched backend DATA. The full public operation still scans and
validates the captured namespace before selecting its first delete.

Exact4 source copies were captured before the GREEN command. The full4343-file
census was captured after it: other4339 equal the prior verified v7 full census.
Do not relabel that honest binding limit as contemporaneous full before/after.
v2 ACK0 missing-reader failure is retained; its integration omitted apply-fed
pointer refs and fell back to the documented per-key old-ref lookup. v3 forwards
original apply-fed refs and maintains original negativeCoverage Add/nextRoot.

This synchronous diagnostic is unbounded and performs one deletion only. Original
bounded phase16 still refuses. NoQ32/latency/production native/final acceptance.
Next use one bounded same-public-fixture phase-cost observation to select the
remaining structural repair; avoid allocation polishing or broad qualification.
Parent4871 INCOMPLETE; M5 NO-GO; all per-PR/final gates and ownership remain.
