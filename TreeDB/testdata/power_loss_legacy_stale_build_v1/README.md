# Historical stale-build DATA V1 evidence

This archive preserves the exact original witness, ledger and contract blobs
from `19ba9fdbca4ca85afc544a8b342d66aad32f01b2`. The original DB test binary
replayed the public stale-base rejection/retry/reopen witness successfully in
0.18 seconds at commit 4. Its DATA V1 freelist generation is 8. The binding
records the source blobs, binary SHA256 and replay receipt.

This archive supplies **zero current-source coverage**. It is outside the
active runner, registry, counterexample ledger and risk inventory. The exact
historical ID, meta-sync cut, variant, seed and generation remain unchanged.
`TestHistoricalStaleBuildV1ArchiveExactLineageAndNoCurrentCoverage` rejects
archive deletion/substitution, lineage changes and accidental active coverage.
The retirement decision is [#5111 comment 6070598999](https://github.com/snissn/gomap/issues/5111#issuecomment-6070598999).

The current saved PRIMARY V5 witness and ordinary PRIMARY capsule V6 witness
have separate names, IDs, selectors, contracts and source paths. They retain
the actual stale successor rejection, unchanged predecessor visibility,
distinguishable retry and public read-only recovery horizon at commit 4.
Their actual freelist generation is 4; neither is a replay of this DATA V1
storage-format evidence. V6 observes the complete retry seal and its subsequent
same-original-file fence, without creating a DATA META event.
