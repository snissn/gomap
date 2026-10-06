# ce808 minimization source applicability

Candidate: ce808d9d523e6ec7c32fe4c773e87d7ed8dcc2a1. Source identity and execution acceptance remain separate.

The ten-file delta changes only COW publication/read ownership, new bounded-tree methods, the new COW decoder module, and its tests. The public raw-KV and MVCC benchmark fixtures match b9 byte-for-byte. No fixture counts, layouts, timers, sync policy, or benchmark comparators were changed.

- The b9 ordinary public lifecycle TryRLock selectors, NotifyError fixes, and their 19-file packet are unchanged. Their synchronization evidence remains applicable; the new seven-package gate also exercises public Close/read and callback witnesses against the changed COW callees.
- The 730 read-repair 17-file packet remains evidence for unchanged public selectors, copied outputs and DB-close admission. Its old workspace cost is superseded. The new gate exercises those routes plus actual full-budget lazy-reader refusal/retry.
- Existing accepted publication, physical file identity pins, dictionary definition ownership, fixed captured basis, resource retirement and rollover primitives are unchanged. Their old runtime packets describe their frozen source. The new gate covers COW canonical finalization, pointer/dictionary lifetimes, accepted-error handoff, checkpoint/maintenance and public crash/power-loss routes after the changed preparation scratch/custody branch.
- The default-read guard fixture and default branches have unchanged source. COW-only additions do not call the new tree methods or COW decoder from the legacy append-only/btree read paths. The raw-KV ordinary mode benchmarks retain identical source. Root determines whether to reuse the actual b9 default-read guard or recollect, without inventing new guard values.
- New NoWAL optimization is limited to the explicitly WAL-off cached canonical helper. Canonical producer RID planning/cache and finalizer remain; command-WAL dependency custody and native producer/recovery APIs retain their old branches. Actual visibility flush and independent live file/dictionary pins occur before the immutable cut is visible. SetSync/Checkpoint keep their established durability boundaries.
- Captured empty-root omission validates the exact retained snapshot index root as an empty normal leaf. It does not inspect latest backend state. The new old/current empty and nonempty basis witness checks later checkpoint publication cannot replace a retained basis.
- Codec pointer backing begins empty, grows geometrically only for actual codec dependencies, retains discarded backing charges through Close, and refuses before allocation at the finite cap. Actual Snappy/LZ4 headers choose their own bounded owner; ZSTD/dictionary decoder bounds and provenance remain unchanged.

Inherited unchanged diagnostic Print and FragmentationReport selectors still require caller coordination with DB.Close; they remain outside the ordinary point/forward/snapshot C2 close-race claim. No C3 Store-fence or sustained C4 acceptance follows from these diagnostic gates.

Commands, raw JSON, exit receipts and full pre/post source checks are retained in this packet. Gate outcome and actual costs will be appended only after their runs complete.
