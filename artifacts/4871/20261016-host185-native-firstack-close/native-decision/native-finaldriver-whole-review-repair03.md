# Finaldriver fixture repair03

**ACCEPT, source-only selected diagnostic readiness.** F3 is resolved without changing production, pricing, G or allocator geometry. Independent gpt-6.1-sol/high; no children, source edits, Go/runtime or Git/GitHub operations.

Verified complete10,431-file archive and exact member set against manifest `8f644d53679cc584cc241dd517dd2ccdf16f6e4c8899fa62819c3a520214b2cb`, archive `66f82d9f0cef64cb920c9ed9c314b017d076b4a18f7bcb6316e50611df7d2bbb`, delta `08e23880b98f9b745d22cd968016588a7a2ef35ba81bb1e2a73912d1640ba0ca`. Full manifest comparison against repair02 proves only native_finaldriver_diagnostic_v6_test.go changed (cf8018fd→79f33a8f); all10,430 other paths match.

F3 was an impossible ready assertion: freelist/allocator_publication_work_v6.go209 obtains Data from the original private allocated-vector length;216 deliberately reports NewIDs=Data and NewIDsExact=false. Duplicate entries remain a conservative claim-count bound. Requiring NewIDsExact contradicted that actual observer contract and was missed by previous source review.

The successor instead requires NewIDs==Data and NewIDs<=the original prebirth Allocator.Allocated bound. It preserves every other ready guard, original false exactness, and PrivateAllocated<=Allocated. Distinct claims cannot exceed counted entries, so this checks the sufficient conservative bound without inventing an exact-discovery scan or new charge. The comment and named observed/ready/bound failure output correctly explain that distinction.

The historical repair02 first arm reached ready and then failed this guard before Fence. Its emitted G is diagnostic evidence only, not a runtime allowance or Q32 acceptance. No successor run result is claimed here. Prior whole-source and repair01/02 coverage composes by exact production/formula/source identity, with F3 now explicitly corrected. No remaining source finding identified in this narrow delta.

No numeric allowance, native publication/Finish/ACK/Close/reopen result, whole retained-union or general progress qualification follows. Q32 remains RED/ACK0. Separate exact-source runtime selection and evidence review remain required. Lease released.
