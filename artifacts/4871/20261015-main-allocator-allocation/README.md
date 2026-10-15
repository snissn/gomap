# Allocator allocation reduction on main

Issues #5130, #5131 and #5132. Reviewed source: cf93b0b64c3688d59d6e265cf3db52b35e572275, parent 3f87d74b98e8b1e809d151611be03785f85e2283.
The 18-file manifest, full patch, raw logs, input census, build commands,
baseline overlay and independent review are retained in this packet.
The original worker handoff predates the root commit; root-main-commit.json and
review.json bind the final committed blobs. Publication preserves source through
this artifact commit's parent; no pull request or merge is claimed here.

Three alternating fixed-1000x fresh-process pairs used the unchanged public
BenchmarkConditionalTxnBaselineBatchWrite, a 10,000-key seed, stopped-timer
Checkpoint every eight writes, CPU 0, GOMAXPROCS=1 and Go 1.26.3.

| Median | Main baseline | Allocator candidate |
| --- | ---: | ---: |
| ns/op | 46,297 | 22,473 |
| B/op | 152,734 | 43,074 |
| allocations/op | 281 | 139 |

All three pairs improve: 71.80% fewer allocated bytes, 50.53% fewer objects and
51.46% lower median time. Timing is directional because these are short runs.
Separate diagnostic-only public-path instrumentation confirms private mutation
activation across Open, seed, warmup and eight writes. Those aggregate counters
are not the denominator of the timed runs; timed binaries are pristine.

Full freelist and focused format, fresh/reopen, readonly and fallback controls
passed. Main's snapshot API and inline append accounting are preserved. The
pre-alpha allocator format deliberately requires freelist_patricia_v2; nonempty
older allocator formats are rejected before relevant filesystem effects.

Current-head CI, final-base integration, PR merge and the original sprint
performance/native ACK/M0/Dgraph qualification remain open. No prior profile is
relabeled as a profile of this main candidate. Binaries are omitted by recorded
hash and recipe; no local source or generated path is released for deletion.
