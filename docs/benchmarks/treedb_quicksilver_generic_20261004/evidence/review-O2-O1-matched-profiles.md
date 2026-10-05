# O2/O1 matched-profile evidence review

Verdict: **ACCEPT_MATCHED_PROFILE_EVIDENCE**, with the retained checkpoint, storage and MISS allocation costs. This is an independent artifact/source-applicability review, not merge authority or acceptance of the final 43-cell campaign. The primary HITS >=1.5x and lower absolute decode/CRC CPU gates are supported. The coordinator owns the explicit tradeoff decision and current CI/review/landed-source gates.

## Identity and scope

Reviewed `O2-O1-four-profile-native-attestation.json` (SHA256 `8bfeb7e2720acd9db8c859e4a3341ac510dedcbeb2e716c5b57c53589338fe55`), all four local capture receipts/reports/profile-insight files and raw CPU/allocs pprof files under `o2-o1-paired-baseline-profile-3m` and `o2-o1-composed-profile-3m`. All 104 locally present attested files match their recorded SHA256 and size. Four `trace.out` files, totaling 139,406,082 bytes, are native-only: their integrity and full profile validation are retained native attestations, not fresh local byte verification.

The four runs retain baseline `63794357c03898da03a37c5412770fdf197f2ce0` and measured candidate `87eb344543709e7752c5f5c222f8a7d42f330dac`, frozen capture SHA256 `45a9c645e8e5be10af82324d9c74a8a13c9663c8a92831ab2c7062bbcf429b69`, and baseline/candidate binary SHA256 `14fbc2da4a172176beeba8700e5cc338943c529c84ae687110ba45581666c615` / `5305dae4e5d085486d0504e63daacf7ca426693b63d5bd59ac523c1cbdd37ae5`. Receipt hashes, plan/manifest/source relationships, build output and actual ldd executable binding, declared resolved libraries, command reconstruction and equal environments check out. Matching durable and fast cells have equal workload semantics apart from source/label/output: seed24, 3M keys, 6M reads, 40k mutations, four readers, ordinary writes and profiling enabled. Raw initial/final oracles include 3M verified keys and 6.04M final misses. Local frozen validation reaches the deliberately absent native-only trace; full four-run PASS is scoped to the native attestation.

No fresh remote Go tool, header, library or native binary byte verification was performed. The previously accepted composed-native receipts supply those historical scopes. Current candidate `4163c789551ef142f9ad27b73f639bcc02ca0f66` has exactly the measured TreeDB subtree `7ccc2e9f9bc3a0cfacf7bb97ee0dc32ba0bd45a0`; Git confirms only two READMEs and the separate point-lookup diagnostic helper differ. The accepted 2131 compiled-input applicability is retained. This does not label the provisional head as landed.

## Absolute HITS evidence

I parsed the raw gzip/protobuf pprof samples independently. CPU figures below are sampled seconds for the same 6M HITS. Decode is the sum of disjoint flat Snappy/LZ4 samples; CRC is the same named CRC512 function on both sides. Nested cumulative stacks are not summed.

| Profile | Total CPU before→after | Flat decode before→after | Flat CRC512 before→after |
| --- | ---: | ---: | ---: |
| durable | 86.83→25.64s | 48.79→4.98s | 11.57→2.65s |
| fast | 88.24→26.83s | 49.48→5.14s | 11.55→2.31s |

Cumulative `decodeFramePayloadTo` also falls 55.56→6.40s durable and 55.85→6.40s fast. Most decode reduction is Snappy; durable LZ4 remains approximately 3.82→3.80s. The evidence supports lower aggregate decode cost, not an improvement of every codec. `O2-O1-matched-profile-summary.json` (SHA256 `426f1a13f99775cc27b60357208d146cde7e2104f645ac6b121a5617fddbde19`) agrees with the independently extracted values. Its CRC aggregate explicitly covers listed top entries rather than an exhaustive checksum total.

Sampled HITS allocation totals decrease 4.957→4.158GB durable and 5.293→4.032GB fast. Flat decode scratch allocations fall 1.123GB→155.180MB and 1.537GB→143.196MB. Existing tree/growth allocations do not uniformly decrease. Cumulative chunk-planner allocation samples increase 2.660→8.574MB durable and 10.486→25.320MB fast; planner CPU is approximately .74→.77s and .65→.68s. Smaller persisted groups can increase recurring planner metadata work. These profiles corroborate that cost family without proving exact attribution of the holdout173 sync MISS bracket.

MISS sampled allocation totals are 66.738→76.037MB durable and 72.046→67.256MB fast, dominated by snapshot-pool samples. No chunk-planner samples appear in these four MISS profiles; absence of sampled stacks is not proof of zero work. Sampled process-wide allocation profiles include instrumentation and cannot be equated exactly with runtime TotalAlloc deltas.

## Gates and retained costs

`O2-O1-paired-full-progress.json` remains SHA256 `b1189da1a832a1a37a9f20a5beaf24c8601ebd2638119d07d25519a0ede1bfd9`: 13 completed pairs, no pending entries and 286 raw hashes. Recomputed three-seed primary HITS medians are 3.473466804x durable and 3.434502827x fast; the minimum of all 13 pairs' read throughput ratios is .944833387. No profiled bundle occurs in the unprofiled hash map. Earlier full-sync, pool and guard reviews continue to apply.

The coordinator assessment accurately retains the ~20% value-log storage increase, explicit fast holdout/ws1 checkpoint costs, durable holdout +40.827ms final-checkpoint tail with unresolved residual, fast2027 scratch-pool cost and original-sync +13.735MB/+2.289B per MISS. Profile results do not erase these costs, establish identical maintenance overlap, or prove their entire causal allocation attribution. No concrete avoidable allocation or redundant preparation defect is established in the changed grouping helper; a new planner cache would require separate lifetime/invalidation proof.

The assessment's conditional scoped read-amplification tradeoff is consistent with the inspected evidence. Ordinary no-WAL ACK remains volatile; explicit sync has stronger durability. Current CI/Codex/review-thread checks, actual landed-source binding and the remaining final scaling/capacity evidence remain coordinator-owned gates. No additional benchmark or production change is required by a newly identified source defect in this review.
