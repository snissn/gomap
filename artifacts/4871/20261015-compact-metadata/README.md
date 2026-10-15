# Compact PRIMARY metadata: bounded evidence, latency guard remains RED

The existing owned MetadataEdgeScratch now stores two inline edges for Record/Manifest. Only Dependency pages retain the full 512-edge/validation scratch. The same parser, CRC, original owner charge, independent result lifetime and clear-before-refund Close remain. Recursive recovery retains parent edges while decoding children; a shared resettable arena scratch cannot replace those independent result lifetimes. This is a capacity correction in the maintained owner, with no new pool, arena or format.

This artifact-only commit preserves a frozen overlay on b3c7ff702538e552e685ded554a79eb256a3380c. Apply `full-source-overlay/` from the archive to that parent to reconstruct the measured candidate. It includes the earlier 20-path absent-closure repair and current 11 paths (nine unchanged-DATA-base witness paths plus two compact-metadata paths). Root source at this artifact commit is not the measured overlay. Source, build commands, binary identities, raw outputs and full source/HEAD/index censuses are retained. The active worktree may proceed independently; its HEAD/index are not changed by publication.

Meaningful RED retained 57344 bytes for a real two-edge Record; focused GREEN preserves actual parser output, parent edges during recursive child decoding, error refund and idempotent Close. Wording clarification for the frozen worker summary: the new control retains Record/Manifest results while decoding ONE Dependency result containing FOUR child edges, not four independently held Dependency decodes. Three focused metadata/recovery tests and the actual public CommitAt PRIMARY generation/reopen lifecycle passed before measurement.

All 18 planned fresh-process samples are retained: three AB/BA/AB pairs per workload, pinned Go1.26.3, CPU0/GOMAXPROCS1 on host192.168.0.111, baseline exact main5bca87ffe6e1bfba2d693264d2dc82936adc234f. Get500ms, DATA1000x, PRIMARY128x. No replacement samples, profiling or retries. Full CommitAt physical ACK is timed; setup/Stats/Close are outside the timer. Production/harness source, HEAD/index and binary identities remained unchanged throughout.

|Operation|Base ns/op|Candidate ns/op|Base ops/s|Candidate ops/s|Time change|B/op|allocs/op|
|---|---:|---:|---:|---:|---:|---:|---:|
|CommitAtPrimaryOnlyAfterDataGeneration|143150|150517|6985.7|6643.8|+5.146%|279179 → 226679|474 → 390|
|ConditionalTxnBaselineBatchWrite|21294|20415|46961.6|48983.6|-4.128%|31943 → 31504|127 → 104|
|GetVersioned|318.8|330.8|3136762.9|3022974.6|+3.764%|0 → 0|0 → 0|

PRIMARY saves52500B/op and84objects/op but remains+5.14635% slower, exceeding the strict5% latency guard. Allocation gains do not waive latency. Get and DATA pass the bounded guard. This short diagnostic is not final qualification; the residual7367ns/op cause is unproven. Generation advances2→0 is intentional and independently lifecycle-tested.

The prior unchanged-DATA-base witness experiment removed the full candidate scan: https://github.com/snissn/gomap/tree/1fbe97de91ee70f4045cf1d56f686d0095846094/artifacts/4871/20261015-primary-scan-repair . Its profile justified this capacity change. No claim that another allocation microfix resolves the remaining latency is made. Independent insert-only dependency reuse remains valid; destructive/unproven DATA transitions retain projection.

Next is one coherent ordinary CI repair: atomically advance consumed admission lineage when removing successful pending publications while keeping durable/WaitThrough/ACK and resource Finish terminal-gated; preserve original File lifetime for queued/running remap. Use deterministic regression/race controls, then inspect remaining actual CI groups. No speculative microfix, repeated timing until significance, guard waiver or product push occurs at this evidence boundary. Repeat affected short guard after coherent lifecycle changes, then current-head CI and independent Sol6.1 review before merge. Native Q32/public ACK and final Dgraph product gates remain open.

Cleanup receipt preserves verified deletion of exactly two released obsolete ELFs:122516558 logical bytes and122523648 observed additional free bytes. Root independently verified absence. Active source, shared baseline/current binaries, all raw evidence and caches were preserved.
