# Private allocator preparation handoff (#5132)

Frozen source: allocation-private-20261015. Six owned changed Go files have been
gofmt formatted; exact hashes in gofmt-original.json. Formatting the measured
integration bytes in memory exactly equals all six original source files
(gofmt-semantic-diff.json); integration was not modified. Final coherent source was recomposed and validated; see final-freeze.md for
commit binding, race/public results and the complete current evidence packet.
No Git/GitHub mutation was performed by this lane.

## Ownership and validation

privatePreparation grants authority only after dirty graph detachment. Durable
paths copy on first mutation; zero page ID alone gives no authority. Sharing via
cloneForAllocatorPrepare revokes both transactions; private staging subsequently
detaches dirty state so rollback aliases stay immutable. Every materializer exit
revokes authority, and the transaction is consumed before sink callbacks. Generic
sink byte isolation remains intact. No global pool/callback, duplicate allocator,
R1 capacity system, output representation optimization, or durability change.

Representation RED preceded production changes (representation-red.txt). Seven
new controls passed in integrated full freelist: repeated mutation, clone alias
revocation, rollback detach, retained candidate/reopen, durable parent with dirty
children, validation-error revocation, and partial sink failure. Full freelist and
targeted DB Patricia/format controls exit0: ../repair-batch-focused.{json,log}.
Exact public Write allocation stacks include mutatePrivateChunkWithStats and
cloneForPrivateAllocatorPrepare; counters are transaction-owned, with no hook.

The retained dirty metadata fixture now borrows via actual production clone.
Its prior saved private root pointer did not register sharing and bypassed the
contract. Success/partial parent-write-failure deep-equality and zero-ID oracles
remain. Root escape audit found BeginCandidate base sharing, explicit clone,
frozen candidate, and a local read-only sizing copy; no other mutable escape.
The impossible metadata run uses three free pages: chunk+reservation+header needs
three while retaining nonempty selected chunk. Four became possible with direct
terminal emission. No ownership oracle was waived.

## Public diagnostics

public-combined-early compares exact ordinary177 to canonical+private, three
paired adaptive300ms CPU0/GOMAXPROCS1 runs. Medians154768 to45128 B/op (-70.84%),
286 to144 allocs/op (-49.65%), 46819 to22288 ns/op (-52.40%). Adaptive iteration
counts differ: directional only, not final qualification. Harness seed10000 and
stopped-timer Checkpoint every8 writes preserved; no zipper patch in this binary.
Separate CPU/Rate1 observers do not supply timing comparisons.

public-private-incremental uses a fair benchmark-only topology counterfactual:
Go overlay replaces exactly two files, with exact variants/diffs/manifest in
topology-only-overlay. No BeginCandidate private detach/grant; ordinary prepare
clone; mandatory ordinary materializer detach once. No extra copy is charged.
Private activation tests are expected RED for this observer variant; it is not a
correctness candidate. Three alternating paired fixed10000x runs give medians
56005 to45130 B/op (-19.42%), 156 to144 allocs/op (-7.69%), 24299 to22352 ns/op
(-8.01%). Every pair improves latency.

Matched1000x Rate1 profiles (1001 writes including warmup) reduce the disjoint
flat state-clone union16277536 to5468048 B (-66.41%). Includes mandatory detach:
detachUnmaterialized60896 to2737792 B; chunk clone13736448 to2313216 B; node
clone2480192 to417040 B. No cumulative frames are summed. Exact Write allocation
totals55930440 to45120016 B. Displayed pprof percentages use whole-process
denominators72347352/59959320 B, not Write-only totals. Use *-write-exact-* logs,
attribution-exact.json and state-clone-attribution.json; all small rows retained.

Keep combined-versus-ordinary and private-incremental results separate; never add
percentages. No retained-heap qualification was performed by this lane. Final
formatted/P1-repaired source has focused correctness/race and public measurement
rebound in final-freeze.md; graph-wide acceptance remains with root. Exact
argv, env, source-before/after inputs, binary/overlay SHA, profiles and receipts
are retained in both public evidence directories.

Combined binary SHA f242def2fa2eb12761fd2a40b6817e92a63cfcf1390040f4febd8eb219e8b0d3.
Topology-only SHA 8d68ea3b37cb47ee44109ad140a1e3d83512d5ea33a2f5253d0d156e25d95891.

Serial runtime released. No owned child remains. No paths are released for
deletion: preserve unique source, binaries, profiles, overlay and all evidence.
