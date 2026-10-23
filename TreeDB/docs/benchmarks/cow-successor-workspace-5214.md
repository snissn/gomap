# COW successor workspace diagnostic (#5214)

This packet investigates the actual Dgraph point-read scratch allocation; it
does not establish final server throughput, RSS, recovery or merge acceptance.
The source anchor is `857890de8dec2cae5d9047b99bc7d5373c748990`.
Whole-process cumulative Dgraph allocation samples put 39.91MB flat at
`cowReadWorkspace.pointScratch`. That sample includes setup and diagnostics and
is not timed bytes/op, live memory, or an estimated throughput improvement.
The separate 26.92MB owned-iterator allocation is outside this implementation.

## Ownership and selected maintained reuse

Public `Store.GetAt` captures and closes one physical read cut per call. It uses
the captured successor, not the direct raw `DB.Get` optimization. Previously,
a nonempty backend basis required a point scratch allocation on each fresh cut.
The original `cowBackendBasis.physicalWorkspaceLocked` and `pointScratch` seam
already supports admitted traversal reuse for direct raw points. Successors
now try that owner's mutex; a busy owner uses the unchanged per-cut path.
Copy-out finishes before unlocking. Snappy/LZ4 stay on the maintained bounded
block-decoder path, while generic codecs and dictionary authority retain their
existing policies. The exact basis owns the lazy scratch and any block decoder
until final release; multiple retained bases remain separately budget charged.

Batch arenas have the wrong last-consumer boundary. The ordinary tree iterator
pool does not carry COW admission or successor state. Reusing exported snapshot
identities would let stale closed aliases refer to a new owner. None is used.
No new arena, pool, publisher, on-disk format or durability boundary is added.

This is a representation optimization: baseline logical behavior should already
pass. Activation and allocation evidence replace a fabricated behavioral red.
The original independent-reader test is preserved, including its successor case.

## Predeclared early A/B

Compile the exact same `cow_successor_workspace_external_test.go` harness against
the anchor and candidate using a test overlay. Record the file hash, both exact
source heads, toolchain, binaries/hashes, environment and commands. Prebuilds,
correctness tests and profiles are outside the measurement slot. No samples have
been collected at construction time.

`BenchmarkCOWSuccessorWorkspace` has ten leaves: inline/forced-pointer GetAt,
depth 1/8, concurrency 1/8 (eight cells), and unchanged prefix iteration at depth
8/concurrency 1 for both value layouts (two cells). Each point operation includes
one complete public `GetAt` and a deterministic result oracle. Concurrent N is
the total number of calls, not N for each worker. One prefix operation consumes
all 32 keys x eight versions, including iterator Close; it is a control, not a
claim of point improvement. The functional fixture also checks exact-key scans.

Use fixed `-test.benchtime=1024x -test.count=1` with identical GOMAXPROCS=8.
Preload/checkpoint, resolved `no_wal_fast/cow_ordered`, drained mutable/queue
state, nonzero physical handoff and actual pointer-layout checks prove route
activation. Compression is off to isolate traversal; focused correctness tests
separately cover cold compressed leaves and dictionary fallback. Every point
and both iterator paths warm outside timing. There is no read/write operation
deferred past the timed call; setup, checkpoint, warmup, stats and Close of the DB
are excluded equally. Automatic checkpoint and periodic vacuum are disabled on
both arms; foreground pruning behavior is unchanged.

Execute three alternating pairs **A/B, B/A, A/B**, where A is the anchor and B
is the candidate: six process invocations, ten leaves each, **60 logical rows**.
Retain all stdout/stderr, failed rows and nulls. Report each row and paired
direction in ns/op, B/op and allocs/op, with ops/s derived from ns/op. These
short runs are a discriminator, not statistical/final performance acceptance.
Do not rerun to obtain significance. Reject material concurrent/time regression
even if bytes improve. Inconclusive or nonadvancing evidence returns to source
comparison; it does not waive the original gates.

After permission for jobs, focused candidate commands are:

```sh
go test ./TreeDB/caching -run 'TestCOW(SuccessorBasisWorkspace|RetainedReadersOwnPhysicalScratch|RetainedPhysicalScratchAdmissionAndRelease|OrderedIteratorDictionaryLeafFallback|InlineSuccessorUsesOwnedWinner|PhysicalLeafCache)' -count=1
go test ./TreeDB/mvcc -run '^TestCOWSuccessorWorkspacePublicGetAtAndHistory$' -count=1
go test -race ./TreeDB/caching ./TreeDB/mvcc -run 'TestCOW(SuccessorBasisWorkspace|RetainedReadersOwnPhysicalScratch|OrderedIteratorDictionaryLeafFallback|SuccessorWorkspacePublicGetAtAndHistory)' -count=1
go test -c ./TreeDB/mvcc -o "$ARTIFACTS/candidate.test"
```

For each prebuilt arm, in the assigned quiet slot:

```sh
GOMAXPROCS=8 "$BINARY" -test.run='^$' -test.bench='^BenchmarkCOWSuccessorWorkspace$' -test.benchtime=1024x -test.count=1
```

Set TMPDIR and all artifact/output paths under the assigned `/mnt/fast4tb` task
directory. These standalone Go benchmark results/profiles are not benchprof
inputs. A viable discriminator still needs unchanged strict PRIMARY/ordinary/COW
guards, applicable races, exact-head review/CI and final public-server evidence.
