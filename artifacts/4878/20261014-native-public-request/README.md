# Native public request checkpoint (partial #4878)

This packet captures the actual SAME mvccadmission Authority/Capability request
controls and exact recovered diagnostic package. It does not activate public
bounded PruneVersions, current-output deletion, physical Finish or Close.
The protected whole-publication fixture remains byte-identical to SHA
77f14c80a007ce9b918e007e5cce24e5d78ffa022c6a92386cfb58bda246d55f.
It was copied unchanged to TreeDB/mvcc for the retained compile RED, then moved
here so this intermediate source checkpoint does not break unrelated tagged
builds. Restore that exact file there when the maintained ABI is implemented.

Request CaptureRequest and Validate reserve actual shared OrdinalScanWork before
observing the existing authority. The immutable comparable request owns no
publisher, snapshot, resources or mutation permission. ObservePrune consumes it
only on the original ordinary producer Cut before PONR; it validates owner,
foreign epoch and exact version timestamp, while native selection remains
responsible for anchor, tombstone and selected physical revision proofs.
These methods allocate no storage. They certify neither floor drain nor physical
commit. The controls test 80 accepted legal future writes, sticky foreign work,
owner replacement, different DB incarnation, budget refusal and zero allocation.

Focused commands use GOWORK=off GOMAXPROCS=2 CGO_ENABLED=1 GOPROXY=off GOSUMDB=off
GOTOOLCHAIN=local and Go1.26.4, with the provisional native-codec-worker.mod:

    go test -p 2 -modfile=... ./TreeDB/internal/mvccadmission -count=1 -v
    go test -race -p 2 -modfile=... ./TreeDB/internal/mvccadmission -count=1 -v
    go test -p 2 -modfile=... -tags treedb_test ./TreeDB/mvcc -run '^TestPruneWholeCurrentPublicationSustainedPreflight$' -count=1

The first public fixture RED lacked the route package; the second lists the
actual missing cursor/options/stats/DB route hook ABI after exact trace restore.
No synthetic route counters or scalar preparation state were added.

The next causal implementation is native DATA selected proof/current absence
append, constructor-aware exact current directory replacement, and phased
physical publication/Finish using the existing DurableRootTransaction owner.
Existing synchronous build groups keep write locks across returns, existing
ordinary zipper construction uses unlimited work, and current coordinator Finish
still drains ordinary callbacks synchronously. Those helpers do not qualify this
packet. Original 33/80/4096, Q32/1MiB, input32/resource16/file17/two-index, all19,
format/fault/performance/whole-cost/platform/M0 gates all remain open.

No generated fixture directories were created by these request controls.
All commands exited. Existing dirty native_zstd diagnostic files remain protected.
