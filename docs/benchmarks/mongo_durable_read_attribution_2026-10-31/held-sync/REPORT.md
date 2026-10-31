# Native held-physical-sync progress control: PASS

Acceptance owner: root coordinator. This is a diagnostic prototype and inquiry control, not a production repair, RED bug proof, final12 reproduction, throughput comparison or completion of #5225.

Source base: `95c6ff3de473786bd98b692375cfdb9cef951668`, branch `codex/mongo-read-attribution-20261031`. Only source diff is the added test plus reflect import in `TreeDB/native_command_wal_group_test.go`. No production changes, group-cap/delay overrides, CRC changes, commits, pushes or GitHub writes.

## Result

The first normal optimized build and first coherent test batch passed. The new control passed in 0.60 seconds; all six named tests passed in 5.05 seconds including native independent updates, poison cases, singleton/checkpoint/close, crash recovery, and raw held sync. Raw command/log/exit/resource receipts are retained next to this report.

The two-row BSON fixture has ten binary fields of 100 bytes each and no secondary index. It uses indexedSliceOptions(ProfileCommandWALDurable), NativeSemanticBackend and the native CollectionManager. Read-only reflection of an existing immutable NativeJSONRead capture verified *caching.NativeSemanticCapture, nonzero baseRoot10, no captured/typed source, and only the updated ID in pending after update. Root10 remained unchanged during actual nonzero-LSN ResourceCommandWAL BeforeDependencyFileSync at LSN3. Thus native_json_read.lookupEntry must access the retained physical baseline for the unchanged ID; this is additional proof beyond checkpoint success.

While actual sync was held, Collection.Get for the unchanged ID returned exact bytes within the fixed two-second progress bound and the UpdateBSONSet ACK remained blocked. The hook was released; matched=true, modified=true and nil error followed. Both rows matched their exact BSON bytes before checkpoint, manager/DB close and after fresh reopen. The returned read result remained unchanged through subsequent reads/checkpoint/close; its mutation after close did not alter the reopened rows. A permanent regression could strengthen this with mutation plus readback while the original owner is still open. cap==len is only a shape assertion, not ownership proof.

Cleanup always releases and joins the update and any started reader before removing the hook. Manager/DB close and captured read close errors are checked. The outer test timeout bounds a failed join without uninstalling an in-use hook.

## Bounds and identity

Build: pinned Go1.26.10, normal optimization, -p=1 -gcflags=all=-c=1 -mod=readonly -trimpath -buildvcs=false; no race. GOWORK/GOENV/GOPROXY/GOSUMDB=off and GOTOOLCHAIN=local; explicit pinned GOROOT fixes an inherited Go1.25 GOROOT discovered before build. GOROOT/GOTOOLDIR/compiler agreement and hashes are in toolchain.json; binary ELF/build metadata are in binary.json.

The assigned root filesystem was used because /mnt/fast4tb was mounted with zero available bytes. Task-local TMPDIR/GOCACHE were used. A bounded child process-group guard sampled packet allocation/free filesystem bytes, stopping at the floor plus64MiB reserve or packet cap minus64MiB reserve, with a20minute deadline. No guard stop occurred. Build peak packet allocation1,065,664,512bytes and minimum observed filesystem free8,842,489,856bytes, above floor8,589,934,592bytes and below total packet cap2GiB. Runtime minimum free9,246,855,168bytes. Guard records its own sampling/limits; these are observed bounds, not an atomic filesystem reservation.

Initial worktree source was briefly absent during coordinator materialization; this worker made no source writes until the coordinator sent READY. No failed compile/test or corrective batch occurred. Source hashes/diff snapshots are in source.json/source.diff/source.go.

## Disposition and remaining gates

PASS rejects unavoidable physical-sync blocking of independent native physical-baseline Collection.Get for this fixture. It does not attribute final12 durable-B latency, exclude intermittent/shared-resource costs, identify a production optimization, establish general crash certification, or authorize the optional public campaign.

Runtime grant has been released to root. No build/runtime process remains owned by this worker. Task cache/tmp are retained for coordinator disposition; evidence including source and ELF is retained. No race or broader suite was run, per coordinator scheduling gate. Remaining: root exact-source review/acceptance, decision about retaining a smaller permanent regression versus this diagnostic prototype, scheduling any race run under resource limits, and source-bound conclusion/next question. Public profiling remains a coordinator-owned conditional branch.
