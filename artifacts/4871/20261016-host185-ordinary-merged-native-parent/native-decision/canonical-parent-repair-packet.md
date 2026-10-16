# Canonical parent repair: frozen source and pure evidence

The two native gates now compare the complete embedded parent record against the exact capsule-format projection of the saved current record. Both use `nativeCapsuleParentRecordMatchesV6`. The source repair and authorized pure controls are complete; independent successor review and a separate actual33 grant remain required. Public **Q32 RED/ACK0** is unchanged.

## Source and custody

Selection SHA `da97231d275d2c45f904754c3e2f1b93e9122672fd7681fcb09f41fc6c8fe2da`. Base is immutable source03 manifest `9e3596eb05d4222ba4acf747a4045452efb8210a55a74201f5923da1fef0ad88`; its failed actual33 run and diagnosis are retained. This batch did not run that route.

Full successor: `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-build/canonical-parent-repair-source`. Manifest `c6545a7247ce3180de81cf4c8136b1d0fa04e423ec014b44a6aac1965960cd4f`; verified full archive `4b78a22b7a7244f45d4379907261d5dcc6ccb5f70d9c6ee7f20be7de307296e1`; all 10,348 files reconstructable. Source03-to-successor patch `10c1d94195567545e7ddecfbec271004aad2c3ea455d18599bc78d44bd35480e`. Full before/after manifests for every command match this freeze. No Git/GH operation or child agent.

Changed paths (7 total, 2 new):

- `TreeDB/db/native_capsule_parent_v6.go` (new)
- `TreeDB/db/native_capsule_parent_v6_test.go` (new)
- `TreeDB/db/native_mvcc_final_basis_v6.go`
- `TreeDB/db/native_prebuilder_pricing_for_test_v6.go`
- `TreeDB/db/native_publication_remainder_v6_test.go`
- `TreeDB/db/primary_authority_scope_v6.go`
- `TreeDB/mvcc/native_prune_backend_route_v6_test.go`

The projection validates target0/1 before arithmetic, copies the full `DurableRootRecordV1`, sets `UserRootPageID=PrimaryCapsulePageV6(target)+2`, zeros `ParentRecordPageID`, `ParentCommitSeq` and `ParentRecordDigest`, and compares the complete result. It does not project/omit any other field. Current output remains target+1. The codec and current-record seal-preparation normalization are unchanged.

`prepareNativeSealInstallationV6` retains all saved/current record, meta, slot, generation-tagged parent Ref, sequence, transaction, prefix, capacity and resource identity predicates. `nativeAuthorityScopeV6.begin` retains its existing protected basis and prefix predicates. Both replace only their raw embedded-parent comparison. No new state, owner, resource alias, format, authority, callback, nil-work fallback or retained storage is introduced. Original ordinary paths do not call this helper.

## Exact fixed traffic and formula change

The shared calculator prices four fixed operations per gate: target/pointer validation and address arithmetic; full record copy; four format-defined writes; full comparison. Copy and comparison each touch two complete records. Additional fixed traffic covers both operand pointers, target read, UserRootPageID write, two ancestry scalar writes and the ancestry digest write:

`4 records; 4*sizeof(DurableRootRecordV1)+2*sizeof(*DurableRootRecordV1)+4*sizeof(uint64)+sizeof([32]byte)`.

On pinned linux/amd64 Go1.26.3 this is **4 records / 1,296 bytes per gate** (record304, pointer8). Existing coarse gate/header admission is retained; no old comparison credit is subtracted. Each gate reserves this added traffic before operand inspection/projection. Failed Reserve remains effect-free. There is no per-header retained charge/refund claim.

| Applicable term | Before | Successor |
| --- | --- | --- |
| Full invocation F records | `281+D+2K+23P+8M+3S+N+Z+V+A` | `289+D+2K+23P+8M+3S+N+Z+V+A` |
| Post-adoption R records (included in F) | `141+21P+7M+3S+N` | `145+21P+7M+3S+N` |
| F byte delta | — | +2,592 |
| R byte delta (included in F) | — | +1,296 |
| Retained allocation/charge delta | — | zero |
| Finish/control ownership delta | — | zero |

Final-basis actual initial Reserve and observer role2 share the calculator. Actual authority Reserve and both checked/unchecked authority calculators share it; the remainder and observer R use the checked calculator. Observer expression label is F289. The synthetic pricing control now reports R239/124048 for its unchanged synthetic geometry; this is post-adoption arithmetic only, neither actual33 geometry nor a grant. Other variable terms, overflow/refusal guards and limitations remain unchanged. No caps or protected fixture inputs changed.

## Verification

Pinned Go binary SHA `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`, compiler SHA `3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`. Environment: GOTOOLCHAIN=local, GOWORK=off, GOMAXPROCS=2, GOMEMLIMIT=8GiB, package timeout3m, outer timeout300s, task-local cache/tmp and verified offline dependency cache. Exact argv/env in `proposed-commands.json` and each executed command receipt.

| Command | Result | Elapsed | RUN events | PID/PGID |
| --- | --- | --- | --- | --- |
| compile-only | PASS | 71.372s | 0 | 864067/864067 |
| pure-normal | PASS | 2.045s | 10 | 865476/865476 |
| pure-race | PASS | 78.429s | 10 | 865552/865552 |

Compile-only ran zero test bodies. Normal and race each ran these three top-level tests, ten RUN events including subtests, no failures/skips:

- `TestNativeCapsuleParentRecordProjectionV6`: codec-derived operands in both slot directions; raw inequality and projected full equality; +1/+2/wrong-slot separation; invalid targets/nil; every scalar/digest byte including nested refs and encoded ancestry fields; deliberate source ancestry canonicalization; original operand immutability; actual final-basis and authority admission refusal/unwind for record and byte exhaustion.
- `TestNativePublicationRemainderAndFloorV6`: updated actual calculator, unchanged synthetic geometry, floor restoration on success/refusal/error, original Q32 refusal and overflow checks.
- `TestPrimaryCapsuleV6CompleteIndependentSlots`: original independent capsule integrity/slot control unchanged.

The mismatch loop mutates the real decoded parent record one scalar or digest byte at a time and restores it; it is not a subset-field comparison. No DB is opened, native candidate activated, coordinator enqueued, authority written, Finish run, or ACK produced. Each command has actual PID/PGID, verified subreaper, timeout handling and process/group/adopted-child join receipts; all groups and children are empty. Complete results/logs under `verification/`; no retry or source repair after execution.

## Proposed later original actual33 observation — UNEXECUTED

`proposed-actual33-command.json` binds this exact source and archive and retains the original fixture, Q32/1MiB/caps and one-test regex. It requires a separate root grant:

```sh
/mnt/fast4tb/gomap-mvcc-4871-20261015/toolchains/go1.26.3/bin/go test -p=1 ./TreeDB/mvcc -run '^TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6$' -count=1 -v -timeout=3m
```

Use its full pinned env, TMPDIR `/mnt/fast4tb/gomap-mvcc-4871-20261015/native-build/canonical-parent-repair-observation/tmp`, outer240s and the same robust archive/hash/process receipt contract. This proposal stops at the existing prebuilder observation/refusal and grants no final driver/publication runtime.

The prior actual33 failure is not relabeled as success. This repair proves the pure format relation and protected refusal behavior; it does not prove the real DB route recovered. Private map provenance, incomplete shard Z observation, retained dedup/Finish, ordinary/native integration, and full public progress remain open. No numeric diagnostic allowance, timing claim or Q32 acceptance. Source/runtime lease released after this handoff.
