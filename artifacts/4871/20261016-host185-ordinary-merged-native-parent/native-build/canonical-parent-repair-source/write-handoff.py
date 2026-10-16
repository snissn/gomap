from pathlib import Path
import hashlib,json
D=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');B=D/'native-build/canonical-parent-repair-source';O=B/'verification'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text())
def dump(p,v):p.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
summary=load(O/'summary.json');assert summary['commands_executed']==3 and summary['all_groups_empty'] and summary['source_still_frozen'];assert all(x['exit_code']==0 for x in summary['results'])
results=[load(O/n/'result.json') for n in ['compile-only','pure-normal','pure-race']]
old=load(D/'native-build/prebuilder-observation03/actual33-promoted-prebuilder-observation/command.json')
proposal={k:old[k] for k in ['argv','cwd','env','outer_timeout_seconds']};proposal['env']['TMPDIR']=str(D/'native-build/canonical-parent-repair-observation/tmp');proposal.update(name='actual33-promoted-prebuilder-observation-after-parent-repair',executed=False,requires_root_verification_grant=True,source_manifest_sha256=sha(B/'source-frozen.json'),source_archive=str(B/'source-frozen.tar.gz'),source_archive_sha256=sha(B/'source-frozen.tar.gz'),native_final_driver_publication_authorized=False,public_status='Q32 RED/ACK0')
dump(B/'proposed-actual33-command.json',proposal)
changed=load(B/'changed-files.json');receipt=load(B/'receipt.json')
packet={'decision':'source repair and pure controls complete; independent review and separate actual33 grant pending','selection':str(D/'native-decision/root-canonical-parent-repair-selection.md'),'selection_sha256':sha(D/'native-decision/root-canonical-parent-repair-selection.md'),'source':receipt,'changed_files':changed,'helper':{'symbol':'nativeCapsuleParentRecordMatchesV6','gates':['NativeMVCCCurrentDeleteV6.prepareNativeSealInstallationV6','nativeAuthorityScopeV6.begin'],'canonical_fields':{'UserRootPageID':'PrimaryCapsulePageV6(target)+2','ParentRecordPageID':0,'ParentCommitSeq':0,'ParentRecordDigest':'[32]byte{}'},'all_other_record_fields':'full equality, unchanged','target_validation':'target<=1 before arithmetic','format_change':False,'other_identity_predicates_removed':False},'pricing':{'symbol':'nativeCapsuleParentRecordWorkV6','per_gate_records':4,'per_gate_bytes_expression':'4*sizeof(DurableRootRecordV1)+2*sizeof(*DurableRootRecordV1)+4*sizeof(uint64)+sizeof([32]byte)','pinned_record_size':304,'per_gate_bytes':1296,'F_records_before':'281+D+2K+23P+8M+3S+N+Z+V+A','F_records_after':'289+D+2K+23P+8M+3S+N+Z+V+A','F_records_delta':8,'F_bytes_delta':2592,'R_records_before':'141+21P+7M+3S+N','R_records_after':'145+21P+7M+3S+N','R_records_delta':4,'R_bytes_delta':1296,'synthetic_R_records':239,'synthetic_R_bytes':124048,'synthetic_is_allowance':False,'added_retained_allocations':0,'retained_charge_delta':0,'finish_operations_delta':0},'compiler':load(O/'identity.json'),'verification_results':results,'verification_summary':summary,'verification_proposal':str(B/'proposed-commands.json'),'verification_proposal_sha256':sha(B/'proposed-commands.json'),'proposed_actual33_command':str(B/'proposed-actual33-command.json'),'proposed_actual33_command_sha256':sha(B/'proposed-actual33-command.json'),'actual33_executed_in_batch':False,'native_publication_executed':False,'public_status':'Q32 RED/ACK0','source_runtime_lease':'released after frozen handoff','remaining_limits':['Source03 prior actual33 failure remains retained, not erased','No DB route recovery or native activation/fence/Finish/ACK demonstrated','Private captured txn/sink/planning map provenance unresolved','Shard Z Q32 observation incomplete','Disjoint retention/Finish/integration/public progress qualification unchanged']}
dump(D/'native-decision/canonical-parent-repair-packet.json',packet)
rows='\n'.join(f"| {r['name']} | PASS | {r['elapsed_seconds']:.3f}s | {r['test_RUN_count']} | {r['pid']}/{r['pgid']} |" for r in results)
paths='\n'.join('- `'+x['path']+'`'+(' (new)' if x['new'] else '') for x in changed)
md=f'''# Canonical parent repair: frozen source and pure evidence

The two native gates now compare the complete embedded parent record against the exact capsule-format projection of the saved current record. Both use `nativeCapsuleParentRecordMatchesV6`. The source repair and authorized pure controls are complete; independent successor review and a separate actual33 grant remain required. Public **Q32 RED/ACK0** is unchanged.

## Source and custody

Selection SHA `{packet['selection_sha256']}`. Base is immutable source03 manifest `9e3596eb05d4222ba4acf747a4045452efb8210a55a74201f5923da1fef0ad88`; its failed actual33 run and diagnosis are retained. This batch did not run that route.

Full successor: `{B}`. Manifest `{receipt['source_manifest_sha256']}`; verified full archive `{receipt['source_archive_sha256']}`; all {receipt['archive_verified_files']:,} files reconstructable. Source03-to-successor patch `{receipt['delta_sha256']}`. Full before/after manifests for every command match this freeze. No Git/GH operation or child agent.

Changed paths ({len(changed)} total, {sum(x['new'] for x in changed)} new):

{paths}

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

Pinned Go binary SHA `{packet['compiler']['go_binary_sha256']}`, compiler SHA `{packet['compiler']['compile_binary_sha256']}`. Environment: GOTOOLCHAIN=local, GOWORK=off, GOMAXPROCS=2, GOMEMLIMIT=8GiB, package timeout3m, outer timeout300s, task-local cache/tmp and verified offline dependency cache. Exact argv/env in `proposed-commands.json` and each executed command receipt.

| Command | Result | Elapsed | RUN events | PID/PGID |
| --- | --- | --- | --- | --- |
{rows}

Compile-only ran zero test bodies. Normal and race each ran these three top-level tests, ten RUN events including subtests, no failures/skips:

- `TestNativeCapsuleParentRecordProjectionV6`: codec-derived operands in both slot directions; raw inequality and projected full equality; +1/+2/wrong-slot separation; invalid targets/nil; every scalar/digest byte including nested refs and encoded ancestry fields; deliberate source ancestry canonicalization; original operand immutability; actual final-basis and authority admission refusal/unwind for record and byte exhaustion.
- `TestNativePublicationRemainderAndFloorV6`: updated actual calculator, unchanged synthetic geometry, floor restoration on success/refusal/error, original Q32 refusal and overflow checks.
- `TestPrimaryCapsuleV6CompleteIndependentSlots`: original independent capsule integrity/slot control unchanged.

The mismatch loop mutates the real decoded parent record one scalar or digest byte at a time and restores it; it is not a subset-field comparison. No DB is opened, native candidate activated, coordinator enqueued, authority written, Finish run, or ACK produced. Each command has actual PID/PGID, verified subreaper, timeout handling and process/group/adopted-child join receipts; all groups and children are empty. Complete results/logs under `verification/`; no retry or source repair after execution.

## Proposed later original actual33 observation — UNEXECUTED

`proposed-actual33-command.json` binds this exact source and archive and retains the original fixture, Q32/1MiB/caps and one-test regex. It requires a separate root grant:

```sh
{D}/toolchains/go1.26.3/bin/go test -p=1 ./TreeDB/mvcc -run '^TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6$' -count=1 -v -timeout=3m
```

Use its full pinned env, TMPDIR `{proposal['env']['TMPDIR']}`, outer240s and the same robust archive/hash/process receipt contract. This proposal stops at the existing prebuilder observation/refusal and grants no final driver/publication runtime.

The prior actual33 failure is not relabeled as success. This repair proves the pure format relation and protected refusal behavior; it does not prove the real DB route recovered. Private map provenance, incomplete shard Z observation, retained dedup/Finish, ordinary/native integration, and full public progress remain open. No numeric diagnostic allowance, timing claim or Q32 acceptance. Source/runtime lease released after this handoff.
'''
(D/'native-decision/canonical-parent-repair-packet.md').write_text(md)
print(json.dumps({'md_sha256':sha(D/'native-decision/canonical-parent-repair-packet.md'),'json_sha256':sha(D/'native-decision/canonical-parent-repair-packet.json'),'actual33_proposal_sha256':sha(B/'proposed-actual33-command.json'),'source_manifest_sha256':receipt['source_manifest_sha256'],'summary':summary},indent=2))
