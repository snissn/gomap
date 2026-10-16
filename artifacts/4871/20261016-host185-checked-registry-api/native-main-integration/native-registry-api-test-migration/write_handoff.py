from pathlib import Path
import json,hashlib,tarfile,re
A=Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/native-main-integration/native-registry-api-test-migration');W=A.parents[1]/'native-integrated'
changes=json.loads((A/'changed-files.json').read_text());freeze=json.loads((A/'owned-freeze.json').read_text())
packages=sorted({'./'+str(Path(n).parent) for n in changes}); checks={}
with tarfile.open(A/'owned-before-source.tar.gz') as old:
 for n in changes:
  pre=old.extractfile(n).read().decode();post=(W/n).read_text()
  post_without_error_tuple=re.sub(r'\n\s*if snapshotErr != nil \{\n\s*return 0, 0, snapshotErr\n\s*\}', '',post)
  nums=lambda s: re.findall(r'(?<![\w])(?:0[xX][0-9a-fA-F]+|\d+(?:\.\d+)?)(?:[a-zA-Z_]\w*)?',re.sub(r'//[^\n]*|"(?:\\.|[^"\\])*"|`[^`]*`','',s))
  timers=lambda s:re.findall(r'\.(?:StartTimer|StopTimer|ResetTimer|ReportAllocs|SetBytes)\([^\n]*',s)
  checks[n]={'original_numeric_literals_unchanged_excluding_new_error_return_tuple':nums(pre)==nums(post_without_error_tuple),'timer_calls_unchanged':timers(pre)==timers(post)}
assert all(all(v.values()) for v in checks.values())
(A/'workload-timer-audit.json').write_text(json.dumps(checks,indent=2)+'\n')
selectors=[{'packages':['./TreeDB','./TreeDB/caching'],'normal_and_race_selector':'Test(AcquireSnapshot_|AcquireBackendSnapshotFastPath_|COWCut|COWPublicReadOwnerCaptureRacesClose|ReadSnapshotsSeeQueuedWrites|ProfileFast_ReadSnapshotsSeeQueuedWrites|RootDomainProbeStats_)'}, {'packages':['./TreeDB/mvcc'],'normal_and_race_selector':'Test(Prune|NativePrune|COW|ExactKey)'}, {'packages':['./TreeDB/collections'],'normal_and_race_selector':'Test(CollectionCatalogOverlayRootsAreVisibleAfterReopen|CollectionOverlayPrimaryProbeMissDoesNotFallThroughToBase|VectorIndexInsert|CollectionBSONOrderedV2MetadataAndEntryCorruptionFailClosed|TypedGraph)'}, {'packages':['./TreeDB/internal/templatedb','./TreeDB/cmd/treemap'],'normal_and_race_selector':'Test(Stable|Compact)'}]
(A/'proposed-validation.json').write_text(json.dumps({'not_executed':True,'first_step':'Root coherent all-writers source checkpoint, then compile all affected test packages with -run ^$ to find indirect method-set/type errors','affected_packages':packages,'focused_selectors':selectors,'environment':'Pinned task Go; GOTOOLCHAIN=local GOWORK=off GOENV=off; task cache and TMPDIR; root owns runtime concurrency reservation','limits':'Proposals only. Protected workloads/caps/alloc oracles remain unchanged. No benchmark execution by this worker.'},indent=2)+'\n')
sha=hashlib.sha256((A/'owned-freeze.json').read_bytes()).hexdigest()
(A/'HANDOFF.md').write_text(f'''# Checked Snapshot API test migration handoff

SOURCE LEASE RELEASED for owned existing `*_test.go` outside `TreeDB/db/**` and `TreeDB/lifecycle/**`. No compiler, test body, benchmark, profile, Git mutation or external action ran. Other writers remain active; this is not a whole-tree freeze. Root's NEW `TreeDB/caching/snapshot_acquisition_error_test.go` and `TreeDB/snapshot_acquisition_error_test.go` are excluded.

Worktree `{W}`, branch `{freeze['branch']}`, HEAD `{freeze['head']}`. Initial source04 manifest `{freeze['source04_manifest']}`. Owned freeze SHA256 `{sha}`. `owned-before.json` and `owned-after.json` cover1935 original owned files; `changed-files.json` identifies216changed files. `owned-delta.patch` and before/after source archives reconstruct every changed file exactly. Two preimages already differed from HEAD (`TreeDB/mvcc/native_prune_backend_route_v6_test.go`, `TreeDB/mvcc/native_finaldriver_diagnostic_v6_test.go`); their original bytes were reconstructed and hash-verified, preserving those earlier changes.

Migrated475AcquireSnapshot calls,7AcquireBackendSnapshotFastPath calls,2AcquireStableSnapshot calls and2AcquireSnapshotForRoot calls. Updated4mock methods and2inline provider interfaces. The2indirect `snapshotVectorIndexPrimaryCache` calls now check its error. No lifecycle Register/RegisterWithHint/Unregister callers exist in the outside test partition;26Register matches concern unrelated service/embedding APIs and remain unchanged. Other root-named indirect helper chains have no owned direct callers, including the removed nil-only acquireCOWSnapshot helper.

Successful test/benchmark acquisitions fail explicitly on unexpected error; returning helpers/callbacks return the exact acquisition error using original return arity. Concurrent workers use existing publishErr/done/check mechanisms. The preexisting COW publication/capture loop has no error channel and now uses safe nonfatal t.Errorf plus return, retaining its WaitGroup join. Close-race operations accept only the existing ErrClosed outcome; final closed assertion requires true ErrClosed. Capacity refusal requires original ErrCOWCapacity. Rotation refusal still requires nil snapshot and NotifyError, and now requires nonnil actual error. Fast-path inapplicability requires nil,nil; true errors fail. Mock wrappers forward actual errors rather than disguising them as nil snapshots.

Source assurance only: pinnedGo1.26.3gofmt parsed and formatted all216files and final -d is empty. `validation.json`/`caller-inventory-after.json` record all486calls, with no unchecked-arity sites found. `workload-timer-audit.json` confirms original numeric literals and timer-operation sequences unchanged across216files; new zero-valued error-return tuple operands are explicitly excluded from the numeric comparison. Existing oracles, workloads and bounds are retained; only checked-arity/error handling changed, plus testing.TB passed to one existing scalar helper so an acquisition error cannot silently become0. No skips, retries, panic wrappers, allocation allowances or product changes were added.

Applicable policies read: rootAGENTS.md,TreeDB/AGENTS.md,CONTRIBUTING.md,tasklocal executorSKILL.md andworker-prompts.md, plus source-selection and test-partition decisions. No additional review cap/stop rule exists in applicable repo policy. Ownership exclusions and no-runtime instruction govern this packet. No unrelated simplification was appropriate for this API migration.

`proposed-validation.json` lists affected packages and suggested normal/race selectors. Root must first combine all writers' changes and typecheck every affected package; gofmt and lexical auditing do not prove method-set/type correctness. Root owns compilation, runtime/race validation and independent coherent-source review. This delivers source migration for review, not native/graph acceptance or current-head CI qualification.
''')
(A/'artifact-hashes.json').write_text(json.dumps({str(p.relative_to(A)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(A.rglob('*')) if p.is_file() and p.name!='artifact-hashes.json'},indent=2)+'\n')
print(json.dumps({'packages':packages,'workload_timer_checks':len(checks),'freeze_sha256':sha,'handoff':str(A/'HANDOFF.md')}))
