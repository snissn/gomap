package treedb

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// TestDurabilityProfilePublicEntrypointInventory is intentionally exhaustive
// for the exported top-level DB surface. Adding a public write, batch, sync,
// lifecycle, or maintenance method must update this inventory and its routing
// assertion instead of silently bypassing the resolved profile contract.
func TestDurabilityProfilePublicEntrypointInventory(t *testing.T) {
	treeDBDir := profileInventoryTreeDBDir(t)
	methods := profileInventoryMethods(t, treeDBDir, "treedb", "DB")
	want := []string{
		"AcquireMVCCReadCut", "AcquireSnapshot", "BeginLogicalMVCC", "CaptureNativeSemanticState", "Checkpoint", "Close", "CommitLogicalMVCC", "CompactIndex", "CompactStorage",
		"CompactStorageLeafPageLogOwnerClassification", "CompactStoragePlan", "DeferredVectorBuildMaintenance", "Delete",
		"DeleteRange", "DeleteSync", "DurabilityMode", "EstimateMVCCWriteWork", "FragmentationReport", "Get",
		"GetAppend", "GetMany", "GetManyParallelPlan", "GetManyView", "GetUnsafe",
		"GetVersioned", "GetVersionedAppend", "Has", "HasMany", "HasPrefixes",
		"InitConditionalTxn", "InitConditionalTxnWithSnapshot", "Iterator", "LeafGenerationGC",
		"LeafGenerationPack", "LeafGenerationPackFromPlan", "LeafGenerationPackRunOnce",
		"LeafGenerationPlan", "MVCCWriteWorkLimit", "MaintenancePhase", "NewBatch", "NewBatchWithSize",
		"NativeCommandWALCompletionBytes", "NativeSemanticBackend", "NewConditionalTxn", "NewConditionalTxnWithSnapshot", "NewMVCCAdmission", "NewMVCCWriteWorkEstimator", "PreflightMVCCPrune", "PrepareMVCCPrune", "PrepareNativeCommandWAL", "Print", "ResolvedProfile",
		"ReverseIterator", "SeekGE", "SeekGEVersionRange", "Set", "SetMaintenancePhase", "SetPointWithMVCCInput", "SetNativeSemanticPublication", "SetSync", "Stats", "SubmitLogicalMVCC", "SupportsLogicalMVCC", "SupportsLogicalMVCCStaging", "SupportsMVCCReadCut",
		"Update", "UpdateSync", "VacuumIndexOnline", "VacuumOnlineStats", "ValueLogGC", "ValueLogRewriteOnline",
	}
	slices.Sort(want)
	if !slices.Equal(methods, want) {
		t.Fatalf("exported DB entrypoint inventory changed\n got: %v\nwant: %v", methods, want)
	}

	batchMethods := profileInventoryMethods(t, treeDBDir, "treedb", "commandWALPublicBatch")
	wantBatch := []string{
		"Close", "Delete", "DeleteRange", "DeleteView", "DeleteViewWithReplayBytes",
		"GetByteSize", "Replay", "Reset", "Set", "SetView", "SetViewWithReplayBytes", "SetWithMVCCInput", "StageLogicalMVCC",
		"Write", "WriteSync",
	}
	slices.Sort(wantBatch)
	if !slices.Equal(batchMethods, wantBatch) {
		t.Fatalf("command-WAL public batch entrypoint inventory changed\n got: %v\nwant: %v", batchMethods, wantBatch)
	}

	publicBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "public.go"))
	profileBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "profiles.go"))
	backendOpenBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "open_backend.go"))
	batchBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "command_wal_public_cached.go"))
	profileInventoryRequireBody(t, batchBodies, "(*DB).NativeCommandWALCompletionBytes", "unsafe.Sizeof(nativePublicCommandWALCompletion{})")
	for _, fragment := range []string{"!tdb.commandWALCached", "tdb.backend == nil", "announced: durable", "tdb.commandWALGroupCommit.durableIntents.Add(1)"} {
		profileInventoryRequireBody(t, batchBodies, "(*DB).PrepareNativeCommandWAL", fragment)
	}
	profileInventoryRequireBody(t, batchBodies, "(*nativePublicCommandWALCompletion).RegisterNativeCommandWAL", "registerPublicCommandWALPolicy")
	profileInventoryRequireBody(t, batchBodies, "(*nativePublicCommandWALCompletion).CompleteNativeCommandWAL", "awaitPublicCommandWAL")
	backendBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "command_wal_raw.go"))
	backendDBBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "db.go"))
	orderedRootBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "ordered_root_publish.go"))

	for _, fragment := range []string{"db.captureReadOwners()", "cached.SeekGEVersionRange(start, end)", "return seekGEBackend(backend, start, end)"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB).SeekGEVersionRange", fragment)
	}
	nativeBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "native_semantic.go"))
	maintenanceBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "bg_vacuum.go"))
	profileInventoryRequireBody(t, maintenanceBodies, "(*DB).DeferredVectorBuildMaintenance", "&DeferredVectorBuildMaintenance{db: db}")
	profileInventoryRequireBody(t, maintenanceBodies, "(*DeferredVectorBuildMaintenance).Scoped", "db: c.db")
	for _, fragment := range []string{"db.captureReadOwners()", "backend.ActivateNativeSemanticOwner()"} {
		profileInventoryRequireBody(t, nativeBodies, "(*DB).NativeSemanticBackend", fragment)
	}
	for _, name := range []string{"CaptureNativeSemanticState", "SetNativeSemanticPublication"} {
		profileInventoryRequireBody(t, nativeBodies, "(*DB)."+name, "db.captureReadOwners()")
		profileInventoryRequireBody(t, nativeBodies, "(*DB)."+name, "cached.COWMode()")
	}
	readCutBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "mvcc_read_cut.go"))
	for _, name := range []string{"SupportsMVCCReadCut", "AcquireMVCCReadCut", "PreflightMVCCPrune"} {
		profileInventoryRequireBody(t, readCutBodies, "(*DB)."+name, "db.captureReadOwners()")
		if name != "PreflightMVCCPrune" {
			profileInventoryRequireBody(t, readCutBodies, "(*DB)."+name, "cached.COWMode()")
		}
	}
	profileInventoryRequireBody(t, readCutBodies, "(*DB).AcquireMVCCReadCut", "cached.AcquireMVCCReadCut()")
	for _, fragment := range []string{"db.captureReadOwners()", "cached.SupportsLogicalMVCC()", "db.Checkpoint()"} {
		profileInventoryRequireBody(t, readCutBodies, "(*DB).PrepareMVCCPrune", fragment)
	}
	logicalBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "cow_ordered_native.go"))
	for _, fragment := range []string{"db.captureReadOwners()", "cached.SupportsLogicalMVCC()"} {
		profileInventoryRequireBody(t, logicalBodies, "(*DB).SupportsLogicalMVCC", fragment)
	}
	for _, fragment := range []string{
		"db.beginPublicOperation()", "db.cached.SupportsLogicalMVCC()", "input.Matches(mutations)",
		"db.cached.NewBatchWithSize", "newCommandWALPublicBatch", "b.GetByteSize()",
		"producer.StageLogicalMVCC(mutations, input)", "b.WriteSync()", "b.Write()", "b.Close()",
	} {
		profileInventoryRequireBody(t, logicalBodies, "(*DB).CommitLogicalMVCC", fragment)
	}
	for _, fragment := range []string{"b.cowState.batchError", "b.isCOW()", "b.preparePayloadForAppend()", "producer.StageLogicalMVCC(mutations, input)"} {
		profileInventoryRequireBody(t, batchBodies, "(*commandWALPublicBatch).StageLogicalMVCC", fragment)
	}
	logicalStageBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "caching", "cow_logical_input.go"))
	for _, fragment := range []string{"input.Matches(mutations)", "b.cowReserve", "b.cowCopyStorage(total)", "input.BindPhysical(i, key)", "b.noteEntryAppend()"} {
		profileInventoryRequireBody(t, logicalStageBodies, "(*Batch).StageLogicalMVCC", fragment)
	}
	profileInventoryRequireBody(t, publicBodies, "Open", "resolveOpenProfileOptions")
	profileInventoryRequireBody(t, publicBodies, "VacuumIndexOffline", "resolveOpenProfileOptions")
	vlogRewriteBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "vlog_rewrite.go"))
	profileInventoryRequireBody(t, vlogRewriteBodies, "ValueLogRewriteOffline", "resolveOpenProfileOptions")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackend", "resolveOpenProfileOptions")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackendWithCachedLeafLog", "openBackendWithCachedServices(opts)")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackendWithCachedLeafLogStats", "OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance", "openBackendWithCachedServices(opts)")
	profileInventoryRequireBody(t, backendOpenBodies, "openBackendWithCachedServices", "resolveOpenProfileOptions(&opts)")
	profileInventoryRequireBody(t, backendOpenBodies, "openBackendWithCachedServices", "openResolvedWithCacheRole(opts, true)")
	profileInventoryRequireBody(t, profileBodies, "OptionsFor", "ApplyProfile")
	profileInventoryRequireBody(t, profileBodies, "OptionsForBenchmark", "ApplyBenchmarkProfile")
	for _, name := range []string{"ApplyProfile", "ApplyBenchmarkProfile"} {
		profileInventoryRequireBody(t, profileBodies, name, "applyResolvedProfile")
	}
	for _, name := range []string{"setWithMVCCInput", "Delete", "DeleteRange"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "commandWALOrdinaryWriteRequiresSync")
	}
	for _, name := range []string{"setSyncWithMVCCInput", "DeleteSync"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "appendPublicRawKVPointCommand")
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "true")
	}
	profileInventoryRequireBody(t, publicBodies, "(*DB).Set", "db.setWithMVCCInput(key, value, mvccadmission.Input{})")
	profileInventoryRequireBody(t, publicBodies, "(*DB).SetSync", "db.setSyncWithMVCCInput(key, value, mvccadmission.Input{})")
	for _, helper := range []string{"setWithMVCCInput", "setSyncWithMVCCInput"} {
		for _, fragment := range []string{"db.beginPublicOperation()", "db.writeCOWPointAdmittedWithMVCCInput", "db.cached.SetAfterCommandWALAppendWithMVCCInput", "db.appendPublicRawKVPointCommand"} {
			profileInventoryRequireBody(t, publicBodies, "(*DB)."+helper, fragment)
		}
	}
	for _, fragment := range []string{"if sync", "db.setSyncWithMVCCInput(key, value, input)", "db.setWithMVCCInput(key, value, input)"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB).SetPointWithMVCCInput", fragment)
	}
	for _, fragment := range []string{"db.beginPublicOperation()", "db.backend.NewMVCCAdmission()"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB).NewMVCCAdmission", fragment)
	}
	admissionBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "mvcc_admission.go"))
	for _, fragment := range []string{"db.closing.Load()", "db.mvccAdmission.Issue()"} {
		profileInventoryRequireBody(t, admissionBodies, "(*DB).NewMVCCAdmission", fragment)
	}
	for _, fragment := range []string{"db.lockUpdateKey(key)", "db.writeViaCommitCombinerWithMVCCInput", "db.writeSingleKVWithMVCCInput"} {
		profileInventoryRequireBody(t, admissionBodies, "(*DB).SetPointWithMVCCInput", fragment)
	}
	for _, name := range []string{"NewBatch", "NewBatchWithSize"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "newCommandWALPublicBatch")
	}
	for _, name := range []string{"Checkpoint", "Close"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "checkpointCachedForPublicCommandWAL")
	}
	profileInventoryRequireBody(t, batchBodies, "(*commandWALPublicBatch).SetWithMVCCInput", "b.setViewWithMVCCInput(key, value, false, false, input)")
	profileInventoryRequireBody(t, batchBodies, "(*commandWALPublicBatch).setView", "b.setViewWithMVCCInput(key, value, retainReplayViews, useInnerView, mvccadmission.Input{})")
	for _, fragment := range []string{"b.preparePayloadForAppend()", "b.isCOW()", "qualified.SetWithMVCCInput(key, value, input)"} {
		profileInventoryRequireBody(t, batchBodies, "(*commandWALPublicBatch).setViewWithMVCCInput", fragment)
	}
	profileInventoryRequireBody(t, batchBodies, "(*commandWALPublicBatch).Write", "commandWALOrdinaryWriteRequiresSync")
	profileInventoryRequireBody(t, batchBodies, "(*commandWALPublicBatch).WriteSync", "b.write(true, true)")
	profileInventoryRequireBody(t, backendBodies, "(*DB).NewCommandWALIntent", "db.resolvedProfile == ProfileCommandWALDurable")
	for _, name := range []string{"AppendCommandWALIntent", "AppendStagedCommandWALIntent", "publishCommandWALNoop"} {
		profileInventoryRequireBody(t, backendBodies, "(*DB)."+name, "commandWALIntentPublishSync")
	}
	profileInventoryRequireBody(t, backendDBBodies, "Open", "validateOptions")
	profileInventoryRequireBody(t, backendDBBodies, "validateOptions", "validateResolvedDurabilityProfile")

	profileInventoryRequireBody(t, orderedRootBodies, "(*DB).publishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder", "publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized")
	profileInventoryRequireBody(t, orderedRootBodies, "(*DB).publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized", "commandWALIntentPublishSync")
}

// The owned submission surface is deliberately classified separately from
// ordinary writes: staging grants no ACK and completion must retain the exact
// original producer/physical-prefix route. Work shaping grants no admission.
func TestDurabilityProfileOrderedSubmissionEntrypointInventory(t *testing.T) {
	treeDBDir := profileInventoryTreeDBDir(t)
	for _, item := range []struct {
		dir, pkg, receiver string
		want               []string
	}{
		{treeDBDir, "treedb", "LogicalMVCCBuilder", []string{"Abort", "Append", "Submit"}},
		{treeDBDir, "treedb", "LogicalMVCCCompletion", []string{"Complete"}},
		{treeDBDir, "treedb", "MVCCWriteWorkEstimator", []string{"Estimate"}},
		{filepath.Join(treeDBDir, "mvcc"), "mvcc", "CommitBuilder", []string{"Abort", "AppendGroup", "Submit"}},
		{filepath.Join(treeDBDir, "mvcc"), "mvcc", "CommitCompletion", []string{"Complete"}},
	} {
		if got := profileInventoryMethods(t, item.dir, item.pkg, item.receiver); !slices.Equal(got, item.want) {
			t.Fatalf("%s exported entrypoint inventory changed\n got: %v\nwant: %v", item.receiver, got, item.want)
		}
	}
	for _, source := range []struct {
		path   string
		routes map[string][]string
	}{
		{"cow_ordered_builder.go", map[string][]string{
			"(*DB).SupportsLogicalMVCCStaging":       {"db.lifecycleMu.RLock()", "db.supportsLogicalMVCCStagingLocked()"},
			"(*DB).supportsLogicalMVCCStagingLocked": {"db.cached.SupportsLogicalMVCC()", "db.commandWALCached", "!db.publicBatchWriteSyncPhaseEnabled"},
			"(*DB).BeginLogicalMVCC":                 {"db.beginPublicOperation()", "db.supportsLogicalMVCCStagingLocked()", "db.cached.NewBatchWithSize(0)", "newCommandWALPublicBatch", "public.GetByteSize()"},
			"(*LogicalMVCCBuilder).Append":           {"b.batch.StageLogicalMVCC(mutations, input)", "durabilitycut.AfterLogicalMVCCStaging"},
			"(*LogicalMVCCBuilder).Abort":            {"batch.Close()", "db.lifecycleMu.RUnlock()"},
			"(*LogicalMVCCBuilder).Submit":           {"commandSync := syncWrite || db.commandWALOrdinaryWriteRequiresSync()", "batch.submitCOWWrite(commandSync)", "batch.completeCOWWrite(commandSync, err)", "c.db, c.batch", "b.db, b.batch = nil, nil"},
		}},
		{"cow_ordered_native.go", map[string][]string{
			"(*DB).SubmitLogicalMVCC":           {"db.CommitLogicalMVCC(mutations, input, syncWrite)", "db.beginPublicOperation()", "db.cached.SupportsLogicalMVCC()", "input.Matches(mutations)", "public.StageLogicalMVCC(mutations, input)", "public.cowState.collectionSealed = true", "public.submitCOWWrite(true)", "public.completeCOWWrite(true, err)", "completion.db, completion.batch"},
			"(*LogicalMVCCCompletion).Complete": {"b.completeCOWWrite(b.cowState.commandSync, nil)", "b.Close()", "db.lifecycleMu.RUnlock()"},
		}},
		{"command_wal_public_cached.go", map[string][]string{
			"(*commandWALPublicBatch).writeCOW":         {"b.submitCOWWrite(commandSync)", "b.completeCOWWrite(commandSync, err)"},
			"(*commandWALPublicBatch).submitCOWWrite":   {"durableIntents.Add(1)", "inner.WriteAfterCommandWALAppendWithPreparedRevision(commandSync, b.registerCOWAppend)"},
			"(*commandWALPublicBatch).completeCOWWrite": {"b.db.awaitPublicCommandWAL", "b.db.finishPublicCommandWALGroupPublication", "inner.CompleteCOWAcknowledgement"},
		}},
		{"cow_work.go", map[string][]string{
			"(*DB).EstimateMVCCWriteWork":        {"db.NewMVCCWriteWorkEstimator().Estimate(keyBytes, valueBytes)"},
			"(*DB).NewMVCCWriteWorkEstimator":    {"db.lifecycleMu.RLock()", "db.cached.NewMVCCWriteWorkEstimator()"},
			"(*DB).MVCCWriteWorkLimit":           {"db.lifecycleMu.RLock()", "db.cached.MVCCWriteWorkLimit()"},
			"(*MVCCWriteWorkEstimator).Estimate": {"e.inner.Estimate(keyBytes, valueBytes)"},
		}},
		{filepath.Join("mvcc", "ordered_builder.go"), map[string][]string{
			"(*Store).SupportsCommitGroupStaging": {"owner.SupportsLogicalMVCCStaging()"},
			"(*Store).BeginCommitGroup":           {"s.lockDiscardFloorRead()", "owner.BeginLogicalMVCC(&b.engine)"},
			"(*CommitBuilder).AppendGroup":        {"mvcckey.EncodedLen(m.Key)", "b.seen[id]", "b.store.discardFloor", "b.engine.Append(mutations, b.store.admission.LogicalInput"},
			"(*CommitBuilder).Submit":             {"b.engine.Submit(mode == CommitDurable, &c.engine)", "c.store = b.store", "b.engine.Abort()"},
			"(*CommitBuilder).Abort":              {"b.abortLocked()"},
			"(*CommitBuilder).abortLocked":        {"b.engine.Abort()", "b.store.mu.RUnlock()"},
		}},
		{filepath.Join("mvcc", "ordered_completion.go"), map[string][]string{
			"(*Store).SubmitCommitGroupAt": {"s.commitLogicalGroupsWithCompletion", "s.CommitGroupAt(groups, mode)"},
			"(*CommitCompletion).Complete": {"c.engine.Complete()", "c.store.mu.RUnlock()"},
		}},
	} {
		bodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, source.path))
		for name, fragments := range source.routes {
			for _, fragment := range fragments {
				profileInventoryRequireBody(t, bodies, name, fragment)
			}
		}
	}
}

func profileInventoryTreeDBDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

func profileInventoryMethods(t *testing.T, dir, packageName, receiverName string) []string {
	t.Helper()
	fset := token.NewFileSet()
	packages, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	pkg := packages[packageName]
	if pkg == nil {
		t.Fatalf("package %q not found in %s", packageName, dir)
	}
	var methods []string
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || !fn.Name.IsExported() || len(fn.Recv.List) != 1 {
				continue
			}
			if profileInventoryReceiverName(fn.Recv.List[0].Type) == receiverName {
				methods = append(methods, fn.Name.Name)
			}
		}
	}
	slices.Sort(methods)
	return methods
}

func profileInventoryReceiverName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return ""
}

func profileInventoryFunctionBodies(t *testing.T, path string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	bodies := make(map[string]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) == 1 {
			name = "(*" + profileInventoryReceiverName(fn.Recv.List[0].Type) + ")." + name
		}
		var out bytes.Buffer
		if err := format.Node(&out, fset, fn.Body); err != nil {
			t.Fatalf("format body %s in %s: %v", name, path, err)
		}
		bodies[name] = out.String()
	}
	return bodies
}

func profileInventoryRequireBody(t *testing.T, bodies map[string]string, name, fragment string) {
	t.Helper()
	body, ok := bodies[name]
	if !ok {
		t.Fatalf("profile entrypoint %s missing from inventory source", name)
	}
	if !strings.Contains(body, fragment) {
		t.Fatalf("profile entrypoint %s does not route through %q\n%s", name, fragment, body)
	}
}
