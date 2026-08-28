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
		"AcquireSnapshot", "Checkpoint", "Close", "CompactIndex", "CompactStorage",
		"CompactStorageLeafPageLogOwnerClassification", "CompactStoragePlan", "Delete",
		"DeleteRange", "DeleteSync", "DurabilityMode", "FragmentationReport", "Get",
		"GetAppend", "GetMany", "GetManyParallelPlan", "GetManyView", "GetUnsafe",
		"GetVersioned", "GetVersionedAppend", "Has", "HasMany", "HasPrefixes",
		"InitConditionalTxn", "InitConditionalTxnWithSnapshot", "Iterator", "LeafGenerationGC",
		"LeafGenerationPack", "LeafGenerationPackFromPlan", "LeafGenerationPackRunOnce",
		"LeafGenerationPlan", "MaintenancePhase", "NewBatch", "NewBatchWithSize",
		"NewConditionalTxn", "NewConditionalTxnWithSnapshot", "Print", "ResolvedProfile",
		"ReverseIterator", "SeekGE", "Set", "SetMaintenancePhase", "SetSync", "Stats",
		"Update", "UpdateSync", "VacuumIndexOnline", "VacuumOnlineStats", "ValueLogGC", "ValueLogRewriteOnline",
	}
	slices.Sort(want)
	if !slices.Equal(methods, want) {
		t.Fatalf("exported DB entrypoint inventory changed\n got: %v\nwant: %v", methods, want)
	}

	batchMethods := profileInventoryMethods(t, treeDBDir, "treedb", "commandWALPublicBatch")
	wantBatch := []string{
		"Close", "Delete", "DeleteRange", "DeleteView", "DeleteViewWithReplayBytes",
		"GetByteSize", "Replay", "Reset", "Set", "SetView", "SetViewWithReplayBytes",
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
	backendBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "command_wal_raw.go"))
	backendDBBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "db.go"))
	orderedRootBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "db", "ordered_root_publish.go"))

	profileInventoryRequireBody(t, publicBodies, "Open", "resolveOpenProfileOptions")
	profileInventoryRequireBody(t, publicBodies, "VacuumIndexOffline", "resolveOpenProfileOptions")
	vlogRewriteBodies := profileInventoryFunctionBodies(t, filepath.Join(treeDBDir, "vlog_rewrite.go"))
	profileInventoryRequireBody(t, vlogRewriteBodies, "ValueLogRewriteOffline", "resolveOpenProfileOptions")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackend", "resolveOpenProfileOptions")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackendWithCachedLeafLog", "Open(opts)")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackendWithCachedLeafLogStats", "OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(opts)")
	profileInventoryRequireBody(t, backendOpenBodies, "OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance", "Open(opts)")
	profileInventoryRequireBody(t, profileBodies, "OptionsFor", "ApplyProfile")
	profileInventoryRequireBody(t, profileBodies, "OptionsForBenchmark", "ApplyBenchmarkProfile")
	for _, name := range []string{"ApplyProfile", "ApplyBenchmarkProfile"} {
		profileInventoryRequireBody(t, profileBodies, name, "applyResolvedProfile")
	}
	for _, name := range []string{"Set", "Delete", "DeleteRange"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "commandWALOrdinaryWriteRequiresSync")
	}
	for _, name := range []string{"SetSync", "DeleteSync"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "appendPublicRawKVPointCommand")
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "true")
	}
	for _, name := range []string{"NewBatch", "NewBatchWithSize"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "newCommandWALPublicBatch")
	}
	for _, name := range []string{"Checkpoint", "Close"} {
		profileInventoryRequireBody(t, publicBodies, "(*DB)."+name, "checkpointCachedForPublicCommandWAL")
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
