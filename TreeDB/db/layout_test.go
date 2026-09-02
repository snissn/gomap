package db

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"syscall"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestLayoutPaths_DefaultSplitDirs(t *testing.T) {
	dir := t.TempDir()

	layout := resolveStorageLayout(dir)
	if got, want := layout.rootDir, dir; got != want {
		t.Fatalf("rootDir=%q, want %q", got, want)
	}
	if got, want := layout.walDir, filepath.Join(dir, walDirName); got != want {
		t.Fatalf("walDir=%q, want %q", got, want)
	}
	if got, want := layout.valueVLogDir, filepath.Join(dir, valueVLogDirName); got != want {
		t.Fatalf("valueVLogDir=%q, want %q", got, want)
	}
	if got, want := layout.leafVLogDir, filepath.Join(dir, leafVLogDirName); got != want {
		t.Fatalf("leafVLogDir=%q, want %q", got, want)
	}
	if got, want := layout.columnAssetDir, filepath.Join(dir, columnAssetDirName); got != want {
		t.Fatalf("columnAssetDir=%q, want %q", got, want)
	}
}

func TestOpen_FreshDBCreatesSplitStorageDirs(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	layout := resolveStorageLayout(dir)
	for _, path := range []string{layout.walDir, layout.valueVLogDir, layout.leafVLogDir, layout.columnAssetDir} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%s): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be a directory", path)
		}
	}
}

func TestDBColumnAssetRootDirDoesNotAllocateM1634(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := ColumnAssetRootDirPath(dir)
	if got := db.ColumnAssetRootDir(); got != want {
		t.Fatalf("ColumnAssetRootDir=%q, want %q", got, want)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = db.ColumnAssetRootDir()
	}); got != 0 {
		t.Fatalf("ColumnAssetRootDir allocated %.2f times per call", got)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	readonly, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer readonly.Close()
	if got := readonly.ColumnAssetRootDir(); got != want {
		t.Fatalf("readonly ColumnAssetRootDir=%q, want %q", got, want)
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = readonly.ColumnAssetRootDir()
	}); got != 0 {
		t.Fatalf("readonly ColumnAssetRootDir allocated %.2f times per call", got)
	}
}

func TestEnsureStorageLayoutDirsSyncsRootForColumnAssetsM12A(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	dir := t.TempDir()
	var synced []string
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	if _, err := os.Stat(ColumnAssetRootDirPath(dir)); err != nil {
		t.Fatalf("Stat(column_assets): %v", err)
	}
	for _, path := range synced {
		if path == dir {
			return
		}
	}
	t.Fatalf("new storage layout dirs did not sync DB root %q; synced=%v", dir, synced)
}

func TestSyncStorageLayoutDirRequiredEmitsModeledNamespaceBarrier(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("generic parent-directory sync is unsupported on Windows")
	}
	dir := t.TempDir()
	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	defer restore()

	if err := syncStorageLayoutDirRequired(dir); err != nil {
		t.Fatalf("syncStorageLayoutDirRequired: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("namespace barrier events=%v, want before and after", events)
	}
	for i, want := range []durabilitycut.Point{
		durabilitycut.BeforeNewFileDirectorySync,
		durabilitycut.AfterNewFileDirectorySync,
	} {
		if events[i].Point != want || events[i].Resource != durabilitycut.ResourceAuxiliary || events[i].Path != dir {
			t.Fatalf("namespace barrier event[%d]=%+v, want point=%s resource=%s path=%q", i, events[i], want, durabilitycut.ResourceAuxiliary, dir)
		}
	}
}

func TestSyncStorageLayoutDirRequiredFailsClosedAfterModeledBarrier(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("generic parent-directory sync is unsupported on Windows")
	}
	dir := t.TempDir()
	wantErr := errors.New("injected post-directory-sync cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterNewFileDirectorySync {
			return wantErr
		}
		return nil
	})
	defer restore()

	err := syncStorageLayoutDirRequired(dir)
	if !errors.Is(err, wantErr) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("syncStorageLayoutDirRequired error=%v, want injected error joined with ErrRecoveryRequired", err)
	}
}

func TestSyncStorageLayoutDirRequiredClassifiesUnsupportedSync(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("generic parent-directory sync is unsupported on Windows")
	}
	dir := t.TempDir()
	previous := syncStorageLayoutDirHandle
	syncStorageLayoutDirHandle = func(string) error { return syscall.EINVAL }
	t.Cleanup(func() { syncStorageLayoutDirHandle = previous })

	var events []durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		events = append(events, event)
		return nil
	})
	defer restore()

	err := syncStorageLayoutDirRequired(dir)
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("syncStorageLayoutDirRequired error=%v, want unsupported and recovery-required", err)
	}
	if len(events) != 1 || events[0].Point != durabilitycut.BeforeNewFileDirectorySync {
		t.Fatalf("namespace barrier events=%v, want only before-sync", events)
	}
}

func TestEnsureStorageLayoutDirsSyncsFreshNamespaceBottomUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	dir := filepath.Join(parent, "db")
	var synced []string
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	if want := storageLayoutSyncOrderThroughRootForTest(dir); !reflect.DeepEqual(synced, want) {
		t.Fatalf("namespace sync order=%v, want bottom-up %v", synced, want)
	}
}

func TestEnsureStorageLayoutDirsSyncsMissingAncestorChainBottomUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	outer := filepath.Join(parent, "outer")
	dir := filepath.Join(outer, "db")
	var synced []string
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("ensureStorageLayoutDirs: %v", err)
	}
	if want := storageLayoutSyncOrderThroughRootForTest(dir); !reflect.DeepEqual(synced, want) {
		t.Fatalf("namespace sync order=%v, want dependency-closed %v", synced, want)
	}
}

func TestEnsureStorageLayoutDirsSyncsOnlyMissingNamespaceEdges(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	dir := filepath.Join(parent, "db")
	child := filepath.Join(dir, "child")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("Mkdir(root): %v", err)
	}

	var synced []string
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := EnsureStorageLayoutDirs(0o700, dir, child); err != nil {
		t.Fatalf("EnsureStorageLayoutDirs(partial): %v", err)
	}
	if want := []string{dir}; !reflect.DeepEqual(synced, want) {
		t.Fatalf("partial-layout namespace syncs=%v, want %v", synced, want)
	}

	synced = nil
	if err := EnsureStorageLayoutDirs(0o700, dir, child); err != nil {
		t.Fatalf("EnsureStorageLayoutDirs(existing): %v", err)
	}
	if len(synced) != 0 {
		t.Fatalf("fully existing layout unexpectedly synchronized namespace parents: %v", synced)
	}
}

func TestCreateMissingStorageLayoutDirsSyncsPlannedMissingConcurrentCreate(t *testing.T) {
	parent := t.TempDir()
	path := filepath.Join(parent, "raced")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("Mkdir(concurrent winner): %v", err)
	}

	created, err := createMissingStorageLayoutDirs([]string{path}, 0o700)
	if err != nil {
		t.Fatalf("createMissingStorageLayoutDirs: %v", err)
	}
	if want := []string{path}; !reflect.DeepEqual(created, want) {
		t.Fatalf("created namespace edges=%v, want planned-missing EEXIST edge %v", created, want)
	}
}

func TestEnsureStorageLayoutDirsRetriesUnprovenNamespaceAfterSyncFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	dir := filepath.Join(parent, "db")
	wantErr := errors.New("injected outer-parent sync failure")
	previous := syncStorageLayoutDir
	var first []string
	syncStorageLayoutDir = func(path string) error {
		first = append(first, path)
		if path == parent {
			return wantErr
		}
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := ensureStorageLayoutDirs(dir); !errors.Is(err, wantErr) {
		t.Fatalf("first ensureStorageLayoutDirs error=%v, want %v", err, wantErr)
	}
	if want := []string{dir, parent}; !reflect.DeepEqual(first, want) {
		t.Fatalf("first namespace syncs=%v, want %v", first, want)
	}

	var retried []string
	syncStorageLayoutDir = func(path string) error {
		retried = append(retried, path)
		return nil
	}
	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("retry ensureStorageLayoutDirs: %v", err)
	}
	if want := storageLayoutSyncOrderThroughRootForTest(dir); !reflect.DeepEqual(retried, want) {
		t.Fatalf("retry namespace syncs=%v, want repair of unproven edges %v", retried, want)
	}

	if err := os.WriteFile(filepath.Join(dir, "index.db"), []byte("initialized"), 0o600); err != nil {
		t.Fatalf("WriteFile(index proof): %v", err)
	}
	retried = nil
	if err := ensureStorageLayoutDirs(dir); err != nil {
		t.Fatalf("proven ensureStorageLayoutDirs: %v", err)
	}
	if len(retried) != 0 {
		t.Fatalf("initialized steady-state layout unexpectedly synchronized namespaces: %v", retried)
	}
}

func TestEnsureStorageLayoutDirsForOpenRetriesFreshCompositeOuterParentFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "db")
	mainDir := filepath.Join(root, "maindb")
	dictDir := filepath.Join(root, "dictdb")
	proof := filepath.Join(mainDir, "index.db")
	wantErr := errors.New("injected public outer-parent sync failure")
	previous := syncStorageLayoutDir
	var first []string
	syncStorageLayoutDir = func(path string) error {
		first = append(first, path)
		if path == parent {
			return wantErr
		}
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := EnsureStorageLayoutDirsForOpen(0o755, proof, root, mainDir, dictDir); !errors.Is(err, wantErr) {
		t.Fatalf("first public layout ensure error=%v, want %v", err, wantErr)
	}
	if want := []string{root, parent}; !reflect.DeepEqual(first, want) {
		t.Fatalf("first public namespace syncs=%v, want %v", first, want)
	}

	var retried []string
	syncStorageLayoutDir = func(path string) error {
		retried = append(retried, path)
		return nil
	}
	if err := EnsureStorageLayoutDirsForOpen(0o755, proof, root, mainDir, dictDir); err != nil {
		t.Fatalf("retry public layout ensure: %v", err)
	}
	if want := storageLayoutSyncOrderThroughRootForTest(root); !reflect.DeepEqual(retried, want) {
		t.Fatalf("retry public namespace syncs=%v, want proof-absent repair %v", retried, want)
	}
}

func TestEnsureStorageLayoutDirsForOpenRetriesFullMissingAncestorChain(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through exact child handles")
	}
	existing := t.TempDir()
	outer := filepath.Join(existing, "newparent")
	root := filepath.Join(outer, "db")
	mainDir := filepath.Join(root, "maindb")
	dictDir := filepath.Join(root, "dictdb")
	proof := filepath.Join(mainDir, "index.db")
	wantErr := errors.New("injected pre-existing-boundary sync failure")
	previous := syncStorageLayoutDir
	var first []string
	syncStorageLayoutDir = func(path string) error {
		first = append(first, path)
		if path == existing {
			return wantErr
		}
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := EnsureStorageLayoutDirsForOpen(0o755, proof, root, mainDir, dictDir); !errors.Is(err, wantErr) {
		t.Fatalf("first public layout ensure error=%v, want %v", err, wantErr)
	}
	if want := []string{root, outer, existing}; !reflect.DeepEqual(first, want) {
		t.Fatalf("first public namespace syncs=%v, want dependency prefix %v", first, want)
	}

	var retried []string
	syncStorageLayoutDir = func(path string) error {
		retried = append(retried, path)
		return nil
	}
	if err := EnsureStorageLayoutDirsForOpen(0o755, proof, root, mainDir, dictDir); err != nil {
		t.Fatalf("retry public layout ensure: %v", err)
	}
	wantRetry := []string{root, outer, existing}
	for current := filepath.Dir(existing); ; current = filepath.Dir(current) {
		wantRetry = append(wantRetry, current)
		if parent := filepath.Dir(current); parent == current {
			break
		}
	}
	if !reflect.DeepEqual(retried, wantRetry) {
		t.Fatalf("retry public namespace syncs=%v, want full proof-absent ancestor repair %v", retried, wantRetry)
	}
}

func TestEnsureStorageLayoutDirsFreshCompositeSyncCount(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "db")
	mainDir := filepath.Join(root, "maindb")
	dictDir := filepath.Join(root, "dictdb")
	templateDir := filepath.Join(root, "templatedb")
	var synced []string
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(path string) error {
		synced = append(synced, path)
		return nil
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	if err := EnsureStorageLayoutDirsForOpen(0o755, filepath.Join(mainDir, "index.db"), root, mainDir, dictDir, templateDir); err != nil {
		t.Fatalf("ensure public layout: %v", err)
	}
	for _, backendDir := range []string{mainDir, dictDir, templateDir} {
		if err := ensureStorageLayoutDirs(backendDir); err != nil {
			t.Fatalf("ensure backend layout %q: %v", backendDir, err)
		}
	}
	var want []string
	for _, path := range []string{root, mainDir, dictDir, templateDir} {
		want = append(want, storageLayoutSyncOrderThroughRootForTest(path)...)
	}
	if !reflect.DeepEqual(synced, want) {
		t.Fatalf("fresh composite namespace syncs=%v, want %v", synced, want)
	}

	for _, backendDir := range []string{mainDir, dictDir, templateDir} {
		if err := os.WriteFile(filepath.Join(backendDir, "index.db"), []byte("initialized"), 0o600); err != nil {
			t.Fatalf("WriteFile(%q initialization proof): %v", backendDir, err)
		}
	}
	synced = nil
	if err := EnsureStorageLayoutDirsForOpen(0o755, filepath.Join(mainDir, "index.db"), root, mainDir, dictDir, templateDir); err != nil {
		t.Fatalf("ensure existing public layout: %v", err)
	}
	for _, backendDir := range []string{mainDir, dictDir, templateDir} {
		if err := ensureStorageLayoutDirs(backendDir); err != nil {
			t.Fatalf("ensure existing backend layout %q: %v", backendDir, err)
		}
	}
	if len(synced) != 0 {
		t.Fatalf("steady-state reopen added namespace syncs: %v", synced)
	}
}

func TestStorageLayoutAncestorRepairSetsReachRoot(t *testing.T) {
	base := filepath.Join(t.TempDir(), "outer", "db")
	wantParents := storageLayoutSyncOrderThroughRootForTest(filepath.Dir(base))
	parents, err := storageLayoutAncestorParents(base)
	if err != nil {
		t.Fatalf("storageLayoutAncestorParents: %v", err)
	}
	if !reflect.DeepEqual(parents, wantParents) {
		t.Fatalf("ancestor parents=%v, want deepest-first %v", parents, wantParents)
	}

	wantChildren := storageLayoutSyncOrderThroughRootForTest(base)
	wantChildren = wantChildren[:len(wantChildren)-1]
	children, err := storageLayoutAncestorChildren(base)
	if err != nil {
		t.Fatalf("storageLayoutAncestorChildren: %v", err)
	}
	if !reflect.DeepEqual(children, wantChildren) {
		t.Fatalf("ancestor children=%v, want deepest-first edges %v", children, wantChildren)
	}
}

func storageLayoutSyncOrderThroughRootForTest(path string) []string {
	current, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		panic(err)
	}
	var paths []string
	for {
		paths = append(paths, current)
		parent := filepath.Dir(current)
		if parent == current {
			return paths
		}
		current = parent
	}
}

func TestEnsureStorageLayoutDirsPropagatesEachNamespaceSyncFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	for _, failAt := range []string{"root", "outer-parent"} {
		t.Run(failAt, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "db")
			wantErr := errors.New("injected " + failAt + " sync failure")
			previous := syncStorageLayoutDir
			syncStorageLayoutDir = func(path string) error {
				switch failAt {
				case "root":
					if path == dir {
						return wantErr
					}
				case "outer-parent":
					if path == parent {
						return wantErr
					}
				}
				return nil
			}
			t.Cleanup(func() {
				syncStorageLayoutDir = previous
			})

			if err := ensureStorageLayoutDirs(dir); !errors.Is(err, wantErr) {
				t.Fatalf("ensureStorageLayoutDirs error=%v, want %v", err, wantErr)
			}
		})
	}
}

func TestEnsureStorageLayoutDirsPreservesTypedUnsupportedStatus(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows persists directory creation through the exact child handle")
	}
	parent := t.TempDir()
	dir := filepath.Join(parent, "db")
	previous := syncStorageLayoutDir
	syncStorageLayoutDir = func(string) error {
		return ErrNamespacePersistenceUnsupported
	}
	t.Cleanup(func() {
		syncStorageLayoutDir = previous
	})

	err := ensureStorageLayoutDirs(dir)
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) {
		t.Fatalf("ensureStorageLayoutDirs error=%v, want ErrNamespacePersistenceUnsupported", err)
	}
}

func TestHasLegacyMixedWALValueSegments_DetectsValueLogsInWAL(t *testing.T) {
	for _, name := range []string{"value-l0-000001.log", "vlog-l0-000001.log"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			layout := resolveStorageLayout(dir)
			if err := os.MkdirAll(layout.walDir, 0o700); err != nil {
				t.Fatalf("MkdirAll(wal): %v", err)
			}
			if err := os.WriteFile(filepath.Join(layout.walDir, name), []byte("x"), 0o600); err != nil {
				t.Fatalf("WriteFile(value log): %v", err)
			}

			ok, err := hasLegacyMixedWALValueSegments(dir)
			if err != nil {
				t.Fatalf("hasLegacyMixedWALValueSegments: %v", err)
			}
			if !ok {
				t.Fatalf("expected legacy mixed WAL/value layout to be detected")
			}
		})
	}
}

func TestHasLegacyMixedWALValueSegments_IgnoresCommitOnlyWAL(t *testing.T) {
	dir := t.TempDir()
	layout := resolveStorageLayout(dir)
	if err := os.MkdirAll(layout.walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(layout.walDir, "commit-l0-000001.log"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile(commit log): %v", err)
	}

	ok, err := hasLegacyMixedWALValueSegments(dir)
	if err != nil {
		t.Fatalf("hasLegacyMixedWALValueSegments: %v", err)
	}
	if ok {
		t.Fatalf("unexpected legacy mixed WAL/value layout detection")
	}
}
