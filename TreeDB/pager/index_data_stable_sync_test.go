package pager

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSyncIndexDataWithStableFileRejectsNilBeforeDurabilityCut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var points []durabilitycut.Point
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceIndex {
			points = append(points, event.Point)
		}
		return nil
	})
	defer restore()

	if err := p.SyncIndexDataWithStableFile(nil); err == nil {
		t.Fatal("nil stable target unexpectedly entered durability barrier")
	}
	if len(points) != 0 {
		t.Fatalf("nil stable target emitted durability cuts=%v want none", points)
	}
}

func TestSyncIndexDataWithStableFileDrainsLiveMappingsAndSurvivesClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	pageID, err := p.Alloc(1)
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if err := p.Write(pageID, bytes.Repeat([]byte{0x5a}, page.PageSize)); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if len(p.dirtyChunks) == 0 {
		_ = p.Close()
		t.Fatal("live pager has no dirty mmap chunk before index-data barrier")
	}
	stable, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	defer stable.Close()
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		_ = p.Close()
		t.Fatalf("live stable-file barrier: %v", err)
	}
	if len(p.dirtyChunks) != 0 {
		_ = p.Close()
		t.Fatalf("dirty chunks after live barrier=%d want 0", len(p.dirtyChunks))
	}
	info, err := stable.Stat()
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if got := p.durableFileSize.Load(); got != info.Size() {
		_ = p.Close()
		t.Fatalf("durable file size=%d want %d", got, info.Size())
	}
	if err := p.Write(pageID, bytes.Repeat([]byte{0xa5}, page.PageSize)); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	closedTarget, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if err := closedTarget.Close(); err != nil {
		_ = p.Close()
		t.Fatal(err)
	}
	if err := p.SyncIndexDataWithStableFile(closedTarget); err == nil {
		_ = p.Close()
		t.Fatal("closed stable target unexpectedly completed durability barrier")
	}
	if len(p.dirtyChunks) == 0 {
		_ = p.Close()
		t.Fatal("failed stable-file sync did not restore dirty mmap bookkeeping")
	}
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		_ = p.Close()
		t.Fatalf("retry stable-file barrier: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	originalFence := syncPageFileFn
	retainedFences := 0
	syncPageFileFn = func(file *os.File) error {
		if file != stable {
			t.Fatal("after-close fence lost retained handle")
		}
		retainedFences++
		return originalFence(file)
	}
	defer func() { syncPageFileFn = originalFence }()
	if err := p.SyncIndexDataWithStableFile(stable); err != nil {
		t.Fatalf("retained stable-file barrier after pager close: %v", err)
	}
	if retainedFences != 1 {
		t.Fatalf("after-close file fences=%d want1", retainedFences)
	}
	reopened, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	data, err := reopened.Get(pageID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{0xa5}, page.PageSize)) {
		t.Fatal("mapped write lost after reopen")
	}
}

func TestSyncPagesWithStableFileUsesPinnedIdentityAfterPathReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(2); err != nil {
		t.Fatal(err)
	}
	if err := p.Write(0, bytes.Repeat([]byte{0x6d}, page.PageSize)); err != nil {
		t.Fatal(err)
	}

	pinned, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	moved := filepath.Join(dir, "index.original")
	if err := os.Rename(path, moved); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("Windows open index handles prevent path replacement: %v", err)
		}
		t.Fatal(err)
	}
	replacement, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := replacement.Truncate(int64(2 * page.PageSize)); err != nil {
		t.Fatal(err)
	}

	if err := p.SyncPagesWithStableFile(replacement, []uint64{0}); err == nil {
		t.Fatal("replacement handle unexpectedly crossed mapped-page barrier")
	}
	if err := p.SyncPagesWithStableFile(pinned, []uint64{0}); err != nil {
		t.Fatalf("pinned mapped-page barrier after path replacement: %v", err)
	}
}

func TestSyncIndexDataRejectsWrongFileAndRetainsDirtyChunks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Write(id, bytes.Repeat([]byte{0x39}, page.PageSize)); err != nil {
		t.Fatal(err)
	}
	pinned, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	wrongPath := filepath.Join(dir, "wrong.db")
	if runtime.GOOS != "windows" {
		if err := os.Rename(path, filepath.Join(dir, "original.db")); err != nil {
			t.Fatal(err)
		}
		wrongPath = path
	}
	wrong, err := os.Create(wrongPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wrong.Close()
	originalMapped, originalFile := syncMappedFileFn, syncPageFileFn
	defer func() { syncMappedFileFn, syncPageFileFn = originalMapped, originalFile }()
	mappedCalls, fileCalls := 0, 0
	syncMappedFileFn = func(data []byte) error {
		mappedCalls++
		return originalMapped(data)
	}
	syncPageFileFn = func(file *os.File) error {
		fileCalls++
		return originalFile(file)
	}
	if err := p.SyncIndexDataWithStableFile(wrong); err == nil {
		t.Fatal("rebound path handle accepted for mapped index durability")
	}
	if mappedCalls != 0 || fileCalls != 0 {
		t.Fatalf("identity mismatch issued mapped=%d file=%d fences; want none", mappedCalls, fileCalls)
	}
	if len(p.dirtyChunks) == 0 {
		t.Fatal("identity failure lost dirty chunks")
	}
	if err := p.SyncIndexDataWithStableFile(pinned); err != nil {
		t.Fatal(err)
	}
	if len(p.dirtyChunks) != 0 {
		t.Fatal("successful retained identity sync left dirty chunks")
	}
}

func TestSyncIndexDataFileFenceFailureAndEmptyRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db")
	p, err := Open(path, syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Write(id, bytes.Repeat([]byte{0x74}, page.PageSize)); err != nil {
		t.Fatal(err)
	}
	original := syncPageFileFn
	defer func() { syncPageFileFn = original }()
	want := errors.New("file fence failed")
	calls := 0
	syncPageFileFn = func(file *os.File) error {
		calls++
		if file != p.file {
			t.Fatal("file fence target changed")
		}
		if calls == 1 {
			return want
		}
		return original(file)
	}
	if err := p.SyncIndexData(); !errors.Is(err, want) {
		t.Fatalf("sync error=%v", err)
	}
	if len(p.dirtyChunks) == 0 {
		t.Fatal("file fence failure lost dirty chunks")
	}
	if err := p.SyncIndexData(); err != nil {
		t.Fatal(err)
	}
	if len(p.dirtyChunks) != 0 {
		t.Fatal("retry did not drain chunks")
	}
	if err := p.SyncIndexData(); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("file fences=%d want3 including empty dirty set", calls)
	}
}

func TestSyncDirtyChunksMappedPolicyAndFlushFailure(t *testing.T) {
	p, err := Open(filepath.Join(t.TempDir(), "index.db"), syncPagesTestChunkSize(1))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id, err := p.Alloc(1)
	if err != nil {
		t.Fatal(err)
	}
	dirty := func() {
		t.Helper()
		if err := p.Write(id, bytes.Repeat([]byte{0x42}, page.PageSize)); err != nil {
			t.Fatal(err)
		}
	}
	originalMapped, originalFile := syncMappedFileFn, syncPageFileFn
	t.Cleanup(func() { syncMappedFileFn, syncPageFileFn = originalMapped, originalFile })
	var order []string
	failMapped := false
	want := errors.New("mapped flush failed")
	syncMappedFileFn = func(data []byte) error {
		order = append(order, "mapped")
		if failMapped {
			return want
		}
		return originalMapped(data)
	}
	syncPageFileFn = func(file *os.File) error {
		order = append(order, "file")
		return originalFile(file)
	}
	dirty()
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	expected := []string{"file"}
	if mappedRangeSyncRequired() {
		expected = []string{"mapped", "file"}
	}
	if !reflect.DeepEqual(order, expected) {
		t.Fatalf("full sync order=%v want%v", order, expected)
	}
	order = nil
	dirty()
	if err := p.FlushDirtyChunksFrom(0); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"mapped"}) {
		t.Fatalf("flush-only order=%v", order)
	}
	order = nil
	dirty()
	if err := p.syncDirtyChunksWithFile(false, 0, nil); err != nil {
		t.Fatalf("flush-only validated unused file target: %v", err)
	}
	if !reflect.DeepEqual(order, []string{"mapped"}) {
		t.Fatalf("flush-only nil target order=%v", order)
	}
	order = nil
	dirty()
	failMapped = true
	if err := p.FlushDirtyChunksFrom(0); !errors.Is(err, want) {
		t.Fatalf("mapped failure=%v", err)
	}
	if len(p.dirtyChunks) == 0 {
		t.Fatal("failed mapped flush lost dirty chunks")
	}
	if !reflect.DeepEqual(order, []string{"mapped"}) {
		t.Fatalf("failed flush order=%v", order)
	}
	failMapped = false
	if err := p.FlushDirtyChunksFrom(0); err != nil {
		t.Fatal(err)
	}
	if len(p.dirtyChunks) != 0 {
		t.Fatal("mapped retry did not drain chunks")
	}
}
