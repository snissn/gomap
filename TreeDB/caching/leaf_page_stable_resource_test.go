//go:build !windows

package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCachingLeafPageLogStableBatchPinsExactRawSegment(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	cached, err := Open(dir, backend, Options{
		IndexOuterLeavesInValueLog: true,
		RelaxedSync:                true,
		AllowUnsafe:                true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("open cache: %v", err)
	}
	defer func() { _ = cached.Close() }()

	stable, ok := newCachingLeafPageLog(cached, &cached.leafLog).(backenddb.LeafPageStableBatchLog)
	if !ok {
		t.Fatal("cached leaf-page log does not expose stable batch capture")
	}
	pages := [][]byte{
		buildSparseLeafPageForLeafLogTestWithTag(t, 's'),
		buildSparseLeafPageForLeafLogTestWithTag(t, 't'),
	}
	ptrs, resources, err := stable.AppendLeafPagesWithStableResources(pages)
	if err != nil {
		t.Fatalf("stable append: %v", err)
	}
	if resources == nil {
		t.Fatal("stable append returned nil resources")
	}
	defer resources.Release()
	if len(ptrs) != len(pages) {
		t.Fatalf("pointer count=%d want %d", len(ptrs), len(pages))
	}
	descriptors := resources.Descriptors()
	if len(descriptors) != 1 {
		t.Fatalf("resource count=%d want 1", len(descriptors))
	}
	descriptor := descriptors[0]
	if descriptor.Kind() != rootpublication.ResourceOuterLeafLog {
		t.Fatalf("kind=%q want %q", descriptor.Kind(), rootpublication.ResourceOuterLeafLog)
	}
	fields := descriptor.ReachabilityFields()
	if len(fields) != 1 || fields[0] != rootpublication.ReachabilityOuterLeafRawPointer {
		t.Fatalf("reachability=%v", fields)
	}
	if descriptor.Frontier().Bytes == 0 {
		t.Fatal("captured frontier is empty")
	}

	segmentPath, _, ok := newCachingLeafPageLog(cached, &cached.leafLog).(interface {
		CurrentValueLogSegment() (string, uint32, bool)
	}).CurrentValueLogSegment()
	if !ok {
		t.Fatal("missing current leaf segment")
	}
	moved := segmentPath + ".moved"
	if err := os.Rename(segmentPath, moved); err != nil {
		t.Fatalf("rename captured segment: %v", err)
	}
	if err := os.WriteFile(segmentPath, bytes.Repeat([]byte{0xee}, page.PageSize), 0o600); err != nil {
		t.Fatalf("create path replacement: %v", err)
	}
	token := resources.Tokens()[0]
	got := make([]byte, 8)
	if _, err := token.ReadAt(got, int64(ptrs[0].Offset)); err != nil {
		t.Fatalf("read pinned segment after path replacement: %v", err)
	}
	if bytes.Equal(got, bytes.Repeat([]byte{0xee}, len(got))) {
		t.Fatal("stable token reopened the replacement pathname")
	}
	if filepath.Clean(token.DiagnosticPath()) == filepath.Clean(segmentPath) {
		t.Fatal("diagnostic path must be DB-relative")
	}
}
