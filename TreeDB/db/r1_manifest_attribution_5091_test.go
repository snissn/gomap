//go:build linux

package db

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// A short opt-in source characterization, never an acceptance benchmark.
func TestR1ManifestAttribution5091(t *testing.T) {
	n, e := strconv.Atoi(os.Getenv("GOMAP_R1_GC_REVISIONS"))
	if e != nil {
		t.Skip("opt-in nonqualifying GC characterization")
	}
	if n < 1 || n > 512 {
		t.Fatal("revisions outside short budget")
	}
	d, e := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	for range n {
		c, e := d.PrepareLeafGenerationManifestStableClosure()
		if e != nil {
			t.Fatal(e)
		}
		c.Release()
	}
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	now := time.Now()
	s, e := d.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{})
	elapsed := time.Since(now)
	runtime.ReadMemStats(&b)
	if e != nil || s.ManifestRevisionsDeleted != n {
		t.Fatalf("stats=%+v err=%v", s, e)
	}
	t.Logf("revisions=%d elapsed_ns=%d allocated_bytes=%d allocations=%d stats=%+v", n, elapsed.Nanoseconds(), b.TotalAlloc-a.TotalAlloc, b.Mallocs-a.Mallocs, s)
}
func TestR1ParentGenerationDoesNotCertifyInventory5091(t *testing.T) {
	dir := t.TempDir()
	p, e := os.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	a, e := rootpublication.StableNamespaceParentGeneration(p)
	if e != nil {
		t.Fatal(e)
	}
	path := dir + "/child"
	if e = os.WriteFile(path, []byte("first"), 0600); e != nil {
		t.Fatal(e)
	}
	b, e := rootpublication.StableNamespaceParentGeneration(p)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(path, []byte("changed"), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := rootpublication.StableNamespaceParentGeneration(p)
	if e != nil {
		t.Fatal(e)
	}
	if a != b || b != c {
		t.Fatalf("unexpected parent identity change %d %d %d", a, b, c)
	}
}
