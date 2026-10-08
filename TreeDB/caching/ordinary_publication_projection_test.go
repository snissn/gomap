package caching

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"strconv"
	"sync/atomic"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type ordinaryProjectionGroupBackend struct {
	*backenddb.DB
	groups   int
	handoffs atomic.Int64
}

func (b *ordinaryProjectionGroupBackend) BeginRootPublicationBuildGroup() (*backenddb.RootPublicationBuildGroup, error) {
	b.groups++
	return b.DB.BeginRootPublicationBuildGroup()
}

// The installed caching group is the actual production producer, including
// lane forwarding. Count token transfers, not factory presence.
type ordinaryHandoffWitness struct {
	*cachingLeafPageLogGroup
	handoffs *atomic.Int64
}

func (b *ordinaryProjectionGroupBackend) SetLeafPageLog(log backenddb.LeafPageLog) {
	if group, ok := log.(*cachingLeafPageLogGroup); ok {
		log = &ordinaryHandoffWitness{group, &b.handoffs}
	}
	b.DB.SetLeafPageLog(log)
}
func (log *ordinaryHandoffWitness) LeafPageLogForApply(raw func([]page.ValuePtr, []*rootpublication.StableResourceToken) error, child func(*rootpublication.StableResourceSet) error) (backenddb.LeafPageLog, bool, error) {
	factory, ok := any(log.cachingLeafPageLogGroup).(interface {
		LeafPageLogForApply(func([]page.ValuePtr, []*rootpublication.StableResourceToken) error, func(*rootpublication.StableResourceSet) error) (backenddb.LeafPageLog, bool, error)
	})
	if !ok {
		return nil, false, nil
	}
	return factory.LeafPageLogForApply(func(ptrs []page.ValuePtr, tokens []*rootpublication.StableResourceToken) error {
		log.handoffs.Add(1)
		return raw(ptrs, tokens)
	}, child)
}

// Exercise the real caching checkpoint planner, stable caching leaf producer,
// and physical chunk group. Backend-only group tests cannot prove this bridge.
func TestCachingCheckpointOrdinaryDestructiveProjection(t *testing.T) {
	dir := t.TempDir()
	backendOpts := backenddb.Options{Dir: dir, Durability: backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true, IndexOuterLeavesInValueLog: true, LeafPrefixCompression: true,
		IndexColumnarLeaves: true, IndexPackedValuePtr: true, ValueLog: backenddb.ValueLogOptions{ForcePointers: true}}
	backend, err := backenddb.Open(backendOpts)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &ordinaryProjectionGroupBackend{DB: backend}
	cache, err := Open(dir, wrapped, Options{FlushThreshold: 1 << 20, MemtableShards: 1, JournalLanes: 1,
		DisableWAL: true, AllowUnsafe: true, ForceValueLogPointers: true, IndexOuterLeavesInValueLog: true,
		FlushBuildChunkCap: 32, FlushBackendMaxEntries: 32, FlushBuildConcurrency: 2, FlushBuildMinEntries: 1, FlushBuildMinUnits: 1})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	defer func() {
		if cache != nil {
			_ = cache.Close()
		}
	}()
	const count = 256
	key := func(i int) []byte { return []byte(fmt.Sprintf("cached-projection/%08d", i)) }
	old := bytes.Repeat([]byte("old-value|"), 64)
	fresh := bytes.Repeat([]byte("new-value|"), 64)
	for i := 0; i < count; i++ {
		if err := cache.Set(key(i), old); err != nil {
			t.Fatal(err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	beforeSeq := backend.State().CommitSeq
	beforeGroups := wrapped.groups
	beforeHandoffs := wrapped.handoffs.Load()
	before := backend.Stats()
	for i := 0; i < count; i++ {
		if i%3 == 0 {
			err = cache.Delete(key(i))
		} else {
			err = cache.Set(key(i), fresh)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := cache.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if wrapped.handoffs.Load() <= beforeHandoffs {
		t.Fatal("ordinary destructive checkpoint did not hand fresh stable tokens directly to Apply")
	}
	if wrapped.groups <= beforeGroups {
		t.Fatal("checkpoint did not build a real backend chunk group")
	}
	if backend.State().CommitSeq != beforeSeq+1 {
		t.Fatalf("chunk group published %d commits", backend.State().CommitSeq-beforeSeq)
	}
	after := backend.Stats()
	for _, name := range []string{"treedb.durable_root.candidate.full_scans", "treedb.durable_root.candidate.outer_leaf_bodies"} {
		if after[name] != before[name] {
			t.Fatalf("%s changed %s -> %s", name, before[name], after[name])
		}
	}
	leafBefore, _ := strconv.ParseUint(before["treedb.durable_root.candidate.leaf_only_scans"], 10, 64)
	leafAfter, _ := strconv.ParseUint(after["treedb.durable_root.candidate.leaf_only_scans"], 10, 64)
	if leafAfter-leafBefore != 2 {
		t.Fatalf("leaf-only scans=%d want 2", leafAfter-leafBefore)
	}
	verify := func(db *backenddb.DB) {
		for i := 0; i < count; i++ {
			got, err := db.Get(key(i))
			if err != nil {
				t.Fatal(err)
			}
			if i%3 == 0 {
				if got != nil {
					t.Fatalf("deleted key %d = %q", i, got)
				}
			} else if !bytes.Equal(got, fresh) {
				t.Fatalf("key %d = %q", i, got)
			}
		}
	}
	verify(backend)
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache = nil
	reopened, err := backenddb.Open(backendOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	verify(reopened)
}
