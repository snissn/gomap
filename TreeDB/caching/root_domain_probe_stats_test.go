package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	statGetManyNativeCalls           = "treedb.cache.root_domain_probes.getmany.native_calls"
	statGetManyNativeKeys            = "treedb.cache.root_domain_probes.getmany.native_keys"
	statGetManyNativeUnique          = "treedb.cache.root_domain_probes.getmany.native_unique"
	statGetManyFallbackCalls         = "treedb.cache.root_domain_probes.getmany.fallback_calls"
	statGetManyFallbackKeys          = "treedb.cache.root_domain_probes.getmany.fallback_keys"
	statGetManyBackendFallbackCalls  = "treedb.cache.root_domain_probes.getmany.backend_fallback_calls"
	statGetManyBackendFallbackUnique = "treedb.cache.root_domain_probes.getmany.backend_fallback_unique"
	statHasManyNativeCalls           = "treedb.cache.root_domain_probes.snapshot_hasmany.native_calls"
	statHasManyNativeKeys            = "treedb.cache.root_domain_probes.snapshot_hasmany.native_keys"
	statHasManyNativeUnique          = "treedb.cache.root_domain_probes.snapshot_hasmany.native_unique"
	statHasManyBackendFallbackCalls  = "treedb.cache.root_domain_probes.snapshot_hasmany.backend_fallback_calls"
	statHasManyBackendFallbackUnique = "treedb.cache.root_domain_probes.snapshot_hasmany.backend_fallback_unique"
	statHasPrefixesNativeCalls       = "treedb.cache.root_domain_probes.snapshot_hasprefixes.native_calls"
	statHasPrefixesNativePrefixes    = "treedb.cache.root_domain_probes.snapshot_hasprefixes.native_prefixes"
	statHasPrefixesNativeUnique      = "treedb.cache.root_domain_probes.snapshot_hasprefixes.native_unique"
	statHasPrefixesFallbackCalls     = "treedb.cache.root_domain_probes.snapshot_hasprefixes.fallback_calls"
	statHasPrefixesFallbackUnique    = "treedb.cache.root_domain_probes.snapshot_hasprefixes.fallback_unique"
)

func mustProbeStatsTable(t *testing.T, ops ...rootDomainTestOp) memtable.Table {
	t.Helper()
	return newRootDomainTestTable(t, ops...)
}

func TestRootDomainProbeStats_GetManyNativeAndFallback(t *testing.T) {
	t.Run("native", func(t *testing.T) {
		db := &DB{
			backend:          panicBackend{},
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		}
		key := []byte("k")
		db.memtables.Store(&memtableView{
			rootPointShards: []rootDomainSnapshot{
				{immutables: []memtable.Table{mustProbeStatsTable(t, rootDomainTestOp{key: "k", value: "v"})}},
			},
		})

		got, err := db.GetMany([][]byte{key, key, key})
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		if len(got) != 3 || string(got[0]) != "v" || string(got[1]) != "v" || string(got[2]) != "v" {
			t.Fatalf("unexpected GetMany values: %#v", got)
		}

		stats := db.Stats()
		if got := statUint64(t, stats, statGetManyNativeCalls); got != 1 {
			t.Fatalf("native calls=%d want 1", got)
		}
		if got := statUint64(t, stats, statGetManyNativeKeys); got != 3 {
			t.Fatalf("native keys=%d want 3", got)
		}
		if got := statUint64(t, stats, statGetManyNativeUnique); got != 1 {
			t.Fatalf("native unique=%d want 1", got)
		}
		if got := statUint64(t, stats, statGetManyFallbackCalls); got != 0 {
			t.Fatalf("fallback calls=%d want 0", got)
		}
		if got := statUint64(t, stats, statGetManyBackendFallbackCalls); got != 0 {
			t.Fatalf("backend fallback calls=%d want 0", got)
		}
	})

	t.Run("legacy_fallback", func(t *testing.T) {
		mt, err := memtable.NewWithCapacityMode(0, memtable.ModeHashSorted)
		if err != nil {
			t.Fatalf("new memtable: %v", err)
		}
		mt.SetEntry([]byte("k"), []byte("v"), page.ValuePtr{}, node.FlagInline)

		db := &DB{
			backend:          panicBackend{},
			mutableShards:    []memShard{{mem: mt}},
			mutableShardMask: 0,
		}

		got, err := db.GetMany([][]byte{[]byte("k"), []byte("k"), []byte("k")})
		if err != nil {
			t.Fatalf("GetMany: %v", err)
		}
		if len(got) != 3 || string(got[0]) != "v" || string(got[1]) != "v" || string(got[2]) != "v" {
			t.Fatalf("unexpected GetMany values: %#v", got)
		}

		stats := db.Stats()
		if got := statUint64(t, stats, statGetManyNativeCalls); got != 0 {
			t.Fatalf("native calls=%d want 0", got)
		}
		if got := statUint64(t, stats, statGetManyFallbackCalls); got != 1 {
			t.Fatalf("fallback calls=%d want 1", got)
		}
		if got := statUint64(t, stats, statGetManyFallbackKeys); got != 3 {
			t.Fatalf("fallback keys=%d want 3", got)
		}
	})
}

func TestRootDomainProbeStats_GetManyBackendFallbackCountsDedupedMisses(t *testing.T) {
	backend := &countingBackend{}
	db := &DB{
		backend:          backend,
		mutableShards:    make([]memShard, 1),
		mutableShardMask: 0,
	}
	db.memtables.Store(&memtableView{
		rootPointShards: []rootDomainSnapshot{
			{immutables: []memtable.Table{mustProbeStatsTable(t, rootDomainTestOp{key: "other", value: "v"})}},
		},
	})

	got, err := db.GetMany([][]byte{[]byte("missing"), []byte("missing"), []byte("missing")})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 3 || string(got[0]) != "backend" || string(got[1]) != "backend" || string(got[2]) != "backend" {
		t.Fatalf("unexpected GetMany values: %#v", got)
	}

	stats := db.Stats()
	if got := statUint64(t, stats, statGetManyNativeCalls); got != 1 {
		t.Fatalf("native calls=%d want 1", got)
	}
	if got := statUint64(t, stats, statGetManyNativeUnique); got != 1 {
		t.Fatalf("native unique=%d want 1", got)
	}
	if got := statUint64(t, stats, statGetManyBackendFallbackCalls); got != 1 {
		t.Fatalf("backend fallback calls=%d want 1", got)
	}
	if got := statUint64(t, stats, statGetManyBackendFallbackUnique); got != 1 {
		t.Fatalf("backend fallback unique=%d want 1", got)
	}
}

func TestRootDomainProbeStats_SnapshotHasManyAndHasPrefixes(t *testing.T) {
	t.Run("snapshot_hasmany_native", func(t *testing.T) {
		dir := t.TempDir()
		backend, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			t.Fatalf("open backend: %v", err)
		}
		defer func() {
			if err := backend.Close(); err != nil {
				t.Fatalf("close backend: %v", err)
			}
		}()

		db := &DB{
			backend:          backend,
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		}
		snap := &Snapshot{
			db:      db,
			backend: backend.AcquireSnapshot(),
			rootPointShards: []rootDomainSnapshot{
				{immutables: []memtable.Table{mustProbeStatsTable(t, rootDomainTestOp{key: "k", value: "v"})}},
			},
		}
		defer func() {
			if err := snap.Close(); err != nil {
				t.Fatalf("close snapshot: %v", err)
			}
		}()

		got, err := snap.HasMany([][]byte{[]byte("k"), []byte("k"), []byte("k")})
		if err != nil {
			t.Fatalf("HasMany: %v", err)
		}
		if len(got) != 3 || !got[0] || !got[1] || !got[2] {
			t.Fatalf("unexpected HasMany values: %#v", got)
		}

		stats := db.Stats()
		if got := statUint64(t, stats, statHasManyNativeCalls); got != 1 {
			t.Fatalf("native calls=%d want 1", got)
		}
		if got := statUint64(t, stats, statHasManyNativeKeys); got != 3 {
			t.Fatalf("native keys=%d want 3", got)
		}
		if got := statUint64(t, stats, statHasManyNativeUnique); got != 1 {
			t.Fatalf("native unique=%d want 1", got)
		}
		if got := statUint64(t, stats, statHasManyBackendFallbackCalls); got != 0 {
			t.Fatalf("backend fallback calls=%d want 0", got)
		}
	})

	t.Run("snapshot_hasmany_backend_fallback", func(t *testing.T) {
		dir := t.TempDir()
		backend, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			t.Fatalf("open backend: %v", err)
		}
		defer func() {
			if err := backend.Close(); err != nil {
				t.Fatalf("close backend: %v", err)
			}
		}()
		if err := backend.Set([]byte("k"), []byte("v")); err != nil {
			t.Fatalf("backend set: %v", err)
		}

		db := &DB{
			backend:          backend,
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		}
		snap := &Snapshot{
			db:      db,
			backend: backend.AcquireSnapshot(),
		}
		defer func() {
			if err := snap.Close(); err != nil {
				t.Fatalf("close snapshot: %v", err)
			}
		}()

		got, err := snap.HasMany([][]byte{[]byte("k"), []byte("k"), []byte("k")})
		if err != nil {
			t.Fatalf("HasMany: %v", err)
		}
		if len(got) != 3 || !got[0] || !got[1] || !got[2] {
			t.Fatalf("unexpected HasMany values: %#v", got)
		}

		stats := db.Stats()
		if got := statUint64(t, stats, statHasManyNativeCalls); got != 0 {
			t.Fatalf("native calls=%d want 0", got)
		}
		if got := statUint64(t, stats, statHasManyBackendFallbackCalls); got != 1 {
			t.Fatalf("backend fallback calls=%d want 1", got)
		}
		if got := statUint64(t, stats, statHasManyBackendFallbackUnique); got != 1 {
			t.Fatalf("backend fallback unique=%d want 1", got)
		}
	})

	t.Run("snapshot_hasprefixes_native", func(t *testing.T) {
		dir := t.TempDir()
		backend, err := backenddb.Open(backenddb.Options{Dir: dir})
		if err != nil {
			t.Fatalf("open backend: %v", err)
		}
		defer func() {
			if err := backend.Close(); err != nil {
				t.Fatalf("close backend: %v", err)
			}
		}()

		db := &DB{
			backend:          backend,
			mutableShards:    make([]memShard, 1),
			mutableShardMask: 0,
		}
		snap := &Snapshot{
			db:      db,
			backend: backend.AcquireSnapshot(),
			rootIterator: rootDomainSnapshot{
				immutables: []memtable.Table{mustProbeStatsTable(t,
					rootDomainTestOp{key: "aa0", value: "v0"},
					rootDomainTestOp{key: "ab0", value: "v1"},
				)},
			},
		}
		defer func() {
			if err := snap.Close(); err != nil {
				t.Fatalf("close snapshot: %v", err)
			}
		}()

		got, err := snap.HasPrefixes([][]byte{[]byte("aa"), []byte("aa"), []byte("zz")})
		if err != nil {
			t.Fatalf("HasPrefixes: %v", err)
		}
		if len(got) != 3 || !got[0] || !got[1] || got[2] {
			t.Fatalf("unexpected HasPrefixes values: %#v", got)
		}

		stats := db.Stats()
		if got := statUint64(t, stats, statHasPrefixesNativeCalls); got != 1 {
			t.Fatalf("native calls=%d want 1", got)
		}
		if got := statUint64(t, stats, statHasPrefixesNativePrefixes); got != 3 {
			t.Fatalf("native prefixes=%d want 3", got)
		}
		if got := statUint64(t, stats, statHasPrefixesNativeUnique); got != 2 {
			t.Fatalf("native unique=%d want 2", got)
		}
		if got := statUint64(t, stats, statHasPrefixesFallbackCalls); got != 1 {
			t.Fatalf("fallback calls=%d want 1", got)
		}
		if got := statUint64(t, stats, statHasPrefixesFallbackUnique); got != 1 {
			t.Fatalf("fallback unique=%d want 1", got)
		}
	})
}
