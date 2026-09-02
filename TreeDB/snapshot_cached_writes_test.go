package treedb

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"
)

func TestAcquireSnapshot_IncludesCachedWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("k1")
	v1 := bytes.Repeat([]byte("a"), 64)
	v2 := bytes.Repeat([]byte("b"), 64)

	if err := db.Set(key, v1); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	got, err := snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get v1: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("snapshot get v1 mismatch: got=%q want=%q", got, v1)
	}

	if err := db.Set(key, v2); err != nil {
		t.Fatalf("set v2: %v", err)
	}

	got, err = snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get after overwrite: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("snapshot isolation mismatch after overwrite: got=%q want=%q", got, v1)
	}
}

func TestAcquireSnapshot_IncludesCachedWrites_ValuePointers(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		Durability:     DurabilityWALOffRelaxed,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1 << 20,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	key := []byte("k1")
	v1 := bytes.Repeat([]byte("a"), 64)
	v2 := bytes.Repeat([]byte("b"), 64)

	if err := db.Set(key, v1); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	got, err := snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get v1: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("snapshot get v1 mismatch: got=%q want=%q", got, v1)
	}

	if err := db.Set(key, v2); err != nil {
		t.Fatalf("set v2: %v", err)
	}

	got, err = snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get after overwrite: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("snapshot isolation mismatch after overwrite: got=%q want=%q", got, v1)
	}
}

func TestAcquireSnapshot_HasManyAndHasPrefixesIncludeCachedWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir:            dir,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("acct/alice/doc-1"), []byte("v1")); err != nil {
		t.Fatalf("set alice: %v", err)
	}
	if err := db.Set([]byte("acct/bob/doc-1"), []byte("v2")); err != nil {
		t.Fatalf("set bob: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()

	if err := db.Delete([]byte("acct/alice/doc-1")); err != nil {
		t.Fatalf("delete alice after snapshot: %v", err)
	}
	if err := db.Set([]byte("acct/carol/doc-1"), []byte("v3")); err != nil {
		t.Fatalf("set carol after snapshot: %v", err)
	}

	hasMany, err := snap.HasMany([][]byte{
		[]byte("acct/alice/doc-1"),
		[]byte("acct/bob/doc-1"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("snapshot HasMany: %v", err)
	}
	wantMany := []bool{true, true, false}
	if !reflect.DeepEqual(hasMany, wantMany) {
		t.Fatalf("snapshot HasMany mismatch: got=%v want=%v", hasMany, wantMany)
	}

	hasPrefixes, err := snap.HasPrefixes([][]byte{
		[]byte("acct/alice/"),
		[]byte("acct/bob/"),
		[]byte("acct/carol/"),
	})
	if err != nil {
		t.Fatalf("snapshot HasPrefixes: %v", err)
	}
	wantPrefixes := []bool{true, true, false}
	if !reflect.DeepEqual(hasPrefixes, wantPrefixes) {
		t.Fatalf("snapshot HasPrefixes mismatch: got=%v want=%v", hasPrefixes, wantPrefixes)
	}

	liveHasMany, err := db.HasMany([][]byte{
		[]byte("acct/alice/doc-1"),
		[]byte("acct/bob/doc-1"),
		[]byte("acct/carol/doc-1"),
	})
	if err != nil {
		t.Fatalf("db HasMany: %v", err)
	}
	wantLiveMany := []bool{false, true, true}
	if !reflect.DeepEqual(liveHasMany, wantLiveMany) {
		t.Fatalf("db HasMany mismatch: got=%v want=%v", liveHasMany, wantLiveMany)
	}

	liveHasPrefixes, err := db.HasPrefixes([][]byte{
		[]byte("acct/alice/"),
		[]byte("acct/bob/"),
		[]byte("acct/carol/"),
	})
	if err != nil {
		t.Fatalf("db HasPrefixes: %v", err)
	}
	wantLivePrefixes := []bool{false, true, true}
	if !reflect.DeepEqual(liveHasPrefixes, wantLivePrefixes) {
		t.Fatalf("db HasPrefixes mismatch: got=%v want=%v", liveHasPrefixes, wantLivePrefixes)
	}

	stats := db.Stats()
	if got := mustStatUint64(t, stats, "treedb.cache.root_domain_probes.snapshot_hasmany.native_calls"); got != 2 {
		t.Fatalf("snapshot HasMany native calls=%d want 2", got)
	}
	if got := mustStatUint64(t, stats, "treedb.cache.root_domain_probes.snapshot_hasmany.backend_fallback_calls"); got != 1 {
		t.Fatalf("snapshot HasMany backend fallback calls=%d want 1", got)
	}
	if got := mustStatUint64(t, stats, "treedb.cache.root_domain_probes.snapshot_hasprefixes.native_calls"); got != 2 {
		t.Fatalf("snapshot HasPrefixes native calls=%d want 2", got)
	}
	if got := mustStatUint64(t, stats, "treedb.cache.root_domain_probes.snapshot_hasprefixes.fallback_calls"); got != 2 {
		t.Fatalf("snapshot HasPrefixes fallback calls=%d want 2", got)
	}
}

func mustStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stat %q", key)
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse stat %q=%q: %v", key, raw, err)
	}
	return v
}
