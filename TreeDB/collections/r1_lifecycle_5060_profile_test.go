package collections

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type r1LifecycleProfileRecord5060 struct {
	Opener     string                 `json:"opener"`
	Profile    string                 `json:"profile"`
	Durability string                 `json:"durability"`
	Effective  map[string]bool        `json:"effective"`
	Persisted  backenddb.FormatConfig `json:"persisted"`
}

// Read scalar configuration only while this fresh/reopened backend is quiesced.
// This test-only inspection fails when the runtime field contract changes; it
// adds no production API and never accesses pointers through unsafe.
func r1LifecycleProfile5060(t testing.TB, db *backenddb.DB) r1LifecycleProfileRecord5060 {
	t.Helper()
	cfg, ok, err := backenddb.LoadFormatConfig(db.Dir())
	if err != nil || !ok {
		t.Fatalf("supported profile missing persisted format: %v", err)
	}
	value := reflect.ValueOf(db).Elem()
	actual := make(map[string]bool)
	for key, field := range map[string]string{"outer": "indexOuterLeavesInValueLog", "packed": "indexPackedValuePtr", "prefix": "leafPrefixCompression", "columnar": "indexColumnarLeaves", "internal_base": "indexInternalBaseDelta", "command_wal": "commandWAL"} {
		v := value.FieldByName(field)
		if !v.IsValid() || v.Kind() != reflect.Bool {
			t.Fatalf("missing effective field %s", field)
		}
		actual[key] = v.Bool()
	}
	actual["disable_background_prune"] = value.FieldByName("pruner").FieldByName("stopCh").IsNil()
	manager := value.FieldByName("valueLogManager").Elem()
	actual["verified_reads"] = !manager.FieldByName("disableReadChecksum").Bool()
	actual["current_writable_mmap"] = manager.FieldByName("currentWritableMmap").FieldByName("v").Uint() != 0
	want := map[string]bool{"outer": true, "packed": true, "prefix": true, "columnar": true, "internal_base": false, "command_wal": true, "disable_background_prune": true, "verified_reads": true, "current_writable_mmap": false}
	if !reflect.DeepEqual(actual, want) || db.ResolvedProfile() != backenddb.ProfileCommandWALDurable || db.DurabilityMode() != backenddb.DurabilityDurable || !cfg.IndexOuterLeavesInValueLog || !cfg.IndexPackedValuePtr || !cfg.LeafPrefixCompression || !cfg.IndexColumnarLeaves || cfg.IndexInternalBaseDelta || cfg.DurabilityProfile != backenddb.ProfileCommandWALDurable || !cfg.RequiresCommandWALV2() {
		t.Fatalf("unsupported effective/persisted profile: effective=%v persisted=%+v profile=%s durability=%v", actual, cfg, db.ResolvedProfile(), db.DurabilityMode())
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		t.Fatalf("profile maintenance admission: %v", err)
	}
	return r1LifecycleProfileRecord5060{Opener: "OptionsFor(ProfileCommandWALDurable)+OpenBackend", Profile: string(db.ResolvedProfile()), Durability: "durable", Effective: actual, Persisted: cfg}
}

func r1LifecycleOpen5060(t testing.TB, dir string) (*backenddb.DB, func() error, r1LifecycleProfileRecord5060) {
	t.Helper()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.DisableBackgroundPrune = true
	db, closeOwner, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	// Register cleanup before any assertion; returned owner closes backend first,
	// then the dictionary/template owners. No sleep or leaked-worker suppression.
	closed := false
	cleanup := func() error {
		if closed {
			return nil
		}
		closed = true
		return closeOwner()
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Errorf("profile owner close: %v", err)
		}
	})
	return db, cleanup, r1LifecycleProfile5060(t, db)
}

func r1LifecycleNew5060(t testing.TB, indexed bool) (string, *backenddb.DB, *Collection, func() error) {
	t.Helper()
	dir := t.TempDir()
	db, cleanup, _ := r1LifecycleOpen5060(t, dir)
	meta := r1MutationMeta5059(indexed)
	manager := NewCollectionManager(db)
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	return dir, db, col, cleanup
}

type r1LifecycleFallbackState5060 struct {
	CommitSeq  uint64                      `json:"commit_seq"`
	UserRoot   uint64                      `json:"user_root"`
	SystemRoot uint64                      `json:"system_root"`
	AppliedLSN uint64                      `json:"applied_lsn"`
	NextLSN    uint64                      `json:"next_lsn"`
	Roots      []backenddb.RecoverableRoot `json:"roots"`
	Slots      map[string]string           `json:"slots"`
}

type r1LifecycleFinalRecord5060 struct {
	RefreshNS int64                           `json:"refresh_ns"`
	Before    r1LifecycleFallbackState5060    `json:"before"`
	After     r1LifecycleFallbackState5060    `json:"after"`
	TypedGCNS int64                           `json:"typed_gc_ns"`
	TypedGC   ColumnAssetGCStats              `json:"typed_gc"`
	LeafGCNS  int64                           `json:"leaf_gc_ns"`
	LeafGC    backenddb.LeafGenerationGCStats `json:"leaf_gc"`
}

func r1LifecycleFallbackStateCapture5060(t testing.TB, db *backenddb.DB) r1LifecycleFallbackState5060 {
	t.Helper()
	set, err := db.CaptureRecoverableRootSetForInspection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	roots := set.Roots()
	set.Release()
	state := db.State()
	slots := make(map[string]string)
	for key, value := range db.Stats() {
		if strings.HasPrefix(key, "treedb.durable_root.") {
			slots[key] = value
		}
	}
	return r1LifecycleFallbackState5060{CommitSeq: state.CommitSeq, UserRoot: state.RootPageID, SystemRoot: state.SystemRootPageID, AppliedLSN: state.AppliedCommandLSN, NextLSN: db.CommandWALNextLSN(), Roots: roots, Slots: slots}
}

// Run after all root-changing maintenance. GC may publish its own physical
// manifest revisions; the recorded fallback boundary is immediately before GC.
func r1LifecycleFinal5060(t testing.TB, db *backenddb.DB, col *Collection, candidates *[]ColumnAssetRef) r1LifecycleFinalRecord5060 {
	t.Helper()
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		t.Fatalf("final maintenance admission: %v", err)
	}
	result := r1LifecycleFinalRecord5060{Before: r1LifecycleFallbackStateCapture5060(t, db)}
	result.RefreshNS = r1LifecycleTime5060(t, db.RefreshCommandWALCheckpointFallback)
	result.After = r1LifecycleFallbackStateCapture5060(t, db)
	before, after := result.Before, result.After
	if before.UserRoot != after.UserRoot || before.SystemRoot != after.SystemRoot || before.AppliedLSN != after.AppliedLSN || before.NextLSN != after.NextLSN || after.CommitSeq < before.CommitSeq {
		t.Fatal("fallback refresh changed logical roots/coverage")
	}
	durable := 0
	for _, root := range after.Roots {
		if root.Durable {
			durable++
			if root.UserRootPageID != after.UserRoot || root.SystemRootPageID != after.SystemRoot || root.AppliedCommandLSN != after.AppliedLSN {
				t.Fatalf("fallback did not converge: %+v", root)
			}
		}
	}
	if durable != 2 {
		t.Fatalf("durable fallback slots=%d want 2: %s", durable, fmt.Sprint(after.Roots))
	}
	var gc ColumnAssetGCStats
	result.TypedGCNS = r1LifecycleTime5060(t, func() (err error) {
		gc, err = col.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{SegmentDetails: true, CandidateRefs: *candidates})
		return err
	})
	r1LifecycleRetireCandidates5060(t, gc, candidates)
	result.TypedGC = r1LifecycleAggregateGC5060(gc)
	result.LeafGCNS = r1LifecycleTime5060(t, func() (err error) {
		result.LeafGC, err = db.LeafGenerationGC(context.Background(), backenddb.LeafGenerationGCOptions{})
		return err
	})
	return result
}
