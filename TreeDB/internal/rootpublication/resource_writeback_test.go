package rootpublication

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestStableResourceBeginWritebackSelectionAndFullSync(t *testing.T) {
	file := writeStableResourceFixture(t, t.TempDir(), "append", "0123456789")
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	base := StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceValueLog, ResourceID: "1", Generation: 1, DiagnosticPath: "value_vlog/append", File: file, Frontier: DurableFrontier{Bytes: 5}, Reachability: ReachabilityValueLogPointer}
	for _, tc := range []struct {
		name   string
		change func(*StableResourceSpec)
		valid  bool
		hint   bool
	}{
		{name: "value", valid: true, hint: true},
		{name: "raw", change: func(s *StableResourceSpec) {
			s.Kind = ResourceOuterLeafLog
			s.Reachability = ReachabilityOuterLeafRawPointer
		}, valid: true, hint: true},
		{name: "generic", change: func(s *StableResourceSpec) { s.MetadataOwner = nil }, valid: true},
		{name: "synced", change: func(s *StableResourceSpec) { s.ContentSynced = true }, valid: true},
		{name: "custom-flush", change: func(s *StableResourceSpec) {
			s.MetadataOwner = nil
			s.FlushThrough = func(*os.File, DurableFrontier) error { return nil }
		}, valid: true},
		{name: "custom-sync", change: func(s *StableResourceSpec) {
			s.MetadataOwner = nil
			s.SyncThrough = func(*os.File, DurableFrontier) error { return nil }
		}, valid: true},
		{name: "owned-custom-flush", change: func(s *StableResourceSpec) { s.FlushThrough = func(*os.File, DurableFrontier) error { return nil } }},
		{name: "owned-custom-sync", change: func(s *StableResourceSpec) { s.SyncThrough = func(*os.File, DurableFrontier) error { return nil } }},
		{name: "owned-operations", change: func(s *StableResourceSpec) { s.OwnedOperations = &ownedTokenOperationsFixture{owner: &owner} }, valid: true},
		{name: "index", change: func(s *StableResourceSpec) { s.Kind = ResourceIndex; s.Reachability = ReachabilityIndexFile }, valid: true},
		{name: "wal", change: func(s *StableResourceSpec) {
			s.Kind = ResourceCommandWAL
			s.Reachability = ReachabilityCommandWALActive
		}, valid: true},
		{name: "external-rid", change: func(s *StableResourceSpec) {
			s.Kind = ResourceCommandWALExternalRID
			s.Reachability = ReachabilityCommandWALExternalRIDFence
		}, valid: true},
		{name: "mismatched", change: func(s *StableResourceSpec) { s.Reachability = ReachabilityOuterLeafRawPointer }, valid: true},
		{name: "recovery-default", change: func(s *StableResourceSpec) { s.ContentSynced = true }, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := base
			if tc.change != nil {
				tc.change(&spec)
			}
			token, err := NewStableResourceToken(spec)
			if !tc.valid {
				if !errors.Is(err, ErrResourceOwnership) || token != nil {
					t.Fatalf("token=%v error=%v", token, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer token.Release()
			if got := reflect.ValueOf(token.flush).Pointer() == reflect.ValueOf(beginResourceWriteback).Pointer(); got != tc.hint {
				t.Fatalf("writeback selected=%v want %v", got, tc.hint)
			}
			if err := token.FlushThrough(); err != nil {
				t.Fatal(err)
			}
			if tc.hint && (token.hasSyncedFrontier || token.metrics.physicalFileSyncs.Load() != 0) {
				t.Fatal("hint earned content-sync credit")
			}
			before, after := 0, 0
			if err := token.SyncThroughWithFileSyncObserver(func(*os.File) error { before++; return nil }, func(*os.File) error { after++; return nil }); err != nil {
				t.Fatal(err)
			}
			want := 1
			if spec.ContentSynced {
				want = 0
			}
			if before != want || after != want || token.metrics.physicalFileSyncs.Load() != 1 {
				t.Fatalf("full sync observers=%d/%d physical=%d", before, after, token.metrics.physicalFileSyncs.Load())
			}
		})
	}
	if owner.Bytes() != 0 {
		t.Fatalf("retained metadata=%d", owner.Bytes())
	}
}

func TestStableResourceWritebackCoalescedImportClonePinnedFrontier(t *testing.T) {
	dir := t.TempDir()
	file := writeStableResourceFixture(t, dir, "append", "0123456789")
	originalInfo, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var producer, consumer retainedalloc.Owner
	producer.Initialize(0)
	consumer.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	spec := StableResourceSpec{MetadataOwner: &producer, PinRegistry: registry, Kind: ResourceValueLog, ResourceID: "1", Generation: 1, DiagnosticPath: "value_vlog/append", File: file, Frontier: DurableFrontier{Bytes: 5}, Reachability: ReachabilityValueLogPointer}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	// Observe this test-owned callback only; production retains a static function.
	originalFlush := token.flush
	originalSync := token.sync
	var syncedBytes uint64
	token.sync = func(file *os.File, frontier DurableFrontier) error {
		syncedBytes = frontier.Bytes
		return originalSync(file, frontier)
	}
	calls := 0
	token.flush = func(pinned *os.File, frontier DurableFrontier) error {
		info, err := pinned.Stat()
		if err != nil {
			return err
		}
		if !os.SameFile(originalInfo, info) || frontier.Bytes != 10 {
			t.Fatalf("wrong pinned identity or coalesced frontier: %d", frontier.Bytes)
		}
		calls++
		return originalFlush(pinned, frontier)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&producer)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(token); err != nil {
		token.Release()
		t.Fatal(err)
	}
	spec.Frontier.Bytes = 10
	later, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Add(later); err != nil {
		later.Release()
		t.Fatal(err)
	}
	source, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ImportStableResourceSetMetadata(&consumer, source)
	if err != nil {
		t.Fatal(err)
	}
	clone, err := CloneStableResourceSetExcludingKinds(imported)
	if err != nil {
		t.Fatal(err)
	}
	source.Release()
	imported.Release()
	// Windows disallows renaming this open file. Keep the ownership, coalescing,
	// import/clone and full-sync assertions below on every platform; additionally
	// replace the diagnostic path where open-file rename is supported.
	if runtime.GOOS != "windows" {
		if err = os.Rename(dir+"/append", dir+"/moved"); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(dir+"/append", []byte("replacement"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = clone.FlushThrough(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || token.hasSyncedFrontier || token.metrics.physicalFileSyncs.Load() != 0 {
		t.Fatal("hint callback missing or earned durability credit")
	}
	if err = clone.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if token.metrics.physicalFileSyncs.Load() != 1 || syncedBytes != 10 {
		t.Fatal("full sync failed to cover greatest frontier")
	}
	clone.Release()
	if producer.Bytes() != 0 || consumer.Bytes() != 0 {
		t.Fatalf("metadata retained=%d/%d", producer.Bytes(), consumer.Bytes())
	}
}
