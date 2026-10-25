package collections

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Independent facades must derive alreadyExisted from the same namespace owner,
// rather than a pre-read followed by an independently published create.
func TestNativeCreateCollectionWithStatus(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := treedb.OptionsFor(profile, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			owner, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := owner.NativeSemanticBackend()
			if err != nil {
				owner.Close()
				t.Fatal(err)
			}
			var managers []*CollectionManager
			defer func() {
				for _, manager := range managers {
					if err := manager.Close(); err != nil {
						t.Error(err)
					}
				}
				if err := owner.Close(); err != nil {
					t.Error(err)
				}
			}()
			const writers = 8
			for i := 0; i < writers; i++ {
				managers = append(managers, NewCollectionManager(backend))
			}
			meta := nativeTextPublicMeta()
			type outcome struct {
				meta    *CollectionMeta
				existed bool
				err     error
			}
			out := make(chan outcome, writers)
			start := make(chan struct{})
			var pending sync.WaitGroup
			for _, manager := range managers {
				pending.Add(1)
				go func(manager *CollectionManager) {
					defer pending.Done()
					<-start
					created, existed, err := manager.CreateCollectionWithStatus(*meta)
					out <- outcome{created, existed, err}
				}(manager)
			}
			close(start)
			pending.Wait()
			close(out)
			createdCount := 0
			normalized, err := nativeJSONMeta(*meta)
			if err != nil {
				t.Fatal(err)
			}
			for result := range out {
				if result.err != nil || result.meta == nil || !sameCollectionMeta(*result.meta, normalized) {
					t.Fatalf("concurrent create: %+v", result)
				}
				if !result.existed {
					createdCount++
				}
			}
			if createdCount != 1 {
				t.Fatalf("fresh create statuses=%d want 1", createdCount)
			}
			read, err := managers[0].nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			beforeLSN := backend.CommandWALNextLSN()
			incarnation := read.collection.incarnation
			if incarnation == 0 {
				t.Fatal("create did not install collection identity")
			}
			for _, manager := range managers {
				got, existed, err := manager.CreateCollectionWithStatus(*meta)
				if err != nil || !existed || got == nil {
					t.Fatalf("compatible no-op: existed=%t meta=%v err=%v", existed, got, err)
				}
				// Returned metadata is caller-owned, independent of the catalog.
				got.Indexes[0].Field = "caller-owned"
			}
			bad := *meta.copy()
			bad.Indexes[0].Field = "different"
			if got, existed, err := managers[1].CreateCollectionWithStatus(bad); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), "incompatible") || got != nil || existed {
				t.Fatalf("incompatible create: existed=%t meta=%v err=%v", existed, got, err)
			}
			current, err := managers[0].nativeJSON.Capture(meta.Name)
			if err != nil {
				t.Fatal(err)
			}
			if current.capture.LogicalSequence() != read.capture.LogicalSequence() || current.collection.incarnation != incarnation || !sameCollectionMeta(current.meta, normalized) || backend.CommandWALNextLSN() != beforeLSN {
				current.Close()
				t.Fatal("compatible/incompatible create changed logical state, incarnation, metadata or WAL")
			}
			current.Close()
			called := false
			_, _, err = managers[0].CreateCollectionWithPreparedCommandWALIntentStatus(*meta, func() (*backenddb.CommandWALIntent, error) {
				called = true
				return nil, errors.New("must not run")
			})
			if !errors.Is(err, errCollectionDBNil) || called {
				t.Fatalf("prepared contract changed: callback=%t err=%v", called, err)
			}
			if _, err := managers[0].CreateCollection(nil); err == nil || !strings.Contains(err.Error(), "nil collection metadata") {
				t.Fatalf("ordinary nil metadata contract changed: %v", err)
			}
			if err := managers[0].Close(); err != nil {
				t.Fatal(err)
			}
			if got, existed, err := managers[0].CreateCollectionWithStatus(*meta); !errors.Is(err, backenddb.ErrClosed) || got != nil || existed {
				t.Fatalf("closed create: existed=%t meta=%v err=%v", existed, got, err)
			}
		})
	}
}

func TestNativeCreateCollectionWithStatusRefusal(t *testing.T) {
	c, def, budget := openPinnedNativeHNSWAdmissionFixture(t, nil)
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	captureBytes := nativeHNSWAdmissionCaptureBytes(t, c, budget)
	blocker, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	available := budget.Limits().MaxInFlightBytes - budget.Stats().ReservedBytes
	if available < captureBytes {
		t.Fatal("setup has no room for another capture")
	}
	if err = blocker.Resize(available - captureBytes); err != nil {
		t.Fatal(err)
	}
	meta := CollectionMeta{Name: "new", VectorIndexes: []VectorIndexDefinition{def}}
	dbCoord := collectionDBSchemaCoordinatorForDB(c.nativeJSON.db)
	dbCoord.mu.Lock()
	beforeNamespaces := len(dbCoord.collections)
	dbCoord.mu.Unlock()
	beforeLSN := c.nativeJSON.db.CommandWALNextLSN()
	for attempt := 0; attempt < 3; attempt++ {
		meta.Name = fmt.Sprintf("refused_%d", attempt)
		before := budget.Stats()
		got, existed, err := c.manager.CreateCollectionWithStatus(meta)
		if !errors.Is(err, memtable.ErrCOWCapacity) || got != nil || existed {
			t.Fatalf("attempt %d: existed=%t meta=%v err=%v", attempt, existed, got, err)
		}
		after := budget.Stats()
		if after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases || c.nativeJSON.db.CommandWALNextLSN() != beforeLSN {
			t.Fatalf("refused create changed receipt or WAL: before=%+v after=%+v", before, after)
		}
		dbCoord.mu.Lock()
		afterNamespaces := len(dbCoord.collections)
		dbCoord.mu.Unlock()
		if afterNamespaces != beforeNamespaces {
			t.Fatalf("refused create retained namespace coordinators: before=%d after=%d", beforeNamespaces, afterNamespaces)
		}
	}
	blocker.Close()
	current, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	if current.capture.LogicalSequence() != read.capture.LogicalSequence() || current.capture.State() != read.capture.State() {
		current.Close()
		t.Fatal("refused create installed a catalog successor")
	}
	current.Close()
	if got, existed, err := c.manager.CreateCollectionWithStatus(meta); err != nil || got == nil || existed {
		t.Fatalf("create after release: existed=%t meta=%v err=%v", existed, got, err)
	}
}
