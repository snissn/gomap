//go:build !windows

package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// CREATE crosses actual public maintenance after staging, with no recapture or
// retry. The cold arm requires a different paired pager/catalog; the published
// arm checkpoints pending rows while keeping logical sequence/LSN unchanged.
func TestNativeCreatePreservesPhysicalRebase(t *testing.T) {
	for _, mode := range []string{"cold_move", "published_checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
			opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, t.TempDir())
			opts.MemtableMode = "cow_btree"
			opts.FlushThreshold = 1 << 30
			opts.BackgroundCheckpointInterval = -1
			opts.BackgroundCheckpointIdleDuration = -1
			opts.MaxWALBytes = -1
			var owner *treedb.DB
			var backend *backenddb.DB
			var manager *CollectionManager
			var held, middle, current backenddb.NativeSemanticCapture
			var budget interface{ Stats() memtable.COWStats }
			closeAll := func() error {
				var err error
				for _, slot := range []*backenddb.NativeSemanticCapture{&current, &middle, &held} {
					if *slot != nil {
						c := *slot
						*slot = nil
						err = errors.Join(err, c.Close())
					}
				}
				if manager != nil {
					m := manager
					manager = nil
					err = errors.Join(err, m.Close())
				}
				if owner != nil {
					o := owner
					owner = nil
					backend = nil
					err = errors.Join(err, o.Close())
				}
				if budget != nil {
					if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
						err = errors.Join(err, fmt.Errorf("full close retained admission: %+v", stats))
					}
					budget = nil
				}
				return err
			}
			t.Cleanup(func() {
				if err := closeAll(); err != nil {
					t.Error(err)
				}
			})
			open := func() {
				t.Helper()
				var err error
				owner, err = treedb.Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				backend, err = owner.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager = NewCollectionManager(backend)
				if !manager.UsesNativeCommandWALApply() {
					t.Fatal("INVALID_WITNESS: native owner inactive")
				}
			}
			open()
			if _, err := manager.CreateCollection(&CollectionMeta{Name: "seed"}); err != nil {
				t.Fatal(err)
			}
			seed, err := manager.OpenCollection("seed")
			if err != nil {
				t.Fatal(err)
			}
			document := []byte(`{"payload":"` + strings.Repeat("x", 256) + `"}`)
			id := func(row int) []byte { return []byte(fmt.Sprintf("d%03d", row)) }
			for batch := 0; batch < 2; batch++ {
				ids, docs := make([][]byte, 128), make([][]byte, 128)
				for i := range ids {
					ids[i], docs[i] = id(batch*128+i), document
				}
				if _, err := seed.InsertBatch(ids, docs); err != nil {
					t.Fatal(err)
				}
				if err := owner.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}
			if err := closeAll(); err != nil {
				t.Fatalf("seed checked Close: %v", err)
			}
			open()
			seed, err = manager.OpenCollection("seed")
			if err != nil {
				t.Fatal(err)
			}
			published := mode == "published_checkpoint"
			if published {
				if _, err := seed.Insert([]byte("pending"), []byte(`{"pending":true}`)); err != nil {
					t.Fatal(err)
				}
			}
			held, err = backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			budget = held.NativeAllocator().(interface{ Stats() memtable.COWStats })
			before := held.Snapshot()
			beforeToken, valid := before.StateToken()
			beforePager := before.Pager()
			beforeCatalog, err := loadCollectionCatalog(before, "seed")
			if err != nil || beforeCatalog == nil {
				t.Fatalf("original physical catalog: %v", err)
			}
			beforeRoot := beforeCatalog.rootID(collectionPrimaryRootName("seed"))
			sequence, nextLSN := held.LogicalSequence(), backend.CommandWALNextLSN()
			if !valid || beforePager == nil || beforeRoot == 0 || (held.State() != nil) != published {
				t.Fatal("INVALID_WITNESS: cold/published opening not activated")
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var unblockOnce sync.Once
			unblock := func() { unblockOnce.Do(func() { close(release) }) }
			var admissions atomic.Int32
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				rel, err := filepath.Rel(opts.Dir, e.Root)
				if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
					return nil
				}
				if e.Point == durabilitycut.BeforeCOWAdmission && admissions.Add(1) == 1 {
					close(entered)
					<-release
				}
				return nil
			})
			done := make(chan error, 1)
			joined := false
			var checkpointDone chan error
			defer func() {
				unblock()
				if !joined {
					select {
					case err := <-done:
						if err != nil {
							t.Errorf("target join cleanup: %v", err)
						}
					case <-time.After(10 * time.Second):
						t.Error("target cleanup join timed out")
					}
				}
				if checkpointDone != nil {
					select {
					case err := <-checkpointDone:
						if err != nil {
							t.Errorf("checkpoint cleanup join: %v", err)
						}
					case <-time.After(10 * time.Second):
						t.Error("checkpoint cleanup join timed out")
					}
				}
				restore()
			}()
			go func() { _, err := manager.CreateCollection(&CollectionMeta{Name: "target"}); done <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("INVALID_WITNESS: CREATE did not park")
			}
			if published {
				checkpointDone = make(chan error, 1)
				go func(done chan error) { done <- owner.Checkpoint() }(checkpointDone)
				select {
				case err := <-checkpointDone:
					checkpointDone = nil
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("public Checkpoint timed out")
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := owner.VacuumIndexOnline(ctx); err != nil {
					t.Fatalf("original public vacuum: %v", err)
				}
				work := owner.VacuumOnlineStats()
				if !work.WorkCompleted || work.Canceled || work.ReplacementPagerPages == 0 {
					t.Fatalf("INVALID_WITNESS: vacuum %+v", work)
				}
			}
			middle, err = backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			afterToken, valid := middle.Snapshot().StateToken()
			afterPager := middle.Snapshot().Pager()
			afterCatalog, err := loadCollectionCatalog(middle.Snapshot(), "seed")
			if err != nil || afterCatalog == nil {
				t.Fatalf("actual maintained catalog: %v", err)
			}
			afterRoot := afterCatalog.rootID(collectionPrimaryRootName("seed"))
			if !valid || middle.LogicalSequence() != sequence || backend.CommandWALNextLSN() != nextLSN || (middle.State() != nil) != published || afterToken.SystemRootPageID == beforeToken.SystemRootPageID || afterRoot == 0 || afterRoot == beforeRoot {
				t.Fatalf("INVALID_WITNESS: physical-only movement missing: logical=%d->%d nextLSN=%d->%d token=%+v->%+v root=%d->%d", sequence, middle.LogicalSequence(), nextLSN, backend.CommandWALNextLSN(), beforeToken, afterToken, beforeRoot, afterRoot)
			}
			if published {
				if middle.State() == held.State() {
					t.Fatal("INVALID_WITNESS: checkpoint did not refresh published state")
				}
			} else if afterPager == beforePager {
				t.Fatal("INVALID_WITNESS: cold vacuum did not replace pager")
			}
			t.Logf("CREATE_PHYSICAL mode=%s state_moved=%t pager_moved=%t system=%d->%d primary=%d->%d logical=%d nextLSN=%d", mode, middle.State() != held.State(), afterPager != beforePager, beforeToken.SystemRootPageID, afterToken.SystemRootPageID, beforeRoot, afterRoot, sequence, nextLSN)
			observer, err := backenddb.NewNativeInstallObserverForTest(backend, commitlog.CommandKindCatalogCreateCollection, nextLSN)
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Close()
			unblock()
			select {
			case err := <-done:
				joined = true
				if err != nil {
					t.Fatalf("original once-only CREATE: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("CREATE did not join")
			}
			restore()
			restore = func() {}
			select {
			case event := <-observer.Events():
				if observer.Dropped() || !event.Consumed || !event.Assigned || !event.LogicalInstalled || !event.WALCompleted || event.AssignedLSN != nextLSN || event.LogicalSequence != sequence+1 {
					t.Errorf("CREATE acknowledgement: %+v", event)
				}
			default:
				t.Error("missing CREATE acknowledgement")
			}
			select {
			case event := <-observer.Events():
				t.Errorf("duplicate CREATE acknowledgement: %+v", event)
			default:
			}
			observer.Close()
			if admissions.Load() != 1 || backend.CommandWALNextLSN() != nextLSN+1 {
				t.Errorf("CREATE retried/assigned twice: admissions=%d nextLSN=%d", admissions.Load(), backend.CommandWALNextLSN())
			}
			current, err = backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			state, ok := current.State().(*nativeEngineVersion)
			if !ok {
				t.Fatal("missing accepted native state")
			}
			original, found := state.names.Get("seed")
			if !found {
				t.Fatal("unrelated collection disappeared")
			}
			domain, found := original.domains.Get("primary")
			if !found || domain.catalogRoot != afterRoot || current.Snapshot().Pager() != afterPager {
				t.Fatalf("accepted CREATE retained stale physical provenance: domain=%+v", domain)
			}
			if _, found := state.names.Get("target"); !found {
				t.Error("accepted CREATE missing descriptor after plan retirement")
			}
			if token, ok := held.Snapshot().StateToken(); !ok || token != beforeToken || held.Snapshot().Pager() != beforePager {
				t.Error("original pinned cut changed")
			}
			for row := 0; row < 256; row++ {
				if got, err := seed.Get(id(row)); err != nil || !bytes.Equal(got, document) {
					t.Fatalf("current seed %d: %v", row, err)
				}
			}
			if published {
				if got, err := seed.Get([]byte("pending")); err != nil || !bytes.Equal(got, []byte(`{"pending":true}`)) {
					t.Fatalf("checkpointed pending ACK: %s %v", got, err)
				}
			}
			if err := closeAll(); err != nil {
				t.Fatalf("original checked Close: %v", err)
			}
			open()
			if target, err := manager.OpenCollection("target"); err != nil || target == nil {
				t.Fatalf("fresh reopen CREATE: %v", err)
			}
			seed, err = manager.OpenCollection("seed")
			if err != nil {
				t.Fatal(err)
			}
			for row := 0; row < 256; row++ {
				if got, err := seed.Get(id(row)); err != nil || !bytes.Equal(got, document) {
					t.Fatalf("reopened seed %d: %v", row, err)
				}
			}
			if published {
				if got, err := seed.Get([]byte("pending")); err != nil || !bytes.Equal(got, []byte(`{"pending":true}`)) {
					t.Fatalf("reopened pending ACK: %s %v", got, err)
				}
			}
			if backend.CommandWALNextLSN() != nextLSN+1 {
				t.Error("reopen changed command identity")
			}
			if err := closeAll(); err != nil {
				t.Fatalf("reopened checked Close: %v", err)
			}
		})
	}
}

// A genuine intervening CREATE changes original catalog dependencies. The
// staged operation must not silently acquire a new incarnation or retry.
func TestNativeCreateOriginalCatalogConflict(t *testing.T) {
	for _, conflictName := range []string{"target", "other"} {
		t.Run(conflictName, func(t *testing.T) {
			_, backend, _, col := historyExpiryFixture(t, &CollectionMeta{Name: "seed"})
			capture, err := backend.CaptureNativeSemanticState()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := capture.Close(); err != nil {
					t.Error(err)
				}
			}()
			base, ok := capture.State().(*nativeEngineVersion)
			if !ok {
				t.Fatal("missing native opening")
			}
			native, err := prepareNativeJSONCreatePlan(base, CollectionMeta{Name: "target"})
			if err != nil {
				t.Fatal(err)
			}
			plan := native.Reprepare.(*nativeJSONCreatePlan)
			plan.bootstrap, plan.opening = base, capture
			defer func() {
				if err := plan.close(); err != nil {
					t.Error(err)
				}
			}()
			native.Basis, native.StartSequence = capture.State(), capture.LogicalSequence()
			payload, err := EncodeCatalogCreateCollectionCommandWALPayload(CollectionMeta{Name: "target"})
			if err != nil {
				t.Fatal(err)
			}
			intent, err := backend.NewTrustedCommandWALIntent(commitlog.CommandKindCatalogCreateCollection, commitlog.CommandScopeCatalog, commitlog.PayloadFormatCatalogCreateCollectionV1, payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := col.nativeJSON.CreateCollection(CollectionMeta{Name: conflictName}); err != nil {
				t.Fatal(err)
			}
			before := backend.CommandWALNextLSN()
			var appends, installs atomic.Int32
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == durabilitycut.BeforeDependencyAppend && e.Resource == durabilitycut.ResourceCommandWAL {
					appends.Add(1)
				}
				if e.Point == durabilitycut.BeforeCOWCutSwap {
					installs.Add(1)
				}
				return nil
			})
			defer restore()
			result, writeErr := backend.WriteNativePublication(native, intent, false)
			if !result.Consumed {
				writeErr = errors.Join(writeErr, cancelPrivateNativePublication(native))
			}
			if !errors.Is(writeErr, backenddb.ErrConcurrentModification) || result.Assigned || result.LogicalInstalled || result.AssignedLSN != 0 || backend.CommandWALNextLSN() != before || appends.Load() != 0 || installs.Load() != 0 {
				t.Fatalf("original conflict changed identity/published: result=%+v before=%d next=%d appends=%d installs=%d err=%v", result, before, backend.CommandWALNextLSN(), appends.Load(), installs.Load(), writeErr)
			}
			t.Logf("CREATE_CONFLICT name=%s result=%+v nextLSN=%d append=%d install=%d error=%v", conflictName, result, before, appends.Load(), installs.Load(), writeErr)
		})
	}
}

// A postappend error is an assigned completion failure, never permission to
// retry this CREATE. The existing durability hook only exercises that boundary.
func TestNativeCreateAssignedFailureDoesNotRetry(t *testing.T) {
	failure := errors.New("CREATE completion control")
	owner, err := treedb.Open(nativeTypedGroupOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil && !errors.Is(err, failure) {
			t.Errorf("checked Close: %v", err)
		}
	}()
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	defer func() {
		if err := manager.Close(); err != nil && !errors.Is(err, failure) {
			t.Errorf("checked manager Close: %v", err)
		}
	}()
	if _, err := manager.CreateCollection(&CollectionMeta{Name: "seed"}); err != nil {
		t.Fatal(err)
	}
	before := backend.CommandWALNextLSN()
	var admissions, appends atomic.Int32
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWAdmission {
			admissions.Add(1)
		}
		if e.Point == durabilitycut.AfterDependencyAppend && e.Resource == durabilitycut.ResourceCommandWAL {
			appends.Add(1)
			return failure
		}
		return nil
	})
	defer func() { restore() }()
	created, err := manager.CreateCollection(&CollectionMeta{Name: "target"})
	restore()
	restore = func() {}
	if created != nil || !errors.Is(err, failure) || !errors.Is(err, ErrCommitAmbiguous) || admissions.Load() != 1 || appends.Load() != 1 || backend.CommandWALNextLSN() != before+1 {
		t.Fatalf("assigned CREATE retried/lost completion error: created=%+v admission=%d appends=%d nextLSN=%d before=%d err=%v", created, admissions.Load(), appends.Load(), backend.CommandWALNextLSN(), before, err)
	}
	t.Logf("CREATE_ASSIGNED admissions=%d append=%d nextLSN=%d->%d error=%v", admissions.Load(), appends.Load(), before, backend.CommandWALNextLSN(), err)
}
