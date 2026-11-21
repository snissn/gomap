package treedb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func acceptedPressureOptions(t *testing.T, profile Profile) Options {
	t.Helper()
	opts := indexedSliceOptions(profile, t.TempDir())
	opts.MaxWALBytes = 128 << 20 // Run the actual size worker without reaching its byte threshold.
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	opts.MemtableShards = 2
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxSources = 4
	opts.COWMemtableLimits.MaxGenerations = 8
	opts.DisableBackgroundPrune = true
	opts.BackgroundIndexVacuumInterval = -1
	return opts
}

func TestCOWPublicAcceptedPressureOverlapsFollowingWrite(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	const (
		capacity = 32
		trigger  = capacity * 3 / 4
		wait     = 5 * time.Second
	)
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			opts := acceptedPressureOptions(t, profile)
			maintenanceErr := make(chan error, 1)
			opts.NotifyError = func(err error) {
				select {
				case maintenanceErr <- err:
				default:
				}
			}
			open := func() (*DB, *backenddb.DB, *collections.CollectionManager) {
				t.Helper()
				db, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				backend, err := db.NativeSemanticBackend()
				if err != nil {
					t.Fatal(err)
				}
				manager := collections.NewCollectionManager(backend)
				t.Cleanup(func() { _ = manager.Close() })
				return db, backend, manager
			}
			closeDB := func(db *DB, manager *collections.CollectionManager) {
				t.Helper()
				cache := db.cached
				if err := manager.Close(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				requirePublicCOWFinalZero(t, cache)
			}
			db, backend, manager := open()
			_, err := manager.CreateCollection(&collections.CollectionMeta{
				Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
			})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			id := func(n int) []byte { return []byte(fmt.Sprintf("row-%03d", n)) }
			doc := func(n int) []byte {
				return nativeBSONDocument(t, bson.D{{Key: "_id", Value: string(id(n))}, {Key: "n", Value: int64(n)}})
			}
			insert := func(n int, body []byte) error {
				_, err := rows.InsertBatchValidatedBSON([][]byte{id(n)}, [][]byte{body})
				return err
			}
			if err := insert(0, doc(0)); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			held, err := rows.OpenCollectionReadView()
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			prefix, _, err := backend.PrivateCOWAcceptedPrefix(db.txnOwner)
			if err != nil {
				t.Fatal(err)
			}
			original := prefix.LogicalFrontiers()
			if original.V != original.S.LogicalSequence || original.C != original.S.CanonicalLSN {
				t.Fatalf("initial checkpoint did not retire original prefix: %+v", original)
			}
			cut := rootpublication.SealedPrefix{LogicalSequence: original.V + trigger, CanonicalLSN: original.C + trigger}
			prefixHeld, releasePrefix := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(releasePrefix) })
			var claimed, followingAppended, followingSynced atomic.Bool
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceValueLog &&
					len(event.Paths) > 0 && strings.HasPrefix(filepath.Clean(event.Paths[0]), filepath.Clean(opts.Dir)+string(filepath.Separator)) &&
					claimed.CompareAndSwap(false, true) {
					close(prefixHeld)
					<-releasePrefix
				}
				if event.Resource == durabilitycut.ResourceCommandWAL {
					if event.Point == durabilitycut.AfterDependencyAppend && event.LSN == cut.CanonicalLSN+1 {
						followingAppended.Store(true)
					}
					if event.Point == durabilitycut.AfterDependencyFileSync && event.StablePath != nil && followingAppended.Load() {
						followingSynced.Store(true)
					}
				}
				return nil
			})
			defer restore()
			defer unblock()
			for n := 1; n <= trigger; n++ {
				if err := insert(n, doc(n)); err != nil {
					t.Fatalf("ordinary slot %d: %v", n, err)
				}
			}
			select {
			case <-prefixHeld:
			case err := <-maintenanceErr:
				t.Fatalf("original worker failed before physical Apply: %v", err)
			case <-time.After(wait):
				t.Fatalf("original worker did not begin physical Apply before hard exhaustion: pending=%d %+v", prefix.AcceptedPendingCount(), prefix.LogicalFrontiers())
			}
			if frontiers := prefix.LogicalFrontiers(); frontiers.V != cut.LogicalSequence || frontiers.C != cut.CanonicalLSN || frontiers.S != original.S {
				t.Fatalf("held Apply does not own expected unsealed prefix: %+v, cut %+v", frontiers, cut)
			}
			done := make(chan error, 1)
			following := doc(trigger + 1)
			go func() { done <- insert(trigger+1, following) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("following native INSERT failed with old Apply held: %v", err)
				}
			case <-time.After(wait):
				unblock()
				select {
				case <-done:
				case <-time.After(wait):
					t.Error("following native INSERT failed to join after release")
				}
				t.Fatal("following native INSERT could not finish while old Apply held")
			}
			if frontiers := prefix.LogicalFrontiers(); frontiers.V != cut.LogicalSequence+1 || frontiers.C != cut.CanonicalLSN+1 || frontiers.S != original.S ||
				(profile == ProfileCommandWALDurable && (frontiers.W != cut.CanonicalLSN+1 || !followingSynced.Load())) {
				t.Fatalf("following INSERT has no original completion with old Apply held: %+v, cut %+v", frontiers, cut)
			}
			got, err := held.FetchDocumentsByID([][]byte{id(0), id(trigger + 1)}, collections.DocumentFetchOptions{})
			if err != nil || len(got.Results) != 2 || !got.Results[0].Found || got.Results[1].Found {
				t.Fatalf("held old cut changed: %+v %v", got, err)
			}
			if value, ok := bson.Raw(got.Results[0].Document).Lookup("n").Int64OK(); !ok || value != 0 {
				t.Fatalf("held row changed: %v", bson.Raw(got.Results[0].Document))
			}
			unblock()
			ctx, cancel := context.WithTimeout(context.Background(), wait)
			defer cancel()
			if err := prefix.WaitAccepted(ctx, cut); err != nil {
				t.Fatal(err)
			}
			if got := prefix.LogicalFrontiers(); got.S != cut || got.C != cut.CanonicalLSN+1 || prefix.AcceptedPendingCount() != 1 {
				t.Fatalf("original seal retired the wrong prefix: %+v, want %+v and one live suffix", got, cut)
			}
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			closeDB(db, manager)

			db, _, manager = open()
			rows, err = manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			for n := 0; n <= trigger+1; n++ {
				body, err := rows.Get(id(n))
				if err != nil {
					t.Fatal(err)
				}
				actual, ok := bson.Raw(body).Lookup("n").Int64OK()
				if !ok || actual != int64(n) {
					t.Fatalf("typed reopen row %d: %v", n, bson.Raw(body))
				}
			}
			closeDB(db, manager)
		})
	}
}

func TestCOWPublicAcceptedPressureReportsOriginalSealFailure(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := acceptedPressureOptions(t, ProfileCommandWALDurable)
	report := make(chan error, 1)
	opts.NotifyError = func(err error) {
		select {
		case report <- err:
		default:
		}
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	backend, err := db.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	defer manager.Close()
	_, err = manager.CreateCollection(&collections.CollectionMeta{
		Name: "rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	prefix, _, err := backend.PrivateCOWAcceptedPrefix(db.txnOwner)
	if err != nil {
		t.Fatal(err)
	}
	original := prefix.LogicalFrontiers()
	failure := errors.New("injected accepted pressure seal failure")
	var sealAttempts atomic.Int32
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.BeforePublicationSealWrite && event.Resource == durabilitycut.ResourceSeal && event.Root == backend.Dir() {
			sealAttempts.Add(1)
			return failure
		}
		return nil
	})
	defer restore()
	const trigger = 24
	for n := 0; n < trigger; n++ {
		id := fmt.Sprintf("row-%03d", n)
		body := nativeBSONDocument(t, bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(n)}})
		if _, err := rows.InsertBatchValidatedBSON([][]byte{[]byte(id)}, [][]byte{body}); err != nil {
			t.Fatalf("durable INSERT %d: %v", n, err)
		}
	}
	select {
	case err := <-report:
		if !errors.Is(err, failure) {
			t.Fatalf("original seal error lost: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original seal failure did not reach the owner")
	}
	current := prefix.LogicalFrontiers()
	if current.S != original.S || current.C != original.C+trigger || current.W != current.C || sealAttempts.Load() != 1 {
		t.Fatalf("failed seal changed original durability: before=%+v after=%+v attempts=%d", original, current, sealAttempts.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := prefix.WaitAccepted(ctx, rootpublication.SealedPrefix{LogicalSequence: current.V, CanonicalLSN: current.C}); !errors.Is(err, failure) {
		t.Fatalf("failed seal was retired or lost: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, failure) {
		t.Fatalf("checked Close lost original seal failure: %v", err)
	}
}
