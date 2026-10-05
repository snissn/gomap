package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func compactEndpointFixture(t *testing.T, commandWAL bool) string {
	t.Helper()
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileFast, dir)
	if commandWAL {
		opts = treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	}
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		if err := db.SetSync([]byte(fmt.Sprintf("user/%03d", i)), bytes.Repeat([]byte{byte(i + 1)}, 256)); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if commandWAL {
		backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		// Real logged catalog/document writes create system and collection authority.
		// The endpoint never manufactures catalog contents to advance a horizon.
		manager := collections.NewCollectionManager(backend)
		if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "authority", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}); err != nil {
			_ = cleanup()
			t.Fatal(err)
		}
		collection, err := manager.OpenCollection("authority")
		if err != nil {
			_ = cleanup()
			t.Fatal(err)
		}
		if _, err := collection.Insert([]byte("doc"), []byte(`{"name":"keep"}`)); err != nil {
			_ = cleanup()
			t.Fatal(err)
		}
		if err := collection.Flush(); err != nil {
			_ = cleanup()
			t.Fatal(err)
		}
		if err := backend.Checkpoint(); err != nil {
			_ = cleanup()
			t.Fatal(err)
		}
		if err := cleanup(); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestCompactCommandWALSettlePreservesDataAndReceipt(t *testing.T) {
	for _, mode := range []string{"full", "exhaustive"} {
		t.Run(mode, func(t *testing.T) {
			dir := compactEndpointFixture(t, true)
			backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: dir, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			snap := backend.AcquireSnapshot()
			_, revision, err := snap.GetVersioned([]byte("user/000"))
			if err != nil {
				t.Fatal(err)
			}
			_ = snap.Close()
			if err := cleanup(); err != nil {
				t.Fatal(err)
			}
			var receipt compactEndpointReceipt
			packSupported := rootpublication.StableRelativeNamespaceSupported() && rootpublication.StableCrossParentMoveNoReplaceSupported()
			if mode == "exhaustive" && !packSupported {
				// The CLI exits after emitting this same partial receipt. Call its
				// endpoint seam to inspect the typed error without exiting the test.
				opts := treedb.Options{Dir: dir}
				applyPersistedFormatConfig(dir, &opts)
				receipt, err = runCompactCommandWALSettle(context.Background(), opts, treedb.CompactStorageOptions{Mode: treedb.CompactStorageExhaustive, SyncEachPhase: true})
				if !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) {
					t.Fatalf("unsupported pack promotion error: %v", err)
				}
				if receipt.Endpoint != compactCommandWALSettleEndpoint || receipt.Status != "failed" || receipt.InitialStatus != "failed" || receipt.Error == "" || receipt.CleanupStatus != "succeeded" || len(receipt.Reports) != 1 {
					t.Fatalf("missing partial failure/cleanup receipt: %+v", receipt)
				}
				if receipt.Refresh.Status != "not_started" || receipt.Refresh.CheckpointStatus != "not_started" || receipt.Refresh.SummaryBeforeStatus != "not_started" || receipt.Refresh.SummaryAfterStatus != "not_started" || receipt.LeafGC.Status != "not_started" || receipt.AuditStatus != "not_started" || receipt.Refresh.Basis != nil || receipt.Refresh.Result != nil {
					t.Fatal("unsupported initial compact promoted later operations")
				}
				initial := receipt.Reports[0]
				if initial.DryRun || initial.FullyCompacted || initial.PolicyFullyCompacted || initial.ByteMinimized || len(initial.Phases) == 0 || initial.Phases[len(initial.Phases)-1].Name != "leaf-generation-pack-1" {
					t.Fatal("unsupported pack promoted compact completion")
				}
				seenGC, seenCheckpoint := false, false
				for _, phase := range initial.Phases {
					seenGC = seenGC || phase.Name == "value-log-gc"
					seenCheckpoint = seenCheckpoint || phase.Name == "checkpoint-after-value-log-gc"
				}
				if !seenGC || !seenCheckpoint {
					t.Fatal("fixture did not reach genuine applied phases before unsupported pack")
				}
			} else {
				out := captureStdout(t, func() {
					runCompact(dir, []string{"-rw", "-json", "-mode", mode, "-sync-each-phase", "-command-wal-settle"})
				})
				if err := json.Unmarshal([]byte(out), &receipt); err != nil {
					t.Fatal(err)
				}
				if receipt.Endpoint != compactCommandWALSettleEndpoint || receipt.Status != "completed" || len(receipt.Reports) != 2 || receipt.Error != "" {
					t.Fatalf("endpoint=%q status=%q reports=%d error=%q", receipt.Endpoint, receipt.Status, len(receipt.Reports), receipt.Error)
				}
				pub := receipt.Refresh
				if pub.Status != "succeeded" || pub.CheckpointStatus != "succeeded" || pub.Basis == nil || pub.Result == nil ||
					(pub.Result.CommitSeq != pub.Basis.CommitSeq && pub.Result.CommitSeq != pub.Basis.CommitSeq+1) ||
					pub.Result.RootPageID != pub.Basis.RootPageID || pub.Result.SystemRootPageID != pub.Basis.SystemRootPageID ||
					pub.Result.AppliedCommandLSN != pub.Basis.AppliedCommandLSN || pub.Result.MaxEntryRevision != pub.Basis.MaxEntryRevision ||
					pub.NextLSNBefore == 0 || pub.NextLSNAfter != pub.NextLSNBefore {
					t.Fatal("checkpoint refresh changed root/RID/WAL authority")
				}
				if receipt.InitialStatus != "succeeded" || receipt.AuditStatus != "succeeded" || receipt.CleanupStatus != "succeeded" || receipt.LeafGC.Status != "succeeded" ||
					pub.SummaryBeforeStatus != "succeeded" || pub.SummaryAfterStatus != "succeeded" || receipt.Reports[0].DryRun || !receipt.Reports[1].DryRun {
					t.Fatal("missing executed operation or dry-run audit")
				}
			}
			backend, cleanup, err = treedb.OpenBackend(treedb.Options{Dir: dir, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := cleanup(); err != nil {
					t.Error(err)
				}
			}()
			snap = backend.AcquireSnapshot()
			defer snap.Close()
			for i := 0; i < 32; i++ {
				got, err := snap.Get([]byte(fmt.Sprintf("user/%03d", i)))
				if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i + 1)}, 256)) {
					t.Fatalf("reopened user key %d mismatch: %v", i, err)
				}
			}
			if got, err := snap.Has([]byte("missing")); err != nil || got {
				t.Fatalf("missing key present=%t err=%v", got, err)
			}
			_, afterRevision, err := snap.GetVersioned([]byte("user/000"))
			if err != nil || afterRevision != revision {
				t.Fatalf("entry revision changed: %v", err)
			}
			it, err := snap.Iterator(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for ; it.Valid(); it.Next() {
				count++
			}
			iteratorErr, closeErr := it.Error(), it.Close()
			if iteratorErr != nil || closeErr != nil || count != 32 {
				t.Fatalf("user census=%d err=%v close=%v", count, iteratorErr, closeErr)
			}
			if snap.State().SystemRootPageID == 0 {
				t.Fatal("missing system authority")
			}
			collection, err := collections.NewCollectionManager(backend).OpenCollection("authority")
			if err != nil {
				t.Fatal(err)
			}
			document, err := collection.Get([]byte("doc"))
			if err != nil || !bytes.Equal(document, []byte(`{"name":"keep"}`)) {
				t.Fatalf("collection authority changed: %v", err)
			}
			compactEndpointVerifyRecoverableRoots(t, backend)
			// Convergence on the retained physical leaf fixture is a separate native gate.
		})
	}
}

// Inspect actual captured roots after reopen, including selectable older slots.
// Older views may contain fewer keys; every surviving pointer must decode the
// original fixture bytes. The current-view census above remains exact.
func compactEndpointVerifyRecoverableRoots(t *testing.T, backend *treedbdb.DB) {
	t.Helper()
	roots, err := backend.CaptureRecoverableRootSetForInspection(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	if len(roots.Roots()) == 0 {
		t.Fatal("missing recoverable roots")
	}
	for _, root := range roots.Roots() {
		err := func() (err error) {
			snapshot := roots.AcquireSnapshotForRoot(root)
			if snapshot == nil {
				return errors.New("cannot acquire captured root")
			}
			defer func() { err = errors.Join(err, snapshot.Close()) }()
			if found, err := snapshot.Has([]byte("missing")); err != nil || found {
				return fmt.Errorf("recoverable root miss: found=%t err=%v", found, err)
			}
			it, err := snapshot.Iterator(nil, nil)
			if err != nil {
				return err
			}
			defer func() { err = errors.Join(err, it.Close()) }()
			for ; it.Valid(); it.Next() {
				known := false
				for i := 0; i < 32; i++ {
					if bytes.Equal(it.Key(), []byte(fmt.Sprintf("user/%03d", i))) && bytes.Equal(it.Value(), bytes.Repeat([]byte{byte(i + 1)}, 256)) {
						known = true
						break
					}
				}
				if !known {
					return fmt.Errorf("recoverable root fixture mismatch: key=%q", it.Key())
				}
			}
			return it.Error()
		}()
		if err != nil {
			t.Fatalf("recoverable root %d: %v", root.CommitSeq, err)
		}
	}
	if err := roots.Revalidate(); err != nil {
		t.Fatal(err)
	}
}

func compactEndpointFingerprint(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	result := make(map[string][32]byte)
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err == nil {
			result[path] = sha256.Sum256(raw)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCompactCommandWALSettleRejectsBeforeOpen(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelled=%t", cancelled), func(t *testing.T) {
			dir := compactEndpointFixture(t, cancelled)
			before := compactEndpointFingerprint(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := treedb.ErrCommandWALUnsupported
			if cancelled {
				cancel()
				want = context.Canceled
			}
			receipt, err := runCompactCommandWALSettle(ctx, treedb.Options{Dir: dir}, treedb.CompactStorageOptions{Mode: treedb.CompactStorageExhaustive})
			if !errors.Is(err, want) || len(receipt.Reports) != 0 || receipt.Refresh.Status != "not_started" || receipt.CleanupStatus != "not_started" || !reflect.DeepEqual(before, compactEndpointFingerprint(t, dir)) {
				t.Fatalf("pre-open rejection changed files or started maintenance: %v", err)
			}
		})
	}
}

func TestCompactCommandWALSettleRefreshNoopPreservesAuthority(t *testing.T) {
	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: compactEndpointFixture(t, true)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Error(err)
		}
	}()
	fresh := func() compactCheckpointRefresh {
		return compactCheckpointRefresh{Status: "not_started", CheckpointStatus: "not_started", SummaryBeforeStatus: "not_started", SummaryAfterStatus: "not_started"}
	}
	first := fresh()
	if err := refreshCompactCheckpoint(context.Background(), backend, &first); err != nil {
		t.Fatal(err)
	}
	second := fresh()
	if err := refreshCompactCheckpoint(context.Background(), backend, &second); err != nil {
		t.Fatal(err)
	}
	if second.Basis == nil || second.Result == nil || *second.Basis != *second.Result || second.NextLSNBefore != second.NextLSNAfter {
		t.Fatal("already refreshed horizon did not remain a lawful no-op")
	}
	before, _ := backend.StateToken()
	next := backend.CommandWALNextLSN()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := fresh()
	if err := refreshCompactCheckpoint(ctx, backend, &cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled refresh: %v", err)
	}
	after, _ := backend.StateToken()
	if before != after || next != backend.CommandWALNextLSN() || cancelled.Status != "not_started" {
		t.Fatal("cancelled refresh changed authority")
	}
}
