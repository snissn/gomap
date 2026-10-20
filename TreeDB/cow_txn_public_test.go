package treedb

import (
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func cowTxnOpen(t *testing.T, db *DB) *ConditionalTxn {
	t.Helper()
	tx, err := db.NewConditionalTxnWithSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	return tx
}

func TestPublicCOWTxnDisjointCommitsPreserveBoth(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			a, b := cowTxnOpen(t, db), cowTxnOpen(t, db)
			if err := a.Set([]byte("a"), []byte("one")); err != nil {
				t.Fatal(err)
			}
			if err := b.Set([]byte("b"), []byte("two")); err != nil {
				t.Fatal(err)
			}
			if err := a.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := b.CommitSync(); err != nil {
				t.Fatal(err)
			}
			for k, v := range map[string]string{"a": "one", "b": "two"} {
				if got, err := db.Get([]byte(k)); err != nil || string(got) != v {
					t.Fatalf("%s: %q %v", k, got, err)
				}
			}
		})
	}
}
func TestPublicCOWTxnEmptyRangePhantom(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileCommandWALDurable)
	tx := cowTxnOpen(t, db)
	it, err := tx.Snapshot().Iterator([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Fatal("expected empty range")
	}
	_ = it.Close()
	if err := db.Set([]byte("c"), []byte("phantom")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("z"), []byte("write")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("commit=%v", err)
	}
}
func TestPublicCOWTxnMutationWithOpenIteratorRejected(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	tx := cowTxnOpen(t, db)
	it, err := tx.Snapshot().Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("a"), []byte("v")); !errors.Is(err, ErrTxnIteratorOpen) {
		t.Fatalf("mutation with open iterator=%v", err)
	}
	_ = it.Close()
	if err := tx.Set([]byte("a"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func TestPublicTxnNoOvertakeAfterAppendBeforeInstall(t *testing.T) {
	db, _, blocked := cowPublicContractOpen(t, ProfileCommandWALDurable)
	tx := cowTxnOpen(t, db)
	if _, err := tx.Get([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("b"), []byte("txn")); err != nil {
		t.Fatal(err)
	}
	paused, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeCOWCutSwap {
			once.Do(func() { close(paused); <-release })
		}
		return nil
	})
	var released sync.Once
	unpause := func() { released.Do(func() { close(release) }) }
	defer func() { unpause(); restore() }()
	first := make(chan error, 1)
	go func() { first <- db.Set([]byte("a"), []byte("ordinary")) }()
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		blocked.Store(true)
		t.Fatal("append/install pause missing")
	}
	done := make(chan error, 1)
	go func() { done <- tx.Commit() }()
	select {
	case err := <-done:
		t.Fatalf("transaction overtook accepted command: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unpause()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("commit=%v", err)
	}
}

func TestPublicCOWTxnABAWriteSkewAndBlindOverlap(t *testing.T) {
	for _, kind := range []string{"value-ABA", "absent-ABA", "write-skew", "blind-overlap"} {
		t.Run(kind, func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, ProfileCommandWALDurable)
			if kind != "absent-ABA" {
				if err := db.Set([]byte("a"), []byte("old")); err != nil {
					t.Fatal(err)
				}
			}
			tx := cowTxnOpen(t, db)
			if kind != "blind-overlap" {
				if _, err := tx.Get([]byte("a")); err != nil {
					t.Fatal(err)
				}
			}
			target := "a"
			if kind != "blind-overlap" {
				target = "b"
			}
			if err := tx.Set([]byte(target), []byte("txn")); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "absent-ABA":
				if err := db.Set([]byte("a"), []byte("temporary")); err != nil {
					t.Fatal(err)
				}
				if err := db.Delete([]byte("a")); err != nil {
					t.Fatal(err)
				}
			case "value-ABA":
				if err := db.Set([]byte("a"), []byte("temporary")); err != nil {
					t.Fatal(err)
				}
				if err := db.Set([]byte("a"), []byte("old")); err != nil {
					t.Fatal(err)
				}
			case "write-skew":
				other := cowTxnOpen(t, db)
				if _, err := other.Get([]byte("b")); err != nil {
					t.Fatal(err)
				}
				if err := other.Set([]byte("a"), []byte("other")); err != nil {
					t.Fatal(err)
				}
				if err := other.Commit(); err != nil {
					t.Fatal(err)
				}
			case "blind-overlap":
				other := cowTxnOpen(t, db)
				if err := other.Set([]byte("a"), []byte("other")); err != nil {
					t.Fatal(err)
				}
				if err := other.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			next := db.backend.CommandWALNextLSN()
			if err := tx.CommitSync(); !errors.Is(err, ErrConcurrentModification) {
				t.Fatalf("commit=%v", err)
			}
			if got := db.backend.CommandWALNextLSN(); got != next {
				t.Fatalf("conflict appended frame: %d->%d", next, got)
			}
		})
	}
}
func TestPublicCOWTxnStagedOverlayRangeDeleteAndChildren(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	for _, k := range []string{"a", "b", "c"} {
		if err := db.Set([]byte(k), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	tx := cowTxnOpen(t, db)
	if err := tx.Set([]byte("b"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("d"), nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	it, err := tx.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var keys string
	for it.Valid() {
		keys += string(it.Key())
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	if keys != "bcd" {
		t.Fatalf("overlay=%q", keys)
	}
	if err := tx.Set([]byte("e"), nil); !errors.Is(err, ErrTxnIteratorOpen) {
		t.Fatalf("open iterator mutation=%v", err)
	}
	_ = it.Close()
	if err := tx.DeleteRange([]byte("b"), []byte("e")); err != nil {
		t.Fatal(err)
	}
	if found, err := tx.Has([]byte("d")); err != nil || found {
		t.Fatalf("staged range delete missed new key: %t %v", found, err)
	}
	child, err := tx.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.CommitSync(); err != nil {
		t.Fatal(err)
	}
	if child.Valid() || !errors.Is(child.Error(), ErrConditionalTxnClosed) {
		t.Fatalf("child survived commit: %v", child.Error())
	}
	_ = child.Close()
	for _, k := range []string{"a", "b", "c", "d"} {
		if found, err := db.Has([]byte(k)); err != nil || found {
			t.Fatalf("deleted %s: %t %v", k, found, err)
		}
	}
}
func TestPublicCOWTxnRangeBoundsAndDisjointProgress(t *testing.T) {
	for _, inside := range []bool{false, true} {
		t.Run(map[bool]string{false: "disjoint", true: "inside"}[inside], func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, ProfileCommandWALRelaxed)
			tx := cowTxnOpen(t, db)
			it, err := tx.Iterator([]byte("b"), []byte("d"))
			if err != nil {
				t.Fatal(err)
			}
			it.Seek([]byte("z"))
			_ = it.Close()
			key := "d"
			if inside {
				key = "c"
			}
			if err := db.Set([]byte(key), []byte("other")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Set([]byte("z"), []byte("txn")); err != nil {
				t.Fatal(err)
			}
			err = tx.Commit()
			if inside {
				if !errors.Is(err, ErrConcurrentModification) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPublicCOWTxnEmptySyncAndOpeningView(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			if err := db.Set([]byte("a"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			if profile == ProfileNoWALFast {
				boundary := db.NewBatch().(*caching.Batch)
				if err := boundary.WriteSync(); err != nil {
					t.Fatal(err)
				}
				receipt := boundary.COWReceipt()
				if receipt.Assigned || receipt.LogicalInstalled || receipt.AssignedLSN != 0 || receipt.SealedLogicalSeq != 1 {
					t.Fatalf("empty sync receipt=%+v", receipt)
				}
				if seq := db.Stats()["treedb.cache.cow.txn.logical_seq"]; seq != "1" {
					t.Fatalf("empty sync published new logical seq %s", seq)
				}
				_ = boundary.Close()
			}
			tx := cowTxnOpen(t, db)
			if err := db.Set([]byte("a"), []byte("new")); err != nil {
				t.Fatal(err)
			}
			if got, err := tx.Get([]byte("a")); err != nil || string(got) != "old" {
				t.Fatalf("opening view=%q %v", got, err)
			}
			if err := tx.CommitSync(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublicCOWTxnHistoryExpiryAndNoFrame(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileCommandWALDurable, func(o *Options) { o.ValueLog.ForcePointers = false; o.ValueLog.PointerThreshold = 1 << 20 })
	if err := db.Set([]byte("guard"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	tx := cowTxnOpen(t, db)
	if _, err := tx.Get([]byte("guard")); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		b := db.NewBatchWithSize(2048)
		for n := 0; n < 2048; n++ {
			if err := b.Set([]byte(fmt.Sprintf("u/%d/%04d", round, n)), []byte("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatal(err)
		}
		_ = b.Close()
	}
	if err := db.Set([]byte("u/end"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.Get([]byte("guard")); err != nil || string(got) != "old" {
		t.Fatalf("history eviction invalidated retained read: %q %v", got, err)
	}
	if err := tx.Set([]byte("result"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	next := db.backend.CommandWALNextLSN()
	if err := tx.Commit(); !errors.Is(err, ErrTxnTooOld) {
		t.Fatalf("expired commit=%v", err)
	}
	if db.backend.CommandWALNextLSN() != next {
		t.Fatal("TooOld appended command")
	}
}
func TestPublicCOWTxnQualifiedBatchCallbackEscapeRejects(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	b := db.NewBatch().(*caching.Batch)
	defer b.Close()
	if err := b.Set([]byte("a"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := b.WriteAfterCommandWALAppend(false, func() error { called = true; return nil }); !errors.Is(err, caching.ErrCOWUnsupported) || called {
		t.Fatalf("callback escape=%v called=%t", err, called)
	}
	if err := b.PrepareExternalCommandWALPublication(); !errors.Is(err, caching.ErrCOWUnsupported) {
		t.Fatalf("unleased preparation=%v", err)
	}
	if found, err := db.Has([]byte("a")); err != nil || found {
		t.Fatal("rejected callback published effects")
	}
	if tx, err := db.backend.NewConditionalTxn(); !errors.Is(err, ErrConditionalTxnUnsupported) || tx != nil {
		t.Fatalf("private backend advertised independent engine: %v %v", tx, err)
	}
}
func TestPublicCOWTxnLargeDependencyKeysOwnedAndReleased(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	// Exclude the original lazy journal's fixed namespace-aware backing arrays
	// from the private dependency arena delta; this is an actual publication.
	if err := db.Set([]byte("journal-seed"), []byte("seed")); err != nil {
		t.Fatal(err)
	}
	before := db.cached.COWMemoryStats()
	tx := cowTxnOpen(t, db)
	for n := 0; n < 64; n++ {
		key := []byte(fmt.Sprintf("%04d/", n) + strings.Repeat("k", 512))
		if _, err := tx.Get(key); err != nil {
			t.Fatal(err)
		}
		key[0] = 'x'
	}
	if err := tx.Set([]byte("result"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	after := db.cached.COWMemoryStats()
	if after.ExternalBytes > before.ExternalBytes+1<<20 {
		t.Fatalf("private dependency arena retained: before=%d after=%d", before.ExternalBytes, after.ExternalBytes)
	}
}
func TestPublicCOWTxnAcceptedErrorPoisonsBeforeLaterAcceptance(t *testing.T) {
	db, opts, _ := cowPublicContractOpen(t, ProfileCommandWALDurable)
	tx := cowTxnOpen(t, db)
	if err := tx.Set([]byte("a"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set([]byte("b"), []byte("second")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("accepted append observation failure")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
			return injected
		}
		return nil
	})
	err := tx.Commit()
	restore()
	if !errors.Is(err, injected) {
		t.Fatalf("accepted error=%v", err)
	}
	next := db.backend.CommandWALNextLSN()
	if err := db.Set([]byte("later"), []byte("v")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("later acceptance=%v", err)
	}
	if db.backend.CommandWALNextLSN() != next {
		t.Fatal("poisoned writer appended")
	}
	if found, err := db.Has([]byte("a")); err != nil || found {
		t.Fatalf("incomplete accepted command became visible: %t %v", found, err)
	}
	_ = db.Close()
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	a, errA := reopened.Has([]byte("a"))
	b, errB := reopened.Has([]byte("b"))
	if errA != nil || errB != nil || a != b {
		t.Fatalf("partial recovery: a=%t/%v b=%t/%v", a, errA, b, errB)
	}
}
func TestPublicCOWTxnStagedIndexAndEmptyDomain(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	tx := cowTxnOpen(t, db)
	for n := 0; n < 40; n++ {
		if err := tx.Set([]byte(fmt.Sprintf("key/%02d", n)), []byte("first")); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Set([]byte("key/00"), []byte("last")); err != nil {
		t.Fatal(err)
	}
	if got, err := tx.GetAppend([]byte("key/00"), []byte("prefix/")); err != nil || string(got) != "prefix/last" {
		t.Fatalf("indexed staged read=%q %v", got, err)
	}
	if err := tx.Set(nil, []byte("empty")); err != nil {
		t.Fatal(err)
	}
	it, err := tx.Iterator(nil, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Fatal("concrete empty end became unbounded")
	}
	_ = it.Close()
	it, err = tx.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !it.Valid() || len(it.Key()) != 0 || string(it.Value()) != "empty" {
		t.Fatal("empty staged key omitted")
	}
	_ = it.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestPublicCOWTxnSnapshotRejectsReusableHandle(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	var tx ConditionalTxn
	if err := db.InitConditionalTxnWithSnapshot(&tx); err != nil {
		t.Fatal(err)
	}
	old := tx.Snapshot()
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.InitConditionalTxnWithSnapshot(&tx); err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if err := tx.Set([]byte("new"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Get([]byte("new")); !errors.Is(err, ErrConditionalTxnClosed) {
		t.Fatalf("old Get=%v", err)
	}
	if _, err := old.Iterator(nil, nil); !errors.Is(err, ErrConditionalTxnClosed) {
		t.Fatalf("old Iterator=%v", err)
	}
	if got, err := tx.Snapshot().Get([]byte("new")); err != nil || string(got) != "value" {
		t.Fatalf("current Get=%q %v", got, err)
	}
}

func TestPublicCOWTxnSyncReopen(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, opts, _ := cowPublicContractOpen(t, profile)
			tx := cowTxnOpen(t, db)
			for _, key := range []string{"one", "two"} {
				if err := tx.Set([]byte(key), []byte("persistent pointer value")); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.CommitSync(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, key := range []string{"one", "two"} {
				if got, err := reopened.Get([]byte(key)); err != nil || string(got) != "persistent pointer value" {
					t.Fatalf("%s=%q %v", key, got, err)
				}
			}
		})
	}
}

func TestPublicCOWTxnAllPointProducersShareHistory(t *testing.T) {
	producers := map[string]func(*DB) error{
		"Set":        func(db *DB) error { return db.Set([]byte("key"), []byte("changed")) },
		"SetSync":    func(db *DB) error { return db.SetSync([]byte("key"), []byte("changed")) },
		"Delete":     func(db *DB) error { return db.Delete([]byte("key")) },
		"DeleteSync": func(db *DB) error { return db.DeleteSync([]byte("key")) },
		"Batch": func(db *DB) error {
			b := db.NewBatch()
			defer b.Close()
			if err := b.Set([]byte("key"), []byte("changed")); err != nil {
				return err
			}
			return b.Write()
		},
		"BatchSync": func(db *DB) error {
			b := db.NewBatchWithSize(1)
			defer b.Close()
			if err := b.Delete([]byte("key")); err != nil {
				return err
			}
			return b.WriteSync()
		},
		"BatchView": func(db *DB) error {
			b := db.NewBatch()
			defer b.Close()
			if err := b.(interface{ SetView([]byte, []byte) error }).SetView([]byte("key"), []byte("changed")); err != nil {
				return err
			}
			return b.Write()
		},
	}
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			for name, produce := range producers {
				t.Run(name, func(t *testing.T) {
					db, _, _ := cowPublicContractOpen(t, profile)
					if err := db.Set([]byte("key"), []byte("initial")); err != nil {
						t.Fatal(err)
					}
					tx := cowTxnOpen(t, db)
					if _, err := tx.Get([]byte("key")); err != nil {
						t.Fatal(err)
					}
					if err := tx.Set([]byte("result"), []byte("staged")); err != nil {
						t.Fatal(err)
					}
					if err := produce(db); err != nil {
						t.Fatal(err)
					}
					before := db.backend.CommandWALNextLSN()
					if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
						t.Fatalf("commit=%v", err)
					}
					if after := db.backend.CommandWALNextLSN(); after != before {
						t.Fatal("conflict appended a frame")
					}
				})
			}
		})
	}
}

func TestPublicCOWTxnDisjointConcurrentScanProgress(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			if err := db.Set([]byte("a/seed"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			advance, done := make(chan struct{}), make(chan error, 1)
			go func() {
				for range advance {
					if err := db.Set([]byte("z/unrelated"), []byte("v")); err != nil {
						done <- err
						return
					}
					done <- nil
				}
			}()
			defer close(advance)
			for n := 0; n < 32; n++ {
				tx := cowTxnOpen(t, db)
				it, err := tx.Iterator([]byte("a/"), []byte("b/"))
				if err != nil {
					t.Fatal(err)
				}
				if !it.Valid() {
					t.Fatal("missing range seed")
				}
				advance <- struct{}{}
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				_ = it.Close()
				if err := tx.Set([]byte("result"), []byte("v")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("disjoint scan %d: %v", n, err)
				}
			}
		})
	}
}

func TestPublicCOWTxnRangeExpansionLimitPreservesStaging(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	b := db.NewBatchWithSize(4097)
	for n := 0; n < 4097; n++ {
		if err := b.Set([]byte(fmt.Sprintf("a/%04d", n)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	tx := cowTxnOpen(t, db)
	if err := tx.Set([]byte("z/staged"), []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteRange([]byte("a/"), []byte("b/")); !errors.Is(err, caching.ErrBatchDeleteRangeTooLarge) {
		t.Fatalf("expansion=%v", err)
	}
	if got, err := tx.Get([]byte("a/0000")); err != nil || string(got) != "v" {
		t.Fatalf("partial private delete: %q %v", got, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("z/staged")); err != nil || string(got) != "kept" {
		t.Fatalf("prior stage=%q %v", got, err)
	}
}

// The cutover hook owns cache write admission. A writer paused before cache
// admission must not already hold a public group gate needed by that cutover.
func TestPublicCOWTxnCheckpointAdmissionLockOrder(t *testing.T) {
	for _, profile := range []Profile{ProfileCommandWALDurable, ProfileCommandWALRelaxed} {
		t.Run(string(profile), func(t *testing.T) {
			db, _, blocked := cowPublicContractOpen(t, profile)
			cutPaused, writerPaused := make(chan struct{}), make(chan struct{})
			releaseCut, releaseWriter := make(chan struct{}), make(chan struct{})
			var cutOnce, writerOnce, releaseCutOnce, releaseWriterOnce sync.Once
			defer releaseCutOnce.Do(func() { close(releaseCut) })
			defer releaseWriterOnce.Do(func() { close(releaseWriter) })
			db.cached.SetCommandWALCheckpointCutoverHook(func() {
				cutOnce.Do(func() { close(cutPaused); <-releaseCut })
				db.snapshotPublicCommandWALCheckpointCutover()
			})
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == durabilitycut.BeforeCOWAdmission {
					writerOnce.Do(func() { close(writerPaused); <-releaseWriter })
				}
				return nil
			})
			defer restore()
			checkpointDone := make(chan error, 1)
			go func() { checkpointDone <- db.Checkpoint() }()
			select {
			case <-cutPaused:
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("checkpoint did not reach cutover")
			}
			writerDone := make(chan error, 1)
			go func() { writerDone <- db.Set([]byte("writer"), []byte("v")) }()
			select {
			case <-writerPaused:
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("writer did not reach admission")
			}
			releaseCutOnce.Do(func() { close(releaseCut) })
			select {
			case err := <-checkpointDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("checkpoint waits on pre-admission writer group gate")
			}
			releaseWriterOnce.Do(func() { close(releaseWriter) })
			select {
			case err := <-writerDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				blocked.Store(true)
				t.Fatal("writer did not resume")
			}
		})
	}
}

func TestPublicCOWTxnHigherLevelBarrierFailsEligibility(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			tx := cowTxnOpen(t, db)
			if err := tx.Set([]byte("staged"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			called := false
			unregister := db.backend.RegisterCommandWALRawPublishBarrier(func() error { called = true; return nil })
			before := db.backend.CommandWALNextLSN()
			if _, err := db.NewConditionalTxn(); !errors.Is(err, ErrConditionalTxnUnsupported) {
				t.Fatalf("new txn=%v", err)
			}
			if err := tx.Commit(); !errors.Is(err, ErrConditionalTxnUnsupported) {
				t.Fatalf("existing txn=%v", err)
			}
			if db.backend.CommandWALNextLSN() != before || called {
				t.Fatal("ineligible command executed callback/frame")
			}
			unregister()
			if err := db.Set([]byte("resumed"), []byte("v")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublicCOWTxnRegistrationFailurePoisonsBeforeQueuedWriter(t *testing.T) {
	db, _, blocked := cowPublicContractOpen(t, ProfileCommandWALDurable)
	tx := cowTxnOpen(t, db)
	if err := tx.Set([]byte("accepted"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	db.commandWALGroupCommit.nextTicket = publicCommandWALFastRelaxedAppendedTicket - 1
	paused, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend {
			once.Do(func() { close(paused); <-release })
		}
		return nil
	})
	defer restore()
	first := make(chan error, 1)
	go func() { first <- tx.Commit() }()
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatal("append did not pause")
	}
	queued := make(chan error, 1)
	go func() { queued <- db.Set([]byte("later"), []byte("v")) }()
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("ticket exhaustion succeeded")
		}
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatal("first command stuck")
	}
	select {
	case err := <-queued:
		if !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("queued writer=%v", err)
		}
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatal("queued writer stuck")
	}
	if got := db.backend.CommandWALNextLSN(); got != 2 {
		t.Fatalf("later command appended: nextLSN=%d", got)
	}
}

func TestPublicCOWTxnExplicitRevisionABASharesClock(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			put := func(value string) {
				tx := cowTxnOpen(t, db)
				if err := tx.SetWithRevision([]byte("key"), []byte(value), 42); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			put("initial")
			tx := cowTxnOpen(t, db)
			if _, rev, err := tx.GetVersioned([]byte("key")); err != nil || rev != 42 {
				t.Fatalf("opening revision=%d %v", rev, err)
			}
			put("middle")
			put("initial")
			if err := tx.Set([]byte("result"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			before := db.backend.CommandWALNextLSN()
			if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
				t.Fatalf("ABA commit=%v", err)
			}
			if db.backend.CommandWALNextLSN() != before {
				t.Fatal("revision ABA conflict appended")
			}
		})
	}
}

func TestPublicCOWTxnMVCCInputProducersShareClock(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			for _, mode := range []string{"point", "point-sync", "batch"} {
				t.Run(mode, func(t *testing.T) {
					db, _, _ := cowPublicContractOpen(t, profile)
					key, err := mvcckey.Encode([]byte("logical"), 1)
					if err != nil {
						t.Fatal(err)
					}
					owner := db.NewMVCCAdmission()
					if owner == nil {
						t.Fatal("missing capability")
					}
					tx := cowTxnOpen(t, db)
					if _, err := tx.Get(key); err != nil {
						t.Fatal(err)
					}
					input := owner.Input(key, 1, 0)
					if mode == "batch" {
						b := db.NewBatch()
						defer b.Close()
						if err := b.(interface {
							SetWithMVCCInput([]byte, []byte, mvccadmission.Input) error
						}).SetWithMVCCInput(key, []byte("value"), input); err != nil {
							t.Fatal(err)
						}
						err = b.Write()
					} else {
						err = db.SetPointWithMVCCInput(key, []byte("value"), mode == "point-sync", input)
					}
					if err != nil {
						t.Fatal(err)
					}
					if err := tx.Set([]byte("result"), []byte("v")); err != nil {
						t.Fatal(err)
					}
					before := db.backend.CommandWALNextLSN()
					if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
						t.Fatalf("MVCC-input conflict=%v", err)
					}
					if db.backend.CommandWALNextLSN() != before {
						t.Fatal("MVCC-input conflict appended")
					}
				})
			}
		})
	}
}

func TestPublicCOWTxnRawSetOpsSharesClockAndOwnsKeys(t *testing.T) {
	db, _, _ := cowPublicContractOpen(t, ProfileNoWALFast)
	tx := cowTxnOpen(t, db)
	if _, err := tx.Get([]byte("key")); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch().(*caching.Batch)
	defer b.Close()
	key := []byte("key")
	if err := b.SetOps([]batch.Entry{{Type: batch.OpPut, Key: key, Value: []byte("v"), Revision: 42}}); err != nil {
		t.Fatal(err)
	}
	key[0] = 'x'
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("key")); err != nil || string(got) != "v" {
		t.Fatalf("owned SetOps key=%q %v", got, err)
	}
	if err := tx.Set([]byte("result"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("SetOps conflict=%v", err)
	}
}

func TestPublicCOWTxnPhysicalSuccessorPreservesSequence(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			if err := db.Set([]byte("key"), []byte("opening")); err != nil {
				t.Fatal(err)
			}
			tx := cowTxnOpen(t, db)
			if _, err := tx.Get([]byte("key")); err != nil {
				t.Fatal(err)
			}
			seq := db.Stats()["treedb.cache.cow.txn.logical_seq"]
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if next := db.Stats()["treedb.cache.cow.txn.logical_seq"]; next != seq {
				t.Fatalf("physical checkpoint advanced logical seq: %s -> %s", seq, next)
			}
			if err := tx.Set([]byte("result"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("retained opening view across physical successor: %v", err)
			}
		})
	}
}

func TestPublicCOWTxnGroupCommitSharesSyncAfterInstall(t *testing.T) {
	db, opts, blocked := cowPublicContractOpen(t, ProfileCommandWALDurable)
	held := db.AcquireSnapshot()
	if held == nil {
		t.Fatal("opening snapshot unavailable")
	}
	defer held.Close()
	db.commandWALGroupCommit.delay = time.Second
	db.commandWALGroupCommit.maxCommits = 2
	db.commandWALGroupCommit.maxBytes = 1 << 30
	registered := make(chan struct{})
	var once sync.Once
	db.commandWALGroupCommit.testAfterRegister = func(_ uint64, _ bool) { once.Do(func() { close(registered) }) }
	groups := make(chan int, 4)
	db.commandWALGroupCommit.testBeforeSync = func(n int) { groups <- n }
	a, b := cowTxnOpen(t, db), cowTxnOpen(t, db)
	if err := a.Set([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set([]byte("b"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	aDone, bDone := make(chan error, 1), make(chan error, 1)
	go func() { aDone <- a.Commit() }()
	select {
	case <-registered:
	case <-time.After(10 * time.Second):
		blocked.Store(true)
		t.Fatal("first registration did not complete")
	}
	if got, err := db.Get([]byte("a")); err != nil || string(got) != "one" {
		t.Fatalf("registration preceded complete logical install: %q %v", got, err)
	}
	go func() { bDone <- b.Commit() }()
	for _, done := range []chan error{aDone, bDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			blocked.Store(true)
			t.Fatal("group blocked with cache lease held")
		}
	}
	select {
	case n := <-groups:
		if n != 2 {
			t.Fatalf("group size=%d", n)
		}
	default:
		t.Fatal("no group sync")
	}
	select {
	case n := <-groups:
		t.Fatalf("redundant group sync: %d", n)
	default:
	}
	// The final accepted command is the group's durable barrier. Checkpoint
	// must cover that identity without requiring a later mutation to advance
	// the cache's cut, and without changing an already-held logical view.
	accepted := publicStatUint64(t, db, "treedb.command_wal.live_accepted_max_lsn")
	logical := publicStatUint64(t, db, "treedb.cache.cow.txn.logical_seq")
	if accepted != 3 || logical != 2 {
		t.Fatalf("group endpoint accepted=%d logical=%d, want 3/2", accepted, logical)
	}
	for key, want := range map[string]string{"a": "one", "b": "two"} {
		if got, err := db.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("current %s=%q: %v", key, got, err)
		}
		cowPublicContractSnapshotValue(t, held, key, nil, false)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint immediately after group ACK: %v", err)
	}
	if got := publicStatUint64(t, db, "treedb.applied_command_lsn"); got != accepted {
		t.Fatalf("checkpoint applied=%d want accepted=%d", got, accepted)
	}
	if got := publicStatUint64(t, db, "treedb.cache.cow.txn.logical_seq"); got != logical {
		t.Fatalf("checkpoint logical=%d want=%d", got, logical)
	}
	for _, key := range []string{"a", "b"} {
		cowPublicContractSnapshotValue(t, held, key, nil, false)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for key, want := range map[string]string{"a": "one", "b": "two"} {
		if got, err := reopened.Get([]byte(key)); err != nil || string(got) != want {
			t.Fatalf("reopened %s=%q: %v", key, got, err)
		}
	}
	if got := publicStatUint64(t, reopened, "treedb.applied_command_lsn"); got != accepted {
		t.Fatalf("reopened applied=%d want=%d", got, accepted)
	}
}

// The untouched baseline refuses this retained-view vacuum shape for capacity.
// P1 preserves its logical view/sequence; reserved refresh headroom is P2-owned.
func TestPublicCOWTxnMaintenanceRefusalPreservesLogicalView(t *testing.T) {
	for _, profile := range cowPublicContractProfiles {
		t.Run(string(profile), func(t *testing.T) {
			db, _, _ := cowPublicContractOpen(t, profile)
			if err := db.Set([]byte("key"), []byte("opening")); err != nil {
				t.Fatal(err)
			}
			tx := cowTxnOpen(t, db)
			if _, err := tx.Get([]byte("key")); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			seq := db.Stats()["treedb.cache.cow.txn.logical_seq"]
			err := db.VacuumIndexOnline(context.Background())
			if err != nil && !errors.Is(err, memtable.ErrCOWCapacity) && !errors.Is(err, errVacuumUnsupported) {
				t.Fatalf("maintenance=%v", err)
			}
			if next := db.Stats()["treedb.cache.cow.txn.logical_seq"]; next != seq {
				t.Fatalf("physical maintenance published logical change: %s -> %s", seq, next)
			}
			if got, err := tx.Get([]byte("key")); err != nil || string(got) != "opening" {
				t.Fatalf("retained view=%q %v", got, err)
			}
			if got, err := db.Get([]byte("key")); err != nil || string(got) != "opening" {
				t.Fatalf("current logical view=%q %v", got, err)
			}
			if err == nil {
				if err := tx.Set([]byte("result"), []byte("v")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
