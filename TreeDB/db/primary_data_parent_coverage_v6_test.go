package db

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// This public publication-path control also compiles against the original
// runtime: the old code physically syncs unchanged DATA on the second update.
func TestPrimaryDataParentCoverageV6PhysicalBarriers(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var data, primary int
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.AfterIndexDataSync {
			switch filepath.Base(e.Path) {
			case indexFileName:
				data++
			case primaryIndexFileName:
				primary++
			}
		}
		return nil
	})
	defer restore()
	write := func(key []byte, wantData int) {
		t.Helper()
		data, primary = 0, 0
		if err := db.SetSync(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
		if data != wantData || primary != 2 {
			t.Fatalf("physical barriers DATA=%d PRIMARY=%d, want %d/2", data, primary, wantData)
		}
	}
	write([]byte("first"), 1)
	old := db.AcquireSnapshot()
	write([]byte("second"), 0)
	if got, err := old.Get([]byte("first")); err != nil || string(got) != "value" {
		t.Fatalf("retained parent read %q: %v", got, err)
	}
	old.Close()
	write(bytes.Repeat([]byte("x"), 3500), 1)
	write([]byte("after-materialization"), 0)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	write([]byte("first-after-reopen"), 1)
	write([]byte("second-after-reopen"), 0)
	for _, key := range [][]byte{[]byte("first"), []byte("second"), bytes.Repeat([]byte("x"), 3500)} {
		if got, err := db.Get(key); err != nil || string(got) != "value" {
			t.Fatalf("reopen read %q: %v", got, err)
		}
	}
}

func TestPrimaryDataParentCoverageV6InheritedDebtAndRetry(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetSync([]byte("seed"), []byte("stable")); err != nil {
		t.Fatal(err)
	}
	// Actual relaxed DATA materialization precedes a later bank-only member.
	if err := db.Set(bytes.Repeat([]byte("x"), 3500), []byte("inherited")); err != nil {
		t.Fatal(err)
	}
	parentRecord := db.durableRoot.record
	parentReceipt := db.durableRoot.primary.dataCompletion
	data := 0
	injected := errors.New("before capsule, after DATA")
	fail := true
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if filepath.Base(e.Path) == indexFileName && e.Point == durabilitycut.AfterIndexDataSync {
			data++
		}
		if fail && filepath.Base(e.Path) == primaryIndexFileName && e.Point == durabilitycut.BeforeMetaWrite {
			return injected
		}
		return nil
	})
	defer restore()
	if err := db.SetSync([]byte("later-bank"), []byte("pending")); !errors.Is(err, injected) {
		t.Fatalf("pre-install failure = %v", err)
	}
	if data == 0 {
		t.Fatal("inherited DATA materialization skipped its physical sync")
	}
	if db.durableRoot.record != parentRecord || db.durableRoot.primary.dataCompletion != parentReceipt {
		t.Fatal("pre-capsule refusal advanced completed parent authority")
	}
	fail = false
	data = 0
	if err := db.SetSync([]byte("retry"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	if data == 0 {
		t.Fatal("retry discarded inherited DATA debt")
	}
	if got, err := db.Get(bytes.Repeat([]byte("x"), 3500)); err != nil || string(got) != "inherited" {
		t.Fatalf("inherited value %q: %v", got, err)
	}
}
