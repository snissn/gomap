package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func TestCheckStorageMaintenanceReadyNilDBFailsClosed(t *testing.T) {
	var d *DB
	if err := d.CheckStorageMaintenanceReady(); !errors.Is(err, ErrClosed) {
		t.Fatalf("CheckStorageMaintenanceReady nil DB error=%v want ErrClosed", err)
	}
}

func TestCheckStorageMaintenanceReadyClosedDBFailsClosed(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := d.CheckStorageMaintenanceReady(); !errors.Is(err, ErrClosed) {
		t.Fatalf("CheckStorageMaintenanceReady closed DB error=%v want ErrClosed", err)
	}
}

func TestCheckStorageMaintenanceReadyReadOnlyDBFailsClosed(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open setup: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup: %v", err)
	}

	readonly, err := Open(Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer func() { _ = readonly.Close() }()

	if err := readonly.CheckStorageMaintenanceReady(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("CheckStorageMaintenanceReady read-only DB error=%v want ErrReadOnly", err)
	}
}

func TestCheckStorageMaintenanceReadyPoisonedCommandWALFailsClosed(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	d := openCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	d.testFailCommandWALFlush.Store(true)
	if _, err := d.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("k"), []byte("v"), false); !errors.Is(err, errTestCommandWALFlushFailpoint) {
		t.Fatalf("AppendRawKVPointCommandWALTrusted error=%v want command WAL flush failpoint", err)
	}
	d.testFailCommandWALFlush.Store(false)

	if err := d.CheckStorageMaintenanceReady(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("CheckStorageMaintenanceReady poisoned command WAL error=%v want ErrRecoveryRequired", err)
	}
}
