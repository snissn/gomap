package db

import (
	"errors"
	"testing"
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
