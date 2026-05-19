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
