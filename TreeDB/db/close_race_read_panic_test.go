package db

import (
	"errors"
	"testing"
)

// Regression test for close/read races: when snapshot acquisition is blocked
// by closing state, read APIs must fail gracefully instead of panicking.
func TestReadAPIsWhileClosing_DoNotPanic(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	// Simulate the race window where readers observe closing=true and
	// AcquireSnapshot() returns nil.
	d.closing.Store(true)

	key := []byte("k")
	tests := []struct {
		name string
		fn   func() error
	}{
		{name: "Get", fn: func() error { _, err := d.Get(key); return err }},
		{name: "GetAppend", fn: func() error { _, err := d.GetAppend(key, nil); return err }},
		{name: "Has", fn: func() error { _, err := d.Has(key); return err }},
		{name: "GetMany", fn: func() error { _, err := d.GetMany([][]byte{key}); return err }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s panicked while db is closing: %v", tc.name, r)
				}
			}()
			err := tc.fn()
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("%s err=%v want %v", tc.name, err, ErrClosed)
			}
		})
	}
}
