package db

import "testing"

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
		fn   func()
	}{
		{name: "Get", fn: func() { _, _ = d.Get(key) }},
		{name: "GetAppend", fn: func() { _, _ = d.GetAppend(key, nil) }},
		{name: "Has", fn: func() { _, _ = d.Has(key) }},
		{name: "GetMany", fn: func() { _, _ = d.GetMany([][]byte{key}) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("%s panicked while db is closing: %v", tc.name, r)
				}
			}()
			tc.fn()
		})
	}
}
