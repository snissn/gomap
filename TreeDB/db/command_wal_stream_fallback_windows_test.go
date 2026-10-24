//go:build windows

package db

import (
	"fmt"
	"testing"
)

func TestWindowsPublicCommandWALStreamDurableRotationAndReopen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CommandWAL: true, Durability: DurabilityDurable, CommandWALSegmentTargetBytes: 256, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := database.commandWALActiveSegmentSnapshot()
	for i := 0; i < 12; i++ {
		if err = database.Set([]byte(fmt.Sprintf("key-%02d", i)), []byte("durable-stream-value")); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := database.commandWALActiveSegmentSnapshot()
	if current == initial {
		t.Fatal("public Windows command WAL did not rotate")
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for i := 0; i < 12; i++ {
		value, getErr := database.Get([]byte(fmt.Sprintf("key-%02d", i)))
		if getErr != nil || string(value) != "durable-stream-value" {
			t.Fatalf("key %d value=%q err=%v", i, value, getErr)
		}
	}
}
