package treedb_test

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestReopenVerify_DurableWALCoalescedInlineWrites(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileDurable, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := make(map[string][]byte)
	for i := 0; i < 2048; i++ {
		key := []byte(fmt.Sprintf("inline-%04d", i))
		value := []byte(fmt.Sprintf("value-%04d", i))
		if err := db.Set(key, value); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", key, err)
		}
		want[string(key)] = bytes.Clone(value)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for key, value := range want {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q after reopen: %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("get %q mismatch: got %q want %q", key, got, value)
		}
	}
}

func TestReopenVerify_DurableWALForcedValueLogPointers(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileDurable, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.PointerThreshold = 1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := map[string][]byte{}
	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("forced-pointer-%04d", i))
		value := bytes.Repeat([]byte{byte(i)}, 128)
		if err := db.SetSync(key, value); err != nil {
			_ = db.Close()
			t.Fatalf("set sync %q: %v", key, err)
		}
		want[string(key)] = bytes.Clone(value)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for key, value := range want {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q after reopen: %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("get %q mismatch: got %d bytes want %d", key, len(got), len(value))
		}
	}
}

func TestReopenVerify_DurableWALCoalescedInlineAroundPointerRecords(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileDurable, dir)
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.MaxWALBytes = -1
	opts.ValueLog.PointerThreshold = 64

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := map[string][]byte{}
	for i := 0; i < 256; i++ {
		smallKey := []byte(fmt.Sprintf("mixed-small-%04d", i))
		smallValue := []byte(fmt.Sprintf("small-%04d", i))
		largeKey := []byte(fmt.Sprintf("mixed-large-%04d", i))
		largeValue := bytes.Repeat([]byte{byte(i)}, 256)
		if err := db.Set(smallKey, smallValue); err != nil {
			_ = db.Close()
			t.Fatalf("set small %q: %v", smallKey, err)
		}
		if err := db.Set(largeKey, largeValue); err != nil {
			_ = db.Close()
			t.Fatalf("set large %q: %v", largeKey, err)
		}
		want[string(smallKey)] = bytes.Clone(smallValue)
		want[string(largeKey)] = bytes.Clone(largeValue)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for key, value := range want {
		got, err := reopened.Get([]byte(key))
		if err != nil {
			t.Fatalf("get %q after reopen: %v", key, err)
		}
		if !bytes.Equal(got, value) {
			t.Fatalf("get %q mismatch: got %d bytes want %d", key, len(got), len(value))
		}
	}
}
