package db

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPublicDATAOnlyPublicationUsesAbsentClosure(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		if err = d.SetSync([]byte(fmt.Sprintf("key-%d", i)), []byte("inline-value")); err != nil {
			t.Fatal(err)
		}
	}
	if d.state.Load().ValueLogSet != nil {
		t.Error("public empty-log publication retained a physical Set")
	}
	for i, s := range d.durableRoot.slotResources {
		if s != nil {
			t.Errorf("durable slot %d retained an absent resource closure", i)
		}
	}
	held := d.AcquireSnapshot()
	if held == nil {
		t.Fatal("snapshot")
	}
	if err = d.SetSync([]byte("key-0"), []byte("new-value")); err != nil {
		t.Fatal(err)
	}
	got, err := held.Get([]byte("key-0"))
	if err != nil || !bytes.Equal(got, []byte("inline-value")) {
		t.Fatalf("held read: %q %v", got, err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err = d.Get([]byte("key-0"))
	if err != nil || !bytes.Equal(got, []byte("new-value")) {
		t.Fatalf("reopen: %q %v", got, err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
}
