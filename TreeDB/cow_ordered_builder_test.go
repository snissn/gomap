package treedb

import (
	"runtime"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

func TestOrderedBuilderTransfersQueuedClose(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	capability := db.NewMVCCAdmission()
	var b LogicalMVCCBuilder
	if err := db.BeginLogicalMVCC(&b); err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	mutations := []mvccadmission.LogicalMutation{{Key: []byte("key"), Value: []byte("owned"), Timestamp: 1}}
	if err := b.Append(mutations, capability.LogicalInput(mutations, 0)); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for db.lifecycleMu.TryRLock() {
		db.lifecycleMu.RUnlock()
		if time.Now().After(deadline) {
			t.Fatal("Close did not queue")
		}
		runtime.Gosched()
	}
	var c LogicalMVCCCompletion
	submitted := make(chan error, 1)
	go func() { submitted <- b.Submit(true, &c) }()
	select {
	case err := <-submitted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("seal recursively acquired lifecycle RLock behind Close")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close passed retained completion: %v", err)
	default:
	}
	if err := c.Complete(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("completion stranded Close")
	}
}
