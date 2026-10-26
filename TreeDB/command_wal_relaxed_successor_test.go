package treedb

import (
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestPublicCOWRelaxedSuccessorOrdersWithoutAnotherSync(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	opts.BackgroundCheckpointInterval = -1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	before := database.Stats()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var held sync.Once
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			held.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	registered := make(chan struct{}, 1)
	database.commandWALGroupCommit.testAfterRegister = func(_ uint64, durable bool) {
		if !durable {
			registered <- struct{}{}
		}
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- database.SetSync([]byte("durable"), []byte("one")) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("durable predecessor did not reach physical sync")
	}
	go func() { second <- database.Set([]byte("relaxed"), []byte("two")) }()
	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("relaxed successor did not register during predecessor sync")
	}
	select {
	case err := <-second:
		t.Fatalf("relaxed successor bypassed predecessor ordering: %v", err)
	default:
	}
	unblock()
	for _, done := range []chan error{first, second} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ordered completion stalled")
		}
	}
	after := database.Stats()
	requirePublicStatDelta(t, before, after, "treedb.command_wal.file_sync.calls_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 1)
	requirePublicStatDelta(t, before, after, "treedb.command_wal.group_commit.trigger.timeout_total", 1)
	requireRawKVValue(t, database, []byte("durable"), []byte("one"))
	requireRawKVValue(t, database, []byte("relaxed"), []byte("two"))
}
