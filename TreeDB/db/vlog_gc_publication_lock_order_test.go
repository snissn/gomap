package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Drive the public GC and grouped-root producers into their real finalization
// gates. The GC snapshot predates the private group, whose durable gate is held
// while its final batch needs preparation admission. GC must defer rather than
// wait on that durable gate while excluding the producer's preparation.
func TestValueLogGCDefersForPrivatePublicationAndLaterReclaims(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var orphan string
	for seq := uint32(1); seq <= 3; seq++ {
		id, err := valuelog.EncodeFileID(0, seq)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(ValueLogDirPath(dir), fmt.Sprintf("value-l0-%06d.log", seq))
		writer, err := valuelog.NewWriter(path, id)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(0, nil, uint64(seq), []byte("unreferenced value")); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		registerTestValueLogProducer(t, dir, path, id)
		if seq == 1 {
			orphan = path
		}
	}
	gcReady, continueGC := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(continueGC) }) }
	defer release()
	database.testValueLogGCBeforeRevalidateHook = func() { close(gcReady); <-continueGC }
	defer func() { database.testValueLogGCBeforeRevalidateHook = nil }()
	gcDone := make(chan error, 1)
	go func() { _, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{}); gcDone <- err }()
	select {
	case <-gcReady:
	case <-time.After(2 * time.Second):
		t.Fatal("GC did not reach destructive revalidation")
	}
	group, err := database.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer group.Close()
	final := database.NewPhysicalBatch().(*Batch)
	defer final.Close()
	if err := final.Set([]byte("published-after-gc"), []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
		t.Fatal(err)
	}
	published := make(chan error, 1)
	go func() { published <- final.WriteSync() }()
	release()
	select {
	case err := <-gcDone:
		if !errors.Is(err, ErrRecoverableRootSetStale) {
			t.Fatalf("contended GC=%v, want conservative stale refusal", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GC blocked on publication while excluding its preparation")
	}
	select {
	case err := <-published:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("private publication did not drain after GC refusal")
	}
	if err := final.Close(); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("refused GC lost original segment: %v", err)
	}
	if got, err := database.Get([]byte("published-after-gc")); err != nil || string(got) != "complete" {
		t.Fatalf("published value=%q err=%v", got, err)
	}
	database.testValueLogGCBeforeRevalidateHook = nil
	stats, err := database.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("later GC did not reclaim after publication: %+v", stats)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("eligible orphan remains after later GC: %v", err)
	}
}
