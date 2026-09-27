package db

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type physicalCutPausedWriterV1 struct {
	io.Writer
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (w *physicalCutPausedWriterV1) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return w.Writer.Write(p)
}

func TestPhysicalSnapshotCutV1PreservesBothSlotsAcrossReuseAndClose(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(map[bool]string{false: "manifest", true: "directory"}[directory], func(t *testing.T) {
			dir := t.TempDir()
			if directory {
				if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}); err != nil {
					t.Fatal(err)
				}
			}
			database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, DisableSideStores: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			for i := 0; i < 24; i++ {
				if err := database.SetSync([]byte("key"), []byte("warm")); err != nil {
					t.Fatal(err)
				}
			}
			if err := database.SetSync([]byte("key"), []byte("older")); err != nil {
				t.Fatal(err)
			}
			if err := database.SetSync([]byte("key"), []byte("captured")); err != nil {
				t.Fatal(err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			records, active := database.durableRoot.slotRecord, database.durableRoot.slot
			cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer cut.Close()
			for slot, record := range records {
				if err := validateDurableRootLineageV1(&snapshotIndexPageStoreV1{file: cut.file, pageCount: cut.generation.HighWater()}, record); err != nil {
					t.Fatalf("source slot %d already invalid before deferred copy: %v", slot, err)
				}
				unused, err := cut.generation.SnapshotPageUnusedV1(record.ParentRecordPageID, cut.oldest)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("slot=%d commit=%d parentPage=%d parentCommit=%d oldest=%d classifiedUnused=%v free=%v", slot, record.CommitSeq, record.ParentRecordPageID, record.ParentCommitSeq, cut.oldest, unused, cut.generation.Allocatable(record.ParentRecordPageID))
				if uint64(slot) != active && !unused {
					t.Fatal("fixture did not exercise reusable older-slot parent")
				}
			}
			destination := t.TempDir()
			if raw, err := os.ReadFile(filepath.Join(dir, "format.json")); err == nil {
				if err := os.WriteFile(filepath.Join(destination, "format.json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			output, err := os.Create(filepath.Join(destination, indexFileName))
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			writer := &physicalCutPausedWriterV1{Writer: output, entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(writer.release) }) }
			defer unblock()
			done := make(chan error, 1)
			go func() { done <- cut.WriteToContext(context.Background(), writer) }()
			select {
			case <-writer.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("cut did not start")
			}
			// Overwrite both live slots repeatedly, exercising ordinary free and
			// allocator-metadata reuse while the captured index has not been read.
			for i := 0; i < 32; i++ {
				if err := database.SetSync([]byte("key"), []byte("later")); err != nil {
					t.Fatal(err)
				}
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			unblock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := output.Close(); err != nil {
				t.Fatal(err)
			}
			if err := cut.Close(); err != nil {
				t.Fatal(err)
			}
			if err := RebindDurableRootSnapshotV1(destination); err != nil {
				t.Fatal(err)
			}
			for _, fallback := range []bool{false, true} {
				if fallback {
					corruptIndexPageByte(t, destination, active)
				}
				restored, err := Open(Options{Dir: destination, DisableSideStores: true, ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				slot, want := active, "captured"
				if fallback {
					slot, want = 1-active, "older"
				}
				if got, err := restored.Get([]byte("key")); err != nil || string(got) != want {
					t.Fatalf("fallback=%v got=%q err=%v want=%q", fallback, got, err, want)
				}
				if restored.durableRoot.record.Freelist != records[slot].Freelist {
					t.Fatal("allocator generation changed in cut")
				}
				if err := restored.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestPhysicalSnapshotCutV1CancellationReleasesLease(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cut.WriteToContext(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if database.stableIndexCaptures.Load() != 0 {
		t.Fatal("cut leaked stable index lease")
	}
	if err := cut.WriteToContext(context.Background(), io.Discard); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestPhysicalSnapshotCutV1RefusesInvalidSourceParent(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for i := 0; i < 4; i++ {
		if err := database.SetSync([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	older := database.durableRoot.slotRecord[1-database.durableRoot.slot]
	corruptIndexPageByte(t, dir, older.ParentRecordPageID)
	cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err == nil {
		_ = cut.Close()
		t.Fatal("accepted invalid source parent")
	}
	if database.stableIndexCaptures.Load() != 0 {
		t.Fatal("failed cut leaked stable index lease")
	}
}
