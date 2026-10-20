package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func originalCacheReadTracker(t *testing.T) (*DB, string) {
	t.Helper()
	d := &DB{valueLogIdentityPins: rootpublication.NewIdentityPinRegistry()}
	if err := d.allocateValueLogRefTracker(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "original-count-cache")
	if err := os.WriteFile(path, make([]byte, 4096), 0600); err != nil {
		t.Fatal(err)
	}
	return d, path
}
func TestValueLogRefCacheReadPreadmitsExactFileAndBuffer(t *testing.T) {
	for _, phase := range []string{"before-open", "before-buffer", "complete"} {
		t.Run(phase, func(t *testing.T) {
			d, path := originalCacheReadTracker(t)
			tracker := d.valueLogRefTracker
			owner := tracker.metadata
			budget := &retainedScratchBudget{capacity: ^uint64(0)}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer enrollment.Close()
			base := owner.Bytes()
			fileCharge, err := rootpublication.StableFileMetadataCharge(uint64(len(path)))
			if err != nil {
				t.Fatal(err)
			}
			switch phase {
			case "before-open":
				budget.capacity = base
			case "before-buffer":
				budget.capacity = base + fileCharge
			}
			data, err := tracker.readCache(path)
			switch phase {
			case "before-open":
				if !errors.Is(err, retainedalloc.ErrCapacity) || tracker.cacheReadFile != nil || data != nil {
					t.Fatalf("preadmission open refusal=%v", err)
				}
			case "before-buffer":
				if !errors.Is(err, retainedalloc.ErrCapacity) || tracker.cacheReadFile == nil || tracker.cacheReadData != nil {
					t.Fatalf("preadmission buffer refusal=%v", err)
				}
			case "complete":
				if err != nil || len(data) != 4096 || cap(data) != 4096 || owner.Bytes() != base+fileCharge+retainedalloc.AllocationCharge(4096) {
					t.Fatalf("exact owned cache=%v bytes=%d", err, owner.Bytes())
				}
			}
			if err = tracker.finishCacheRead(); err != nil {
				t.Fatal(err)
			}
			if owner.Bytes() != base || tracker.cacheReadFile != nil || tracker.cacheReadData != nil {
				t.Fatal("actual file/buffer disposal did not scrub/refund")
			}
			if err = tracker.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestValueLogRefCacheReadActualCloseRefusalRetainsSameTracker(t *testing.T) {
	d, path := originalCacheReadTracker(t)
	tracker := d.valueLogRefTracker
	prior := errors.New("original unrelated DB error")
	d.bgErr = prior
	data, err := tracker.readCache(path)
	if err != nil {
		t.Fatal(err)
	}
	file := tracker.cacheReadFile
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	charged := tracker.metadata.Bytes()
	receipt := tracker.finishCacheRead()
	if !errors.Is(receipt, os.ErrClosed) || tracker.cacheReadFile != file || len(tracker.cacheReadData) != len(data) || tracker.metadata.Bytes() != charged {
		t.Fatalf("original cache payload lost: %v", receipt)
	}
	d.retainOriginalCleanupError(receipt)
	if !errors.Is(d.backgroundError(), receipt) || !errors.Is(d.backgroundError(), prior) {
		t.Fatal("original error ownership lost")
	}
	for i := 0; i < 3; i++ {
		if tracker.finishCacheRead() != receipt || tracker.close() != receipt || tracker.metadata.Bytes() != charged {
			t.Fatal("replayed/refunded failed original cache")
		}
	}
}
