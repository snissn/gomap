package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"path/filepath"
	"testing"
)

func TestPrimaryArenaOwnedReleaseQueueCannotReceiveOrdinaryAssistance(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	component := testComponent(t, a, "native", 1)
	directory := testDirectory(t, a, component, 1)
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	queue, ready, err := a.NewReleaseQueue(work)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	if ready, err = a.DropToReleaseQueue(directory, queue, work); !ready || err != nil {
		t.Fatal(ready, err)
	}
	before := a.Counters()
	if ready, err = a.ProgressOrdinaryRelease(32, work); !ready || err != nil {
		t.Fatal(ready, err)
	}
	if a.Counters().BanksFreed != before.BanksFreed || a.slot(directory.PageID).class != Directory || a.valid(component) == nil {
		t.Fatal("ordinary release drained the retained owner's directory or component")
	}
	// No writes intervene: the same finite queue releases its own physical closure.
	for turn := 0; turn < 32; turn++ {
		work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, progress, err := a.ReleaseQueueStep(queue, work)
		if !ready || err != nil {
			t.Fatal(ready, err)
		}
		if !progress {
			if a.slot(directory.PageID).class != 0 || a.slot(component.PageID).class != 0 {
				t.Fatal("owner closure not released")
			}
			return
		}
	}
	t.Fatal("owner release failed finite no-write progress")
}
