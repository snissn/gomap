package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestNativeMVCCOriginalSetPhysicalClose(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	set := m.CurrentSetNoRefresh()
	shape, known := set.RetentionSizes()
	if !known || shape.FileCount != 1 || set.nativeFiles[0] != f {
		t.Fatal("frontier is not original producer file")
	}
	charge := registry.MetadataOwner().Bytes()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	originalHandle := f.File
	if done, err := m.ReleaseNativeMVCCSetStep(set, &iterator.OrdinalScanWork{}); done || err != nil || set.RefCount.Load() != 1 {
		t.Fatal("zero work released Set", err)
	}
	for i := 0; i < 8 && f.RefCount.Load() != 0; i++ {
		work := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
		if done, err := m.ReleaseNativeMVCCSetStep(set, work); done || err != nil {
			t.Fatal("original last edge did not pause", err)
		}
	}
	if f.RefCount.Load() != 0 || !f.nativeCallbacks.Load() || f.closeAttempted {
		t.Fatal("last edge/mode claim not original pre-close position")
	}
	if err := m.finishTerminalFile(f); !errors.Is(err, ErrNativeMVCCFileWork) {
		t.Fatal("ordinary terminal walker drained native callback", err)
	}
	if err := f.Close(); !errors.Is(err, ErrNativeMVCCFileWork) {
		t.Fatal("ordinary File close drained native callback", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := originalHandle.Stat(); err != nil {
		t.Fatal("OS handle closed before native physical work", err)
	}
	if registry.MetadataOwner().Bytes() != charge || f.producerCharge == 0 || m.files[f.ID] != f {
		t.Fatal("debt refunded at last-edge decrement")
	}
	for turns := 0; turns < 1000; turns++ {
		work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		done, err := m.ReleaseNativeMVCCSetStep(set, work)
		if err != nil || work.Records > 32 || work.Bytes > 1<<20 {
			t.Fatal("physical quantum", err)
		}
		if done {
			if len(m.files) != 0 || set.Files != nil || f.producerCharge != 0 || m.producerCharge != 0 || f.nativeCallbacks.Load() {
				t.Fatal("actual terminal aliases/debt survive success")
			}
			if err := originalHandle.Close(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("actual original OS handle not closed", err)
			}
			if ready, err := m.ReleaseNativeMVCCSetStep(set, &iterator.OrdinalScanWork{}); !ready || err != nil {
				t.Fatal("terminal release replay", err)
			}
			return
		}
	}
	t.Fatal("physical Set release did not complete")
}

func TestNativeMVCCSetLastFileRetainsActualManagerAcrossOrdinaryClose(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	set := m.CurrentSetNoRefresh()
	owner := registry.MetadataOwner()
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, 80)
	for i := range budgets {
		var err error
		budgets[i], err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		captures[i], err = owner.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	charge := m.producerCharge
	interleaved := false
	for turn := 0; turn < 1000; turn++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := m.ReleaseNativeMVCCSetStep(set, &w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual finite release", err)
		}
		if !ready && f.manager == nil && m.fileAliases.Load() == 0 && !interleaved {
			// The final File alias is physically gone while its original lease
			// disposal is paused. Ordinary Close must not refund this SAME Manager.
			if m.producerCharge != charge {
				t.Fatal("Manager refunded before Set receipt completed")
			}
			if err = m.Close(); err != nil {
				t.Fatal(err)
			}
			if m.producerCharge != charge {
				t.Fatal("ordinary Close refunded Manager borrowed by unfinished Set receipt")
			}
			// Exercise the SAME terminal operation called by Close completion and
			// late original File borrower release, under its original membership lock.
			m.mu.Lock()
			m.refundTerminalMetadataLocked()
			m.mu.Unlock()
			if m.producerCharge != charge {
				t.Fatal("original terminal refund consumed Manager borrowed by unfinished Set receipt")
			}
			interleaved = true
		}
		if ready {
			if !interleaved || m.producerCharge != 0 || m.wrapperBorrows.Load() != 0 {
				t.Fatal("last-file interleave/completion missing")
			}
			for i, e := range captures {
				e.Close()
				budgets[i].Close()
				if budgets[i].Stats().TotalBytes != 0 {
					t.Fatal("capture admission leaked")
				}
			}
			return
		}
	}
	t.Fatal("actual Set/Manager cleanup never completed")
}

func TestNativeMVCCSetInitialCloseCompletionRetainsActualManager(t *testing.T) {
	m, f, registry := originalTerminalFixture(t)
	set := m.CurrentSetNoRefresh()
	owner := registry.MetadataOwner()
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, 80)
	for i := range budgets {
		var err error
		budgets[i], err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		captures[i], err = owner.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	// A real controlled worker joins the original retry-worker completion edge.
	// Only its schedule is held: the effect under test is the initial original
	// Manager.Close completion, including its reachable terminal refund caller.
	releaseWorker := make(chan struct{})
	m.retryWorkers.Add(1)
	go func() { defer m.retryWorkers.Done(); <-releaseWorker }()
	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.RLock()
		closing := m.closeRunning
		m.mu.RUnlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			close(releaseWorker)
			t.Fatal("original Close did not reach worker join")
		}
		runtime.Gosched()
	}
	charge := m.producerCharge
	paused := false
	for turn := 0; turn < 1000; turn++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := m.ReleaseNativeMVCCSetStep(set, &w)
		if err != nil {
			close(releaseWorker)
			t.Fatal(err)
		}
		if !ready && f.manager == nil && m.fileAliases.Load() == 0 {
			paused = true
			break
		}
		if ready {
			close(releaseWorker)
			t.Fatal("Set completed before real last-file pause")
		}
	}
	close(releaseWorker)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if !paused || m.producerCharge != charge || m.producerMetadata == nil {
		t.Fatal("initial original Close completion refunded Manager held by unfinished Set receipt")
	}
	for turn := 0; turn < 1000; turn++ {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, err := m.ReleaseNativeMVCCSetStep(set, &w)
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			if m.producerCharge != 0 || m.wrapperBorrows.Load() != 0 {
				t.Fatal("actual final Manager cleanup incomplete")
			}
			for i, e := range captures {
				e.Close()
				budgets[i].Close()
				if budgets[i].Stats().TotalBytes != 0 {
					t.Fatal("capture admission leaked")
				}
			}
			return
		}
	}
	t.Fatal("physical Set/Manager did not finish")
}
