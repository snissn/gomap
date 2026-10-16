package primaryarena

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

type closeBudget struct{ bytes uint64 }
type closeLease struct {
	budget *closeBudget
	bytes  uint64
}

func (b *closeBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	b.bytes += n
	return &closeLease{b, n}, nil
}
func (l *closeLease) Resize(n uint64) error {
	l.budget.bytes = l.budget.bytes - l.bytes + n
	l.bytes = n
	return nil
}
func (l *closeLease) Close() { l.budget.bytes -= l.bytes; l.bytes = 0 }

func TestArenaPhysicalCloseWorkOriginalCaptureRemains(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "arena.db"))
	if err != nil {
		t.Fatal(err)
	}
	if !a.pager.SupportsBoundedPhysicalClose() {
		a.Close()
		t.Skip("native partial mapping unsupported")
	}
	b := &closeBudget{}
	e, err := a.MetadataOwner().Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	before := a.MetadataOwner().Bytes()
	zero := &iterator.OrdinalScanWork{}
	if done, err := a.CloseWork(zero); done || err != nil || a.MetadataOwner().Bytes() != before {
		t.Fatal("refusal changed original owner")
	}
	if done, err := a.CloseWork(&iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}); done || err != nil {
		t.Fatal("first physical phase", done, err)
	}
	if err := a.Close(); !errors.Is(err, ErrCloseWorkRequired) {
		t.Fatal("ordinary callback stole physical work", err)
	}
	calls := 0
	for {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		done, err := a.CloseWork(w)
		if err != nil {
			t.Fatal(err)
		}
		if w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("actual work exceeded admission")
		}
		calls++
		if done {
			break
		}
		// The same bounded pager close polls its original asynchronous grower.
		// Scheduling that worker is separate from this turn's measured work.
		runtime.Gosched()
		if calls > 4096 {
			pagerState := reflect.ValueOf(a.pager).Elem()
			stack := make([]byte, 64<<10)
			stack = stack[:runtime.Stack(stack, true)]
			t.Fatalf("physical finish did not complete: arenaPhase=%d pagerPhase=%d growDone=%#x work=%+v metadataBytes=%d physicalClosed=%t\n%s",
				a.closeWork.phase, pagerState.FieldByName("closeWork").FieldByName("phase").Int(), pagerState.FieldByName("growDone").Pointer(),
				w, a.MetadataOwner().Bytes(), a.MetadataOwner().PhysicalClosed(), stack)
		}
	}
	if calls < 2 || !a.MetadataOwner().PhysicalClosed() || b.bytes == 0 {
		t.Fatal("physical cleanup erased held capture metadata")
	}
	var receipt retainedalloc.EnrollmentReleaseWork
	for n := 0; ; n++ {
		if n > 100 {
			t.Fatal("capture cleanup stalled")
		}
		done, err := e.CloseWorkInto(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, &receipt)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if b.bytes != 0 || a.MetadataOwner().Bytes() != 0 {
		t.Fatal("last actual capture did not refund")
	}
}
