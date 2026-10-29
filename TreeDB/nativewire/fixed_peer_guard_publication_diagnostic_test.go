package nativewire

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// This failure-only evidence does not prove which publication advanced the
// guard. Meta writes precede visible state, events have no CommitSeq, and a
// publisher stack can identify an asynchronous worker rather than its caller.
// Installing the observer can change timing. Overflow and absent events are
// inconclusive. The observer never retains handles or calls back into TreeDB.
type guardPublicationDiagnostic struct {
	mu      sync.Mutex
	records [128]guardPublicationRecord
	next    uint64
}

type guardPublicationRecord struct {
	at     time.Time
	label  string
	point  durabilitycut.Point
	root   string
	path   string
	offset int64
	length int64
	pcs    [32]uintptr
	npc    int
}

func installGuardPublicationDiagnostic(t *testing.T) *guardPublicationDiagnostic {
	t.Helper()
	d := &guardPublicationDiagnostic{}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource != durabilitycut.ResourceMeta || (event.Point != durabilitycut.BeforeMetaWrite && event.Point != durabilitycut.AfterMetaWrite) {
			return nil
		}
		d.mu.Lock()
		record := guardPublicationRecord{at: time.Now(), point: event.Point, root: event.Root, path: event.Path, offset: event.Offset, length: event.Length}
		record.npc = runtime.Callers(2, record.pcs[:])
		d.records[d.next%uint64(len(d.records))] = record
		d.next++
		d.mu.Unlock()
		return nil
	})
	// Register before the runtime's cleanup so the runtime closes first. This
	// makes owned event producers quiescent before restoring and dumping.
	t.Cleanup(func() {
		restore()
		if !t.Failed() {
			return
		}
		d.mu.Lock()
		count := d.next
		first := uint64(0)
		if count > uint64(len(d.records)) {
			first = count - uint64(len(d.records))
		}
		records := make([]guardPublicationRecord, 0, count-first)
		for i := first; i < count; i++ {
			records = append(records, d.records[i%uint64(len(d.records))])
		}
		d.mu.Unlock()
		t.Logf("guard publication diagnostic: captured=%d dropped=%d; meta event has no commit sequence, timing is perturbed", len(records), first)
		for _, record := range records {
			if record.label != "" {
				t.Logf("guard diagnostic %s %s", record.at.UTC().Format(time.RFC3339Nano), record.label)
				continue
			}
			t.Logf("guard diagnostic %s event=%s root=%q path=%q offset=%d length=%d", record.at.UTC().Format(time.RFC3339Nano), record.point, record.root, record.path, record.offset, record.length)
			frames := runtime.CallersFrames(record.pcs[:record.npc])
			for {
				frame, more := frames.Next()
				t.Logf("  %s %s:%d", frame.Function, frame.File, frame.Line)
				if !more {
					break
				}
			}
		}
	})
	return d
}

func (d *guardPublicationDiagnostic) mark(format string, args ...any) {
	label := fmt.Sprintf(format, args...)
	d.mu.Lock()
	d.records[d.next%uint64(len(d.records))] = guardPublicationRecord{at: time.Now(), label: label}
	d.next++
	d.mu.Unlock()
}
