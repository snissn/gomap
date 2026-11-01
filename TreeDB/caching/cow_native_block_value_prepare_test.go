package caching

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// Candidate-only control: unlike the baseline progress overlay, this test calls
// the routing seam to distinguish pre-read refusal from its shared fallback.
// A deliberately corrupt payload CRC makes any accidental real read observable.
func TestNativeValueBlockWorkspaceRawPreparationBeforePayload(t *testing.T) {
	for _, extra := range []bool{false, true} {
		for _, retained := range []string{"fresh", "input", "output"} {
			t.Run(fmt.Sprintf("extra=%t/retained=%s", extra, retained), func(t *testing.T) {
				db, _, capture, ptrs, want := nativeBlockValueFixture(t, -1, 2)
				basis, budget := capture.cut.basis, db.cow.budget
				f, _, err := capture.Snapshot().PinnedValueLogFile(ptrs[0].FileID)
				if err != nil {
					t.Fatal(err)
				}
				shape, err := valuelog.InspectCOWRecord(f, ptrs[0], db.cow.readLimits())
				if err != nil {
					t.Fatal(err)
				}
				mu, owner := &basis.readMu, &basis.reader
				if extra {
					basis.readMu.Lock()
					defer basis.readMu.Unlock()
					mu, owner = &basis.readSlots[0].mu, &basis.readSlots[0].reader
				}
				var old *cowReadWorkspace
				if retained != "fresh" {
					mu.Lock()
					old, err = basis.workspaceLocked(owner, budget, db.cow.readLimits())
					if err == nil {
						if retained == "input" {
							err = old.grow(shape.PayloadBytes(), 0)
						} else {
							err = old.grow(0, shape.ValueBytes)
						}
					}
					mu.Unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
				before := budget.Stats().ReservedBytes
				overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
				room := uint64(0)
				if retained == "fresh" {
					probe, err := admitCOWReadWorkspace(budget, db.cow.readLimits())
					if err != nil {
						t.Fatal(err)
					}
					room = budget.Stats().ReservedBytes - before
					probe.close()
				}
				pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before - overhead - room)
				if err != nil {
					t.Fatal(err)
				}
				defer pressure.Close()
				writer, err := os.OpenFile(f.Name(), os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Close()
				var crc [1]byte
				off := int64(ptrs[0].Offset - 4)
				if _, err := f.ReadAt(crc[:], off); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := writer.WriteAt(crc[:], off); err != nil {
						t.Error(err)
					}
				}()
				if _, err := writer.WriteAt([]byte{crc[0] ^ 1}, off); err != nil {
					t.Fatal(err)
				}
				calls := 0
				nativeValueWithHeldPhysical(t, db.cow, func() error {
					handled, value, err := basis.tryNativeBasisValue(db.cow, ptrs[0], func(uint64) error { calls++; return nil })
					if handled || value != nil || err != nil || calls != 0 {
						return fmt.Errorf("preparation read corrupt payload/callback: handled=%t len=%d err=%v calls=%d", handled, len(value), err, calls)
					}
					return nil
				})
				if *owner != nil {
					t.Fatal("refused exact owner remains installed")
				}
				if old != nil && (old.allocations != nil || old.input != nil || old.output != nil || old.budget != nil || old.lease != nil) {
					t.Fatal("raw partial owner retained refunded backing")
				}
				if _, err := writer.WriteAt(crc[:], off); err != nil {
					t.Fatal(err)
				}
				pressure.Close()
				got, err := capture.NativeValueAtPointer(ptrs[0], nil)
				if err != nil || !bytes.Equal(got, want[0]) {
					t.Fatal("read after refusal released", err)
				}
			})
		}
	}
}
