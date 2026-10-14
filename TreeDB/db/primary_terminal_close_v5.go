package db

import (
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

var errTerminalRetirementBlockedV5 = errors.New("db: terminal owned retirement remains physically retained")

func (db *DB) terminalWorkV5() *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
}
func (db *DB) recordTerminalWorkV5(w *iterator.OrdinalScanWork) {
	db.terminalRetirementRecords += w.Records
	db.terminalRetirementBytes += w.Bytes
	db.terminalRetirementSteps++
}

func (db *DB) finishTerminalRetirementV5(runtime *rootPublicationRuntimeV1) error {
	selectionWork := db.terminalWorkV5()
	if !selectionWork.Reserve(1, 2*uint64(unsafe.Sizeof(DB{}))) {
		return errTerminalRetirementBlockedV5
	}
	db.primaryCustodyMu.Lock()
	hasSnapshots := db.primaryCustodyHead != nil
	db.primaryCustodyMu.Unlock()
	hasReceipt := false
	if runtime != nil && runtime.coordinator != nil {
		if !selectionWork.Reserve(1, 2*uint64(unsafe.Sizeof(rootpublication.Coordinator{}))) {
			db.recordTerminalWorkV5(selectionWork)
			return errTerminalRetirementBlockedV5
		}
		hasReceipt = runtime.coordinator.HasOwnedFinishV5()
	}
	db.recordTerminalWorkV5(selectionWork)
	if !hasSnapshots && !hasReceipt {
		return nil
	}
	if db.valueLogManager != nil {
		w := db.terminalWorkV5()
		ready, e := db.valueLogManager.BeginTerminalRetirementV5(w)
		db.recordTerminalWorkV5(w)
		if e != nil || !ready {
			return errors.Join(errTerminalRetirementBlockedV5, e)
		}
	}
	if hasReceipt {
		w := db.terminalWorkV5()
		owner, ready, e := runtime.coordinator.TakeTerminalOwnedFinishV5(w)
		db.recordTerminalWorkV5(w)
		if e != nil || !ready {
			return errors.Join(errTerminalRetirementBlockedV5, e)
		}
		if owner != nil {
			for {
				w = db.terminalWorkV5()
				var progress bool
				ready, progress, e = owner.Step(w)
				db.recordTerminalWorkV5(w)
				if e != nil {
					return errors.Join(errTerminalRetirementBlockedV5, e)
				}
				if ready {
					break
				}
				// Callback headers alone are not proof of progress. A callback must retain
				// finite cursor progress and either complete or expose an explicit refusal.
				if !progress {
					return errTerminalRetirementBlockedV5
				}
			}
		}
	}
	for {
		w := db.terminalWorkV5()
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(DB{}))) {
			return errTerminalRetirementBlockedV5
		}
		db.primaryCustodyMu.Lock()
		c := db.primaryCustodyHead
		db.primaryCustodyMu.Unlock()
		if c == nil {
			db.recordTerminalWorkV5(w)
			return nil
		}
		ready, progress, e := c.terminalStepV5(db, w)
		if e != nil {
			db.recordTerminalWorkV5(w)
			return errors.Join(errTerminalRetirementBlockedV5, e)
		}
		if !ready && !progress {
			db.recordTerminalWorkV5(w)
			return fmt.Errorf("%w: private Snapshot still has an active read", errTerminalRetirementBlockedV5)
		}
		if ready && w.Reserve(1, 2*uint64(unsafe.Sizeof(DB{}))+2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
			db.primaryCustodyMu.Lock()
			if db.primaryCustodyHead != c {
				completedLive := c.done && c.ownerDB == nil
				db.primaryCustodyMu.Unlock()
				db.recordTerminalWorkV5(w)
				if completedLive {
					continue
				}
				return errTerminalRetirementBlockedV5
			}
			if c.next != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(primarySnapshotCustodyV5{}))) {
				db.primaryCustodyMu.Unlock()
				db.recordTerminalWorkV5(w)
				continue
			}
			db.primaryCustodyHead = c.next
			if c.next != nil {
				c.next.prev = nil
			}
			c.next, c.prev = nil, nil
			c.ownerDB = nil
			db.primaryCustodyMu.Unlock()
		}
		db.recordTerminalWorkV5(w)
	}
}
