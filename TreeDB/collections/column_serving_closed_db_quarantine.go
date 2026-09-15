package collections

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// A coordinator normally owns the physical ledger only while its DB is open.
// If close cannot confirm every physical release, this registry becomes the
// durable Go cleanup owner after the ordinary coordinator and lifecycle
// registries are removed. DB close removes the guardian's effective maintenance
// pin; the retained guardian object is only cleanup-state ownership at that
// point. Records are never serving authority and deliberately do not retain the
// closed *DB object graph. A reopened DB has a distinct serving key and
// coordinator; the monotonic record ID identifies this cleanup owner.
type columnGraphClosedDBPhysicalQuarantineRecord struct {
	id         uint64
	root       string
	collection string
	ledger     *typedGraphPhysicalResourceLedger
	closeErr   string
}

var columnGraphClosedDBPhysicalQuarantines struct {
	sync.Mutex
	nextID  uint64
	records map[uint64]*columnGraphClosedDBPhysicalQuarantineRecord
}

// ColumnGraphClosedDBPhysicalQuarantineStats reports a process-global physical
// cleanup owner that survived DB close. The snapshot is diagnostic only, has no
// post-close lifecycle-pin effect, and cannot be used as serving authority.
type ColumnGraphClosedDBPhysicalQuarantineStats struct {
	ID         uint64                           `json:"id"`
	Root       string                           `json:"root"`
	Collection string                           `json:"collection"`
	CloseError string                           `json:"close_error,omitempty"`
	Physical   ColumnGraphPhysicalResourceStats `json:"physical"`
}

func registerColumnGraphClosedDBPhysicalQuarantine(db *backenddb.DB, root, collection string, ledger *typedGraphPhysicalResourceLedger, closeErr error) {
	if db == nil || ledger == nil {
		return
	}
	ledger.Lock()
	if ledger.stats.empty() {
		ledger.Unlock()
		return
	}
	ledger.Unlock()

	columnGraphClosedDBPhysicalQuarantines.Lock()
	defer columnGraphClosedDBPhysicalQuarantines.Unlock()
	for _, record := range columnGraphClosedDBPhysicalQuarantines.records {
		if record != nil && record.ledger == ledger {
			if closeErr != nil {
				record.closeErr = closeErr.Error()
			}
			return
		}
	}
	columnGraphClosedDBPhysicalQuarantines.nextID++
	record := &columnGraphClosedDBPhysicalQuarantineRecord{
		id: columnGraphClosedDBPhysicalQuarantines.nextID, root: root,
		collection: collection, ledger: ledger,
	}
	if closeErr != nil {
		record.closeErr = closeErr.Error()
	}
	if columnGraphClosedDBPhysicalQuarantines.records == nil {
		columnGraphClosedDBPhysicalQuarantines.records = make(map[uint64]*columnGraphClosedDBPhysicalQuarantineRecord)
	}
	columnGraphClosedDBPhysicalQuarantines.records[record.id] = record
}

// ColumnGraphClosedDBPhysicalQuarantineSnapshot is a bounded metadata snapshot.
// Empty records left by a late external holder close are discarded here; no OS
// work is performed while the global registry lock is held.
func ColumnGraphClosedDBPhysicalQuarantineSnapshot() []ColumnGraphClosedDBPhysicalQuarantineStats {
	columnGraphClosedDBPhysicalQuarantines.Lock()
	defer columnGraphClosedDBPhysicalQuarantines.Unlock()
	out := make([]ColumnGraphClosedDBPhysicalQuarantineStats, 0, len(columnGraphClosedDBPhysicalQuarantines.records))
	for id, record := range columnGraphClosedDBPhysicalQuarantines.records {
		if record == nil || record.ledger == nil {
			delete(columnGraphClosedDBPhysicalQuarantines.records, id)
			continue
		}
		stats, limits := record.ledger.snapshotWithLimits()
		if stats.empty() {
			delete(columnGraphClosedDBPhysicalQuarantines.records, id)
			continue
		}
		out = append(out, ColumnGraphClosedDBPhysicalQuarantineStats{
			ID: record.id, Root: record.root, Collection: record.collection,
			CloseError: record.closeErr, Physical: columnGraphPhysicalResourceStats(limits, stats, true),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// retryColumnGraphClosedDBPhysicalQuarantines is intentionally package-local:
// normal retry sites are pre-admission and DB teardown. Tests may use this to
// prove a retryable close failure can be cleared after teardown without making
// a closed DB's holder reusable.
func retryColumnGraphClosedDBPhysicalQuarantines() error {
	columnGraphClosedDBPhysicalQuarantines.Lock()
	records := make([]*columnGraphClosedDBPhysicalQuarantineRecord, 0, len(columnGraphClosedDBPhysicalQuarantines.records))
	for _, record := range columnGraphClosedDBPhysicalQuarantines.records {
		records = append(records, record)
	}
	columnGraphClosedDBPhysicalQuarantines.Unlock()
	var retryErr error
	for _, record := range records {
		if record == nil || record.ledger == nil {
			continue
		}
		if err := record.ledger.retryQuarantinedPhysicalCleanup(); err != nil {
			retryErr = errors.Join(retryErr, fmt.Errorf("collections: closed DB physical cleanup root=%q collection=%q: %w", record.root, record.collection, err))
		}
	}
	_ = ColumnGraphClosedDBPhysicalQuarantineSnapshot() // prune confirmed records
	return retryErr
}

func (ledger *typedGraphPhysicalResourceLedger) closeForDB(db *backenddb.DB, root, collection string) error {
	if ledger == nil {
		return nil
	}
	ledger.cleanupMu.Lock()
	ledger.Lock()
	ledger.closedDB = true
	ledger.Unlock()
	retryErr := ledger.retryQuarantinedPhysicalCleanupLocked()
	stats := ledger.snapshot()
	ledger.cleanupMu.Unlock()
	if stats.empty() {
		return retryErr
	}
	retainedErr := fmt.Errorf("collections: DB close retains serving physical resources for collection %q: %w", collection, errColumnServingSegmentCleanupRetained)
	closeErr := errors.Join(retryErr, retainedErr)
	registerColumnGraphClosedDBPhysicalQuarantine(db, root, collection, ledger, closeErr)
	return closeErr
}
