package collections

import (
	"fmt"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func (domain *collectionWriteDomain) recordPendingCommandWALLSNLocked(lsn uint64) error {
	if domain == nil || lsn == 0 {
		return nil
	}
	if domain.pendingCommandWALFirst == 0 {
		domain.pendingCommandWALFirst = lsn
		domain.pendingCommandWALLast = lsn
		return nil
	}
	if lsn != domain.pendingCommandWALLast+1 {
		return fmt.Errorf("%w: pending collection command WAL range [%d,%d] cannot cover interleaved lsn %d", backenddb.ErrCommandWALAppliedLSNNonContig, domain.pendingCommandWALFirst, domain.pendingCommandWALLast, lsn)
	}
	domain.pendingCommandWALLast = lsn
	return nil
}

func (domain *collectionWriteDomain) pendingCommandWALCoverageIntentLocked(db *backenddb.DB) (*backenddb.CommandWALIntent, uint64, error) {
	if domain == nil || db == nil || !db.CommandWALEnabled() {
		return nil, 0, nil
	}
	first, last := domain.pendingCommandWALFirst, domain.pendingCommandWALLast
	if first == 0 || last == 0 {
		return nil, 0, nil
	}
	state := db.State()
	if state == nil {
		return nil, 0, backenddb.ErrClosed
	}
	current := state.AppliedCommandLSN
	if last <= current {
		domain.clearPendingCommandWALThroughLocked(current)
		return nil, 0, nil
	}
	if first > current+1 {
		return nil, 0, fmt.Errorf("%w: pending collection command WAL starts at %d after applied %d", backenddb.ErrCommandWALAppliedLSNNonContig, first, current)
	}
	applied := last
	intent, err := backenddb.NewCommandWALCoverageIntent(applied, backenddb.CommandWALLSNRange{First: current + 1, Last: applied})
	if err != nil {
		return nil, 0, err
	}
	return intent, applied, nil
}

func (domain *collectionWriteDomain) clearPendingCommandWALThroughLocked(lsn uint64) {
	if domain == nil || lsn == 0 {
		return
	}
	first, last := domain.pendingCommandWALFirst, domain.pendingCommandWALLast
	if first == 0 || last == 0 || lsn < first {
		return
	}
	if lsn >= last {
		domain.pendingCommandWALFirst = 0
		domain.pendingCommandWALLast = 0
		return
	}
	domain.pendingCommandWALFirst = lsn + 1
}
