package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/pager"
)

// The existing generation is born before Pager/Manager creation. Its original
// registry guard survives registry.Close until actual startup completion. The
// fatal error is the external ownership edge after Open returns nil,error.
func newStartupIndexGeneration(d *DB, lock *lockfile.Lock) (*indexGen, error) {
	g, err := newIndexGenWithMetadata(1, nil, nil, nil, d.valueLogIdentityPins.MetadataOwner())
	if err != nil {
		return nil, err
	}
	custody, err := rootpublication.NewOriginalCleanupCustody(d.valueLogIdentityPins, g.metadata)
	if err != nil {
		return nil, errors.Join(err, g.close())
	}
	g.startupDB, g.startupLock, g.startupCustody = d, lock, custody
	return g, nil
}

func (g *indexGen) finishStartupSuccess() {
	custody := g.startupCustody
	g.startupDB, g.startupManager, g.startupLock, g.startupCustody = nil, nil, nil, nil
	g.startupConstructed = false
	custody.Complete(nil, nil)
}

// A consumed Manager edge and an error are independent. Never invoke a failed
// original Close again, or drop the original Manager merely on callback return.
func (g *indexGen) closeStartupManager() error {
	if !g.startupManagerAttempted {
		g.startupManagerAttempted = true
		g.startupManagerErr = g.startupManager.Close()
		if g.startupManagerErr == nil {
			g.startupManager = nil
		}
	}
	return g.startupManagerErr
}

func (g *indexGen) finishStartupFailure(operation error, closeLock bool) error {
	if g.startupReceipt.cause != nil {
		return errors.Join(operation, &g.startupReceipt)
	}
	var cleanup error
	if g.startupConstructed {
		cleanup = g.startupDB.Close()
	} else {
		cleanup = g.close()
		cleanup = errors.Join(cleanup, g.closeStartupManager())
		if g.startupDB.ghostManager != nil {
			g.startupDB.ghostManager.closeAll()
		}
	}
	// A producer may already have retained its partially opened original pager
	// in its own fatal receipt. Preserve that actual refusal rather than treating
	// the absent second return value as successful physical cleanup.
	var partial *pager.ReadOnlyCleanupError
	if errors.As(operation, &partial) {
		cleanup = errors.Join(cleanup, partial)
	}
	var partialManager *valuelog.ManagerCleanupFailure
	if errors.As(operation, &partialManager) {
		// A nil Manager result cannot erase a constructor's retained original
		// File/parent refusal. The fatal receipt owns that same payload and LOCK.
		cleanup = errors.Join(cleanup, partialManager)
	}
	if cleanup == nil && closeLock {
		cleanup = g.startupLock.Close()
	}
	if cleanup != nil {
		g.startupReceipt.cause = errors.Join(operation, cleanup, ErrRecoveryRequired)
		g.startupReceipt.generation = g
		g.startupCustody.Complete(g, &g.startupReceipt)
		g.metadata.CleanupFailed()
		return &g.startupReceipt
	}
	custody := g.startupCustody
	g.startupDB, g.startupManager, g.startupLock, g.startupCustody = nil, nil, nil, nil
	g.startupConstructed = false
	g.scrubAndRefundGeneration()
	custody.Complete(nil, nil)
	return operation
}
