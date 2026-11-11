package caching

import (
	"context"
	"errors"
	"fmt"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// backendMaintenanceError adds no allocations on successful calls while
// preserving errors.Is/As through a failure's concrete ownership phase.
func backendMaintenanceError(stage string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("backend maintenance %s: %w", stage, err)
}

// RunBackendMaintenance fences cached writers around an existing backend
// operation. Its caller performs the required checkpoint first. COW then
// refreshes the equivalent backend basis before source writes resume.
func (db *DB) RunBackendMaintenance(fn func() error) error {
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true}, fn)
}

// RunBackendMaintenanceWithPostRefresh completes caller-owned retirement after
// the old COW basis drains, under the SAME maintenance writer fence. A failed
// operation or basis refresh never grants permission to run the settle callback.
func (db *DB) RunBackendMaintenanceWithPostRefresh(fn, settle func() error) error {
	return db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true, afterCOWRefresh: settle}, fn)
}

func (db *DB) refreshCOWBackendBasis() error {
	basis, err := db.cow.captureBackendBasis(db.backend.(backendSnapshotProvider))
	if err != nil {
		return backendMaintenanceError("capture COW basis", err)
	}
	defer db.releaseCOWBasisOnUnwind(&basis)
	old, err := db.installCOWBasis(basis, nil)
	err = joinCOWCleanupError(err, basis.release())
	basis = nil
	if err != nil {
		return backendMaintenanceError("install COW basis", err)
	}
	if old != nil {
		err = old.drain()
	}
	db.cow.refreshRequired.Store(false)
	return backendMaintenanceError("drain previous COW basis", err)
}

// Original capture ownership also survives a provider panic/Goexit. The
// containing basis admission covers these fixed deferred-call cells. Normal
// paths consume and clear the slot explicitly, preserving their error order.
func (db *DB) releaseCOWBasisOnUnwind(slot **cowBackendBasis) {
	basis := *slot
	*slot = nil
	if basis != nil {
		// A live slot means the install did not return normally. Physical
		// maintenance may already be accepted even if capture release succeeds.
		// Publish reconciliation debt before its fallible destructor runs.
		db.cow.refreshRequired.Store(true)
		if err := basis.release(); err != nil {
			db.poisonNativeHandoff(err)
			db.retainOriginalCleanupError(err)
		}
	}
}

func (db *DB) finishCOWBackendMaintenance(fnErr error) error {
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return errors.Join(fnErr, backendMaintenanceError("refresh backend value-log set after callback", err))
		}
	}
	return errors.Join(fnErr, db.refreshCOWBackendBasis())
}

// ValueLogRewriteOnline fences a named equivalent physical rewrite with the
// existing cache owner. The caller supplies ordinary rewrite options only; it
// cannot grant this role to a callback or another physical publication.
func (db *DB) ValueLogRewriteOnline(ctx context.Context, opts backenddb.ValueLogRewriteOnlineOptions) (stats backenddb.ValueLogRewriteStats, err error) {
	if db == nil || db.cow == nil {
		return stats, backenddb.ErrNativeSemanticUnsupported
	}
	err = db.runWithBackendMaintenanceOptions(backendMaintenanceOptions{skipCheckpoint: true, completeAdmittedCOW: true}, func() error {
		// Read the original role only after admission: pending Close cannot dispose
		// its containing cache while this operation owns the original flush fence.
		if db.cow.txnOwner == nil {
			return backenddb.ErrNativeSemanticUnsupported
		}
		rewriter, ok := db.backend.(interface {
			ValueLogRewriteOnlineForPrivateCOW(*transactionowner.Owner, context.Context, backenddb.ValueLogRewriteOnlineOptions) (backenddb.ValueLogRewriteStats, error)
		})
		if !ok {
			return backenddb.ErrNativeSemanticUnsupported
		}
		var rewriteErr error
		stats, rewriteErr = rewriter.ValueLogRewriteOnlineForPrivateCOW(db.cow.txnOwner, ctx, opts)
		return rewriteErr
	})
	return stats, err
}
