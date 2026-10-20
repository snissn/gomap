package caching

import (
	"context"
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

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
		return err
	}
	old, err := db.installCOWBasis(basis, nil)
	err = joinCOWCleanupError(err, basis.release())
	if err != nil {
		return err
	}
	if old != nil {
		err = old.drain()
	}
	db.cow.refreshRequired.Store(false)
	return err
}

func (db *DB) finishCOWBackendMaintenance(fnErr error) error {
	if refresher, ok := db.backend.(valueLogSetRefresher); ok {
		if err := refresher.RefreshValueLogSet(); err != nil {
			return errors.Join(fnErr, err)
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
