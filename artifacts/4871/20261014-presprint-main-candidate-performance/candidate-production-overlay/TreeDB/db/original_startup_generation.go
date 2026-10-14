package db

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
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
		metadata, charge, cleanup := g.closeWithCompletionV6()
		g = nil
		if metadata != nil && charge != 0 {
			metadata.RemovePending(charge)
		}
		return nil, errors.Join(err, cleanup)
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

func (g *indexGen) finishStartupFailure(operation error, closeLock bool) (*retainedalloc.Owner, uint64, error) {
	if g.startupReceipt.cause != nil {
		return nil, 0, errors.Join(operation, &g.startupReceipt)
	}
	var cleanup error
	if g.startupConstructed {
		cleanup = g.startupDB.Close()
	} else {
		cleanup = g.close()
		cleanup = errors.Join(cleanup, g.closeStartupManager())
		if g.startupDB.ghostManager != nil {
			g.startupDB.closeGhostManagerCompletionV6(false)
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
		return nil, 0, &g.startupReceipt
	}
	custody := g.startupCustody
	g.startupDB, g.startupManager, g.startupLock, g.startupCustody = nil, nil, nil, nil
	g.startupConstructed = false
	metadata, charge, err := g.detachGenerationMetadataCompletionV6()
	g = nil
	custody.Complete(nil, nil)
	custody = nil
	return metadata, charge, errors.Join(operation, err)
}

// Offline maintenance borrows the already-admitted read-only source wrapper
// before any setup or replacement admission. Its original startup receipt owns
// failed cleanup after the source DB has withdrawn ordinary discovery aliases.
// Successful Open consumed startupCustody; this borrow does not recreate it.
func (g *indexGen) retainOfflineSourceV1(d *DB, lock *lockfile.Lock) {
	g.retainWrapperBorrow()
	g.startupDB, g.startupManager, g.startupLock = d, d.valueLogManager, lock
	g.startupConstructed = true
}

// Return the original containing contribution only after physical source
// completion. The caller clears its source/generation aliases before refund.
func (g *indexGen) finishOfflineSourceV1(operation error) (*retainedalloc.Owner, uint64, error) {
	if g.startupReceipt.cause != nil {
		return nil, 0, &g.startupReceipt
	}
	cleanup := g.startupDB.Close()
	// A completed DB Close can precede the outer namespace parent Close.
	// Preserve the original physical refusal carried by the operation.
	// A semantic operation refusal alone does not retain source or LOCK.
	var original *originalDBCleanupFailure
	if errors.As(operation, &original) {
		cleanup = errors.Join(cleanup, original)
	}
	if cleanup != nil {
		g.startupReceipt.cause = errors.Join(operation, cleanup, ErrRecoveryRequired)
		g.startupReceipt.generation = g
		g.metadata.CleanupFailed()
		return nil, 0, &g.startupReceipt
	}
	sourceDB, sourceManager, sourceLock := g.startupDB, g.startupManager, g.startupLock
	g.startupDB, g.startupManager, g.startupLock = nil, nil, nil
	g.startupConstructed = false
	metadata, charge, err := g.releaseWrapperBorrowCompletionV6(nil, nil)
	if err != nil {
		g.startupDB, g.startupManager, g.startupLock = sourceDB, sourceManager, sourceLock
		g.startupConstructed = true
		// The original close result is checked again under the generation's
		// completion lock. A refusal keeps the same borrow and physical payload.
		g.startupReceipt.cause = errors.Join(operation, err, ErrRecoveryRequired)
		g.startupReceipt.generation = g
		g.metadata.CleanupFailed()
		return nil, 0, &g.startupReceipt
	}
	sourceDB, sourceManager, sourceLock = nil, nil, nil
	return metadata, charge, operation
}

// The offline DATA replacement reuses the original admitted index generation
// and startup receipt. The source DB, replacement pager and original LOCK stay
// in that same payload until both physical completion edges have succeeded.
func (g *indexGen) finishOfflineReplacementV1(operation error) (*retainedalloc.Owner, uint64, error) {
	if g.startupReceipt.cause != nil {
		return nil, 0, &g.startupReceipt
	}
	if g.startupDB == nil {
		return nil, 0, operation
	}
	cleanup := errors.Join(g.close(), g.startupDB.Close())
	if cleanup != nil {
		g.startupReceipt.cause = errors.Join(operation, cleanup, ErrRecoveryRequired)
		g.startupReceipt.generation = g
		g.startupCustody.Complete(g, &g.startupReceipt)
		g.metadata.CleanupFailed()
		return nil, 0, &g.startupReceipt
	}
	g.finishStartupSuccess()
	metadata, charge, err := g.detachGenerationMetadataCompletionV6()
	g = nil
	return metadata, charge, errors.Join(operation, err)
}

// Admit the physical parent/child before opening them. The original generation
// receives both handles immediately; failed last Close is never a scope refund.
func (g *indexGen) openOfflineReplacementPagerV1(dir string, chunkSize int64) error {
	metadata := g.metadata
	parentCharge, err := rootpublication.StableFileMetadataCharge(uint64(len(dir)))
	if err != nil {
		return err
	}
	if err = metadata.AddPending(parentCharge); err != nil {
		return err
	}
	parent, err := rootpublication.OpenStableParent(dir)
	if err != nil {
		metadata.RemovePending(parentCharge)
		return err
	}
	g.stableNamespaceParent = parent
	g.stableNamespaceMetadata = metadata
	g.stableNamespaceParentCharge = parentCharge
	path := filepath.Join(dir, indexNewFileName)
	fileCharge, err := rootpublication.StableFileMetadataCharge(uint64(len(path)))
	if err != nil {
		return err
	}
	if err = metadata.AddPending(fileCharge); err != nil {
		return err
	}
	file, err := rootpublication.OpenStableChildFile(parent, indexNewFileName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		metadata.RemovePending(fileCharge)
		return err
	}
	g.pager, err = pager.OpenOwnedFileWithMetadata(file, file.Name(), chunkSize, pager.OpenOptions{}, metadata, fileCharge)
	return err
}
