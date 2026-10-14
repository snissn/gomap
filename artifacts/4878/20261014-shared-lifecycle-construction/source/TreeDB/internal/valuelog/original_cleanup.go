package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"os"
)

// FileCleanupFailure retains the same original File and first physical result.
// Error observation never retries an already consumed constituent.
type FileCleanupFailure struct {
	file  *File
	cause error
}

func (e *FileCleanupFailure) Error() string {
	return "value-log original file cleanup: " + e.cause.Error()
}
func (e *FileCleanupFailure) Unwrap() error { return e.cause }

// ManagerCleanupFailure keeps the original map, files, mappings and parent
// operands reachable after DB/startup discovery withdraws its Manager field.
type ManagerCleanupFailure struct {
	manager *Manager
	cause   error
	prior   error // linked once before publication at the original DB error seam
}

func (e *ManagerCleanupFailure) Error() string {
	return "value-log original manager cleanup: " + e.cause.Error()
}
func (e *ManagerCleanupFailure) Unwrap() error { return e.cause }

// LinkPriorCleanupError binds the preexisting DB sibling once. Physical causes
// and original failed-file links remain immutable. Error callbacks never run
// while Manager.mu is held; DB retention is performed by the original caller.
func (e *ManagerCleanupFailure) LinkPriorCleanupError(prior error) {
	if prior == nil || prior == e || errors.Is(prior, e) {
		return
	}
	e.TryLinkPriorCleanupError(prior)
}
func (e *ManagerCleanupFailure) OriginalCleanupPriorAvailable() bool {
	e.manager.mu.Lock()
	defer e.manager.mu.Unlock()
	return e.prior == nil
}
func (e *ManagerCleanupFailure) TryLinkPriorCleanupError(prior error) bool {
	e.manager.mu.Lock()
	defer e.manager.mu.Unlock()
	if e.prior != nil || prior == nil || prior == e {
		return false
	}
	e.prior = prior
	return true
}

// CleanupError returns the original sticky receipt without replaying disposal.
func (m *Manager) CleanupError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closeErr
}

// Called outside manager membership locks. The original File joins its actual
// final parent close and preserves the unique final-close operand on refusal.
func (f *File) closeOriginalRetirementParent() error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	return f.closeSuppliedRetirementParentLocked(releaseRetirementParent(f))
}
func (f *File) closeSuppliedRetirementParentLocked(parent *os.File) error {
	if parent != nil && !f.terminalParentAttempted {
		f.terminalParent = parent
	}
	if f.terminalParentAttempted {
		return f.terminalParentErr
	}
	if f.terminalParent == nil {
		return nil
	}
	f.terminalParentAttempted = true
	if err := f.terminalParent.Close(); err != nil {
		f.terminalParentErr = err
		if f.producerMetadata != nil {
			f.producerMetadata.CleanupFailed()
		}
		return err
	}
	f.terminalParent = nil
	return nil
}

func (m *Manager) finishTerminalFile(f *File) error {
	w := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	for {
		ready, err := m.finishNativeMVCCTerminalStep(f, false, &w)
		if ready || err != nil {
			return err
		}
	}
}

func (m *Manager) retainOriginalFileFailureLocked(f *File) {
	if !f.failureRegistered {
		f.failureNext, f.failureRegistered = m.failedFiles, true
		m.failedFiles = f
	}
	if m.closeErr == nil {
		m.closeReceipt.manager, m.closeReceipt.cause = m, f.terminalErr
		m.closeErr = &m.closeReceipt
	}
	if m.producerMetadata != nil {
		m.producerMetadata.CleanupFailed()
	}
}

// Each embedded original failure node is initialized before becoming visible;
// its next/error fields then stay immutable. Do not invoke error callbacks
// under the Manager lock or mutate a previously published error chain.
func (e *ManagerCleanupFailure) Is(target error) bool {
	e.manager.mu.RLock()
	head, prior := e.manager.failedFiles, e.prior
	e.manager.mu.RUnlock()
	for f := head; f != nil; f = f.failureNext {
		if errors.Is(f.terminalErr, target) {
			return true
		}
	}
	return prior != nil && errors.Is(prior, target)
}

// Physical success plus removal of the original last discovery/Set edges is
// required before the producer's containing wrapper contribution is refunded.
func (f *File) refundProducerMetadata() {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if !f.producerDetached.Load() || f.terminalWalk.Load() || f.closeErr != nil || f.terminalParentErr != nil || f.constructionParentErrors[0] != nil || f.constructionParentErrors[1] != nil || f.RefCount.Load() != 0 || f.producerDeleteGuards.Load() != 0 {
		return
	}
	// An admitted destructive caller owns deleteMu until its exact-parent
	// borrow and namespace action finish. Never tear down its physical aliases
	// while Evict/terminal Close is joining the same original File.
	if !f.deleteMu.TryLock() {
		return
	}
	defer f.deleteMu.Unlock()
	owner, charge := f.producerMetadata, f.producerCharge
	f.producerMetadata, f.producerCharge = nil, 0
	manager := f.manager
	f.File, f.manager = nil, nil
	if manager != nil && manager.fileAliases.Add(-1) < 0 {
		panic("value-log Manager file alias underflow")
	}
	// Path/identities remain immutable diagnostic locators for already-consumed
	// original operations; they never authorize a replacement physical object.
	f.dictLookup, f.templateLookup, f.templateDefCache = nil, nil, nil
	f.groupedFrameCacheBudget = nil
	if owner != nil {
		owner.RemovePending(charge)
	}
}
func (m *Manager) refundTerminalMetadataLocked() {
	if m.wrapperNativeCleanup || !m.closing || m.closeRunning || len(m.files) != 0 || m.closeErr != nil || m.wrapperBorrows.Load() != 0 || m.fileAliases.Load() != 0 {
		return
	}
	owner, charge := m.producerMetadata, m.producerCharge
	m.clearTerminalAliasesLocked()
	if owner != nil {
		owner.RemovePending(charge)
	}
}

// Successful authorized unlink remains a disposition even when original
// physical Close refused. Preserve that first refusal and original payload;
// never relabel completed unlink as rollback or refund failed backing.
func (m *Manager) finishOriginalDeletion(f *File, physical error, parent *os.File) error {
	f.terminalMu.Lock()
	defer f.terminalMu.Unlock()
	f.closeMu.Lock()
	parentErr := f.closeSuppliedRetirementParentLocked(parent)
	f.closeMu.Unlock()
	actual := errors.Join(physical, parentErr)
	if f.terminalAttempted {
		return errors.Join(f.terminalErr, actual)
	}
	f.terminalAttempted, f.terminalErr = true, actual
	if actual != nil {
		m.mu.Lock()
		m.retainOriginalFileFailureLocked(f)
		m.mu.Unlock()
		return m.closeErr
	}
	f.refundProducerMetadata()
	return nil
}

// Both fixed constructor parent operands belong to the preborn original File.
// A refused actual last Close remains reachable and cannot be retried.
func (f *File) closeConstructionParent(index int) error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if f.constructionParentAttempted[index] {
		return f.constructionParentErrors[index]
	}
	parent := f.constructionParents[index]
	if parent == nil {
		return nil
	}
	f.constructionParentAttempted[index] = true
	if err := parent.Close(); err != nil {
		f.constructionParentErrors[index] = err
		if f.producerMetadata != nil {
			f.producerMetadata.CleanupFailed()
		}
		return err
	}
	f.constructionParents[index] = nil
	return nil
}
