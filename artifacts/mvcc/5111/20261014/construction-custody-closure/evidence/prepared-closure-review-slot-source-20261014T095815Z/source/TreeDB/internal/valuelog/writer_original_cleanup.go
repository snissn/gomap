package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
)

// The bounded original predecessor is part of the admitted Writer. A refused
// destructive close freezes this operand and prohibits another rotation.
type writerPredecessor struct {
	file, parent             *os.File
	proof                    *rootpublication.StableNamespaceCreationProof
	custody                  *rootpublication.OriginalCleanupCustody
	identity                 rootpublication.StableIdentity
	observed                 bool
	attempted, failed        bool
	fileCharge, parentCharge uint64
}

// WriterCleanupFailure owns the SAME original Writer, including partial
// construction, pending successor and refused predecessor operands.
type WriterCleanupFailure struct {
	writer *Writer
	causes [12]error
	count  int
	prior  error
}

func (e *WriterCleanupFailure) Error() string { return "value-log original writer cleanup failed" }
func (e *WriterCleanupFailure) LinkPriorCleanupError(prior error) {
	if prior == nil || prior == e || errors.Is(prior, e) {
		return
	}
	e.TryLinkPriorCleanupError(prior)
}
func (e *WriterCleanupFailure) OriginalCleanupPriorAvailable() bool {
	e.writer.closeMu.Lock()
	defer e.writer.closeMu.Unlock()
	return e.prior == nil
}
func (e *WriterCleanupFailure) TryLinkPriorCleanupError(prior error) bool {
	e.writer.closeMu.Lock()
	defer e.writer.closeMu.Unlock()
	if e.prior != nil || prior == nil || prior == e {
		return false
	}
	e.prior = prior
	return true
}
func (e *WriterCleanupFailure) Is(target error) bool {
	e.writer.closeMu.Lock()
	causes, count, prior := e.causes, e.count, e.prior
	e.writer.closeMu.Unlock()
	for i := 0; i < count; i++ {
		if errors.Is(causes[i], target) {
			return true
		}
	}
	return prior != nil && errors.Is(prior, target)
}
func (e *WriterCleanupFailure) As(target any) bool {
	e.writer.closeMu.Lock()
	causes, count, prior := e.causes, e.count, e.prior
	e.writer.closeMu.Unlock()
	for i := 0; i < count; i++ {
		if errors.As(causes[i], target) {
			return true
		}
	}
	return prior != nil && errors.As(prior, target)
}
func (w *Writer) addCleanupFailureLocked(err error) {
	if err == nil {
		return
	}
	e := &w.cleanupFailure
	e.writer = w
	if e.count < len(e.causes) {
		e.causes[e.count] = err
		e.count++
	}
	if w.closeErr == nil {
		w.closeErr = e
	}
	if w.metadata != nil {
		w.metadata.CleanupFailed()
	}
}
func (w *Writer) disposePredecessorLocked() error {
	p := &w.failedPredecessor
	if p.attempted {
		return w.closeErr
	}
	p.attempted = true
	if p.file != nil {
		if err := w.closeRotatedResource(p.file); err != nil {
			p.failed = true
			w.addCleanupFailureLocked(err)
		} else {
			p.file = nil
		}
	}
	if p.parent != nil {
		if err := w.closeRotatedResource(p.parent); err != nil {
			p.failed = true
			w.addCleanupFailureLocked(err)
		} else {
			p.parent = nil
		}
	}
	if p.proof != nil {
		if err := releaseStableCreationProof(p.proof, p.custody); err != nil {
			p.failed = true
			w.addCleanupFailureLocked(err)
		} else {
			p.proof, p.custody = nil, nil
		}
	}
	if p.observed && w.stableResourcePins != nil {
		if err := w.stableResourcePins.Unobserve(p.identity); err != nil {
			p.failed = true
			w.addCleanupFailureLocked(err)
		} else {
			p.observed = false
		}
	}
	if p.failed {
		return w.closeErr
	}
	charge := p.fileCharge + p.parentCharge
	*p = writerPredecessor{}
	if w.metadata != nil && charge != 0 {
		w.metadataCharge -= charge
		w.metadata.RemovePending(charge)
	}
	return w.closeErr
}
func (w *Writer) disposePendingSuccessorLocked() error {
	p := w.pendingStableSuccessor
	if p == nil {
		return w.closeErr
	}
	if p.cleanupAttempted {
		return w.closeErr
	}
	p.cleanupAttempted = true
	if p.creationProof != nil {
		if err := p.creationProof.ReleaseWithError(); err != nil {
			p.failure.add(err)
			w.addCleanupFailureLocked(err)
		} else {
			p.creationProof = nil
		}
	}
	if p.file != nil {
		if err := p.file.Close(); err != nil {
			p.failure.add(err)
			w.addCleanupFailureLocked(err)
		} else {
			p.file = nil
		}
	}
	if p.parentOwned && p.parent != nil && !p.parentCloseAttempted {
		p.parentCloseAttempted = true
		if err := p.parent.Close(); err != nil {
			p.failure.add(err)
			w.addCleanupFailureLocked(err)
		} else {
			p.parent = nil
		}
	}
	if p.stableObserved && w.stableResourcePins != nil {
		if err := w.stableResourcePins.Unobserve(p.stableIdentity); err != nil {
			p.failure.add(err)
			w.addCleanupFailureLocked(err)
		} else {
			p.stableObserved = false
		}
	}
	if p.failure.count != 0 {
		p.failStop = true
		p.disposeMetadata()
		return w.closeErr
	}
	w.refundPendingWriterReserveLocked(p)
	p.disposeMetadata()
	w.pendingStableSuccessor = nil
	return w.closeErr
}

// The original Writer retains a token whose physical release refused, including
// constructor/rotation cleanup before a publication wrapper exists.
func (w *Writer) releaseUnusedStableToken(token *rootpublication.StableResourceToken) {
	if token == nil {
		return
	}
	err := token.ReleaseWithError()
	if err == nil {
		return
	}
	w.closeMu.Lock()
	for i := range w.cleanupTokens {
		if w.cleanupTokens[i] == nil {
			w.cleanupTokens[i] = token
			break
		}
	}
	w.addCleanupFailureLocked(err)
	w.closeMu.Unlock()
}
func (w *Writer) refundPendingWriterReserveLocked(p *pendingValueLogSuccessor) {
	if p.writerFileReserve != 0 && w.metadata != nil {
		charge := p.writerFileReserve
		p.writerFileReserve = 0
		w.metadataCharge -= charge
		w.metadata.RemovePending(charge)
	}
}

// This is the preborn receipt within the original admitted rotation wrapper.
type StableRotationCleanupFailure struct {
	rotation *StableResourceRotation
	causes   [2]error
	prior    error
}

func (e *StableRotationCleanupFailure) Error() string {
	return "value-log original rotation cleanup failed"
}
func (e *StableRotationCleanupFailure) LinkPriorCleanupError(prior error) {
	if prior == nil || prior == e || errors.Is(prior, e) {
		return
	}
	e.TryLinkPriorCleanupError(prior)
}
func (e *StableRotationCleanupFailure) OriginalCleanupPriorAvailable() bool {
	e.rotation.releaseMu.Lock()
	defer e.rotation.releaseMu.Unlock()
	return e.prior == nil
}
func (e *StableRotationCleanupFailure) TryLinkPriorCleanupError(prior error) bool {
	e.rotation.releaseMu.Lock()
	defer e.rotation.releaseMu.Unlock()
	if e.prior != nil || prior == nil || prior == e {
		return false
	}
	e.prior = prior
	return true
}
func (e *StableRotationCleanupFailure) Is(target error) bool {
	e.rotation.releaseMu.Lock()
	causes, prior := e.causes, e.prior
	e.rotation.releaseMu.Unlock()
	return errors.Is(causes[0], target) || errors.Is(causes[1], target) || (prior != nil && errors.Is(prior, target))
}
func (e *StableRotationCleanupFailure) As(target any) bool {
	e.rotation.releaseMu.Lock()
	causes, prior := e.causes, e.prior
	e.rotation.releaseMu.Unlock()
	return errors.As(causes[0], target) || errors.As(causes[1], target) || (prior != nil && errors.As(prior, target))
}
