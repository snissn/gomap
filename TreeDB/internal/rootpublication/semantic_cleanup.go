package rootpublication

import "sync/atomic"

// SemanticCleanupWork is the original semantic owner's destructor. It runs on
// the existing cache flush worker, outside publication and read latches. Each
// physical member is detached only after successful destruction; failure keeps
// the exact original objects and their admitted charges on this same Work.
type SemanticCleanupWork interface{ FinishSemanticCleanup() error }

// SemanticCleanup is embedded and admitted once in each native EngineVersion.
// Last-reference release merely detaches and enqueues this original record.
// Next belongs exclusively to the existing worker's intrusive queue/failed list.
// No replacement work, resource table, scheduler or failure keeper is created.
type SemanticCleanup struct {
	Next      *SemanticCleanup
	Work      SemanticCleanupWork
	Err       error
	queued    atomic.Bool
	attempted bool
}

// EnqueueOnce transfers the original record exactly once, without allocation.
func (w *SemanticCleanup) EnqueueOnce() bool {
	return w != nil && w.Work != nil && w.queued.CompareAndSwap(false, true)
}

// Finish invokes original destruction once. Observing a terminal failure never
// retries any successful or failed destructor and never drops original custody.
// The sole worker serializes this method with its queue transfer.
func (w *SemanticCleanup) Finish() error {
	if w.attempted {
		return w.Err
	}
	w.attempted = true
	w.Err = w.Work.FinishSemanticCleanup()
	if w.Err == nil {
		w.Work = nil
	}
	return w.Err
}
