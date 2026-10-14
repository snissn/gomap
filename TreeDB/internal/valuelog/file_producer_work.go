package valuelog

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// This external receipt is embedded in the original Set whose actual File
// aliases are being disposed. It survives the File's containing refund.
type fileProducerReleaseWork struct {
	file    *File
	phase   uint8
	removal retainedalloc.RemovalWork
}

func (state *fileProducerReleaseWork) releaseStep(native bool, w *iterator.OrdinalScanWork) (bool, error) {
	if state.phase == 2 {
		return true, nil
	}
	if state.phase == 1 {
		ready, err := state.removal.ContinueWork(w)
		if ready {
			state.phase = 2
		}
		return ready, err
	}
	f := state.file
	if f == nil {
		state.phase = 2
		return true, nil
	}
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(*f))+2*uint64(unsafe.Sizeof(*state))+128) {
		return false, nil
	}
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	if !f.producerDetached.Load() || f.terminalWalk.Load() || f.closeErr != nil || f.terminalParentErr != nil || f.constructionParentErrors[0] != nil || f.constructionParentErrors[1] != nil || f.RefCount.Load() != 0 || f.producerDeleteGuards.Load() != 0 {
		state.file = nil
		state.phase = 2
		return true, nil
	}
	if !f.deleteMu.TryLock() {
		return false, nil
	}
	defer f.deleteMu.Unlock()
	owner, charge := f.producerMetadata, f.producerCharge
	if owner != nil {
		ready, err := state.removal.BindPendingWork(owner, charge, w)
		if !ready || err != nil {
			return false, err
		}
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*f))+64) {
		return false, nil
	}
	manager := f.manager
	f.producerMetadata, f.producerCharge = nil, 0
	f.File, f.manager = nil, nil
	f.dictLookup, f.templateLookup, f.templateDefCache = nil, nil, nil
	f.groupedFrameCacheBudget = nil
	state.file = nil
	if manager != nil && manager.fileAliases.Add(-1) < 0 {
		panic("value-log Manager file alias underflow")
	}
	if owner == nil {
		state.phase = 2
		return true, nil
	}
	state.phase = 1
	// Return through both original File unlocks before the external receipt
	// refunds the containing allocation on its next step.
	return false, nil
}
