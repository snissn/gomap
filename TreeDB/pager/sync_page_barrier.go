package pager

import "os"

var syncPageFileBarrierFn = syncPageFileBarrierData

// syncPageFileBarrier orders phase-one index data before the later target-meta
// write. The target meta still receives the ordinary full stable-storage sync.
func syncPageFileBarrier(file *os.File) error {
	if file == nil {
		return nil
	}
	return syncPageFileBarrierFn(file)
}

func syncPageFileBarrierData(file *os.File) error {
	return syncPageFile(file)
}
