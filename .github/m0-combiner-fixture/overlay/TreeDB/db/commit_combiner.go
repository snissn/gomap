package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
)

const (
	commitCombineQueueCap = 4096
	commitCombineMaxBatch = 64
)

var errCommitCombinerClosed = errors.New("treedb: commit combiner closed")

type commitCombineReq struct {
	input  mvccadmission.Input
	key    []byte
	value  []byte
	del    bool
	sync   bool
	result chan error
	m0     *m0CombinerSlot
}

func (db *DB) startCommitCombiner() {
	if db == nil || db.readOnly {
		return
	}
	db.combineMu.Lock()
	defer db.combineMu.Unlock()
	if db.combineReqCh != nil {
		return
	}
	db.combineReqCh = make(chan *commitCombineReq, commitCombineQueueCap)
	db.combineStopCh = make(chan struct{})
	db.combineDoneCh = make(chan struct{})
	reqCh := db.combineReqCh
	stopCh := db.combineStopCh
	doneCh := db.combineDoneCh
	go db.commitCombinerLoop(reqCh, stopCh, doneCh)
}

func (db *DB) stopCommitCombiner() {
	if db == nil {
		return
	}
	db.combineMu.Lock()
	stopCh := db.combineStopCh
	doneCh := db.combineDoneCh
	db.combineReqCh = nil
	db.combineStopCh = nil
	db.combineDoneCh = nil
	db.combineMu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}
}

func (db *DB) writeViaCommitCombiner(key, value []byte, del, sync bool) (bool, error) {
	return db.writeViaCommitCombinerWithMVCCInput(key, value, del, sync, mvccadmission.Input{})
}
func (db *DB) writeViaCommitCombinerWithMVCCInput(key, value []byte, del, sync bool, input mvccadmission.Input) (bool, error) {
	return db.writeViaCommitCombinerWithM0Observation(key, value, del, sync, input, nil)
}

func (db *DB) writeViaCommitCombinerWithM0Observation(key, value []byte, del, sync bool, input mvccadmission.Input, observation *m0CombinerSlot) (bool, error) {
	if db == nil {
		return true, errors.New("missing db")
	}
	if db.readOnly {
		return true, ErrReadOnly
	}
	if err := db.publicationPoisonedError(); err != nil {
		return true, err
	}

	db.combineMu.RLock()
	reqCh := db.combineReqCh
	stopCh := db.combineStopCh
	if observation != nil {
		// Borrow this exact original worker lifetime under the original lock.
		observation.borrowDone(db.combineDoneCh)
	}
	db.combineMu.RUnlock()
	if reqCh == nil || stopCh == nil {
		observation.enqueueOutcome("NO_CHANNEL")
		return false, nil
	}

	// Fast saturation probe avoids copy/alloc overhead when the queue is already
	// full and we can immediately fall back to the direct write path.
	if cap(reqCh) > 0 && len(reqCh) == cap(reqCh) {
		select {
		case <-stopCh:
			observation.enqueueOutcome("SATURATED_STOP")
			return true, errCommitCombinerClosed
		default:
			observation.enqueueOutcome("SATURATED_FALLBACK")
			return false, nil
		}
	}

	req := &commitCombineReq{
		input:  input,
		key:    append([]byte(nil), key...),
		del:    del,
		sync:   sync,
		result: make(chan error, 1),
		m0:     observation,
	}
	req.input = input.BindOwnedKey(key, req.key)
	if !del {
		req.value = append([]byte(nil), value...)
	}

	observation.enqueueBefore()
	select {
	case reqCh <- req:
		observation.enqueueOutcome("ENQUEUED")
	case <-stopCh:
		observation.enqueueOutcome("SEND_STOP")
		return true, errCommitCombinerClosed
	default:
		// Queue saturation fallback: let caller use direct write path.
		observation.enqueueOutcome("SEND_FALLBACK")
		return false, nil
	}

	observation.selectBefore()
	select {
	case err := <-req.result:
		observation.selected("RESULT")
		return true, err
	case <-stopCh:
		observation.selected("STOP")
		return true, errCommitCombinerClosed
	}
}

func (db *DB) writeSingleKV(key, value []byte, del, sync bool) error {
	return db.writeSingleKVWithMVCCInput(key, value, del, sync, mvccadmission.Input{})
}
func (db *DB) writeSingleKVWithMVCCInput(key, value []byte, del, sync bool, input mvccadmission.Input) error {
	b := db.NewBatch().(*Batch)
	var err error
	if del {
		err = b.batch.Delete(key)
	} else {
		err = b.batch.SetWithMVCCInput(key, value, input)
	}
	if err == nil {
		if sync {
			err = b.WriteSync()
		} else {
			err = b.Write()
		}
	}
	if closeErr := b.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (db *DB) applyCombinedBatch(batch []*commitCombineReq) error {
	if len(batch) == 0 {
		return nil
	}
	m0StampBatch(batch, "APPLY_BEFORE")
	defer m0StampBatch(batch, "APPLY_AFTER")
	anySync := false
	b := db.newBatchWithEntryReserve(len(batch)).(*Batch)
	for _, req := range batch {
		if req == nil {
			continue
		}
		var err error
		if req.del {
			err = b.DeleteView(req.key)
		} else {
			// request key/value slices are combiner-owned copies, safe for SetView.
			err = b.batch.SetViewWithMVCCInput(req.key, req.value, req.input)
		}
		if err != nil {
			m0StampBatch(batch, "CLOSE_BEFORE")
			closeErr := b.Close() // Original setup-error Close remains ignored.
			m0StampBatch(batch, "CLOSE_AFTER")
			m0BatchErrors(batch, "SETUP", err, closeErr, true)
			return err
		}
		if req.sync {
			anySync = true
		}
	}
	var err error
	m0StampBatch(batch, "WRITE_BEFORE")
	if anySync {
		err = b.WriteSync()
	} else {
		err = b.Write()
	}
	m0StampBatch(batch, "WRITE_AFTER")
	m0StampBatch(batch, "CLOSE_BEFORE")
	closeErr := b.Close()
	m0StampBatch(batch, "CLOSE_AFTER")
	kind := "WRITE"
	if anySync {
		kind = "WRITE_SYNC"
	}
	m0BatchErrors(batch, kind, err, closeErr, false)
	if err == nil {
		err = closeErr
	}
	return err
}

func (db *DB) finishCombined(batch []*commitCombineReq, err error) {
	for i, req := range batch {
		if req == nil {
			continue
		}
		req.m0.sendBefore("APPLIED")
		req.result <- err
		req.m0.sendAfter()
		// Application and result delivery are the final consumers. Do not retain
		// request-owned keys/values in the loop's reusable batch storage.
		batch[i] = nil
	}
}

func (db *DB) drainCombined(reqCh <-chan *commitCombineReq, err error) {
	for {
		select {
		case req := <-reqCh:
			if req == nil {
				return
			}
			req.m0.dequeued()
			req.m0.sendBefore("DRAINED")
			req.result <- err
			req.m0.sendAfter()
		default:
			return
		}
	}
}

// collectCombinedBatch harvests only work already queued after the first request.
// The existing batch cap bounds collection even with continuously ready producers.
// It never waits for a speculative arrival.
func collectCombinedBatch(batch []*commitCombineReq, reqCh <-chan *commitCombineReq, stopCh <-chan struct{}) ([]*commitCombineReq, bool) {
	for len(batch) < commitCombineMaxBatch {
		select {
		case <-stopCh:
			return batch, true
		case req := <-reqCh:
			if req != nil {
				req.m0.dequeued()
				batch = append(batch, req)
			}
		default:
			return batch, false
		}
	}
	return batch, false
}

func (db *DB) commitCombinerLoop(reqCh <-chan *commitCombineReq, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	var scratch [commitCombineMaxBatch]*commitCombineReq
	var m0BatchSerial uint64 // Opaque local ordinal, not a commit/LSN identity.
	for {
		select {
		case <-stopCh:
			db.drainCombined(reqCh, errCommitCombinerClosed)
			return
		case first := <-reqCh:
			if first == nil {
				continue
			}
			first.m0.dequeued()
			batch := scratch[:1]
			batch[0] = first
			batch, stopped := collectCombinedBatch(batch, reqCh, stopCh)
			m0BatchSerial++
			m0BindBatch(batch, m0BatchSerial)
			err := db.applyCombinedBatch(batch)
			db.finishCombined(batch, err)
			if stopped {
				db.drainCombined(reqCh, errCommitCombinerClosed)
				return
			}
		}
	}
}
