package db

import (
	"errors"
	"time"
)

const (
	commitCombineQueueCap = 4096
	commitCombineMaxBatch = 64
	commitCombineLinger   = 50 * time.Microsecond
)

var errCommitCombinerClosed = errors.New("treedb: commit combiner closed")

type commitCombineReq struct {
	key    []byte
	value  []byte
	del    bool
	sync   bool
	result chan error
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
	db.combineMu.RUnlock()
	if reqCh == nil || stopCh == nil {
		return false, nil
	}

	// Fast saturation probe avoids copy/alloc overhead when the queue is already
	// full and we can immediately fall back to the direct write path.
	if cap(reqCh) > 0 && len(reqCh) == cap(reqCh) {
		select {
		case <-stopCh:
			return true, errCommitCombinerClosed
		default:
			return false, nil
		}
	}

	req := &commitCombineReq{
		key:    append([]byte(nil), key...),
		del:    del,
		sync:   sync,
		result: make(chan error, 1),
	}
	if !del {
		req.value = append([]byte(nil), value...)
	}

	select {
	case reqCh <- req:
	case <-stopCh:
		return true, errCommitCombinerClosed
	default:
		// Queue saturation fallback: let caller use direct write path.
		return false, nil
	}

	select {
	case err := <-req.result:
		return true, err
	case <-stopCh:
		return true, errCommitCombinerClosed
	}
}

func (db *DB) writeSingleKV(key, value []byte, del, sync bool) error {
	b := db.NewBatch().(*Batch)
	var err error
	if del {
		err = b.batch.Delete(key)
	} else {
		err = b.batch.Set(key, value)
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
			err = b.SetView(req.key, req.value)
		}
		if err != nil {
			_ = b.Close()
			return err
		}
		if req.sync {
			anySync = true
		}
	}
	var err error
	if anySync {
		err = b.WriteSync()
	} else {
		err = b.Write()
	}
	if closeErr := b.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (db *DB) finishCombined(batch []*commitCombineReq, err error) {
	for _, req := range batch {
		if req == nil {
			continue
		}
		req.result <- err
	}
}

func (db *DB) drainCombined(reqCh <-chan *commitCombineReq, err error) {
	for {
		select {
		case req := <-reqCh:
			if req == nil {
				return
			}
			req.result <- err
		default:
			return
		}
	}
}

func (db *DB) commitCombinerLoop(reqCh <-chan *commitCombineReq, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	defer close(doneCh)
	for {
		select {
		case <-stopCh:
			db.drainCombined(reqCh, errCommitCombinerClosed)
			return
		case first := <-reqCh:
			if first == nil {
				continue
			}
			batch := make([]*commitCombineReq, 0, commitCombineMaxBatch)
			batch = append(batch, first)
			deadline := time.Now().Add(commitCombineLinger)
			timer := time.NewTimer(commitCombineLinger)
			for len(batch) < commitCombineMaxBatch {
				wait := time.Until(deadline)
				if wait <= 0 {
					break
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(wait)
				select {
				case req := <-reqCh:
					if req != nil {
						batch = append(batch, req)
					}
				case <-timer.C:
					// Linger elapsed.
				case <-stopCh:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					err := db.applyCombinedBatch(batch)
					db.finishCombined(batch, err)
					db.drainCombined(reqCh, errCommitCombinerClosed)
					return
				}
				if time.Now().After(deadline) {
					break
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			err := db.applyCombinedBatch(batch)
			db.finishCombined(batch, err)
		}
	}
}
