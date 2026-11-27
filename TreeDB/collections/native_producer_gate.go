package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

type nativeProducerLock struct {
	coord     *collectionSchemaCoordinator
	exclusive bool
}

func (l nativeProducerLock) unlock() {
	if l.coord == nil {
		return
	}
	if l.exclusive {
		l.coord.vectorDDLMutationMu.Unlock()
	} else {
		l.coord.vectorDDLMutationMu.RUnlock()
	}
}

// Producer maintenance runs outside the shared schema/mutation gate. Recheck
// pressure after acquiring it so concurrent writers and DDL use one current cut
// without retaining a reservation while waiting for the gate.
func (c *Collection) prepareNativeProducerMutation() (func(), bool, error) {
	lock, ok, err := c.prepareNativeProducerMutationLock()
	if err != nil {
		return nil, ok, err
	}
	if lock.exclusive {
		return lock.coord.vectorDDLMutationMu.Unlock, ok, nil
	}
	return lock.coord.vectorDDLMutationMu.RUnlock, ok, nil
}

func (c *Collection) prepareNativeProducerMutationLock() (nativeProducerLock, bool, error) {
	prepared, err := c.nativeJSON.db.PrepareNativeProducerObservation()
	if err != nil {
		return nativeProducerLock{}, false, err
	}
	for {
		lock, err := c.lockNativeProducerMutationLock()
		if err != nil {
			return nativeProducerLock{}, false, err
		}
		if c.manager.isClosing() {
			lock.unlock()
			return nativeProducerLock{}, false, backenddb.ErrClosed
		}
		fresh, err := c.nativeJSON.db.ObserveNativeProducer()
		if err != nil {
			lock.unlock()
			return nativeProducerLock{}, false, err
		}
		if !fresh.UnderPressure || fresh == prepared {
			return lock, true, nil
		}
		lock.unlock()
		prepared, err = c.nativeJSON.db.PrepareNativeProducerObservation()
		if err != nil {
			return nativeProducerLock{}, false, err
		}
	}
}

// Scalar transactions must be able to reach the same durable group together.
// Inspect the current schema while holding shared admission, so a concurrent
// vector declaration cannot pass between the scalar check and transaction
// opening. Handle metadata and the coordinator's load hints can be stale.
func (c *Collection) lockNativeProducerMutation() (func(), error) {
	lock, err := c.lockNativeProducerMutationLock()
	if err != nil {
		return nil, err
	}
	if lock.exclusive {
		return lock.coord.vectorDDLMutationMu.Unlock, nil
	}
	return lock.coord.vectorDDLMutationMu.RUnlock, nil
}

func (c *Collection) lockNativeProducerMutationLock() (nativeProducerLock, error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nativeProducerLock{}, backenddb.ErrClosed
	}
	coord.vectorDDLMutationMu.RLock()
	if cut, err := c.nativeJSON.db.BorrowNativeSemanticCut(); err == nil {
		if engine, ok := cut.State().(*nativeEngineVersion); ok && engine != nil && engine.inventory == nil {
			if colState, found := engine.names.Get(c.name); found && colState != nil && colState.schema != nil && colState.schema.meta != nil {
				exclusive := len(colState.schema.meta.VectorIndexes) != 0 || coord.hasPartitionLiveCarrier()
				if releaseErr := cut.Release(); releaseErr != nil {
					coord.vectorDDLMutationMu.RUnlock()
					return nativeProducerLock{}, releaseErr
				}
				if !exclusive {
					return nativeProducerLock{coord: coord}, nil
				}
				coord.vectorDDLMutationMu.RUnlock()
				coord.vectorDDLMutationMu.Lock()
				return nativeProducerLock{coord: coord, exclusive: true}, nil
			}
		}
		if releaseErr := cut.Release(); releaseErr != nil {
			coord.vectorDDLMutationMu.RUnlock()
			return nativeProducerLock{}, releaseErr
		}
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		coord.vectorDDLMutationMu.RUnlock()
		return nativeProducerLock{}, err
	}
	exclusive := len(read.meta.VectorIndexes) != 0 || coord.hasPartitionLiveCarrier()
	err = read.Close()
	if err != nil {
		coord.vectorDDLMutationMu.RUnlock()
		return nativeProducerLock{}, err
	}
	if !exclusive {
		return nativeProducerLock{coord: coord}, nil
	}
	// Never upgrade an RWMutex while retaining its read lock. DDL may change
	// the schema in this gap; exclusive admission remains safe for either cut,
	// and the caller rechecks pressure before opening its original transaction.
	coord.vectorDDLMutationMu.RUnlock()
	coord.vectorDDLMutationMu.Lock()
	return nativeProducerLock{coord: coord, exclusive: true}, nil
}
