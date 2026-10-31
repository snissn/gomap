package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// Producer maintenance runs outside the shared schema/mutation gate. Recheck
// pressure after acquiring it so concurrent writers and DDL use one current cut
// without retaining a reservation while waiting for the gate. At most one
// changed-observation maintenance recheck follows the initial preparation;
// concurrent suffixes cannot turn advisory scheduling into an unbounded loop.
func (c *Collection) prepareNativeProducerMutation() (func(), bool, error) {
	prepared, err := c.nativeJSON.db.PrepareNativeProducerObservation()
	if err != nil {
		return nil, false, err
	}
	for rechecks := 0; ; rechecks++ {
		unlock, err := c.lockNativeProducerMutation()
		if err != nil {
			return nil, false, err
		}
		if c.manager.isClosing() {
			unlock()
			return nil, false, backenddb.ErrClosed
		}
		fresh, err := c.nativeJSON.db.ObserveNativeProducer()
		if err != nil {
			unlock()
			return nil, false, err
		}
		if !fresh.UnderPressure || fresh == prepared || rechecks == 1 {
			return unlock, true, nil
		}
		unlock()
		prepared, err = c.nativeJSON.db.PrepareNativeProducerObservation()
		if err != nil {
			return nil, false, err
		}
	}
}

// Scalar transactions must be able to reach the same durable group together.
// Inspect the current schema while holding shared admission, so a concurrent
// vector declaration cannot pass between the scalar check and transaction
// opening. Handle metadata and the coordinator's load hints can be stale.
func (c *Collection) lockNativeProducerMutation() (func(), error) {
	coord := c.collectionSchemaCoordinator()
	if coord == nil {
		return nil, backenddb.ErrClosed
	}
	coord.vectorDDLMutationMu.RLock()
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		coord.vectorDDLMutationMu.RUnlock()
		return nil, err
	}
	exclusive := len(read.meta.VectorIndexes) != 0 || coord.hasPartitionLiveCarrier()
	err = read.Close()
	if err != nil {
		coord.vectorDDLMutationMu.RUnlock()
		return nil, err
	}
	if !exclusive {
		return coord.vectorDDLMutationMu.RUnlock, nil
	}
	// Never upgrade an RWMutex while retaining its read lock. DDL may change
	// the schema in this gap; exclusive admission remains safe for either cut,
	// and the caller rechecks pressure before opening its original transaction.
	coord.vectorDDLMutationMu.RUnlock()
	coord.vectorDDLMutationMu.Lock()
	return coord.vectorDDLMutationMu.Unlock, nil
}
