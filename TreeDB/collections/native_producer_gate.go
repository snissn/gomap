package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// prepareNativeProducerMutation performs maintenance off the existing collection
// gate, then revalidates its advisory observation before opening any transaction
// or invoking an application body. The returned unlock has the original scope.
func (c *Collection) prepareNativeProducerMutation() (func(), bool, error) {
	prepared, err := c.nativeJSON.db.PrepareNativeProducerObservation()
	if err != nil {
		return nil, false, err
	}
	return c.lockNativeProducerAfterPreparation(prepared)
}

// This phase takes only a value identity: no cut or admission receipt crosses
// the wait for the collection gate. Changed pressure is drained off-gate; an
// unchanged pressured result proceeds to the original finite admission/refusal
// so held readers cannot turn this into a no-progress maintenance loop.
func (c *Collection) lockNativeProducerAfterPreparation(prepared backenddb.NativeProducerObservation) (func(), bool, error) {
	for {
		unlock, held := c.lockNativeHNSWMutationIfNeeded()
		if c.manager.isClosing() {
			unlock()
			return nil, false, backenddb.ErrClosed
		}
		if !held {
			return unlock, false, nil
		}
		fresh, err := c.nativeJSON.db.ObserveNativeProducer()
		if err != nil {
			unlock()
			return nil, false, err
		}
		if !fresh.UnderPressure || fresh == prepared {
			return unlock, true, nil
		}
		unlock()
		prepared, err = c.nativeJSON.db.PrepareNativeProducerObservation()
		if err != nil {
			return nil, false, err
		}
	}
}
