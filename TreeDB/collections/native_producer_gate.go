package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// Producer maintenance runs outside the shared schema/mutation gate. Recheck
// pressure after acquiring it so concurrent writers and DDL use one current cut
// without retaining a reservation while waiting for the gate.
func (c *Collection) prepareNativeProducerMutation() (func(), bool, error) {
	prepared, err := c.nativeJSON.db.PrepareNativeProducerObservation()
	if err != nil {
		return nil, false, err
	}
	for {
		unlock := c.lockVectorIndexMutation()
		if c.manager.isClosing() {
			unlock()
			return nil, false, backenddb.ErrClosed
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
