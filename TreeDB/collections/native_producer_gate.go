package collections

import backenddb "github.com/snissn/gomap/TreeDB/db"

// Producer maintenance completes before a native transaction opens. Modern
// vector assets are immutable roots and need no process-local runtime gate.
func (c *Collection) prepareNativeProducerMutation() (func(), bool, error) {
	if err := c.nativeJSON.db.PrepareNativeProducer(); err != nil {
		return nil, false, err
	}
	if c.manager.isClosing() {
		return nil, false, backenddb.ErrClosed
	}
	return func() {}, false, nil
}
