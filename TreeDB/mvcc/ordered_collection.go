package mvcc

import "time"

// CollectionDeadline returns the engine-established monotonic boundary for
// this caller-owned aggregate. An inactive builder returns zero. It neither
// assigns a command nor promises durability. A producer may submit earlier
// for idle work, predecessor completion, bounds or shutdown.
func (b *CommitBuilder) CollectionDeadline() time.Time {
	if b == nil {
		return time.Time{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store == nil {
		return time.Time{}
	}
	return b.engine.CollectionDeadline()
}
