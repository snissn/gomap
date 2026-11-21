package rootpublication

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// Limit both scheduling fan-out and the number of entries inspected for this
// optimization. The normal Linux layout has eight document value logs.
const (
	maxParallelValueLogs = 8
	valueLogSyncWorkers  = 4
)

type valueLogSyncInput struct {
	token    *StableResourceToken
	frontier DurableFrontier
}

func (set *StableResourceSet) syncIndependentValueLogsLocked() (bool, error) {
	if runtime.GOOS != "linux" {
		return false, nil
	}
	var inputs [maxParallelValueLogs]valueLogSyncInput
	count := 0
	eligible := set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		// A directly owned token without ownedOperations uses the
		// constructor's built-in sync. The owned constructor rejects
		// opaque callbacks, and imported tokens can defer to such callbacks.
		if token == nil || token.released.Load() || token.metadata == nil ||
			token.kind != ResourceValueLog || token.reachability != ReachabilityValueLogPointer ||
			token.stability != ResourceMutableAppend || token.hasSyncedFrontier ||
			token.importedSource != nil || token.ownedOperations != nil ||
			entry.frontier.RIDCount != 0 || entry.frontier.MaxLSN != 0 || count == len(inputs) {
			return false
		}
		for _, prior := range inputs[:count] {
			if sameStableObject(prior.token.identity, token.identity) {
				return false
			}
		}
		inputs[count] = valueLogSyncInput{token: token, frontier: entry.frontier}
		count++
		return true
	})
	if !eligible || count < 2 {
		return false, nil
	}

	// The caller holds the set lock until all workers finish. Each worker
	// borrows a different pinned file and syncs the coalesced entry frontier.
	// Join failures in traversal order, just as in the serial path.
	var group sync.WaitGroup
	var failures [maxParallelValueLogs]error
	workers := min(count, valueLogSyncWorkers)
	for worker := range workers {
		group.Go(func() {
			for i := worker; i < count; i += workers {
				input := inputs[i]
				if err := input.token.syncThrough(input.frontier); err != nil {
					failures[i] = fmt.Errorf("sync stable resource %+v: %w", input.token.logicalKey(), err)
				}
			}
		})
	}
	group.Wait()
	return true, errors.Join(failures[:count]...)
}
