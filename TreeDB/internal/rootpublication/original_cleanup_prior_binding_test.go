package rootpublication

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestOriginalCleanupPriorBindingTransfersOneOriginalEdge(t *testing.T) {
	for _, useRW := range []bool{false, true} {
		var mutex sync.Mutex
		var rwMutex sync.RWMutex
		var prior error
		binding := BindOriginalCleanupPrior(&mutex, &prior)
		if useRW {
			binding = BindOriginalCleanupPriorRW(&rwMutex, &prior)
		}
		originals := make([]error, 16)
		var linked atomic.Int32
		var wait sync.WaitGroup
		for index := range originals {
			originals[index] = errors.New("same original contender")
			wait.Add(1)
			go func(original error) {
				defer wait.Done()
				if binding.TryLink(original) {
					linked.Add(1)
				}
			}(originals[index])
		}
		wait.Wait()
		if linked.Load() != 1 || binding.Available() {
			t.Fatalf("rw=%v linked=%d available=%v", useRW, linked.Load(), binding.Available())
		}
		found := false
		for _, original := range originals {
			found = found || prior == original
		}
		if !found {
			t.Fatal("binding replaced the original error")
		}
	}
}
