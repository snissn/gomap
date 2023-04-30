package gomap

import (
	"fmt"
	"os"
	"runtime"
	"sync"
)

type HashmapDistributed struct {
	maps    []*Hashmap
	mutexes []sync.RWMutex
}

func (h *HashmapDistributed) New(folder string) error {
	// Get the number of CPUs
	numCPU := runtime.NumCPU()

	// Initialize the slice of Hashmap pointers and mutexes
	h.maps = make([]*Hashmap, numCPU)
	h.mutexes = make([]sync.RWMutex, numCPU)

	// Create a new Hashmap for each CPU
	for i := 0; i < numCPU; i++ {
		partitionFolder := fmt.Sprintf("%s/partition-%d", folder, i)
		err := os.MkdirAll(partitionFolder, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory for partition: %w", err)
		}

		h.maps[i] = &Hashmap{}
		h.maps[i].New(partitionFolder)
	}

	return nil
}

func (h *HashmapDistributed) Get(key []byte) ([]byte, error) {
	hash := hash(key)
	mapIndex := hash % Hash(len(h.maps))
	h.mutexes[mapIndex].RLock()         // lock for reading
	defer h.mutexes[mapIndex].RUnlock() // unlock after reading
	return h.maps[mapIndex].Get(key)
}

func (h *HashmapDistributed) Add(key []byte, value []byte) error {
	hash := hash(key)
	mapIndex := hash % Hash(len(h.maps))
	h.mutexes[mapIndex].Lock()         // lock for writing
	defer h.mutexes[mapIndex].Unlock() // unlock after writing
	h.maps[mapIndex].Add(key, value)
	return nil
}
