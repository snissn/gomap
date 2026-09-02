package redisserver

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

type keyLocker struct {
	locks []sync.Mutex
}

func newKeyLocker(n int) *keyLocker {
	if n <= 0 {
		n = 256
	}
	return &keyLocker{locks: make([]sync.Mutex, n)}
}

func (k *keyLocker) lockKey(key []byte) func() {
	if k == nil || len(k.locks) == 0 {
		return func() {}
	}
	idx := int(xxhash.Sum64(key) % uint64(len(k.locks)))
	k.locks[idx].Lock()
	return func() { k.locks[idx].Unlock() }
}

func (k *keyLocker) lockPair(a, b []byte) func() {
	if k == nil || len(k.locks) == 0 {
		return func() {}
	}
	idxA := int(xxhash.Sum64(a) % uint64(len(k.locks)))
	idxB := int(xxhash.Sum64(b) % uint64(len(k.locks)))
	if idxA == idxB {
		k.locks[idxA].Lock()
		return func() { k.locks[idxA].Unlock() }
	}
	if idxA > idxB {
		idxA, idxB = idxB, idxA
	}
	k.locks[idxA].Lock()
	k.locks[idxB].Lock()
	return func() {
		k.locks[idxB].Unlock()
		k.locks[idxA].Unlock()
	}
}
