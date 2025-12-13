package gomap

import (
	"sync"
	"time"
)

// KVStore is the minimal interface implemented by backends used here.
type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

// cacheEntry stores a pending write or delete.
type cacheEntry struct {
	value []byte
	del   bool
}

// CacheKV buffers writes and flushes them to the underlying store.
// WARNING: no WAL; pending writes are lost on crash before flush.
type CacheKV struct {
	backend KVStore

	mu         sync.RWMutex
	pending    map[string]cacheEntry
	flushing   map[string]cacheEntry // Items currently being flushed (visible to Get)
	pendingLen int

	flushMu sync.Mutex // Serializes Flush operations to backend

	maxEntries int
	maxBytes   int

	flushInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewCacheKV wraps a KVStore with a write-back cache.
// flushInterval <=0 disables timer flushes.
func NewCacheKV(backend KVStore, maxEntries, maxBytes int, flushInterval time.Duration) *CacheKV {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20 // 1MB
	}
	c := &CacheKV{
		backend:       backend,
		pending:       make(map[string]cacheEntry),
		maxEntries:    maxEntries,
		maxBytes:      maxBytes,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}
	if flushInterval > 0 {
		c.wg.Add(1)
		go c.flushLoop()
	}
	return c
}

func (c *CacheKV) Get(key []byte) ([]byte, error) {
	k := string(key)
	c.mu.RLock()
	if e, ok := c.pending[k]; ok {
		c.mu.RUnlock()
		if e.del {
			return nil, nil
		}
		return e.value, nil
	}
	if e, ok := c.flushing[k]; ok {
		c.mu.RUnlock()
		if e.del {
			return nil, nil
		}
		return e.value, nil
	}
	c.mu.RUnlock()
	return c.backend.Get(key)
}

func (c *CacheKV) Put(key, value []byte) error {
	k := string(key)
	c.mu.Lock()
	c.pending[k] = cacheEntry{value: append([]byte(nil), value...)}
	c.pendingLen += len(k) + len(value)
	shouldFlush := len(c.pending) >= c.maxEntries || c.pendingLen >= c.maxBytes
	c.mu.Unlock()
	if shouldFlush {
		return c.Flush()
	}
	return nil
}

func (c *CacheKV) Delete(key []byte) error {
	k := string(key)
	c.mu.Lock()
	c.pending[k] = cacheEntry{del: true}
	c.pendingLen += len(k)
	shouldFlush := len(c.pending) >= c.maxEntries || c.pendingLen >= c.maxBytes
	c.mu.Unlock()
	if shouldFlush {
		return c.Flush()
	}
	return nil
}

// Flush writes pending changes to the backend. No WAL; pending data is volatile until flushed.
func (c *CacheKV) Flush() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return nil
	}
	entries := c.pending
	c.pending = make(map[string]cacheEntry)
	c.flushing = entries
	c.pendingLen = 0
	c.mu.Unlock()

	// Ensure flushing is cleared even if write fails
	defer func() {
		c.mu.Lock()
		c.flushing = nil
		c.mu.Unlock()
	}()

	var batchKeys [][]byte
	var batchVals [][]byte
	for k, e := range entries {
		if e.del {
			if err := c.backend.Delete([]byte(k)); err != nil {
				return err
			}
			continue
		}
		batchKeys = append(batchKeys, []byte(k))
		batchVals = append(batchVals, e.value)
	}

	if len(batchKeys) > 0 {
		if b, ok := c.backend.(interface {
			PutMany([][]byte, [][]byte) error
		}); ok {
			return b.PutMany(batchKeys, batchVals)
		}
		for i := range batchKeys {
			if err := c.backend.Put(batchKeys[i], batchVals[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close stops the flush loop and flushes remaining writes.
func (c *CacheKV) Close() error {
	if c.flushInterval > 0 {
		close(c.stopCh)
		c.wg.Wait()
	}
	return c.Flush()
}

func (c *CacheKV) flushLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = c.Flush()
		case <-c.stopCh:
			return
		}
	}
}
