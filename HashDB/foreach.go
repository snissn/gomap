package hashdb

// ForEach calls fn for every live key/value pair in the DB.
// The iteration order is arbitrary.
//
// DB is not goroutine-safe; the caller must not mutate the DB concurrently.
func (h *DB) ForEach(fn func(key, value []byte) error) error {
	// For snapshot/export use-cases, complete any in-progress rehash so all keys
	// are visible in the active table.
	for h.rehashInProgress {
		if err := h.rehashStep(^uint64(0)); err != nil {
			return err
		}
	}

	for _, k := range h.keys {
		if k.slabOffset == 0 || k.slabOffset == Tombstone {
			continue
		}
		item, err := h.unmarshalItemFromSlab(k)
		if err != nil {
			return err
		}
		if err := fn(item.Key, item.Value); err != nil {
			return err
		}
	}
	return nil
}

// ForEach calls fn for every live key/value pair in the sharded store.
// The iteration order is arbitrary.
//
// ForEach takes an exclusive snapshot of the store:
// - blocks concurrent writers,
// - flushes shard write-back caches to the backend DBs,
// - and then iterates backend state.
func (h *HashDB) ForEach(fn func(key, value []byte) error) error {
	if h == nil {
		return nil
	}

	// Lock all shards to prevent concurrent mutation and to make cache flushes stable.
	for i := range h.locks {
		h.locks[i].Lock()
	}
	defer func() {
		for i := len(h.locks) - 1; i >= 0; i-- {
			h.locks[i].Unlock()
		}
	}()

	for i := range h.shards {
		if h.shards[i] == nil {
			continue
		}
		// Bring any volatile cached writes into the backend so iteration sees them.
		if err := h.shards[i].Flush(); err != nil {
			return err
		}

		h.shards[i].backendMu.Lock()
		db := h.shards[i].db
		if db != nil {
			for db.rehashInProgress {
				if err := db.rehashStep(^uint64(0)); err != nil {
					h.shards[i].backendMu.Unlock()
					return err
				}
			}
		}
		h.shards[i].backendMu.Unlock()
	}

	for i := range h.shards {
		if h.shards[i] == nil || h.shards[i].db == nil {
			continue
		}
		h.shards[i].backendMu.Lock()
		err := h.shards[i].db.ForEach(fn)
		h.shards[i].backendMu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
