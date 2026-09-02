package hashdb

// Has reports whether key exists.
//
// Note: For the sharded HashDB type, Has is implemented as Get+nil check and may
// read the value. Use TreeDB if you need ordered iteration or richer read APIs.
func (h *HashDB) Has(key []byte) (bool, error) {
	v, err := h.Get(key)
	return v != nil, err
}

// Has reports whether key exists in a single-shard DB.
func (h *DB) Has(key []byte) (bool, error) {
	v, err := h.Get(key)
	return v != nil, err
}

// Has reports whether key exists in the cached DB wrapper.
func (c *CachedDB) Has(key []byte) (bool, error) {
	v, err := c.Get(key)
	return v != nil, err
}
