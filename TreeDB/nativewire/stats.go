package nativewire

import "sync"

const nativeStatsPrefix = "treedb.native_wire."

// Stats is a string-valued snapshot of native-wire and TreeDB counters.
type Stats map[string]string

type counters struct {
	mu     sync.Mutex
	values map[string]uint64
}

func (c *counters) add(key string, delta uint64) {
	if c == nil || key == "" || delta == 0 {
		return
	}
	c.mu.Lock()
	if c.values == nil {
		c.values = make(map[string]uint64)
	}
	c.values[key] += delta
	c.mu.Unlock()
}

func (c *counters) inc(key string) {
	c.add(key, 1)
}

func (c *counters) snapshot() map[string]uint64 {
	out := make(map[string]uint64)
	if c == nil {
		return out
	}
	c.mu.Lock()
	for key, value := range c.values {
		out[key] = value
	}
	c.mu.Unlock()
	return out
}
