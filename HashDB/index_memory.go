package hashdb

import "fmt"

// IndexMemoryPolicy configures memory pinning/advice for the on-disk hash index.
//
// Controls (SwissHash control bytes) are small and benefit from being pinned
// to RAM. Keys are much larger and are treated as best-effort OS hints.
type IndexMemoryPolicy struct {
	LockControls       bool
	LockControlsStrict bool

	AdviseKeysWillNeed bool
	AdviseKeysRandom   bool
}

// DefaultIndexMemoryPolicy is the default memory pinning/advice configuration.
var DefaultIndexMemoryPolicy = IndexMemoryPolicy{
	LockControls:       true,
	LockControlsStrict: false,
	AdviseKeysWillNeed: true,
	AdviseKeysRandom:   true,
}

// SetIndexMemoryPolicy overrides the default policy. Call before Open.
func (h *DB) SetIndexMemoryPolicy(policy IndexMemoryPolicy) {
	h.indexMemoryPolicy = policy
	h.indexMemoryPolicySet = true
}

func (h *DB) indexMemoryPolicyOrDefault() IndexMemoryPolicy {
	if !h.indexMemoryPolicySet {
		h.indexMemoryPolicy = DefaultIndexMemoryPolicy
		h.indexMemoryPolicySet = true
	}
	return h.indexMemoryPolicy
}

func (h *DB) applyIndexMemoryPolicy(controls []byte, keys []byte) error {
	policy := h.indexMemoryPolicyOrDefault()

	// ---------------------------------------------------------
	// 1. The Swiss Hash (Control Bytes) -> "Really Mean It"
	// ---------------------------------------------------------
	if policy.LockControls {
		if err := lockBytes(controls); err != nil {
			if policy.LockControlsStrict {
				return fmt.Errorf("failed to hard-pin control bytes (check memlock/ulimit -l): %w", err)
			}
		} else {
			h.controlsLocked = true
		}
	}

	// ---------------------------------------------------------
	// 2. The Key Array (Data) -> "Hope / Hint"
	// ---------------------------------------------------------
	if policy.AdviseKeysWillNeed {
		_ = adviseWillNeed(keys)
	}
	if policy.AdviseKeysRandom {
		_ = adviseRandom(keys)
	}

	return nil
}

func (h *DB) unlockControlsIfNeeded(controls []byte) {
	if !h.controlsLocked {
		return
	}
	_ = unlockBytes(controls)
	h.controlsLocked = false
}
