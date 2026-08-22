package embedding

import (
	"fmt"
	"sync"
)

// Factory builds an Embedder from validated config. Factories must fail
// closed on any config they cannot honor exactly.
type Factory func(Config) (Embedder, error)

// Registry maps provider names to factories. A zero-value Registry is not
// usable; construct with NewRegistry.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register installs factory under name. Empty names, nil factories, and
// duplicate names fail closed.
func (r *Registry) Register(name string, factory Factory) error {
	if name == "" {
		return fmt.Errorf("embedding: provider name must be non-empty")
	}
	if factory == nil {
		return fmt.Errorf("embedding: provider %q factory must be non-nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.factories[name]; ok {
		return fmt.Errorf("embedding: provider %q: %w", name, ErrProviderAlreadyRegistered)
	}
	r.factories[name] = factory
	return nil
}

// Create validates cfg and resolves its provider into an Embedder. Unknown
// providers fail with ErrUnknownProvider wrapped alongside the requested name.
func (r *Registry) Create(cfg Config) (Embedder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	factory, ok := r.factories[cfg.Provider]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("embedding: provider %q: %w", cfg.Provider, ErrUnknownProvider)
	}
	emb, err := factory(cfg)
	if err != nil {
		return nil, fmt.Errorf("embedding: provider %q create: %w", cfg.Provider, err)
	}
	if emb.Dimensions() != cfg.Dimensions {
		return nil, fmt.Errorf("embedding: provider %q builds %d dims, config declares %d: %w",
			cfg.Provider, emb.Dimensions(), cfg.Dimensions, ErrDimensionMismatch)
	}
	return emb, nil
}

var (
	defaultOnce sync.Once
	defaultReg  *Registry
)

// DefaultRegistry returns the process-wide registry with the deterministic
// reference "hashing" provider pre-registered. Additional real-model providers
// (future work) register here or in caller-owned registries.
func DefaultRegistry() *Registry {
	defaultOnce.Do(func() {
		defaultReg = NewRegistry()
		if err := defaultReg.Register(ProviderHashing, NewHashingEmbedder); err != nil {
			panic(fmt.Sprintf("embedding: register built-in hashing provider: %v", err))
		}
	})
	return defaultReg
}
