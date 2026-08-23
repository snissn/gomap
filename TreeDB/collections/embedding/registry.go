package embedding

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// Factory builds an Embedder from validated config. Factories must fail
// closed on any config they cannot honor exactly.
type Factory func(Config) (Embedder, error)

// Registry maps provider names to factories. A zero-value Registry is not
// usable; construct with NewRegistry.
type Registry struct {
	mu            sync.RWMutex
	factories     map[string]Factory
	providerLocks map[string]chan struct{}
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		factories:     make(map[string]Factory),
		providerLocks: make(map[string]chan struct{}),
	}
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
	token := make(chan struct{}, 1)
	token <- struct{}{}
	r.providerLocks[name] = token
	return nil
}

// Create validates cfg and resolves its provider with a background context.
func (r *Registry) Create(cfg Config) (Embedder, error) {
	return r.CreateContext(context.Background(), cfg)
}

// CreateContext resolves a provider while honoring cancellation during any
// wait for that provider's serialized factory/dimension boundary.
func (r *Registry) CreateContext(ctx context.Context, cfg Config) (Embedder, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	factory, ok := r.factories[cfg.Provider]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("embedding: provider %q: %w", cfg.Provider, ErrUnknownProvider)
	}
	unlockProvider, err := r.LockProvider(ctx, cfg.Provider)
	if err != nil {
		return nil, err
	}
	defer unlockProvider()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	emb, err := factory(cfg)
	if err != nil {
		return nil, fmt.Errorf("embedding: provider %q create: %w", cfg.Provider, err)
	}
	if err := ValidateEmbedder(emb); err != nil {
		return nil, fmt.Errorf("embedding: provider %q create: %w", cfg.Provider, err)
	}
	dimensions := emb.Dimensions()
	if dimensions != cfg.Dimensions {
		return nil, fmt.Errorf("embedding: provider %q builds %d dims, config declares %d: %w",
			cfg.Provider, dimensions, cfg.Dimensions, ErrDimensionMismatch)
	}
	return emb, nil
}

// ValidateEmbedder rejects nil and typed-nil provider results before any
// method invocation. It is shared by registry and collection ingest gates.
func ValidateEmbedder(emb Embedder) error {
	if emb == nil {
		return ErrInvalidEmbedder
	}
	value := reflect.ValueOf(emb)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return ErrInvalidEmbedder
		}
	}
	return nil
}

// LockProvider serializes EmbedBatch calls for one registered provider name
// across all ingestion invocations. Arbitrary providers are not required to be
// concurrency-safe; callers must release the returned lock exactly once.
func (r *Registry) LockProvider(ctx context.Context, name string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.RLock()
	token, ok := r.providerLocks[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("embedding: provider %q: %w", name, ErrUnknownProvider)
	}
	select {
	case <-token:
		if err := ctx.Err(); err != nil {
			token <- struct{}{}
			return nil, err
		}
		var once sync.Once
		return func() {
			once.Do(func() { token <- struct{}{} })
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
