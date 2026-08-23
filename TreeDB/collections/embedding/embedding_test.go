package embedding

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{Provider: "hashing", Dimensions: 16}
}

func TestConfigValidateRejectsEmptyProvider(t *testing.T) {
	cfg := Config{Provider: "", Dimensions: 16}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "provider") {
		t.Fatalf("Validate()=%v want provider error", err)
	}
}

func TestConfigValidateRejectsNonPositiveDimensions(t *testing.T) {
	for _, dims := range []int{0, -1} {
		cfg := Config{Provider: "hashing", Dimensions: dims}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "dimension") {
			t.Fatalf("Validate(dims=%d)=%v want dimension error", dims, err)
		}
	}
}

func TestRegistryCreateUnknownProviderFailsClosed(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("known", func(Config) (Embedder, error) {
		return stubEmbedder{dims: 4}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, err := reg.Create(Config{Provider: "missing", Dimensions: 4})
	if !errors.Is(err, ErrUnknownProvider) {
		t.Fatalf("Create(unknown)=%v want ErrUnknownProvider", err)
	}
}
func TestRegistryCreateRejectsNilAndTypedNilEmbedders(t *testing.T) {
	tests := []struct {
		name    string
		factory Factory
	}{
		{
			name: "nil interface",
			factory: func(Config) (Embedder, error) {
				return nil, nil
			},
		},
		{
			name: "typed nil pointer",
			factory: func(Config) (Embedder, error) {
				var emb *panicOnUseEmbedder
				return emb, nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()
			if err := reg.Register("malformed", tc.factory); err != nil {
				t.Fatalf("Register: %v", err)
			}
			emb, err := reg.Create(Config{Provider: "malformed", Dimensions: 4})
			if !errors.Is(err, ErrInvalidEmbedder) {
				t.Fatalf("Create=(%v, %v) want ErrInvalidEmbedder", emb, err)
			}
			if emb != nil {
				t.Fatalf("Create returned malformed embedder %#v", emb)
			}
		})
	}
}

func TestRegistryCreateReleasesProviderLockAfterFactoryPanic(t *testing.T) {
	reg := NewRegistry()
	var panicFactory atomic.Bool
	panicFactory.Store(true)
	if err := reg.Register("panic-once", func(Config) (Embedder, error) {
		if panicFactory.Swap(false) {
			panic("factory exploded")
		}
		return stubEmbedder{dims: 4}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	cfg := Config{Provider: "panic-once", Dimensions: 4}
	if _, err := reg.Create(cfg); !errors.Is(err, ErrInvalidEmbedder) {
		t.Fatalf("panic Create err=%v want ErrInvalidEmbedder", err)
	}
	emb, err := reg.Create(cfg)
	if err != nil || emb == nil {
		t.Fatalf("Create after panic=(%v, %v), provider lock was not released", emb, err)
	}
}

func TestRegistryCreatePreservesFactoryCauseAndProviderContext(t *testing.T) {
	cause := errors.New("provider credentials unavailable")
	reg := NewRegistry()
	if err := reg.Register("remote", func(Config) (Embedder, error) {
		return nil, cause
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	emb, err := reg.Create(Config{Provider: "remote", Dimensions: 4})
	if !errors.Is(err, cause) {
		t.Fatalf("Create=(%v, %v) does not preserve factory cause", emb, err)
	}
	if !strings.Contains(err.Error(), `"remote"`) {
		t.Fatalf("Create error %q does not name provider", err)
	}
}

func TestRegistryCreateRejectsFactoryDimensionMismatch(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("wrong-width", func(Config) (Embedder, error) {
		return stubEmbedder{dims: 3}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	emb, err := reg.Create(Config{Provider: "wrong-width", Dimensions: 4})
	if !errors.Is(err, ErrDimensionMismatch) {
		t.Fatalf("Create=(%v, %v) want ErrDimensionMismatch", emb, err)
	}
	if !strings.Contains(err.Error(), `"wrong-width"`) {
		t.Fatalf("Create error %q does not name provider", err)
	}
}

func TestRegistryCreateContextHonorsProviderLockCancellation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register("blocked", func(Config) (Embedder, error) {
		return stubEmbedder{dims: 4}, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	unlock, err := reg.LockProvider(context.Background(), "blocked")
	if err != nil {
		t.Fatalf("LockProvider: %v", err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = reg.CreateContext(ctx, Config{Provider: "blocked", Dimensions: 4})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CreateContext err=%v want context deadline", err)
	}
}

func TestRegistryRegisterRejectsDuplicateAndInvalid(t *testing.T) {
	reg := NewRegistry()
	factory := func(Config) (Embedder, error) { return stubEmbedder{dims: 4}, nil }
	if err := reg.Register("dup", factory); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := reg.Register("dup", factory); !errors.Is(err, ErrProviderAlreadyRegistered) {
		t.Fatalf("duplicate Register=%v want ErrProviderAlreadyRegistered", err)
	}
	if err := reg.Register("", factory); err == nil {
		t.Fatal("empty-name Register accepted")
	}
	if err := reg.Register("nilfactory", nil); err == nil {
		t.Fatal("nil-factory Register accepted")
	}
}

// TestEmbedBatchOrderPreservedAndAligned proves index alignment of the batch
// path through the shared bounded-concurrency runner: unit(i) results land at
// out[i] even when completion order is scrambled by per-item jitter.
func TestRunBatchPreservesOrderUnderConcurrency(t *testing.T) {
	const n = 256
	results := make([]int, n)
	var mu sync.Mutex
	err := RunBatch(context.Background(), n, func(_ context.Context, i int) error {
		if i > 0 && i%7 == 0 {
			time.Sleep(time.Microsecond) // scramble completion order
		}
		mu.Lock()
		results[i] = i * 3
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("RunBatch: %v", err)
	}
	for i, got := range results {
		if got != i*3 {
			t.Fatalf("results[%d]=%d want %d", i, got, i*3)
		}
	}
}

// TestRunBatchFailsWholeBatchOnErrorPosition proves fail-closed semantics:
// an error at any position fails the entire batch with a typed error naming
// the failing position; no partial success is reported.
func TestRunBatchFailsWholeBatchOnErrorPosition(t *testing.T) {
	const n = 64
	const failAt = 37
	boom := errors.New("boom at position")
	err := RunBatch(context.Background(), n, func(_ context.Context, i int) error {
		if i == failAt {
			return boom
		}
		return nil
	})
	if err == nil {
		t.Fatal("RunBatch succeeded, want whole-batch failure")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v want wrapped %v", err, boom)
	}
	if !strings.Contains(err.Error(), "37") {
		t.Fatalf("err=%v want failing position in message", err)
	}
}

func TestRunBatchNegativeCountFailsClosed(t *testing.T) {
	if err := RunBatch(context.Background(), -1, func(context.Context, int) error { return nil }); err == nil {
		t.Fatal("RunBatch(n=-1) accepted")
	}
}

// TestRunBatchCancellationStopsWorkAndLeaksNoGoroutines drives cancellation
// mid-flight: workers must observe context cancellation, stop picking up new
// units, return to the caller, and leave no goroutines behind.
func TestRunBatchCancellationStopsWorkAndLeaksNoGoroutines(t *testing.T) {
	before := runtimeNumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	var started atomic.Int64
	done := make(chan error, 1)
	go func() {
		done <- RunBatch(ctx, 100000, func(bctx context.Context, _ int) error {
			started.Add(1)
			select {
			case <-bctx.Done():
				return bctx.Err()
			case <-time.After(time.Millisecond):
				return nil
			}
		})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for started.Load() < 8 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch=%v want context.Canceled", err)
	}
	leaked := waitGoroutinesSettle(before, 5*time.Second)
	if leaked != 0 {
		t.Fatalf("%d goroutine(s) leaked after canceled batch (baseline %d)", leaked, before)
	}
}

func TestEmbedBatchEmptyBatchFailsClosed(t *testing.T) {
	emb, err := DefaultRegistry().Create(validConfig())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	vectors, err := emb.EmbedBatch(context.Background(), nil)
	if !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("EmbedBatch(nil)=(%v,%v) want ErrEmptyBatch", vectors, err)
	}
	if vectors != nil {
		t.Fatalf("EmbedBatch(nil) returned %d vectors, want none", len(vectors))
	}
	vectors, err = emb.EmbedBatch(context.Background(), [][]byte{})
	if !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("EmbedBatch(empty)=(%v,%v) want ErrEmptyBatch", vectors, err)
	}
}

type stubEmbedder struct{ dims int }

func (s stubEmbedder) Dimensions() int { return s.dims }
func (s stubEmbedder) EmbedBatch(ctx context.Context, texts [][]byte) ([][]float32, error) {
	out := make([][]float32, len(texts))
	if err := RunBatch(ctx, len(texts), func(ctx context.Context, i int) error {
		out[i] = make([]float32, s.dims)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

type panicOnUseEmbedder struct{}

func (*panicOnUseEmbedder) Dimensions() int {
	panic("Dimensions called on typed-nil embedder")
}

func (*panicOnUseEmbedder) EmbedBatch(context.Context, [][]byte) ([][]float32, error) {
	panic("EmbedBatch called on typed-nil embedder")
}
