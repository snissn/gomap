package rootpublication

import (
	"context"
	"sync/atomic"
	"testing"
)

type testCapturedMutableResource struct {
	flushes atomic.Uint64
	syncs   atomic.Uint64
}

func (*testCapturedMutableResource) StableIdentity() string       { return "value-log:00000001" }
func (*testCapturedMutableResource) StableGeneration() uint64     { return 1 }
func (*testCapturedMutableResource) StableDiagnosticPath() string { return "replacement-prone.log" }
func (*testCapturedMutableResource) Frontier() uint64             { return 64 }
func (p *testCapturedMutableResource) FlushThrough(context.Context, uint64) error {
	p.flushes.Add(1)
	return nil
}
func (p *testCapturedMutableResource) SyncThrough(context.Context, uint64) error {
	p.syncs.Add(1)
	return nil
}

func TestNewMutableAppendToken_UsesCapturedProviderAndNoNamespaceForAppend(t *testing.T) {
	provider := &testCapturedMutableResource{}
	var releases atomic.Uint64
	token, err := NewMutableAppendToken(StableResourceValueLog, "value_vlog", "system_root.value_log", provider, StableNamespaceToken{}, func() {
		releases.Add(1)
	})
	if err != nil {
		t.Fatalf("NewMutableAppendToken: %v", err)
	}
	set, err := NewStableResourceSet([]StableResourceToken{token})
	if err != nil {
		t.Fatalf("NewStableResourceSet: %v", err)
	}
	if err := set.FlushAndSync(context.Background()); err != nil {
		t.Fatalf("FlushAndSync: %v", err)
	}
	if provider.flushes.Load() != 1 || provider.syncs.Load() != 1 {
		t.Fatalf("provider calls flush=%d sync=%d want 1/1", provider.flushes.Load(), provider.syncs.Load())
	}
	set.Release()
	set.Release()
	if releases.Load() != 1 {
		t.Fatalf("releases=%d want 1", releases.Load())
	}
}
