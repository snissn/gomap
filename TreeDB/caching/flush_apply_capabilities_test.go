package caching

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type reducerPublishOverrideBackend struct {
	*backenddb.DB
	value uint64
	calls int
}

func (b *reducerPublishOverrideBackend) FlushApplyReducerPublishNs() uint64 {
	b.calls++
	return b.value
}

func TestBackendFlushApplyCapabilityTracksCurrentProducer(t *testing.T) {
	var absent *DB
	if got := absent.backendFlushApplyReducerPublishNs(); got != 0 {
		t.Fatalf("nil cache=%d", got)
	}
	concrete := new(backenddb.DB)
	database := &DB{backend: concrete}
	if got := database.backendFlushApplyReducerPublishNs(); got != 0 {
		t.Fatalf("new concrete counter=%d", got)
	}
	override := &reducerPublishOverrideBackend{DB: concrete, value: 17}
	database.backend = override
	if got := database.backendFlushApplyReducerPublishNs(); got != 17 || override.calls != 1 {
		t.Fatalf("decorator override=%d calls=%d", got, override.calls)
	}
	override.value = 29
	if got := database.backendFlushApplyReducerPublishNs(); got != 29 || override.calls != 2 {
		t.Fatalf("counter snapshot cached=%d calls=%d", got, override.calls)
	}
	// An unsupported, non-comparable value adapter must neither panic on an
	// interface identity comparison nor retain the previous provider.
	database.backend = struct {
		BackendDB
		uncomparable []byte
	}{NewMockBackend(), []byte{1}}
	if got := database.backendFlushApplyReducerPublishNs(); got != 0 {
		t.Fatalf("unsupported producer retained previous counter=%d", got)
	}
	var typedNil *backenddb.DB
	database.backend = typedNil
	if got := database.backendFlushApplyReducerPublishNs(); got != 0 {
		t.Fatalf("typed nil counter=%d", got)
	}
	database.backend = nil
	if got := database.backendFlushApplyReducerPublishNs(); got != 0 {
		t.Fatalf("absent producer counter=%d", got)
	}
}

func TestBackendFlushApplyCapabilityConcreteAllocations(t *testing.T) {
	database := &DB{backend: new(backenddb.DB)}
	var got uint64
	allocs := testing.AllocsPerRun(1000, func() {
		got = database.backendFlushApplyReducerPublishNs()
	})
	if got != 0 {
		t.Fatalf("new concrete counter=%d", got)
	}
	t.Logf("concrete reducer/publish accessor: %g allocations/call (non-timed control)", allocs)
	if allocs != 0 {
		t.Fatalf("concrete accessor allocations=%g, want zero", allocs)
	}
}
