// Package mvcctest provides a reusable conformance suite for adapters built on
// TreeDB's public external-timestamp MVCC surface.
//
// The package deliberately models operations as function fields. Downstream
// adapters can translate their own iterator and lifecycle types without making
// those types implement a TreeDB-specific interface.
package mvcctest

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/mvcc"
)

// DurabilityClass names the storage acknowledgement class requested by a
// conformance test. OpenFunc maps it to the backend's concrete options.
type DurabilityClass string

const (
	DurabilityDurable       DurabilityClass = "durable"
	DurabilityWALOnRelaxed  DurabilityClass = "wal_on_relaxed"
	DurabilityWALOffRelaxed DurabilityClass = "wal_off_relaxed"
)

// Iterator is the retained-version iterator surface consumed by the harness.
type Iterator interface {
	Valid() bool
	Entry() mvcc.Version
	Next()
	Seek(logical []byte, timestamp uint64)
	Stats() mvcc.VersionIteratorStats
	Error() error
	Close() error
}

// Adapter is the smallest public MVCC surface exercised by the conformance
// suite. Close must close the owning database handle.
type Adapter struct {
	CommitAt            func(uint64, []mvcc.Mutation, mvcc.CommitMode) error
	GetAt               func([]byte, uint64) (mvcc.Result, error)
	IterateVersions     func(mvcc.VersionIteratorOptions) (Iterator, error)
	DiscardFloor        func() (uint64, error)
	AdvanceDiscardFloor func(uint64, mvcc.CommitMode) error
	PruneVersions       func(mvcc.PruneOptions) (mvcc.PruneStats, error)
	Close               func() error
}

// OpenFunc opens one adapter rooted at dir. A later call with the same dir is
// a reopen and must observe the prior durable state.
type OpenFunc func(dir string, durability DurabilityClass) (Adapter, error)

// FromStore adapts an mvcc.Store plus its owning database close function.
func FromStore(store *mvcc.Store, closeFn func() error) Adapter {
	if store == nil {
		return Adapter{Close: closeFn}
	}
	return Adapter{
		CommitAt: store.CommitAt,
		GetAt:    store.GetAt,
		IterateVersions: func(options mvcc.VersionIteratorOptions) (Iterator, error) {
			return store.IterateVersions(options)
		},
		DiscardFloor:        store.DiscardFloor,
		AdvanceDiscardFloor: store.AdvanceDiscardFloor,
		PruneVersions:       store.PruneVersions,
		Close:               closeFn,
	}
}

func (adapter Adapter) validate() error {
	var missing []string
	checks := []struct {
		name string
		ok   bool
	}{
		{"CommitAt", adapter.CommitAt != nil},
		{"GetAt", adapter.GetAt != nil},
		{"IterateVersions", adapter.IterateVersions != nil},
		{"DiscardFloor", adapter.DiscardFloor != nil},
		{"AdvanceDiscardFloor", adapter.AdvanceDiscardFloor != nil},
		{"PruneVersions", adapter.PruneVersions != nil},
		{"Close", adapter.Close != nil},
	}
	for _, check := range checks {
		if !check.ok {
			missing = append(missing, check.name)
		}
	}
	if len(missing) != 0 {
		return fmt.Errorf("mvcctest: incomplete adapter: %v", missing)
	}
	return nil
}

func closeAdapter(adapter Adapter) error {
	if adapter.Close == nil {
		return errors.New("mvcctest: adapter has no Close function")
	}
	return adapter.Close()
}
