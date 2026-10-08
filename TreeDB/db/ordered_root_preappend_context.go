package db

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// OrderedRootPreAppendContext contains only the exact identity projected under
// the existing raw command serializer. It grants no resource registration or
// durability authority. The serializer remains held until append asserts it.
type OrderedRootPreAppendContext struct{ AppliedCommandLSN uint64 }

// OrderedRootPreAppendPlan owns the actual context batches and exact system
// key/type batch supplied to the same read-only preparation and serial Apply.
// The caller keeps these immutable and closes them after publication returns.
// A failed preparation never assigns a command identity or writes an asset.
// This is an operation plan, not a finite allocation certificate.
type OrderedRootPreAppendPlan struct {
	ContextDeltas []OrderedRootDeltaBatchPublishInput
	SystemDelta   *batch.Batch
	// Optional call-time credit facets. A selected packet supplies all three.
	// Admission consumes request credit before storage, stores only actual
	// resident creator/borrower edges, and grants no finite constructor authority.
	TerminalRequest, TerminalCreator, TerminalResident rootpublication.StableMetadataAccount
	// Borrowed only under the exact producer/command serializers. The actual
	// Manager validates installed bindings and copies claim keys before WAL.
	TerminalDeleteBindings []rootpublication.StableTerminalDeleteBinding
}

type OrderedRootPreAppendBuilder func(OrderedRootPreAppendContext) (OrderedRootPreAppendPlan, error)

var ErrOrderedRootPreAppendMismatch = errors.New("treedb: installed command context differs from pre-WAL operation plan")

// PublishOrderedRootDeltaBatchGroupWithPreAppendContextAndPreparedLimits uses
// the existing serialized batch publisher. Preparation runs under its held
// raw/write/commit guards before WAL; installed context must match every
// prepared byte and the late system delta must match exact keys and types.
// The finite allocation-credit guard remains closed at the existing boundary.
func (db *DB) PublishOrderedRootDeltaBatchGroupWithPreAppendContextAndPreparedLimits(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, prepare OrderedRootPreAppendBuilder, install OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, system OrderedRootGroupCommandWALSystemBuilder, limits *PreparedRootPublicationLimits) (uint64, []uint64, error) {
	if prepare == nil {
		return 0, nil, ErrOrderedRootPreAppendMismatch
	}
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, preflight, intent, install, system, orderedRootCommandWALPublishOptions{preparedLimits: limits, preAppendContext: prepare})
}

// The staged form inherits the actual raw-publish/teardown guards. It never
// creates a second publisher or changes terminal release admission.
func (db *DB) PublishStagedOrderedRootDeltaBatchGroupWithPreAppendContextAndPreparedLimits(ordered []OrderedRootDeltaBatchPublishInput, preflight OrderedRootGroupPreflight, intent *CommandWALIntent, prepare OrderedRootPreAppendBuilder, install OrderedRootDeltaBatchGroupCommandWALDeltaBuilder, system OrderedRootGroupCommandWALSystemBuilder, limits *PreparedRootPublicationLimits) (uint64, []uint64, error) {
	if prepare == nil {
		return 0, nil, ErrOrderedRootPreAppendMismatch
	}
	return db.publishOrderedRootDeltaBatchGroupWithCommandWALContextAndSystemDeltaBuilderSerialized(ordered, preflight, intent, install, system, orderedRootCommandWALPublishOptions{rawPublishLocked: true, teardownPinned: true, preparedLimits: limits, preAppendContext: prepare})
}

func checkPreAppendContextBatches(plan, actual []OrderedRootDeltaBatchPublishInput) error {
	if len(plan) != len(actual) {
		return ErrOrderedRootPreAppendMismatch
	}
	for i := range plan {
		p, a := plan[i], actual[i]
		if p.BaseRoot != a.BaseRoot || p.StoragePolicy != a.StoragePolicy || p.IncludeDeletedOnColdBuild != a.IncludeDeletedOnColdBuild {
			return ErrOrderedRootPreAppendMismatch
		}
		if err := checkPreAppendBatch(p.Delta, a.Delta, true); err != nil {
			return err
		}
	}
	return nil
}

func checkPreAppendBatch(plan, actual *batch.Batch, exactValues bool) error {
	if plan == nil || actual == nil || plan.HasDeleteRanges() || actual.HasDeleteRanges() {
		return ErrOrderedRootPreAppendMismatch
	}
	planned, installed := plan.SortedEntries(), actual.SortedEntries()
	if len(planned) != len(installed) {
		return ErrOrderedRootPreAppendMismatch
	}
	for i, p := range planned {
		a := installed[i]
		if p.Type != a.Type || !bytes.Equal(p.Key, a.Key) || exactValues && (p.IsPtr != a.IsPtr || p.ValuePtr != a.ValuePtr || p.Revision != a.Revision || !bytes.Equal(p.Value, a.Value)) {
			return ErrOrderedRootPreAppendMismatch
		}
	}
	return nil
}
