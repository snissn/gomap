package db

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type stableContractTestLeafLog struct {
	ptrs      []page.LeafLogPtr
	resources *rootpublication.StableResourceSet
	err       error
}

func (log *stableContractTestLeafLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return log.ptrs[0], log.err
}

func (log *stableContractTestLeafLog) AppendLeafPages([][]byte) ([]page.LeafLogPtr, error) {
	return log.ptrs, log.err
}

func (log *stableContractTestLeafLog) AppendLeafPageWithStableResources([]byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return log.ptrs[0], log.resources, log.err
}

func (log *stableContractTestLeafLog) AppendLeafPagesWithStableResources([][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return log.ptrs, log.resources, log.err
}

func (log *stableContractTestLeafLog) AppendPreparedLeafPageWithStableResources([]byte, []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return log.ptrs[0], log.resources, log.err
}

func (log *stableContractTestLeafLog) AppendPreparedLeafPagesWithStableResources([][]byte, [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return log.ptrs, log.resources, log.err
}

func (log *stableContractTestLeafLog) AppendPreparedLeafPageChildRefsWithStableResources(_ [][]byte, _ [][]byte, refs []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error) {
	refs = refs[:0]
	for _, ptr := range log.ptrs {
		refs = append(refs, page.LeafLogChildRef(ptr))
	}
	return refs, log.resources, log.err
}

func (*stableContractTestLeafLog) Flush() error { return nil }
func (*stableContractTestLeafLog) Sync() error  { return nil }

type stableContractDescriptor struct {
	generation   uint64
	kind         rootpublication.ResourceKind
	reachability rootpublication.ReachabilityField
	frontier     uint64
	onRelease    func()
}

func stableContractResourceSet(t *testing.T, descriptors ...stableContractDescriptor) *rootpublication.StableResourceSet {
	t.Helper()
	if len(descriptors) == 0 {
		return nil
	}
	builder := rootpublication.NewStableResourceSetBuilder(descriptors[0].reachability)
	for i, descriptor := range descriptors {
		file, err := os.CreateTemp(t.TempDir(), "stable-leaf-*")
		if err != nil {
			t.Fatalf("create stable resource %d: %v", i, err)
		}
		if err := file.Truncate(4096); err != nil {
			_ = file.Close()
			t.Fatalf("truncate stable resource %d: %v", i, err)
		}
		t.Cleanup(func() { _ = file.Close() })
		token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{
			Kind: descriptor.kind, LogicalLane: "test-leaf", ResourceID: fmt.Sprintf("segment-%d", i),
			Generation: descriptor.generation, DiagnosticPath: fmt.Sprintf("leaf_vlog/test-%d.vlog", i),
			File: file, Frontier: rootpublication.DurableFrontier{Bytes: descriptor.frontier},
			Reachability: descriptor.reachability, OnRelease: descriptor.onRelease,
		})
		if err != nil {
			t.Fatalf("create stable token %d: %v", i, err)
		}
		if err := builder.Add(token); err != nil {
			token.Release()
			t.Fatalf("add stable token %d: %v", i, err)
		}
	}
	set, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		t.Fatalf("freeze stable resources: %v", err)
	}
	return set
}

func TestLeafPageStableWrapperRejectsMalformedProviderAuthority(t *testing.T) {
	ptrs := []page.LeafLogPtr{
		{FileID: 11, Offset: 32, RecordLengthHint: 16},
		{FileID: 12, Offset: 64, RecordLengthHint: 16},
	}
	outer := func(generation uint64, frontier uint64) stableContractDescriptor {
		return stableContractDescriptor{
			generation: generation, kind: rootpublication.ResourceOuterLeafLog,
			reachability: rootpublication.ReachabilityOuterLeafRawPointer, frontier: frontier,
		}
	}
	tests := []struct {
		name      string
		resources func(*testing.T) *rootpublication.StableResourceSet
	}{
		{name: "nil", resources: func(*testing.T) *rootpublication.StableResourceSet { return nil }},
		{name: "partial", resources: func(t *testing.T) *rootpublication.StableResourceSet {
			return stableContractResourceSet(t, outer(uint64(ptrs[0].ValueLogFileID()), 4096))
		}},
		{name: "extra", resources: func(t *testing.T) *rootpublication.StableResourceSet {
			return stableContractResourceSet(t,
				outer(uint64(ptrs[0].ValueLogFileID()), 4096),
				outer(uint64(ptrs[1].ValueLogFileID()), 4096),
				outer(uint64(page.ValueLogFileID(13)), 4096))
		}},
		{name: "wrong-generation", resources: func(t *testing.T) *rootpublication.StableResourceSet {
			return stableContractResourceSet(t,
				outer(uint64(ptrs[0].ValueLogFileID()), 4096),
				outer(uint64(page.ValueLogFileID(13)), 4096))
		}},
		{name: "wrong-reachability", resources: func(t *testing.T) *rootpublication.StableResourceSet {
			return stableContractResourceSet(t,
				stableContractDescriptor{generation: uint64(ptrs[0].ValueLogFileID()), kind: rootpublication.ResourceValueLog, reachability: rootpublication.ReachabilityValueLogPointer, frontier: 4096},
				stableContractDescriptor{generation: uint64(ptrs[1].ValueLogFileID()), kind: rootpublication.ResourceValueLog, reachability: rootpublication.ReachabilityValueLogPointer, frontier: 4096})
		}},
		{name: "frontier-before-pointer", resources: func(t *testing.T) *rootpublication.StableResourceSet {
			return stableContractResourceSet(t,
				outer(uint64(ptrs[0].ValueLogFileID()), ptrs[0].Offset),
				outer(uint64(ptrs[1].ValueLogFileID()), 4096))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resources := test.resources(t)
			log := &leafPageLogWithRecordLengthHints{inner: &stableContractTestLeafLog{ptrs: ptrs, resources: resources}}
			_, returned, err := log.AppendLeafPagesWithStableResources([][]byte{{1}, {2}})
			if err == nil {
				if returned != nil {
					returned.Release()
				}
				t.Fatal("malformed provider authority was accepted")
			}
			if returned != nil {
				returned.Release()
				t.Fatal("rejected provider authority was returned")
			}
			if resources != nil && resources.Owner() != rootpublication.ResourceOwnerReleased {
				t.Fatalf("rejected resources owner=%v want released", resources.Owner())
			}
		})
	}
}

func TestLeafPageStableWrappersValidateEveryInterface(t *testing.T) {
	ptr := page.LeafLogPtr{FileID: 21, Offset: 32, RecordLengthHint: 16}
	pageBytes := []byte{1}
	prepared := []byte{2}
	providerErr := errors.New("provider failed")
	tests := []struct {
		name string
		call func(*leafPageLogWithRecordLengthHints) (*rootpublication.StableResourceSet, error)
	}{
		{name: "single", call: func(log *leafPageLogWithRecordLengthHints) (*rootpublication.StableResourceSet, error) {
			_, resources, err := log.AppendLeafPageWithStableResources(pageBytes)
			return resources, err
		}},
		{name: "batch", call: func(log *leafPageLogWithRecordLengthHints) (*rootpublication.StableResourceSet, error) {
			_, resources, err := log.AppendLeafPagesWithStableResources([][]byte{pageBytes})
			return resources, err
		}},
		{name: "prepared-single", call: func(log *leafPageLogWithRecordLengthHints) (*rootpublication.StableResourceSet, error) {
			_, resources, err := log.AppendPreparedLeafPageWithStableResources(pageBytes, prepared)
			return resources, err
		}},
		{name: "prepared-batch", call: func(log *leafPageLogWithRecordLengthHints) (*rootpublication.StableResourceSet, error) {
			_, resources, err := log.AppendPreparedLeafPagesWithStableResources([][]byte{pageBytes}, [][]byte{prepared})
			return resources, err
		}},
		{name: "prepared-child-refs", call: func(log *leafPageLogWithRecordLengthHints) (*rootpublication.StableResourceSet, error) {
			_, resources, err := log.AppendPreparedLeafPageChildRefsWithStableResources([][]byte{pageBytes}, [][]byte{prepared}, nil)
			return resources, err
		}},
	}
	for _, test := range tests {
		t.Run(test.name+"/nil-success", func(t *testing.T) {
			log := &leafPageLogWithRecordLengthHints{inner: &stableContractTestLeafLog{ptrs: []page.LeafLogPtr{ptr}}}
			resources, err := test.call(log)
			if err == nil || resources != nil {
				if resources != nil {
					resources.Release()
				}
				t.Fatalf("nil authority result resources=%v err=%v", resources, err)
			}
		})
		t.Run(test.name+"/error-release", func(t *testing.T) {
			resources := stableContractResourceSet(t, stableContractDescriptor{
				generation: uint64(ptr.ValueLogFileID()), kind: rootpublication.ResourceOuterLeafLog,
				reachability: rootpublication.ReachabilityOuterLeafRawPointer, frontier: 4096,
			})
			log := &leafPageLogWithRecordLengthHints{inner: &stableContractTestLeafLog{ptrs: []page.LeafLogPtr{ptr}, resources: resources, err: providerErr}}
			returned, err := test.call(log)
			if !errors.Is(err, providerErr) || returned != nil {
				if returned != nil {
					returned.Release()
				}
				t.Fatalf("provider error result resources=%v err=%v", returned, err)
			}
			if resources.Owner() != rootpublication.ResourceOwnerReleased {
				t.Fatalf("provider error resources owner=%v want released", resources.Owner())
			}
		})
	}
}
