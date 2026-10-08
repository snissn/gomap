package caching

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"path/filepath"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type stableValueWriter interface {
	valueWriter
	CertifyStableCreationNamespace() error
	StableResourceToken(valuelog.StableResourceRegistration) (*rootpublication.StableResourceToken, error)
	RotateToWithStableResources(path string, fileID uint32, syncCurrent bool, closed, active valuelog.StableResourceRegistration) (*valuelog.StableResourceRotation, error)
	StableCreationNamespacePending() (bool, error)
	StableNamespaceParentGeneration() (uint64, error)
}

type stableOuterLeafCapture struct {
	db                                               *DB
	lane                                             *lane
	builder                                          *rootpublication.StableResourceSetBuilder
	tokens                                           []*rootpublication.StableResourceToken
	parentGeneration                                 uint64
	metadata                                         *retainedalloc.Owner
	metadataCharge, tokensCharge, registrationCharge uint64
}

type stableResourceMetadataProvider interface {
	StableResourceMetadataOwner() *retainedalloc.Owner
}

func newStableOuterLeafCapture(db *DB, lane *lane) (*stableOuterLeafCapture, error) {
	var metadata *retainedalloc.Owner
	if provider, ok := db.backend.(stableResourceMetadataProvider); ok {
		metadata = provider.StableResourceMetadataOwner()
	}
	if metadata == nil {
		return &stableOuterLeafCapture{db: db, lane: lane,
			builder: rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer)}, nil
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableOuterLeafCapture{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(metadata, rootpublication.ReachabilityOuterLeafRawPointer)
	if err != nil {
		metadata.RemovePending(charge)
		return nil, err
	}
	return &stableOuterLeafCapture{db: db, lane: lane, builder: builder, metadata: metadata, metadataCharge: charge}, nil
}

func (capture *stableOuterLeafCapture) registration(path string, fileID uint32, namespace rootpublication.NamespaceOperation) (valuelog.StableResourceRegistration, error) {
	if capture == nil || capture.db == nil || capture.lane == nil || path == "" || fileID == 0 {
		return valuelog.StableResourceRegistration{}, fmt.Errorf("%w: incomplete outer-leaf stable registration", rootpublication.ErrUnresolvedResource)
	}
	registration, charge, err := valuelog.NewOuterLeafStableRegistration(filepath.Dir(capture.db.dir), path, uint32(capture.lane.id), capture.parentGeneration, namespace, capture.db.valueLogIdentityPins, capture.metadata)
	if err != nil {
		return valuelog.StableResourceRegistration{}, err
	}
	capture.registrationCharge += charge
	registration.Generation = uint64(fileID)
	return registration, nil
}

func (capture *stableOuterLeafCapture) bindParentGeneration(writer stableValueWriter) error {
	if capture == nil || writer == nil {
		return rootpublication.ErrResourceOwnership
	}
	if capture.parentGeneration != 0 {
		return nil
	}
	generation, err := writer.StableNamespaceParentGeneration()
	if err != nil {
		return err
	}
	if generation == 0 {
		return fmt.Errorf("%w: outer-leaf namespace parent has zero generation", rootpublication.ErrUnresolvedResource)
	}
	capture.parentGeneration = generation
	return nil
}

func (capture *stableOuterLeafCapture) addToken(token *rootpublication.StableResourceToken) error {
	if capture == nil || capture.builder == nil || token == nil {
		return rootpublication.ErrResourceOwnership
	}
	if capture.metadata != nil && len(capture.tokens) == cap(capture.tokens) {
		capacity := cap(capture.tokens) * 2
		if capacity == 0 {
			capacity = 2
		}
		charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(token)))
		if err := capture.metadata.AddPending(charge); err != nil {
			return err
		}
		next := make([]*rootpublication.StableResourceToken, len(capture.tokens), capacity)
		copy(next, capture.tokens)
		clear(capture.tokens)
		old := capture.tokensCharge
		capture.tokens, capture.tokensCharge = next, charge
		capture.metadata.RemovePending(old)
	}
	capture.tokens = append(capture.tokens, token)
	return nil
}

func (capture *stableOuterLeafCapture) mergeChild(child *rootpublication.StableResourceSet) error {
	if capture == nil || capture.builder == nil || child == nil {
		return rootpublication.ErrResourceOwnership
	}
	return capture.builder.Merge(child)
}

func (capture *stableOuterLeafCapture) addRotation(rotation *valuelog.StableResourceRotation) error {
	if rotation == nil {
		return rootpublication.ErrResourceOwnership
	}
	defer rotation.Release()
	if token := rotation.TakeClosed(); token != nil {
		if err := capture.addToken(token); err != nil {
			token.Release()
			return err
		}
	}
	if token := rotation.TakeActive(); token != nil {
		if err := capture.addToken(token); err != nil {
			token.Release()
			return err
		}
	}
	return nil
}

func (capture *stableOuterLeafCapture) captureCurrent(writer valueWriter, path string, fileID uint32) error {
	stableWriter, ok := writer.(stableValueWriter)
	if !ok {
		return fmt.Errorf("%w: outer-leaf writer lacks stable capture", rootpublication.ErrUnresolvedResource)
	}
	if err := capture.bindParentGeneration(stableWriter); err != nil {
		return err
	}
	created, err := stableWriter.StableCreationNamespacePending()
	if err != nil {
		return err
	}
	namespace := rootpublication.NamespaceNone
	if created {
		namespace = rootpublication.NamespaceCreate
	}
	registration, err := capture.registration(path, fileID, namespace)
	if err != nil {
		return err
	}
	token, err := stableWriter.StableResourceToken(registration)
	if err != nil {
		return err
	}
	if err := capture.addToken(token); err != nil {
		token.Release()
		return err
	}
	return nil
}

func (capture *stableOuterLeafCapture) freeze(ptrs []page.ValuePtr) (*rootpublication.StableResourceSet, error) {
	if capture == nil || capture.builder == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	for i, token := range capture.tokens {
		required := false
		for _, ptr := range ptrs {
			if uint64(ptr.FileID) == token.Generation() {
				required = true
				break
			}
		}
		capture.tokens[i] = nil
		if !required {
			token.Release()
			continue
		}
		if err := capture.builder.Add(token); err != nil {
			token.Release()
			capture.abandon()
			return nil, err
		}
	}
	capture.disposeTokens()
	set, err := capture.builder.Freeze()
	if err != nil {
		capture.builder.Abandon()
	}
	capture.builder = nil
	capture.disposeMetadata()
	return set, err
}

func (capture *stableOuterLeafCapture) abandon() {
	if capture == nil {
		return
	}
	if capture.builder != nil {
		capture.builder.Abandon()
	}
	capture.releaseTokens()
	capture.builder = nil
	capture.disposeMetadata()
}

func (capture *stableOuterLeafCapture) releaseTokens() {
	for _, token := range capture.tokens {
		token.Release()
	}
	capture.disposeTokens()
}

// disposeTokens ends the capture's array aliases; transferred entries retain
// their own token metadata independently.
func (capture *stableOuterLeafCapture) disposeTokens() {
	clear(capture.tokens)
	capture.tokens = nil
	if capture.metadata != nil {
		capture.metadata.RemovePending(capture.tokensCharge)
		capture.tokensCharge = 0
	}
}
func (capture *stableOuterLeafCapture) disposeMetadata() {
	if capture.metadata == nil {
		return
	}
	metadata, charge := capture.metadata, capture.metadataCharge+capture.registrationCharge
	capture.db, capture.lane, capture.metadata = nil, nil, nil
	capture.metadataCharge = 0
	capture.registrationCharge = 0
	metadata.RemovePending(charge)
}
