package caching

import (
	"fmt"
	"path/filepath"

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
	db               *DB
	lane             *lane
	builder          *rootpublication.StableResourceSetBuilder
	tokens           []*rootpublication.StableResourceToken
	parentGeneration uint64
}

func newStableOuterLeafCapture(db *DB, lane *lane) *stableOuterLeafCapture {
	return &stableOuterLeafCapture{
		db: db, lane: lane,
		builder: rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer),
	}
}

func (capture *stableOuterLeafCapture) registration(path string, fileID uint32, namespace rootpublication.NamespaceOperation) (valuelog.StableResourceRegistration, error) {
	if capture == nil || capture.db == nil || capture.lane == nil || path == "" || fileID == 0 {
		return valuelog.StableResourceRegistration{}, fmt.Errorf("%w: incomplete outer-leaf stable registration", rootpublication.ErrUnresolvedResource)
	}
	diagnosticPath, err := filepath.Rel(filepath.Dir(capture.db.dir), path)
	if err != nil || diagnosticPath == "." || filepath.IsAbs(diagnosticPath) {
		return valuelog.StableResourceRegistration{}, fmt.Errorf("%w: outer-leaf diagnostic path: %v", rootpublication.ErrUnresolvedResource, err)
	}
	registration := valuelog.StableResourceRegistration{
		Kind:               rootpublication.ResourceOuterLeafLog,
		LogicalLane:        fmt.Sprintf("outer-leaf-%d", capture.lane.id),
		Generation:         uint64(fileID),
		DiagnosticPath:     filepath.ToSlash(diagnosticPath),
		Reachability:       rootpublication.ReachabilityOuterLeafRawPointer,
		ParentGeneration:   capture.parentGeneration,
		NamespaceOperation: namespace,
		PinRegistry:        capture.db.valueLogIdentityPins,
	}
	if namespace != rootpublication.NamespaceNone {
		registration.NewName = filepath.Base(path)
	}
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
	required := make(map[uint64]struct{}, len(ptrs))
	for _, ptr := range ptrs {
		required[uint64(ptr.FileID)] = struct{}{}
	}
	for _, token := range capture.tokens {
		if _, ok := required[token.Generation()]; !ok {
			token.Release()
			continue
		}
		if err := capture.builder.Add(token); err != nil {
			token.Release()
			capture.builder.Abandon()
			capture.releaseTokens()
			capture.builder = nil
			return nil, err
		}
	}
	capture.tokens = nil
	set, err := capture.builder.Freeze()
	if err != nil {
		capture.builder.Abandon()
	}
	capture.builder = nil
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
}

func (capture *stableOuterLeafCapture) releaseTokens() {
	for _, token := range capture.tokens {
		token.Release()
	}
	capture.tokens = nil
}
