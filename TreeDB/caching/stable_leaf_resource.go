package caching

import (
	"fmt"
	"math"
	"path/filepath"
	"slices"
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

type finiteStableValueWriter interface {
	CertifyStableCreationNamespaceWithFiniteMetadata(*valuelog.FiniteStableMetadata) error
	StableResourceTokenWithFiniteMetadata(valuelog.StableResourceRegistration, *valuelog.FiniteStableMetadata) (*rootpublication.StableResourceToken, error)
	RotateToWithStableResourcesWithFiniteMetadata(string, uint32, bool, valuelog.StableResourceRegistration, valuelog.StableResourceRegistration, *valuelog.FiniteStableMetadata) (*valuelog.StableResourceRotation, error)
}

type stableOuterLeafCapture struct {
	db                     *DB
	lane                   *lane
	builder                *rootpublication.StableResourceSetBuilder
	tokens                 []*rootpublication.StableResourceToken
	parentGeneration       uint64
	finite                 *PreparedFiniteLeafWorkspace
	metadata               *valuelog.FiniteStableMetadata
	required               []uint64
	maxTokens, maxPointers uint64
}

func newStableOuterLeafCapture(db *DB, lane *lane) *stableOuterLeafCapture {
	return &stableOuterLeafCapture{
		db: db, lane: lane,
		builder: rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer),
	}
}

// The finite capture owns only its local metadata. It deliberately creates no
// generic builder until existing rootpublication allocation hooks are supplied.
func newFiniteStableOuterLeafCapture(db *DB, l *lane, owner *PreparedFiniteLeafWorkspace, count int) (*stableOuterLeafCapture, error) {
	if owner == nil || count < 1 || uint64(count) > owner.maxBatch {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if err := owner.validate(db, l); err != nil {
		return nil, err
	}
	if uint64(count) > math.MaxUint64/7 {
		return nil, errPreparedFiniteLeafWorkspace
	}
	if err := owner.charge(uint64(unsafe.Sizeof(stableOuterLeafCapture{}))); err != nil {
		return nil, err
	}
	metadata, err := owner.stableMetadataOwner()
	if err != nil {
		return nil, err
	}
	return &stableOuterLeafCapture{db: db, lane: l, finite: owner, metadata: metadata, maxTokens: 7 * uint64(count), maxPointers: uint64(count)}, nil
}

func (capture *stableOuterLeafCapture) requireFiniteHooks(owner *PreparedFiniteLeafWorkspace) error {
	if capture == nil || capture.finite != owner || owner == nil || capture.metadata == nil {
		return errPreparedFiniteLeafWorkspace
	}
	if err := owner.validate(capture.db, capture.lane); err != nil {
		return err
	}
	return capture.metadata.RequireRootPublicationHooks()
}

// sortedRequired changes only capture-local generation membership. Its exact
// backing is admitted before make, reused only by this capture and cleared on
// every transfer/failure/close. It grants no file, namespace or token authority.
func (capture *stableOuterLeafCapture) sortedRequired(ptrs []page.ValuePtr) error {
	if capture == nil || capture.finite == nil || uint64(len(ptrs)) > capture.maxPointers {
		return errPreparedFiniteLeafWorkspace
	}
	if err := capture.finite.validate(capture.db, capture.lane); err != nil {
		return err
	}
	if len(ptrs) > cap(capture.required) {
		count := uint64(len(ptrs))
		if count > uint64(math.MaxInt)/uint64(unsafe.Sizeof(uint64(0))) {
			return errPreparedFiniteLeafWorkspace
		}
		if err := capture.finite.charge(count * uint64(unsafe.Sizeof(uint64(0)))); err != nil {
			return err
		}
		next := make([]uint64, len(ptrs))
		clear(capture.required[:cap(capture.required)])
		capture.required = next
	}
	capture.required = capture.required[:len(ptrs)]
	for i, ptr := range ptrs {
		capture.required[i] = uint64(ptr.FileID)
	}
	slices.Sort(capture.required)
	capture.required = slices.Compact(capture.required)
	clear(capture.required[len(capture.required):cap(capture.required)])
	return nil
}

func (capture *stableOuterLeafCapture) certifyCurrent(writer stableValueWriter) error {
	if capture.finite == nil {
		return writer.CertifyStableCreationNamespace()
	}
	if err := capture.requireFiniteHooks(capture.finite); err != nil {
		return err
	}
	finiteWriter, ok := writer.(finiteStableValueWriter)
	if !ok {
		return errPreparedFiniteLeafWorkspace
	}
	return finiteWriter.CertifyStableCreationNamespaceWithFiniteMetadata(capture.metadata)
}

func (capture *stableOuterLeafCapture) rotate(writer stableValueWriter, path string, fileID uint32, syncCurrent bool, closed, active valuelog.StableResourceRegistration) (*valuelog.StableResourceRotation, error) {
	if capture.finite == nil {
		return writer.RotateToWithStableResources(path, fileID, syncCurrent, closed, active)
	}
	if err := capture.requireFiniteHooks(capture.finite); err != nil {
		return nil, err
	}
	finiteWriter, ok := writer.(finiteStableValueWriter)
	if !ok {
		return nil, errPreparedFiniteLeafWorkspace
	}
	return finiteWriter.RotateToWithStableResourcesWithFiniteMetadata(path, fileID, syncCurrent, closed, active, capture.metadata)
}

func (capture *stableOuterLeafCapture) registration(path string, fileID uint32, namespace rootpublication.NamespaceOperation) (valuelog.StableResourceRegistration, error) {
	if capture == nil || capture.db == nil || capture.lane == nil || path == "" || fileID == 0 {
		return valuelog.StableResourceRegistration{}, fmt.Errorf("%w: incomplete outer-leaf stable registration", rootpublication.ErrUnresolvedResource)
	}
	if capture.finite != nil {
		// Rel/fmt/string growth remains part of the future metadata hook. Do
		// not delegate this allocation while finite admission is incomplete.
		if err := capture.requireFiniteHooks(capture.finite); err != nil {
			return valuelog.StableResourceRegistration{}, err
		}
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
	if capture == nil || token == nil || capture.builder == nil && capture.finite == nil {
		return rootpublication.ErrResourceOwnership
	}
	if capture.finite != nil {
		if err := capture.finite.validate(capture.db, capture.lane); err != nil {
			return err
		}
		if uint64(len(capture.tokens)) >= capture.maxTokens {
			return errPreparedFiniteLeafWorkspace
		}
		if cap(capture.tokens) == 0 {
			size := uint64(unsafe.Sizeof((*rootpublication.StableResourceToken)(nil)))
			if capture.maxTokens > uint64(math.MaxInt)/size {
				return errPreparedFiniteLeafWorkspace
			}
			if err := capture.finite.charge(capture.maxTokens * size); err != nil {
				return err
			}
			capture.tokens = make([]*rootpublication.StableResourceToken, 0, int(capture.maxTokens))
		}
		// Constructor already debited the global birth before file/stat/pin work.
	}
	capture.tokens = append(capture.tokens, token)
	return nil
}

func (capture *stableOuterLeafCapture) mergeChild(child *rootpublication.StableResourceSet) error {
	if capture == nil || capture.builder == nil || child == nil {
		return rootpublication.ErrResourceOwnership
	}
	if capture.finite != nil {
		return errPreparedFiniteLeafWorkspace
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
	var token *rootpublication.StableResourceToken
	if capture.finite == nil {
		token, err = stableWriter.StableResourceToken(registration)
	} else {
		if hookErr := capture.requireFiniteHooks(capture.finite); hookErr != nil {
			return hookErr
		}
		finiteWriter, ok := writer.(finiteStableValueWriter)
		if !ok {
			return errPreparedFiniteLeafWorkspace
		}
		token, err = finiteWriter.StableResourceTokenWithFiniteMetadata(registration, capture.metadata)
	}
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
	if capture == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	if capture.finite != nil {
		defer func() { clear(capture.required[:cap(capture.required)]) }()
		if err := capture.requireFiniteHooks(capture.finite); err != nil {
			return nil, err
		}
		if err := capture.sortedRequired(ptrs); err != nil {
			return nil, err
		}
	}
	if capture.builder == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	var required map[uint64]struct{}
	if capture.finite == nil {
		required = make(map[uint64]struct{}, len(ptrs))
		for _, ptr := range ptrs {
			required[uint64(ptr.FileID)] = struct{}{}
		}
	}
	for i, token := range capture.tokens {
		// Detach each slot before transferring/releasing; failure cleanup sees
		// only still-owned tokens, including no historical full-cap tails.
		capture.tokens[i] = nil
		keep := false
		if capture.finite == nil {
			_, keep = required[token.Generation()]
		} else {
			_, keep = slices.BinarySearch(capture.required, token.Generation())
		}
		if !keep {
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
	clear(capture.tokens[:cap(capture.tokens)])
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
		if token != nil {
			token.Release()
		}
	}
	clear(capture.tokens[:cap(capture.tokens)])
	clear(capture.required[:cap(capture.required)])
	capture.tokens = nil
	capture.required = nil
}
