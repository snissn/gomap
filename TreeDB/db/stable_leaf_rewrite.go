package db

import (
	"context"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"path/filepath"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

type rewriteStableOuterLeafCapture struct {
	writer                                                                             *rewriteWriter
	builder                                                                            *rootpublication.StableResourceSetBuilder
	tokens                                                                             []*rootpublication.StableResourceToken
	parentGeneration                                                                   uint64
	dictionaryIDs                                                                      []uint64
	templateIDs                                                                        []uint64
	metadata                                                                           *retainedalloc.Owner
	metadataCharge, tokensCharge, registrationCharge, dictionaryCharge, templateCharge uint64
}

func (capture *rewriteStableOuterLeafCapture) captureDictionary(ctx context.Context, dictID uint64, dictionary []byte) error {
	if capture == nil || dictID == 0 || len(dictionary) == 0 {
		return nil
	}
	for _, id := range capture.dictionaryIDs {
		if id == dictID {
			return nil
		}
	}
	if err := capture.reserveID(&capture.dictionaryIDs, &capture.dictionaryCharge); err != nil {
		return err
	}
	var provider StableDictionaryResourceProvider
	if capture.writer != nil && capture.writer.stableDictionaryResourceProvider != nil {
		provider = capture.writer.stableDictionaryResourceProvider()
	}
	resources, err := captureStableDictionaryResources(ctx, provider, dictID, dictionary, capture.metadata)
	if err != nil {
		return err
	}
	if resources == nil {
		return fmt.Errorf("%w: dictionary %d has no stable resource closure", rootpublication.ErrUnresolvedResource, dictID)
	}
	if err := capture.builder.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	capture.dictionaryIDs = append(capture.dictionaryIDs, dictID)
	return nil
}

func (capture *rewriteStableOuterLeafCapture) captureEncodedTemplatePayload(store templ.Store, payload []byte) error {
	if capture == nil {
		return nil
	}
	templateID, err := templ.EncodedPayloadTemplateID(payload)
	if err != nil {
		return err
	}
	for _, id := range capture.templateIDs {
		if id == templateID {
			return nil
		}
	}
	if err := capture.reserveID(&capture.templateIDs, &capture.templateCharge); err != nil {
		return err
	}
	provider, ok := store.(StableTemplateResourceProvider)
	if !ok {
		return fmt.Errorf("%w: template %d lacks stable resource provider", rootpublication.ErrUnresolvedResource, templateID)
	}
	resources, err := captureStableTemplateResources(provider, store, templateID, capture.metadata)
	if err != nil {
		return err
	}
	if err := capture.builder.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	capture.templateIDs = append(capture.templateIDs, templateID)
	return nil
}

func newRewriteStableOuterLeafCapture(writer *rewriteWriter) (*rewriteStableOuterLeafCapture, error) {
	if writer == nil || writer.leafDir == "" || writer.leafStaging {
		return nil, fmt.Errorf("%w: rewrite writer is not an authoritative raw outer-leaf producer", rootpublication.ErrUnresolvedResource)
	}
	if writer.stableRegistryErr != nil {
		return nil, writer.stableRegistryErr
	}
	if writer.stableResourcePins == nil {
		return nil, fmt.Errorf("%w: raw outer-leaf producer lacks the DB-scoped pin registry", rootpublication.ErrUnresolvedResource)
	}
	metadata := writer.stableResourceMetadata
	var charge uint64
	var builder *rootpublication.StableResourceSetBuilder
	if metadata != nil {
		charge = retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rewriteStableOuterLeafCapture{})))
		if err := metadata.AddPending(charge); err != nil {
			return nil, err
		}
		var err error
		builder, err = rootpublication.NewStableResourceSetBuilderWithMetadata(metadata, rootpublication.ReachabilityOuterLeafRawPointer)
		if err != nil {
			metadata.RemovePending(charge)
			return nil, err
		}
	} else {
		builder = rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer)
	}
	capture := &rewriteStableOuterLeafCapture{writer: writer, builder: builder, metadata: metadata, metadataCharge: charge}

	return capture, nil
}

func (capture *rewriteStableOuterLeafCapture) bindParentGeneration(writer *valuelog.Writer) error {
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
		return fmt.Errorf("%w: raw outer-leaf parent has zero generation", rootpublication.ErrUnresolvedResource)
	}
	capture.parentGeneration = generation
	return nil
}

func (capture *rewriteStableOuterLeafCapture) registration(path string, fileID uint32, operation rootpublication.NamespaceOperation) (valuelog.StableResourceRegistration, error) {
	if capture == nil || capture.writer == nil || path == "" || fileID == 0 || capture.parentGeneration == 0 {
		return valuelog.StableResourceRegistration{}, fmt.Errorf("%w: incomplete standalone outer-leaf registration", rootpublication.ErrUnresolvedResource)
	}
	registration, charge, err := valuelog.NewOuterLeafStableRegistration(filepath.Dir(capture.writer.leafDir), path, uint32(capture.writer.leafLane), capture.parentGeneration, operation, capture.writer.stableResourcePins, capture.metadata)
	if err != nil {
		return valuelog.StableResourceRegistration{}, err
	}
	capture.registrationCharge += charge
	registration.Generation = uint64(fileID)
	return registration, nil
}

func (capture *rewriteStableOuterLeafCapture) add(token *rootpublication.StableResourceToken) error {
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

func (capture *rewriteStableOuterLeafCapture) addRotation(rotation *valuelog.StableResourceRotation) error {
	if rotation == nil {
		return rootpublication.ErrResourceOwnership
	}
	defer rotation.Release()
	if token := rotation.TakeClosed(); token != nil {
		if err := capture.add(token); err != nil {
			token.Release()
			return err
		}
	}
	if token := rotation.TakeActive(); token != nil {
		if err := capture.add(token); err != nil {
			token.Release()
			return err
		}
	}
	return nil
}

func (capture *rewriteStableOuterLeafCapture) captureRotation(writer *valuelog.Writer, nextPath string, nextFileID uint32, syncCurrent bool) (bool, error) {
	if err := capture.bindParentGeneration(writer); err != nil {
		return false, err
	}
	closedOperation := rootpublication.NamespaceNone
	created, err := writer.StableCreationNamespacePending()
	if err != nil {
		return false, err
	}
	if created {
		closedOperation = rootpublication.NamespaceCreate
	}
	closed, err := capture.registration(capture.writer.leafCurrentPath, capture.writer.leafCurrentFileID, closedOperation)
	if err != nil {
		return false, err
	}
	active, err := capture.registration(nextPath, nextFileID, rootpublication.NamespaceCreate)
	if err != nil {
		return false, err
	}
	rotation, err := writer.RotateToWithStableResources(nextPath, nextFileID, syncCurrent, closed, active)
	if err != nil {
		if rotation != nil {
			rotation.Release()
		}
		return valuelog.RotationInstalled(err), err
	}
	if err := capture.addRotation(rotation); err != nil {
		return true, err
	}
	return true, nil
}

func (capture *rewriteStableOuterLeafCapture) captureCurrent() error {
	if capture == nil || capture.writer == nil || capture.writer.leafW == nil {
		return fmt.Errorf("%w: standalone outer-leaf writer unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := capture.bindParentGeneration(capture.writer.leafW); err != nil {
		return err
	}
	operation := rootpublication.NamespaceNone
	created, err := capture.writer.leafW.StableCreationNamespacePending()
	if err != nil {
		return err
	}
	if created {
		operation = rootpublication.NamespaceCreate
	}
	registration, err := capture.registration(capture.writer.leafCurrentPath, capture.writer.leafCurrentFileID, operation)
	if err != nil {
		return err
	}
	token, err := capture.writer.leafW.StableResourceToken(registration)
	if err != nil {
		return err
	}
	if err := capture.add(token); err != nil {
		token.Release()
		return err
	}
	return nil
}

func (capture *rewriteStableOuterLeafCapture) freeze(ptrs []page.LeafLogPtr) (*rootpublication.StableResourceSet, error) {
	if capture == nil || capture.builder == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	for i, token := range capture.tokens {
		required := false
		for _, ptr := range ptrs {
			if uint64(ptr.ValueLogFileID()) == token.Generation() {
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

func (capture *rewriteStableOuterLeafCapture) abandon() {
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

func (capture *rewriteStableOuterLeafCapture) releaseTokens() {
	for _, token := range capture.tokens {
		token.Release()
	}
	capture.disposeTokens()
}

// reserveID retains only the producer dedup keys. Growth admits the full new
// backing during overlap; the old backing ends before its refund.
func (capture *rewriteStableOuterLeafCapture) reserveID(ids *[]uint64, admitted *uint64) error {
	if len(*ids) < cap(*ids) {
		return nil
	}
	capacity := cap(*ids) * 2
	if capacity == 0 {
		capacity = 2
	}
	var charge uint64
	if capture.metadata != nil {
		charge = retainedalloc.AllocationCharge(uint64(capacity) * 8)
		if err := capture.metadata.AddPending(charge); err != nil {
			return err
		}
	}
	next := make([]uint64, len(*ids), capacity)
	copy(next, *ids)
	clear(*ids)
	old := *admitted
	*ids = next
	*admitted = charge
	if capture.metadata != nil {
		capture.metadata.RemovePending(old)
	}
	return nil
}
func (capture *rewriteStableOuterLeafCapture) disposeTokens() {
	clear(capture.tokens)
	capture.tokens = nil
	if capture.metadata != nil {
		capture.metadata.RemovePending(capture.tokensCharge)
	}
	capture.tokensCharge = 0
}
func (capture *rewriteStableOuterLeafCapture) disposeMetadata() {
	owner := capture.metadata
	charge := capture.metadataCharge + capture.registrationCharge + capture.dictionaryCharge + capture.templateCharge
	clear(capture.dictionaryIDs)
	clear(capture.templateIDs)
	capture.dictionaryIDs, capture.templateIDs = nil, nil
	capture.writer, capture.metadata = nil, nil
	capture.metadataCharge, capture.registrationCharge, capture.dictionaryCharge, capture.templateCharge = 0, 0, 0, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}
