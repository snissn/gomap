package db

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

type rewriteStableOuterLeafCapture struct {
	writer           *rewriteWriter
	builder          *rootpublication.StableResourceSetBuilder
	tokens           []*rootpublication.StableResourceToken
	parentGeneration uint64
	dictionaryIDs    map[uint64]struct{}
	templateIDs      map[uint64]struct{}
}

func (capture *rewriteStableOuterLeafCapture) captureDictionary(ctx context.Context, dictID uint64, dictionary []byte) error {
	if capture == nil || dictID == 0 || len(dictionary) == 0 {
		return nil
	}
	if _, ok := capture.dictionaryIDs[dictID]; ok {
		return nil
	}
	var provider StableDictionaryResourceProvider
	if capture.writer != nil && capture.writer.stableDictionaryResourceProvider != nil {
		provider = capture.writer.stableDictionaryResourceProvider()
	}
	resources, err := captureStableDictionaryResources(ctx, provider, dictID, dictionary)
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
	if capture.dictionaryIDs == nil {
		capture.dictionaryIDs = make(map[uint64]struct{})
	}
	capture.dictionaryIDs[dictID] = struct{}{}
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
	if _, ok := capture.templateIDs[templateID]; ok {
		return nil
	}
	provider, ok := store.(StableTemplateResourceProvider)
	if !ok {
		return fmt.Errorf("%w: template %d lacks stable resource provider", rootpublication.ErrUnresolvedResource, templateID)
	}
	resources, err := captureStableTemplateResources(provider, store, templateID)
	if err != nil {
		return err
	}
	if err := capture.builder.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	if capture.templateIDs == nil {
		capture.templateIDs = make(map[uint64]struct{})
	}
	capture.templateIDs[templateID] = struct{}{}
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
	capture := &rewriteStableOuterLeafCapture{
		writer:  writer,
		builder: rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityOuterLeafRawPointer),
	}
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
	diagnosticPath, err := filepath.Rel(filepath.Dir(capture.writer.leafDir), path)
	if err != nil || diagnosticPath == "." || filepath.IsAbs(diagnosticPath) {
		return valuelog.StableResourceRegistration{}, fmt.Errorf("%w: standalone outer-leaf diagnostic path: %v", rootpublication.ErrUnresolvedResource, err)
	}
	registration := valuelog.StableResourceRegistration{
		Kind:               rootpublication.ResourceOuterLeafLog,
		LogicalLane:        fmt.Sprintf("outer-leaf-%d", capture.writer.leafLane),
		Generation:         uint64(fileID),
		DiagnosticPath:     filepath.ToSlash(diagnosticPath),
		Reachability:       rootpublication.ReachabilityOuterLeafRawPointer,
		ParentGeneration:   capture.parentGeneration,
		NamespaceOperation: operation,
		PinRegistry:        capture.writer.stableResourcePins,
	}
	if operation != rootpublication.NamespaceNone {
		registration.NewName = filepath.Base(path)
	}
	return registration, nil
}

func (capture *rewriteStableOuterLeafCapture) add(token *rootpublication.StableResourceToken) error {
	if capture == nil || capture.builder == nil || token == nil {
		return rootpublication.ErrResourceOwnership
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
	required := make(map[uint64]struct{}, len(ptrs))
	for _, ptr := range ptrs {
		required[uint64(ptr.ValueLogFileID())] = struct{}{}
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

func (capture *rewriteStableOuterLeafCapture) abandon() {
	if capture == nil {
		return
	}
	if capture.builder != nil {
		capture.builder.Abandon()
	}
	capture.releaseTokens()
	capture.builder = nil
}

func (capture *rewriteStableOuterLeafCapture) releaseTokens() {
	for _, token := range capture.tokens {
		token.Release()
	}
	capture.tokens = nil
}
