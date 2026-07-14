package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/template"
)

type testStableTemplateProvider struct {
	templateID   uint64
	definition   []byte
	file         *os.File
	captureErr   error
	captureCalls atomic.Int32
	releaseCalls atomic.Int32
}

func newTestStableTemplateProvider(t *testing.T, definition []byte) *testStableTemplateProvider {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "template-authority-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(definition); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return &testStableTemplateProvider{
		templateID: template.TemplateID(definition, 0),
		definition: append([]byte(nil), definition...),
		file:       file,
	}
}

func (provider *testStableTemplateProvider) GetCandidates(context.Context, uint64, int) ([]template.Candidate, error) {
	if provider == nil || provider.templateID == 0 {
		return nil, nil
	}
	return []template.Candidate{{ID: provider.templateID, Size: len(provider.definition)}}, nil
}

func (provider *testStableTemplateProvider) GetTemplateDef(_ context.Context, templateID uint64) ([]byte, error) {
	if provider == nil || templateID != provider.templateID {
		return nil, template.ErrMissingTemplate
	}
	return append([]byte(nil), provider.definition...), nil
}

func (provider *testStableTemplateProvider) PutTemplateDef(context.Context, []byte, []uint64) (uint64, error) {
	return 0, errors.New("test template store is read-only")
}

func (provider *testStableTemplateProvider) CaptureTemplateResources(_ context.Context, templateID uint64) (*rootpublication.StableResourceSet, error) {
	provider.captureCalls.Add(1)
	if provider.captureErr != nil {
		return nil, provider.captureErr
	}
	if provider == nil || provider.file == nil || templateID != provider.templateID {
		return nil, fmt.Errorf("test template %d unavailable", templateID)
	}
	digest := sha256.Sum256(provider.definition)
	logical := rootpublication.StableLogicalObligation{
		Class: "template-generation", Kind: "template", Namespace: "test",
		Generation: templateID, FileID: templateID, Offset: 0, Length: int64(len(provider.definition)),
		Reachability: rootpublication.ReachabilityTemplateGeneration, Digest: digest,
	}
	token, err := rootpublication.NewStableProducerResourceTokenForDomain(
		rootpublication.StableProducerTemplate,
		rootpublication.StableResourceSpec{
			Kind: rootpublication.ResourceTemplate, LogicalLane: "test/template", ResourceID: "template",
			Generation: templateID, DiagnosticPath: "template.test", File: provider.file,
			Frontier:           rootpublication.DurableFrontier{Bytes: uint64(len(provider.definition))},
			Digest:             sha256.Sum256([]byte("test-template-physical-v1")),
			Reachability:       rootpublication.ReachabilityTemplateGeneration,
			LogicalObligations: []rootpublication.StableLogicalObligation{logical}, ContentSynced: true,
			OnRelease: func() { provider.releaseCalls.Add(1) },
		},
		"authoritative-transitive",
	)
	if err != nil {
		return nil, err
	}
	builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTemplateGeneration)
	if err := builder.Add(token); err != nil {
		token.Release()
		builder.Abandon()
		return nil, err
	}
	resources, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
	}
	return resources, err
}

func TestValidateStableTemplateResourceClosureRejectsDefinitionMismatch(t *testing.T) {
	definition := []byte("template-definition-a")
	provider := newTestStableTemplateProvider(t, definition)
	resources, err := provider.CaptureTemplateResources(context.Background(), provider.templateID)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Release()
	if err := ValidateStableTemplateResourceClosure(resources, provider.templateID, definition); err != nil {
		t.Fatalf("validate matching closure: %v", err)
	}
	if err := ValidateStableTemplateResourceClosure(resources, provider.templateID, []byte("template-definition-b")); !errors.Is(err, rootpublication.ErrResourceConflict) {
		t.Fatalf("definition mismatch error=%v want resource conflict", err)
	}
}
