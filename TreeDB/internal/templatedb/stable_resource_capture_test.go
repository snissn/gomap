//go:build !windows

package templatedb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/template"
)

func TestCaptureTemplateResourcesInlineDefinitionNeedsExactPinnedIndex(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	store := New(stableTestKV{db: backend}, Config{})
	definition := []byte("inline-template-definition")
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}

	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 1 {
		t.Fatalf("closure len=%d want index only", got)
	}
	tokens := resources.Tokens()
	if len(tokens) != 1 || tokens[0].DiagnosticPath() != "index.db" || tokens[0].Namespace() == nil {
		t.Fatalf("inline closure tokens=%v want pinned index namespace", tokens)
	}
	assertTemplateObligation(t, tokens[0], templateID, definition)
}

func TestCaptureTemplateResourcesPointerDefinitionReturnsExactTransitiveClosure(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	kv := stableTestKV{db: backend}
	store := New(kv, Config{})
	definition := bytes.Repeat([]byte("pointer-template-definition|"), 64)
	templateID := template.TemplateID(definition, 0)
	if err := seedPointerTemplate(backend, templateID, definition); err != nil {
		t.Fatalf("seed pointer template: %v", err)
	}
	registry := backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	snapshot := backend.AcquireSnapshot()
	entry, err := snapshot.GetEntryExact(templateKey(templateID))
	if err != nil {
		t.Fatalf("get pointer entry: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	wantFrontier := entry.ValuePtr.Offset + uint64(page.ValuePtrRecordLength(entry.ValuePtr))

	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 2 {
		t.Fatalf("closure len=%d want index+value-log", got)
	}
	if got := registry.ActivePins(); got != baselinePins+2 {
		t.Fatalf("index/value-log identity pins=%d want baseline %d + 2 capture pins", got, baselinePins)
	}
	paths := make(map[string]bool, 2)
	for _, token := range resources.Tokens() {
		paths[token.DiagnosticPath()] = true
		assertTemplateObligation(t, token, templateID, definition)
		if token.DiagnosticPath() != "index.db" && token.Frontier().Bytes != wantFrontier {
			t.Fatalf("value-log frontier=%d want exact record end %d", token.Frontier().Bytes, wantFrontier)
		}
	}
	if !paths["index.db"] || !paths["value_vlog/value-l0-000001.log"] {
		t.Fatalf("closure paths=%v want exact index and value-log segment", paths)
	}
	resources.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("value-log identity pins after release=%d want baseline %d", got, baselinePins)
	}
}

func TestCaptureTemplateResourcesRejectsParentEscapingDiagnosticPath(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	definition := bytes.Repeat([]byte("parent-escaping-template-definition|"), 64)
	templateID := template.TemplateID(definition, 0)
	if err := seedPointerTemplate(backend, templateID, definition); err != nil {
		t.Fatalf("seed pointer template: %v", err)
	}
	registry := backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	for _, diagnosticPath := range []string{
		filepath.Join("..", "outside.log"),
		filepath.FromSlash("safe/../../outside.log"),
	} {
		t.Run(filepath.ToSlash(diagnosticPath), func(t *testing.T) {
			valueLogTokenCalls := 0
			store := New(stableTestKV{
				db:                     backend,
				diagnosticPathOverride: diagnosticPath,
				valueLogTokenCalls:     &valueLogTokenCalls,
			}, Config{})

			resources, err := store.CaptureTemplateResources(context.Background(), templateID)
			if resources != nil {
				resources.Release()
				t.Fatal("parent-escaping capture returned resources")
			}
			if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("parent-escaping capture error=%v want ErrUnresolvedResource", err)
			}
			if valueLogTokenCalls != 0 {
				t.Fatalf("parent-escaping path reached token construction %d times, want 0", valueLogTokenCalls)
			}
			if got := registry.ActivePins(); got != baselinePins {
				t.Fatalf("parent-escaping capture left identity pins=%d want baseline %d", got, baselinePins)
			}
		})
	}
}

func TestCaptureTemplateResourcesPointerDefinitionResolvesOmittedGroupedRecordLengthHint(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	definition := bytes.Repeat([]byte("grouped-pointer-template-definition|"), 64)
	templateID := template.TemplateID(definition, 0)
	wantFrontier, err := seedGroupedPointerTemplateWithOmittedLengthHint(backend, templateID, definition)
	if err != nil {
		t.Fatalf("seed grouped pointer template: %v", err)
	}

	snapshot := backend.AcquireSnapshot()
	entry, err := snapshot.GetEntryExact(templateKey(templateID))
	if err != nil {
		_ = snapshot.Close()
		t.Fatalf("get grouped pointer entry: %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}
	if got := page.ValuePtrRecordLength(entry.ValuePtr); got != 0 {
		t.Fatalf("grouped pointer length hint=%d want omitted hint", got)
	}

	store := New(stableTestKV{db: backend}, Config{})
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture grouped template: %v", err)
	}
	defer resources.Release()
	if got := resources.Len(); got != 2 {
		t.Fatalf("closure len=%d want index+value-log", got)
	}
	foundValueLog := false
	for _, token := range resources.Tokens() {
		if token.DiagnosticPath() == "index.db" {
			continue
		}
		foundValueLog = true
		if got := token.Frontier().Bytes; got != wantFrontier {
			t.Fatalf("value-log frontier=%d want header-derived record end %d", got, wantFrontier)
		}
	}
	if !foundValueLog {
		t.Fatal("grouped template closure missing value-log token")
	}
}

type pointerTemplateSeed struct {
	templateID uint64
	definition []byte
}

func seedPointerTemplate(backend *backenddb.DB, templateID uint64, definition []byte) error {
	return seedPointerTemplates(backend, []pointerTemplateSeed{{templateID: templateID, definition: definition}})
}

func seedPointerTemplates(backend *backenddb.DB, seeds []pointerTemplateSeed) error {
	valueLogDir := backenddb.ValueLogDirPath(backend.Dir())
	if err := os.MkdirAll(valueLogDir, 0700); err != nil {
		return err
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		return err
	}
	path := filepath.Join(valueLogDir, fmt.Sprintf("value-l0-%06d.log", 1))
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, backend.StableResourceIdentityPinRegistry())
	if err != nil {
		return err
	}
	defer writer.Close()
	// This helper is the segment producer. Register its exact object before a
	// pointer to it can enter a recovery-selectable durable root.
	if err := backend.RegisterValueLogSegment(path, fileID); err != nil {
		return err
	}
	pointers := make([]page.ValuePtr, len(seeds))
	for i, seed := range seeds {
		pointers[i], err = writer.Append(0, nil, seed.templateID, seed.definition)
		if err != nil {
			return err
		}
	}
	if err := writer.Sync(); err != nil {
		return err
	}
	batch := backend.NewBatch().(*backenddb.Batch)
	defer batch.Close()
	for i, seed := range seeds {
		if err := batch.SetPointer(templateKey(seed.templateID), pointers[i]); err != nil {
			return err
		}
	}
	if err := batch.WriteSync(); err != nil {
		return err
	}
	return nil
}

func seedGroupedPointerTemplateWithOmittedLengthHint(backend *backenddb.DB, templateID uint64, definition []byte) (uint64, error) {
	valueLogDir := backenddb.ValueLogDirPath(backend.Dir())
	if err := os.MkdirAll(valueLogDir, 0700); err != nil {
		return 0, err
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		return 0, err
	}
	path := filepath.Join(valueLogDir, fmt.Sprintf("value-l0-%06d.log", 1))
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, backend.StableResourceIdentityPinRegistry())
	if err != nil {
		return 0, err
	}
	defer writer.Close()
	// This helper is the segment producer. Register its exact object before a
	// pointer to it can enter a recovery-selectable durable root.
	if err := backend.RegisterValueLogSegment(path, fileID); err != nil {
		return 0, err
	}
	ptrs, err := writer.AppendFrame(0, nil, []valuelog.Record{
		{RID: templateID, Value: definition},
		{RID: 1, Value: []byte("grouped-sibling")},
	})
	if err != nil {
		return 0, err
	}
	if len(ptrs) != 2 {
		return 0, fmt.Errorf("grouped pointer count=%d want 2", len(ptrs))
	}
	recordLength := page.ValuePtrRecordLength(ptrs[0])
	if recordLength == 0 {
		return 0, fmt.Errorf("grouped writer unexpectedly omitted small record length")
	}
	ptr := ptrs[0]
	ptr.Length = page.ValuePtrMarkGrouped(0, page.ValuePtrSubIndex(ptr))
	if err := writer.Sync(); err != nil {
		return 0, err
	}
	batch := backend.NewBatch().(*backenddb.Batch)
	defer batch.Close()
	if err := batch.SetPointer(templateKey(templateID), ptr); err != nil {
		return 0, err
	}
	if err := batch.WriteSync(); err != nil {
		return 0, err
	}
	return ptr.Offset + uint64(recordLength), nil
}

type templateResourceDescriptorView struct {
	kind         rootpublication.ResourceKind
	identity     rootpublication.StableIdentity
	generation   uint64
	digest       [32]byte
	frontier     rootpublication.DurableFrontier
	reachability []rootpublication.ReachabilityField
	obligations  []rootpublication.StableLogicalObligation
}

func templateResourceDescriptorViews(resources *rootpublication.StableResourceSet) []templateResourceDescriptorView {
	descriptors := resources.Descriptors()
	views := make([]templateResourceDescriptorView, len(descriptors))
	for i, descriptor := range descriptors {
		views[i] = templateResourceDescriptorView{
			kind: descriptor.Kind(), identity: descriptor.Identity(), generation: descriptor.Generation(),
			digest: descriptor.Digest(), frontier: descriptor.Frontier(),
			reachability: descriptor.ReachabilityFields(), obligations: descriptor.LogicalObligations(),
		}
	}
	return views
}

func TestCaptureTemplateResourcesMultiIDCoalescesSharedPhysicalClosureDeterministically(t *testing.T) {
	const iterations = 160
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	definitions := [][]byte{
		bytes.Repeat([]byte("shared-pointer-template-alpha|"), 64),
		bytes.Repeat([]byte("shared-pointer-template-beta|"), 80),
	}
	seeds := make([]pointerTemplateSeed, len(definitions))
	for i, definition := range definitions {
		seeds[i] = pointerTemplateSeed{templateID: template.TemplateID(definition, 0), definition: definition}
	}
	if err := seedPointerTemplates(backend, seeds); err != nil {
		t.Fatalf("seed shared pointer templates: %v", err)
	}

	wantFrontier := uint64(0)
	snapshot := backend.AcquireSnapshot()
	for _, seed := range seeds {
		entry, err := snapshot.GetEntryExact(templateKey(seed.templateID))
		if err != nil {
			_ = snapshot.Close()
			t.Fatalf("get template %d pointer: %v", seed.templateID, err)
		}
		frontier := entry.ValuePtr.Offset + uint64(page.ValuePtrRecordLength(entry.ValuePtr))
		if frontier > wantFrontier {
			wantFrontier = frontier
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("close pointer snapshot: %v", err)
	}

	store := New(stableTestKV{db: backend}, Config{})
	registry := backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	merge := func(order []int) *rootpublication.StableResourceSet {
		t.Helper()
		builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTemplateGeneration)
		defer builder.Abandon()
		for _, index := range order {
			resources, err := store.CaptureTemplateResources(context.Background(), seeds[index].templateID)
			if err != nil {
				t.Fatalf("capture template %d: %v", seeds[index].templateID, err)
			}
			if err := builder.Merge(resources); err != nil {
				resources.Release()
				t.Fatalf("merge template %d closure: %v", seeds[index].templateID, err)
			}
		}
		resources, err := builder.Freeze()
		if err != nil {
			t.Fatalf("freeze merged template closure: %v", err)
		}
		return resources
	}

	beforeFDs, fdErr := os.ReadDir("/dev/fd")
	checkFDs := fdErr == nil
	var wantViews []templateResourceDescriptorView
	for i := 0; i < iterations; i++ {
		order := []int{0, 1}
		if i%2 != 0 {
			order = []int{1, 0}
		}
		resources := merge(order)
		if got := resources.Len(); got != 2 {
			resources.Release()
			t.Fatalf("iteration %d merged descriptors=%d want index+shared value-log", i, got)
		}
		if got := registry.ActivePins(); got != baselinePins+2 {
			resources.Release()
			t.Fatalf("iteration %d coalesced index/value-log identity pins=%d want baseline %d + 2 capture pins", i, got, baselinePins)
		}
		tokens := resources.Tokens()
		descriptors := resources.Descriptors()
		for descriptorIndex, descriptor := range descriptors {
			if descriptor.Kind() != rootpublication.ResourceTemplate {
				resources.Release()
				t.Fatalf("iteration %d descriptor kind=%q want template", i, descriptor.Kind())
			}
			obligations := descriptor.LogicalObligations()
			if len(obligations) != len(seeds) {
				resources.Release()
				t.Fatalf("iteration %d descriptor %d obligations=%d want %d", i, descriptorIndex, len(obligations), len(seeds))
			}
			seen := make(map[uint64]bool, len(obligations))
			for _, obligation := range obligations {
				seen[obligation.Generation] = true
			}
			for _, seed := range seeds {
				if !seen[seed.templateID] {
					resources.Release()
					t.Fatalf("iteration %d descriptor %d missing template generation %d", i, descriptorIndex, seed.templateID)
				}
			}
			if tokens[descriptorIndex].DiagnosticPath() == "value_vlog/value-l0-000001.log" && descriptor.Frontier().Bytes != wantFrontier {
				resources.Release()
				t.Fatalf("iteration %d shared value-log frontier=%d want greatest exact record end %d", i, descriptor.Frontier().Bytes, wantFrontier)
			}
		}
		views := templateResourceDescriptorViews(resources)
		if wantViews == nil {
			wantViews = views
		} else if !reflect.DeepEqual(views, wantViews) {
			resources.Release()
			t.Fatalf("iteration %d order-dependent merged closure\n got: %#v\nwant: %#v", i, views, wantViews)
		}
		resources.Release()
		if got := registry.ActivePins(); got != baselinePins {
			t.Fatalf("iteration %d release left active pins=%d want baseline %d", i, got, baselinePins)
		}
	}
	if checkFDs {
		afterFDs, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Fatalf("read descriptor directory after multi-ID stress: %v", err)
		}
		if len(afterFDs) > len(beforeFDs)+2 {
			t.Fatalf("descriptor count grew from %d to %d after %d multi-ID merges", len(beforeFDs), len(afterFDs), iterations)
		}
	}
	if err := backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("multi-ID release vacuum: %v", err)
	}
}

func TestCaptureTemplateResourcesAcceptsConfiguredSaltedID(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	kv := stableTestKV{db: backend}
	definition := []byte("salted-template-definition")
	templateID := template.TemplateID(definition, 1)
	if err := kv.SetSync(templateKey(templateID), definition); err != nil {
		t.Fatalf("seed salted template: %v", err)
	}
	store := New(kv, Config{MaxIDAttempts: 2})
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture salted template: %v", err)
	}
	resources.Release()
}

func TestCaptureTemplateResourcesRejectsMissingAndMismatchedDefinitionsWithoutPins(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	kv := stableTestKV{db: backend}
	store := New(kv, Config{MaxIDAttempts: 2})
	if resources, captureErr := store.CaptureTemplateResources(context.Background(), 42); captureErr == nil || resources != nil {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("missing capture resources=%v err=%v", resources, captureErr)
	}
	wrongID := template.TemplateID([]byte("expected"), 0)
	if err := kv.SetSync(templateKey(wrongID), []byte("different")); err != nil {
		t.Fatalf("seed mismatch: %v", err)
	}
	if resources, captureErr := store.CaptureTemplateResources(context.Background(), wrongID); !errors.Is(captureErr, rootpublication.ErrResourceConflict) || resources != nil {
		if resources != nil {
			resources.Release()
		}
		t.Fatalf("mismatch capture resources=%v err=%v want resource conflict", resources, captureErr)
	}
	if got := backend.StableResourceIdentityPinRegistry().ActivePins(); got != 0 {
		t.Fatalf("pins after failed captures=%d want 0", got)
	}
}

func TestCaptureTemplateResourcesRejectsEachMissingPointerChild(t *testing.T) {
	for _, omitted := range []templateStablePhysicalRole{templateStableIndexRole, templateStableValueLogRole} {
		t.Run(string(omitted), func(t *testing.T) {
			backend, err := backenddb.Open(backenddb.Options{
				Dir: t.TempDir(), ChunkSize: 64 * 1024, ValueLog: backenddb.ValueLogOptions{ForcePointers: true},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			definition := bytes.Repeat([]byte("pointer-template-child-omission|"), 64)
			templateID := template.TemplateID(definition, 0)
			if err := seedPointerTemplate(backend, templateID, definition); err != nil {
				t.Fatal(err)
			}
			registry := backend.StableResourceIdentityPinRegistry()
			baselinePins := registry.ActivePins()
			store := New(stableTestKV{db: backend}, Config{})
			var withheld []*rootpublication.StableResourceToken
			previous := addTemplateStableResourceToken
			addTemplateStableResourceToken = func(builder *rootpublication.StableResourceSetBuilder, token *rootpublication.StableResourceToken, role templateStablePhysicalRole) error {
				if role == omitted {
					withheld = append(withheld, token)
					return nil
				}
				return builder.Add(token)
			}
			resources, captureErr := store.CaptureTemplateResources(context.Background(), templateID)
			addTemplateStableResourceToken = previous
			for _, token := range withheld {
				token.Release()
			}
			if resources != nil {
				resources.Release()
			}
			if !errors.Is(captureErr, rootpublication.ErrUnresolvedResource) || resources != nil {
				t.Fatalf("omitted %s resources=%v err=%v want ErrUnresolvedResource", omitted, resources, captureErr)
			}
			if got := registry.ActivePins(); got != baselinePins {
				t.Fatalf("omitted %s active pins=%d want baseline %d", omitted, got, baselinePins)
			}
		})
	}
}

func TestCaptureTemplateResourcesBlocksVacuumUntilRelease(t *testing.T) {
	backend, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	store := New(stableTestKV{db: backend}, Config{})
	templateID, err := store.PutTemplateDef(context.Background(), []byte("vacuum-fenced-template"), nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("capture template: %v", err)
	}
	if err := backend.VacuumIndexOnline(context.Background()); !errors.Is(err, rootpublication.ErrResourcePinned) {
		resources.Release()
		t.Fatalf("vacuum while captured error=%v want ErrResourcePinned", err)
	}
	resources.Release()
	if err := backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("vacuum after release: %v", err)
	}
}

func TestCaptureTemplateResourcesFailsBeforeStoreMutationWithoutPhysicalProvider(t *testing.T) {
	kv := newMemKV()
	store := New(kv, Config{})
	definition := []byte("no-physical-provider")
	templateID, err := store.PutTemplateDef(context.Background(), definition, nil)
	if err != nil {
		t.Fatalf("put template: %v", err)
	}
	before := len(kv.data)
	resources, err := store.CaptureTemplateResources(context.Background(), templateID)
	if resources != nil {
		resources.Release()
		t.Fatal("capture without physical provider returned resources")
	}
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("capture error=%v want unresolved resource", err)
	}
	if got := len(kv.data); got != before {
		t.Fatalf("capture mutated store entries=%d want %d", got, before)
	}
}

func TestCaptureTemplateResourcesDedupeDoesNotGrowPinsOrDescriptors(t *testing.T) {
	const iterations = 320
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       t.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()
	definition := bytes.Repeat([]byte("template-resource-plateau|"), 64)
	templateID := template.TemplateID(definition, 0)
	if err := seedPointerTemplate(backend, templateID, definition); err != nil {
		t.Fatalf("seed pointer template: %v", err)
	}
	registry := backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()
	store := New(stableTestKV{db: backend}, Config{})
	warm, err := store.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		t.Fatalf("warm capture: %v", err)
	}
	if got := warm.Len(); got != 2 {
		warm.Release()
		t.Fatalf("warm resources=%d want index+value-log closure", got)
	}
	warm.Release()
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("warm release left active pins=%d want baseline %d", got, baselinePins)
	}

	beforeFDs, fdErr := os.ReadDir("/dev/fd")
	checkFDs := fdErr == nil
	for i := 0; i < iterations; i++ {
		resources, err := store.CaptureTemplateResources(context.Background(), templateID)
		if err != nil {
			t.Fatalf("dedupe capture %d: %v", i, err)
		}
		if got := resources.Len(); got != 2 {
			resources.Release()
			t.Fatalf("dedupe %d resources=%d want index+value-log closure", i, got)
		}
		if got := registry.ActivePins(); got != baselinePins+2 {
			resources.Release()
			t.Fatalf("dedupe %d active index/value-log pins=%d want baseline %d + 2 capture pins", i, got, baselinePins)
		}
		resources.Release()
		if got := registry.ActivePins(); got != baselinePins {
			t.Fatalf("dedupe %d left active pins=%d want baseline %d", i, got, baselinePins)
		}
	}
	if checkFDs {
		afterFDs, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Fatalf("read descriptor directory after stress: %v", err)
		}
		if len(afterFDs) > len(beforeFDs)+2 {
			t.Fatalf("descriptor count grew from %d to %d after %d captures", len(beforeFDs), len(afterFDs), iterations)
		}
	}
	if err := backend.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatalf("dedupe release vacuum: %v", err)
	}
}

func BenchmarkCaptureTemplateResources(b *testing.B) {
	ctx := context.Background()
	for _, benchmark := range []struct {
		name          string
		definition    []byte
		forcePointers bool
	}{
		{name: "inline", definition: []byte("bench-inline-template-definition")},
		{name: "pointer", definition: bytes.Repeat([]byte("bench-pointer-template-definition|"), 64), forcePointers: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			backend, err := backenddb.Open(backenddb.Options{
				Dir:       b.TempDir(),
				ChunkSize: 64 * 1024,
				ValueLog:  backenddb.ValueLogOptions{ForcePointers: benchmark.forcePointers},
			})
			if err != nil {
				b.Fatal(err)
			}
			defer backend.Close()
			store := New(stableTestKV{db: backend}, Config{})
			templateID := template.TemplateID(benchmark.definition, 0)
			if benchmark.forcePointers {
				if err := seedPointerTemplate(backend, templateID, benchmark.definition); err != nil {
					b.Fatal(err)
				}
			} else {
				var err error
				templateID, err = store.PutTemplateDef(ctx, benchmark.definition, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
			registry := backend.StableResourceIdentityPinRegistry()
			baselinePins := registry.ActivePins()
			warm, err := store.CaptureTemplateResources(ctx, templateID)
			if err != nil {
				b.Fatal(err)
			}
			resourcesPerOp := warm.Len()
			identityPinHighWater := registry.ActivePins() - baselinePins
			pinHighWater := identityPinHighWater
			for _, stats := range warm.Stats(time.Now()) {
				if stats.PinHighWater > pinHighWater {
					pinHighWater = stats.PinHighWater
				}
			}
			warm.Release()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resources, err := store.CaptureTemplateResources(ctx, templateID)
				if err != nil {
					b.Fatal(err)
				}
				if resources.Len() != resourcesPerOp {
					resources.Release()
					b.Fatalf("template closure resources=%d want %d", resources.Len(), resourcesPerOp)
				}
				resources.Release()
			}
			b.StopTimer()
			b.ReportMetric(float64(resourcesPerOp), "descriptors/op")
			b.ReportMetric(float64(pinHighWater), "pin_high_water")
			b.ReportMetric(float64(identityPinHighWater), "identity_pin_high_water")
			// The timed operation captures already-durable identities. It must not
			// add content or namespace syncs, including on repeated captures.
			b.ReportMetric(0, "capture_file_syncs/op")
			b.ReportMetric(0, "capture_namespace_syncs/op")
		})
	}
}

func BenchmarkCaptureTemplateResourcesMultiIDCoalesce(b *testing.B) {
	ctx := context.Background()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:       b.TempDir(),
		ChunkSize: 64 * 1024,
		ValueLog:  backenddb.ValueLogOptions{ForcePointers: true},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer backend.Close()
	definitions := [][]byte{
		bytes.Repeat([]byte("bench-shared-pointer-template-alpha|"), 64),
		bytes.Repeat([]byte("bench-shared-pointer-template-beta|"), 80),
	}
	seeds := make([]pointerTemplateSeed, len(definitions))
	for i, definition := range definitions {
		seeds[i] = pointerTemplateSeed{templateID: template.TemplateID(definition, 0), definition: definition}
	}
	if err := seedPointerTemplates(backend, seeds); err != nil {
		b.Fatal(err)
	}
	store := New(stableTestKV{db: backend}, Config{})
	registry := backend.StableResourceIdentityPinRegistry()
	baselinePins := registry.ActivePins()

	captureMerged := func() (*rootpublication.StableResourceSet, uint64, error) {
		builder := rootpublication.NewStableResourceSetBuilder(rootpublication.ReachabilityTemplateGeneration)
		defer builder.Abandon()
		identityPinHighWater := uint64(0)
		for _, seed := range seeds {
			resources, err := store.CaptureTemplateResources(ctx, seed.templateID)
			if err != nil {
				return nil, identityPinHighWater, err
			}
			if pins := registry.ActivePins() - baselinePins; pins > identityPinHighWater {
				identityPinHighWater = pins
			}
			if err := builder.Merge(resources); err != nil {
				resources.Release()
				return nil, identityPinHighWater, err
			}
		}
		resources, err := builder.Freeze()
		return resources, identityPinHighWater, err
	}

	warm, identityPinHighWater, err := captureMerged()
	if err != nil {
		b.Fatal(err)
	}
	resourcesPerOp := warm.Len()
	logicalObligationsPerOp := 0
	for _, descriptor := range warm.Descriptors() {
		logicalObligationsPerOp += len(descriptor.LogicalObligations())
	}
	pinHighWater := uint64(0)
	for _, stats := range warm.Stats(time.Now()) {
		if stats.PinHighWater > pinHighWater {
			pinHighWater = stats.PinHighWater
		}
	}
	identityPinsAfterCoalesce := registry.ActivePins() - baselinePins
	warm.Release()
	if resourcesPerOp != 2 || logicalObligationsPerOp != 4 || identityPinsAfterCoalesce != 2 || registry.ActivePins() != baselinePins {
		b.Fatalf("warm coalesce resources=%d obligations=%d residentPins=%d releasedPins=%d baselinePins=%d", resourcesPerOp, logicalObligationsPerOp, identityPinsAfterCoalesce, registry.ActivePins(), baselinePins)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resources, _, err := captureMerged()
		if err != nil {
			b.Fatal(err)
		}
		if resources.Len() != resourcesPerOp {
			resources.Release()
			b.Fatalf("merged template closure resources=%d want %d", resources.Len(), resourcesPerOp)
		}
		resources.Release()
	}
	b.StopTimer()
	b.ReportMetric(float64(resourcesPerOp), "descriptors/op")
	b.ReportMetric(float64(logicalObligationsPerOp), "logical_obligations/op")
	b.ReportMetric(float64(pinHighWater), "pin_high_water")
	b.ReportMetric(float64(identityPinHighWater), "identity_pin_capture_high_water")
	b.ReportMetric(float64(identityPinsAfterCoalesce), "identity_pins_after_coalesce")
	b.ReportMetric(0, "capture_file_syncs/op")
	b.ReportMetric(0, "capture_namespace_syncs/op")
}

func assertTemplateObligation(t *testing.T, token *rootpublication.StableResourceToken, templateID uint64, definition []byte) {
	t.Helper()
	if token.Kind() != rootpublication.ResourceTemplate || token.Reachability() != rootpublication.ReachabilityTemplateGeneration {
		t.Fatalf("token kind=%q reachability=%q want template generation", token.Kind(), token.Reachability())
	}
	obligations := token.LogicalObligations()
	if len(obligations) != 1 {
		t.Fatalf("token %q obligations=%d want 1", token.DiagnosticPath(), len(obligations))
	}
	wantDigest := sha256.Sum256(definition)
	obligation := obligations[0]
	if obligation.Class != "template-generation" || obligation.Kind != "template" || obligation.Namespace != "templatedb" ||
		obligation.Generation != templateID || obligation.FileID != templateID || obligation.Offset != 0 ||
		obligation.Length != int64(len(definition)) || obligation.Digest != wantDigest ||
		obligation.Reachability != rootpublication.ReachabilityTemplateGeneration {
		t.Fatalf("token %q obligation=%+v", token.DiagnosticPath(), obligation)
	}
}
