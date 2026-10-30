package collections

import (
	"errors"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

type nativeDiagnosticNoBuildGroup struct{}

func (nativeDiagnosticNoBuildGroup) BuildNativeOrderedRoot(backenddb.OrderedRootDeltaBatchPublishInput) (uint64, error) {
	panic("unexpected physical construction")
}

func TestNativeMaterializeDiagnosticRetainsOriginalCatalogFailure(t *testing.T) {
	original := errors.New("original catalog construction failure")
	build := &nativeEngineBuild{catalog: &nativeDomainBuild{failure: original}}
	err := build.materializeDomains(nativeDiagnosticNoBuildGroup{}, nil)
	if !errors.Is(err, original) || !strings.Contains(err.Error(), "native materialize catalog") {
		t.Fatalf("lost stage/cause: %v", err)
	}
	if build.failure != err || build.complete {
		t.Fatal("failed original ledger not retained")
	}
	again := build.materializeDomains(nativeDiagnosticNoBuildGroup{}, nil)
	if again != err {
		t.Fatalf("failure replaced: %v", again)
	}
	var refusal backenddb.NativeSemanticBuildRetryableRefusal
	if errors.As(err, &refusal) {
		t.Fatal("terminal materialization became retryable")
	}
}

func TestNativeTypedDiagnosticMarksDeltaOrderAndKeepsFailure(t *testing.T) {
	work := &nativeTypedMaterialization{input: nativeTypedMaterializeInput{deltas: []*nativeTypedDelta{nil}}}
	err := work.materialize()
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || !strings.Contains(err.Error(), "stage=delta-order") {
		t.Fatalf("lost refusal discriminator: %v", err)
	}
	if work.failure != err || work.complete || !work.started || work.input.deltas != nil {
		t.Fatal("changed terminal custody or input release")
	}
	var refusal backenddb.NativeSemanticBuildRetryableRefusal
	if errors.As(err, &refusal) {
		t.Fatal("terminal failure classified prebuild retryable")
	}
}

func TestNativeOrdinaryGraphDiagnosticDistinguishesStaleBasis(t *testing.T) {
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphMeta())
	if err != nil {
		t.Fatal(err)
	}
	if !nativeOrdinaryColumnGraphMeta(meta) {
		t.Fatal("fixture must exercise the ordinary graph lane")
	}
	recipe := &nativeVectorGraphRecipe{ordinary: true, sourceRoot: 41}
	delta := &nativeTypedDelta{data: &nativeTypedRows{graph: recipe}, appliedLSN: 7}
	work := &nativeTypedMaterialization{meta: meta, input: nativeTypedMaterializeInput{baseManifestRoot: 43}}
	err = work.materializeOrdinaryVectorGraph(delta)
	if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("changed sentinel: %v", err)
	}
	for _, field := range []string{"ordinary graph basis", "ordinary=true", "meta_ordinary=true", "recipe_root=41", "build_root=43", "lsn=7"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing %q: %v", field, err)
		}
	}
	if recipe.sourceRoot != 41 || work.input.baseManifestRoot != 43 || delta.appliedLSN != 7 || work.resources != nil {
		t.Fatal("diagnostic changed basis/command or allocated output")
	}
}
