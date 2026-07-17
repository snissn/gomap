package treedb_test

import (
	"errors"
	"fmt"
	"os"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

const authoritativeResourcesVariantID = "public-authoritative-resources-stable-image"

// TestPowerLossCertificationAuthoritativeResourcesPublicReopen retains one
// normal-public-open witness whose stable image contains production collection
// template, secondary, text, column, vector, and auxiliary physical resources.
// The selected crash boundary is emitted by the real checkpoint path; the test
// does not relabel the producer-authority unit matrix as power-loss evidence.
func TestPowerLossCertificationAuthoritativeResourcesPublicReopen(t *testing.T) {
	profileName, profile := certificationProfileFromEnv(t)
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	wantCutID := "cut/" + authoritativeResourcesVariantID + "/after-meta-sync/000"
	if selector != (powerlossoracle.ReplaySelector{}) {
		if selector.CutID != wantCutID || selector.VariantID != authoritativeResourcesVariantID || selector.Seed != powerLossOracleSeed {
			t.Fatalf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, wantCutID, authoritativeResourcesVariantID, powerLossOracleSeed)
		}
	}

	dir := t.TempDir()
	opts := treedb.OptionsFor(profile, dir)
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.IndexOuterLeavesInValueLog = true
	backend, closeBackend, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = closeBackend()
		}
	}()

	manager := collections.NewCollectionManager(backend)
	meta := collections.CollectionMeta{
		Name: "certification-docs",
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatJSON,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled:        true,
				ProfileSupport: collections.ColumnStoreProfileBenchmarkRelaxed,
				Columns: []collections.ColumnStoreColumn{
					{Name: "score", Path: "score", ValueType: collections.ColumnStoreValueInt64, Owner: collections.TypedStorageOwnerColumnPart},
					{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, Owner: collections.TypedStorageOwnerColumnPart, VectorDims: 3},
				},
				SortKey: []collections.ColumnSortKey{{Column: "score"}},
			},
		},
		VectorIndexes: []collections.VectorIndexDefinition{{
			Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine,
			Dimensions: 3, M: 2, Strategy: collections.VectorIndexStrategyColumnGraph,
		}},
		TextIndexes: []collections.TextIndexDefinition{{
			Name: "lexical", Version: collections.TextIndexVersionV2,
			Fields: []collections.TextIndexField{{Field: "title", Weight: 2}, {Field: "body"}}, StorePositions: true,
		}},
	}
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	templateMeta := collections.CollectionMeta{
		Name:    "certification-templates",
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatTemplateV1},
		Indexes: []collections.IndexDefinition{{Name: "by_title", Field: "title", ValueType: collections.IndexValueString}},
	}
	if _, err := manager.CreateCollection(&templateMeta); err != nil {
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([][]byte, 16)
	docs := make([][]byte, 16)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("doc-%02d", i))
		docs[i] = []byte(fmt.Sprintf(`{"title":"durability %02d","body":"stable resource closure","score":%d,"embedding":[1,0,%d]}`, i, i, i%2))
	}
	if _, err := collection.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	templateCollection, err := manager.OpenCollection(templateMeta.Name)
	if err != nil {
		t.Fatal(err)
	}
	templateDocs := make([][]byte, len(docs))
	for i := range templateDocs {
		templateDocs[i], err = collections.EncodeTemplateV1DocumentJSON(docs[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := templateCollection.InsertBatch(ids, templateDocs); err != nil {
		t.Fatal(err)
	}
	if err := templateCollection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss-certification: stop after authoritative-resource checkpoint meta sync")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			return cutErr
		}
		return nil
	})
	if err := backend.Set([]byte("certification/post-cut"), []byte(profileName)); err != nil {
		restore()
		t.Fatal(err)
	}
	err = backend.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) {
		t.Fatalf("checkpoint cut error=%v want=%v", err, cutErr)
	}
	if err := closeBackend(); err != nil && !errors.Is(err, cutErr) {
		t.Logf("close after injected post-meta cut: %v", err)
	}
	closed = true

	readOnly := os.Getenv(powerlossoracle.EnvEvidenceReopenMode) == powerlossoracle.EvidenceReopenReadOnly
	result, reopened, closeReopened, err := powerlossreopen.Stable(model, opts, readOnly)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rejected {
		t.Fatalf("public Open rejected authoritative-resource stable image: %v", result.Err)
	}
	if reopened == nil {
		t.Fatal("public Open returned no database")
	}
	if got, err := reopened.Get([]byte("certification/post-cut")); err != nil || string(got) != profileName {
		_ = closeReopened()
		t.Fatalf("post-cut value=%q err=%v want=%q", got, err, profileName)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

func certificationProfileFromEnv(t *testing.T) (string, treedb.Profile) {
	t.Helper()
	switch profile := os.Getenv("TREEDB_POWERLOSS_PROFILE"); profile {
	case "command_wal_durable":
		return profile, treedb.ProfileCommandWALDurable
	case "command_wal_relaxed":
		return profile, treedb.ProfileCommandWALRelaxed
	case "no_wal_fast":
		return profile, treedb.ProfileNoWALFast
	case "":
		t.Fatal("TREEDB_POWERLOSS_PROFILE is required for certification resource replay")
	default:
		t.Fatalf("unsupported TREEDB_POWERLOSS_PROFILE=%q", profile)
	}
	return "", treedb.ProfileNoWALFast
}
