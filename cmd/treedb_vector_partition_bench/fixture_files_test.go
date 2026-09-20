package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func testExternalFixtureImportArgsV1(t *testing.T) ([]string, string) {
	t.Helper()
	root := t.TempDir()
	train, test := contiguousFloat64Matrix(40, 4), contiguousFloat64Matrix(10, 4)
	for i := range train {
		copy(train[i], []float64{float64(i + 1), 2, 3, 4})
	}
	copy(train[3], train[2]) // Corpus duplicates are legitimate distinct IDs.
	for i := range test {
		copy(test[i], []float64{1, float64(i + 1), 7, 3})
	}
	files := make(map[string]vectorPartitionSystemDatasetFileV1)
	for name, rows := range map[string][][]float64{"train.f32": train, "test.f32": test} {
		if err := writeVectorPartitionSystemFloat32RowsV1(filepath.Join(root, name), rows, files); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(root, "fixture")
	return []string{"import-fixture", "-train", filepath.Join(root, "train.f32"), "-test", filepath.Join(root, "test.f32"), "-out", out,
		"-fixture", "test-external", "-source", "test-source", "-source-revision", "pinned-test-revision",
		"-train-sha256", files["train.f32"].SHA256, "-test-sha256", files["test.f32"].SHA256,
		"-train-rows", "40", "-test-rows", "10", "-train-offset", "2", "-test-offset", "2", "-vectors", "32", "-queries", "4", "-dimensions", "4", "-seed", "1"}, out
}

func TestExternalFixtureImportRoundTripV1(t *testing.T) {
	args, out := testExternalFixtureImportArgsV1(t)
	if err := run(args, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, err := loadFixture(out)
	if err != nil {
		t.Fatal(err)
	}
	if m.Generator != externalFixtureGeneratorV1 || m.External.TrainOffset != 2 || m.External.TestOffset != 2 {
		t.Fatalf("missing source selection: %+v", m)
	}
	vectors, queries, err := loadFixtureDataV1(out, m)
	if err != nil || fixtureChecksumFromData(vectors, queries) != m.Checksum {
		t.Fatalf("round trip changed checksum: %v", err)
	}
	want := normalize([]float64{3, 2, 3, 4})
	for d := range want {
		want[d] = float64(float32(want[d]))
	}
	if !reflect.DeepEqual(vectors[0], want) || !reflect.DeepEqual(vectors[0], vectors[1]) {
		t.Fatal("selection, FP32 rounding, or corpus duplicates changed")
	}
	if err := run(args, io.Discard); err == nil {
		t.Fatal("overwrote existing fixture")
	}
	newDir := filepath.Join(filepath.Dir(out), "relocated")
	if err := os.Rename(out, newDir); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadFixture(newDir)
	if err != nil || loaded != m {
		t.Fatal("relocation changed manifest identity")
	}
	got, queryGot, err := m8ProductionFixtureDataV1(config{dataset: newDir}, loaded)
	if err != nil || !reflect.DeepEqual(got, vectors) || !reflect.DeepEqual(queryGot, queries) {
		t.Fatalf("M8 did not load frozen external bytes: %v", err)
	}
	if v, q, err := m8ProductionFixtureDataV1(config{dataset: "missing", m8VariantDBs: []string{"parent-only"}}, m); err != nil || v != nil || q != nil {
		t.Fatal("matrix parent allocated fixture")
	}
	cache := t.TempDir()
	var truthOutput strings.Builder
	if err := run([]string{"generate-truth-cache", "-dataset", newDir, "-out", cache, "-seed", "1", "-max-exact-truth-visits", "256"}, &truthOutput); err != nil {
		t.Fatal(err)
	}
	truthSHA := strings.TrimPrefix(strings.Fields(truthOutput.String())[1], "artifact_sha256=")
	truth, _, err := m8ReadTruthCacheV1(m8TruthCacheArtifactPathV1(cache, m8TruthCacheIdentityV1(m, 10)), m, m.Queries, 10, uint64(m.Vectors), truthSHA)
	if err != nil {
		t.Fatal(err)
	}
	wantTruth, err := m8ExactTruthFixtureV1(vectors, queries, 10)
	if err != nil || !reflect.DeepEqual(truth, wantTruth) {
		t.Fatalf("canonical FP32 truth mismatch: %v", err)
	}
	exported := filepath.Join(t.TempDir(), "export")
	if err := run([]string{"system-export-dataset", "-dataset", newDir, "-truth-cache", cache, "-truth-cache-sha256", truthSHA, "-out", exported}, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"documents.f32", "queries.f32"} {
		a, _ := os.ReadFile(filepath.Join(newDir, name))
		b, err := os.ReadFile(filepath.Join(exported, name))
		if err != nil || !reflect.DeepEqual(a, b) {
			t.Fatalf("system export changed %s: %v", name, err)
		}
	}
	// Query-only loading must not allocate/read the corpus; corpus-only loading
	// likewise does not own the query stream or an exact truth pass.
	if err := os.Rename(filepath.Join(newDir, "documents.f32"), filepath.Join(newDir, "documents.saved")); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixtureQueriesV1(newDir, m); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixtureVectorsV1(newDir, m); err == nil {
		t.Fatal("missing external corpus silently regenerated")
	}
}

func TestExternalFixtureImportBoundsV1(t *testing.T) {
	for _, tc := range []struct{ flag, value string }{
		{"-vectors", "1000001"}, {"-queries", "1000001"}, {"-dimensions", "4097"}, {"-test-offset", "7"},
		{"-train-offset", "-1"}, {"-train-rows", "9223372036854775807"}, {"-max-checksum-visits", "127"}, {"-train-sha256", "invalid"},
	} {
		t.Run(tc.flag, func(t *testing.T) {
			args, out := testExternalFixtureImportArgsV1(t)
			args = append(args, "-train", "must-not-open", tc.flag, tc.value)
			if err := run(args, io.Discard); err == nil || strings.Contains(err.Error(), "must-not-open") {
				t.Fatalf("did not reject before input read: %v", err)
			}
			if _, err := os.Lstat(out); !os.IsNotExist(err) {
				t.Fatal("invalid import created output")
			}
		})
	}
	args, _ := testExternalFixtureImportArgsV1(t)
	if err := run(args, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"generate-fixture", "-out", t.TempDir(), "-vectors", "32", "-generator", externalFixtureGeneratorV1}, io.Discard); err == nil {
		t.Fatal("external data accepted by procedural generator")
	}
}

func TestExternalFixtureCorruptionV1(t *testing.T) {
	for _, kind := range []string{"truncated", "trailing", "hash", "nan", "infinity", "zero", "nonunit", "symlink", "metadata"} {
		t.Run(kind, func(t *testing.T) {
			args, out := testExternalFixtureImportArgsV1(t)
			if err := run(args, io.Discard); err != nil {
				t.Fatal(err)
			}
			m, err := loadFixture(out)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(out, "documents.f32")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "truncated":
				raw = raw[:len(raw)-1]
			case "trailing":
				raw = append(raw, 0)
			case "hash":
				raw[0] ^= 1
			case "nan":
				binary.LittleEndian.PutUint32(raw, math.Float32bits(float32(math.NaN())))
			case "infinity":
				binary.LittleEndian.PutUint32(raw, math.Float32bits(float32(math.Inf(1))))
			case "zero":
				clear(raw[:16])
			case "nonunit":
				binary.LittleEndian.PutUint32(raw, math.Float32bits(12))
			case "metadata":
				m.External.Documents.Bytes++
			case "symlink":
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".saved", path); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "symlink" {
				if err := os.WriteFile(path, raw, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if kind != "hash" {
				digest := sha256.Sum256(raw)
				m.External.Documents.SHA256 = hex.EncodeToString(digest[:])
			}
			if _, err := loadFixtureVectorsV1(out, m); err == nil {
				t.Fatal("accepted invalid external corpus")
			}
		})
	}
}

func TestExternalFixtureSourceHashAndSplitLeakageV1(t *testing.T) {
	args, out := testExternalFixtureImportArgsV1(t)
	path := filepath.Join(filepath.Dir(out), "train.f32")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1 // Outside selected slice: whole-source hash must still fail.
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(args, io.Discard); err == nil || !strings.Contains(err.Error(), "SHA256 mismatch") {
		t.Fatalf("unselected source corruption was not detected: %v", err)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("failed source check published fixture")
	}
	args, out = testExternalFixtureImportArgsV1(t)
	path = filepath.Join(filepath.Dir(out), "test.f32")
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for d, v := range []float32{6, 4, 6, 8} {
		binary.LittleEndian.PutUint32(raw[(2*4+d)*4:], math.Float32bits(v))
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	args = append(args, "-test-sha256", hex.EncodeToString(digest[:]))
	if err := run(args, io.Discard); err == nil || !strings.Contains(err.Error(), "duplicates normalized corpus row") {
		t.Fatalf("normalized cross-split leakage was accepted: %v", err)
	}
}

func TestExternalFixtureProceduralManifestRemainsUnchangedV1(t *testing.T) {
	m := m8QualificationFixturesV1[0]
	raw, err := json.Marshal(m)
	if err != nil || strings.Contains(string(raw), "external") {
		t.Fatal("changed legacy manifest encoding")
	}
	m.External.Identity = "not-procedural"
	if err := validateFixtureSyntax(m, maxVectors); err == nil {
		t.Fatal("accepted contradictory source provenance")
	}
}

func TestExternalFixtureTruthIdentityRejectsChangedQueriesV1(t *testing.T) {
	args, dataset := testExternalFixtureImportArgsV1(t)
	if err := run(args, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(filepath.Dir(dataset), "truth")
	truthArgs := []string{"generate-truth-cache", "-dataset", dataset, "-out", cache, "-seed", "1", "-max-exact-truth-visits", "256"}
	if err := run(append(truthArgs, "-max-exact-truth-visits", "255"), io.Discard); err == nil {
		t.Fatal("truth cap did not charge both checksum and canonical passes")
	}
	var output strings.Builder
	if err := run(truthArgs, &output); err != nil {
		t.Fatal(err)
	}
	truthSHA := strings.TrimPrefix(strings.Fields(output.String())[1], "artifact_sha256=")
	oldIdentity := m8TruthCacheIdentityV1(m, 10)
	path := filepath.Join(dataset, "queries.f32")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	first := append([]byte(nil), raw[:16]...)
	copy(raw[:16], raw[16:32])
	copy(raw[16:32], first)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	m.External.Queries.SHA256 = hex.EncodeToString(digest[:])
	if _, err := loadFixtureQueriesV1(dataset, m); err != nil {
		t.Fatal(err)
	}
	if m8TruthCacheIdentityV1(m, 10) == oldIdentity {
		t.Fatal("changed query file reused old truth identity")
	}
	if _, _, err := m8ReadTruthCacheV1(m8TruthCacheArtifactPathV1(cache, oldIdentity), m, m.Queries, 10, uint64(m.Vectors), truthSHA); err == nil {
		t.Fatal("query-only consumer accepted stale trusted truth")
	}
	manifest, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataset, "fixture_manifest.json"), manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(truthArgs, io.Discard); err == nil || !strings.Contains(err.Error(), "checksum does not match") {
		t.Fatalf("published truth under stale fixture checksum: %v", err)
	}
}

func TestExternalFixtureRetainedBuildAndReplayV1(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	args, dataset := testExternalFixtureImportArgsV1(t)
	if err := run(args, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Dir(dataset)
	dbDir := filepath.Join(root, "retained")
	build := []string{"-dataset", dataset, "-out", filepath.Join(root, "m3"), "-m3-persist-db", dbDir,
		"-partitions", "4", "-probes", "1", "-overlap", "0", "-top-k", "10", "-stage", "overlap,partition_index",
		"-partition-repetitions", "1", "-partition-pivots", "2", "-partition-max-leaf-bucket", "8", "-partition-degree", "4",
		"-router-max-scalar-work", "50000000000", "-shard-plan", "byte_bounded", "-shard-plan-target-hot-bytes",
		strconv.FormatUint(uint64(vectorpartition.PackFixedOverheadBytesV1+6*(alignedRowBytesForTest(4)+vectorpartition.GraphIdentityOverheadPerRowV1)), 10)}
	var output bytes.Buffer
	if err := runWithHermeticProvenance(t, build, &output); err != nil {
		t.Fatal(err)
	}
	descriptor, err := m3ReadVariantDescriptorV1(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	vectors, _, err := loadFixtureDataV1(dataset, m)
	if err != nil {
		t.Fatal(err)
	}
	assets, err := openM8ProductionExistingAssetSetV1(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m8ValidateExistingAssetsFixtureV1(assets.collection, assets.status.Manifest, m, vectors); err != nil {
		_ = assets.Close()
		t.Fatal(err)
	}
	representatives := assets.status.Representatives
	if err := assets.Close(); err != nil {
		t.Fatal(err)
	}
	cache := filepath.Join(root, "truth")
	output.Reset()
	if err := run([]string{"generate-truth-cache", "-dataset", dataset, "-out", cache, "-seed", "1", "-max-exact-truth-visits", "256"}, &output); err != nil {
		t.Fatal(err)
	}
	truthSHA := strings.TrimPrefix(strings.Fields(output.String())[1], "artifact_sha256=")
	truth, _, err := m8ReadTruthCacheV1(m8TruthCacheArtifactPathV1(cache, m8TruthCacheIdentityV1(m, 10)), m, m.Queries, 10, uint64(m.Vectors), truthSHA)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the retained attribution loader without collecting serving rows.
	report := m8ProductionReportV1{Dataset: m, DatasetDirectory: dataset, Variant: &descriptor,
		RouterRepresentatives: representatives, Config: m8ProductionConfigEvidenceV1{TopK: 10, RouterScoreBudget: 1024}}
	if err := m8QualificationRetainedAttributionV1(root, report, truth, m8ProductionMeasurementTranscriptV1{}); err != nil {
		t.Fatal(err)
	}
	report.DatasetDirectory = t.TempDir()
	if err := m8QualificationRetainedAttributionV1(root, report, truth, m8ProductionMeasurementTranscriptV1{}); err == nil || !strings.Contains(err.Error(), "outside qualification root") {
		t.Fatalf("external retained queries escaped qualification root: %v", err)
	}
	report.DatasetDirectory = dataset
	if err := os.Rename(filepath.Join(dataset, "queries.f32"), filepath.Join(dataset, "queries.saved")); err != nil {
		t.Fatal(err)
	}
	if err := m8QualificationRetainedAttributionV1(root, report, truth, m8ProductionMeasurementTranscriptV1{}); err == nil {
		t.Fatal("retained attribution silently regenerated missing external queries")
	}
	if err := os.Rename(filepath.Join(dataset, "queries.saved"), filepath.Join(dataset, "queries.f32")); err != nil {
		t.Fatal(err)
	}
	cfg := config{dataset: dataset, out: filepath.Join(root, "feasibility"), m8TruthCache: cache, m8TruthCacheSHA256: truthSHA,
		topK: 10, recallTarget: 0, m8MembershipProbes: 2, m8MembershipPackLimit: 4, maxBytes: 1 << 30}
	artifact, err := m8RunMembershipFeasibilityV1(cfg, m, vectors, dbDir, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if err := m8ReplayMembershipFeasibilityV1(cfg, m, dbDir, descriptor, artifact); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dataset, "documents.f32"), filepath.Join(dataset, "documents.saved")); err != nil {
		t.Fatal(err)
	}
	if err := m8ReplayMembershipFeasibilityV1(cfg, m, dbDir, descriptor, artifact); err == nil {
		t.Fatal("retained replay silently regenerated missing external corpus")
	}
}
