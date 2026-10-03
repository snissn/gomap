package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func writeFixedPeerDatasetTestV1(t testing.TB, rows, dimensions int) string {
	t.Helper()
	path := t.TempDir()
	raw := make([]byte, rows*dimensions*4)
	for i := 0; i < rows; i++ {
		binary.LittleEndian.PutUint32(raw[(i*dimensions+2+i%(dimensions-2))*4:], math.Float32bits(1))
	}
	sum := sha256.Sum256(raw)
	manifest := fixedPeerVectorDatasetManifestV1{Version: 1, Docs: rows, Dimensions: dimensions, Metric: "cosine", Normalized: true, FloatFormat: "float32_le_row_major", DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "documents.f32"}
	manifest.Files = map[string]struct {
		Bytes  int64
		SHA256 string
	}{"documents.f32": {Bytes: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "manifest.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "documents.f32"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func datasetTestConfigV1(t testing.TB, rows, dimensions int) FixedPeerTCPConfigV1 {
	config := initializationTestConfigsV1(t)[0]
	config.VectorInitialization.MaxSourceRows = uint64(rows + 3)
	config.VectorInitialization.IndexDefinition.Dimensions = dimensions
	return config
}
func TestFixedPeerVectorDatasetFrozenAdmissionV1(t *testing.T) {
	path := writeFixedPeerDatasetTestV1(t, 600, 128)
	config := datasetTestConfigV1(t, 600, 128)
	dataset, err := readFixedPeerVectorDatasetV1(path, config)
	if err != nil {
		t.Fatal(err)
	}
	if dataset.identity.SourceRows != 603 || dataset.identity.Dimensions != 128 || dataset.identity.InputBytes > commitlog.VectorPrepareMaxSourceBytesV1 {
		t.Fatalf("identity=%+v", dataset.identity)
	}
	// The acknowledged dataset is a private bounded snapshot, not a mutable path.
	if err := os.WriteFile(filepath.Join(path, "documents.f32"), []byte("changed after admission"), 0600); err != nil {
		t.Fatal(err)
	}
	ids, docs, err := dataset.chunkV1(0, 128)
	if err != nil || len(ids) != 128 || len(docs) != 128 {
		t.Fatalf("frozen chunk: %v", err)
	}
	var document struct{ Embedding []float32 }
	if err := json.Unmarshal(docs[0], &document); err != nil || len(document.Embedding) != 128 || document.Embedding[2] != 1 {
		t.Fatalf("frozen input changed: %+v %v", document, err)
	}
	if _, err := readFixedPeerVectorDatasetV1(path, config); err == nil {
		t.Fatal("admitted changed file")
	}
}
func TestFixedPeerVectorDatasetRefusesBeforeMutationV1(t *testing.T) {
	for _, name := range []string{"count", "dimensions", "bytes", "length", "sha", "nonfinite", "zero", "plane"} {
		t.Run(name, func(t *testing.T) {
			path := writeFixedPeerDatasetTestV1(t, 600, 128)
			config := datasetTestConfigV1(t, 600, 128)
			manifestPath := filepath.Join(path, "manifest.json")
			raw, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			var manifest fixedPeerVectorDatasetManifestV1
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "count":
				manifest.Docs = 16384
			case "dimensions":
				manifest.Dimensions = 129
			case "bytes":
				manifest.Docs = 16380
				manifest.Dimensions = 4096
				config.VectorInitialization.MaxSourceRows = 16384
				config.VectorInitialization.IndexDefinition.Dimensions = 4096
			case "length":
				entry := manifest.Files["documents.f32"]
				entry.Bytes++
				manifest.Files["documents.f32"] = entry
			case "sha":
				entry := manifest.Files["documents.f32"]
				entry.SHA256 = string(make([]byte, 64))
				manifest.Files["documents.f32"] = entry
			default:
				vectors, e := os.ReadFile(filepath.Join(path, "documents.f32"))
				if e != nil {
					t.Fatal(e)
				}
				switch name {
				case "nonfinite":
					binary.LittleEndian.PutUint32(vectors, math.Float32bits(float32(math.NaN())))
				case "zero":
					clear(vectors[:128*4])
				case "plane":
					clear(vectors[:128*4])
					binary.LittleEndian.PutUint32(vectors, math.Float32bits(1))
				}
				sum := sha256.Sum256(vectors)
				entry := manifest.Files["documents.f32"]
				entry.SHA256 = hex.EncodeToString(sum[:])
				manifest.Files["documents.f32"] = entry
				if e := os.WriteFile(filepath.Join(path, "documents.f32"), vectors, 0600); e != nil {
					t.Fatal(e)
				}
			}
			raw, err = json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(manifestPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			// Pure admission runs before fresh-cluster polling, catalog publication, or submit.
			client := &FixedPeerTCPClientV1{config: config}
			initialized, err := client.InitializeVectorDatasetV1(context.Background(), "refusal", path)
			if err == nil || initialized.Stage != "dataset-admission" || initialized.Catalog.Epoch != 0 || initialized.Create.Evidence.Index != 0 || len(initialized.Chunks) != 0 {
				t.Fatalf("reached mutation: %+v %v", initialized, err)
			}
		})
	}
}
func BenchmarkFixedPeerVectorDatasetFreezeV1(b *testing.B) {
	for _, rows := range []int{512, 10000} {
		b.Run(fmtDatasetRowsV1(rows), func(b *testing.B) {
			path := writeFixedPeerDatasetTestV1(b, rows, 128)
			config := datasetTestConfigV1(b, rows, 128)
			b.ReportAllocs()
			b.SetBytes(int64(rows * 128 * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := readFixedPeerVectorDatasetV1(path, config); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
func fmtDatasetRowsV1(rows int) string {
	if rows == 512 {
		return "rows512_dims128"
	}
	return "rows10000_dims128"
}

func TestFixedPeerVectorDatasetCanonicalExporterManifestV1(t *testing.T) {
	path := writeFixedPeerDatasetTestV1(t, 2, 128)
	raw, err := os.ReadFile(filepath.Join(path, "documents.f32"))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	// Literal wire shape from system_dataset.go; never marshal the reader's own type.
	manifest := fmt.Sprintf(`{"version":1,"generator":"fixture","docs":2,"dimensions":128,"queries":1,"top_k":10,"metric":"cosine","normalized":true,"document_id_pattern":"doc-%%06d","document_vectors_file":"documents.f32","query_vectors_file":"queries.f32","float_format":"float32_le_row_major","exact_truth_file":"exact_truth.jsonl","exact_truth_queries":1,"fixture_checksum":"retained","truth_identity":"retained","truth_artifact_sha256":"retained","truth_sha256":"retained","files":{"documents.f32":{"bytes":%d,"sha256":"%x"}}}`, len(raw), sum)
	if err := os.WriteFile(filepath.Join(path, "manifest.json"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	dataset, err := readFixedPeerVectorDatasetV1(path, datasetTestConfigV1(t, 2, 128))
	if err != nil || dataset.identity.SourceRows != 5 {
		t.Fatalf("canonical exporter input refused: %+v %v", dataset, err)
	}
}
func BenchmarkFixedPeerVectorDatasetChunkV1(b *testing.B) {
	path := writeFixedPeerDatasetTestV1(b, 10000, 128)
	dataset, err := readFixedPeerVectorDatasetV1(path, datasetTestConfigV1(b, 10000, 128))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(128 * 128 * 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := dataset.chunkV1(0, 128); err != nil {
			b.Fatal(err)
		}
	}
}

func TestFixedPeerVectorDatasetRequestNamespaceV1(t *testing.T) {
	user := strings.Repeat("x", 64)
	first := fixedPeerVectorDatasetRequestIDV1(user, strings.Repeat("a", 64))
	if len(first) != 137 || len(first+"/prepare") > 512 || len(first+"/dataset/000127") > 1024 || first == fixedPeerVectorDatasetRequestIDV1(user, strings.Repeat("b", 64)) {
		t.Fatalf("invalid dataset namespace: %q", first)
	}
	config := datasetTestConfigV1(t, 600, 128)
	if err := validateFixedPeerDatasetConfigV1(config, user); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{user + "x", "slash/id", "é"} {
		if validateFixedPeerDatasetConfigV1(config, bad) == nil {
			t.Fatalf("expanded public request admission: %q", bad)
		}
	}
}
