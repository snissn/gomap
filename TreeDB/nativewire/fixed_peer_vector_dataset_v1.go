package nativewire

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const fixedPeerDatasetChunkRowsV1 = 128
const fixedPeerDatasetChunkBytesV1 = 1 << 20

// Dataset identity is operator input evidence, never a serving capability.
type FixedPeerVectorDatasetIdentityV1 struct {
	ManifestSHA256, VectorsSHA256 string
	Rows, Dimensions              int
	SourceRows, InputBytes        uint64
	OraclePlaneMaxFraction        float64
}
type FixedPeerVectorSeedChunkV1 struct {
	Ordinal, FirstRow, Rows                int
	RequestID, EntrySHA256, Outcome, Error string
	Result                                 raftcluster.SubmitResultV1
}
type fixedPeerVectorDatasetV1 struct {
	identity  FixedPeerVectorDatasetIdentityV1
	chunkRows int
	vectors   []byte // One admitted immutable FP32 representation; JSON exists only per chunk.
}
type fixedPeerVectorDatasetManifestV1 struct {
	Version             int    `json:"version"`
	Docs                int    `json:"docs"`
	Dimensions          int    `json:"dimensions"`
	Metric              string `json:"metric"`
	FloatFormat         string `json:"float_format"`
	DocumentIDPattern   string `json:"document_id_pattern"`
	DocumentVectorsFile string `json:"document_vectors_file"`
	Normalized          bool   `json:"normalized"`
	Files               map[string]struct {
		Bytes  int64
		SHA256 string
	} `json:"files"`
}

// Read and validate the complete frozen input before catalog or data mutation.
// File length and declared dimensions/count are bounded before owned allocation.
func readFixedPeerVectorDatasetV1(path string, config FixedPeerTCPConfigV1) (*fixedPeerVectorDatasetV1, error) {
	raw, err := readFixedPeerDatasetFileV1(filepath.Join(path, "manifest.json"), 64<<10)
	if err != nil {
		return nil, err
	}
	var m fixedPeerVectorDatasetManifestV1
	if err = json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	v := config.VectorInitialization
	if v == nil || m.Version != 1 || m.Docs < 1 || m.Dimensions < 2 || m.Dimensions > 4096 ||
		m.Metric != "cosine" || !m.Normalized || m.FloatFormat != "float32_le_row_major" ||
		m.DocumentIDPattern != "doc-%06d" || m.DocumentVectorsFile != "documents.f32" ||
		m.Dimensions != v.IndexDefinition.Dimensions || uint64(m.Docs) > commitlog.VectorPrepareMaxSourceRowsV1-3 ||
		uint64(m.Docs)+3 > v.MaxSourceRows {
		return nil, errors.New("dataset dimensions/count/format/config admission mismatch")
	}
	vectorBytes := uint64(m.Docs) * uint64(m.Dimensions) * 4
	inputBytes := vectorBytes + uint64(3*m.Dimensions*4+len("seed-x")+len("seed-minus-x")+len("seed-minus-y"))
	// The shared row cap keeps every generated ordinal within six decimal digits.
	inputBytes += uint64(m.Docs) * uint64(len("doc-000000"))
	if inputBytes > commitlog.VectorPrepareMaxSourceBytesV1 {
		return nil, errors.New("dataset FP32 plus actual ID bytes exceed preparation admission")
	}
	file, ok := m.Files["documents.f32"]
	if !ok || file.Bytes < 0 || uint64(file.Bytes) != vectorBytes || len(file.SHA256) != 64 {
		return nil, errors.New("dataset vector file identity/length mismatch")
	}
	vectors, err := readFixedPeerDatasetFileV1(filepath.Join(path, "documents.f32"), int64(vectorBytes))
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(vectors)
	if uint64(len(vectors)) != vectorBytes || hex.EncodeToString(sum[:]) != file.SHA256 {
		return nil, errors.New("dataset vector SHA256/length mismatch")
	}
	for row := 0; row < m.Docs; row++ {
		var norm, plane float64
		for d := 0; d < m.Dimensions; d++ {
			value := math.Float32frombits(binary.LittleEndian.Uint32(vectors[(row*m.Dimensions+d)*4:]))
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return nil, fmt.Errorf("dataset row %d has nonfinite value", row)
			}
			square := float64(value) * float64(value)
			norm += square
			if d < 2 {
				plane += square
			}
		}
		if norm == 0 || math.Abs(norm-1) > 0.001 || plane > 0.81*norm {
			return nil, fmt.Errorf("dataset row %d fails normalized/nonzero/oracle plane<=0.9 eligibility", row)
		}
	}
	manifestSum := sha256.Sum256(raw)
	return &fixedPeerVectorDatasetV1{identity: FixedPeerVectorDatasetIdentityV1{
		ManifestSHA256: hex.EncodeToString(manifestSum[:]), VectorsSHA256: file.SHA256,
		Rows: m.Docs, Dimensions: m.Dimensions, SourceRows: uint64(m.Docs) + 3,
		InputBytes: inputBytes, OraclePlaneMaxFraction: 0.9}, vectors: vectors, chunkRows: min(fixedPeerDatasetChunkRowsV1, fixedPeerDatasetChunkBytesV1/(m.Dimensions*16+128))}, nil
}
func readFixedPeerDatasetFileV1(path string, capBytes int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Size() < 0 || stat.Size() > capBytes {
		return nil, errors.New("dataset file exceeds admitted byte size")
	}
	raw := make([]byte, int(stat.Size()))
	if _, err = io.ReadFull(f, raw); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, errors.New("dataset file changed size while freezing input")
	}
	return raw, nil
}
func fixedPeerOracleVectorV1(dimensions int, x, y float32) []float32 {
	vector := make([]float32, dimensions)
	vector[0], vector[1] = x, y
	return vector
}
func fixedPeerVectorJSONV1(vector []float32, kind string) ([]byte, error) {
	// A struct avoids a per-row map and keeps stable frozen document bytes.
	return json.Marshal(struct {
		Embedding []float32 `json:"embedding"`
		Kind      string    `json:"kind"`
	}{vector, kind})
}
func (d *fixedPeerVectorDatasetV1) chunkV1(first, count int) ([][]byte, [][]byte, error) {
	ids, docs := make([][]byte, count), make([][]byte, count)
	vector := make([]float32, d.identity.Dimensions)
	total := 0
	for i := range ids {
		row := first + i
		ids[i] = []byte(fmt.Sprintf("doc-%06d", row))
		for dim := range vector {
			vector[dim] = math.Float32frombits(binary.LittleEndian.Uint32(d.vectors[(row*len(vector)+dim)*4:]))
		}
		raw, err := fixedPeerVectorJSONV1(vector, "dataset")
		if err != nil {
			return nil, nil, err
		}
		docs[i] = raw
		total += len(ids[i]) + len(raw)
		if total > fixedPeerDatasetChunkBytesV1 {
			return nil, nil, errors.New("dataset JSON chunk exceeds bounded command bytes")
		}
	}
	return ids, docs, nil
}

func validateFixedPeerDatasetConfigV1(config FixedPeerTCPConfigV1, requestID string) error {
	// Reuse the canonical topology/scope checks without changing fixture defaults.
	if config.VectorInitialization == nil {
		return errors.New("dataset requires initialization intent")
	}
	copied := *config.VectorInitialization
	def := copied.IndexDefinition
	if def.Dimensions < 2 || def.Dimensions > 4096 || def.M < 1 || def.M > 64 || def.EfConstruction < 1 || def.EfConstruction > 4096 || def.EfSearch < 1 || def.EfSearch > 4096 {
		return errors.New("dataset index exceeds initialization admission")
	}
	copied.IndexDefinition.Dimensions, copied.IndexDefinition.M, copied.IndexDefinition.EfConstruction, copied.IndexDefinition.EfSearch = 2, 2, 8, 8
	config.VectorInitialization = &copied
	return validateFixedPeerFixtureV1(config, requestID)
}

// The public API validates the user request's 64 ASCII bytes before this
// dataset-only namespace is derived. Its <=137-byte prefix fits the existing
// 512-byte Prepare request and 1024-byte idempotency key limits with suffixes.
// The frozen manifest includes the separately verified vector SHA256.
func fixedPeerVectorDatasetRequestIDV1(requestID, manifestSHA256 string) string {
	return requestID + "/dataset-" + manifestSHA256
}
