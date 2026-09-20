package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
)

const externalFixtureGeneratorV1 = "treedb_vector_partition_external_f32_v1"
const externalFixtureNormalizationV1 = "binary64_fma_l2_then_float32_v1"

// Value fields keep fixtureManifest comparable. Paths are deliberately absent:
// the same manifest and fixed-name files can be relocated for retained replay.
type externalFixtureSourceV1 struct {
	Identity      string                             `json:"identity"`
	Revision      string                             `json:"revision"`
	Normalization string                             `json:"normalization"`
	TrainSHA256   string                             `json:"train_sha256"`
	TestSHA256    string                             `json:"test_sha256"`
	TrainRows     int                                `json:"train_rows"`
	TestRows      int                                `json:"test_rows"`
	TrainOffset   int                                `json:"train_offset"`
	TestOffset    int                                `json:"test_offset"`
	Documents     vectorPartitionSystemDatasetFileV1 `json:"documents"`
	Queries       vectorPartitionSystemDatasetFileV1 `json:"queries"`
}

func validateExternalFixtureV1(m fixtureManifest) error {
	s := m.External
	if m.Generator != externalFixtureGeneratorV1 {
		if s != (externalFixtureSourceV1{}) {
			return errors.New("procedural fixture must not declare external data")
		}
		return nil
	}
	if s.Identity == "" || len(s.Identity) > 512 || s.Revision == "" || len(s.Revision) > 128 || s.Normalization != externalFixtureNormalizationV1 || m.QueryOrdinalOffset != 0 {
		return errors.New("external fixture requires source identity/revision, explicit normalization, and zero procedural query offset")
	}
	for _, file := range []struct {
		rows, offset, selected int
		sourceSHA              string
		output                 vectorPartitionSystemDatasetFileV1
	}{{s.TrainRows, s.TrainOffset, m.Vectors, s.TrainSHA256, s.Documents}, {s.TestRows, s.TestOffset, m.Queries, s.TestSHA256, s.Queries}} {
		if file.rows < 1 || file.rows > maxVectors || file.offset < 0 || file.selected < 1 || file.selected > file.rows || file.offset > file.rows-file.selected || !m8SHA256V1(file.sourceSHA) || !m8SHA256V1(file.output.SHA256) {
			return errors.New("external fixture has invalid source shape, selection, or SHA256")
		}
		if int64(file.rows)*int64(m.Dimensions)*4 > maxFixtureBytes || file.output.Bytes != int64(file.selected)*int64(m.Dimensions)*4 {
			return errors.New("external fixture FP32 file size exceeds cap or disagrees with shape")
		}
	}
	return nil
}

func loadFixtureVectorsV1(dir string, m fixtureManifest) ([][]float64, error) {
	if err := validatePartitionFixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
		return nil, err
	}
	if m.Generator != externalFixtureGeneratorV1 {
		return fixtureVectors(m), nil
	}
	return readFixtureFloat32SliceV1(filepath.Join(dir, "documents.f32"), m.Vectors, 0, m.Vectors, m.Dimensions, m.External.Documents.SHA256, false)
}

func loadFixtureQueriesV1(dir string, m fixtureManifest) ([][]float64, error) {
	if err := validateM3FixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
		return nil, err
	}
	if m.Generator == externalFixtureGeneratorV1 {
		return readFixtureFloat32SliceV1(filepath.Join(dir, "queries.f32"), m.Queries, 0, m.Queries, m.Dimensions, m.External.Queries.SHA256, false)
	}
	if m.Generator == fixtureGenerator {
		_, queries := fixtureData(m)
		return queries, nil
	}
	return qualificationQueriesV1(m), nil
}

func loadFixtureDataV1(dir string, m fixtureManifest) ([][]float64, [][]float64, error) {
	if err := validateM3FixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
		return nil, nil, err
	}
	if m.Generator != externalFixtureGeneratorV1 {
		vectors, queries := fixtureData(m)
		return vectors, queries, nil
	}
	vectors, err := loadFixtureVectorsV1(dir, m)
	if err != nil {
		return nil, nil, err
	}
	queries, err := loadFixtureQueriesV1(dir, m)
	return vectors, queries, err
}

// Hash the entire pinned input, retaining only the declared contiguous slice.
// Import normalizes once then rounds to FP32; replay never renormalizes those
// bytes, so build, canonical truth, and serving see exactly the same values.
func readFixtureFloat32SliceV1(path string, rows, offset, selected, dims int, wantSHA string, normalizeInput bool) ([][]float64, error) {
	if rows < 1 || rows > maxVectors || selected < 1 || selected > rows || offset < 0 || offset > rows-selected || dims < 1 || dims > maxDimensions || !m8SHA256V1(wantSHA) || int64(rows)*int64(dims)*4 > maxFixtureBytes || int64(selected)*int64(dims)*8 > maxFixtureBytes {
		return nil, errors.New("FP32 fixture shape/hash exceeds pre-allocation bounds")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	wantBytes := int64(rows) * int64(dims) * 4
	if !info.Mode().IsRegular() || info.Size() != wantBytes {
		return nil, fmt.Errorf("FP32 fixture %s must be a regular file of exactly %d bytes", path, wantBytes)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Size() != wantBytes {
		return nil, errors.New("FP32 fixture changed during open")
	}
	h := sha256.New()
	r := bufio.NewReaderSize(io.TeeReader(f, h), 1<<20)
	rowBytes := int64(dims) * 4
	if _, err := io.CopyN(io.Discard, r, int64(offset)*rowBytes); err != nil {
		return nil, err
	}
	matrix := contiguousFloat64Matrix(selected, dims)
	raw := make([]byte, rowBytes)
	for i, row := range matrix {
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, err
		}
		var norm2 float64
		for d := range row {
			value := float64(math.Float32frombits(binary.LittleEndian.Uint32(raw[d*4:])))
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("FP32 fixture row %d contains non-finite values", offset+i)
			}
			row[d] = value
			norm2 = math.FMA(value, value, norm2)
		}
		if norm2 == 0 || (!normalizeInput && math.Abs(norm2-1) > 1e-6) {
			return nil, fmt.Errorf("FP32 fixture row %d has invalid norm squared %g", offset+i, norm2)
		}
		if normalizeInput {
			normalize(row)
			for d := range row {
				row[d] = float64(float32(row[d]))
				if row[d] == 0 { // canonicalize signed zero for split-leakage checks
					row[d] = 0
				}
			}
		}
	}
	if _, err := io.CopyN(io.Discard, r, int64(rows-offset-selected)*rowBytes); err != nil {
		return nil, err
	}
	if _, err := r.ReadByte(); err != io.EOF {
		return nil, errors.New("FP32 fixture has trailing bytes or read failure")
	}
	if hex.EncodeToString(h.Sum(nil)) != wantSHA {
		return nil, fmt.Errorf("FP32 fixture %s SHA256 mismatch", path)
	}
	return matrix, nil
}

func rejectExternalFixtureSplitLeakageV1(vectors, queries [][]float64) error {
	// Only queries need a hash table. Duplicate corpus vectors remain distinct
	// documents with the existing ordinal-based IDs and deterministic tie order.
	queryHashes := make(map[[sha256.Size]byte]struct{}, len(queries))
	raw := make([]byte, len(vectors[0])*4)
	rowHash := func(row []float64) [sha256.Size]byte {
		for d, value := range row {
			binary.LittleEndian.PutUint32(raw[d*4:], math.Float32bits(float32(value)))
		}
		return sha256.Sum256(raw)
	}
	for _, query := range queries {
		queryHashes[rowHash(query)] = struct{}{}
	}
	for i, row := range vectors {
		if _, found := queryHashes[rowHash(row)]; found {
			return fmt.Errorf("external fixture query duplicates normalized corpus row %d", i)
		}
	}
	return nil
}

func runImportFixtureV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench import-fixture", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var train, test, out string
	var maxChecksumVisits int64
	m := fixtureManifest{SchemaVersion: schemaVersion, Generator: externalFixtureGeneratorV1, Arithmetic: fixtureArithmetic, Metric: "cosine", Checksum: strings.Repeat("0", 64)}
	s := &m.External
	s.Normalization = externalFixtureNormalizationV1
	fs.StringVar(&train, "train", "", "raw FP32LE row-major training file")
	fs.StringVar(&test, "test", "", "raw FP32LE row-major query file")
	fs.StringVar(&out, "out", "", "fresh fixture directory")
	fs.StringVar(&m.Fixture, "fixture", "", "frozen selected-fixture name")
	fs.StringVar(&s.Identity, "source", "", "public dataset identity")
	fs.StringVar(&s.Revision, "source-revision", "", "pinned dataset revision")
	fs.StringVar(&s.TrainSHA256, "train-sha256", "", "SHA256 of entire training file")
	fs.StringVar(&s.TestSHA256, "test-sha256", "", "SHA256 of entire query file")
	fs.IntVar(&s.TrainRows, "train-rows", 0, "rows in entire training file")
	fs.IntVar(&s.TestRows, "test-rows", 0, "rows in entire query file")
	fs.IntVar(&s.TrainOffset, "train-offset", 0, "first selected training row")
	fs.IntVar(&s.TestOffset, "test-offset", 0, "first selected query row")
	fs.IntVar(&m.Vectors, "vectors", 0, "selected corpus rows")
	fs.IntVar(&m.Queries, "queries", 0, "selected query rows")
	fs.IntVar(&m.Dimensions, "dimensions", 0, "source dimensions")
	fs.Int64Var(&m.Seed, "seed", 1, "benchmark seed; does not resample source rows")
	fs.Int64Var(&maxChecksumVisits, "max-checksum-visits", maxBenchmarkWorkUnits, "exact checksum query/corpus visit cap")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || train == "" || test == "" || out == "" {
		return errors.New("import-fixture requires train, test, fresh out, source identity/hashes/shape, and no positional arguments")
	}
	s.Documents = vectorPartitionSystemDatasetFileV1{Bytes: int64(m.Vectors) * int64(m.Dimensions) * 4, SHA256: m.Checksum}
	s.Queries = vectorPartitionSystemDatasetFileV1{Bytes: int64(m.Queries) * int64(m.Dimensions) * 4, SHA256: m.Checksum}
	if err := validateM3FixtureWithCaps(m, maxVectors, maxFixtureBytes); err != nil {
		return err
	}
	if visits, err := memoryMul(int64(m.Vectors), int64(m.Queries)); err != nil || maxChecksumVisits < 1 || visits > maxChecksumVisits {
		return errors.New("import fixture checksum work exceeds explicit visit cap")
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		return errors.New("import fixture output must not exist")
	}
	vectors, err := readFixtureFloat32SliceV1(train, s.TrainRows, s.TrainOffset, m.Vectors, m.Dimensions, s.TrainSHA256, true)
	if err != nil {
		return err
	}
	queries, err := readFixtureFloat32SliceV1(test, s.TestRows, s.TestOffset, m.Queries, m.Dimensions, s.TestSHA256, true)
	if err != nil {
		return err
	}
	if err := rejectExternalFixtureSplitLeakageV1(vectors, queries); err != nil {
		return err
	}
	m.Checksum = fixtureChecksumFromData(vectors, queries)
	if err := os.Mkdir(out, 0o755); err != nil {
		return err
	}
	files := make(map[string]vectorPartitionSystemDatasetFileV1)
	if err := writeVectorPartitionSystemFloat32RowsV1(filepath.Join(out, "documents.f32"), vectors, files); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemFloat32RowsV1(filepath.Join(out, "queries.f32"), queries, files); err != nil {
		return err
	}
	s.Documents, s.Queries = files["documents.f32"], files["queries.f32"]
	// Publish the manifest last. Interrupted imports are not admissible fixtures.
	f, err := os.OpenFile(filepath.Join(out, "fixture_manifest.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(m)
	err = errors.Join(encodeErr, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "fixture=%s vectors=%d queries=%d dimensions=%d checksum=%s\n", out, m.Vectors, m.Queries, m.Dimensions, m.Checksum)
	return err
}
