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
)

type vectorPartitionSystemDatasetFileV1 struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type vectorPartitionSystemDatasetManifestV1 struct {
	Version             int                                           `json:"version"`
	Generator           string                                        `json:"generator"`
	Docs                int                                           `json:"docs"`
	Dimensions          int                                           `json:"dimensions"`
	Queries             int                                           `json:"queries"`
	TopK                int                                           `json:"top_k"`
	Metric              string                                        `json:"metric"`
	Normalized          bool                                          `json:"normalized"`
	DocumentIDPattern   string                                        `json:"document_id_pattern"`
	DocumentVectorsFile string                                        `json:"document_vectors_file"`
	QueryVectorsFile    string                                        `json:"query_vectors_file"`
	FloatFormat         string                                        `json:"float_format"`
	ExactTruthFile      string                                        `json:"exact_truth_file"`
	ExactTruthQueries   int                                           `json:"exact_truth_queries"`
	FixtureChecksum     string                                        `json:"fixture_checksum"`
	TruthIdentity       string                                        `json:"truth_identity"`
	TruthArtifactSHA256 string                                        `json:"truth_artifact_sha256"`
	TruthSHA256         string                                        `json:"truth_sha256"`
	Files               map[string]vectorPartitionSystemDatasetFileV1 `json:"files"`
}

func runVectorPartitionSystemExportDatasetV1(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("treedb_vector_partition_bench system-export-dataset", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var dataset, truthCache, truthSHA, out string
	var topK int
	fs.StringVar(&dataset, "dataset", "", "fixture manifest directory")
	fs.StringVar(&truthCache, "truth-cache", "", "trusted truth-cache directory")
	fs.StringVar(&truthSHA, "truth-cache-sha256", "", "trusted truth-cache artifact SHA256")
	fs.StringVar(&out, "out", "", "fresh exported dataset directory")
	fs.IntVar(&topK, "top-k", 10, "neighbors per query")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || dataset == "" || truthCache == "" || !m8SHA256V1(truthSHA) || out == "" || topK != 10 {
		return errors.New("system-export-dataset requires dataset, truth cache and SHA256, fresh output, top-k 10, and no positional arguments")
	}
	fixture, err := loadFixture(dataset)
	if err != nil {
		return err
	}
	if err := validateM3FixtureWithCaps(fixture, maxVectors, maxFixtureBytes); err != nil {
		return err
	}
	vectors, queries := fixtureData(fixture)
	if fixtureChecksumFromData(vectors, queries) != fixture.Checksum {
		return errors.New("fixture checksum does not match generated vector/query/truth stream")
	}
	truthPath := m8TruthCacheArtifactPathV1(truthCache, m8TruthCacheIdentityV1(fixture, topK))
	truth, artifactSHA, err := m8ReadTruthCacheV1(truthPath, fixture, len(queries), topK, uint64(fixture.Vectors), truthSHA)
	if err != nil {
		return fmt.Errorf("system-export-dataset truth: %w", err)
	}
	truthContentSHA, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		return err
	}
	out, err = filepath.Abs(filepath.Clean(out))
	if err != nil {
		return err
	}
	if err := os.Mkdir(out, 0o755); err != nil {
		return fmt.Errorf("create fresh export directory: %w", err)
	}
	files := make(map[string]vectorPartitionSystemDatasetFileV1)
	if err := writeVectorPartitionSystemFloat32RowsV1(filepath.Join(out, "documents.f32"), vectors, files); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemFloat32RowsV1(filepath.Join(out, "queries.f32"), queries, files); err != nil {
		return err
	}
	if err := writeVectorPartitionSystemTruthJSONLV1(filepath.Join(out, "exact_truth.jsonl"), truth, files); err != nil {
		return err
	}
	manifest := vectorPartitionSystemDatasetManifestV1{
		Version: 1, Generator: fixture.Generator, Docs: fixture.Vectors, Dimensions: fixture.Dimensions, Queries: fixture.Queries,
		TopK: topK, Metric: fixture.Metric, Normalized: true, DocumentIDPattern: "doc-%06d", DocumentVectorsFile: "documents.f32",
		QueryVectorsFile: "queries.f32", FloatFormat: "float32_le_row_major", ExactTruthFile: "exact_truth.jsonl", ExactTruthQueries: len(truth),
		FixtureChecksum: fixture.Checksum, TruthIdentity: m8TruthCacheIdentityV1(fixture, topK), TruthArtifactSHA256: artifactSHA,
		TruthSHA256: truthContentSHA, Files: files,
	}
	manifestPath := filepath.Join(out, "manifest.json")
	f, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "dataset=%s manifest=%s fixture_checksum=%s truth_sha256=%s\n", out, manifestPath, fixture.Checksum, truthContentSHA)
	return err
}

func writeVectorPartitionSystemFloat32RowsV1(path string, rows [][]float64, files map[string]vectorPartitionSystemDatasetFileV1) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	h := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(f, h), 1<<20)
	var raw [4]byte
	for _, row := range rows {
		for _, value := range row {
			binary.LittleEndian.PutUint32(raw[:], math.Float32bits(float32(value)))
			if _, err := w.Write(raw[:]); err != nil {
				_ = f.Close()
				return err
			}
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	info, err := f.Stat()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	files[filepath.Base(path)] = vectorPartitionSystemDatasetFileV1{Bytes: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}
	return nil
}

func writeVectorPartitionSystemTruthJSONLV1(path string, truth [][]m8CanonicalResultV1, files map[string]vectorPartitionSystemDatasetFileV1) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	h := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(f, h), 1<<20)
	encoder := json.NewEncoder(w)
	for query, row := range truth {
		ids := make([]string, len(row))
		for i := range row {
			ids[i] = row[i].ID
		}
		if err := encoder.Encode(struct {
			QueryID     string   `json:"query_id"`
			DocumentIDs []string `json:"document_ids"`
		}{QueryID: fmt.Sprintf("query-%06d", query), DocumentIDs: ids}); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	info, err := f.Stat()
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	files[filepath.Base(path)] = vectorPartitionSystemDatasetFileV1{Bytes: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))}
	return nil
}
