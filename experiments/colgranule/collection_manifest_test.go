package colgranule

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestColumnCollectionManifestReopensAndPartSetQueries(t *testing.T) {
	ds := syntheticJSONBenchDataset(256)
	opts, err := JSONBenchColumnPartOptions(ds, 32)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry1 := publishJSONBenchPartRows(t, workspace, opts, ds, 101, 0, 128)
	entry2 := publishJSONBenchPartRows(t, workspace, opts, ds, 102, 128, 256)
	manifest, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{
		NewColumnManifestPartRef(ColumnPartRoleBase, 1, entry1),
		NewColumnManifestPartRef(ColumnPartRoleBase, 2, entry2),
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	if err := workspace.SaveCollectionManifest(manifest); err != nil {
		t.Fatalf("SaveCollectionManifest: %v", err)
	}
	if err := workspace.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("reopen workspace: %v", err)
	}
	defer reopened.Close()
	reopenedManifest, err := reopened.LoadCollectionManifest()
	if err != nil {
		t.Fatalf("LoadCollectionManifest: %v", err)
	}
	if reopenedManifest.Collection != "jsonbench" || len(reopenedManifest.PartSet.BaseParts) != 2 || reopenedManifest.ActiveGeneration != 2 {
		t.Fatalf("bad reopened collection manifest=%+v", reopenedManifest)
	}
	reader, err := OpenColumnPartSetReader(reopened, reopenedManifest, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("OpenColumnPartSetReader: %v", err)
	}
	if stats := reader.VisibilityStats(); stats.VisibleRows != ds.Rows || stats.BaseParts != 2 || stats.DeltaParts != 0 {
		t.Fatalf("visibility stats=%+v want rows=%d base=2 delta=0", stats, ds.Rows)
	}
	assertJSONBenchPartSetQueriesMatchRaw(t, reader, ds)
}

func TestColumnCollectionManifestRejectsUnknownVersionAndChecksumMismatch(t *testing.T) {
	ds := syntheticJSONBenchDataset(64)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	entry := publishJSONBenchPartRows(t, workspace, opts, ds, 201, 0, ds.Rows)
	manifest, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{
		NewColumnManifestPartRef(ColumnPartRoleBase, 1, entry),
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewColumnCollectionManifest: %v", err)
	}
	if err := workspace.SaveCollectionManifest(manifest); err != nil {
		t.Fatalf("SaveCollectionManifest: %v", err)
	}
	path := filepath.Join(ColumnWorkspaceNamespaceForDir(dir).ManifestDir, columnCollectionManifestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	corrupt := append([]byte(nil), data...)
	binary.LittleEndian.PutUint16(corrupt[4:], columnCollectionManifestBinaryVersion+1)
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile version: %v", err)
	}
	if _, err := workspace.LoadCollectionManifest(); err == nil || !strings.Contains(err.Error(), "unsupported collection manifest binary version") {
		t.Fatalf("LoadCollectionManifest version err=%v want unsupported version", err)
	}

	corrupt = append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 0xff
	if err := os.WriteFile(path, corrupt, 0o644); err != nil {
		t.Fatalf("WriteFile checksum: %v", err)
	}
	if _, err := workspace.LoadCollectionManifest(); err == nil || !strings.Contains(err.Error(), "collection manifest binary checksum") {
		t.Fatalf("LoadCollectionManifest checksum err=%v want checksum mismatch", err)
	}
}

func TestColumnCollectionManifestBinaryEncodeDecodeAndLegacyFallback(t *testing.T) {
	manifest := syntheticColumnCollectionManifestForBenchmark(2)
	payload, err := EncodeColumnCollectionManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeColumnCollectionManifest: %v", err)
	}
	if !bytes.HasPrefix(payload, []byte(columnCollectionManifestBinaryMagic)) {
		t.Fatalf("manifest payload magic=%q want binary %q", payload[:min(len(payload), 4)], columnCollectionManifestBinaryMagic)
	}
	if binary.LittleEndian.Uint16(payload[4:]) != columnCollectionManifestBinaryVersion {
		t.Fatalf("manifest binary version=%d want %d", binary.LittleEndian.Uint16(payload[4:]), columnCollectionManifestBinaryVersion)
	}
	decoded, err := DecodeColumnCollectionManifest(payload)
	if err != nil {
		t.Fatalf("DecodeColumnCollectionManifest binary: %v", err)
	}
	if decoded.ActiveGeneration != manifest.ActiveGeneration || len(decoded.PartSet.BaseParts) != len(manifest.PartSet.BaseParts) {
		t.Fatalf("decoded binary manifest generation/parts=(%d,%d) want (%d,%d)", decoded.ActiveGeneration, len(decoded.PartSet.BaseParts), manifest.ActiveGeneration, len(manifest.PartSet.BaseParts))
	}

	legacy, err := encodeColumnCollectionManifestJSONEnvelope(manifest)
	if err != nil {
		t.Fatalf("encode legacy manifest: %v", err)
	}
	decoded, err = DecodeColumnCollectionManifest(legacy)
	if err != nil {
		t.Fatalf("DecodeColumnCollectionManifest legacy: %v", err)
	}
	if decoded.ActiveGeneration != manifest.ActiveGeneration || len(decoded.PartSet.BaseParts) != len(manifest.PartSet.BaseParts) {
		t.Fatalf("decoded legacy manifest generation/parts=(%d,%d) want (%d,%d)", decoded.ActiveGeneration, len(decoded.PartSet.BaseParts), manifest.ActiveGeneration, len(manifest.PartSet.BaseParts))
	}
}

func TestColumnCollectionManifestBinaryRejectsHeaderAndChecksumCorruption(t *testing.T) {
	manifest := syntheticColumnCollectionManifestForBenchmark(2)
	payload, err := EncodeColumnCollectionManifest(manifest)
	if err != nil {
		t.Fatalf("EncodeColumnCollectionManifest: %v", err)
	}
	badVersion := append([]byte(nil), payload...)
	binary.LittleEndian.PutUint16(badVersion[4:], columnCollectionManifestBinaryVersion+1)
	if _, err := DecodeColumnCollectionManifest(badVersion); err == nil || !strings.Contains(err.Error(), "unsupported collection manifest binary version") {
		t.Fatalf("DecodeColumnCollectionManifest version err=%v want unsupported binary version", err)
	}

	badChecksum := append([]byte(nil), payload...)
	badChecksum[len(badChecksum)-1] ^= 0xff
	if _, err := DecodeColumnCollectionManifest(badChecksum); err == nil || !strings.Contains(err.Error(), "collection manifest binary checksum") {
		t.Fatalf("DecodeColumnCollectionManifest checksum err=%v want binary checksum mismatch", err)
	}
}

func TestColumnCollectionManifestPartCoverageDescriptor(t *testing.T) {
	ds := syntheticJSONBenchDataset(64)
	opts, err := JSONBenchColumnPartOptions(ds, 16)
	if err != nil {
		t.Fatalf("JSONBenchColumnPartOptions: %v", err)
	}
	dir := t.TempDir()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "jsonbench"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()
	entry := publishJSONBenchPartRows(t, workspace, opts, ds, 301, 0, ds.Rows)
	ref := NewColumnManifestPartRefWithCoverage(ColumnPartRoleBase, 7, entry, []ColumnSourcePartGeneration{{PartID: 11, GenerationID: 3}}, 2)
	if ref.Coverage.Role != ColumnPartRoleBase || ref.Coverage.GenerationID != 7 || ref.Coverage.CompactionLevel != 2 {
		t.Fatalf("bad coverage identity=%+v", ref.Coverage)
	}
	if len(ref.Coverage.SourceParts) != 1 || ref.Coverage.SourceParts[0].PartID != 11 || ref.Coverage.SourceParts[0].GenerationID != 3 {
		t.Fatalf("bad coverage sources=%+v", ref.Coverage.SourceParts)
	}
	if got, want := ref.Coverage.AssetRefs[0], entry.AssetRef; got != want {
		t.Fatalf("coverage asset ref=%+v want %+v", got, want)
	}
	if len(ref.Coverage.SortKeyColumns) == 0 || len(ref.Coverage.SortKeyLower) == 0 {
		t.Fatalf("coverage missing sort key bounds=%+v", ref.Coverage)
	}
	if ref.Coverage.Rows != entry.Rows || ref.Coverage.VisibleRows != entry.VisibleRows || ref.Coverage.DeletedRows != entry.Rows-entry.VisibleRows {
		t.Fatalf("coverage rows=%+v entry rows=%d visible=%d", ref.Coverage, entry.Rows, entry.VisibleRows)
	}
	deltaRef := NewColumnManifestPartRefWithCoverageOptions(ColumnPartRoleDelta, 8, entry, ColumnPartCoverageOptions{
		SourceRowRootGeneration: 44,
		SourceRowVersionLower:   120,
		SourceRowVersionUpper:   121,
	})
	if deltaRef.Coverage.SourceRowRootGeneration != 44 || deltaRef.Coverage.SourceRowVersionLower != 120 || deltaRef.Coverage.SourceRowVersionUpper != 121 {
		t.Fatalf("bad source row/root metadata=%+v", deltaRef.Coverage)
	}
	if _, err := NewColumnCollectionManifest("jsonbench", opts, nil, []ColumnManifestPartRef{deltaRef}, nil); err != nil {
		t.Fatalf("NewColumnCollectionManifest with delta source metadata: %v", err)
	}
	badDelta := deltaRef
	badDelta.Coverage.SourceRowVersionUpper = badDelta.Coverage.SourceRowVersionLower
	if _, err := NewColumnCollectionManifest("jsonbench", opts, nil, []ColumnManifestPartRef{badDelta}, nil); err == nil || !strings.Contains(err.Error(), "source row version") {
		t.Fatalf("NewColumnCollectionManifest err=%v want source row version rejection", err)
	}
	if _, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{ref}, nil, nil); err != nil {
		t.Fatalf("NewColumnCollectionManifest with coverage: %v", err)
	}
	ref.Coverage.Checksums[0]++
	if _, err := NewColumnCollectionManifest("jsonbench", opts, []ColumnManifestPartRef{ref}, nil, nil); err == nil || !strings.Contains(err.Error(), "checksums") {
		t.Fatalf("NewColumnCollectionManifest err=%v want coverage checksum rejection", err)
	}
}

func BenchmarkColumnCollectionManifestDecode10K(b *testing.B) {
	manifest := syntheticColumnCollectionManifestForBenchmark(10_000)
	payload, err := EncodeColumnCollectionManifest(manifest)
	if err != nil {
		b.Fatalf("EncodeColumnCollectionManifest: %v", err)
	}
	b.ReportMetric(float64(len(payload)), "manifest_bytes")
	b.ReportMetric(float64(len(manifest.PartSet.BaseParts)), "parts")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeColumnCollectionManifest(payload); err != nil {
			b.Fatalf("DecodeColumnCollectionManifest: %v", err)
		}
	}
}

func BenchmarkColumnCollectionManifestViewDecode10K(b *testing.B) {
	manifest := syntheticColumnCollectionManifestForBenchmark(10_000)
	payload, err := EncodeColumnCollectionManifest(manifest)
	if err != nil {
		b.Fatalf("EncodeColumnCollectionManifest: %v", err)
	}
	view, err := DecodeColumnCollectionManifestView(payload)
	if err != nil {
		b.Fatalf("DecodeColumnCollectionManifestView: %v", err)
	}
	if view.BodyBytes() == 0 {
		b.Fatalf("empty manifest view")
	}
	b.ReportMetric(float64(len(payload)), "manifest_bytes")
	b.ReportMetric(float64(len(manifest.PartSet.BaseParts)), "parts")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view, err := DecodeColumnCollectionManifestView(payload)
		if err != nil {
			b.Fatalf("DecodeColumnCollectionManifestView: %v", err)
		}
		benchSink += int64(view.BodyBytes())
	}
}

func publishJSONBenchPartRows(t testing.TB, workspace *ColumnWorkspace, opts ColumnStoreOptions, ds JSONBenchDataset, partID uint64, start int, end int) ColumnWorkspacePartManifest {
	t.Helper()
	part, err := BuildColumnPart(partID, opts, ColumnBatch{Rows: end - start, Columns: sliceJSONBenchColumns(ds, start, end)})
	if err != nil {
		t.Fatalf("BuildColumnPart(%d,%d:%d): %v", partID, start, end, err)
	}
	entry, err := workspace.PublishPart(part, ds.Dictionaries)
	if err != nil {
		t.Fatalf("PublishPart(%d): %v", partID, err)
	}
	return entry
}

func sliceJSONBenchColumns(ds JSONBenchDataset, start int, end int) map[string][]int64 {
	out := make(map[string][]int64, len(ds.Columns))
	for name, values := range ds.Columns {
		out[name] = append([]int64(nil), values[start:end]...)
	}
	return out
}

func assertJSONBenchPartSetQueriesMatchRaw(t *testing.T, reader *ColumnPartSetReader, ds JSONBenchDataset) {
	t.Helper()
	codes, err := jsonBenchQueryCodes(ds)
	if err != nil {
		t.Fatalf("jsonBenchQueryCodes: %v", err)
	}
	raw := map[string]struct {
		rows   int
		digest uint64
	}{
		"Q1": {},
		"Q2": {},
		"Q3": {},
		"Q4": {},
		"Q5": {},
	}
	raw["Q1"] = rowsDigest(runJSONBenchQ1(ds, codes))
	raw["Q2"] = rowsDigest(runJSONBenchQ2(ds, codes))
	raw["Q3"] = rowsDigest(runJSONBenchQ3(ds, codes))
	raw["Q4"] = rowsDigest(runJSONBenchQ4(ds, codes))
	raw["Q5"] = rowsDigest(runJSONBenchQ5(ds, codes))
	timings, err := RunJSONBenchColumnPartSetQueries(reader, ds, 2)
	if err != nil {
		t.Fatalf("RunJSONBenchColumnPartSetQueries: %v", err)
	}
	if len(timings) != 5 {
		t.Fatalf("part-set timings=%d want 5", len(timings))
	}
	for _, timing := range timings {
		want, ok := raw[timing.Query]
		if !ok {
			t.Fatalf("unexpected part-set query %s", timing.Query)
		}
		if timing.Engine != "encoded_column_part_set" {
			t.Fatalf("%s engine=%q want encoded_column_part_set", timing.Query, timing.Engine)
		}
		if timing.ResultRows != want.rows || timing.ResultDigest != want.digest {
			t.Fatalf("%s rows/digest=(%d,%d) raw=(%d,%d)", timing.Query, timing.ResultRows, timing.ResultDigest, want.rows, want.digest)
		}
		if len(timing.Attempts) != 2 || timing.Attempts[0].Cache != "cold" || timing.Attempts[1].Cache != "warm" {
			t.Fatalf("%s attempts=%+v want cold,warm", timing.Query, timing.Attempts)
		}
		if timing.Diagnostics.PartsConsidered == 0 || timing.Diagnostics.RowsReturned != ds.Rows {
			t.Fatalf("%s diagnostics=%+v want part-set rows=%d", timing.Query, timing.Diagnostics, ds.Rows)
		}
	}
}

func rowsDigest(rows int, digest uint64) struct {
	rows   int
	digest uint64
} {
	return struct {
		rows   int
		digest uint64
	}{rows: rows, digest: digest}
}

func syntheticColumnCollectionManifestForBenchmark(parts int) ColumnCollectionManifest {
	ref := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, FileID: 1, Offset: 1, Length: 16, Checksum: 7}
	tcs1 := TCS1PartRecord{Version: tcs1Version, Kind: tcs1PartImageKind, PartID: 1, Rows: 1, ImageVersion: columnPartImageVersion, PayloadBytes: 8, TotalBytes: 16, PayloadCRC32: 99, AssetRef: ref}
	base := make([]ColumnManifestPartRef, parts)
	for i := range base {
		partID := uint64(i + 1)
		partRef := ref
		partRef.Offset = int64(i + 1)
		partRef.Checksum = uint32(i + 7)
		record := tcs1
		record.PartID = partID
		record.AssetRef = partRef
		base[i] = NewColumnManifestPartRef(ColumnPartRoleBase, uint64(i+1), ColumnWorkspacePartManifest{
			PartID:        partID,
			Rows:          1,
			VisibleRows:   1,
			SchemaVersion: 1,
			SortKey:       []SortKeyColumn{{Column: "id"}},
			AssetRef:      partRef,
			TCS1:          record,
			ImageBytes:    8,
			ManifestBytes: 4,
			Sections:      1,
			AssetBytes:    16,
			PublishedUnix: int64(i + 1),
		})
	}
	opts := partTestOptions([]SortKeyColumn{{Column: "id"}})
	manifest, err := NewColumnCollectionManifest("bench", opts, base, nil, nil)
	if err != nil {
		panic(err)
	}
	return manifest
}
