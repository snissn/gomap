package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestLocalHNSWAttributionInputsV1(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	digest := func(path string) string {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		return hex.EncodeToString(sum[:])
	}
	truth := write("truth", "sealed truth bytes")
	fixture := fixtureManifest{Checksum: strings.Repeat("a", 64), Queries: 256}
	calibration, holdout := localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: fixture.Checksum, TruthArtifactSHA256: digest(truth), Selection: localHNSWQuerySplitSelectionV1}, localHNSWQuerySplitV1{Schema: "vector_partition_4105_query_split_v1", DatasetChecksum: fixture.Checksum, TruthArtifactSHA256: digest(truth), Selection: localHNSWQuerySplitSelectionV1}
	for i := 0; i < fixture.Queries; i++ {
		if localHNSWCalibrationOrdinalV1(i) {
			calibration.Ordinals = append(calibration.Ordinals, i)
		} else {
			holdout.Ordinals = append(holdout.Ordinals, i)
		}
	}
	marshal := func(name string, value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return write(name, string(raw))
	}
	calibrationPath, holdoutPath := marshal("calibration.json", calibration), marshal("holdout.json", holdout)
	descriptor := write("descriptor", "descriptor")
	var reports [3]string
	for i := range reports {
		reports[i] = write("report"+strconv.Itoa(i), "report")
	}
	db := filepath.Join(dir, "db")
	if err := os.Mkdir(db, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := localHNSWAttributionInputConfigV1{Fixture: fixture, RetainedDB: db, Descriptor: descriptor, CalibrationSplit: calibrationPath, HoldoutSplit: holdoutPath, TruthArtifact: truth, HistoricalSearchReports: reports, DescriptorSHA256: digest(descriptor), CalibrationSplitSHA256: digest(calibrationPath), HoldoutSplitSHA256: digest(holdoutPath), TruthArtifactSHA256: digest(truth)}
	for i := range reports {
		cfg.HistoricalReportSHA256[i] = digest(reports[i])
	}
	if _, err := localHNSWAttributionInputsV1(cfg); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*localHNSWAttributionInputConfigV1)
	}{
		{"wrong digest", func(c *localHNSWAttributionInputConfigV1) { c.HistoricalReportSHA256[0] = strings.Repeat("b", 64) }},
		{"retained DB file", func(c *localHNSWAttributionInputConfigV1) { c.RetainedDB = descriptor }},
		{"nonregular", func(c *localHNSWAttributionInputConfigV1) { c.TruthArtifact = db }},
		{"oversize", func(c *localHNSWAttributionInputConfigV1) {
			c.Descriptor = write("oversized", strings.Repeat("x", m3VariantDescriptorMaxBytesV1+1))
			c.DescriptorSHA256 = strings.Repeat("c", 64)
		}},
		{"split digest", func(c *localHNSWAttributionInputConfigV1) { c.CalibrationSplitSHA256 = strings.Repeat("d", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := cfg
			tc.mutate(&got)
			if _, err := localHNSWAttributionInputsV1(got); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	dbLink := filepath.Join(dir, "db-link")
	if err := os.Symlink(db, dbLink); err != nil {
		t.Fatal(err)
	}
	cfg.RetainedDB = dbLink
	if _, err := localHNSWAttributionInputsV1(cfg); err == nil {
		t.Fatal("retained DB symlink accepted")
	}
	cfg.RetainedDB = db
	symlink := filepath.Join(dir, "descriptor-link")
	if err := os.Symlink(descriptor, symlink); err != nil {
		t.Fatal(err)
	}
	cfg.Descriptor = symlink
	if _, err := localHNSWAttributionInputsV1(cfg); err == nil {
		t.Fatal("symlink accepted")
	}
}
