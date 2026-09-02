package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestAuditSummaryUsageExposesReadOnlyCommand(t *testing.T) {
	if !strings.Contains(usageText, "audit-summary") || !strings.Contains(usageText, "read-only") {
		t.Fatalf("usageText missing audit-summary read-only command: %q", usageText)
	}
}

func TestAuditSummaryJSONOutputShape(t *testing.T) {
	dir := buildAuditSummaryFixture(t)

	out := captureStdout(t, func() {
		runAuditSummary(dir, []string{
			"-json",
			"-frame-stats",
			"-frame-top-lengths", "4",
			"-top-segments", "3",
			"-top-generations", "3",
			"-gzip-samples", "3",
			"-gzip-sample-max-bytes", "4096",
		})
	})

	var report auditSummaryReport
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("decode audit-summary json: %v\n%s", err, out)
	}
	if report.SchemaVersion != auditSummarySchemaVersion {
		t.Fatalf("schema_version=%d want %d", report.SchemaVersion, auditSummarySchemaVersion)
	}
	if report.Command != "treemap audit-summary" {
		t.Fatalf("command=%q", report.Command)
	}
	if report.Dir != filepath.Clean(dir) || report.RootDir != filepath.Clean(dir) || report.MainDir != filepath.Join(filepath.Clean(dir), "maindb") {
		t.Fatalf("unexpected dirs: dir=%q root=%q main=%q", report.Dir, report.RootDir, report.MainDir)
	}
	if !report.Options.FrameStats || report.Options.GzipSamples != 3 || report.Options.GzipSampleMaxBytes != 4096 {
		t.Fatalf("unexpected options: %+v", report.Options)
	}
	assertAuditStorageDomain(t, report.Storage.Domains, "maindb", true)
	assertAuditStorageDomain(t, report.Storage.Domains, "index_db", true)
	assertAuditStorageDomain(t, report.Storage.Domains, "value_vlog", true)
	assertAuditStorageDomain(t, report.Storage.Domains, "leaf_vlog", true)

	if !report.CompactPlan.DryRun {
		t.Fatalf("compact plan must be dry-run: %+v", report.CompactPlan)
	}
	if report.ValueLog.RewritePlan.SegmentsTotal == 0 {
		t.Fatalf("expected value-log rewrite plan to see segments: %+v", report.ValueLog.RewritePlan)
	}
	if report.ValueLog.GCDryRun.SegmentsTotal == 0 {
		t.Fatalf("expected value-log GC dry-run to see segments: %+v", report.ValueLog.GCDryRun)
	}
	if report.LeafGeneration.GenerationsTotal == 0 {
		t.Fatalf("expected leaf-generation summary, got %+v", report.LeafGeneration)
	}
	if len(report.LogFamilies) != 2 {
		t.Fatalf("log family count=%d want 2", len(report.LogFamilies))
	}
	families := auditFamiliesByName(report.LogFamilies)
	for _, name := range []string{"value_vlog", "leaf_vlog"} {
		family := families[name]
		if family.Name == "" {
			t.Fatalf("missing log family %q in %+v", name, report.LogFamilies)
		}
		if !family.Exists || family.Segments == 0 || family.Bytes == 0 {
			t.Fatalf("family %s missing segment inventory: %+v", name, family)
		}
		if len(family.TopSegmentsByBytes) == 0 || len(family.TopSegmentsByBytes) > 3 {
			t.Fatalf("family %s top segment shape invalid: %+v", name, family.TopSegmentsByBytes)
		}
		if family.FrameScan == nil || family.FrameScan.RecordsTotal == 0 || family.FrameScan.StoredPayloadBytes == 0 {
			t.Fatalf("family %s frame scan missing: %+v", name, family.FrameScan)
		}
		if len(family.GzipSamples) == 0 || len(family.GzipSamples) > 3 {
			t.Fatalf("family %s gzip sample count invalid: %+v", name, family.GzipSamples)
		}
		for _, sample := range family.GzipSamples {
			if sample.InputBytes == 0 || sample.GzipBytes == 0 || sample.GzipRatio <= 0 {
				t.Fatalf("family %s invalid gzip sample: %+v", name, sample)
			}
			if sample.InputBytes > 4096 {
				t.Fatalf("family %s gzip sample ignored max bytes: %+v", name, sample)
			}
		}
	}
}

func TestAuditSummaryMainDBDirWithSiblingSideStoreUsesParentRoot(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")
	if err := os.Mkdir(mainDir, 0o755); err != nil {
		t.Fatalf("mkdir maindb: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "dictdb"), 0o755); err != nil {
		t.Fatalf("mkdir dictdb: %v", err)
	}
	if got := resolveTreemapRootDir(mainDir, mainDir); got != root {
		t.Fatalf("resolveTreemapRootDir(mainDir with sibling side store)=%q want %q", got, root)
	}
}

func TestAuditSummaryMainDBDirWithAncientSiblingUsesParentRoot(t *testing.T) {
	root := t.TempDir()
	opts := treedb.Options{
		Dir:        root,
		Durability: treedb.DurabilityWALOffRelaxed,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	}
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open root fixture: %v", err)
	}
	if err := db.Set([]byte("root-key"), bytes.Repeat([]byte("v"), 512)); err != nil {
		_ = db.Close()
		t.Fatalf("set root fixture: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint root fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close root fixture: %v", err)
	}
	_ = os.RemoveAll(filepath.Join(root, "dictdb"))
	_ = os.RemoveAll(filepath.Join(root, "templatedb"))
	ancientDir := filepath.Join(root, "ancient")
	if err := os.Mkdir(ancientDir, 0o755); err != nil {
		t.Fatalf("mkdir ancient sibling: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ancientDir, "freezer.dat"), bytes.Repeat([]byte("a"), 123), 0o644); err != nil {
		t.Fatalf("write ancient sibling file: %v", err)
	}

	mainDir := filepath.Join(root, "maindb")
	report, err := collectAuditSummary(mainDir, auditSummaryCollectOptions{CompactMode: treedbdb.CompactStorageFull})
	if err != nil {
		t.Fatalf("collectAuditSummary(root maindb): %v", err)
	}
	if report.RootDir != root {
		t.Fatalf("RootDir=%q want root %q", report.RootDir, root)
	}
	ancient := auditStorageDomainsByName(report.Storage.Domains)["ancient"]
	if !ancient.Exists || ancient.Path != ancientDir || ancient.Bytes != 123 {
		t.Fatalf("ancient domain not reported from root sibling: %+v", ancient)
	}
}

func TestAuditSummaryFlatMainDBDirNamedMainDBUsesInputAsRoot(t *testing.T) {
	parent := t.TempDir()
	flatDir := filepath.Join(parent, "maindb")
	opts := treedb.Options{
		Dir:               flatDir,
		DisableSideStores: true,
		Durability:        treedb.DurabilityWALOffRelaxed,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	}
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open flat fixture: %v", err)
	}
	if err := db.Set([]byte("flat-key"), bytes.Repeat([]byte("v"), 512)); err != nil {
		_ = db.Close()
		t.Fatalf("set flat fixture: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint flat fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close flat fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(parent, "unrelated.bin"), bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatalf("write unrelated sibling: %v", err)
	}

	report, err := collectAuditSummary(flatDir, auditSummaryCollectOptions{CompactMode: treedbdb.CompactStorageFull})
	if err != nil {
		t.Fatalf("collectAuditSummary(flat maindb): %v", err)
	}
	if report.RootDir != flatDir {
		t.Fatalf("RootDir=%q want flat dir %q", report.RootDir, flatDir)
	}
	rootDomain := auditStorageDomainsByName(report.Storage.Domains)["root"]
	expectedRoot, err := auditPathUsage("root", flatDir)
	if err != nil {
		t.Fatalf("auditPathUsage(flat root): %v", err)
	}
	if rootDomain.Path != flatDir || rootDomain.Bytes != expectedRoot.Bytes || rootDomain.Files != expectedRoot.Files {
		t.Fatalf("root domain includes wrong path/usage: got=%+v want=%+v", rootDomain, expectedRoot)
	}
}

func TestSummarizeCompactUsagesPreservesMissingDomainExists(t *testing.T) {
	dir := t.TempDir()
	existingEmpty := filepath.Join(dir, "empty")
	if err := os.Mkdir(existingEmpty, 0o755); err != nil {
		t.Fatalf("mkdir existing empty domain: %v", err)
	}
	missing := filepath.Join(dir, "missing")
	rows := summarizeCompactUsages([]treedbdb.CompactStorageUsage{
		{Name: "missing", Path: missing},
		{Name: "empty", Path: existingEmpty},
		{Name: "nonzero", Path: missing, Bytes: 7},
	})
	byName := auditStorageDomainsByName(rows)
	if byName["missing"].Exists {
		t.Fatalf("missing zero-valued domain reported exists: %+v", byName["missing"])
	}
	if !byName["empty"].Exists {
		t.Fatalf("existing empty domain reported missing: %+v", byName["empty"])
	}
	if !byName["nonzero"].Exists {
		t.Fatalf("nonzero usage domain reported missing: %+v", byName["nonzero"])
	}
}

func TestAuditSummaryMarkdownOutputShape(t *testing.T) {
	dir := buildAuditSummaryFixture(t)

	out := captureStdout(t, func() {
		runAuditSummary(dir, []string{
			"-top-segments", "2",
			"-top-generations", "2",
			"-gzip-samples", "1",
			"-gzip-sample-max-bytes", "1024",
		})
	})

	for _, want := range []string{
		"# TreeDB audit summary",
		"## Storage breakdown",
		"## Compact plan / current debt",
		"## Value-log audit summary",
		"## Leaf-generation audit summary",
		"## Log families",
		"### `value_vlog`",
		"### `leaf_vlog`",
		"Frame scan: omitted",
		"Gzip samples:",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("markdown output missing %q:\n%s", want, out)
		}
	}
}

func TestAuditSummaryReadOnlyDoesNotMutateFiles(t *testing.T) {
	dir := buildAuditSummaryFixture(t)
	before := snapshotRegularFiles(t, dir)

	_ = captureStdout(t, func() {
		runAuditSummary(dir, []string{
			"-json",
			"-frame-stats",
			"-gzip-samples", "2",
			"-gzip-sample-max-bytes", "2048",
		})
	})

	after := snapshotRegularFiles(t, dir)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("audit-summary mutated regular files:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestGzipAuditSamplesReportsRatiosAndRoles(t *testing.T) {
	dir := t.TempDir()
	files := map[string][]byte{
		"value-l0-000001.log": bytes.Repeat([]byte("a"), 256),
		"value-l0-000002.log": bytes.Repeat([]byte("b"), 1024),
		"value-l0-000003.log": bytes.Repeat([]byte("c"), 512),
	}
	for name, payload := range files {
		if err := os.WriteFile(filepath.Join(dir, name), payload, 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	segments, _, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("listValueLogSegments: %v", err)
	}
	samples, err := gzipAuditSamples(segments, 3, 128)
	if err != nil {
		t.Fatalf("gzipAuditSamples: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("samples=%d want 3: %+v", len(samples), samples)
	}
	roles := strings.Join([]string{samples[0].Sample, samples[1].Sample, samples[2].Sample}, ",")
	for _, want := range []string{"first", "last", "largest"} {
		if !strings.Contains(roles, want) {
			t.Fatalf("sample roles %q missing %q", roles, want)
		}
	}
	for _, sample := range samples {
		if sample.InputBytes == 0 || sample.InputBytes > 128 || sample.GzipBytes == 0 || sample.GzipRatio <= 0 {
			t.Fatalf("invalid gzip sample: %+v", sample)
		}
		if sample.FileBytes > 128 && !sample.Truncated {
			t.Fatalf("expected truncated sample for %+v", sample)
		}
	}
}

func buildAuditSummaryFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	opts := treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	}
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1
	opts.BackgroundIndexVacuumInterval = -1
	opts.MaxWALBytes = -1

	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	for i := 0; i < 768; i++ {
		key := []byte(fmt.Sprintf("audit-key-%06d", i))
		val := bytes.Repeat([]byte{byte(i % 251)}, 512)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set fixture key %d: %v", i, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture: %v", err)
	}
	assertLogFamilyHasData(t, filepath.Join(dir, "maindb", "value_vlog"))
	assertLogFamilyHasData(t, filepath.Join(dir, "maindb", "leaf_vlog"))
	return dir
}

func assertLogFamilyHasData(t *testing.T, dir string) {
	t.Helper()
	segments, bytesOnDisk, err := listValueLogSegments(dir)
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	if len(segments) == 0 || bytesOnDisk == 0 {
		t.Fatalf("expected log family data under %s, segments=%d bytes=%d", dir, len(segments), bytesOnDisk)
	}
}

func assertAuditStorageDomain(t *testing.T, domains []auditSummaryStorageDomain, name string, wantBytes bool) {
	t.Helper()
	for _, domain := range domains {
		if domain.Name != name {
			continue
		}
		if !domain.Exists {
			t.Fatalf("storage domain %s does not exist: %+v", name, domain)
		}
		if wantBytes && domain.Bytes == 0 {
			t.Fatalf("storage domain %s has zero bytes: %+v", name, domain)
		}
		return
	}
	t.Fatalf("missing storage domain %s in %+v", name, domains)
}

func auditFamiliesByName(families []auditSummaryLogFamily) map[string]auditSummaryLogFamily {
	out := make(map[string]auditSummaryLogFamily, len(families))
	for _, family := range families {
		out[family.Name] = family
	}
	return out
}

func auditStorageDomainsByName(domains []auditSummaryStorageDomain) map[string]auditSummaryStorageDomain {
	out := make(map[string]auditSummaryStorageDomain, len(domains))
	for _, domain := range domains {
		out[domain.Name] = domain
	}
	return out
}

type fileSnapshot struct {
	Size   int64
	SHA256 string
}

func snapshotRegularFiles(t *testing.T, root string) map[string]fileSnapshot {
	t.Helper()
	out := make(map[string]fileSnapshot)
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(payload)
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = fileSnapshot{Size: info.Size(), SHA256: hex.EncodeToString(sum[:])}
		return nil
	}); err != nil {
		t.Fatalf("snapshot files under %s: %v", root, err)
	}
	return out
}
