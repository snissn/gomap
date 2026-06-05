package collections

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestAuditColumnRetainedPayloadPathsAbsentJSONBenchPaths2354(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Columns: []ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString},
			{Name: "did", Path: "did", ValueType: ColumnStoreValueString},
			{Name: "operation", Path: "commit.operation", ValueType: ColumnStoreValueString},
			{Name: "collection", Path: "commit.collection", ValueType: ColumnStoreValueString},
		},
	}
	retained, err := columnRetainedPayloadFromJSONDocument(cfg, []byte(`{"time_us":1,"kind":"commit","did":"did:plc:abc","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r1"},"payload":"kept"}`))
	if err != nil {
		t.Fatalf("columnRetainedPayloadFromJSONDocument: %v", err)
	}
	audit, err := AuditColumnRetainedPayloadPathsAbsent(cfg, retained, []string{
		"time_us",
		"kind",
		"did",
		"commit.operation",
		"commit.collection",
	})
	if err != nil {
		t.Fatalf("AuditColumnRetainedPayloadPathsAbsent: %v audit=%+v retained=%s", err, audit, retained)
	}
	if audit.RetainedPayloadEncoding != string(ColumnRetainedPayloadEncodingTemplateV1) || audit.RetainedPayloadEncodingStatus == "" {
		t.Fatalf("unexpected encoding status: %+v", audit)
	}
	if len(audit.Paths) != 5 {
		t.Fatalf("paths=%d want 5 audit=%+v", len(audit.Paths), audit)
	}
	for _, path := range audit.Paths {
		if path.Present {
			t.Fatalf("path %q remained in retained payload: audit=%+v retained=%s", path.Path, audit, retained)
		}
	}
}

func TestAuditColumnRetainedPayloadPathsAbsentFailsClosed2354(t *testing.T) {
	cfg := ColumnStoreConfig{Enabled: true, RetainedPayload: ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON}
	audit, err := AuditColumnRetainedPayloadPathsAbsent(cfg, []byte(`{"commit":{"operation":"create"},"payload":"kept"}`), []string{"commit.operation"})
	if err == nil || !strings.Contains(err.Error(), "commit.operation") {
		t.Fatalf("audit err=%v want commit.operation violation audit=%+v", err, audit)
	}
	if len(audit.Violations) != 1 || audit.Violations[0] != "commit.operation" {
		t.Fatalf("violations=%v want commit.operation", audit.Violations)
	}

	if _, err := AuditColumnRetainedPayloadPathsAbsent(cfg, []byte(`{"unterminated"`), []string{"kind"}); err == nil || !strings.Contains(err.Error(), "invalid JSON") {
		t.Fatalf("malformed retained audit err=%v want invalid JSON failure", err)
	}
}

func TestColumnRetainedPayloadAuditDisabledDoesNotDefaultFull2354(t *testing.T) {
	cfg := ColumnStoreConfig{}
	encoding, status := ColumnRetainedPayloadEncodingStatus(&cfg)
	if encoding != ColumnRetainedPayloadEncodingNone || status != "not_configured" {
		t.Fatalf("encoding/status=%q/%q want none/not_configured", encoding, status)
	}

	audit, err := AuditColumnRetainedPayloadPathsAbsent(cfg, []byte(`{}`), []string{"kind"})
	if err != nil {
		t.Fatalf("AuditColumnRetainedPayloadPathsAbsent disabled config: %v audit=%+v", err, audit)
	}
	if audit.RetainedPayloadPolicy != "" {
		t.Fatalf("disabled audit policy=%q want empty policy", audit.RetainedPayloadPolicy)
	}
	if audit.RetainedPayloadEncoding != ColumnRetainedPayloadEncodingNone || audit.RetainedPayloadEncodingStatus != "not_configured" {
		t.Fatalf("disabled audit encoding/status=%q/%q want none/not_configured", audit.RetainedPayloadEncoding, audit.RetainedPayloadEncodingStatus)
	}
}

func TestColumnRetainedPayloadCompressionStatus2356(t *testing.T) {
	cases := []struct {
		name            string
		cfg             *ColumnStoreConfig
		wantCompression string
		wantPolicy      string
		wantStatus      string
	}{
		{
			name:            "disabled",
			cfg:             &ColumnStoreConfig{},
			wantCompression: "none",
			wantPolicy:      "not_configured",
			wantStatus:      "not_configured",
		},
		{
			name: "none",
			cfg: &ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: ColumnRetainedPayloadNone,
			},
			wantCompression: "none",
			wantPolicy:      "none",
			wantStatus:      "inactive_no_retained_payload",
		},
		{
			name: "full",
			cfg: &ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: ColumnRetainedPayloadFull,
			},
			wantCompression: "value_log_grouped_frame",
			wantPolicy:      "default_value_log_auto",
			wantStatus:      "active_value_log_auto_grouped_frame_full_retained_payload",
		},
		{
			name: "non_column",
			cfg: &ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: ColumnRetainedPayloadNonColumn,
			},
			wantCompression: "value_log_grouped_frame",
			wantPolicy:      "default_value_log_auto_storage_first",
			wantStatus:      "active_value_log_auto_grouped_frame_non_column_retained_payload",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compression, policy, status := ColumnRetainedPayloadCompressionStatus(tc.cfg)
			if compression != tc.wantCompression || policy != tc.wantPolicy || status != tc.wantStatus {
				t.Fatalf("compression/policy/status=%q/%q/%q want %q/%q/%q", compression, policy, status, tc.wantCompression, tc.wantPolicy, tc.wantStatus)
			}
		})
	}
}

func TestAuditCollectionRetainedPayloadDeclaredPathsAbsentTemplateV12382(t *testing.T) {
	col, closeDB := openRetainedPayloadAuditCollection2382(t, jsonbenchRetainedPayloadAuditConfig2382(true), [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r1"},"payload":"kept"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r2"},"nested":{"also":"kept"}}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" {
		t.Fatalf("status=%q want passed audit=%+v", audit.Status, audit)
	}
	if audit.Collection != "events" || audit.CheckedRows != 2 || audit.RetainedPayloadBytes <= 0 {
		t.Fatalf("unexpected collection/count/bytes audit=%+v", audit)
	}
	if audit.RetainedPayloadEncoding != string(ColumnRetainedPayloadEncodingTemplateV1) || audit.RetainedPayloadEncodingStatus != "active_template_v1_non_column_retained_payload" {
		t.Fatalf("unexpected retained encoding audit=%+v", audit)
	}
	if audit.RetainedPayloadCompression != "value_log_grouped_frame" || audit.RetainedPayloadCompressionStatus != "active_value_log_auto_grouped_frame_non_column_retained_payload" {
		t.Fatalf("unexpected retained compression audit=%+v", audit)
	}
	if strings.Join(audit.DeclaredPaths, ",") != "commit.collection,commit.operation,did,kind,time_us" {
		t.Fatalf("declared paths=%v", audit.DeclaredPaths)
	}
	if len(audit.Violations) != 0 || len(audit.Errors) != 0 {
		t.Fatalf("unexpected violations/errors audit=%+v", audit)
	}
}

func TestAuditCollectionRetainedPayloadDeclaredPathsAbsentFailsClosed2382(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(false)
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post"},"payload":"kept"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		Paths: []string{"time_us", "kind", "did", "commit.operation", "commit.collection"},
	})
	if err == nil || !strings.Contains(err.Error(), "commit.operation") {
		t.Fatalf("audit err=%v want commit.operation violation audit=%+v", err, audit)
	}
	if audit.Status != "failed" {
		t.Fatalf("status=%q want failed audit=%+v", audit.Status, audit)
	}
	if len(audit.Violations) != 1 || audit.Violations[0].DocumentID != "doc-000000" || audit.Violations[0].Path != "commit.operation" {
		t.Fatalf("violations=%+v want doc-000000 commit.operation", audit.Violations)
	}
	if audit.CheckedRows != 1 || len(audit.Errors) == 0 {
		t.Fatalf("unexpected checked/errors audit=%+v", audit)
	}
}

func TestAuditCollectionRetainedPayloadDeclaredPathsAbsentSampled2382(t *testing.T) {
	col, closeDB := openRetainedPayloadAuditCollection2382(t, jsonbenchRetainedPayloadAuditConfig2382(true), [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post"},"payload":"kept"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"create","collection":"app.bsky.feed.post"},"payload":"kept"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{MaxDocuments: 1})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent sampled: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed_sampled" || !audit.Truncated || audit.CheckedRows != 1 {
		t.Fatalf("sample audit=%+v want passed_sampled truncated one row", audit)
	}
}

func TestAuditCollectionRetainedPayloadDeclaredPathsAbsentValueReadErrorFailsClosed2384(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		ValueLog:                   backenddb.ValueLogOptions{PointerThreshold: 1},
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatJSON,
			ColumnStore:             jsonbenchRetainedPayloadAuditConfig2382(true),
			DataRootStoragePolicy:   RootStorageCompressed,
			IndexStateStoragePolicy: RootStorageCompressed,
		},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("doc-000000")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post"},"payload":"` + strings.Repeat("kept", 512) + `"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.ValueLogDirPath(dir), "value-l*.log"))
	if err != nil {
		t.Fatalf("glob value log: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("expected value-log segment for forced pointer retained payload")
	}
	for _, path := range paths {
		if err := os.Truncate(path, 0); err != nil {
			t.Fatalf("truncate value log %s: %v", path, err)
		}
	}
	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{})
	if err == nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent err=nil want value read failure audit=%+v", audit)
	}
	if audit.Status != "failed" || len(audit.Errors) == 0 {
		t.Fatalf("audit=%+v want failed status with error", audit)
	}
	if strings.Contains(strings.ToLower(audit.Errors[0]), "panic") {
		t.Fatalf("audit error still reports panic: %+v", audit)
	}
}

func jsonbenchRetainedPayloadAuditConfig2382(includeOperation bool) *ColumnStoreConfig {
	columns := []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "did", Path: "did", ValueType: ColumnStoreValueString, Dictionary: true},
		{Name: "collection", Path: "commit.collection", ValueType: ColumnStoreValueString, Dictionary: true},
	}
	if includeOperation {
		columns = append(columns, ColumnStoreColumn{Name: "operation", Path: "commit.operation", ValueType: ColumnStoreValueString, Dictionary: true})
	}
	return &ColumnStoreConfig{
		Enabled:         true,
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		Columns:         columns,
		SortKey:         []ColumnSortKey{{Column: "time_us"}},
	}
}

func openRetainedPayloadAuditCollection2382(t *testing.T, cfg *ColumnStoreConfig, docs [][]byte) (*Collection, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "events",
		Options: CollectionOptions{
			DocumentFormat:          DocumentFormatJSON,
			ColumnStore:             cfg,
			DataRootStoragePolicy:   RootStorageCompressed,
			IndexStateStoragePolicy: RootStorageCompressed,
		},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	ids := make([][]byte, len(docs))
	for i := range docs {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	return col, func() { _ = d.Close() }
}
