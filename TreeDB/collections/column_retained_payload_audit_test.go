package collections

import (
	"strings"
	"testing"
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
