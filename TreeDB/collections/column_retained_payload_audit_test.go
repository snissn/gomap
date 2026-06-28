package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
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
	if audit.RetainedPayloadEncoding != string(ColumnRetainedPayloadEncodingSemanticStreamV1) || audit.RetainedPayloadEncodingStatus != "active_semantic_stream_v1_non_column_retained_payload" {
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
			name: "non_column_default_semantic_stream",
			cfg: &ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: ColumnRetainedPayloadNonColumn,
			},
			wantCompression: "semantic_stream_v1_blocks",
			wantPolicy:      "retained_semantic_stream_v1_side_root",
			wantStatus:      "active_semantic_stream_v1_non_column_retained_payload",
		},
		{
			name: "non_column_explicit_template_v1",
			cfg: &ColumnStoreConfig{
				Enabled:                 true,
				RetainedPayload:         ColumnRetainedPayloadNonColumn,
				RetainedPayloadEncoding: ColumnRetainedPayloadEncodingTemplateV1,
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
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingTemplateV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
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

func TestAuditCollectionRetainedPayloadDeclaredPathsAbsentSemanticStreamV12662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r1"},"payload":"kept"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r2"},"payload":"also kept"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent semantic-stream-v1: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 2 {
		t.Fatalf("audit=%+v want passed two rows", audit)
	}
	if audit.RetainedPayloadEncoding != string(ColumnRetainedPayloadEncodingSemanticStreamV1) || audit.RetainedPayloadEncodingStatus != "active_semantic_stream_v1_non_column_retained_payload" {
		t.Fatalf("unexpected retained encoding audit=%+v", audit)
	}
	if audit.RetainedPayloadCompression != "semantic_stream_v1_blocks" || audit.RetainedPayloadCompressionStatus != "active_semantic_stream_v1_non_column_retained_payload" {
		t.Fatalf("unexpected retained compression audit=%+v", audit)
	}

	statsAudit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeShapeStats:      true,
		IncludeSemanticStreams: true,
		SemanticStreamMaxDepth: 8,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent semantic-stream-v1 stats: %v audit=%+v", err, statsAudit)
	}
	if statsAudit.Status != "passed" || len(statsAudit.RetainedPayloadShape) == 0 || len(statsAudit.RetainedPayloadSemanticStreams) == 0 {
		t.Fatalf("stats audit missing decoded retained data: %+v", statsAudit)
	}
	if _, ok := retainedPayloadShapeStat2382(statsAudit.RetainedPayloadShape, "payload", "string"); !ok {
		t.Fatalf("semantic-stream-v1 retained payload shape missing payload: %+v", statsAudit.RetainedPayloadShape)
	}
	if _, ok := retainedPayloadSemanticStreamStat2662(statsAudit.RetainedPayloadSemanticStreams, "payload", "string"); !ok {
		t.Fatalf("semantic-stream-v1 retained semantic stream missing payload: %+v", statsAudit.RetainedPayloadSemanticStreams)
	}
}

func TestAuditCollectionRetainedPayloadShapeStatsTemplateV12662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingTemplateV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r1"},"payload":"kept","nested":{"also":"kept","count":3}}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r2"},"payload":"second","nested":{"also":"two","flag":true}}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeShapeStats: true,
		ShapeMaxDepth:     8,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent shape stats: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 2 {
		t.Fatalf("audit=%+v want passed two rows", audit)
	}
	if len(audit.RetainedPayloadShape) == 0 || audit.RetainedPayloadShapeTruncated {
		t.Fatalf("shape stats=%+v truncated=%v", audit.RetainedPayloadShape, audit.RetainedPayloadShapeTruncated)
	}
	payload, ok := retainedPayloadShapeStat2382(audit.RetainedPayloadShape, "payload", "string")
	if !ok {
		t.Fatalf("missing payload string stat: %+v", audit.RetainedPayloadShape)
	}
	if payload.Occurrences != 2 || payload.Documents != 2 || payload.StringBytes != int64(len("kept")+len("second")) || payload.JSONBytes <= payload.StringBytes {
		t.Fatalf("payload stat=%+v", payload)
	}
	rkey, ok := retainedPayloadShapeStat2382(audit.RetainedPayloadShape, "commit.rkey", "string")
	if !ok || rkey.Occurrences != 2 || rkey.Documents != 2 {
		t.Fatalf("commit.rkey stat=%+v ok=%v shape=%+v", rkey, ok, audit.RetainedPayloadShape)
	}
	nested, ok := retainedPayloadShapeStat2382(audit.RetainedPayloadShape, "nested", "object")
	if !ok || nested.Occurrences != 2 || nested.Documents != 2 {
		t.Fatalf("nested object stat=%+v ok=%v shape=%+v", nested, ok, audit.RetainedPayloadShape)
	}
	if _, ok := retainedPayloadShapeStat2382(audit.RetainedPayloadShape, "kind", "string"); ok {
		t.Fatalf("declared kind path leaked into shape stats: %+v", audit.RetainedPayloadShape)
	}
	if _, ok := retainedPayloadShapeStat2382(audit.RetainedPayloadShape, "commit.operation", "string"); ok {
		t.Fatalf("declared commit.operation path leaked into shape stats: %+v", audit.RetainedPayloadShape)
	}

	limited, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeShapeStats: true,
		ShapeMaxPaths:     1,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent limited shape stats: %v audit=%+v", err, limited)
	}
	if !limited.RetainedPayloadShapeTruncated || len(limited.RetainedPayloadShape) != 1 {
		t.Fatalf("limited shape=%+v truncated=%v want one truncated row", limited.RetainedPayloadShape, limited.RetainedPayloadShapeTruncated)
	}
}

func TestAuditCollectionRetainedPayloadValueFamilyStats2662(t *testing.T) {
	col, closeDB := openRetainedPayloadAuditCollection2382(t, jsonbenchRetainedPayloadAuditConfig2382(true), [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/3aaa"}}},"payload":"same","empty":""}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"subject":{"uri":"at://did:plc:bob/app.bsky.feed.post/3bbb"}}},"payload":"same","empty":""}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeValueFamilyStats: true,
		ValueFamilyMaxDepth:     8,
		ValueFamilyMaxUnique:    10,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent value family stats: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 2 {
		t.Fatalf("audit=%+v want passed two rows", audit)
	}
	if len(audit.RetainedPayloadValueFamilies) == 0 || audit.RetainedPayloadValueFamiliesTruncated {
		t.Fatalf("value families=%+v truncated=%v", audit.RetainedPayloadValueFamilies, audit.RetainedPayloadValueFamiliesTruncated)
	}
	rkey, ok := retainedPayloadValueFamilyStat2662(audit.RetainedPayloadValueFamilies, "commit.rkey")
	if !ok {
		t.Fatalf("missing commit.rkey value-family stat: %+v", audit.RetainedPayloadValueFamilies)
	}
	if rkey.Occurrences != 2 || rkey.Documents != 2 || rkey.StringBytes != int64(len("r-0001")+len("r-0002")) {
		t.Fatalf("commit.rkey stat=%+v", rkey)
	}
	if rkey.JSONBytes <= rkey.StringBytes || rkey.OracleInputBytes != rkey.JSONBytes+rkey.Occurrences {
		t.Fatalf("commit.rkey encoded byte counters=%+v", rkey)
	}
	if rkey.MinLength != 6 || rkey.MaxLength != 6 || rkey.MeanLength != 6 {
		t.Fatalf("commit.rkey lengths=%+v", rkey)
	}
	if rkey.TrackedUniqueValues != 2 || rkey.UniqueValuesTruncated || rkey.RepeatedValues != 0 {
		t.Fatalf("commit.rkey unique counters=%+v", rkey)
	}
	if rkey.CommonPrefix != "r-000" || rkey.CommonPrefixBytes != len("r-000") {
		t.Fatalf("commit.rkey common prefix=%q/%d", rkey.CommonPrefix, rkey.CommonPrefixBytes)
	}
	if retainedPayloadValueFamilyBucketCount2662(rkey.LengthBuckets, "le_8") != 2 {
		t.Fatalf("commit.rkey length buckets=%+v", rkey.LengthBuckets)
	}
	if rkey.GzipBytes <= 0 || rkey.GzipToInputRatio <= 0 || rkey.ZSTDBytes <= 0 || rkey.ZSTDToInputRatio <= 0 {
		t.Fatalf("commit.rkey compression oracle counters=%+v", rkey)
	}

	payload, ok := retainedPayloadValueFamilyStat2662(audit.RetainedPayloadValueFamilies, "payload")
	if !ok || payload.TrackedUniqueValues != 1 || payload.RepeatedValues != 1 || payload.CommonPrefix != "same" || payload.CommonSuffix != "same" {
		t.Fatalf("payload repeated-family stat=%+v ok=%v", payload, ok)
	}
	subjectURI, ok := retainedPayloadValueFamilyStat2662(audit.RetainedPayloadValueFamilies, "commit.record.subject.uri")
	if !ok || subjectURI.CommonPrefix != "at://did:plc:" || subjectURI.CommonPrefixBytes != len("at://did:plc:") {
		t.Fatalf("subject uri family stat=%+v ok=%v", subjectURI, ok)
	}
	empty, ok := retainedPayloadValueFamilyStat2662(audit.RetainedPayloadValueFamilies, "empty")
	if !ok || empty.MinLength != 0 || empty.MaxLength != 0 || empty.MeanLength != 0 || empty.StringBytes != 0 {
		t.Fatalf("empty string family stat=%+v ok=%v", empty, ok)
	}
	emptyJSON, err := json.Marshal(empty)
	if err != nil {
		t.Fatalf("marshal empty string family stat: %v", err)
	}
	if !strings.Contains(string(emptyJSON), `"min_length":0`) || !strings.Contains(string(emptyJSON), `"max_length":0`) || !strings.Contains(string(emptyJSON), `"mean_length":0`) {
		t.Fatalf("empty string family JSON omitted zero length fields: %s", emptyJSON)
	}
	if _, ok := retainedPayloadValueFamilyStat2662(audit.RetainedPayloadValueFamilies, "kind"); ok {
		t.Fatalf("declared kind path leaked into value-family stats: %+v", audit.RetainedPayloadValueFamilies)
	}
	if _, ok := retainedPayloadValueFamilyStat2662(audit.RetainedPayloadValueFamilies, "commit.operation"); ok {
		t.Fatalf("declared commit.operation path leaked into value-family stats: %+v", audit.RetainedPayloadValueFamilies)
	}

	uniqueLimited, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeValueFamilyStats: true,
		ValueFamilyMaxUnique:    1,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent unique-limited value families: %v audit=%+v", err, uniqueLimited)
	}
	rkeyLimited, ok := retainedPayloadValueFamilyStat2662(uniqueLimited.RetainedPayloadValueFamilies, "commit.rkey")
	if !ok || rkeyLimited.TrackedUniqueValues != 1 || !rkeyLimited.UniqueValuesTruncated {
		t.Fatalf("unique-limited commit.rkey stat=%+v ok=%v", rkeyLimited, ok)
	}

	pathLimited, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeValueFamilyStats: true,
		ValueFamilyMaxPaths:     1,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent path-limited value families: %v audit=%+v", err, pathLimited)
	}
	if !pathLimited.RetainedPayloadValueFamiliesTruncated || len(pathLimited.RetainedPayloadValueFamilies) != 1 {
		t.Fatalf("path-limited value families=%+v truncated=%v want one truncated row", pathLimited.RetainedPayloadValueFamilies, pathLimited.RetainedPayloadValueFamiliesTruncated)
	}
}

func TestAuditCollectionRetainedPayloadSemanticStreamStats2662(t *testing.T) {
	compressibleBody := strings.Repeat("semantic-stream-compressible-", 32)
	col, closeDB := openRetainedPayloadAuditCollection2382(t, jsonbenchRetainedPayloadAuditConfig2382(true), [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/3aaa"},"likeCount":7,"repost":false,"body":"` + compressibleBody + `"}},"tags":["alpha","beta"],"empty":null}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"subject":{"uri":"at://did:plc:bob/app.bsky.feed.post/3bbb"},"likeCount":8,"repost":true,"body":"` + compressibleBody + `"}},"tags":["alpha"],"empty":null}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeSemanticStreams: true,
		SemanticStreamMaxDepth: 8,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent semantic streams: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 2 {
		t.Fatalf("audit=%+v want passed two rows", audit)
	}
	if len(audit.RetainedPayloadSemanticStreams) == 0 || audit.RetainedPayloadSemanticStreamsTruncated {
		t.Fatalf("semantic streams=%+v truncated=%v", audit.RetainedPayloadSemanticStreams, audit.RetainedPayloadSemanticStreamsTruncated)
	}
	if audit.RetainedPayloadSemanticStreamInputBytes <= 0 || audit.RetainedPayloadSemanticStreamZSTDBytes <= 0 {
		t.Fatalf("semantic stream total bytes missing: input=%d zstd=%d audit=%+v", audit.RetainedPayloadSemanticStreamInputBytes, audit.RetainedPayloadSemanticStreamZSTDBytes, audit)
	}
	if audit.RetainedPayloadSemanticStreamInputBytes > 512 && audit.RetainedPayloadSemanticStreamZSTDBytes >= audit.RetainedPayloadSemanticStreamInputBytes {
		t.Fatalf("semantic stream zstd oracle did not reduce compressible aggregate input: input=%d zstd=%d", audit.RetainedPayloadSemanticStreamInputBytes, audit.RetainedPayloadSemanticStreamZSTDBytes)
	}

	rkey, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "commit.rkey", "string")
	if !ok {
		t.Fatalf("missing commit.rkey semantic stream: %+v", audit.RetainedPayloadSemanticStreams)
	}
	if rkey.Occurrences != 2 || rkey.Documents != 2 || rkey.StringBytes != int64(len("r-0001")+len("r-0002")) {
		t.Fatalf("commit.rkey semantic stream=%+v", rkey)
	}
	const (
		rkeyJSONBytes      = 16 // `"r-0001"` and `"r-0002"` are 8 JSON bytes each.
		rkeyStreamBytes    = 18 // Each 8-byte JSON token gets one newline separator.
		rkeyEntryByteWidth = 9
	)
	if rkey.JSONBytes != rkeyJSONBytes || rkey.StreamInputBytes != rkeyStreamBytes || rkey.MinStreamBytes != rkeyEntryByteWidth || rkey.MaxStreamBytes != rkeyEntryByteWidth {
		t.Fatalf("commit.rkey semantic stream byte counters=%+v", rkey)
	}
	if rkey.ZSTDBytes <= 0 || rkey.ZSTDToInputRatio <= 0 {
		t.Fatalf("commit.rkey semantic stream zstd counters=%+v", rkey)
	}
	tags, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "tags[]", "string")
	if !ok || tags.Occurrences != 3 || tags.Documents != 2 || tags.StringBytes != int64(len("alpha")+len("beta")+len("alpha")) {
		t.Fatalf("tags semantic stream=%+v ok=%v", tags, ok)
	}
	likes, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "commit.record.likeCount", "number")
	const (
		likeJSONBytes   = 2 // `7` and `8`.
		likeStreamBytes = 4 // Two one-byte numbers plus two newline separators.
	)
	if !ok || likes.Occurrences != 2 || likes.Documents != 2 || likes.JSONBytes != likeJSONBytes || likes.StreamInputBytes != likeStreamBytes {
		t.Fatalf("likeCount semantic stream=%+v ok=%v", likes, ok)
	}
	repost, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "commit.record.repost", "bool")
	const (
		repostJSONBytes   = 9  // `false` plus `true`.
		repostStreamBytes = 11 // The two boolean tokens plus two newline separators.
	)
	if !ok || repost.Occurrences != 2 || repost.Documents != 2 || repost.JSONBytes != repostJSONBytes || repost.StreamInputBytes != repostStreamBytes {
		t.Fatalf("repost semantic stream=%+v ok=%v", repost, ok)
	}
	empty, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "empty", "null")
	const (
		nullJSONBytes   = 8  // Two `null` tokens.
		nullStreamBytes = 10 // Two `null` tokens plus two newline separators.
	)
	if !ok || empty.Occurrences != 2 || empty.Documents != 2 || empty.JSONBytes != nullJSONBytes || empty.StreamInputBytes != nullStreamBytes {
		t.Fatalf("empty null semantic stream=%+v ok=%v", empty, ok)
	}
	if _, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "kind", "string"); ok {
		t.Fatalf("declared kind path leaked into semantic streams: %+v", audit.RetainedPayloadSemanticStreams)
	}
	if _, ok := retainedPayloadSemanticStreamStat2662(audit.RetainedPayloadSemanticStreams, "commit.operation", "string"); ok {
		t.Fatalf("declared commit.operation path leaked into semantic streams: %+v", audit.RetainedPayloadSemanticStreams)
	}

	limited, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeSemanticStreams: true,
		SemanticStreamMaxPaths: 1,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent path-limited semantic streams: %v audit=%+v", err, limited)
	}
	if !limited.RetainedPayloadSemanticStreamsTruncated || len(limited.RetainedPayloadSemanticStreams) != 1 {
		t.Fatalf("path-limited semantic streams=%+v truncated=%v want one truncated row", limited.RetainedPayloadSemanticStreams, limited.RetainedPayloadSemanticStreamsTruncated)
	}
	if limited.RetainedPayloadSemanticStreamInputBytes != audit.RetainedPayloadSemanticStreamInputBytes {
		t.Fatalf("limited semantic input bytes=%d want full total %d", limited.RetainedPayloadSemanticStreamInputBytes, audit.RetainedPayloadSemanticStreamInputBytes)
	}
}

func TestColumnRetainedSemanticStreamV1BlockLayoutAudit2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	docs := [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"text":"hello"}},"payload":"same"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"text":"world"}},"payload":"same"}`),
	}
	accounting, err := ColumnRetainedSemanticStreamV1StorageAccountingFromJSONDocuments(*cfg, docs)
	if err != nil {
		t.Fatalf("ColumnRetainedSemanticStreamV1StorageAccountingFromJSONDocuments: %v", err)
	}
	audit, err := ColumnRetainedSemanticStreamV1BlockLayoutAuditFromJSONDocuments(*cfg, docs, 0)
	if err != nil {
		t.Fatalf("ColumnRetainedSemanticStreamV1BlockLayoutAuditFromJSONDocuments: %v", err)
	}
	if audit.Rows != len(docs) || audit.BlockRows != columnRetainedSemanticStreamV1BlockRows || audit.BlockCount != 1 {
		t.Fatalf("unexpected block layout row/block counters: %+v", audit)
	}
	if audit.PrimaryLocatorBytes != accounting.PrimaryLocatorBytes || audit.StoredBlockBytes != accounting.BlockBytes {
		t.Fatalf("audit/accounting mismatch audit=%+v accounting=%+v", audit, accounting)
	}
	if audit.RawBlockBytes < audit.StoredBlockBytes || audit.StoredBlockBytes <= 0 {
		t.Fatalf("unexpected stored/raw block bytes audit=%+v accounting=%+v", audit, accounting)
	}
	if audit.RawBlockBytes != audit.BlockHeaderBytes+audit.PathMetadataBytes+audit.EntryMetadataBytes+audit.ScalarValueBytes {
		t.Fatalf("block byte attribution mismatch: %+v", audit)
	}
	if audit.PathStreamCount < 3 || audit.ValueCount < 6 || audit.PathZSTDInputBytes <= 0 || audit.PathZSTDEncodedBytes <= 0 {
		t.Fatalf("missing stream counters: %+v", audit)
	}
	rkey, ok := retainedSemanticStreamV1PathLayoutStat2662(audit.Paths, "commit.rkey")
	if !ok {
		t.Fatalf("missing commit.rkey block path stat: %+v", audit.Paths)
	}
	if rkey.Occurrences != 2 || rkey.Blocks != 1 {
		t.Fatalf("commit.rkey occurrence counters=%+v", rkey)
	}
	if got, want := rkey.ScalarValueBytes, int64(len(`"r-0001"`)+len(`"r-0002"`)); got != want {
		t.Fatalf("commit.rkey scalar bytes=%d want %d stat=%+v", got, want, rkey)
	}
	if rkey.TotalBytes != rkey.PathMetadataBytes+rkey.EntryMetadataBytes+rkey.ScalarValueBytes || rkey.ZSTDBytes <= 0 || rkey.ZSTDToTotalRatio <= 0 {
		t.Fatalf("commit.rkey byte accounting=%+v", rkey)
	}
	for _, codec := range []string{"snappy", "lz4", "zstd"} {
		stat, ok := retainedSemanticStreamV1CodecStat2662(audit.BlockCodecStats, codec)
		if !ok {
			t.Fatalf("missing %s block codec stat: %+v", codec, audit.BlockCodecStats)
		}
		if stat.Blocks != 1 || stat.RawBytes != audit.RawBlockBytes || stat.StoredBytes <= 0 || stat.StoredToRawRatio <= 0 {
			t.Fatalf("%s codec stat=%+v audit=%+v", codec, stat, audit)
		}
	}

	limited, err := ColumnRetainedSemanticStreamV1BlockLayoutAuditFromJSONDocuments(*cfg, docs, 1)
	if err != nil {
		t.Fatalf("limited block layout audit: %v", err)
	}
	if !limited.PathsTruncated || len(limited.Paths) != 1 || limited.RawBlockBytes != audit.RawBlockBytes {
		t.Fatalf("limited paths=%+v truncated=%v raw=%d want truncated one raw %d", limited.Paths, limited.PathsTruncated, limited.RawBlockBytes, audit.RawBlockBytes)
	}
}

func TestColumnRetainedSemanticStreamV1StoredBlockZSTDWrapper2662(t *testing.T) {
	const rows = 512
	documents := make([][]byte, rows)
	for row := range documents {
		documents[row] = []byte(fmt.Sprintf(
			`{"payload":"%s","commit":{"cid":"bafy-test-%06d","record":{"$type":"app.bsky.feed.post","text":"%s"}}}`,
			strings.Repeat("same-retained-payload-", 8),
			row,
			strings.Repeat("semantic stream retained zstd body ", 12),
		))
	}

	raw, err := encodeColumnRetainedSemanticStreamV1RawBlock(documents)
	if err != nil {
		t.Fatalf("encode raw semantic-stream-v1 block: %v", err)
	}
	stored, err := encodeColumnRetainedSemanticStreamV1Block(documents)
	if err != nil {
		t.Fatalf("encode stored semantic-stream-v1 block: %v", err)
	}
	if !bytes.HasPrefix(stored, columnRetainedSemanticStreamV1BlockZSTDMagic) {
		t.Fatalf("stored block magic=%q len=%d raw=%d want zstd wrapper", stored[:len(columnRetainedSemanticStreamV1BlockMagic)], len(stored), len(raw))
	}
	if len(stored) >= len(raw) {
		t.Fatalf("stored block len=%d want below raw len=%d", len(stored), len(raw))
	}
	decoded, err := decodeColumnRetainedSemanticStreamV1StoredBlock(stored)
	if err != nil {
		t.Fatalf("decode stored semantic-stream-v1 block: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded stored block mismatch len=%d raw=%d", len(decoded), len(raw))
	}
	if legacyDecoded, err := decodeColumnRetainedSemanticStreamV1StoredBlock(raw); err != nil || !bytes.Equal(legacyDecoded, raw) {
		t.Fatalf("decode legacy raw semantic-stream-v1 block err=%v decoded_len=%d raw=%d", err, len(legacyDecoded), len(raw))
	}
	rowCount, err := columnRetainedSemanticStreamV1BlockRowCount(stored)
	if err != nil || rowCount != rows {
		t.Fatalf("row count=%d err=%v want %d", rowCount, err, rows)
	}
	allRows, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(stored)
	if err != nil {
		t.Fatalf("decode all stored semantic-stream-v1 rows: %v", err)
	}
	if len(allRows) != rows {
		t.Fatalf("decoded rows=%d want %d", len(allRows), rows)
	}
	for _, row := range []int{0, 37, rows - 1} {
		single, err := decodeColumnRetainedSemanticStreamV1BlockRowJSON(stored, uint64(row))
		if err != nil {
			t.Fatalf("decode single stored semantic-stream-v1 row %d: %v", row, err)
		}
		if !bytes.Equal(allRows[row], single) {
			t.Fatalf("decoded row %d mismatch batch=%s single=%s", row, allRows[row], single)
		}
	}
	got, err := decodeColumnRetainedSemanticStreamV1BlockRowJSON(stored, 37)
	if err != nil {
		t.Fatalf("decode stored semantic-stream-v1 row: %v", err)
	}
	assertJSONMapEqual1875(t, got, map[string]any{
		"payload": strings.Repeat("same-retained-payload-", 8),
		"commit": map[string]any{
			"cid": "bafy-test-000037",
			"record": map[string]any{
				"$type": "app.bsky.feed.post",
				"text":  strings.Repeat("semantic stream retained zstd body ", 12),
			},
		},
	})

	collector, err := newColumnRetainedSemanticStreamV1BlockLayoutCollector()
	if err != nil {
		t.Fatalf("new block layout collector: %v", err)
	}
	defer collector.close()
	if err := collector.addBlock(stored); err != nil {
		t.Fatalf("collector.addBlock stored wrapper: %v", err)
	}
	audit, err := collector.result(rows, 0)
	if err != nil {
		t.Fatalf("collector.result: %v", err)
	}
	if audit.StoredBlockBytes != int64(len(stored)) || audit.RawBlockBytes != int64(len(raw)) {
		t.Fatalf("audit stored/raw bytes=%d/%d want %d/%d audit=%+v", audit.StoredBlockBytes, audit.RawBlockBytes, len(stored), len(raw), audit)
	}
}

func TestColumnRetainedSemanticStreamV1InsertFastPathMatchesRetainedJSONPipeline3067(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	documents := [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"$type":"app.bsky.feed.post","text":"hello","tags":["one",{"nested":true}],"empty":{}}},"payload":{"body":"kept","count":17}}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"$type":"app.bsky.feed.post","text":"world","facets":[{"index":{"byteStart":0,"byteEnd":5}}]}},"identity":{"seq":12,"handle":"example.test"}}`),
	}

	prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocuments(*cfg, documents, nil)
	if err != nil {
		t.Fatalf("prepare semantic-stream-v1 documents: %v", err)
	}
	if prepared.semanticStreamBlocks == nil {
		t.Fatal("prepare semantic-stream-v1 documents did not return block table")
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)

	iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	if !iter.Valid() {
		t.Fatal("semantic-stream-v1 block table is empty")
	}
	gotBlock := append([]byte(nil), iter.UnsafeValue()...)
	iter.Next()
	if iter.Valid() {
		t.Fatal("semantic-stream-v1 block table has more than one block")
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iterate semantic-stream-v1 block table: %v", err)
	}

	retainedJSON := make([][]byte, len(documents))
	for i, document := range documents {
		retained, err := columnRetainedPayloadJSONFromJSONDocument(*cfg, document)
		if err != nil {
			t.Fatalf("legacy retained JSON row %d: %v", i, err)
		}
		retainedJSON[i] = retained
	}
	wantBlock, err := encodeColumnRetainedSemanticStreamV1Block(retainedJSON)
	if err != nil {
		t.Fatalf("legacy semantic-stream-v1 block: %v", err)
	}
	assertColumnRetainedSemanticStreamV1BlockShapeEqual(t, gotBlock, wantBlock)

	gotRows, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(gotBlock)
	if err != nil {
		t.Fatalf("decode semantic-stream-v1 fast path rows: %v", err)
	}
	wantRows, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(wantBlock)
	if err != nil {
		t.Fatalf("decode legacy semantic-stream-v1 rows: %v", err)
	}
	if len(gotRows) != len(wantRows) {
		t.Fatalf("semantic-stream-v1 fast path row count=%d want %d", len(gotRows), len(wantRows))
	}
	for i := range gotRows {
		assertJSONEqualM13C(t, gotRows[i], wantRows[i])
	}
}

func assertColumnRetainedSemanticStreamV1BlockShapeEqual(t *testing.T, gotBlock, wantBlock []byte) {
	t.Helper()
	got := canonicalColumnRetainedSemanticStreamV1BlockValueBytesForTest(t, gotBlock)
	want := canonicalColumnRetainedSemanticStreamV1BlockValueBytesForTest(t, wantBlock)
	if bytes.Equal(got, want) {
		return
	}
	gotSum := sha256.Sum256(got)
	wantSum := sha256.Sum256(want)
	t.Fatalf("semantic-stream-v1 block shape mismatch after canonical value JSON: got len=%d sha256=%x want len=%d sha256=%x", len(got), gotSum, len(want), wantSum)
}

func canonicalColumnRetainedSemanticStreamV1BlockValueBytesForTest(t *testing.T, block []byte) []byte {
	t.Helper()
	raw, err := decodeColumnRetainedSemanticStreamV1StoredBlock(block)
	if err != nil {
		t.Fatalf("decode semantic-stream-v1 stored block: %v", err)
	}
	reader := bytes.NewReader(raw[len(columnRetainedSemanticStreamV1BlockMagic):])
	out := make([]byte, 0, len(raw))
	out = append(out, columnRetainedSemanticStreamV1BlockMagic...)

	rows := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "row count")
	if err := validateColumnRetainedSemanticStreamV1BlockRows(rows); err != nil {
		t.Fatalf("validate semantic-stream-v1 row count: %v", err)
	}
	out = binary.AppendUvarint(out, rows)

	pathCount := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "path count")
	out = binary.AppendUvarint(out, pathCount)
	for pathOrdinal := uint64(0); pathOrdinal < pathCount; pathOrdinal++ {
		segmentCount := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "path segment count")
		out = binary.AppendUvarint(out, segmentCount)
		for segmentOrdinal := uint64(0); segmentOrdinal < segmentCount; segmentOrdinal++ {
			segmentLen := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "path segment length")
			segment := readColumnRetainedSemanticStreamV1TestBytes(t, reader, segmentLen, "path segment")
			out = binary.AppendUvarint(out, segmentLen)
			out = append(out, segment...)
		}

		entryCount := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "entry count")
		out = binary.AppendUvarint(out, entryCount)
		var last uint64
		for entryOrdinal := uint64(0); entryOrdinal < entryCount; entryOrdinal++ {
			delta := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "row delta")
			entryRow, err := columnRetainedSemanticStreamV1EntryRow(last, delta, entryOrdinal, rows)
			if err != nil {
				t.Fatalf("validate semantic-stream-v1 row delta: %v", err)
			}
			last = entryRow
			out = binary.AppendUvarint(out, delta)

			valueLen := readColumnRetainedSemanticStreamV1TestUvarint(t, reader, "value length")
			value := readColumnRetainedSemanticStreamV1TestBytes(t, reader, valueLen, "value")
			canonical := canonicalJSONValueForSemanticStreamV1Test(t, value)
			out = binary.AppendUvarint(out, uint64(len(canonical)))
			out = append(out, canonical...)
		}
	}
	if reader.Len() != 0 {
		t.Fatalf("semantic-stream-v1 block has %d trailing bytes", reader.Len())
	}
	return out
}

func readColumnRetainedSemanticStreamV1TestUvarint(t *testing.T, reader *bytes.Reader, label string) uint64 {
	t.Helper()
	value, err := binary.ReadUvarint(reader)
	if err != nil {
		t.Fatalf("read semantic-stream-v1 %s: %v", label, err)
	}
	return value
}

func readColumnRetainedSemanticStreamV1TestBytes(t *testing.T, reader *bytes.Reader, length uint64, label string) []byte {
	t.Helper()
	if length > uint64(reader.Len()) {
		t.Fatalf("read semantic-stream-v1 %s: length %d exceeds remaining %d", label, length, reader.Len())
	}
	buf := make([]byte, int(length))
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("read semantic-stream-v1 %s bytes: %v", label, err)
	}
	return buf
}

func canonicalJSONValueForSemanticStreamV1Test(t *testing.T, raw []byte) []byte {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		t.Fatalf("decode semantic-stream-v1 value JSON: %v", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("canonicalize semantic-stream-v1 value JSON: %v", err)
	}
	return canonical
}

func TestColumnRetainedSemanticStreamV1StoredBlockZSTDRawLimitFallback2662(t *testing.T) {
	raw, err := encodeColumnRetainedSemanticStreamV1RawBlock([][]byte{
		[]byte(`{"payload":"same","commit":{"record":{"text":"same retained body"}}}`),
		[]byte(`{"payload":"same","commit":{"record":{"text":"same retained body"}}}`),
	})
	if err != nil {
		t.Fatalf("encode raw semantic-stream-v1 block: %v", err)
	}

	stored, err := encodeColumnRetainedSemanticStreamV1StoredBlockWithRawLimit(raw, len(raw)-1)
	if err != nil {
		t.Fatalf("encode stored semantic-stream-v1 block: %v", err)
	}
	if !bytes.Equal(stored, raw) {
		t.Fatalf("stored block len=%d raw=%d want raw fallback when decoded length exceeds wrapper limit", len(stored), len(raw))
	}
	decoded, err := decodeColumnRetainedSemanticStreamV1StoredBlock(stored)
	if err != nil {
		t.Fatalf("decode raw fallback semantic-stream-v1 block: %v", err)
	}
	if !bytes.Equal(decoded, raw) {
		t.Fatalf("decoded raw fallback mismatch len=%d raw=%d", len(decoded), len(raw))
	}
}

func TestColumnRetainedSemanticStreamV1DecodeCacheDenseBlockGate2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1

	sparse := make([]DocumentRecord, 0, minColumnRetainedSemanticStreamV1DecodeCacheRowsPerBlock)
	for i := 0; i < minColumnRetainedSemanticStreamV1DecodeCacheRowsPerBlock; i++ {
		var blockKey [sha256.Size]byte
		binary.BigEndian.PutUint64(blockKey[sha256.Size-8:], uint64(i+1))
		sparse = append(sparse, DocumentRecord{Document: encodeColumnRetainedSemanticStreamV1Locator(blockKey[:], 0)})
	}
	if cache := newColumnRetainedSemanticStreamV1DecodeCacheForDocumentRecords(cfg, sparse); cache != nil {
		t.Fatalf("sparse records unexpectedly enabled full-block decode cache: %+v", cache)
	}

	var denseBlockKey [sha256.Size]byte
	denseBlockKey[0] = 0x7b
	dense := make([]DocumentRecord, 0, minColumnRetainedSemanticStreamV1DecodeCacheRowsPerBlock)
	for i := 0; i < minColumnRetainedSemanticStreamV1DecodeCacheRowsPerBlock; i++ {
		dense = append(dense, DocumentRecord{Document: encodeColumnRetainedSemanticStreamV1Locator(denseBlockKey[:], uint64(i))})
	}
	cache := newColumnRetainedSemanticStreamV1DecodeCacheForDocumentRecords(cfg, dense)
	if cache == nil {
		t.Fatal("dense records did not enable full-block decode cache")
	}
	if !cache.shouldCacheBlock(string(denseBlockKey[:])) {
		t.Fatal("dense block was not allowed in decode cache")
	}
	var otherBlockKey [sha256.Size]byte
	otherBlockKey[0] = 0x8c
	if cache.shouldCacheBlock(string(otherBlockKey[:])) {
		t.Fatal("unobserved block should not be allowed in decode cache")
	}
}

func TestColumnRetainedSemanticStreamV1RejectsOversizedRowCount2662(t *testing.T) {
	raw := append([]byte(nil), columnRetainedSemanticStreamV1BlockMagic...)
	raw = binary.AppendUvarint(raw, columnRetainedSemanticStreamV1BlockRows+1)
	raw = binary.AppendUvarint(raw, 0)

	if _, err := columnRetainedSemanticStreamV1BlockRowCount(raw); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("row count err=%v want exceeds max", err)
	}
	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(raw); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("decode all rows err=%v want exceeds max", err)
	}
	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowJSON(raw, 0); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("decode single row err=%v want exceeds max", err)
	}
}

func TestColumnRetainedSemanticStreamV1RejectsOutOfRangeEntryRows2662(t *testing.T) {
	raw := append([]byte(nil), columnRetainedSemanticStreamV1BlockMagic...)
	raw = binary.AppendUvarint(raw, 1) // block rows
	raw = binary.AppendUvarint(raw, 1) // path count
	raw = binary.AppendUvarint(raw, 1) // path segment count
	raw = binary.AppendUvarint(raw, uint64(len("payload")))
	raw = append(raw, "payload"...)
	raw = binary.AppendUvarint(raw, 1) // entry count
	raw = binary.AppendUvarint(raw, 1) // row delta, outside the one-row block
	raw = binary.AppendUvarint(raw, uint64(len(`"same"`)))
	raw = append(raw, `"same"`...)

	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(raw); err == nil || !strings.Contains(err.Error(), "outside block rows") {
		t.Fatalf("decode all rows err=%v want outside block rows", err)
	}
	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowJSON(raw, 0); err == nil || !strings.Contains(err.Error(), "outside block rows") {
		t.Fatalf("decode single row err=%v want outside block rows", err)
	}
	collector, err := newColumnRetainedSemanticStreamV1BlockLayoutCollector()
	if err != nil {
		t.Fatalf("new block layout collector: %v", err)
	}
	defer collector.close()
	if err := collector.addBlock(raw); err == nil || !strings.Contains(err.Error(), "outside block rows") {
		t.Fatalf("collector.addBlock err=%v want outside block rows", err)
	}
}

func TestColumnRetainedSemanticStreamV1RejectsEntryRowDeltaOverflow2662(t *testing.T) {
	raw := append([]byte(nil), columnRetainedSemanticStreamV1BlockMagic...)
	raw = binary.AppendUvarint(raw, columnRetainedSemanticStreamV1BlockRows)
	raw = binary.AppendUvarint(raw, 1) // path count
	raw = binary.AppendUvarint(raw, 1) // path segment count
	raw = binary.AppendUvarint(raw, uint64(len("payload")))
	raw = append(raw, "payload"...)
	raw = binary.AppendUvarint(raw, 2) // entry count
	raw = binary.AppendUvarint(raw, ^uint64(0))
	raw = binary.AppendUvarint(raw, uint64(len(`"first"`)))
	raw = append(raw, `"first"`...)
	raw = binary.AppendUvarint(raw, 1)
	raw = binary.AppendUvarint(raw, uint64(len(`"second"`)))
	raw = append(raw, `"second"`...)

	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(raw); err == nil || !strings.Contains(err.Error(), "outside block rows") {
		t.Fatalf("decode all first invalid row err=%v want outside block rows", err)
	}
	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowJSON(raw, 0); err == nil || !strings.Contains(err.Error(), "outside block rows") {
		t.Fatalf("decode single first invalid row err=%v want outside block rows", err)
	}

	raw = append([]byte(nil), columnRetainedSemanticStreamV1BlockMagic...)
	raw = binary.AppendUvarint(raw, columnRetainedSemanticStreamV1BlockRows)
	raw = binary.AppendUvarint(raw, 1) // path count
	raw = binary.AppendUvarint(raw, 1) // path segment count
	raw = binary.AppendUvarint(raw, uint64(len("payload")))
	raw = append(raw, "payload"...)
	raw = binary.AppendUvarint(raw, 2) // entry count
	raw = binary.AppendUvarint(raw, columnRetainedSemanticStreamV1BlockRows-1)
	raw = binary.AppendUvarint(raw, uint64(len(`"first"`)))
	raw = append(raw, `"first"`...)
	raw = binary.AppendUvarint(raw, ^uint64(0))
	raw = binary.AppendUvarint(raw, uint64(len(`"second"`)))
	raw = append(raw, `"second"`...)

	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(raw); err == nil || !strings.Contains(err.Error(), "row delta overflow") {
		t.Fatalf("decode all overflow err=%v want row delta overflow", err)
	}
	if _, err := decodeColumnRetainedSemanticStreamV1BlockRowJSON(raw, 0); err == nil || !strings.Contains(err.Error(), "row delta overflow") {
		t.Fatalf("decode single overflow err=%v want row delta overflow", err)
	}
	collector, err := newColumnRetainedSemanticStreamV1BlockLayoutCollector()
	if err != nil {
		t.Fatalf("new block layout collector: %v", err)
	}
	defer collector.close()
	if err := collector.addBlock(raw); err == nil || !strings.Contains(err.Error(), "row delta overflow") {
		t.Fatalf("collector.addBlock err=%v want row delta overflow", err)
	}
}

func TestColumnRetainedSemanticStreamV1StoredBlockZSTDFailsClosed2662(t *testing.T) {
	raw, err := encodeColumnRetainedSemanticStreamV1RawBlock([][]byte{[]byte(`{"payload":"same"}`), []byte(`{"payload":"same"}`)})
	if err != nil {
		t.Fatalf("encode raw semantic-stream-v1 block: %v", err)
	}
	compressed := semanticStreamZSTDCompress2662(t, raw)

	cases := []struct {
		name string
		raw  []byte
		want string
	}{
		{
			name: "invalid zstd",
			raw:  semanticStreamZSTDStoredBlockWithRawLength2662(uint64(len(raw)), []byte("not-zstd")),
			want: "decode semantic-stream-v1 retained block zstd payload",
		},
		{
			name: "wrong decoded length",
			raw:  semanticStreamZSTDStoredBlockWithRawLength2662(uint64(len(raw)+1), compressed),
			want: "decoded length",
		},
		{
			name: "decoded invalid magic",
			raw:  semanticStreamZSTDStoredBlock2662(t, []byte("not-a-raw-semantic-block")),
			want: "decoded to invalid block",
		},
		{
			name: "oversized raw length",
			raw:  semanticStreamZSTDStoredBlockWithRawLength2662(maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes+1, []byte("x")),
			want: "exceeds max",
		},
	}
	for _, tc := range cases {
		if _, err := decodeColumnRetainedSemanticStreamV1StoredBlock(tc.raw); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s err=%v want %q", tc.name, err, tc.want)
		}
	}
}

func semanticStreamZSTDStoredBlock2662(t testing.TB, raw []byte) []byte {
	t.Helper()
	compressed := semanticStreamZSTDCompress2662(t, raw)
	return semanticStreamZSTDStoredBlockWithRawLength2662(uint64(len(raw)), compressed)
}

func semanticStreamZSTDCompress2662(t testing.TB, raw []byte) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	compressed := enc.EncodeAll(raw, nil)
	enc.Close()
	return compressed
}

func semanticStreamZSTDStoredBlockWithRawLength2662(rawLen uint64, compressed []byte) []byte {
	out := make([]byte, 0, len(columnRetainedSemanticStreamV1BlockZSTDMagic)+binary.MaxVarintLen64+len(compressed))
	out = append(out, columnRetainedSemanticStreamV1BlockZSTDMagic...)
	out = binary.AppendUvarint(out, rawLen)
	out = append(out, compressed...)
	return out
}

func TestColumnRetainedSemanticStreamV1BlockCodecLZ4RawFallback2662(t *testing.T) {
	collector, err := newColumnRetainedSemanticStreamV1BlockLayoutCollector()
	if err != nil {
		t.Fatalf("new block layout collector: %v", err)
	}
	defer collector.close()

	raw := incompressibleLZ4Fixture2662(t)
	encodedBytes, ok := collector.encodeBlockCodec("lz4", raw)
	if !ok || encodedBytes != 0 {
		t.Fatalf("lz4 fixture encodedBytes=%d ok=%v want valid raw fallback", encodedBytes, ok)
	}
	collector.observeBlockCodec("lz4", raw)

	stat := collector.codecs["lz4"]
	if stat == nil {
		t.Fatal("missing lz4 codec stat")
	}
	if stat.Blocks != 1 || stat.RawBytes != int64(len(raw)) || stat.EncodedBytes != 0 || stat.StoredBytes != int64(len(raw)) || stat.RawFallbackBlocks != 1 || stat.KeptBlocks != 0 || stat.EncodeErrors != 0 {
		t.Fatalf("lz4 raw fallback stat=%+v raw=%d", stat, len(raw))
	}
}

func incompressibleLZ4Fixture2662(t *testing.T) []byte {
	t.Helper()
	for seed := uint64(1); seed <= 64; seed++ {
		raw := make([]byte, 64*1024)
		var counter uint64
		for off := 0; off < len(raw); {
			var input [16]byte
			binary.LittleEndian.PutUint64(input[:8], seed)
			binary.LittleEndian.PutUint64(input[8:], counter)
			sum := sha256.Sum256(input[:])
			off += copy(raw[off:], sum[:])
			counter++
		}
		dst := make([]byte, len(raw))
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil {
			t.Fatalf("lz4 fixture probe seed=%d: %v", seed, err)
		}
		if n == 0 {
			return raw
		}
	}
	t.Fatal("could not build deterministic incompressible lz4 fixture")
	return nil
}

func TestAuditCollectionRetainedPayloadSemanticStreamBlockLayout2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"text":"hello"}},"payload":"same"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"text":"world"}},"payload":"same"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeSemanticStreamBlockLayout: true,
		SemanticStreamBlockMaxPaths:      1,
	})
	if err != nil {
		t.Fatalf("AuditRetainedPayloadDeclaredPathsAbsent semantic-stream-v1 block layout: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 2 {
		t.Fatalf("audit=%+v want passed two rows", audit)
	}
	if audit.RetainedPayloadSemanticStreamBlockLayout == nil {
		t.Fatalf("missing semantic stream block layout: %+v", audit)
	}
	layout := audit.RetainedPayloadSemanticStreamBlockLayout
	if layout.BlockCount != 1 || layout.RawBlockBytes <= 0 || len(layout.BlockCodecStats) == 0 || len(layout.Paths) != 1 || !layout.PathsTruncated {
		t.Fatalf("unexpected block layout: %+v", layout)
	}
	if _, ok := retainedSemanticStreamV1PathLayoutStat2662(layout.Paths, "kind"); ok {
		t.Fatalf("declared kind leaked into block layout paths: %+v", layout.Paths)
	}

	limited, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeSemanticStreamBlockLayout: true,
		MaxDocuments:                     1,
	})
	if err != nil {
		t.Fatalf("limited semantic-stream-v1 block layout audit: %v audit=%+v", err, limited)
	}
	if !limited.Truncated || limited.CheckedRows != 1 || limited.RetainedPayloadSemanticStreamBlockLayout != nil {
		t.Fatalf("limited audit=%+v want truncated one row without mixed full-block layout", limited)
	}
}

func TestAuditCollectionRetainedPayloadSemanticStreamBlockLayoutSampledFailsClosed2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"text":"hello"}},"payload":"same"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"text":"world"}},"payload":"same"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		Paths:                            []string{"payload"},
		IncludeSemanticStreamBlockLayout: true,
		MaxDocuments:                     1,
	})
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("sampled semantic-stream-v1 block layout err=%v audit=%+v want payload violation", err, audit)
	}
	if audit.Status != "failed" || !audit.Truncated || audit.CheckedRows != 1 || audit.RetainedPayloadSemanticStreamBlockLayout != nil {
		t.Fatalf("sampled semantic-stream-v1 block layout audit=%+v want failed truncated one row without emitted layout", audit)
	}
	if len(audit.Violations) != 1 || audit.Violations[0].DocumentID != "semantic-stream-v1-sampled-blocks" || audit.Violations[0].Path != "payload" {
		t.Fatalf("sampled semantic-stream-v1 block layout violations=%+v want sampled-block payload", audit.Violations)
	}
}

func TestAuditCollectionRetainedPayloadSemanticStreamBlockLayoutSampledDecodedStatsFailsClosed2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"text":"hello"}}}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"text":"world"}},"payload":"same"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		Paths:                            []string{"payload"},
		IncludeShapeStats:                true,
		IncludeSemanticStreamBlockLayout: true,
		MaxDocuments:                     1,
	})
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Fatalf("sampled semantic-stream-v1 decoded stats block layout err=%v audit=%+v want payload violation", err, audit)
	}
	if audit.Status != "failed" || !audit.Truncated || audit.CheckedRows != 1 || audit.RetainedPayloadSemanticStreamBlockLayout != nil {
		t.Fatalf("sampled semantic-stream-v1 decoded stats audit=%+v want failed truncated one row without emitted layout", audit)
	}
	if len(audit.Violations) != 1 || audit.Violations[0].DocumentID != "semantic-stream-v1-sampled-blocks" || audit.Violations[0].Path != "payload" {
		t.Fatalf("sampled semantic-stream-v1 decoded stats violations=%+v want sampled-block payload", audit.Violations)
	}
}

func TestAuditCollectionRetainedPayloadSemanticStreamBlockLayoutRecordsSingleRowBlock2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"text":"hello"}},"payload":"same"}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeSemanticStreamBlockLayout: true,
	})
	if err != nil {
		t.Fatalf("inline semantic-stream-v1 block layout audit: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || audit.CheckedRows != 1 {
		t.Fatalf("audit=%+v want passed one semantic block row", audit)
	}
	layout := audit.RetainedPayloadSemanticStreamBlockLayout
	if layout == nil || layout.BlockCount != 1 || layout.RawBlockBytes <= 0 || layout.StoredBlockBytes <= 0 || layout.PrimaryLocatorBytes <= 0 {
		t.Fatalf("single-row block layout=%+v audit=%+v", layout, audit)
	}
}

func TestAuditCollectionRetainedPayloadSemanticStreamBlockLayoutLiteralDottedKey2662(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled:                 true,
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
		Columns: []ColumnStoreColumn{
			{Name: "nested", Path: "a.b", ValueType: ColumnStoreValueString, Nullable: true},
		},
	}
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"a.b":"literal-one","other":1}`),
		[]byte(`{"a.b":"literal-two","other":2}`),
	})
	defer closeDB()

	audit, err := col.AuditRetainedPayloadDeclaredPathsAbsent(ColumnRetainedPayloadCollectionAuditOptions{
		IncludeSemanticStreamBlockLayout: true,
	})
	if err != nil {
		t.Fatalf("literal dotted key block layout audit: %v audit=%+v", err, audit)
	}
	if audit.Status != "passed" || len(audit.Violations) != 0 {
		t.Fatalf("literal dotted key audit=%+v want passed without nested a.b violation", audit)
	}
	layout := audit.RetainedPayloadSemanticStreamBlockLayout
	if layout == nil {
		t.Fatalf("literal dotted key missing block layout: %+v", audit)
	}
	if _, ok := retainedSemanticStreamV1PathLayoutStat2662(layout.Paths, "a.b"); !ok {
		t.Fatalf("literal dotted key path missing from layout paths: %+v", layout.Paths)
	}
}

func TestAuditCollectionRetainedPayloadSemanticStreamBlockLayoutValidatesLocators2662(t *testing.T) {
	cfg := jsonbenchRetainedPayloadAuditConfig2382(true)
	cfg.RetainedPayloadEncoding = ColumnRetainedPayloadEncodingSemanticStreamV1
	col, closeDB := openRetainedPayloadAuditCollection2382(t, cfg, [][]byte{
		[]byte(`{"time_us":1,"kind":"commit","did":"did:plc:one","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"r-0001","record":{"text":"hello"}},"payload":"same"}`),
		[]byte(`{"time_us":2,"kind":"commit","did":"did:plc:two","commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"r-0002","record":{"text":"world"}},"payload":"same"}`),
	})
	defer closeDB()

	snap := col.db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	retained, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), []byte("doc-000000"), nil)
	if err != nil || !found {
		t.Fatalf("collectionGetAppendAtCatalogRoot primary: found=%v err=%v", found, err)
	}
	blockKey, row, ok, err := parseColumnRetainedSemanticStreamV1Locator(retained)
	if err != nil || !ok {
		t.Fatalf("parse semantic-stream locator ok=%v err=%v retained=%x", ok, err, retained)
	}
	rowCache := make(map[string]uint64)
	if ok, err := validateColumnRetainedSemanticStreamV1LocatorAtSnapshot(snap, catalog, retained, rowCache); !ok || err != nil {
		t.Fatalf("validate live locator ok=%v err=%v", ok, err)
	}
	if len(rowCache) != 1 {
		t.Fatalf("row cache entries=%d want one", len(rowCache))
	}

	missingKey := append([]byte(nil), blockKey...)
	missingKey[0] ^= 0xff
	missingLocator := encodeColumnRetainedSemanticStreamV1Locator(missingKey, row)
	if ok, err := validateColumnRetainedSemanticStreamV1LocatorAtSnapshot(snap, catalog, missingLocator, nil); !ok || err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing locator ok=%v err=%v want missing-block failure", ok, err)
	}

	outOfRangeLocator := encodeColumnRetainedSemanticStreamV1Locator(blockKey, 2)
	if ok, err := validateColumnRetainedSemanticStreamV1LocatorAtSnapshot(snap, catalog, outOfRangeLocator, nil); !ok || err == nil || !strings.Contains(err.Error(), "outside block rows") {
		t.Fatalf("out-of-range locator ok=%v err=%v want row-range failure", ok, err)
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

func retainedPayloadShapeStat2382(stats []ColumnRetainedPayloadShapePathStat, path, kind string) (ColumnRetainedPayloadShapePathStat, bool) {
	for _, stat := range stats {
		if stat.Path == path && stat.ValueKind == kind {
			return stat, true
		}
	}
	return ColumnRetainedPayloadShapePathStat{}, false
}

func retainedPayloadValueFamilyStat2662(stats []ColumnRetainedPayloadValueFamilyStat, path string) (ColumnRetainedPayloadValueFamilyStat, bool) {
	for _, stat := range stats {
		if stat.Path == path {
			return stat, true
		}
	}
	return ColumnRetainedPayloadValueFamilyStat{}, false
}

func retainedPayloadSemanticStreamStat2662(stats []ColumnRetainedPayloadSemanticStreamStat, path, kind string) (ColumnRetainedPayloadSemanticStreamStat, bool) {
	for _, stat := range stats {
		if stat.Path == path && stat.ValueKind == kind {
			return stat, true
		}
	}
	return ColumnRetainedPayloadSemanticStreamStat{}, false
}

func retainedSemanticStreamV1PathLayoutStat2662(stats []ColumnRetainedSemanticStreamV1PathLayoutStat, path string) (ColumnRetainedSemanticStreamV1PathLayoutStat, bool) {
	for _, stat := range stats {
		if stat.Path == path {
			return stat, true
		}
	}
	return ColumnRetainedSemanticStreamV1PathLayoutStat{}, false
}

func retainedSemanticStreamV1CodecStat2662(stats []ColumnRetainedSemanticStreamV1CodecStat, codec string) (ColumnRetainedSemanticStreamV1CodecStat, bool) {
	for _, stat := range stats {
		if stat.Codec == codec {
			return stat, true
		}
	}
	return ColumnRetainedSemanticStreamV1CodecStat{}, false
}

func retainedPayloadValueFamilyBucketCount2662(buckets []ColumnRetainedPayloadLengthBucket, name string) int64 {
	for _, bucket := range buckets {
		if bucket.Bucket == name {
			return bucket.Count
		}
	}
	return 0
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
