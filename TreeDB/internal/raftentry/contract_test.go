package raftentry

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/nativewire"
)

const (
	createCollectionDigestV1Hex = "50d795a174052328acd5aa8c1378668a26384c32691d8eacd968b9ddbb2f070d"
	insertBatchDigestV1Hex      = "7091242d8878fb3a1d1d2b6882ae19130d800dcffa853733433c6879a122952a"
)

func TestDecodeCommandEntryV1AcceptsCreateCollectionContract(t *testing.T) {
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	entry, err := DecodeCommandEntryV1(raw, DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1: %v", err)
	}
	if entry.Row.Decision != DecisionAccepted {
		t.Fatalf("decision=%s, want accepted", entry.Row.Decision)
	}
	if entry.Row.DuplicateMode != DuplicateFailClosedAllowedV1 {
		t.Fatalf("duplicate mode=%s, want fail closed", entry.Row.DuplicateMode)
	}
	if entry.Idempotency != IdempotencyRequiredV1 || len(entry.IdempotencyKey) == 0 {
		t.Fatalf("idempotency=%s keyLen=%d", entry.Idempotency, len(entry.IdempotencyKey))
	}
	if entry.Target.ScopeRule != ScopeRuleSingleGroupV1 || entry.Target.DatabaseScope != DatabaseScopeDefaultV1 || entry.Target.CatalogScope != CatalogScopeDefaultV1 {
		t.Fatalf("target scope=%+v", entry.Target)
	}
	if entry.Target.CommandID != nativewire.CommandCreateCollection || len(entry.Target.CollectionMeta) == 0 || len(entry.Target.ExpectedCatalogVersion) == 0 {
		t.Fatalf("incomplete target identity: %+v", entry.Target)
	}
	if entry.Digest.Hex() != createCollectionDigestV1Hex {
		t.Fatalf("create collection digest=%s want %s", entry.Digest.Hex(), createCollectionDigestV1Hex)
	}
}

func TestDigestV1StabilityAcrossMetadataAndApplyEntryID(t *testing.T) {
	base := deterministicInsertEntry(t, nil, 0)
	variant := deterministicInsertEntry(t, []nativewire.Section{
		{ID: nativewire.SectionAckPolicy, Bytes: uvarintPayload(uint64(nativewire.AckSynced))},
		{ID: nativewire.SectionConsistencyPolicy, Bytes: uvarintPayload(3)},
		{ID: nativewire.SectionCompression, Bytes: []byte{1}},
		{ID: nativewire.SectionTraceContext, Bytes: []byte("trace")},
		{ID: nativewire.SectionDeadline, Bytes: []byte("deadline")},
		{ID: 9000, Bytes: []byte("ignored")},
	}, nativewire.CommandFlagOmitResultIDs|nativewire.CommandFlagOmitResponseMeta)
	if !bytes.Equal(base, variant) {
		t.Fatalf("native-wire metadata changed deterministic entry\nbase=%x\nvariant=%x", base, variant)
	}
	d0, err := ValidateCommandDigestInputV1(base, DecodeOptions{
		ApplyEntryID: ApplyEntryID{Term: 1, Index: 2},
		RequestMetadata: RequestMetadataV1{
			RequestID:                 11,
			AckPolicy:                 nativewire.AckVisible,
			DeadlineUnixNanos:         100,
			TraceContext:              []byte("trace-a"),
			Compression:               "none",
			OmitResultIDs:             false,
			OmitResponseMeta:          false,
			ClusterRouteKnown:         true,
			ClusterRouteDatabase:      "default",
			ClusterRouteCatalog:       "default",
			ClusterRouteCollection:    "users",
			ClusterRouteShape:         "token",
			ClusterRouteGroupID:       "group-a",
			ClusterRouteMembers:       []string{"node-a", "node-b"},
			ClusterRouteLeaderHint:    "node-a",
			ClusterRoutePlacementMode: "collection",
			ClusterRouteTokenKnown:    true,
			ClusterRouteToken:         11,
			ClusterRoutePartitionID:   "token-000000",
		},
	})
	if err != nil {
		t.Fatalf("ValidateCommandDigestInputV1 base: %v", err)
	}
	d1, err := ValidateCommandDigestInputV1(variant, DecodeOptions{
		ApplyEntryID: ApplyEntryID{Term: 9, Index: 10},
		RequestMetadata: RequestMetadataV1{
			RequestID:                 99,
			AckPolicy:                 nativewire.AckSynced,
			DeadlineUnixNanos:         900,
			TraceContext:              []byte("trace-b"),
			Compression:               "lz4",
			OmitResultIDs:             true,
			OmitResponseMeta:          true,
			ClusterRouteKnown:         true,
			ClusterRouteDatabase:      "tenant",
			ClusterRouteCatalog:       "catalog-b",
			ClusterRouteCollection:    "events",
			ClusterRouteShape:         "collection",
			ClusterRouteGroupID:       "group-b",
			ClusterRouteMembers:       []string{"node-c", "node-d"},
			ClusterRouteLeaderHint:    "node-c",
			ClusterRoutePlacementMode: "collection",
			ClusterRouteTokenKnown:    true,
			ClusterRouteToken:         99,
			ClusterRoutePartitionID:   "token-000099",
		},
	})
	if err != nil {
		t.Fatalf("ValidateCommandDigestInputV1 variant: %v", err)
	}
	if d0 != d1 {
		t.Fatalf("ApplyEntryID or metadata changed digest: %s != %s", d0.Hex(), d1.Hex())
	}
	if d0.Hex() != insertBatchDigestV1Hex {
		t.Fatalf("insert digest=%s want %s", d0.Hex(), insertBatchDigestV1Hex)
	}
}

func TestDigestV1CoversScopeAndCatalogIdentity(t *testing.T) {
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	base, err := ValidateCommandDigestInputV1(raw, DecodeOptions{})
	if err != nil {
		t.Fatalf("ValidateCommandDigestInputV1 base: %v", err)
	}
	differentDatabase, err := ValidateCommandDigestInputV1(raw, DecodeOptions{DatabaseScope: "database/tenant-b"})
	if err != nil {
		t.Fatalf("ValidateCommandDigestInputV1 database: %v", err)
	}
	differentCatalog, err := ValidateCommandDigestInputV1(raw, DecodeOptions{CatalogScope: "catalog/tenant-b"})
	if err != nil {
		t.Fatalf("ValidateCommandDigestInputV1 catalog: %v", err)
	}
	if base == differentDatabase {
		t.Fatal("database scope did not affect CommandDigestV1")
	}
	if base == differentCatalog {
		t.Fatal("catalog scope did not affect CommandDigestV1")
	}
}

func TestDecodeCommandEntryV1AcceptsMutationWideningRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		command nativewire.CommandID
	}{
		{"insert", "../nativewire/testdata/v1/insert_batch_entry.hex", nativewire.CommandInsertBatch},
		{"replace", "../nativewire/testdata/v1/replace_batch_entry.hex", nativewire.CommandReplaceBatch},
		{"delete", "../nativewire/testdata/v1/delete_batch_entry.hex", nativewire.CommandDeleteBatch},
		{"update_bson_set", "../nativewire/testdata/v1/update_bson_set_entry.hex", nativewire.CommandUpdateBSONSet},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := ClassifyNativeWireCommandV1(tc.command)
			if !row.Known || row.Decision != DecisionAccepted || row.CommandWALStatus != "WAL-supported" {
				t.Fatalf("row=%+v, want accepted WAL-supported mutation row", row)
			}
			entry, err := DecodeCommandEntryV1(readHexFixture(t, tc.fixture), DecodeOptions{})
			if err != nil {
				t.Fatalf("DecodeCommandEntryV1: %v", err)
			}
			if entry.Target.CommandID != tc.command {
				t.Fatalf("target command=%d, want %d", entry.Target.CommandID, tc.command)
			}
		})
	}
}

func TestDecodeCommandEntryV1RejectsDDLBarriersReadsAndUnknowns(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture string
		command nativewire.CommandID
	}{
		{"create_index", "../nativewire/testdata/v1/create_index_entry.hex", nativewire.CommandCreateIndex},
		{"drop_index", "../nativewire/testdata/v1/drop_index_entry.hex", nativewire.CommandDropIndex},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := ClassifyNativeWireCommandV1(tc.command)
			if !row.Known || row.Decision != DecisionRejected || row.CommandWALStatus != "WAL-rejected" {
				t.Fatalf("row=%+v, want WAL-rejected R3a rejection", row)
			}
			if _, err := DecodeCommandEntryV1(readHexFixture(t, tc.fixture), DecodeOptions{}); codeOf(err) != ErrorUnsupportedCommandV1 {
				t.Fatalf("DecodeCommandEntryV1 err=%v code=%s, want unsupported command", err, codeOf(err))
			}
		})
	}
	for _, command := range []nativewire.CommandID{
		nativewire.CommandDropCollection,
		nativewire.CommandFlushCollection,
		nativewire.CommandFlushAll,
		nativewire.CommandCheckpoint,
		nativewire.CommandGetMany,
		nativewire.CommandOpenScan,
		nativewire.CommandCursorNext,
	} {
		row := ClassifyNativeWireCommandV1(command)
		if !row.Known || row.Decision != DecisionRejected {
			t.Fatalf("command %d row=%+v, want known rejected row", command, row)
		}
	}
	if row := ClassifyNativeWireCommandV1(nativewire.CommandID(9999)); row.Known {
		t.Fatalf("unknown command classified as known: %+v", row)
	}
}

func TestDecodeCommandEntryV1MapsReadOnlyNativeWireRejection(t *testing.T) {
	readOnly := appendDeterministicEntryRaw(nativewire.CommandGetMany, []nativewire.Section{
		{ID: nativewire.SectionCollectionRef, Bytes: []byte{1, 'c'}},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, []byte("a"))},
	})
	if _, err := DecodeCommandEntryV1(readOnly, DecodeOptions{}); codeOf(err) != ErrorReadOnlyV1 {
		t.Fatalf("read-only command err=%v code=%s", err, codeOf(err))
	}
	readOnlyFutureVersion := appendDeterministicEntryRawWithHeader(nativewire.DeterministicEntryVersion, nativewire.CommandGetMany, 2, 0, []nativewire.Section{
		{ID: nativewire.SectionCollectionRef, Bytes: []byte{1, 'c'}},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, []byte("a"))},
	})
	if _, err := DecodeCommandEntryV1(readOnlyFutureVersion, DecodeOptions{}); codeOf(err) != ErrorUnsupportedVersionV1 {
		t.Fatalf("read-only future command version err=%v code=%s", err, codeOf(err))
	}
	explain := appendDeterministicEntryRaw(nativewire.CommandExplain, nil)
	if _, err := DecodeCommandEntryV1(explain, DecodeOptions{}); codeOf(err) != ErrorReadOnlyV1 {
		t.Fatalf("explain command err=%v code=%s", err, codeOf(err))
	}
}

func TestDecodeCommandEntryV1RejectsMalformedOversizedMissingGuardAndNoIdempotency(t *testing.T) {
	if _, err := DecodeCommandEntryV1([]byte("bad"), DecodeOptions{}); codeOf(err) != ErrorMalformedEntryV1 {
		t.Fatalf("malformed err=%v code=%s", err, codeOf(err))
	}
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	if _, err := DecodeCommandEntryV1(raw, DecodeOptions{Limits: nativewire.Limits{MaxFrameSize: uint64(len(raw) - 1)}}); codeOf(err) != ErrorResourceExhaustedV1 {
		t.Fatalf("oversized err=%v code=%s", err, codeOf(err))
	}
	missingGuard := deterministicCreateCollectionEntry(t, "client-a:create:users", false)
	if _, err := DecodeCommandEntryV1(missingGuard, DecodeOptions{}); codeOf(err) != ErrorMissingGuardV1 {
		t.Fatalf("missing guard err=%v code=%s", err, codeOf(err))
	}
	noID := deterministicCreateCollectionEntry(t, NoIdempotencyTokenV1, true)
	if _, err := DecodeCommandEntryV1(noID, DecodeOptions{}); codeOf(err) != ErrorNoIdempotencyV1 {
		t.Fatalf("NoIdempotency err=%v code=%s", err, codeOf(err))
	}
	missingID := appendDeterministicEntryRaw(nativewire.CommandCreateCollection, []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(missingID, DecodeOptions{}); codeOf(err) != ErrorNoIdempotencyV1 {
		t.Fatalf("missing idempotency err=%v code=%s", err, codeOf(err))
	}
	emptyID := appendDeterministicEntryRaw(nativewire.CommandCreateCollection, []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionIdempotencyKey, Bytes: nil},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(emptyID, DecodeOptions{}); codeOf(err) != ErrorNoIdempotencyV1 {
		t.Fatalf("empty idempotency err=%v code=%s", err, codeOf(err))
	}
	duplicateSingleton := appendDeterministicEntryRaw(nativewire.CommandCreateCollection, []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("client-a:create:users")},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(duplicateSingleton, DecodeOptions{}); codeOf(err) != ErrorMalformedEntryV1 {
		t.Fatalf("duplicate singleton err=%v code=%s", err, codeOf(err))
	}
	unknownVersion := appendDeterministicEntryRawWithHeader(nativewire.DeterministicEntryVersion+1, nativewire.CommandCreateCollection, 1, 0, []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("client-a:create:users")},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(unknownVersion, DecodeOptions{}); codeOf(err) != ErrorUnsupportedVersionV1 {
		t.Fatalf("unknown version err=%v code=%s", err, codeOf(err))
	}
	unknownFeature := appendDeterministicEntryRawWithHeader(nativewire.DeterministicEntryVersion, nativewire.CommandCreateCollection, 1, 1, []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("client-a:create:users")},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(unknownFeature, DecodeOptions{}); codeOf(err) != ErrorUnsupportedFeatureV1 {
		t.Fatalf("unknown feature err=%v code=%s", err, codeOf(err))
	}
	unsupportedCommandVersion := appendDeterministicEntryRawWithHeader(nativewire.DeterministicEntryVersion, nativewire.CommandCreateCollection, 2, 0, []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("client-a:create:users")},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(unsupportedCommandVersion, DecodeOptions{}); codeOf(err) != ErrorUnsupportedVersionV1 {
		t.Fatalf("unsupported command version err=%v code=%s", err, codeOf(err))
	}
	unknownCommand := appendDeterministicEntryRaw(nativewire.CommandID(9999), []nativewire.Section{
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("client-a:create:users")},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	})
	if _, err := DecodeCommandEntryV1(unknownCommand, DecodeOptions{}); codeOf(err) != ErrorUnsupportedCommandV1 {
		t.Fatalf("unknown command err=%v code=%s", err, codeOf(err))
	}
}

func TestDecodeCommandEntryV1RejectsTargetAndScopeMismatch(t *testing.T) {
	raw := readHexFixture(t, "../nativewire/testdata/v1/create_collection_entry.hex")
	entry, err := DecodeCommandEntryV1(raw, DecodeOptions{})
	if err != nil {
		t.Fatalf("DecodeCommandEntryV1: %v", err)
	}
	target := entry.Target.Clone()
	target.ExpectedCatalogVersion = []byte{99}
	if _, err := DecodeCommandEntryV1(raw, DecodeOptions{ExpectedTarget: &target}); codeOf(err) != ErrorTargetMismatchV1 {
		t.Fatalf("target mismatch err=%v code=%s", err, codeOf(err))
	}
	if _, err := DecodeCommandEntryV1(raw, DecodeOptions{ScopeRule: ScopeRuleV1("multi-group-v1")}); codeOf(err) != ErrorUnsupportedScopeRuleV1 {
		t.Fatalf("scope mismatch err=%v code=%s", err, codeOf(err))
	}
}

func TestApplyResultV1VocabularyIsStable(t *testing.T) {
	statuses := []struct {
		name string
		got  ApplyStatusV1
		want string
	}{
		{"ApplyStatusApplied", ApplyStatusApplied, "applied"},
		{"ApplyStatusAlreadyApplied", ApplyStatusAlreadyApplied, "already-applied"},
		{"ApplyStatusDeterministicGuardFailure", ApplyStatusDeterministicGuardFailure, "deterministic-guard-failure"},
		{"ApplyStatusRejectedUnsupported", ApplyStatusRejectedUnsupported, "rejected-unsupported"},
		{"ApplyStatusRejectedMalformed", ApplyStatusRejectedMalformed, "rejected-malformed"},
		{"ApplyStatusRejectedConflict", ApplyStatusRejectedConflict, "rejected-conflict"},
		{"ApplyStatusRecoveryRequired", ApplyStatusRecoveryRequired, "recovery-required"},
	}
	for _, tc := range statuses {
		if string(tc.got) != tc.want {
			t.Fatalf("%s=%q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if ErrorNoneV1 != "" {
		t.Fatalf("ErrorNoneV1=%q, want empty success code", ErrorNoneV1)
	}
	errors := []struct {
		name string
		got  DeterministicErrorCodeV1
		want string
	}{
		{"ErrorUnsupportedCommandV1", ErrorUnsupportedCommandV1, "unsupported-command"},
		{"ErrorMalformedEntryV1", ErrorMalformedEntryV1, "malformed-entry"},
		{"ErrorUnsupportedVersionV1", ErrorUnsupportedVersionV1, "unsupported-version"},
		{"ErrorUnsupportedFeatureV1", ErrorUnsupportedFeatureV1, "unsupported-feature"},
		{"ErrorMissingGuardV1", ErrorMissingGuardV1, "missing-guard"},
		{"ErrorTargetMismatchV1", ErrorTargetMismatchV1, "target-mismatch"},
		{"ErrorRejectedConflictV1", ErrorRejectedConflictV1, "rejected-conflict"},
		{"ErrorReadOnlyV1", ErrorReadOnlyV1, "read-only"},
		{"ErrorUnsafeDurabilityModeV1", ErrorUnsafeDurabilityModeV1, "unsafe-durability-mode"},
		{"ErrorResourceExhaustedV1", ErrorResourceExhaustedV1, "resource-exhausted"},
		{"ErrorNoIdempotencyV1", ErrorNoIdempotencyV1, "no-idempotency"},
		{"ErrorResultReplayRequiredV1", ErrorResultReplayRequiredV1, "result-replay-required"},
		{"ErrorUnknownRequiredFieldV1", ErrorUnknownRequiredFieldV1, "unknown-required-field"},
		{"ErrorUnsupportedScopeRuleV1", ErrorUnsupportedScopeRuleV1, "unsupported-scope-rule"},
	}
	for _, tc := range errors {
		if string(tc.got) != tc.want {
			t.Fatalf("%s=%q, want %q", tc.name, tc.got, tc.want)
		}
	}
}

func TestR3aAllowlistCoversNativeWireV1CommandsAndDocsMatrix(t *testing.T) {
	rowsByNative := make(map[string]CommandRowV1)
	for _, row := range AllCommandRowsV1() {
		if !row.Known || row.NativeWireCommand == "" || row.CommandName == "" || row.CommandWALKind == "" || row.CommandWALStatus == "" || row.Reason == "" {
			t.Fatalf("incomplete command row: %+v", row)
		}
		if _, exists := rowsByNative[row.NativeWireCommand]; exists {
			t.Fatalf("duplicate row for %s", row.NativeWireCommand)
		}
		rowsByNative[row.NativeWireCommand] = row
	}
	for _, schema := range nativewire.MustV1Registry().Schemas() {
		row := ClassifyNativeWireCommandV1(schema.ID)
		if !row.Known {
			t.Fatalf("missing row for schema %+v", schema)
		}
	}
	alignment := loadAlignment(t)
	alignmentByNative := make(map[string]alignmentEntry, len(alignment.Entries))
	for _, entry := range alignment.Entries {
		if _, exists := alignmentByNative[entry.NativeWireCommand]; exists {
			t.Fatalf("duplicate alignment entry for %s", entry.NativeWireCommand)
		}
		alignmentByNative[entry.NativeWireCommand] = entry
		row, ok := rowsByNative[entry.NativeWireCommand]
		if !ok {
			t.Fatalf("alignment entry %s has no R3a row", entry.NativeWireCommand)
		}
		if row.CommandWALStatus != entry.SupportMatrixStatus || row.CommandWALKind != entry.CommandWALKind {
			t.Fatalf("%s row=%+v alignment=%+v", entry.NativeWireCommand, row, entry)
		}
		if row.Decision == DecisionAccepted && entry.SupportMatrixStatus != "WAL-supported" {
			t.Fatalf("%s accepted without WAL-supported status", entry.NativeWireCommand)
		}
	}
	for command, row := range rowsByNative {
		entry, ok := alignmentByNative[command]
		if !ok {
			t.Fatalf("R3a row %s missing alignment entry", command)
		}
		if row.CommandWALStatus != entry.SupportMatrixStatus || row.CommandWALKind != entry.CommandWALKind {
			t.Fatalf("%s row=%+v alignment=%+v", command, row, entry)
		}
	}
	if row := ClassifyNativeWireCommandV1(nativewire.CommandCreateCollection); row.Decision != DecisionAccepted || row.DuplicateMode != DuplicateFailClosedAllowedV1 || row.ResultReplayMode != ResultReplayFailClosedV1 {
		t.Fatalf("create collection row=%+v", row)
	}
	if code := rowRejectionCodeV1(ClassifyNativeWireCommandV1(nativewire.CommandGetMany)); code != ErrorReadOnlyV1 {
		t.Fatalf("read-only row rejection code=%s, want %s", code, ErrorReadOnlyV1)
	}
}

func deterministicInsertEntry(t *testing.T, extra []nativewire.Section, commandFlags uint64) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandInsertBatch, Version: 1, Flags: commandFlags})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("id1")},
		{ID: nativewire.SectionCollectionRef, Bytes: []byte{1, 'c'}},
		{ID: nativewire.SectionDocumentFormat, Bytes: []byte{byte(nativewire.DocumentFormatBSON)}},
		{ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, []byte("a"))},
		{ID: nativewire.SectionDocuments, Bytes: nativewire.AppendByteVector(nil, []byte{5, 0, 0, 0, 0})},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)},
	}
	sections = append(sections, extra...)
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func deterministicCreateCollectionEntry(t *testing.T, idempotency string, includeGuard bool) []byte {
	t.Helper()
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: createCollectionMetaPayload("users")},
	}
	if includeGuard {
		sections = append(sections, nativewire.Section{ID: nativewire.SectionExpectedCatalogVersion, Bytes: uvarintPayload(7)})
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil && includeGuard {
		t.Fatalf("ValidateRequestSections: %v", err)
	}
	if err != nil {
		return appendDeterministicEntryRaw(nativewire.CommandCreateCollection, sections)
	}
	entry, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatalf("AppendDeterministicEntry: %v", err)
	}
	return entry
}

func createCollectionMetaPayload(name string) []byte {
	dst := uvarintPayload(1)
	dst = appendString(dst, name)
	dst = binary.AppendUvarint(dst, uint64(nativewire.DocumentFormatDefault))
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	dst = binary.AppendVarint(dst, 0)
	dst = binary.AppendUvarint(dst, 0)
	return dst
}

func appendString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendDeterministicEntryRaw(commandID nativewire.CommandID, sections []nativewire.Section) []byte {
	return appendDeterministicEntryRawWithHeader(nativewire.DeterministicEntryVersion, commandID, 1, 0, sections)
}

func appendDeterministicEntryRawWithHeader(entryVersion uint64, commandID nativewire.CommandID, commandVersion uint64, commandFlags uint64, sections []nativewire.Section) []byte {
	deterministic := make([]nativewire.Section, 0, len(sections))
	for _, section := range sections {
		switch section.ID {
		case nativewire.SectionCommandHeader:
			continue
		case nativewire.SectionDeadline, nativewire.SectionTraceContext, nativewire.SectionAckPolicy, nativewire.SectionConsistencyPolicy, nativewire.SectionChecksum, nativewire.SectionCompression, nativewire.SectionResponseMeta, nativewire.SectionCursorMeta:
			continue
		default:
			deterministic = append(deterministic, section)
		}
	}
	sortSections(deterministic)
	dst := []byte(nativewire.DeterministicEntryMagic)
	dst = binary.AppendUvarint(dst, entryVersion)
	dst = binary.AppendUvarint(dst, uint64(commandID))
	dst = binary.AppendUvarint(dst, commandVersion)
	dst = binary.AppendUvarint(dst, commandFlags)
	dst = binary.AppendUvarint(dst, uint64(len(deterministic)))
	for _, section := range deterministic {
		dst = binary.AppendUvarint(dst, uint64(section.ID))
		dst = binary.AppendUvarint(dst, uint64(len(section.Bytes)))
		dst = append(dst, section.Bytes...)
	}
	return dst
}

func sortSections(sections []nativewire.Section) {
	for i := 1; i < len(sections); i++ {
		for j := i; j > 0 && sections[j].ID < sections[j-1].ID; j-- {
			sections[j], sections[j-1] = sections[j-1], sections[j]
		}
	}
}

func uvarintPayload(value uint64) []byte {
	return binary.AppendUvarint(nil, value)
}

func codeOf(err error) DeterministicErrorCodeV1 {
	code, _ := ErrorCodeOf(err)
	return code
}

type alignmentManifest struct {
	Entries []alignmentEntry `json:"entries"`
}

type alignmentEntry struct {
	NativeWireCommand   string `json:"nativewire_command"`
	CommandWALKind      string `json:"command_wal_kind"`
	SupportMatrixStatus string `json:"support_matrix_status"`
}

func loadAlignment(t *testing.T) alignmentManifest {
	t.Helper()
	raw, err := os.ReadFile(repoPath(t, "../../docs/spec/command-wal-nativewire-alignment.json"))
	if err != nil {
		t.Fatalf("read alignment: %v", err)
	}
	var manifest alignmentManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode alignment: %v", err)
	}
	return manifest
}

func readHexFixture(t *testing.T, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(repoPath(t, rel))
	if err != nil {
		t.Fatalf("read fixture %s: %v", rel, err)
	}
	compact := bytes.Join(bytes.Fields(raw), nil)
	out, err := hex.DecodeString(string(compact))
	if err != nil {
		t.Fatalf("decode fixture %s: %v", rel, err)
	}
	return out
}

func repoPath(t *testing.T, rel string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), rel))
}
