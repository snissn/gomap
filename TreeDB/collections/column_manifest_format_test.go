package collections

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"strings"
	"testing"
	"unsafe"
)

func TestColumnManifestRecordDecodeAcceptsV1CompatibilityM1634(t *testing.T) {
	header := mustEncodeColumnManifestHeaderRecordVersionM1634(t, 1)
	decodedHeader, err := decodeColumnManifestHeaderRecord(header)
	if err != nil {
		t.Fatalf("decodeColumnManifestHeaderRecord v1: %v", err)
	}
	if decodedHeader.Generation != 7 || decodedHeader.ExpectedParts != 1 {
		t.Fatalf("unexpected v1 header: %+v", decodedHeader)
	}

	asset := ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  "events_column_assets",
			Generation: 7,
			PartID:     3,
			FileID:     1,
			Offset:     16,
			Length:     128,
			Checksum:   99,
		},
		Rows:         42,
		Bytes:        128,
		PublishID:    11,
		GenerationID: 7,
		Reason:       string(ColumnPublishOperationInsert),
	}
	part := mustEncodeColumnManifestPartRecordVersionM1634(t, asset, 1)
	decodedPart, err := decodeColumnManifestPartRecord(part)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRecord v1: %v", err)
	}
	if decodedPart.AssetRef != asset.Ref || decodedPart.Rows != 0 || decodedPart.Bytes != asset.Bytes || decodedPart.Reason != asset.Reason {
		t.Fatalf("unexpected v1 part decode: %+v", decodedPart)
	}

	records := []columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: header},
		{key: columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID), value: part},
	}
	snapshot, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestRecords v1: %v", err)
	}
	if len(snapshot.Parts) != 1 || snapshot.Parts[0].Rows != 0 {
		t.Fatalf("unexpected v1 snapshot parts: %+v", snapshot.Parts)
	}

	scanHeader, err := decodeColumnManifestHeaderRecordForScan(header)
	if err != nil {
		t.Fatalf("decodeColumnManifestHeaderRecordForScan v1: %v", err)
	}
	if scanHeader.generation != 7 || scanHeader.expectedParts != 1 {
		t.Fatalf("unexpected v1 scan header: %+v", scanHeader)
	}
	scanRef, reason, err := decodeColumnManifestPartRefForScan(part, asset.Ref.Namespace)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRefForScan v1: %v", err)
	}
	if scanRef != asset.Ref || string(reason) != asset.Reason {
		t.Fatalf("unexpected v1 scan part: ref=%+v reason=%q", scanRef, string(reason))
	}
	rewriteRef, _, err := columnAssetRewriteManifestPartRefForPatch(part, asset.Ref.Namespace)
	if err != nil {
		t.Fatalf("columnAssetRewriteManifestPartRefForPatch v1: %v", err)
	}
	if rewriteRef != asset.Ref {
		t.Fatalf("unexpected v1 rewrite ref: %+v", rewriteRef)
	}
}

func TestColumnManifestRecordDecodeSidecarOperationReasonDoesNotInferRole1787(t *testing.T) {
	for _, version := range []uint16{columnManifestRecordVersionV2, columnManifestRecordVersionV3, columnManifestRecordVersion} {
		t.Run("v"+string(rune('0'+version)), func(t *testing.T) {
			asset := ColumnPreparedAsset{
				Ref: ColumnAssetRef{
					Kind:       ColumnAssetKindTCS1Int64Values,
					Namespace:  "events_column_assets",
					Generation: 7,
					PartID:     4,
					FileID:     1,
					Offset:     16,
					Length:     64,
					Checksum:   99,
				},
				Rows:         42,
				Bytes:        64,
				PublishID:    11,
				GenerationID: 7,
				Reason:       string(ColumnPublishOperationInsert),
			}
			raw := mustEncodeColumnManifestPartRecordVersionM1634(t, asset, version)
			decoded, err := decodeColumnManifestPartRecord(raw)
			if err != nil {
				t.Fatalf("decodeColumnManifestPartRecord v%d: %v", version, err)
			}
			if decoded.PartRole != "" {
				t.Fatalf("decoded sidecar part role=%q want empty", decoded.PartRole)
			}
			rewriteRef, _, err := columnAssetRewriteManifestPartRefForPatch(raw, asset.Ref.Namespace)
			if err != nil {
				t.Fatalf("columnAssetRewriteManifestPartRefForPatch v%d: %v", version, err)
			}
			if rewriteRef != asset.Ref {
				t.Fatalf("rewrite ref=%+v want %+v", rewriteRef, asset.Ref)
			}
		})
	}
}

func TestColumnManifestPartRecordSortKeyV4Compatibility1948(t *testing.T) {
	asset := ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1TypedColumnPart,
			Namespace:  "events_column_assets",
			Generation: 9,
			PartID:     typedColumnPartAssetPartID,
			FileID:     1,
			Offset:     32,
			Length:     256,
			Checksum:   123,
		},
		Rows:         8,
		Bytes:        256,
		PublishID:    17,
		GenerationID: 9,
		Reason:       string(ColumnPublishOperationInsert),
		PartRole:     ColumnManifestPartRoleBase,
		SortKey:      columnSortKeyMatchString([]ColumnSortKey{{Column: "time_us"}, {Column: "did"}}),
	}

	current := mustEncodeColumnManifestPartRecordVersionM1634(t, asset, columnManifestRecordVersionV4)
	decoded, err := decodeColumnManifestPartRecord(current)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRecord v4: %v", err)
	}
	if !columnSortKeysEqual(decoded.SortKey, []ColumnSortKey{{Column: "time_us"}, {Column: "did"}}) {
		t.Fatalf("decoded sort key=%+v", decoded.SortKey)
	}
	scanSortKey, err := decodeColumnManifestPartSortKeyForScan(current)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartSortKeyForScan v4: %v", err)
	}
	if !columnSortKeysEqual(scanSortKey, decoded.SortKey) {
		t.Fatalf("scan sort key=%+v want %+v", scanSortKey, decoded.SortKey)
	}
	ref, rows, bytesN, publishID, generationID, reason, err := decodeColumnManifestPartFieldsForScan(current, asset.Ref.Namespace)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartFieldsForScan v4: %v", err)
	}
	if ref != asset.Ref || rows != asset.Rows || bytesN != asset.Bytes || publishID != asset.PublishID || generationID != asset.GenerationID || string(reason) != asset.Reason {
		t.Fatalf("scan fields ref=%+v rows=%d bytes=%d publish=%d generation=%d reason=%q", ref, rows, bytesN, publishID, generationID, string(reason))
	}
	role, err := decodeColumnManifestPartRoleForScan(current, asset.Ref, []byte(asset.Reason))
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRoleForScan v4: %v", err)
	}
	if role != asset.PartRole {
		t.Fatalf("scan role=%q want %q", role, asset.PartRole)
	}
	rewriteRef, _, err := columnAssetRewriteManifestPartRefForPatch(current, asset.Ref.Namespace)
	if err != nil {
		t.Fatalf("columnAssetRewriteManifestPartRefForPatch v4: %v", err)
	}
	if rewriteRef != asset.Ref {
		t.Fatalf("rewrite ref=%+v want %+v", rewriteRef, asset.Ref)
	}

	v3 := mustEncodeColumnManifestPartRecordVersionM1634(t, asset, columnManifestRecordVersionV3)
	decodedV3, err := decodeColumnManifestPartRecord(v3)
	if err != nil {
		t.Fatalf("decodeColumnManifestPartRecord v3: %v", err)
	}
	if len(decodedV3.SortKey) != 0 {
		t.Fatalf("v3 sort key=%+v want empty", decodedV3.SortKey)
	}
	if _, _, _, _, _, _, err := decodeColumnManifestPartFieldsForScan(v3, asset.Ref.Namespace); err != nil {
		t.Fatalf("decodeColumnManifestPartFieldsForScan v3: %v", err)
	}
	if role, err := decodeColumnManifestPartRoleForScan(v3, asset.Ref, []byte(asset.Reason)); err != nil || role != asset.PartRole {
		t.Fatalf("decodeColumnManifestPartRoleForScan v3 role=%q err=%v", role, err)
	}
	if sortKey, err := decodeColumnManifestPartSortKeyForScan(v3); err != nil || len(sortKey) != 0 {
		t.Fatalf("decodeColumnManifestPartSortKeyForScan v3 sort_key=%+v err=%v", sortKey, err)
	}
}

func TestColumnManifestPartRecordRejectsSortKeyOnNonTypedAsset1948(t *testing.T) {
	asset := ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  "events_column_assets",
			Generation: 9,
			PartID:     1,
			FileID:     1,
			Offset:     32,
			Length:     256,
			Checksum:   123,
		},
		Rows:         8,
		Bytes:        256,
		PublishID:    17,
		GenerationID: 9,
		Reason:       string(ColumnPublishOperationInsert),
		PartRole:     ColumnManifestPartRoleBase,
		SortKey:      columnSortKeyMatchString([]ColumnSortKey{{Column: "time_us"}}),
	}
	raw := mustEncodeColumnManifestPartRecordVersionM1634(t, asset, columnManifestRecordVersionV4)
	if _, err := decodeColumnManifestPartRecord(raw); err == nil || !strings.Contains(err.Error(), "sort key") {
		t.Fatalf("decodeColumnManifestPartRecord err=%v want sort-key rejection", err)
	}
}

func mustEncodeColumnManifestHeaderRecordVersionM1634(t *testing.T, version uint16) []byte {
	t.Helper()
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestHeaderMagic)
	writeManifestUint16(&b, version)
	writeManifestString(&b, "events")
	writeManifestString(&b, string(ColumnPublishOperationInsert))
	writeManifestUint64(&b, 7)
	writeManifestUint64(&b, 123)
	writeManifestUint64(&b, 456)
	writeManifestUint64(&b, 42)
	writeManifestUint64(&b, 100)
	writeManifestUint64(&b, 20)
	writeManifestUint64(&b, 80)
	writeManifestUint64(&b, 1)
	return b.Bytes()
}

func mustEncodeColumnManifestPartRecordVersionM1634(t *testing.T, asset ColumnPreparedAsset, version uint16) []byte {
	t.Helper()
	var b bytes.Buffer
	writeManifestUint32(&b, columnManifestPartMagic)
	writeManifestUint16(&b, version)
	writeManifestString(&b, string(asset.Ref.Kind))
	writeManifestString(&b, asset.Ref.Namespace)
	writeManifestUint64(&b, asset.Ref.Generation)
	writeManifestUint64(&b, asset.Ref.PartID)
	writeManifestUint64(&b, uint64(asset.Ref.FileID))
	writeManifestUint64(&b, uint64(asset.Ref.Offset))
	writeManifestUint64(&b, uint64(asset.Ref.Length))
	writeManifestUint64(&b, uint64(asset.Ref.Checksum))
	if version >= columnManifestRecordVersionV2 {
		writeManifestUint64(&b, uint64(asset.Rows))
	}
	writeManifestUint64(&b, uint64(asset.Bytes))
	writeManifestUint64(&b, asset.PublishID)
	writeManifestUint64(&b, asset.GenerationID)
	writeManifestString(&b, asset.Reason)
	if version >= columnManifestRecordVersionV3 {
		writeManifestString(&b, string(asset.PartRole))
	}
	if version >= columnManifestRecordVersionV4 {
		sortKey, err := columnSortKeysFromMatchString(asset.SortKey)
		if err != nil {
			t.Fatalf("columnSortKeysFromMatchString: %v", err)
		}
		writeColumnManifestSortKey(&b, sortKey)
	}
	return b.Bytes()
}

// Actual changed records, rather than source length, determine the owned
// mutation backing. Deletes clone only keys; unchanged values create nothing.
func TestColumnManifestMutationPrebirthOwnsExactMerge(t *testing.T) {
	current := []columnManifestRecord{{key: []byte("a"), value: []byte("old")}, {key: []byte("b"), value: []byte("same")}, {key: []byte("d"), value: []byte("removed")}}
	next := []columnManifestRecord{{key: []byte("a"), value: []byte("new")}, {key: []byte("b"), value: []byte("same")}, {key: []byte("c"), value: []byte("added")}}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 1 << 20, 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	account := &credit.facets[nativeRequestSourceCredit]
	before := credit.snapshot().Debited[nativeRequestSourceCredit]
	mutations, err := buildColumnManifestMutationDeltaWithMetadataAccount(current, next, account)
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations) != 3 || cap(mutations) != 3 ||
		string(mutations[0].record.key) != "a" || mutations[0].deleted ||
		string(mutations[1].record.key) != "c" || mutations[1].deleted ||
		string(mutations[2].record.key) != "d" || !mutations[2].deleted {
		t.Fatal("merge semantics changed", mutations)
	}
	var expected uint64
	for _, raw := range []uint64{3 * uint64(unsafe.Sizeof(columnManifestMutation{})), 1, 3, 1, 5, 1} {
		scan := raw == 3*uint64(unsafe.Sizeof(columnManifestMutation{}))
		class, err := rootpublication.StableBackingClassBytes(raw, scan)
		if err != nil {
			t.Fatal(err)
		}
		expected += class
	}
	if debit := credit.snapshot().Debited[nativeRequestSourceCredit] - before; debit != expected {
		t.Fatalf("actual individual allocation classes = %d want %d", debit, expected)
	}
	next[0].key[0], next[0].value[0], current[2].key[0] = 'z', 'z', 'z'
	if string(mutations[0].record.key) != "a" || string(mutations[0].record.value) != "new" || string(mutations[2].record.key) != "d" {
		t.Fatal("owned mutation aliases input")
	}
}

func TestColumnManifestMutationRefusalBeforeBackingBirth(t *testing.T) {
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 1 << 20, 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	account := &credit.facets[nativeRequestSourceCredit]
	before := credit.snapshot()
	bad := []columnManifestRecord{{key: []byte("b"), value: []byte("b")}, {key: []byte("a"), value: []byte("a")}}
	if mutations, err := buildColumnManifestMutationDeltaWithMetadataAccount(nil, bad, account); err == nil || mutations != nil {
		t.Fatal("invalid ordering constructed owned backing")
	}
	if credit.snapshot() != before {
		t.Fatal("invalid complete input changed constructor credit")
	}
	// A real valid merge whose complete debit is refused creates no partial
	// cloned key/value or output capacity and does not change cumulative debit.
	credit.mu.Lock()
	credit.limits[nativeRequestSourceCredit] = credit.used[nativeRequestSourceCredit]
	credit.mu.Unlock()
	before = credit.snapshot()
	valid := []columnManifestRecord{{key: []byte("a"), value: []byte("new")}}
	if mutations, err := buildColumnManifestMutationDeltaWithMetadataAccount(nil, valid, account); !errors.Is(err, ErrPreparedInsertResourceLimit) || mutations != nil {
		t.Fatal("denied merge exposed partial output", err)
	}
	if credit.snapshot() != before {
		t.Fatal("denied complete debit changed credit")
	}
}

func TestColumnManifestPrebirthCodecPreservesBytes(t *testing.T) {
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 1 << 20, 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	account := &credit.facets[nativeRequestSourceCredit]
	asset := ColumnPreparedAsset{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "events_column_assets", Generation: 7, PartID: 3, FileID: 1, Offset: 16, Length: 128, Checksum: 99}, Rows: 42, Bytes: 128, PublishID: 123, GenerationID: 7, Reason: string(ColumnPublishOperationInsert), PartRole: ColumnManifestPartRoleBase, SortKey: columnSortKeyMatchString([]ColumnSortKey{{Column: "time_us"}, {Column: "did"}})}
	headerInput := ColumnPublishManifestEncodeInput{Collection: "events", Operation: ColumnPublishOperationInsert, AppliedCommandLSN: 123, ColumnStore: ColumnStoreConfig{SchemaHash: 456}, Prepared: ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset}, RowCount: 42, CommandBytes: 100, RowRemainderBytes: 20, ColumnPayloadBytes: 80}, metadataAccount: account}
	header, err := encodeColumnManifestHeaderRecord(headerInput, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(header, mustEncodeColumnManifestHeaderRecordVersionM1634(t, columnManifestRecordVersion)) || cap(header) != len(header) {
		t.Fatal("header grammar/capacity changed")
	}
	part, err := encodeColumnManifestPartRecordWithMetadataAccount(asset, account)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(part, mustEncodeColumnManifestPartRecordVersionM1634(t, asset, columnManifestRecordVersion)) || cap(part) != len(part) {
		t.Fatal("part grammar/capacity changed")
	}
	before := credit.snapshot()
	asset.SortKey = "unterminated"
	if raw, err := encodeColumnManifestPartRecordWithMetadataAccount(asset, account); err == nil || raw != nil {
		t.Fatal("malformed sort metadata allocated output")
	}
	if credit.snapshot() != before {
		t.Fatal("malformed sort metadata debited output")
	}
	credit.mu.Lock()
	credit.limits[nativeRequestSourceCredit] = credit.used[nativeRequestSourceCredit]
	credit.mu.Unlock()
	if raw, err := encodeColumnManifestHeaderRecord(headerInput, 7); !errors.Is(err, ErrPreparedInsertResourceLimit) || raw != nil {
		t.Fatal("denied header exposed owned bytes", err)
	}
}
