package collections

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
)

func ownedManifestDecodeFixture(t *testing.T) []columnManifestRecord {
	t.Helper()
	base := ColumnPreparedAsset{Ref: ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: "events_column_assets", Generation: 7, PartID: 3, FileID: 1, Offset: 16, Length: 128, Checksum: 99}, Rows: 42, Bytes: 128, PublishID: 123, GenerationID: 7, Reason: string(ColumnPublishOperationUpdate), PartRole: ColumnManifestPartRoleDelta, SortKey: columnSortKeyMatchString([]ColumnSortKey{{Column: "time_us", Direction: ColumnSortAscending}, {Column: "did", Direction: ColumnSortAscending}})}
	header, err := encodeColumnManifestHeaderRecord(ColumnPublishManifestEncodeInput{Collection: "events", Operation: ColumnPublishOperationUpdate, AppliedCommandLSN: 123, ColumnStore: ColumnStoreConfig{SchemaHash: 456}, Prepared: ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{base}}}, 7)
	if err != nil {
		t.Fatal(err)
	}
	records := []columnManifestRecord{{key: []byte(columnManifestHeaderRecordKey), value: header}}
	for i, kind := range []ColumnAssetKind{ColumnAssetKindTCS1TypedColumnPart, ColumnAssetKindTCS1AggregateMetadata, ColumnAssetKindTCS1DictionaryCodes, ColumnAssetKindTCS1Int64Values} {
		asset := base
		asset.Ref.Kind = kind
		asset.Ref.Offset += int64(i) * 128
		var key []byte
		if i == 0 {
			key = columnManifestPartRecordKey(7, 3)
		} else {
			asset.Reason = "time_us"
			asset.PartRole = ""
			asset.SortKey = ""
			switch kind {
			case ColumnAssetKindTCS1AggregateMetadata:
				key = columnManifestAggregateMetadataRecordKey(7, 3, asset.Reason)
			case ColumnAssetKindTCS1DictionaryCodes:
				key = columnManifestDictionaryCodesRecordKey(7, 3, asset.Reason)
			case ColumnAssetKindTCS1Int64Values:
				key = columnManifestInt64ValuesRecordKey(7, 3, asset.Reason)
			}
		}
		raw, err := encodeColumnManifestPartRecord(asset)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, columnManifestRecord{key: key, value: raw})
	}
	marker := columnManifestSegmentOwnership{Ref: base.Ref, Frontier: uint64(base.Ref.Offset + base.Ref.Length)}
	raw, err := encodeColumnManifestSegmentOwnership(marker)
	if err != nil {
		t.Fatal(err)
	}
	records = append(records, columnManifestRecord{key: columnManifestSegmentOwnershipRecordKey(marker.Ref.FileID), value: raw})
	sortColumnManifestRecords(records)
	return records
}

func TestColumnManifestOwnedDecodePreservesAuxSortAndOwnership(t *testing.T) {
	records := ownedManifestDecodeFixture(t)
	ordinary, err := decodeColumnManifestRecords(records)
	if err != nil {
		t.Fatal(err)
	}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{16 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	facet := &credit.facets[nativeRequestSourceCredit]
	before := credit.snapshot()
	owned, err := decodeColumnManifestRecordsWithMetadataAccount(records, facet)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ordinary, owned) {
		t.Fatalf("changed decoded grammar: ordinary=%+v owned=%+v", ordinary, owned)
	}
	if len(owned.AggregateMetadata) != 1 || len(owned.DictionaryCodes) != 1 || len(owned.Int64Values) != 1 || len(owned.SegmentOwnership) != 1 || len(owned.Parts[0].SortKey) != 2 {
		t.Fatal("accepted row metadata family disappeared")
	}
	if credit.snapshot().Debited[nativeRequestSourceCredit] <= before.Debited[nativeRequestSourceCredit] {
		t.Fatal("actual owned decoder had no debit")
	}
	// Every decoded string/array remains owned after source records are scrubbed.
	for _, r := range records {
		clear(r.key)
		clear(r.value)
	}
	if !reflect.DeepEqual(ordinary, owned) {
		t.Fatal("decoder retained caller record aliases")
	}
}

func TestColumnManifestOwnedCopyAdmitsBeforeCloningAndPreservesAliases(t *testing.T) {
	records := ownedManifestDecodeFixture(t)
	entries := make([]systemTargetEntry, len(records))
	for i, r := range records {
		entries[i] = systemTargetEntry{key: r.key, value: r.value, flags: node.FlagInline}
	}
	iter := &systemTargetIterator{entries: entries}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{16 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	facet := &credit.facets[nativeRequestSourceCredit]
	before := credit.snapshot()
	credit.mu.Lock()
	credit.limits[nativeRequestSourceCredit] = credit.used[nativeRequestSourceCredit]
	credit.mu.Unlock()
	if copied, err := loadColumnManifestRecordsFromIterator(iter, facet); !errors.Is(err, ErrPreparedInsertResourceLimit) || copied != nil {
		t.Fatalf("denied copy exposed result: %v", err)
	}
	if credit.snapshot().Debited != before.Debited {
		t.Fatal("denied complete copy changed debit")
	}
	credit.mu.Lock()
	credit.limits[nativeRequestSourceCredit] = 16 << 20
	credit.mu.Unlock()
	copied, err := loadColumnManifestRecordsFromIterator(iter, facet)
	if err != nil {
		t.Fatal(err)
	}
	if len(copied) != len(records) || cap(copied) != len(copied) {
		t.Fatal("actual exact record capacity changed")
	}
	for i, r := range records {
		if !bytes.Equal(copied[i].key, r.key) || !bytes.Equal(copied[i].value, r.value) {
			t.Fatal("copy differs")
		}
	}
	expected, err := decodeColumnManifestRecords(copied)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		clear(r.key)
		clear(r.value)
	}
	got, err := decodeColumnManifestRecords(copied)
	if err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatalf("owned copy retained source bytes: %v", err)
	}
}

func TestColumnManifestOwnedCopyPreservesIndependentIdentitySeekAndUnknownStop(t *testing.T) {
	records := ownedManifestDecodeFixture(t)
	identity := ColumnManifestIdentity{Format: columnManifestFormatTCS1, Version: 1, Generation: 7, Checksum: 123}
	encoded := encodeColumnManifestIdentityRecord(identity)
	entries := []systemTargetEntry{
		{key: columnManifestIdentityProjectionKey, value: encoded, flags: node.FlagInline},
		// Existing identity lookup and manifest scan are separate seeks. This
		// unrelated key must neither suppress the header nor become a record.
		{key: []byte("\x00unrelated-between-identity-and-header"), value: []byte("outside"), flags: node.FlagInline},
	}
	for _, r := range records {
		entries = append(entries, systemTargetEntry{key: r.key, value: r.value, flags: node.FlagInline})
	}
	entries = append(entries, systemTargetEntry{key: []byte("\x07unknown-stop"), value: []byte("outside"), flags: node.FlagInline})
	it := &systemTargetIterator{entries: entries}
	copies, err := iterator.CopyOwnedInlineRecords(it, columnManifestInlineRecordSelection(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer iterator.ClearOwnedInlineRecords(copies)
	if len(copies) != len(records)+1 {
		t.Fatalf("independent seek changed manifest closure: %d", len(copies))
	}
	if err := validateColumnManifestIdentityFromOwnedRecords(copies, identity); err != nil {
		t.Fatal(err)
	}
	for i, r := range records {
		if !bytes.Equal(copies[i+1].Key, r.key) || !bytes.Equal(copies[i+1].Value, r.value) {
			t.Fatal("shared grammar changed selected bytes")
		}
	}
	entries[0].flags = node.FlagTombstone
	if output, err := iterator.CopyOwnedInlineRecords(it, columnManifestInlineRecordSelection(true), nil); !errors.Is(err, iterator.ErrInlineRecordMissing) || output != nil {
		t.Fatalf("deleted identity escaped as success: %v", err)
	}
}

type changingOwnedManifestIterator struct {
	*systemTargetIterator
	seeks int
}

func (it *changingOwnedManifestIterator) Seek(key []byte) {
	it.seeks++
	if it.seeks == 2 {
		it.entries[len(it.entries)-1].value = []byte("changed after complete birth admission")
	}
	it.systemTargetIterator.Seek(key)
}
func TestColumnManifestOwnedCopyRejectsChangedPinnedClosure(t *testing.T) {
	records := ownedManifestDecodeFixture(t)
	entries := make([]systemTargetEntry, len(records))
	for i, r := range records {
		entries[i] = systemTargetEntry{key: r.key, value: r.value, flags: node.FlagInline}
	}
	it := &changingOwnedManifestIterator{systemTargetIterator: &systemTargetIterator{entries: entries}}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{16 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	before := credit.snapshot()
	output, err := loadColumnManifestRecordsFromIterator(it, &credit.facets[nativeRequestSourceCredit])
	if !errors.Is(err, errColumnManifestReadChanged) || output != nil {
		t.Fatalf("changed loan escaped as successful copies: %v", err)
	}
	if credit.snapshot().Debited[nativeRequestSourceCredit] <= before.Debited[nativeRequestSourceCredit] {
		t.Fatal("failed admitted birth was refunded")
	}
	if !bytes.Equal(records[0].key, columnManifestHeaderRecordKeyBytes) {
		t.Fatal("failure scrubbed source aliases")
	}
}
