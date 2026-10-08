package collections

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/page"
	"os"
	"strconv"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnPhysicalImagePreparationHasNoAssetEffects(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	rows := []columnDeclaredRow{{ID: []byte("a"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "old value"}}}}
	input := columnWritePublishInput{meta: CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}, operation: ColumnPublishOperationUpdate, rows: 1, declaredRows: rows, declaredRowsReady: true}
	hook := ColumnPublishAssetPrepareInput{Collection: "events", ColumnStore: *cfg, Operation: ColumnPublishOperationUpdate, AppliedCommandLSN: 77}
	dir := t.TempDir()
	images, err := encodeColumnPhysicalAssetImagePlan(ColumnPublishPreparedAssets{}, input, hook, rows, nil, nil, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
	if err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("image preparation touched filesystem: %v %v", files, err)
	}
	if len(images.assets) != 1 || images.prepared.stableResources != nil || len(images.prepared.Assets) != 0 {
		t.Fatalf("image preparation installed physical authority: %+v", images)
	}
	payload := bytes.Clone(images.assets[0].payload)
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c := &Collection{db: d}
	prepared, err := c.installColumnPhysicalAssetImagePlan(images, input, hook)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.stableResources != nil {
		defer prepared.stableResources.Release()
	}
	if len(prepared.Assets) != 1 {
		t.Fatalf("installed assets=%d", len(prepared.Assets))
	}
	ref := prepared.Assets[0].Ref
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Offset < 0 || ref.Length != int64(len(payload)) || ref.Offset > int64(len(raw))-ref.Length ||
		!bytes.Equal(raw[ref.Offset:ref.Offset+ref.Length], payload) {
		t.Fatal("shared installer changed prepared image bytes")
	}
	// Mutating the caller's row after preparation cannot change the already
	// installed persistent byte image or the plan's owned encoded payload.
	rows[0].Values[0].String = "replacement"
	if !bytes.Equal(images.assets[0].payload, payload) {
		t.Fatal("prepared image aliases caller row value")
	}
}

func TestColumnPhysicalIdentityPreparationDoesNotBurnOrCreate(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	var cold columnPhysicalAssetIdentityReservation
	if err := prepareColumnPhysicalAssetIdentity(root, *cfg, &cold); err == nil {
		cold.release()
		t.Fatal("uncertified cold namespace was admitted")
	}
	files, err := os.ReadDir(root)
	if err != nil || len(files) != 0 {
		t.Fatalf("cold refusal created namespace: %v %v", files, err)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(root, *cfg)
	if err != nil {
		t.Fatal(err)
	}
	initial := appender.fileID
	if err := appender.close(); err != nil {
		t.Fatal(err)
	}
	var first columnPhysicalAssetIdentityReservation
	if err := prepareColumnPhysicalAssetIdentity(root, *cfg, &first); err != nil {
		t.Fatal(err)
	}
	predicted := first.fileID
	if predicted != initial+1 {
		first.release()
		t.Fatalf("tentative=%d previous=%d", predicted, initial)
	}
	before, err := os.ReadDir(first.namespace.SegmentDir)
	if err != nil {
		first.release()
		t.Fatal(err)
	}
	first.release()
	var second columnPhysicalAssetIdentityReservation
	if err := prepareColumnPhysicalAssetIdentity(root, *cfg, &second); err != nil {
		t.Fatal(err)
	}
	defer second.release()
	after, err := os.ReadDir(second.namespace.SegmentDir)
	if err != nil {
		t.Fatal(err)
	}
	if second.fileID != predicted || second.cache.nextFileID != predicted || len(after) != len(before) {
		t.Fatalf("refused preparation burned identity or created file: predicted=%d second=%d files=%d/%d", predicted, second.fileID, len(before), len(after))
	}
}

func TestColumnPhysicalIdentitySharedPlannerMatchesActualAlignedBytes(t *testing.T) {
	cfg := stableColumnAppendTestConfig("native-exact-addresses")
	root := t.TempDir()
	appender, err := newColumnPhysicalAssetSegmentAppender(root, cfg, 71)
	if err != nil {
		t.Fatal(err)
	}
	defer appender.close()
	items := []columnPhysicalAssetAppendItem{
		{payload: []byte("odd"), kind: ColumnAssetKindTCS1PartImage, generation: 7, partID: 1},
		{payload: []byte("aligned int64 payload"), kind: ColumnAssetKindTCS1Int64Values, generation: 7, partID: 2},
		{payload: []byte("dictionary codes"), kind: ColumnAssetKindTCS1DictionaryCodes, generation: 7, partID: 3},
	}
	limits := [nativeRequestCreditKinds]uint64{}
	limits[nativeRequestSourceCredit] = 1 << 20
	credit, err := newNativeRequestCredit(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	predicted, _, end, err := planColumnPhysicalAssetAppendRefs(cfg, 71, 0, items, true, &credit.facets[nativeRequestSourceCredit])
	if err != nil {
		t.Fatal(err)
	}
	actual, err := appender.appendKinds(items)
	if err != nil {
		t.Fatal(err)
	}
	if err := appender.close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(appender.assetPath)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(raw)) != end || len(actual) != len(predicted) {
		t.Fatal("actual file geometry changed the captured address plan")
	}
	for i, ref := range actual {
		if ref != predicted[i] || ref.Checksum != page.Checksum(items[i].payload) || !bytes.Equal(raw[ref.Offset:ref.Offset+ref.Length], items[i].payload) {
			t.Fatal("captured key/reference facts differ from exact installed bytes", i)
		}
		if alignment := columnAssetSegmentPayloadAlignment(items[i].kind, cfg); alignment > 1 && ref.Offset%alignment != 0 {
			t.Fatal("actual address lost native alignment")
		}
	}
	before := credit.snapshot().Debited[nativeRequestSourceCredit]
	malformed := []columnPhysicalAssetAppendItem{{payload: []byte("valid"), kind: ColumnAssetKindTCS1PartImage, generation: 7, partID: 4}, {payload: []byte("invalid"), kind: ColumnAssetKindTCS1PartImage, generation: 0, partID: 5}}
	if refs, _, _, err := planColumnPhysicalAssetAppendRefs(cfg, 71, end, malformed, true, &credit.facets[nativeRequestSourceCredit]); err == nil || refs != nil || credit.snapshot().Debited[nativeRequestSourceCredit] != before {
		t.Fatal("whole-image refusal debited/cloned an earlier ordinary address")
	}
}

func TestColumnPhysicalImageCreditedRowsPreserveBytesAndRefuseWholeInput(t *testing.T) {
	columns := []ColumnStoreColumn{
		{Name: "city", Path: "city", ValueType: ColumnStoreValueString},
		{Name: "rank", Path: "rank", ValueType: ColumnStoreValueInt64},
	}
	rows := []columnDeclaredRow{
		{ID: []byte("one"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: strings.Repeat("a", 1000)}, {Type: ColumnStoreValueInt64, Present: true, Int64: 42}}},
		{ID: []byte("two"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "Paris"}, {Type: ColumnStoreValueInt64, Present: true, Int64: -7}}},
	}
	input := columnPhysicalAssetEncodeInput{Collection: "events", Namespace: "native", Generation: 9, PartID: 1, AppliedCommandLSN: 12, Operation: ColumnPublishOperationUpdate, Columns: columns, Rows: rows}
	ordinary, _, err := encodeColumnPhysicalAsset(input)
	if err != nil {
		t.Fatal(err)
	}
	limits := [nativeRequestCreditKinds]uint64{}
	limits[nativeRequestSourceCredit] = 1 << 20
	credit, err := newNativeRequestCredit(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	facet := &credit.facets[nativeRequestSourceCredit]
	owned, summary, err := encodeColumnPhysicalAssetFromSourceWithMetadataAccount(input, columnDeclaredRowsSource(rows), facet)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ordinary, owned) || len(owned) != cap(owned) || summary.RowCount != 2 {
		t.Fatal("credited encoder changed installed grammar or retained spare buffer capacity")
	}
	decoded, err := decodeColumnPhysicalAsset(owned)
	if err != nil || len(decoded.Rows) != 2 || decoded.Rows[0].Values[0].String != rows[0].Values[0].String || decoded.Rows[1].Values[1].Int64 != -7 {
		t.Fatalf("credited row bytes do not round trip: %v", err)
	}
	// A malformed later row must be refused before any output/control debit.
	before := credit.snapshot().Debited
	rows[1].Values[1].Type = ColumnStoreValueString
	payload, _, err := encodeColumnPhysicalAssetFromSourceWithMetadataAccount(input, columnDeclaredRowsSource(rows), facet)
	if !errors.Is(err, ErrPreparedInsertResourceLimit) || payload != nil || credit.snapshot().Debited != before {
		t.Fatal("later invalid row escaped whole-input preflight")
	}
	rows[1].Values[1].Type = ColumnStoreValueInt64
	// Payload refusal is cumulative; a failed attempt cannot manufacture output
	// backing, nor refund the concrete call-local controls already admitted.
	constrainedLimits := [nativeRequestCreditKinds]uint64{}
	constrainedLimits[nativeRequestSourceCredit] = 512
	constrained, err := newNativeRequestCredit(constrainedLimits)
	if err != nil {
		t.Fatal(err)
	}
	defer constrained.retire()
	payload, _, err = encodeColumnPhysicalAssetFromSourceWithMetadataAccount(input, columnDeclaredRowsSource(rows), &constrained.facets[nativeRequestSourceCredit])
	if !errors.Is(err, ErrPreparedInsertResourceLimit) || payload != nil || constrained.snapshot().Debited[nativeRequestSourceCredit] <= 0 {
		t.Fatal("failed payload birth was not a cumulative credited attempt")
	}
}

func TestColumnPhysicalImageCreditedSparseAndFixedIDRows(t *testing.T) {
	cases := []columnPhysicalAssetEncodeInput{
		{Collection: "events", Namespace: "native", Generation: 9, PartID: 1, AppliedCommandLSN: 12, Operation: ColumnPublishOperationUpdate,
			Columns: []ColumnStoreColumn{{Name: "city", Path: "city", ValueType: ColumnStoreValueString}, {Name: "bio", Path: "bio", ValueType: ColumnStoreValueString}},
			Rows:    []columnDeclaredRow{{ID: []byte("x"), Stored: []bool{false, true}, Values: []columnDeclaredValue{{}, {Type: ColumnStoreValueString, Present: true, String: "changed"}}}}},
		{Collection: "events", Namespace: "native", Generation: 9, PartID: 1, AppliedCommandLSN: 12, Operation: ColumnPublishOperationDelete,
			Rows: []columnDeclaredRow{{ID: []byte("aa"), Deleted: true}, {ID: []byte("bb"), Deleted: true}}},
	}
	for _, input := range cases {
		ordinary, _, err := encodeColumnPhysicalAsset(input)
		if err != nil {
			t.Fatal(err)
		}
		limits := [nativeRequestCreditKinds]uint64{}
		limits[nativeRequestSourceCredit] = 1 << 20
		credit, err := newNativeRequestCredit(limits)
		if err != nil {
			t.Fatal(err)
		}
		owned, _, err := encodeColumnPhysicalAssetFromSourceWithMetadataAccount(input, columnDeclaredRowsSource(input.Rows), &credit.facets[nativeRequestSourceCredit])
		credit.retire()
		if err != nil || !bytes.Equal(ordinary, owned) {
			t.Fatalf("credited sparse/fixed-ID grammar: %v", err)
		}
		if _, err := decodeColumnPhysicalAsset(owned); err != nil {
			t.Fatal(err)
		}
	}
}

// Exercise the actual native row-only image family, including the combined
// four-by-32 request shape. It shares ordinary bytes; credits cannot authorize
// a typed/cold family or a malformed later row.
func TestColumnPhysicalImageCreditedNativeRowFamily(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("native-row-images", &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "city", Path: "city", ValueType: ColumnStoreValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 32, 128} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			rows := make([]columnDeclaredRow, count)
			for i := range rows {
				rows[i] = columnDeclaredRow{ID: []byte(strconv.Itoa(i)), Stored: []bool{true}, Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "Honolulu"}}}
			}
			hook := ColumnPublishAssetPrepareInput{Collection: "native-row-images", ColumnStore: *cfg, Operation: ColumnPublishOperationUpdate, AppliedCommandLSN: 77}
			input := columnWritePublishInput{operation: ColumnPublishOperationUpdate, sparseOnly: true, meta: CollectionMeta{Name: hook.Collection, Options: CollectionOptions{ColumnStore: cfg}}}
			plain, err := encodeColumnPhysicalAssetImagePlan(ColumnPublishPreparedAssets{}, input, hook, rows, nil, nil, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
			if err != nil {
				t.Fatal(err)
			}
			limits := [nativeRequestCreditKinds]uint64{}
			limits[nativeRequestSourceCredit] = 1 << 20
			credit, err := newNativeRequestCredit(limits)
			if err != nil {
				t.Fatal(err)
			}
			defer credit.retire()
			input.nativeSource = &credit.facets[nativeRequestSourceCredit]
			owned, err := encodeColumnPhysicalAssetImagePlan(ColumnPublishPreparedAssets{}, input, hook, rows, nil, nil, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
			if err != nil {
				t.Fatal(err)
			}
			if len(owned.assets) != 1 || cap(owned.assets) != 1 || owned.assets[0].fileID != columnAssetM12ASegmentFileID ||
				!bytes.Equal(owned.assets[0].payload, plain.assets[0].payload) || owned.prepared.stableResources != nil || owned.projectedRefs != nil {
				t.Fatal("credited native image changed shared-segment bytes/geometry or manufactured installed authority")
			}
			before := credit.snapshot().Debited[nativeRequestSourceCredit]
			rows[count-1].Values = nil
			if _, err := encodeColumnPhysicalAssetImagePlan(ColumnPublishPreparedAssets{}, input, hook, rows, nil, nil, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID); err == nil ||
				credit.snapshot().Debited[nativeRequestSourceCredit] != before {
				t.Fatal("later malformed native row caused an earlier output/control birth")
			}
			rows[count-1].Values = []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "Honolulu"}}
			input.operation = ColumnPublishOperationInsert
			hook.Operation = ColumnPublishOperationInsert
			if _, err := encodeColumnPhysicalAssetImagePlan(ColumnPublishPreparedAssets{}, input, hook, rows, nil, nil, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID); err == nil ||
				credit.snapshot().Debited[nativeRequestSourceCredit] != before {
				t.Fatal("cold sidecar constructor family was reached by native image authority")
			}
		})
	}
}

func TestColumnPhysicalImageIdentityFreeBulkMatchesActualBoundIdentity(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "city", Path: "city", ValueType: ColumnStoreValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	rows := []columnDeclaredRow{{ID: []byte("one"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "Paris"}}, Stored: []bool{true}}}
	limits := [nativeRequestCreditKinds]uint64{}
	limits[nativeRequestSourceCredit] = 1 << 20
	credit, err := newNativeRequestCredit(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	input := columnWritePublishInput{meta: CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}, operation: ColumnPublishOperationUpdate, sparseOnly: true, rows: 1, declaredRows: rows, declaredRowsReady: true, nativeSource: &credit.facets[nativeRequestSourceCredit]}
	hook := ColumnPublishAssetPrepareInput{Collection: "events", ColumnStore: *cfg, Operation: ColumnPublishOperationUpdate}
	images, err := encodeNativeColumnPhysicalImagesBeforeIdentity(ColumnPublishPreparedAssets{}, input, hook, rows, 7, columnPhysicalRowAssetPartID)
	if err != nil {
		t.Fatal(err)
	}
	if !images.unboundCommandIdentity || images.commandLSN != 0 || images.projectedRefs != nil || images.prepared.stableResources != nil {
		t.Fatal("bulk preparation acquired tentative or installed identity")
	}
	untouched := bytes.Clone(images.assets[0].payload)
	bad := hook
	bad.Collection = "foreign"
	bad.AppliedCommandLSN = 91
	before := credit.snapshot().Debited[nativeRequestSourceCredit]
	if err := bindNativeColumnPhysicalImagesCommandIdentity(&images, bad); err == nil || !bytes.Equal(untouched, images.assets[0].payload) || credit.snapshot().Debited[nativeRequestSourceCredit] != before {
		t.Fatal("foreign key context changed unpublished bytes or acquired credit")
	}
	hook.AppliedCommandLSN = 91
	if err := bindNativeColumnPhysicalImagesCommandIdentity(&images, hook); err != nil {
		t.Fatal(err)
	}
	if images.unboundCommandIdentity || images.commandLSN != 91 || credit.snapshot().Debited[nativeRequestSourceCredit] != before {
		t.Fatal("late scalar binding grew backing or left unavailable identity")
	}
	ordinary := input
	ordinary.nativeSource = nil
	expected, err := encodeColumnPhysicalAssetImagePlan(ColumnPublishPreparedAssets{}, ordinary, hook, rows, nil, nil, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
	if err != nil {
		t.Fatal(err)
	}
	if len(expected.assets) != 1 || !bytes.Equal(expected.assets[0].payload, images.assets[0].payload) {
		t.Fatal("late identity binding changed the existing installed byte grammar")
	}
	bound := bytes.Clone(images.assets[0].payload)
	hook.AppliedCommandLSN++
	if err := bindNativeColumnPhysicalImagesCommandIdentity(&images, hook); err == nil || !bytes.Equal(bound, images.assets[0].payload) {
		t.Fatal("bound physical image was reassigned to another command identity")
	}
}
