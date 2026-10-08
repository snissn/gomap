package collections

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestColumnPreAppendLocatorMatchesOrdinaryBytes(t *testing.T) {
	old := columnRowCoordinates{Generation: 2, PartID: 1, RowIndex: 9, AppliedCommandLSN: 3}
	cases := []struct {
		name string
		op   ColumnPublishOperation
		docs []columnWriteDocument
	}{
		{"CRL1", ColumnPublishOperationUpdate, []columnWriteDocument{{ID: []byte("a")}}},
		{"CRL2", ColumnPublishOperationUpdate, []columnWriteDocument{{ID: []byte("a"), preserved: &old}}},
		{"CRL3", ColumnPublishOperationUpdate, []columnWriteDocument{{ID: []byte("a"), fieldSources: []columnRowCoordinates{old, {}, old}}}},
		{"delete", ColumnPublishOperationDelete, []columnWriteDocument{{ID: []byte("a")}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			op := columnRowLocatorOperation{rows: len(tc.docs), operation: tc.op, generation: 7, commandLSN: 11}
			ordinary, cleanup, err := buildColumnPrimaryRowLocatorDeltaBatch(ColumnPublishPlan{Rows: op.rows, Operation: op.operation, UpdatedActiveManifest: ColumnManifestIdentity{Generation: op.generation}, AppliedCommandLSN: op.commandLSN}, tc.docs, 9, backenddb.OrderedRootStorageDefault)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanup()
			credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
			if err != nil {
				t.Fatal(err)
			}
			defer credit.retire()
			owned, err := buildColumnPrimaryRowLocatorOwnedBatch(op, tc.docs, 9, backenddb.OrderedRootStorageDefault, &credit.facets[nativeRequestSourceCredit])
			if err != nil {
				t.Fatal(err)
			}
			defer owned.Delta.Close()
			assertColumnOperationBatchEqual(t, ordinary.Delta, owned.Delta)
			// The owned locator's keys survive caller input mutation.
			tc.docs[0].ID[0] = 'z'
			if string(owned.Delta.SortedEntries()[0].Key) != "a" {
				t.Fatal("locator borrowed input key")
			}
		})
	}
}

func assertColumnOperationBatchEqual(t *testing.T, a, b *batch.Batch) {
	t.Helper()
	left, right := a.SortedEntries(), b.SortedEntries()
	if len(left) != len(right) {
		t.Fatalf("entry counts %d/%d", len(left), len(right))
	}
	for i, l := range left {
		r := right[i]
		if l.Type != r.Type || l.IsPtr != r.IsPtr || !bytes.Equal(l.Key, r.Key) || !bytes.Equal(l.Value, r.Value) {
			t.Fatalf("entry %d differs: %+v / %+v", i, l, r)
		}
	}
}

func TestColumnPreAppendManifestMatchesOrdinaryMutationBytes(t *testing.T) {
	identity := ColumnManifestIdentity{Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Generation: 7, Checksum: 19}
	delta := ColumnManifestRootDelta{RootName: "events/manifest", StoragePolicy: RootStorageDefault, Identity: identity, IdentityRecord: encodeColumnManifestIdentityRecordArray(identity), MutationDelta: true, Mutations: []columnManifestMutation{{record: columnManifestRecord{key: []byte("a"), value: []byte("new")}}, {record: columnManifestRecord{key: []byte("b")}, deleted: true}}}
	ordinary, cleanup, err := delta.OrderedRootDeltaBatchPublishInput()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	owned, err := ownedColumnManifestMutationBatch(delta, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer owned.Delta.Close()
	assertColumnOperationBatchEqual(t, ordinary.Delta, owned.Delta)
}

func TestColumnPreAppendOwnedRefusalBeforeDebit(t *testing.T) {
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer credit.retire()
	facet := &credit.facets[nativeRequestSourceCredit]
	before := credit.snapshot()
	if _, err := columnPreAppendSystemKeys(CollectionMeta{Name: "events"}, []string{"same", "same"}, facet); err == nil {
		t.Fatal("duplicate root accepted")
	}
	id := ColumnManifestIdentity{Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Generation: 1, Checksum: 7}
	d := ColumnManifestRootDelta{RootName: "events/manifest", Identity: id, IdentityRecord: encodeColumnManifestIdentityRecordArray(id), MutationDelta: true, StoragePolicy: RootStoragePolicy("unknown")}
	if _, err := ownedColumnManifestMutationBatch(d, facet); err == nil {
		t.Fatal("bad policy accepted")
	}
	op := columnRowLocatorOperation{rows: 2, operation: ColumnPublishOperationUpdate, generation: 7, commandLSN: 11}
	if _, err := buildColumnPrimaryRowLocatorOwnedBatch(op, []columnWriteDocument{{ID: []byte("b")}, {ID: []byte("a")}}, 0, backenddb.OrderedRootStorageDefault, facet); err == nil {
		t.Fatal("unsorted locator accepted")
	}
	if after := credit.snapshot(); after.Debited != before.Debited || after.Retained != before.Retained {
		t.Fatal("refused whole closure changed credit")
	}
	// Exhaust the actual remaining source credit. A valid operation fails the
	// complete debit and yields no partial batch or extra lifetime claim.
	left := before.Reserved[nativeRequestSourceCredit] - before.Debited[nativeRequestSourceCredit]
	if err := facet.ReserveStableMetadata(left); err != nil {
		t.Fatal(err)
	}
	got, err := buildColumnPrimaryRowLocatorOwnedBatch(columnRowLocatorOperation{rows: 1, operation: ColumnPublishOperationUpdate, generation: 7, commandLSN: 11}, []columnWriteDocument{{ID: []byte("a")}}, 0, backenddb.OrderedRootStorageDefault, facet)
	if !errors.Is(err, ErrPreparedInsertResourceLimit) || got.Delta != nil {
		t.Fatalf("exhausted operation %v %+v", err, got)
	}
}

func TestColumnPreAppendSystemUsesActualGenerationKeySet(t *testing.T) {
	meta := CollectionMeta{Name: "events"}
	b, err := columnPreAppendSystemKeys(meta, []string{"other"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if len(b.SortedEntries()) != 2 {
		t.Fatal("unexpected document generation without primary mutation")
	}
	c, err := columnPreAppendSystemKeys(meta, []string{collectionPrimaryRootName(meta.Name)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if len(c.SortedEntries()) != 3 {
		t.Fatal("primary mutation omitted document generation")
	}
}

func TestColumnPreAppendOperationRetainsCreatorUntilClose(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{Enabled: true, Columns: []ColumnStoreColumn{{Name: "city", Path: "city", ValueType: ColumnStoreValueString}}})
	if err != nil {
		t.Fatal(err)
	}
	cfg.ActiveManifest = &ColumnManifestIdentity{Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Generation: 5, Checksum: 17}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	rows := []columnDeclaredRow{{ID: []byte("one"), Values: []columnDeclaredValue{{Type: ColumnStoreValueString, Present: true, String: "Paris"}}, Stored: []bool{true}}}
	input := columnWritePublishInput{meta: CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}, operation: ColumnPublishOperationUpdate, sparseOnly: true, rows: 1, declaredRows: rows, declaredRowsReady: true, nativeSource: &credit.facets[nativeRequestSourceCredit], nativePreparedLimits: &backenddb.PreparedRootPublicationLimits{}}
	f, err := newColumnPreAppendOperation(input)
	if err != nil {
		t.Fatal(err)
	}
	if f.images.commandLSN != 0 || !f.images.unboundCommandIdentity || f.images.projectedRefs != nil || f.images.prepared.stableResources != nil {
		t.Fatal("operation facts acquired authority")
	}
	credit.retire()
	if got := credit.snapshot(); got.Closed || got.Retained != 1 {
		t.Fatalf("creator lost while operation exists: %+v", got)
	}
	if err := input.nativeSource.ReserveStableMetadata(1); err != nil {
		t.Fatal("retired creator refused its retained operation")
	}
	f.close()
	if got := credit.snapshot(); !got.Closed || got.Retained != 0 {
		t.Fatalf("terminal did not drain creator: %+v", got)
	}
	if f.account != nil || f.current != nil || f.images.assets != nil || f.system != nil {
		t.Fatal("terminal retained packet backing")
	}
	f.close()
}

func TestColumnPreAppendInstalledContextReusesOwnedBytes(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Rows = 1
	asset.Reason = string(ColumnPublishOperationUpdate)
	asset.PartRole = ColumnManifestPartRoleDelta
	cfg := testColumnStoreConfig(nil)
	makePlan := func() ColumnPublishPlan {
		p, err := BuildColumnPublishPlan(ColumnPublishPlanInput{
			Collection: "events", ColumnStore: cfg, Operation: ColumnPublishOperationUpdate, AppliedCommandLSN: 11, BaseManifestRootID: 9, ActiveVectorIndexesKnown: true,
			Hooks: ColumnPublishPlanHooks{
				PrepareAssets: func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
					return ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset}, RowCount: 1}, nil
				},
				EncodeManifest: encodeColumnManifestForWrite,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	expected, actual := makePlan(), makePlan()
	docs := []columnWriteDocument{{ID: []byte("a"), fieldSources: []columnRowCoordinates{{Generation: 2, PartID: 1, RowIndex: 3, AppliedCommandLSN: 4}, {}}}}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	facet := &credit.facets[nativeRequestSourceCredit]
	if err = facet.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	f := &columnPreAppendOperationFacts{account: facet, manifestDelta: expected.RootDelta}
	f.images.commandLSN = 11
	f.context[0], err = ownedColumnManifestMutationBatch(expected.RootDelta, facet)
	if err != nil {
		t.Fatal(err)
	}
	f.context[1], err = buildColumnPrimaryRowLocatorOwnedBatch(columnRowLocatorOperation{1, ColumnPublishOperationUpdate, expected.UpdatedActiveManifest.Generation, 11}, docs, 13, backenddb.OrderedRootStorageDefault, facet)
	if err != nil {
		t.Fatal(err)
	}
	f.system, err = columnPreAppendSystemKeys(CollectionMeta{Name: "events"}, []string{"events/manifest"}, facet)
	if err != nil {
		t.Fatal(err)
	}
	defer f.close()
	credit.retire()
	before := credit.snapshot()
	got, err := f.installedContext(actual, docs, 13, backenddb.OrderedRootStorageDefault)
	if err != nil || len(got) != 2 || got[0].Delta != f.context[0].Delta || got[1].Delta != f.context[1].Delta || &got[0] != &f.context[0] {
		t.Fatalf("same owned context: %v", err)
	}
	if after := credit.snapshot(); after != before {
		t.Fatal("reuse allocated/debited or changed creator ownership")
	}
	// A real separately constructed manifest with one changed actual image is
	// rejected; the pre-WAL batch and retired creator remain held unchanged.
	actual.RootDelta.Records[0].value[0] ^= 1
	if got, err = f.installedContext(actual, docs, 13, backenddb.OrderedRootStorageDefault); err == nil || got != nil {
		t.Fatal("installed manifest mismatch accepted")
	}
	actual = makePlan()
	docs[0].fieldSources[0].RowIndex++
	if got, err = f.installedContext(actual, docs, 13, backenddb.OrderedRootStorageDefault); err == nil || got != nil {
		t.Fatal("changed sparse source accepted")
	}
	if after := credit.snapshot(); after != before {
		t.Fatal("refusal dropped creator or charged late backing")
	}
	if len(f.context[0].Delta.SortedEntries()) == 0 {
		t.Fatal("refusal consumed pre-WAL batch")
	}
}

func TestColumnPreAppendPlanLeaseHoldsManifestCreatorAfterOperationClose(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "creator-lifetime", 1)
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	facet := &credit.facets[nativeRequestSourceCredit]
	if err = facet.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	owned := &columnPreAppendOperationFacts{account: facet}
	records := []columnManifestRecord{{key: []byte("owned-key"), value: []byte("owned-value")}}
	plan := ColumnPublishPlan{Enabled: true, Collection: "events", AppliedCommandLSN: 77, ManifestRootBaseID: 44, RootDelta: ColumnManifestRootDelta{BaseRootID: 44, Records: records}, PreparedAssets: []ColumnPreparedAsset{asset}, preAppendAccount: facet}
	lease, err := newColumnPublishPlanLease(col, plan)
	if err != nil {
		t.Fatal(err)
	}
	plan = ColumnPublishPlan{}
	credit.retire()
	owned.close()
	if got := credit.snapshot(); got.Closed || got.Retained != 1 {
		t.Fatalf("creator ended before lease: %+v", got)
	}
	before := credit.snapshot()
	if _, err = lease.beginInstall("events", 78, 44); err == nil {
		t.Fatal("mismatched install accepted")
	}
	if got := credit.snapshot(); got != before {
		t.Fatal("binding refusal released/debited retained creator")
	}
	// Only the trusted synchronous installer receives this view. It must drop
	// the borrowed plan before reporting checked lease completion.
	borrowed, err := lease.beginInstall("events", 77, 44)
	if err != nil {
		t.Fatal(err)
	}
	if string(borrowed.RootDelta.Records[0].value) != "owned-value" {
		t.Fatal("operation close destroyed leased manifest")
	}
	if borrowed.preAppendAccount != nil {
		t.Fatal("installer view received independent creator authority")
	}
	borrowed = ColumnPublishPlan{}
	if err = lease.transferStableResources(backenddb.CommandWALPublishContext{}); err != nil {
		t.Fatal(err)
	}
	if err = lease.finishCommit(); err != nil {
		t.Fatal(err)
	}
	if got := credit.snapshot(); !got.Closed || got.Retained != 0 {
		t.Fatalf("lease failed to drain creator: %+v", got)
	}
	if lease.operationAccount != nil || lease.plan.preAppendAccount != nil || lease.plan.RootDelta.Records != nil || lease.plan.RootDelta.Mutations != nil || lease.plan.durableResourceRequirementsFallback != nil {
		t.Fatal("terminal lease retained borrowed bytes/creator")
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 0)
}

func TestColumnPreAppendSameConstructorReusesManifestOnlyForExactInstalledAssets(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	asset := testColumnPublishPreparedAssetM10A()
	asset.Rows = 1
	asset.PublishID = 11
	asset.Reason = string(ColumnPublishOperationUpdate)
	asset.PartRole = ColumnManifestPartRoleDelta
	prepared := ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset}, RowCount: 1}
	baseInput := ColumnPublishPlanInput{Collection: "events", ColumnStore: cfg, ColumnStoreNormalized: true, Operation: ColumnPublishOperationUpdate, AppliedCommandLSN: 11, BaseManifestRootID: 9, ActiveVectorIndexesKnown: true, Hooks: ColumnPublishPlanHooks{PrepareAssets: func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) { return prepared, nil }, EncodeManifest: encodeColumnManifestForWrite}}
	ordinary, err := BuildColumnPublishPlan(baseInput)
	if err != nil {
		t.Fatal(err)
	}
	credit, err := newNativeRequestCredit([nativeRequestCreditKinds]uint64{1 << 20, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	facet := &credit.facets[nativeRequestSourceCredit]
	if err = facet.RetainStableMetadata(); err != nil {
		t.Fatal(err)
	}
	f := &columnPreAppendOperationFacts{account: facet, manifestDelta: ordinary.RootDelta, manifest: ColumnPublishManifestEncodeResult{Identity: ordinary.UpdatedActiveManifest, ManifestBytes: ordinary.ManifestBytes, Records: ordinary.RootDelta.Records}}
	f.images = columnPhysicalAssetImagePlan{prepared: prepared, generation: asset.GenerationID, commandLSN: 11, projectedRefs: []ColumnAssetRef{asset.Ref}, assets: []pendingColumnAsset{{payload: make([]byte, int(asset.Ref.Length)), rows: asset.Rows, reason: asset.Reason, partRole: asset.PartRole}}}
	lock := new(sync.Mutex)
	lock.Lock()
	f.reservation.lock = lock
	f.system, err = columnPreAppendSystemKeys(CollectionMeta{Name: "events"}, []string{"events/manifest"}, facet)
	if err != nil {
		t.Fatal(err)
	}
	defer f.close()
	defer credit.retire()
	input := baseInput
	input.metadataAccount = facet
	input.preAppendOperation = f
	encodeCalls := 0
	input.Hooks.EncodeManifest = func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
		encodeCalls++
		return ColumnPublishManifestEncodeResult{}, errors.New("unexpected second manifest encode")
	}
	installed, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if encodeCalls != 0 || !sameColumnManifestDeltaBytes(installed.RootDelta, ordinary.RootDelta) || &installed.RootDelta.Records[0] != &f.manifestDelta.Records[0] || &installed.RootDelta.Mutations[0] != &f.manifestDelta.Mutations[0] {
		t.Fatal("constructor copied/reencoded or changed manifest output")
	}
	if installed.preAppendAccount != facet {
		t.Fatal("borrowed output lost actual creator")
	}
	// A returned physical address or immutable asset fact mismatch cannot
	// consume the captured bytes, invoke the duplicate encoder, or gain credit.
	before := credit.snapshot()
	changed := prepared
	changed.Assets = append([]ColumnPreparedAsset(nil), prepared.Assets...)
	changed.Assets[0].Ref.Offset++
	input.Hooks.PrepareAssets = func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) { return changed, nil }
	if result, err := BuildColumnPublishPlan(input); err == nil || result.Enabled {
		t.Fatal("changed installed physical address accepted")
	}
	if after := credit.snapshot(); after != before || encodeCalls != 0 {
		t.Fatal("refused install changed creator/constructor effects")
	}
}
