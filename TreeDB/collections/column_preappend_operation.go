package collections

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// These private facts own operation bytes only. Projected physical refs never
// become a ColumnPublishPlan, resource set, durability closure or published
// manifest. The installed plan is independently validated by the ordinary
// constructor and the DB compares its complete actual batch before Apply.
type columnPreAppendOperationFacts struct {
	account       rootpublication.StableMetadataAccount
	images        columnPhysicalAssetImagePlan
	reservation   columnPhysicalAssetIdentityReservation
	current       []columnManifestRecord
	context       [2]backenddb.OrderedRootDeltaBatchPublishInput
	manifestDelta ColumnManifestRootDelta
	manifest      ColumnPublishManifestEncodeResult
	system        *batch.Batch
}

// newColumnPreAppendOperation owns the same encoder's identity-free bytes and
// retains their actual creator facet through every synchronous publication exit.
// This is a private packet component, not public finite admission. Manifest
// decoding, producer loans and late-value/control census must still close.
func newColumnPreAppendOperation(input columnWritePublishInput) (*columnPreAppendOperationFacts, error) {
	facet, ok := input.nativeSource.(*nativeRequestCreditFacet)
	cfg := input.meta.Options.ColumnStore
	if !ok || facet == nil || facet.kind != nativeRequestSourceCredit || input.nativeOperation != nil || input.nativePreparedLimits == nil || cfg == nil || cfg.AssetManager == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnManifestFormatTCS1 || cfg.ActiveManifest.Generation == math.MaxUint64 || !input.declaredRowsReady || input.rows != len(input.declaredRows) || !input.sparseOnly || input.operation != ColumnPublishOperationUpdate || input.sourceImportV2 != nil || input.splitInsert != nil || input.colocated != nil || input.preparedPlan != nil || len(input.meta.VectorIndexes) != 0 {
		return nil, errColumnNativeContextIncomplete
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(columnPreAppendOperationFacts{})), true)
	if err != nil {
		return nil, err
	}
	if err = facet.ReserveStableMetadata(control); err != nil {
		return nil, err
	}
	if err = facet.RetainStableMetadata(); err != nil {
		return nil, err
	}
	f := &columnPreAppendOperationFacts{account: facet}
	hook := ColumnPublishAssetPrepareInput{Collection: input.meta.Name, ColumnStore: *cfg, Operation: input.operation, CurrentManifest: cfg.ActiveManifest}
	f.images, err = encodeNativeColumnPhysicalImagesBeforeIdentity(ColumnPublishPreparedAssets{}, input, hook, input.declaredRows, cfg.ActiveManifest.Generation+1, columnPhysicalRowAssetPartID)
	if err != nil {
		f.close()
		return nil, err
	}
	return f, nil
}

// This concrete binding exists only until the existing publisher returns. It
// carries operational edges on the synchronous stack, never on the plan/set.
// Its receiver and method value are separately prepaid actual scan classes.
type columnPreAppendInvocation struct {
	collection                *Collection
	input                     columnWritePublishInput
	rootNames                 []string
	manifestBase, locatorBase uint64
}

func (i *columnPreAppendInvocation) clear() { *i = columnPreAppendInvocation{} }
func (i *columnPreAppendInvocation) prepare(ctx backenddb.OrderedRootPreAppendContext) (backenddb.OrderedRootPreAppendPlan, error) {
	return i.collection.prepareColumnPreAppendOperation(i.input, i.rootNames, i.manifestBase, i.locatorBase, ctx, i.input.nativeOperation)
}
func newColumnPreAppendInvocation(c *Collection, input columnWritePublishInput, roots []string, manifest, locator uint64) (*columnPreAppendInvocation, error) {
	if input.nativeOperation == nil || input.nativeOperation.account != input.nativeSource {
		return nil, errColumnNativeContextIncomplete
	}
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(columnPreAppendInvocation{})), true)
	if err != nil {
		return nil, err
	}
	callback, err := rootpublication.StableBackingClassBytes(16, true)
	if err != nil || callback > math.MaxUint64-control {
		return nil, ErrPreparedInsertResourceLimit
	}
	if err = input.nativeSource.ReserveStableMetadata(control + callback); err != nil {
		return nil, err
	}
	return &columnPreAppendInvocation{c, input, roots, manifest, locator}, nil
}

func (f *columnPreAppendOperationFacts) close() {
	if f == nil {
		return
	}
	for i := range f.context {
		if f.context[i].Delta != nil {
			_ = f.context[i].Delta.Close()
			f.context[i].Delta = nil
		}
	}
	if f.system != nil {
		_ = f.system.Close()
		f.system = nil
	}
	f.images = columnPhysicalAssetImagePlan{}
	f.current = nil
	f.manifestDelta = ColumnManifestRootDelta{}
	f.manifest = ColumnPublishManifestEncodeResult{}
	f.reservation.release()
	if f.account != nil {
		f.account.ReleaseStableMetadata()
		f.account = nil
	}
}

func ownedColumnManifestMutationBatch(delta ColumnManifestRootDelta, account rootpublication.StableMetadataAccount) (backenddb.OrderedRootDeltaBatchPublishInput, error) {
	if !delta.MutationDelta {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
	}
	policy, err := delta.validatedStoragePolicy()
	if err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, err
	}
	previous := []byte(columnManifestIdentityRecordKey)
	for _, m := range delta.Mutations {
		if bytes.Compare(previous, m.record.key) >= 0 {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
		}
		previous = m.record.key
	}
	if len(delta.Mutations) == math.MaxInt {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
	}
	count := len(delta.Mutations) + 1
	if uint64(count) > math.MaxUint64/uint64(unsafe.Sizeof(batch.Entry{})) {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
	}
	var total uint64
	for _, allocation := range [...]struct {
		raw  uint64
		scan bool
	}{
		{uint64(count) * uint64(unsafe.Sizeof(batch.Entry{})), true},
		{uint64(count) * uint64(unsafe.Sizeof(batch.Entry{})), true},
		{uint64(unsafe.Sizeof(batch.Batch{})), true},
		{uint64(len(columnManifestIdentityRecordKey)), false},
		{columnManifestIdentityRecordSize, false},
	} {
		class, err := rootpublication.StableBackingClassBytes(allocation.raw, allocation.scan)
		if err != nil || class > math.MaxUint64-total {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, ErrPreparedInsertResourceLimit
		}
		total += class
	}
	if account != nil {
		if err := account.ReserveStableMetadata(total); err != nil {
			return backenddb.OrderedRootDeltaBatchPublishInput{}, err
		}
	}
	entries := make([]batch.Entry, count)
	entries[0] = batch.Entry{Key: []byte(columnManifestIdentityRecordKey), Value: bytes.Clone(delta.IdentityRecord[:])}
	for i, m := range delta.Mutations {
		entries[i+1] = batch.Entry{Key: m.record.key, Value: m.record.value}
		if m.deleted {
			entries[i+1].Type = batch.OpDelete
			entries[i+1].Value = nil
		}
	}
	result, err := batch.NewOwnedPointBatch(entries, nil)
	if err != nil {
		return backenddb.OrderedRootDeltaBatchPublishInput{}, err
	}
	return backenddb.OrderedRootDeltaBatchPublishInput{BaseRoot: delta.BaseRootID, Delta: result, StoragePolicy: policy}, nil
}

// A key/type-only late-value plan uses the SAME existing descriptor key grammar.
// It is not a value-size/output/allocator certificate. Full finite capture must
// also bind actual late-value allowances before public admission can open.
func columnPreAppendSystemKeys(meta CollectionMeta, rootNames []string, account rootpublication.StableMetadataAccount) (*batch.Batch, error) {
	if meta.Name == "" || len(rootNames) > math.MaxInt-2 {
		return nil, ErrPreparedInsertResourceLimit
	}
	for i, name := range rootNames {
		if name == "" {
			return nil, ErrPreparedInsertResourceLimit
		}
		for _, previous := range rootNames[:i] {
			if previous == name {
				return nil, ErrPreparedInsertResourceLimit
			}
		}
	}
	count := 1 + len(rootNames)
	primary := collectionPrimaryRootName(meta.Name)
	documentGeneration := slices.Contains(rootNames, primary)
	if documentGeneration {
		count++
	}
	if uint64(count) > math.MaxUint64/uint64(unsafe.Sizeof(batch.Entry{})) {
		return nil, ErrPreparedInsertResourceLimit
	}
	var total uint64
	add := func(raw uint64, scan bool) error {
		class, err := rootpublication.StableBackingClassBytes(raw, scan)
		if err != nil || class > math.MaxUint64-total {
			return ErrPreparedInsertResourceLimit
		}
		total += class
		return nil
	}
	for range 2 {
		if err := add(uint64(count)*uint64(unsafe.Sizeof(batch.Entry{})), true); err != nil {
			return nil, err
		}
	}
	if err := add(uint64(unsafe.Sizeof(batch.Batch{})), true); err != nil {
		return nil, err
	}
	// String concatenation and its owned byte conversion are distinct births.
	if err := add(uint64(len(systemCollectionMetaPrefix)+len(meta.Name)), false); err != nil {
		return nil, err
	}
	if err := add(uint64(len(systemCollectionMetaPrefix)+len(meta.Name)), false); err != nil {
		return nil, err
	}
	for _, name := range rootNames {
		if name == "" {
			return nil, ErrPreparedInsertResourceLimit
		}
		for range 2 {
			if err := add(uint64(len(systemCollectionRootPrefix)+len(name)), false); err != nil {
				return nil, err
			}
		}
	}
	if documentGeneration {
		for range 2 {
			if err := add(uint64(len(systemCollectionDocumentGenerationPrefix)+len(meta.Name)), false); err != nil {
				return nil, err
			}
		}
	}
	if account != nil {
		if err := account.ReserveStableMetadata(total); err != nil {
			return nil, err
		}
	}
	entries := make([]batch.Entry, 0, count)
	entries = append(entries, batch.Entry{Key: []byte(systemCollectionMetaKey(meta.Name))})
	for _, name := range rootNames {
		entries = append(entries, batch.Entry{Key: []byte(systemCollectionRootKey(name))})
	}
	if documentGeneration {
		entries = append(entries, batch.Entry{Key: []byte(systemCollectionDocumentGenerationKey(meta.Name))})
	}
	slices.SortFunc(entries, func(a, b batch.Entry) int { return bytes.Compare(a.Key, b.Key) })
	return batch.NewOwnedPointBatch(entries, nil)
}

func (c *Collection) prepareColumnPreAppendOperation(input columnWritePublishInput, rootNames []string, manifestBase, locatorBase uint64, ctx backenddb.OrderedRootPreAppendContext, f *columnPreAppendOperationFacts) (backenddb.OrderedRootPreAppendPlan, error) {
	if c == nil || c.db == nil || f == nil || f.system != nil || f.reservation.lock != nil || ctx.AppliedCommandLSN == 0 || input.nativeSource == nil || input.nativeSnapshot == nil ||
		input.sourceImportV2 != nil || input.splitInsert != nil || input.colocated != nil || input.preparedPlan != nil || len(input.meta.VectorIndexes) != 0 || !input.sparseOnly || input.operation != ColumnPublishOperationUpdate {
		return backenddb.OrderedRootPreAppendPlan{}, ErrPreparedInsertResourceLimit
	}
	cfg := input.meta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || cfg.ActiveManifest.Format != columnManifestFormatTCS1 || cfg.ActiveManifest.Generation == math.MaxUint64 {
		return backenddb.OrderedRootPreAppendPlan{}, ErrPreparedInsertResourceLimit
	}
	hook := ColumnPublishAssetPrepareInput{Collection: input.meta.Name, ColumnStore: *cfg, Operation: input.operation, AppliedCommandLSN: ctx.AppliedCommandLSN, CurrentManifest: cfg.ActiveManifest}
	if err := bindNativeColumnPhysicalImagesCommandIdentity(&f.images, hook); err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	if err := prepareSharedColumnPhysicalAssetIdentity(c.db, c.db.ColumnAssetRootDir(), *cfg, &f.reservation, input.nativeSource); err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	projected, err := prepareColumnPhysicalAssetImageIdentity(&f.images, hook, &f.reservation, input.nativeSource)
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	f.current, err = loadColumnManifestRecordsForPublishFromSnapshot(input.nativeSnapshot, manifestBase, input.meta.Name, *cfg, input.nativeSource)
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	prepared := f.images.prepared
	prepared.Assets = projected // operation facts only, no stable resource authority
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{metadataAccount: input.nativeSource, Collection: input.meta.Name, ColumnStore: *cfg, Operation: input.operation, AppliedCommandLSN: ctx.AppliedCommandLSN, CurrentManifest: cfg.ActiveManifest, CurrentManifestRecords: f.current, ActiveVectorIndexesKnown: true, Prepared: prepared})
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	mutations, err := buildColumnManifestMutationDeltaWithMetadataAccount(f.current, manifest.Records, input.nativeSource)
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	if cfg.ManifestRoot == nil {
		return backenddb.OrderedRootPreAppendPlan{}, ErrPreparedInsertResourceLimit
	}
	delta := ColumnManifestRootDelta{RootName: cfg.ManifestRoot.Name, BaseRootID: manifestBase, StoragePolicy: cfg.ManifestRoot.StoragePolicy, Identity: manifest.Identity, IdentityRecord: encodeColumnManifestIdentityRecordArray(manifest.Identity), Records: manifest.Records, Mutations: mutations, MutationDelta: true}
	f.manifestDelta = delta
	f.manifest = manifest
	f.context[0], err = ownedColumnManifestMutationBatch(delta, input.nativeSource)
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	policy, err := collectionRootStoragePolicyForDB(c.db, input.meta, collectionColumnRowLocatorRootName(input.meta.Name))
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	f.context[1], err = buildColumnPrimaryRowLocatorOwnedBatch(columnRowLocatorOperation{input.rows, input.operation, manifest.Identity.Generation, ctx.AppliedCommandLSN}, input.documents, locatorBase, policy, input.nativeSource)
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	f.system, err = columnPreAppendSystemKeys(input.meta, rootNames, input.nativeSource)
	if err != nil {
		return backenddb.OrderedRootPreAppendPlan{}, err
	}
	return backenddb.OrderedRootPreAppendPlan{ContextDeltas: f.context[:], SystemDelta: f.system}, nil
}

var errColumnNativeContextIncomplete = errors.New("collections: native pre-WAL context requires complete owned publication limits")

// installedContext compares the real installer's complete immutable operation
// output before lending the same pre-WAL bytes. It allocates no alternate batch
// or result array and does not turn projected refs into durability authority.
func (f *columnPreAppendOperationFacts) installedContext(plan ColumnPublishPlan, documents []columnWriteDocument, locatorBase uint64, locatorPolicy backenddb.OrderedRootStoragePolicy) ([]backenddb.OrderedRootDeltaBatchPublishInput, error) {
	if f == nil || f.account == nil || f.system == nil || f.context[0].Delta == nil || f.context[1].Delta == nil ||
		!plan.Enabled || plan.AppliedCommandLSN != f.images.commandLSN || plan.UpdatedActiveManifest != f.manifestDelta.Identity ||
		plan.Operation != ColumnPublishOperationUpdate || !sameColumnManifestDeltaBytes(plan.RootDelta, f.manifestDelta) ||
		f.context[1].BaseRoot != locatorBase || f.context[1].StoragePolicy != locatorPolicy {
		return nil, errColumnNativeContextIncomplete
	}
	op := columnRowLocatorOperation{plan.Rows, plan.Operation, plan.UpdatedActiveManifest.Generation, plan.AppliedCommandLSN}
	if err := validateColumnPrimaryRowLocatorOwnedBatch(op, documents, f.context[1].Delta); err != nil {
		return nil, err
	}
	return f.context[:], nil
}

func sameColumnManifestDeltaBytes(a, b ColumnManifestRootDelta) bool {
	if a.sourceDirectoryV2 != nil || b.sourceDirectoryV2 != nil || !a.MutationDelta || !b.MutationDelta ||
		a.RootName != b.RootName || a.BaseRootID != b.BaseRootID || a.StoragePolicy != b.StoragePolicy ||
		a.Identity != b.Identity || a.IdentityRecord != b.IdentityRecord || len(a.Records) != len(b.Records) || len(a.Mutations) != len(b.Mutations) {
		return false
	}
	for i, r := range a.Records {
		if !bytes.Equal(r.key, b.Records[i].key) || !bytes.Equal(r.value, b.Records[i].value) {
			return false
		}
	}
	for i, m := range a.Mutations {
		other := b.Mutations[i]
		if m.deleted != other.deleted || !bytes.Equal(m.record.key, other.record.key) || !bytes.Equal(m.record.value, other.record.value) {
			return false
		}
	}
	return true
}

// installedManifest is a call-time loan of the SAME encoder output. Every
// installed physical coordinate and immutable asset fact must match before
// these bytes can feed the ordinary closure/root-delta validation stages.
func (f *columnPreAppendOperationFacts) installedManifest(input ColumnPublishPlanInput, cfg ColumnStoreConfig, prepared ColumnPublishPreparedAssets) (ColumnPublishManifestEncodeResult, error) {
	if f == nil || f.account == nil || input.metadataAccount != f.account || f.system == nil ||
		f.reservation.lock == nil || input.sourceDirectoryV2 != nil || input.Operation != ColumnPublishOperationUpdate ||
		input.AppliedCommandLSN != f.images.commandLSN || input.BaseManifestRootID != f.manifestDelta.BaseRootID ||
		cfg.ManifestRoot == nil || cfg.ManifestRoot.Name != f.manifestDelta.RootName || cfg.ManifestRoot.StoragePolicy != f.manifestDelta.StoragePolicy ||
		len(prepared.Assets) != len(f.images.projectedRefs) || len(prepared.Assets) != 1 || len(f.images.assets) != 1 ||
		prepared.Assets[0].SortKey != "" || f.images.assets[0].sortKey != "" || f.images.assets[0].reason != string(ColumnPublishOperationUpdate) ||
		prepared.RowCount != f.images.prepared.RowCount || prepared.CommandBytes != f.images.prepared.CommandBytes ||
		prepared.RowRemainderBytes != f.images.prepared.RowRemainderBytes || prepared.ColumnPayloadBytes != f.images.prepared.ColumnPayloadBytes ||
		!sameColumnManifestRecordsBytes(input.CurrentManifestRecords, f.current) ||
		f.manifest.Identity != f.manifestDelta.Identity || !sameColumnManifestRecordsBytes(f.manifest.Records, f.manifestDelta.Records) {
		return ColumnPublishManifestEncodeResult{}, errColumnNativeContextIncomplete
	}
	for i, asset := range prepared.Assets {
		image := f.images.assets[i]
		expected := ColumnPreparedAsset{Ref: f.images.projectedRefs[i], Rows: image.rows, Bytes: int64(len(image.payload)), PublishID: input.AppliedCommandLSN, GenerationID: f.images.generation, Reason: image.reason, PartRole: image.partRole, SortKey: image.sortKey}
		if asset != expected {
			return ColumnPublishManifestEncodeResult{}, errColumnNativeContextIncomplete
		}
	}
	if checksumColumnManifestRecords(ColumnPublishManifestEncodeInput{Collection: input.Collection, Operation: input.Operation, AppliedCommandLSN: input.AppliedCommandLSN, ColumnStore: cfg}, f.manifest.Identity.Generation, f.manifest.Records) != f.manifest.Identity.Checksum {
		return ColumnPublishManifestEncodeResult{}, errColumnNativeContextIncomplete
	}
	return f.manifest, nil
}

func sameColumnManifestRecordsBytes(a, b []columnManifestRecord) bool {
	if len(a) != len(b) {
		return false
	}
	for i, r := range a {
		if !bytes.Equal(r.key, b[i].key) || !bytes.Equal(r.value, b[i].value) {
			return false
		}
	}
	return true
}
