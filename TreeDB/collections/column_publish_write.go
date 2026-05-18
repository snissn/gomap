package collections

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cespare/xxhash/v2"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type columnWritePublishInput struct {
	meta               CollectionMeta
	catalog            *collectionCatalog
	baseCommitSeq      uint64
	baseSystemRoot     uint64
	rootNames          []string
	baseRootIDs        map[string]uint64
	commandWALIntent   *backenddb.CommandWALIntent
	operation          ColumnPublishOperation
	rows               int
	commandBytes       int64
	rowRemainderBytes  int64
	columnPayloadBytes int64
}

func columnStoreWriteEnabled(meta CollectionMeta) bool {
	return meta.Options.ColumnStore != nil && meta.Options.ColumnStore.Enabled
}

func (c *Collection) requireColumnStoreCommandWAL(meta CollectionMeta, commandWALIntent *backenddb.CommandWALIntent) error {
	if !columnStoreWriteEnabled(meta) {
		return nil
	}
	if c != nil && c.commandWALActive(commandWALIntent) {
		return nil
	}
	return fmt.Errorf("%w: column-store writes require command WAL", backenddb.ErrCommandWALRejected)
}

func (c *Collection) publishRootDeltaGroupMaybeColumn(ordered []backenddb.OrderedRootDeltaPublishInput, input columnWritePublishInput) (uint64, []uint64, CollectionMeta, []string, error) {
	if err := c.requireColumnStoreCommandWAL(input.meta, input.commandWALIntent); err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if !columnStoreWriteEnabled(input.meta) {
		newSystemRoot, rootIDs, err := c.publishRootDeltaGroupWithoutColumn(ordered, input)
		return newSystemRoot, rootIDs, input.meta, input.rootNames, err
	}
	if input.commandWALIntent == nil {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("%w: column-store publish requires command WAL intent", backenddb.ErrCommandWALContextMissingFrame)
	}
	columnRootName := collectionColumnManifestRootName(input.meta.Name)
	columnBaseRoot := uint64(0)
	if input.catalog != nil {
		columnBaseRoot = input.catalog.rootID(columnRootName)
	}
	rootNames, baseRootIDs, appendErr := appendColumnManifestRootPublishBase(input.rootNames, input.baseRootIDs, columnRootName, columnBaseRoot)
	if appendErr != nil {
		return 0, nil, CollectionMeta{}, nil, appendErr
	}
	preflight := c.columnPublishRootDescriptorPreflight(input, rootNames, baseRootIDs)
	var plan ColumnPublishPlan
	var updatedMeta CollectionMeta
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(
		ordered,
		preflight,
		input.commandWALIntent,
		func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
			nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
			if err != nil {
				return nil, err
			}
			plan = nextPlan
			columnDelta, err := plan.RootDelta.OrderedRootDeltaPublishInput()
			if err != nil {
				return nil, err
			}
			return []backenddb.OrderedRootDeltaPublishInput{columnDelta}, nil
		},
		func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
				return nil, columnPublishPlanLSNMismatchError(input.meta, ctx.AppliedCommandLSN, plan.AppliedCommandLSN)
			}
			iter, nextMeta, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
			if err == nil {
				updatedMeta = nextMeta
			}
			return iter, err
		},
	)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if updatedMeta.Name == "" {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("collections: column publish did not prepare updated metadata collection=%q operation=%s", input.meta.Name, input.operation)
	}
	return newSystemRoot, rootIDs, updatedMeta, rootNames, nil
}

func (c *Collection) publishRootDeltaBatchGroupMaybeColumn(ordered []backenddb.OrderedRootDeltaBatchPublishInput, preflight backenddb.OrderedRootGroupPreflight, input columnWritePublishInput) (uint64, []uint64, CollectionMeta, []string, error) {
	if err := c.requireColumnStoreCommandWAL(input.meta, input.commandWALIntent); err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if !columnStoreWriteEnabled(input.meta) {
		newSystemRoot, rootIDs, err := c.publishRootDeltaBatchGroupWithoutColumn(ordered, preflight, input)
		return newSystemRoot, rootIDs, input.meta, input.rootNames, err
	}
	if input.commandWALIntent == nil {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("%w: column-store publish requires command WAL intent", backenddb.ErrCommandWALContextMissingFrame)
	}
	columnRootName := collectionColumnManifestRootName(input.meta.Name)
	columnBaseRoot := uint64(0)
	if input.catalog != nil {
		columnBaseRoot = input.catalog.rootID(columnRootName)
	}
	rootNames, baseRootIDs, appendErr := appendColumnManifestRootPublishBase(input.rootNames, input.baseRootIDs, columnRootName, columnBaseRoot)
	if appendErr != nil {
		return 0, nil, CollectionMeta{}, nil, appendErr
	}
	preflight = combineOrderedRootGroupPreflight(preflight, c.columnPublishRootDescriptorPreflight(input, rootNames, baseRootIDs))
	var plan ColumnPublishPlan
	var updatedMeta CollectionMeta
	var cleanupColumnDelta func()
	buildColumnDelta := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaBatchPublishInput, error) {
		nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
		if err != nil {
			return nil, err
		}
		plan = nextPlan
		columnDelta, cleanup, err := plan.RootDelta.OrderedRootDeltaBatchPublishInput()
		if err != nil {
			return nil, err
		}
		cleanupColumnDelta = cleanup
		return []backenddb.OrderedRootDeltaBatchPublishInput{columnDelta}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
			return nil, columnPublishPlanLSNMismatchError(input.meta, ctx.AppliedCommandLSN, plan.AppliedCommandLSN)
		}
		iter, nextMeta, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
		if err == nil {
			updatedMeta = nextMeta
		}
		return iter, err
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	var err error
	newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, buildColumnDelta, buildSystemDelta)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	if cleanupColumnDelta != nil {
		cleanupColumnDelta()
	}
	if updatedMeta.Name == "" {
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("collections: column publish did not prepare updated metadata collection=%q operation=%s", input.meta.Name, input.operation)
	}
	return newSystemRoot, rootIDs, updatedMeta, rootNames, nil
}

func (c *Collection) columnPublishRootDescriptorPreflight(input columnWritePublishInput, rootNames []string, baseRootIDs map[string]uint64) backenddb.OrderedRootGroupPreflight {
	return func() error {
		return c.validateColumnPublishRootDescriptorPreflight(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs)
	}
}

func (c *Collection) validateColumnPublishRootDescriptorPreflight(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64) error {
	if c == nil || c.db == nil {
		return backenddb.ErrClosed
	}
	for _, rootName := range rootNames {
		if _, ok := baseRootIDs[rootName]; !ok {
			return fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, rootName)
		}
	}
	currentCommitSeq, currentSystemRoot := dbCommitSeqAndSystemRoot(c.db)
	if currentSystemRoot == expectedSystemRoot && currentCommitSeq == expectedCommitSeq {
		return nil
	}
	current := c.db.AcquireSnapshot()
	if current == nil {
		return backenddb.ErrClosed
	}
	defer func() { _ = current.Close() }()
	catalog, err := loadCollectionCatalog(current, meta.Name)
	if err != nil {
		return err
	}
	if catalog == nil {
		return errCollectionNotFound
	}
	if !sameCollectionMetaIgnoringColumnManifestProgress(catalog.meta, meta) {
		return fmt.Errorf("collections: concurrent schema modification detected for %q", meta.Name)
	}
	for _, rootName := range rootNames {
		want := baseRootIDs[rootName]
		if got := catalog.rootID(rootName); got != want {
			return errConcurrentRootModification(meta.Name, rootName)
		}
	}
	return nil
}

func sameCollectionMetaIgnoringColumnManifestProgress(a, b CollectionMeta) bool {
	return sameCollectionMeta(clearColumnManifestProgress(a), clearColumnManifestProgress(b))
}

func clearColumnManifestProgress(meta CollectionMeta) CollectionMeta {
	copied := copyCollectionMeta(meta)
	if copied.Options.ColumnStore == nil {
		return copied
	}
	cfg := *copied.Options.ColumnStore
	cfg.ActiveManifest = nil
	cfg.RecoveryAuthoritativeManifest = nil
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 0
	copied.Options.ColumnStore = &cfg
	return copied
}

func combineOrderedRootGroupPreflight(first, second backenddb.OrderedRootGroupPreflight) backenddb.OrderedRootGroupPreflight {
	switch {
	case first == nil:
		return second
	case second == nil:
		return first
	default:
		return func() error {
			if err := first(); err != nil {
				return err
			}
			return second()
		}
	}
}

func (c *Collection) publishRootDeltaGroupWithoutColumn(ordered []backenddb.OrderedRootDeltaPublishInput, input columnWritePublishInput) (uint64, []uint64, error) {
	if input.commandWALIntent != nil {
		return c.db.PublishOrderedRootDeltaGroupWithCommandWALAndSystemDeltaBuilder(ordered, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
		})
	}
	return c.db.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
	})
}

func (c *Collection) publishRootDeltaBatchGroupWithoutColumn(ordered []backenddb.OrderedRootDeltaBatchPublishInput, preflight backenddb.OrderedRootGroupPreflight, input columnWritePublishInput) (uint64, []uint64, error) {
	if input.commandWALIntent != nil {
		if preflight != nil {
			return c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
				return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
			})
		}
		return c.db.PublishOrderedRootDeltaBatchGroupWithCommandWALAndSystemDeltaBuilder(ordered, input.commandWALIntent, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
		})
	}
	if preflight != nil {
		return c.db.PublishOrderedRootDeltaBatchGroupWithPreflightAndSystemDeltaBuilder(ordered, preflight, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
			return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
		})
	}
	return c.db.PublishOrderedRootDeltaBatchGroupWithSystemDeltaBuilder(ordered, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return c.buildRootDescriptorSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, input.rootNames, input.baseRootIDs, rootIDs)
	})
}

func (c *Collection) buildColumnPublishPlanForCommandWALContext(ctx backenddb.CommandWALPublishContext, input columnWritePublishInput, baseManifestRootID uint64) (ColumnPublishPlan, error) {
	return BuildColumnPublishPlan(ColumnPublishPlanInput{
		Collection:            input.meta.Name,
		ColumnStore:           input.meta.Options.ColumnStore,
		ColumnStoreNormalized: true,
		Operation:             input.operation,
		CurrentManifest:       input.meta.Options.ColumnStore.ActiveManifest,
		AppliedCommandLSN:     ctx.AppliedCommandLSN,
		BaseManifestRootID:    baseManifestRootID,
		Hooks: ColumnPublishPlanHooks{
			PrepareAssets: func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
				return ColumnPublishPreparedAssets{
					RowCount:           input.rows,
					CommandBytes:       input.commandBytes,
					RowRemainderBytes:  input.rowRemainderBytes,
					ColumnPayloadBytes: input.columnPayloadBytes,
				}, nil
			},
			EncodeManifest: encodeColumnManifestIdentityForWrite,
		},
	})
}

func encodeColumnManifestIdentityForWrite(input ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
	if err := validateColumnPublishPreparedAssets(input.Prepared); err != nil {
		return ColumnPublishManifestEncodeResult{}, err
	}
	generation := uint64(1)
	if input.CurrentManifest != nil {
		generation = input.CurrentManifest.Generation + 1
	}
	identity := ColumnManifestIdentity{
		Generation: generation,
		Format:     columnManifestFormatTCS1,
		Version:    columnManifestIdentityVersion,
		Checksum:   checksumColumnManifestIdentityForWrite(input, generation),
	}
	return ColumnPublishManifestEncodeResult{
		Identity:      identity,
		ManifestBytes: columnManifestIdentityRecordSize,
	}, nil
}

func checksumColumnManifestIdentityForWrite(input ColumnPublishManifestEncodeInput, generation uint64) uint64 {
	var d xxhash.Digest
	d.Reset()
	writeHashString(&d, input.Collection)
	writeHashString(&d, string(input.Operation))
	writeHashUint64(&d, generation)
	writeHashUint64(&d, input.AppliedCommandLSN)
	writeHashUint64(&d, input.ColumnStore.SchemaHash)
	if input.CurrentManifest != nil {
		writeHashUint64(&d, input.CurrentManifest.Generation)
		writeHashUint64(&d, input.CurrentManifest.Checksum)
	}
	writeHashUint64(&d, uint64(input.Prepared.RowCount))
	writeHashUint64(&d, uint64(input.Prepared.CommandBytes))
	writeHashUint64(&d, uint64(input.Prepared.RowRemainderBytes))
	writeHashUint64(&d, uint64(input.Prepared.ColumnPayloadBytes))
	for _, asset := range input.Prepared.Assets {
		writeHashString(&d, string(asset.Ref.Kind))
		writeHashUint64(&d, uint64(asset.Ref.FileID))
		writeHashUint64(&d, uint64(asset.Ref.Offset))
		writeHashUint64(&d, uint64(asset.Ref.Length))
		writeHashUint64(&d, uint64(asset.Ref.Checksum))
		writeHashUint64(&d, uint64(asset.Bytes))
		writeHashUint64(&d, asset.PublishID)
		writeHashUint64(&d, asset.GenerationID)
		writeHashString(&d, asset.Reason)
	}
	sum := d.Sum64()
	if sum == 0 {
		return 1
	}
	return sum
}

func writeHashUint64(d *xxhash.Digest, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, _ = d.Write(buf[:])
}

func appendColumnManifestRootPublishBase(rootNames []string, baseRootIDs map[string]uint64, columnRootName string, columnBaseRoot uint64) ([]string, map[string]uint64, error) {
	for _, rootName := range rootNames {
		if rootName == columnRootName {
			return nil, nil, fmt.Errorf("collections: column manifest root %q must be published by the column context delta, not the row root group", columnRootName)
		}
	}
	if baseRootIDs == nil {
		baseRootIDs = make(map[string]uint64, 1)
	}
	baseRootIDs[columnRootName] = columnBaseRoot
	return append(rootNames, columnRootName), baseRootIDs, nil
}

func cloneColumnPublishRootNames(rootNames []string) []string {
	if rootNames == nil {
		return nil
	}
	out := make([]string, 0, len(rootNames)+1)
	return append(out, rootNames...)
}

func cloneColumnPublishBaseRootIDs(baseRootIDs map[string]uint64) map[string]uint64 {
	if baseRootIDs == nil {
		return nil
	}
	out := make(map[string]uint64, len(baseRootIDs)+1)
	for name, rootID := range baseRootIDs {
		out[name] = rootID
	}
	return out
}

func columnPublishPlanLSNMismatchError(meta CollectionMeta, ctxLSN, planLSN uint64) error {
	return fmt.Errorf("collections: column publish plan LSN mismatch collection=%q ctx_lsn=%d plan_lsn=%d", meta.Name, ctxLSN, planLSN)
}

func validateColumnPublishPlanRootForMeta(meta CollectionMeta, plan ColumnPublishPlan) (string, error) {
	rootName := plan.ManifestRootName
	if rootName == "" {
		rootName = plan.RootDelta.RootName
	}
	if rootName == "" {
		return "", errors.New("collections: column publish plan missing manifest root name")
	}
	if plan.Collection != meta.Name {
		return "", fmt.Errorf("collections: column publish plan collection %q does not match collection %q", plan.Collection, meta.Name)
	}
	expectedRootName := collectionColumnManifestRootName(meta.Name)
	if rootName != expectedRootName {
		return "", fmt.Errorf("collections: column publish plan root %q does not match collection root %q", rootName, expectedRootName)
	}
	if plan.RootDelta.RootName != "" && plan.RootDelta.RootName != rootName {
		return "", fmt.Errorf("collections: column publish plan root %q does not match root delta %q", rootName, plan.RootDelta.RootName)
	}
	rootIdentity := plan.RootDelta.Identity
	normalizeColumnManifestIdentityDefaults(&rootIdentity)
	if err := validateColumnManifestIdentity(rootIdentity); err != nil {
		return "", err
	}
	if plan.RootDelta.IdentityRecord != encodeColumnManifestIdentityRecordArray(rootIdentity) {
		return "", errors.New("collections: column publish plan root identity record does not match root identity")
	}
	activeIdentity := plan.UpdatedActiveManifest
	normalizeColumnManifestIdentityDefaults(&activeIdentity)
	if activeIdentity != rootIdentity {
		return "", errors.New("collections: column publish plan active manifest identity does not match root delta identity")
	}
	recoveryIdentity := plan.RecoveryAuthoritativeManifest
	normalizeColumnManifestIdentityDefaults(&recoveryIdentity)
	if recoveryIdentity != rootIdentity {
		return "", errors.New("collections: column publish plan recovery-authoritative manifest identity does not match root delta identity")
	}
	return rootName, nil
}

func columnPublishUpdatedMeta(base CollectionMeta, plan ColumnPublishPlan) (CollectionMeta, error) {
	if !plan.Enabled {
		return base, nil
	}
	updated := copyCollectionMeta(base)
	if updated.Options.ColumnStore == nil || !updated.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errColumnPublishPlanRequiresEnabledColumnStore
	}
	cfg := updated.Options.ColumnStore.copy()
	active := plan.UpdatedActiveManifest
	recovery := plan.RecoveryAuthoritativeManifest
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &recovery
	cfg.RecoveryAuthoritativeAppliedCommandLSN = plan.RecoveryAuthoritativeAppliedCommandLSN
	updated.Options.ColumnStore = &cfg
	return normalizeCollectionMeta(updated)
}

func (c *Collection) buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64, plan ColumnPublishPlan) (iterator.UnsafeIterator, error) {
	iter, _, err := c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
	return iter, err
}

func (c *Collection) buildRootDescriptorAndColumnManifestSystemDeltaIteratorAndMetaForMeta(meta CollectionMeta, expectedCommitSeq, expectedSystemRoot uint64, rootNames []string, baseRootIDs map[string]uint64, rootIDs []uint64, plan ColumnPublishPlan) (iterator.UnsafeIterator, CollectionMeta, error) {
	if len(rootIDs) != len(rootNames) {
		return nil, CollectionMeta{}, unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	if !plan.Enabled {
		return nil, CollectionMeta{}, errors.New("collections: disabled column publish plan cannot build system delta")
	}
	columnRootName, err := validateColumnPublishPlanRootForMeta(meta, plan)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	columnRootIndex := -1
	for i, rootName := range rootNames {
		if rootName == columnRootName {
			columnRootIndex = i
			break
		}
	}
	if columnRootIndex < 0 {
		return nil, CollectionMeta{}, fmt.Errorf("collections: column manifest root %q missing from published root descriptors", columnRootName)
	}
	columnBaseRoot, ok := baseRootIDs[columnRootName]
	if !ok {
		return nil, CollectionMeta{}, fmt.Errorf("collections: missing base root for collection %q root %q", meta.Name, columnRootName)
	}
	if plan.ManifestRootBaseID != columnBaseRoot || plan.RootDelta.BaseRootID != columnBaseRoot {
		return nil, CollectionMeta{}, errConcurrentRootModification(meta.Name, columnRootName)
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, CollectionMeta{}, err
	}
	if rootIDs[columnRootIndex] == 0 {
		return nil, CollectionMeta{}, errors.New("collections: column manifest root publish returned zero root")
	}
	// The ordered-root group just built this root. Avoid reading it through the
	// pre-commit snapshot because compressed roots may depend on value-log
	// segment visibility published by the enclosing commit.
	updatedMeta, err := columnPublishUpdatedMeta(meta, plan)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	encodedMeta, err := encodeNormalizedCollectionMeta(updatedMeta)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	updates := make(map[string][]byte, len(rootNames)+1)
	updates[systemCollectionMetaKey(updatedMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		if rootIDs[i] == 0 {
			return nil, CollectionMeta{}, fmt.Errorf("collections: ordered root publish returned zero root for %q", rootName)
		}
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	iter, err := buildSystemDeltaIterator(updates)
	if err != nil {
		return nil, CollectionMeta{}, err
	}
	return iter, updatedMeta, nil
}
