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
	commandBytes       int
	rowRemainderBytes  int
	columnPayloadBytes int
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
	return fmt.Errorf("%w: column_store writes require command WAL", backenddb.ErrCommandWALUnsupported)
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
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("%w: column_store publish requires command WAL intent", backenddb.ErrCommandWALUnsupported)
	}
	columnRootName := collectionColumnManifestRootName(input.meta.Name)
	columnBaseRoot := uint64(0)
	if input.catalog != nil {
		columnBaseRoot = input.catalog.rootID(columnRootName)
	}
	rootNames, baseRootIDs := appendColumnManifestRootPublishBase(input.rootNames, input.baseRootIDs, columnRootName, columnBaseRoot)
	var plan ColumnPublishPlan
	newSystemRoot, rootIDs, err := c.db.PublishOrderedRootDeltaGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(
		ordered,
		input.commandWALIntent,
		func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaPublishInput, error) {
			nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
			if err != nil {
				return nil, err
			}
			plan = nextPlan
			ordered, err := plan.RootDelta.OrderedRootDeltaPublishInput()
			if err != nil {
				return nil, err
			}
			return []backenddb.OrderedRootDeltaPublishInput{ordered}, nil
		},
		func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
			if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
				return nil, errors.New("collections: column publish plan LSN mismatch")
			}
			return c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
		},
	)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	updatedMeta, err := columnPublishUpdatedMeta(input.meta, plan)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
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
		return 0, nil, CollectionMeta{}, nil, fmt.Errorf("%w: column_store publish requires command WAL intent", backenddb.ErrCommandWALUnsupported)
	}
	columnRootName := collectionColumnManifestRootName(input.meta.Name)
	columnBaseRoot := uint64(0)
	if input.catalog != nil {
		columnBaseRoot = input.catalog.rootID(columnRootName)
	}
	rootNames, baseRootIDs := appendColumnManifestRootPublishBase(input.rootNames, input.baseRootIDs, columnRootName, columnBaseRoot)
	var plan ColumnPublishPlan
	var cleanupColumnDelta func()
	defer func() {
		if cleanupColumnDelta != nil {
			cleanupColumnDelta()
		}
	}()
	buildColumnDelta := func(ctx backenddb.CommandWALPublishContext) ([]backenddb.OrderedRootDeltaBatchPublishInput, error) {
		nextPlan, err := c.buildColumnPublishPlanForCommandWALContext(ctx, input, columnBaseRoot)
		if err != nil {
			return nil, err
		}
		plan = nextPlan
		ordered, cleanup, err := plan.RootDelta.OrderedRootDeltaBatchPublishInput()
		if err != nil {
			return nil, err
		}
		cleanupColumnDelta = cleanup
		return []backenddb.OrderedRootDeltaBatchPublishInput{ordered}, nil
	}
	buildSystemDelta := func(ctx backenddb.CommandWALPublishContext, rootIDs []uint64) (iterator.UnsafeIterator, error) {
		if plan.AppliedCommandLSN != ctx.AppliedCommandLSN {
			return nil, errors.New("collections: column publish plan LSN mismatch")
		}
		return c.buildRootDescriptorAndColumnManifestSystemDeltaIteratorForMeta(input.meta, input.baseCommitSeq, input.baseSystemRoot, rootNames, baseRootIDs, rootIDs, plan)
	}
	var newSystemRoot uint64
	var rootIDs []uint64
	var err error
	if preflight != nil {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithPreflightCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, preflight, input.commandWALIntent, buildColumnDelta, buildSystemDelta)
	} else {
		newSystemRoot, rootIDs, err = c.db.PublishOrderedRootDeltaBatchGroupWithCommandWALContextRootBuilderAndSystemDeltaBuilder(ordered, input.commandWALIntent, buildColumnDelta, buildSystemDelta)
	}
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	updatedMeta, err := columnPublishUpdatedMeta(input.meta, plan)
	if err != nil {
		return 0, nil, CollectionMeta{}, nil, err
	}
	return newSystemRoot, rootIDs, updatedMeta, rootNames, nil
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

func appendColumnManifestRootPublishBase(rootNames []string, baseRootIDs map[string]uint64, columnRootName string, columnBaseRoot uint64) ([]string, map[string]uint64) {
	nextRootNames := make([]string, 0, len(rootNames)+1)
	nextRootNames = append(nextRootNames, rootNames...)
	nextRootNames = append(nextRootNames, columnRootName)
	nextBaseRootIDs := make(map[string]uint64, len(baseRootIDs)+1)
	for rootName, rootID := range baseRootIDs {
		nextBaseRootIDs[rootName] = rootID
	}
	nextBaseRootIDs[columnRootName] = columnBaseRoot
	return nextRootNames, nextBaseRootIDs
}

func columnPublishUpdatedMeta(base CollectionMeta, plan ColumnPublishPlan) (CollectionMeta, error) {
	if !plan.Enabled {
		return base, nil
	}
	updated := copyCollectionMeta(base)
	if updated.Options.ColumnStore == nil || !updated.Options.ColumnStore.Enabled {
		return CollectionMeta{}, errors.New("collections: column manifest publish requires enabled column_store metadata")
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
	if len(rootIDs) != len(rootNames) {
		return nil, unexpectedOrderedRootCountError(meta.Name, len(rootNames), len(rootIDs))
	}
	if !plan.Enabled {
		return nil, errors.New("collections: disabled column publish plan cannot build system delta")
	}
	if err := c.validateRootDescriptorSystemDeltaForMeta(meta, expectedCommitSeq, expectedSystemRoot, rootNames, baseRootIDs); err != nil {
		return nil, err
	}
	updatedMeta, err := columnPublishUpdatedMeta(meta, plan)
	if err != nil {
		return nil, err
	}
	encodedMeta, err := encodeCollectionMeta(updatedMeta)
	if err != nil {
		return nil, err
	}
	updates := make(map[string][]byte, len(rootNames)+1)
	updates[systemCollectionMetaKey(updatedMeta.Name)] = encodedMeta
	for i, rootName := range rootNames {
		if rootIDs[i] == 0 {
			return nil, fmt.Errorf("collections: ordered root publish returned zero root for %q", rootName)
		}
		updates[systemCollectionRootKey(rootName)] = encodeRootID(rootIDs[i])
	}
	return buildSystemDeltaIterator(updates)
}
