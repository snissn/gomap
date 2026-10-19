package collections

import (
	"errors"
	"fmt"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Native preparation keeps the existing transaction's single opening and typed
// projection. It does not construct legacy semantic blocks, a second typed
// batch, assets or a publication identity.
func (c *Collection) prepareNativeInsertBatchOwned(ids, documents [][]byte, maxOwnedBytes int64) (prepared *PreparedInsertBatch, resultErr error) {
	if c.manager.isClosing() {
		return nil, backenddb.ErrClosed
	}
	if len(ids) != len(documents) {
		return nil, errors.New("collections: caller-provided batch ids length mismatch")
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: empty batch or byte limit", ErrPreparedInsertIneligible)
	}
	for _, id := range ids {
		if len(id) == 0 {
			return nil, errors.New("collections: document id cannot be empty")
		}
	}
	// The handle's immutable schema permits rejecting unsupported requests and
	// limits before capturing an opening. Validate the actual bound schema too.
	if err := validatePreparedInsert(c.meta, ids, documents, maxOwnedBytes); err != nil {
		return nil, err
	}
	if err := nativeTypedProfile(c.nativeJSON.db, c.meta); err != nil {
		return nil, err
	}
	owned, reserve, err := nativePreparedInsertBudget(c.meta, ids, documents)
	if err != nil {
		return nil, err
	}
	if owned > maxOwnedBytes || reserve > maxOwnedBytes-owned {
		return nil, fmt.Errorf("%w: native prepared bytes %d plus commit reserve %d exceed %d", ErrPreparedInsertResourceLimit, owned, reserve, maxOwnedBytes)
	}
	tx, err := c.nativeJSON.Begin(c.name)
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, tx.Close())
		}
	}()
	meta := tx.bindings[0].probe.meta
	if err := validatePreparedInsert(meta, ids, documents, maxOwnedBytes); err != nil {
		return nil, err
	}
	owned, reserve, err = nativePreparedInsertBudget(meta, ids, documents)
	if err != nil {
		return nil, err
	}
	if owned > maxOwnedBytes || reserve > maxOwnedBytes-owned {
		return nil, fmt.Errorf("%w: native prepared bytes %d plus commit reserve %d exceed %d", ErrPreparedInsertResourceLimit, owned, reserve, maxOwnedBytes)
	}
	if err := tx.InsertBatch(ids, documents); err != nil {
		return nil, err
	}
	if err := tx.admitStage(&tx.bindings[0], memtable.COWAllocationCharge(uint64(unsafe.Sizeof(PreparedInsertBatch{})))); err != nil {
		return nil, err
	}
	// The canonical command and projection already own independent input
	// copies. Keep result IDs in request order without borrowing either backing.
	resultIDs, err := cloneBatchDocumentIDs(ids)
	if err != nil {
		return nil, err
	}
	return &PreparedInsertBatch{collection: c, nativeTxn: tx, ids: resultIDs, ownedBytes: owned, commitReserve: reserve}, nil
}

// The request envelope reuses the native parser/materializer capacity formulas.
// Residual JSON can escape each source byte six times. Scalar strings cannot
// exceed their source; charge both string and byte backing for construction.
// Existing native receipts remain the allocation authority; this envelope only
// checks the public caller's request limit, including input ownership and results.
// Shared opening state and unrelated commands in a checkpoint retain their own
// original engine receipts and are not attributed to this request.
func nativePreparedInsertBudget(meta CollectionMeta, ids, documents [][]byte) (owned, reserve int64, err error) {
	// This gate requires nativeTypedMeta's exactly-one-column schema before
	// the scalar-specific bound below can inspect Columns[0].
	retained, scratch, err := nativeTypedStageCapacity(meta, ids, documents)
	if err != nil {
		return 0, 0, err
	}
	source, err := nativeTypedManifestAppendCapacity(meta)
	if err != nil {
		return 0, 0, err
	}
	for i, doc := range documents {
		residual, e := nativeTypedCheckedMul(uint64(len(doc)), 6)
		if e != nil {
			return 0, 0, e
		}
		if e = nativeTypedCheckedAdd(&residual, 2); e != nil {
			return 0, 0, e
		}
		row, e := nativeTypedDeltaRowBuildCapacity(uint64(len(ids[i])), residual)
		if e != nil {
			return 0, 0, e
		}
		if meta.Options.ColumnStore.Columns[0].ValueType == ColumnStoreValueString {
			strings, e := nativeTypedCheckedMul(uint64(len(doc)), 24)
			if e != nil {
				return 0, 0, e
			}
			if e = nativeTypedCheckedAdd(&row, memtable.COWAllocationCharge(strings)); e != nil {
				return 0, 0, e
			}
		}
		if e = nativeTypedCheckedAdd(&source, row); e != nil {
			return 0, 0, e
		}
	}
	build, err := nativeTypedCollectionBuildCapacity(meta, source)
	if err != nil {
		return 0, 0, err
	}
	// Fixed controls plus per-row quota units cover dependency-array growth,
	// original typed headers, canonical member/header copies and result IDs.
	// Eight source copies also cover the original and composite WAL images.
	owned = saturatingAddNonNegativeInt64(preparedInsertMul(8, preparedInsertInputBytes(ids, documents)), preparedInsertMul(4096, int64(len(ids))))
	owned = saturatingAddNonNegativeInt64(owned, 1<<20)
	const maxInt64 = uint64(^uint64(0) >> 1)
	if retained > maxInt64 || scratch > maxInt64 || build > maxInt64 {
		return 0, 0, ErrPreparedInsertResourceLimit
	}
	owned = saturatingAddNonNegativeInt64(owned, int64(retained))
	reserve = saturatingAddNonNegativeInt64(int64(scratch), int64(build))
	reserve = saturatingAddNonNegativeInt64(reserve, preparedInsertPublisherReserveBytes())
	return owned, reserve, nil
}
