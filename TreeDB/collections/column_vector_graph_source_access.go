package collections

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

const columnVectorGraphServingSetupScopeID = "column-vector-graph-serving-setup"

// columnVectorGraphSourceAccess is the serving holder's only file-access
// capability. Generic prepared readers never receive one and retain their
// existing private AcquireFileRange/read behavior.
//
// The pool owns physical backings. Existing source managers continue to own
// logical mappedresource handles, so checksum, direct-view, pin, and logical
// accounting semantics stay at the loader boundary where they already live.
type columnVectorGraphSourceAccess struct {
	pool     *columnServingSegmentLeaseSet
	buildCtx context.Context
}

func newColumnVectorGraphServingSourceAccess(ctx context.Context, pool *columnServingSegmentLeaseSet) (*columnVectorGraphSourceAccess, error) {
	if pool == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &columnVectorGraphSourceAccess{pool: pool, buildCtx: ctx}, nil
}

func (a *columnVectorGraphSourceAccess) context(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	if a != nil && a.buildCtx != nil {
		return a.buildCtx
	}
	return context.Background()
}

// acquireRange authorizes the exact parent before the pool performs any
// FileID lookup, then transfers the ordinary mappedresource handle to the
// existing source owner. The logical key still names only the requested
// section; a mapped segment prefix is storage and never authority.
func (a *columnVectorGraphSourceAccess) acquireRange(ctx context.Context, rootDir string, parent ColumnAssetRef, manager *mappedresource.Manager, key mappedresource.Key, scope mappedresource.Scope, opts mappedresource.AcquireOptions) (*mappedresource.Handle, error) {
	if a == nil || a.pool == nil {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	// Defer every authority decision to the pool, which authorizes parent before
	// looking up its FileID. A negative sentinel avoids overflowing the checked
	// relative-range validation when a malformed logical key precedes parent.
	relativeOffset := int64(-1)
	if parent.Offset >= 0 && key.Offset >= parent.Offset {
		relativeOffset = key.Offset - parent.Offset
	}
	rangeHandle, err := a.pool.acquireRange(a.context(ctx), rootDir, parent, relativeOffset, key.Length, manager, key, scope, opts)
	if err != nil {
		return nil, err
	}
	handle := rangeHandle.takeMappedResourceHandle()
	if handle == nil {
		return nil, errors.New("collections: serving source acquisition returned no logical handle")
	}
	return handle, nil
}

// withAsset borrows one exact full parent for setup parsing and validation.
// Mapped parents are viewed directly; descriptor-backed parents are copied
// into callback-local scratch. Neither path populates the pool-owned retained
// fallback buffer used by holder logical sources.
func (a *columnVectorGraphSourceAccess) withAsset(ctx context.Context, rootDir string, parent ColumnAssetRef, reason string, fn func([]byte) error) error {
	if a == nil || a.pool == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	if fn == nil {
		return errors.New("collections: serving source setup callback is required")
	}
	key := mappedResourceKeyForColumnAssetRef(parent)
	scope := mappedresource.Scope{
		Kind:       mappedresource.ScopePreparedSearch,
		ID:         columnVectorGraphServingSetupScopeID,
		Collection: a.pool.collection,
		Namespace:  parent.Namespace,
		Generation: parent.Generation,
		Reason:     reason,
	}
	useCtx := a.context(ctx)
	return a.pool.withBorrowedRange(useCtx, rootDir, parent, 0, parent.Length, key, scope, func(borrowed columnServingBorrowedRange) error {
		if err := useCtx.Err(); err != nil {
			return err
		}
		raw := borrowed.Bytes()
		if raw == nil {
			if parent.Length > int64(maxCollectionInt) {
				return fmt.Errorf("collections: serving source setup length=%d overflows int", parent.Length)
			}
			raw = make([]byte, int(parent.Length))
			n, err := borrowed.ReadAt(raw)
			if err != nil && err != io.EOF {
				return err
			}
			if n != len(raw) {
				return io.ErrUnexpectedEOF
			}
		}
		// Setup readers historically use Verify, not CachedVerify. Preserve that
		// full-parent checksum gate even though retained logical sections perform
		// their own section checksum validation below.
		if err := verifyColumnPhysicalAssetReadChecksumWithIntegrityForSegment(raw, parent, true, ColumnAssetReadIntegrityVerify, rootDir, borrowed.Identity()); err != nil {
			return err
		}
		if err := useCtx.Err(); err != nil {
			return err
		}
		return fn(raw)
	})
}

func withColumnVectorGraphSourceAsset(access *columnVectorGraphSourceAccess, ctx context.Context, rootDir string, ref ColumnAssetRef, reason string, fn func([]byte) error) error {
	if access != nil {
		return access.withAsset(ctx, rootDir, ref, reason, fn)
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, ref)
	if err != nil {
		return err
	}
	return fn(raw)
}
