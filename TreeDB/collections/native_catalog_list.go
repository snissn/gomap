package collections

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// listCollectionsBounded reads one complete logical cut, including pending
// catalog and schema changes. The immutable names directory is ordered and has
// an exact count, so a bounded list copies at most the requested schemas without
// scanning remaining names. A nil engine uses the existing full physical
// catalog import first. Returned metadata belongs to the caller.
func (m *NativeJSONManager) listCollectionsBounded(maxCollections int) (out []CollectionMeta, truncated bool, resultErr error) {
	if m == nil || m.db == nil {
		return nil, false, backenddb.ErrClosed
	}
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return nil, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, capture.Close()) }()
	base, ok := capture.State().(*nativeEngineVersion)
	if capture.State() != nil && !ok {
		return nil, false, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		base, err = importNativeJSONCatalog(capture)
		if base != nil {
			defer func() {
				record := base.Release()
				base = nil
				resultErr = errors.Join(resultErr, finishPrivateNative(record))
			}()
		}
		if err != nil {
			return nil, false, err
		}
	}
	count := base.names.Len()
	if maxCollections > 0 && count > maxCollections {
		count, truncated = maxCollections, true
	}
	if count == 0 {
		return nil, false, nil
	}
	out = make([]CollectionMeta, 0, count)
	base.names.Scan(func(_ string, collection *nativeCollectionState) bool {
		out = append(out, *collection.schema.meta.copy())
		return len(out) < count
	})
	return out, truncated, nil
}
