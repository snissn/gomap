package collections

import (
	"bytes"
	"encoding/binary"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

const nativeVectorVersionPrefix = "collection-vector-version/"

func nativeVectorVersionKey(collection, index string) string {
	return nativeVectorVersionPrefix + collection + "/" + index
}

func nativeVectorVersionRecord(interpretation, generation uint64) [16]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], interpretation)
	binary.BigEndian.PutUint64(b[8:], generation)
	return b
}

type vectorDomainRootVersion struct {
	key   string
	value [16]byte
}

func appendVectorDomainVersion(it iterator.UnsafeIterator, version *vectorDomainRootVersion) (iterator.UnsafeIterator, error) {
	base, ok := it.(*systemTargetIterator)
	if !ok || base.idx != 0 {
		_ = it.Close()
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	base.entries = append(base.entries, systemTargetEntry{key: []byte(version.key), value: bytes.Clone(version.value[:])})
	sort.Slice(base.entries, func(i, j int) bool { return bytes.Compare(base.entries[i].key, base.entries[j].key) < 0 })
	return base, nil
}
