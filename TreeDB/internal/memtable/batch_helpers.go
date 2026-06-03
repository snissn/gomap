package memtable

import (
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func batchEntryPayload(op batchpkg.Entry, storeInlinePtrValues bool) (value []byte, ptr page.ValuePtr, flags byte) {
	switch {
	case op.Type == batchpkg.OpDelete:
		return nil, page.ValuePtr{}, node.FlagTombstone
	case op.IsPtr:
		if storeInlinePtrValues {
			return op.Value, op.ValuePtr, node.FlagPointer
		}
		return nil, op.ValuePtr, node.FlagPointer
	default:
		return op.Value, page.ValuePtr{}, node.FlagInline
	}
}
