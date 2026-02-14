package pebblecompat

import "github.com/cockroachdb/pebble"
import treedb "github.com/snissn/gomap/TreeDB"

const (
	defaultInternalPrefix = "\x00pebblecompat\x00"
	initialSeqNum         = uint64(9)
)

// Options configures the TreeDB-backed Pebble-compat wrapper.
type Options struct {
	// TreeDB controls the underlying storage engine.
	TreeDB treedb.Options
	// InternalPrefix controls the reserved metadata prefix.
	// If unset, defaultInternalPrefix is used.
	InternalPrefix []byte
	// Merger controls merge-value semantics for DB.Merge and batch replay.
	// If unset, pebble.DefaultMerger is used.
	Merger *pebble.Merger
	// SharedMetaResolver optionally resolves unsupported SharedSSTMeta descriptors
	// into staged local paths (for example .pcobj or .sst).
	SharedMetaResolver func(meta pebble.SharedSSTMeta) (localPath string, err error)
	// ExternalFileResolver optionally resolves non-local external descriptors into
	// staged local paths (for example .pcobj or .sst).
	ExternalFileResolver func(file pebble.ExternalFile) (localPath string, err error)
}
