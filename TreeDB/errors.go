package treedb

import (
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/tree"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = db.ErrLocked
	// ErrMemtableFull indicates the cached memtable has reached its hard cap.
	ErrMemtableFull = caching.ErrMemtableFull
	// ErrBatchDeleteRangeTooLarge indicates the cached batch DeleteRange fallback
	// exceeded its bounded materialization cap before publishing any mutation.
	ErrBatchDeleteRangeTooLarge = caching.ErrBatchDeleteRangeTooLarge

	// ErrClosed indicates the DB handle has been closed.
	ErrClosed = db.ErrClosed
	// ErrRecoveryRequired indicates the DB must be opened read-write for recovery
	// before the requested read-only or offline-maintenance operation can run.
	ErrRecoveryRequired = db.ErrRecoveryRequired
	// ErrCommandWALUnsupported indicates a directory advertises command_wal_v2
	// before this binary has enabled command WAL execution/recovery.
	ErrCommandWALUnsupported = db.ErrCommandWALUnsupported
	// ErrCommandWALRejected indicates a command is intentionally rejected while
	// command_wal_v2 is active.
	ErrCommandWALRejected = db.ErrCommandWALRejected
	// ErrCommandWALSegmentSeqExhausted indicates no new command-WAL segment
	// sequence is available during open.
	ErrCommandWALSegmentSeqExhausted = db.ErrCommandWALSegmentSeqExhausted
	// ErrUnsupportedRequiredFeature indicates format.json requires a storage
	// feature this binary does not understand.
	ErrUnsupportedRequiredFeature = db.ErrUnsupportedRequiredFeature
	// ErrLegacyFormatRebuildRequired indicates a pre-alpha directory has no
	// compatible persisted durability-profile contract.
	ErrLegacyFormatRebuildRequired = db.ErrLegacyFormatRebuildRequired
	// ErrConditionalTxnClosed indicates a conditional transaction was used after
	// Commit, CommitSync, or Close.
	ErrConditionalTxnClosed = db.ErrConditionalTxnClosed
	// ErrConcurrentModification indicates a conditional publish observed a read
	// precondition that changed after the transaction opened.
	ErrConcurrentModification = db.ErrConcurrentModification
	// ErrLeafGenerationGCStaleScan indicates both bounded dry-run attempts were
	// invalidated by concurrent publishes before their results could be used.
	ErrLeafGenerationGCStaleScan = db.ErrLeafGenerationGCStaleScan
	// ErrLeafGenerationManifestIncompatible indicates the split-leaf manifest
	// requires an unsupported pre-alpha format or has no persistent revision.
	ErrLeafGenerationManifestIncompatible = db.ErrLeafGenerationManifestIncompatible
	// ErrConditionalTxnUnsupported indicates the selected TreeDB mode cannot
	// provide native conditional transaction semantics.
	ErrConditionalTxnUnsupported = db.ErrConditionalTxnUnsupported

	// ErrKeyNotFound indicates the key does not exist.
	ErrKeyNotFound = tree.ErrKeyNotFound
)
