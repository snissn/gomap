package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/collectionwal"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	// ErrLocked indicates the database directory is already opened by another process.
	ErrLocked = lockfile.ErrLocked
	// ErrReadOnly indicates a write was attempted on a read-only DB handle.
	ErrReadOnly = errors.New("treedb: read-only")
	// ErrClosed indicates the DB handle is closed or closing for reads.
	ErrClosed = errors.New("treedb: db is closed")
	// ErrConcurrentModification indicates a publish or maintenance operation
	// observed that a root changed after its validation point.
	ErrConcurrentModification = errors.New("treedb: concurrent modification")
	// ErrLeafGenerationGCStaleScan indicates both bounded dry-run attempts were
	// invalidated by concurrent publishes before their results could be used.
	ErrLeafGenerationGCStaleScan = errors.New("treedb: leaf generation gc scan invalidated")
	// ErrConditionalTxnClosed indicates a conditional transaction was used after
	// Commit, CommitSync, or Close.
	ErrConditionalTxnClosed = errors.New("treedb: conditional transaction is closed")
	// ErrConditionalTxnUnsupported indicates the selected TreeDB mode cannot
	// provide native conditional transaction semantics.
	ErrConditionalTxnUnsupported = errors.New("treedb: conditional transactions unsupported")
	// ErrRecoveryRequired indicates the DB must be opened read-write for recovery
	// before the requested read-only or offline-maintenance operation can run.
	ErrRecoveryRequired = collectionwal.ErrCollectionWALRecoveryRequired
	// ErrLegacyCachedRedoJournalReplayDisabled indicates that a directory still
	// has legacy cached redo-journal segments. Current command-WAL opens do not
	// replay those segments by default; forensic compatibility replay must be
	// requested explicitly.
	ErrLegacyCachedRedoJournalReplayDisabled = errors.New("treedb: legacy cached redo journal replay disabled")
	// ErrUnsupportedRequiredFeature indicates format.json requires a storage
	// feature this binary does not understand.
	ErrUnsupportedRequiredFeature = errors.New("treedb: unsupported required storage feature")
	// ErrCommandWALUnsupported is returned when command_wal_v2 execution is not
	// available for the requested handle or operation.
	ErrCommandWALUnsupported = errors.New("treedb: command_wal_v2 execution is not enabled")
	// ErrCommandWALRejected is the stable public sentinel for commands that are
	// intentionally rejected while command_wal_v2 is active.
	ErrCommandWALRejected = errors.New("treedb: command_wal_v2 command rejected")
	// ErrCommandWALSegmentSeqExhausted indicates the command WAL segment sequence
	// number space has no strictly higher segment available.
	ErrCommandWALSegmentSeqExhausted = errors.New("treedb: command wal segment sequence exhausted")
	// ErrNoRecoverableMeta reports that neither independently decoded durable
	// root slot closed over a complete bounded recovery inventory.
	ErrNoRecoverableMeta = errors.New("treedb: no recoverable durable meta")
	// ErrLegacyFormatRebuildRequired distinguishes a pre-cutover TreeDB index
	// from corruption. TreeDB is pre-alpha; the supported transition is to
	// rebuild the database directory rather than run an in-place migration.
	ErrLegacyFormatRebuildRequired = page.ErrDurableMetaLegacyFormat
)
