package raftentry

import "github.com/snissn/gomap/TreeDB/internal/nativewire"

type DecisionV1 string

const (
	DecisionAccepted DecisionV1 = "accepted"
	DecisionRejected DecisionV1 = "rejected"
)

type DuplicateModeV1 string

const (
	DuplicateReplayRequiredV1      DuplicateModeV1 = "replay-required"
	DuplicateFailClosedAllowedV1   DuplicateModeV1 = "duplicate-fail-closed-allowed"
	DuplicateRejectedUnsupportedV1 DuplicateModeV1 = "rejected"
)

type ResultReplayModeV1 string

const (
	ResultReplayPersistedPayloadOrDigestV1 ResultReplayModeV1 = "persisted-result-payload-or-digest"
	ResultReplayFailClosedV1               ResultReplayModeV1 = "fail-closed-until-result-store"
	ResultReplayNoneRejectedV1             ResultReplayModeV1 = "none-rejected"
)

type CommandRowV1 struct {
	Known                   bool
	CommandID               nativewire.CommandID
	NativeWireCommand       string
	CommandName             string
	Decision                DecisionV1
	DuplicateMode           DuplicateModeV1
	ResultReplayMode        ResultReplayModeV1
	CommandWALStatus        string
	CommandWALKind          string
	DeterministicEntryBytes string
	Reason                  string
}

func ClassifyNativeWireCommandV1(id nativewire.CommandID) CommandRowV1 {
	row, ok := commandRowsV1[id]
	if !ok {
		return CommandRowV1{CommandID: id}
	}
	return row
}

func AllCommandRowsV1() []CommandRowV1 {
	ids := []nativewire.CommandID{
		nativewire.CommandCreateCollection,
		nativewire.CommandListCollections,
		nativewire.CommandCreateIndex,
		nativewire.CommandListIndexes,
		nativewire.CommandDropIndex,
		nativewire.CommandOpenCollection,
		nativewire.CommandCloseCollection,
		nativewire.CommandDropCollection,
		nativewire.CommandInsertBatch,
		nativewire.CommandReplaceBatch,
		nativewire.CommandDeleteBatch,
		nativewire.CommandFlushCollection,
		nativewire.CommandFlushAll,
		nativewire.CommandCheckpoint,
		nativewire.CommandUpdateBSONSet,
		nativewire.CommandGetMany,
		nativewire.CommandIndexLookup,
		nativewire.CommandIndexRange,
		nativewire.CommandOpenScan,
		nativewire.CommandCursorNext,
		nativewire.CommandCursorClose,
		nativewire.CommandExplain,
		nativewire.CommandStats,
	}
	rows := make([]CommandRowV1, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, ClassifyNativeWireCommandV1(id))
	}
	return rows
}

var commandRowsV1 = map[nativewire.CommandID]CommandRowV1{
	nativewire.CommandCreateCollection: acceptedRow(
		nativewire.CommandCreateCollection,
		"CommandCreateCollection",
		"create_collection",
		"CatalogCreateCollection",
		"native-wire deterministic entry fixture",
		"first vertical-slice contract; duplicates fail closed until durable result replay lands",
	),

	nativewire.CommandCreateIndex:    rejectedRow(nativewire.CommandCreateIndex, "CommandCreateIndex", "create_index", "WAL-rejected", "future catalog index create", "future_rejected_v1", "index DDL is deterministic at native-wire level but has no accepted R3a command-WAL payload/recovery contract"),
	nativewire.CommandDropIndex:      rejectedRow(nativewire.CommandDropIndex, "CommandDropIndex", "drop_index", "WAL-rejected", "future catalog index drop", "future_rejected_v1", "index DDL is deterministic at native-wire level but has no accepted R3a command-WAL payload/recovery contract"),
	nativewire.CommandDropCollection: rejectedRow(nativewire.CommandDropCollection, "CommandDropCollection", "drop_collection", "WAL-rejected", "future catalog collection drop", "local_only_rejected_v1", "collection drop is local-only/rejected until catalog payloads and recovery tests exist"),

	nativewire.CommandInsertBatch:   rejectedRow(nativewire.CommandInsertBatch, "CommandInsertBatch", "insert_batch", "WAL-supported", "CollectionInsertBatchByID", "native-wire deterministic entry fixture", "classified for future widening but not accepted before create identity/generation and result replay contracts land"),
	nativewire.CommandReplaceBatch:  rejectedRow(nativewire.CommandReplaceBatch, "CommandReplaceBatch", "replace_batch", "WAL-supported", "CollectionUpdateBatchByID", "native-wire deterministic entry fixture", "classified for future widening but not accepted before create identity/generation and result replay contracts land"),
	nativewire.CommandDeleteBatch:   rejectedRow(nativewire.CommandDeleteBatch, "CommandDeleteBatch", "delete_batch", "WAL-supported", "CollectionDeleteBatchByID", "native-wire deterministic entry fixture", "classified for future widening but not accepted before create identity/generation and result replay contracts land"),
	nativewire.CommandUpdateBSONSet: rejectedRow(nativewire.CommandUpdateBSONSet, "CommandUpdateBSONSet", "update_bson_set", "WAL-supported", "CollectionUpdateBatchByID", "native-wire deterministic entry fixture", "classified for future widening but not accepted before create identity/generation and result replay contracts land"),

	nativewire.CommandFlushCollection: rejectedRow(nativewire.CommandFlushCollection, "CommandFlushCollection", "flush_collection", "WAL-supported", "local durability barrier", "local_only_barrier_v1", "local durability barriers are not replicated command identity"),
	nativewire.CommandFlushAll:        rejectedRow(nativewire.CommandFlushAll, "CommandFlushAll", "flush_all", "WAL-supported", "local durability barrier", "local_only_barrier_v1", "local durability barriers are not replicated command identity"),
	nativewire.CommandCheckpoint:      rejectedRow(nativewire.CommandCheckpoint, "CommandCheckpoint", "checkpoint", "WAL-supported", "local durability barrier", "local_only_barrier_v1", "local durability barriers are not replicated command identity"),

	nativewire.CommandListCollections: rejectedRow(nativewire.CommandListCollections, "CommandListCollections", "list_collections", "read-only", "none", "read_rejected_v1", "reads and metadata listing are not replicated mutations"),
	nativewire.CommandListIndexes:     rejectedRow(nativewire.CommandListIndexes, "CommandListIndexes", "list_indexes", "read-only", "none", "read_rejected_v1", "reads and metadata listing are not replicated mutations"),
	nativewire.CommandOpenCollection:  rejectedRow(nativewire.CommandOpenCollection, "CommandOpenCollection", "open_collection", "read-only", "none", "read_rejected_v1", "connection-local collection handles are not deterministic command identity"),
	nativewire.CommandCloseCollection: rejectedRow(nativewire.CommandCloseCollection, "CommandCloseCollection", "close_collection", "read-only", "none", "read_rejected_v1", "connection-local collection handles are not deterministic command identity"),
	nativewire.CommandGetMany:         rejectedRow(nativewire.CommandGetMany, "CommandGetMany", "get_many", "read-only", "none", "read_rejected_v1", "reads are not replicated mutations"),
	nativewire.CommandIndexLookup:     rejectedRow(nativewire.CommandIndexLookup, "CommandIndexLookup", "index_lookup", "read-only", "none", "read_rejected_v1", "reads are not replicated mutations"),
	nativewire.CommandIndexRange:      rejectedRow(nativewire.CommandIndexRange, "CommandIndexRange", "index_range", "read-only", "none", "read_rejected_v1", "reads are not replicated mutations"),
	nativewire.CommandOpenScan:        rejectedRow(nativewire.CommandOpenScan, "CommandOpenScan", "open_scan", "read-only", "none", "read_rejected_v1", "cursor state is not replicated command identity"),
	nativewire.CommandCursorNext:      rejectedRow(nativewire.CommandCursorNext, "CommandCursorNext", "cursor_next", "read-only", "none", "read_rejected_v1", "cursor state is not replicated command identity"),
	nativewire.CommandCursorClose:     rejectedRow(nativewire.CommandCursorClose, "CommandCursorClose", "cursor_close", "read-only", "none", "read_rejected_v1", "cursor state is not replicated command identity"),
	nativewire.CommandExplain:         rejectedRow(nativewire.CommandExplain, "CommandExplain", "explain", "read-only", "none", "read_rejected_v1", "reads are not replicated mutations"),
	nativewire.CommandStats:           rejectedRow(nativewire.CommandStats, "CommandStats", "stats", "read-only", "none", "read_rejected_v1", "reads are not replicated mutations"),
}

func acceptedRow(id nativewire.CommandID, nativeWireCommand, commandName, walKind, bytes, reason string) CommandRowV1 {
	return CommandRowV1{
		Known:                   true,
		CommandID:               id,
		NativeWireCommand:       nativeWireCommand,
		CommandName:             commandName,
		Decision:                DecisionAccepted,
		DuplicateMode:           DuplicateFailClosedAllowedV1,
		ResultReplayMode:        ResultReplayFailClosedV1,
		CommandWALStatus:        "WAL-supported",
		CommandWALKind:          walKind,
		DeterministicEntryBytes: bytes,
		Reason:                  reason,
	}
}

func rejectedRow(id nativewire.CommandID, nativeWireCommand, commandName, walStatus, walKind, bytes, reason string) CommandRowV1 {
	return CommandRowV1{
		Known:                   true,
		CommandID:               id,
		NativeWireCommand:       nativeWireCommand,
		CommandName:             commandName,
		Decision:                DecisionRejected,
		DuplicateMode:           DuplicateRejectedUnsupportedV1,
		ResultReplayMode:        ResultReplayNoneRejectedV1,
		CommandWALStatus:        walStatus,
		CommandWALKind:          walKind,
		DeterministicEntryBytes: bytes,
		Reason:                  reason,
	}
}
