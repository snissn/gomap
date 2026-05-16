package db

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// CommandWALReplayHandler applies one deterministic command-WAL frame through
// its normal high-level executor and must publish the supplied frame LSN.
type CommandWALReplayHandler func(db *DB, env commitlog.CommandEnvelope) error

var commandWALReplayHandlers sync.Map // map[commitlog.CommandKind]CommandWALReplayHandler

// RegisterCommandWALReplayHandler installs a replay handler for a command kind.
// It is intended for package init registration by higher-level executors.
func RegisterCommandWALReplayHandler(kind commitlog.CommandKind, handler CommandWALReplayHandler) {
	if handler == nil {
		panic("treedb: nil command wal replay handler")
	}
	if _, loaded := commandWALReplayHandlers.LoadOrStore(kind, handler); loaded {
		panic("treedb: duplicate command wal replay handler")
	}
}

func lookupCommandWALReplayHandler(kind commitlog.CommandKind) (CommandWALReplayHandler, bool) {
	handler, ok := commandWALReplayHandlers.Load(kind)
	if !ok {
		return nil, false
	}
	typed, ok := handler.(CommandWALReplayHandler)
	return typed, ok
}
