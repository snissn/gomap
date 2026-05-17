package db

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// CommandWALReplayHandler applies one deterministic command-WAL frame through
// its normal high-level executor and must publish the supplied frame LSN.
type CommandWALReplayHandler func(db *DB, env commitlog.CommandEnvelope) error

type CommandWALReplayHandlerOptions struct {
	NeedsReplayLogSupport bool
}

type commandWALReplayHandlerRegistration struct {
	handler               CommandWALReplayHandler
	needsReplayLogSupport bool
}

var commandWALReplayHandlers sync.Map // map[commitlog.CommandKind]commandWALReplayHandlerRegistration

// RegisterCommandWALReplayHandler installs a replay handler for a command kind.
// It is intended for package init registration by higher-level executors.
func RegisterCommandWALReplayHandler(kind commitlog.CommandKind, handler CommandWALReplayHandler) {
	RegisterCommandWALReplayHandlerWithOptions(kind, handler, CommandWALReplayHandlerOptions{NeedsReplayLogSupport: true})
}

// RegisterCommandWALReplayHandlerWithOptions installs a replay handler with
// explicit recovery support requirements. Handlers that replay only in-memory
// metadata can opt out of value-log/leaf-log replay setup.
func RegisterCommandWALReplayHandlerWithOptions(kind commitlog.CommandKind, handler CommandWALReplayHandler, opts CommandWALReplayHandlerOptions) {
	if handler == nil {
		panic("treedb: nil command wal replay handler")
	}
	registration := commandWALReplayHandlerRegistration{
		handler:               handler,
		needsReplayLogSupport: opts.NeedsReplayLogSupport,
	}
	if _, loaded := commandWALReplayHandlers.LoadOrStore(kind, registration); loaded {
		panic("treedb: duplicate command wal replay handler")
	}
}

func lookupCommandWALReplayHandler(kind commitlog.CommandKind) (commandWALReplayHandlerRegistration, bool) {
	handler, ok := commandWALReplayHandlers.Load(kind)
	if !ok {
		return commandWALReplayHandlerRegistration{}, false
	}
	typed, ok := handler.(commandWALReplayHandlerRegistration)
	return typed, ok
}
