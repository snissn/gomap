package db

import (
	"errors"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// RegisterCommandWALReplayFinalizer retains derived-state work until all
// frames in the current recovery pass have been applied. It accepts
// registrations only from an active command-WAL replay handler, so ordinary
// deterministic/apply managers do not become recovery hook owners.
func (db *DB) RegisterCommandWALReplayFinalizer(finalize func() error) bool {
	if db == nil || finalize == nil || db.commandWALReplayLSN.Load() == 0 || db.commandWALReplayToken.Load() == 0 {
		return false
	}
	db.commandWALReplayFinalizersMu.Lock()
	defer db.commandWALReplayFinalizersMu.Unlock()
	if db.commandWALReplayLSN.Load() == 0 || db.commandWALReplayToken.Load() == 0 {
		return false
	}
	db.commandWALReplayFinalizers = append(db.commandWALReplayFinalizers, finalize)
	return true
}

func (db *DB) runCommandWALReplayFinalizers() error {
	if db == nil {
		return nil
	}
	db.commandWALReplayFinalizersMu.Lock()
	finalizers := db.commandWALReplayFinalizers
	db.commandWALReplayFinalizers = nil
	db.commandWALReplayFinalizersMu.Unlock()
	var errs []error
	for _, finalize := range finalizers {
		if finalize != nil {
			errs = append(errs, finalize())
		}
	}
	return errors.Join(errs...)
}

// CommandWALReplayHandler applies one deterministic command-WAL frame through
// its normal high-level executor and must publish the supplied frame LSN.
type CommandWALReplayHandler func(db *DB, env commitlog.CommandEnvelope) error

// CommandWALNativePrepare shares the existing command-kind registration with
// replay. It prepares immutable effects against the supplied retained complete
// cut; it cannot append, publish, advance coverage or acquire live collections.
// Assignment-dependent native finalization uses the caller's admitted normal
// command append/finalizer after this preassignment preparation.
type CommandWALNativePrepare func(db *DB, env commitlog.CommandEnvelope, basis NativeSemanticState, snapshot *Snapshot, lease *StableResourceCaptureLease) (*NativePreparedPublication, error)

type CommandWALReplayHandlerOptions struct {
	NativePrepare         CommandWALNativePrepare
	NeedsReplayLogSupport bool
}

type commandWALReplayHandlerRegistration struct {
	nativePrepare         CommandWALNativePrepare
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
		nativePrepare:         opts.NativePrepare,
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

// PrepareCommandWALNativeEffects invokes only the registered native preparation
// seam. Unmigrated kinds refuse before assignment instead of publishing partial
// children or treating a replay handler as a composite effects planner.
func (db *DB) PrepareCommandWALNativeEffects(env commitlog.CommandEnvelope, basis NativeSemanticState, snapshot *Snapshot, lease *StableResourceCaptureLease) (*NativePreparedPublication, error) {
	registration, ok := lookupCommandWALReplayHandler(env.Kind)
	if !ok || registration.nativePrepare == nil {
		return nil, ErrNativeSemanticUnsupported
	}
	if snapshot == nil || lease == nil {
		return nil, ErrRootPublicationBasisMismatch
	}
	if err := lease.ValidateDBV1(db); err != nil {
		return nil, err
	}
	return registration.nativePrepare(db, env, basis, snapshot, lease)
}
