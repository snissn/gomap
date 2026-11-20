package db

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type commandWALSealedSync struct {
	target       *commitlog.SealedCommandWALSync
	registeredBy uint64

	// Only the original same-token consumer can contend with the optional
	// attempt. The active successor uses the existing journal-file path.
	mu          sync.Mutex
	foreground  bool
	earlySynced bool
	claimed     bool // guarded by the debt mutex
}

func (debt *CommandWALDependencyDebt) offerSealedSync(lsn uint64, target *commitlog.SealedCommandWALSync) {
	if debt.sealedSyncWake == nil {
		return
	}
	token := target.Token()
	frontier := token.Frontier()
	if frontier.Bytes == 0 || frontier.MaxLSN == 0 || frontier.MaxLSN >= lsn || token.Namespace() != nil {
		return
	}
	debt.mu.Lock()
	defer debt.mu.Unlock()

	// Inspect the complete preceding view or decline. This also rules out
	// any prior active-successor token aliasing the detached file, without
	// adding a scan proportional to a backlog on journal admission.
	const maxDebtEntries = 16
	if debt.sealedSync != nil || debt.capturePins != 0 || debt.deferredReleaseLSN != 0 ||
		len(debt.entries) == 0 || len(debt.entries) > maxDebtEntries {
		return
	}
	last := len(debt.entries) - 1
	entry := debt.entries[last]
	if entry.firstLSN != lsn || len(entry.rotationFiles) != 2 || entry.rotationFiles[0] != token {
		return
	}
	for _, earlier := range debt.entries[:last] {
		if len(earlier.rotationFiles) != 0 || (earlier.firstLSN <= frontier.MaxLSN && len(earlier.resources) != 0) {
			return
		}
	}
	debt.sealedSync = &commandWALSealedSync{target: target, registeredBy: lsn}
	select {
	case debt.sealedSyncWake <- struct{}{}:
	default:
	}
}

func (db *DB) SealedCommandWALSyncNotifications() <-chan struct{} {
	return db.commandWALDebt.sealedSyncWake
}

// RunPendingSealedCommandWALSync is the existing automatic checkpoint
// worker's optional file work. The teardown lease protects the debt owner;
// its capture pin keeps the exact original token live across physical I/O.
func (db *DB) RunPendingSealedCommandWALSync() {
	if !db.teardownMu.TryRLock() {
		return
	}
	defer db.teardownMu.RUnlock()
	if db.closing.Load() || db.commandWALPoisonedError() != nil {
		return
	}
	debt := &db.commandWALDebt
	debt.mu.Lock()
	sealed := debt.sealedSync
	if sealed == nil || sealed.claimed {
		debt.mu.Unlock()
		return
	}
	sealed.claimed = true
	debt.capturePins++
	debt.mu.Unlock()
	defer func() {
		// A publication may still be borrowing debt. Release under the same
		// original custody as other captured WAL operations.
		db.commandWALRawPublishMu.Lock()
		retired := debt.releaseCapture()
		db.commandWALRawPublishMu.Unlock()
		if retired {
			_ = durabilitycut.EmitLSN(durabilitycut.AfterCapturedWALRetirement, durabilitycut.ResourceCommandWAL, db.dir, db.commandWALDurableLSN.Load())
		}
	}()
	sealed.tryEarlySync(db.dir, db.commandWALDir)
}

func (sealed *commandWALSealedSync) tryEarlySync(root, walDir string) {
	if !sealed.mu.TryLock() {
		return
	}
	defer sealed.mu.Unlock()
	if sealed.foreground {
		return
	}
	var err error
	if durabilitycut.Enabled() {
		before, after := commandWALRotationSyncObservers(root, walDir, sealed.target.Token())
		err = sealed.target.Sync(before, after)
	} else {
		err = sealed.target.Sync(nil, nil)
	}
	sealed.earlySynced = err == nil
}

func (sealed *commandWALSealedSync) syncForPublication(root, walDir string) error {
	sealed.mu.Lock()
	defer sealed.mu.Unlock()
	sealed.foreground = true
	if sealed.earlySynced {
		return sealed.target.JournalError()
	}
	return syncCommandWALRotationFile(root, walDir, sealed.target.Token())
}

func syncCommandWALRotationFile(root, walDir string, token *rootpublication.StableResourceToken) error {
	if !durabilitycut.Enabled() {
		return token.SyncThrough()
	}
	before, after := commandWALRotationSyncObservers(root, walDir, token)
	return token.SyncThroughWithFileSyncObserver(before, after)
}

func commandWALRotationSyncObservers(root, walDir string, token *rootpublication.StableResourceToken) (before, after func(*os.File) error) {
	path := filepath.Join(walDir, filepath.Base(token.DiagnosticPath()))
	return func(file *os.File) error {
			return durabilitycut.EmitStablePath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceCommandWAL, root, path, file)
		}, func(file *os.File) error {
			return durabilitycut.EmitStablePath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceCommandWAL, root, path, file)
		}
}
