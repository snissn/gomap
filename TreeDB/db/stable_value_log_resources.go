package db

import (
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// ValueLogStableRecoveryValidator is the recovery-side declaration for the
// value-log inventory row.
type ValueLogStableRecoveryValidator interface {
	ValidateValueLogStableResource(rootpublication.StableResourceToken) error
}

// ValueLogStableDeletionOwner is the deleting-owner declaration consulted by
// GC and rewrite retirement.
type ValueLogStableDeletionOwner interface {
	ValueLogStableResourcePinned(fileID uint32) bool
}

var _ ValueLogStableRecoveryValidator = (*DB)(nil)
var _ ValueLogStableDeletionOwner = (*DB)(nil)

// stableValueLogResourcePin prevents GC/rewrite retirement from reclaiming a
// captured segment until resource debt reaches a releasing terminal state.
type stableValueLogResourcePin struct {
	db     *DB
	fileID uint32
	once   sync.Once
}

func (db *DB) pinStableValueLogResource(fileID uint32) *stableValueLogResourcePin {
	pin := &stableValueLogResourcePin{db: db, fileID: fileID}
	if db == nil || fileID == 0 {
		return pin
	}
	db.stableValueLogPinMu.Lock()
	if db.stableValueLogPinRefs == nil {
		db.stableValueLogPinRefs = make(map[uint32]int)
	}
	db.stableValueLogPinRefs[fileID]++
	db.stableValueLogPinMu.Unlock()
	return pin
}

func (p *stableValueLogResourcePin) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.db == nil || p.fileID == 0 {
			return
		}
		p.db.stableValueLogPinMu.Lock()
		refs := p.db.stableValueLogPinRefs[p.fileID]
		if refs <= 1 {
			delete(p.db.stableValueLogPinRefs, p.fileID)
		} else {
			p.db.stableValueLogPinRefs[p.fileID] = refs - 1
		}
		if len(p.db.stableValueLogPinRefs) == 0 {
			p.db.stableValueLogPinRefs = nil
		}
		p.db.stableValueLogPinMu.Unlock()
	})
}

func (db *DB) stableValueLogPinnedFileIDs() map[uint32]struct{} {
	if db == nil {
		return nil
	}
	db.stableValueLogPinMu.Lock()
	defer db.stableValueLogPinMu.Unlock()
	if len(db.stableValueLogPinRefs) == 0 {
		return nil
	}
	ids := make(map[uint32]struct{}, len(db.stableValueLogPinRefs))
	for fileID := range db.stableValueLogPinRefs {
		ids[fileID] = struct{}{}
	}
	return ids
}

func (db *DB) ValueLogStableResourcePinned(fileID uint32) bool {
	if db == nil || fileID == 0 {
		return false
	}
	db.stableValueLogPinMu.Lock()
	pinned := db.stableValueLogPinRefs[fileID] > 0
	db.stableValueLogPinMu.Unlock()
	return pinned
}

// ValidateValueLogStableResource validates logical identity and frontier
// against the manager's already-open segment generation. DiagnosticPath is
// deliberately not consulted.
func (db *DB) ValidateValueLogStableResource(token rootpublication.StableResourceToken) error {
	if db == nil || db.valueLogManager == nil {
		return fmt.Errorf("value-log stable resource validator unavailable")
	}
	if token.Kind != rootpublication.StableResourceValueLog || token.Namespace != "value_vlog" || !token.MutableAppend {
		return fmt.Errorf("invalid value-log stable resource kind=%q namespace=%q mutable=%t", token.Kind, token.Namespace, token.MutableAppend)
	}
	fileID := uint32(token.Generation)
	if fileID == 0 || uint64(fileID) != token.Generation || token.Identity != fmt.Sprintf("value-log:%08x", fileID) || token.Frontier == 0 {
		return fmt.Errorf("invalid value-log stable resource identity=%q generation=%d frontier=%d", token.Identity, token.Generation, token.Frontier)
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		return fmt.Errorf("value-log stable resource %q is not registered", token.Identity)
	}
	defer func() { _ = db.valueLogManager.Release(set) }()
	file := set.Files[fileID]
	if file == nil {
		return fmt.Errorf("value-log stable resource %q is not registered", token.Identity)
	}
	if size := file.SizeBestEffort(); size < int64(token.Frontier) {
		return fmt.Errorf("value-log stable resource %q frontier %d exceeds length %d", token.Identity, token.Frontier, size)
	}
	return nil
}
