package db

import (
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// ValueLogStableRecoveryValidator is the recovery-side declaration for the
// value-log inventory row.
type ValueLogStableRecoveryValidator interface {
	ValidateValueLogStableResource(rootpublication.StableResourceToken) error
}

// ValueLogStableDeletionOwner is the deleting-owner declaration consulted by
// GC and rewrite retirement.
type ValueLogStableDeletionOwner interface {
	ValueLogStableResourcePinned(identity valuelog.StableFileIdentity) bool
}

var _ ValueLogStableRecoveryValidator = (*DB)(nil)
var _ ValueLogStableDeletionOwner = (*DB)(nil)

// stableValueLogResourcePin prevents GC/rewrite retirement from reclaiming a
// captured segment until resource debt reaches a releasing terminal state.
type stableValueLogResourcePin struct {
	db       *DB
	identity valuelog.StableFileIdentity
	once     sync.Once
}

func (db *DB) pinStableValueLogResource(identity valuelog.StableFileIdentity) *stableValueLogResourcePin {
	pin := &stableValueLogResourcePin{db: db, identity: identity}
	if db == nil || identity.Token() == "" {
		return pin
	}
	db.stableValueLogPinMu.Lock()
	if db.stableValueLogPinRefs == nil {
		db.stableValueLogPinRefs = make(map[valuelog.StableFileIdentity]int)
	}
	db.stableValueLogPinRefs[identity]++
	db.stableValueLogPinMu.Unlock()
	return pin
}

func (p *stableValueLogResourcePin) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.db == nil || p.identity.Token() == "" {
			return
		}
		p.db.stableValueLogPinMu.Lock()
		refs := p.db.stableValueLogPinRefs[p.identity]
		if refs <= 1 {
			delete(p.db.stableValueLogPinRefs, p.identity)
		} else {
			p.db.stableValueLogPinRefs[p.identity] = refs - 1
		}
		if len(p.db.stableValueLogPinRefs) == 0 {
			p.db.stableValueLogPinRefs = nil
		}
		p.db.stableValueLogPinMu.Unlock()
	})
}

func (db *DB) stableValueLogPinnedIdentities() map[valuelog.StableFileIdentity]struct{} {
	if db == nil {
		return nil
	}
	db.stableValueLogPinMu.Lock()
	defer db.stableValueLogPinMu.Unlock()
	if len(db.stableValueLogPinRefs) == 0 {
		return nil
	}
	identities := make(map[valuelog.StableFileIdentity]struct{}, len(db.stableValueLogPinRefs))
	for identity := range db.stableValueLogPinRefs {
		identities[identity] = struct{}{}
	}
	return identities
}

func (db *DB) ValueLogStableResourcePinned(identity valuelog.StableFileIdentity) bool {
	if db == nil || identity.Token() == "" {
		return false
	}
	db.stableValueLogPinMu.Lock()
	pinned := db.stableValueLogPinRefs[identity] > 0
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
	if fileID == 0 || uint64(fileID) != token.Generation || token.Identity == "" || token.Frontier == 0 {
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
	identity, err := file.StableFileIdentity()
	if err != nil {
		return err
	}
	if token.Identity != identity.Token() {
		return fmt.Errorf("value-log stable resource identity mismatch token=%q descriptor=%q", token.Identity, identity.Token())
	}
	if size := file.SizeBestEffort(); size < int64(token.Frontier) {
		return fmt.Errorf("value-log stable resource %q frontier %d exceeds length %d", token.Identity, token.Frontier, size)
	}
	return nil
}
