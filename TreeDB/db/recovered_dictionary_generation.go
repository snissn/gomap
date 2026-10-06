package db

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// AcquireDictionaryIndexGenerationLease fences the currently owned exact index
// against namespace replacement while recovering a parent's canonical dictdb
// dependency. The caller owns the backend through the lease lifetime. This
// acquires no snapshot/read state and performs no checkpoint, lookup or publish.
// On success the caller owns release until transferring it to one exact pinned
// token family. Release is idempotent, counter-only and safe after backend Close.
func (db *DB) AcquireDictionaryIndexGenerationLease(entry rootpublication.DependencyManifestEntryV1) (func(), error) {
	if err := rootpublication.ValidateDictionaryIndexGenerationEntryV1(entry); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, rootpublication.ErrUnresolvedResource
	}
	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if db.closing.Load() {
		return nil, ErrClosed
	}
	generation := db.idx.Load()
	if generation == nil || generation.pager == nil {
		return nil, rootpublication.ErrUnresolvedResource
	}
	parent, err := os.Open(db.dir)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	err = generation.pager.WithStableResourceFile(func(file *os.File) error {
		identity, err := rootpublication.StableIdentityFromFile(file)
		if err != nil {
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, entry.Identity) {
			return fmt.Errorf("%w: dictionary owner index identity differs from manifest", rootpublication.ErrResourceConflict)
		}
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if info.Size() < 0 || uint64(info.Size()) < entry.Frontier.Bytes {
			return rootpublication.ErrFrontierBeyondResource
		}
		namespace, err := rootpublication.NewRecoveredStableNamespaceToken(rootpublication.StableNamespaceSpec{
			Parent: parent, LinkedResource: file, ParentGeneration: entry.Namespace.ParentIdentity.Generation,
			Operation: entry.Namespace.Operation, OldName: entry.Namespace.OldName, NewName: entry.Namespace.NewName,
			DiagnosticPath: entry.Namespace.DiagnosticPath,
		}, entry.Namespace.ParentIdentity)
		if err != nil {
			return err
		}
		namespace.Release()
		return nil
	})
	if err != nil {
		return nil, err
	}
	counter := &db.stableIndexCaptures
	counter.Add(1)
	var released atomic.Bool
	return func() {
		if released.CompareAndSwap(false, true) {
			counter.Add(-1)
		}
	}, nil
}
