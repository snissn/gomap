package commitlog

import (
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

// SealedCommandWALSync borrows the pinned predecessor of a relaxed ext4
// streamed rotation. Its debt owner keeps Token alive and ensures that no
// other handle can sync this predecessor's open-file description concurrently.
type SealedCommandWALSync struct {
	token *rootpublication.StableResourceToken
	owner *journalFileSync
}

func (s *SealedCommandWALSync) Token() *rootpublication.StableResourceToken { return s.token }

func (s *SealedCommandWALSync) JournalError() error { return s.owner.err() }

// Sync verifies the exact original extent and syncs it separately from active
// WAL I/O. Validation and observer errors leave the original sync path
// available. A physical error remains sticky on the shared journal owner.
func (s *SealedCommandWALSync) Sync(before, after func(*os.File) error) error {
	return s.token.WithPinnedFile(func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 || uint64(info.Size()) != s.token.Frontier().Bytes {
			return fmt.Errorf("%w: sealed command WAL extent changed", rootpublication.ErrResourceConflict)
		}
		identity, err := rootpublication.StableIdentityFromFile(file)
		if err != nil {
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, s.token.Identity()) {
			return fmt.Errorf("%w: sealed command WAL identity changed", rootpublication.ErrResourceConflict)
		}
		if before != nil {
			if err := before(file); err != nil {
				return err
			}
		}
		if err := s.owner.syncDistinctFile(func() error { return stableio.SyncFile(file) }); err != nil {
			return err
		}
		if after != nil {
			return after(file)
		}
		return nil
	})
}
