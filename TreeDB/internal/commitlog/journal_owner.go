package commitlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/lockfile"
)

type JournalOwner struct {
	lock *lockfile.Lock
}

func AcquireJournalOwner(dir string) (*JournalOwner, error) {
	if dir == "" {
		return nil, errors.New("commitlog: journal owner dir required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockfile.Acquire(filepath.Join(dir, "command-wal-journal-owner.lock"))
	if err != nil {
		if errors.Is(err, lockfile.ErrLocked) {
			return nil, fmt.Errorf("%w: %v", ErrJournalOwnerExists, err)
		}
		return nil, err
	}
	return &JournalOwner{lock: lock}, nil
}

func (o *JournalOwner) Close() error {
	if o == nil || o.lock == nil {
		return nil
	}
	lock := o.lock
	o.lock = nil
	return lock.Close()
}
