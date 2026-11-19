//go:build linux

package commitlog

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	generationStreamReserveMinEOF = 64 << 20
	generationStreamReserveWindow = 8 << 20
)

// Owned by the retained open file, including after rotation. Its mutex is
// independent of append, rotation and the journal's mandatory file sync.
type generationStreamReserve struct {
	mu       sync.Mutex
	block    int64
	end      int64
	disabled bool
}

// SyncGenerationWAL first completes the original sync. Reservation relies on
// that demonstrated logical prefix and leaves the captured byte and LSN
// frontier unchanged. Foreground appends can proceed on the same inode and
// rotation can retire it while the exact descriptor remains pinned.
func (target *CapturedSync) SyncGenerationWAL() error {
	return target.syncGenerationWAL(reserveStreamTail)
}

func (target *CapturedSync) syncGenerationWAL(reserve func(*os.File, int64, int64) error) error {
	if err := target.Sync(); err != nil {
		return err
	}
	active := target.active
	ceiling := target.journal.segmentTargetBytes
	if !active.preferStream || active.dataOnlySync || ceiling <= generationStreamReserveMinEOF ||
		target.Bytes < generationStreamReserveMinEOF || target.Bytes >= uint64(ceiling) {
		return nil
	}
	state := &active.streamReserve
	state.mu.Lock()
	defer state.mu.Unlock()
	eof := int64(target.Bytes)
	if state.disabled || state.end-eof >= generationStreamReserveWindow/2 {
		return nil
	}
	err := active.token.WithPinnedFile(func(file *os.File) error {
		if state.block == 0 {
			var stat unix.Statfs_t
			if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil ||
				stat.Bsize <= 0 || !walIsExt4Mount(file, uint64(uint32(stat.Type))) {
				state.disabled = true
				return nil
			}
			state.block = int64(stat.Bsize)
		}
		// Skip the last partly written block and never request a range past the
		// configured target. Previously reserved blocks need no new syscall.
		through := eof + min(ceiling-eof, generationStreamReserveWindow)
		through -= through % state.block
		if through <= eof {
			return nil
		}
		start := eof - eof%state.block
		if start < eof {
			start += state.block
		}
		start = max(start, state.end)
		if start >= through {
			return nil
		}
		if err := reserve(file, start, through-start); err != nil {
			if errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
				// Ext4 can leave partial allocation after a capacity failure.
				// Stop retrying this inode and keep the possible bounded tail.
				state.disabled = true
				return nil
			}
			return err
		}
		state.end = through
		return nil
	})
	if err == nil {
		return nil
	}
	// Latch unexpected failures for other callers and checked Close through
	// the existing journal fail-stop. The allocation itself held no sync lock.
	return target.journal.fileSync.run(func() error {
		return fmt.Errorf("commitlog: reserve captured stream WAL tail: %w", err)
	})
}

func reserveStreamTail(file *os.File, offset, length int64) error {
	for {
		err := unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_KEEP_SIZE, offset, length)
		if err != unix.EINTR {
			return err
		}
	}
}
