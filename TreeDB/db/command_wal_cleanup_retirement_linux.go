//go:build linux

package db

import (
	"os"
	"runtime"

	"github.com/snissn/gomap/TreeDB/pager"
	"golang.org/x/sys/unix"
)

// The dominant sealed-file unlinks measured on the dedicated ext4 WAL were
// 149-268 MB. Leave smaller siblings on the ordinary cleanup path.
const minCommandWALRetirementAdviceBytes = 128 << 20

// adviseCommandWALRetirementPages runs only after successful deferred stream
// validation, outside durable publication and journal exclusion. Both durable
// roots cover each selected sealed file. The hint targets the original scan
// reader, and gives no authority to unlink or to skip any durability barrier.
func (db *DB) adviseCommandWALRetirementPages(proof durableWALCleanupProofV1) {
	var wal unix.Statx_t
	checkedLayout := false
	for i := range proof.segments {
		decision := &proof.segments[i]
		if decision.file == nil || !decision.Covered || decision.Active || decision.Size < minCommandWALRetirementAdviceBytes ||
			decision.Size != decision.ScannedBytes || decision.Size != decision.LogicalSize ||
			proof.journalOwner.CleanupResourcePinned(decision.identity) {
			continue
		}
		if !checkedLayout {
			if !db.commandWALRetirementVolume(proof.journal.ActiveIdentity.VolumeID, &wal) {
				return
			}
			checkedLayout = true
		}
		fd := int(decision.file.Fd())
		var actual, linked unix.Statx_t
		if !statCommandWALRetirementPath(fd, "", unix.AT_EMPTY_PATH, &actual) ||
			actual.Mnt_id != wal.Mnt_id || retirementStatDevice(actual) != retirementStatDevice(wal) ||
			actual.Mode&unix.S_IFMT != unix.S_IFREG || actual.Nlink != 1 || actual.Size != uint64(decision.Size) ||
			!statCommandWALRetirementPath(unix.AT_FDCWD, decision.Path, unix.AT_SYMLINK_NOFOLLOW, &linked) ||
			linked.Mnt_id != actual.Mnt_id || linked.Ino != actual.Ino || linked.Mode&unix.S_IFMT != unix.S_IFREG {
			continue
		}
		if hooks := db.testCommandWALCleanupHooks; hooks != nil && hooks.advisePages != nil {
			hooks.advisePages(decision.file, func() error {
				return unix.Fadvise(fd, 0, decision.Size, unix.FADV_DONTNEED)
			})
		} else {
			// A refused or ineffective cache hint leaves cleanup on its usual path.
			_ = unix.Fadvise(fd, 0, decision.Size, unix.FADV_DONTNEED)
		}
		runtime.KeepAlive(decision.file)
	}
}

// Use the live pager and existing directory paths to identify the dedicated
// WAL volume without opening any more descriptors. The active writer's
// ext4 selection was bound to its actual file and exact mount type.
func (db *DB) commandWALRetirementVolume(walDevice uint64, wal *unix.Statx_t) bool {
	layout := resolveStorageLayout(db.dir)
	if !statCommandWALRetirementPath(unix.AT_FDCWD, layout.walDir, unix.AT_SYMLINK_NOFOLLOW, wal) ||
		wal.Mode&unix.S_IFMT != unix.S_IFDIR || retirementStatDevice(*wal) != walDevice {
		return false
	}
	idx := db.idx.Load()
	if idx == nil {
		return false
	}
	dataDevice, ok := commandWALRetirementPagerDevice(idx.pager)
	if !ok || dataDevice == walDevice {
		return false
	}
	if idx.primary != nil {
		primaryDevice, ok := commandWALRetirementPagerDevice(idx.primary.Pager())
		if !ok || primaryDevice != dataDevice {
			return false
		}
	}
	for _, path := range [...]string{layout.rootDir, layout.valueVLogDir, layout.leafVLogDir, layout.columnAssetDir} {
		var dir unix.Statx_t
		if !statCommandWALRetirementPath(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, &dir) ||
			dir.Mode&unix.S_IFMT != unix.S_IFDIR || retirementStatDevice(dir) != dataDevice {
			return false
		}
	}
	return true
}

func commandWALRetirementPagerDevice(p *pager.Pager) (uint64, bool) {
	var stat unix.Stat_t
	err := p.WithStableResourceFile(func(file *os.File) error {
		return unix.Fstat(int(file.Fd()), &stat)
	})
	return uint64(stat.Dev), err == nil
}

func statCommandWALRetirementPath(dirfd int, path string, flags int, stat *unix.Statx_t) bool {
	const mask = unix.STATX_TYPE | unix.STATX_NLINK | unix.STATX_INO | unix.STATX_SIZE | unix.STATX_MNT_ID
	return unix.Statx(dirfd, path, flags|unix.AT_STATX_DONT_SYNC, mask, stat) == nil && stat.Mask&mask == mask
}

func retirementStatDevice(stat unix.Statx_t) uint64 {
	return unix.Mkdev(stat.Dev_major, stat.Dev_minor)
}
