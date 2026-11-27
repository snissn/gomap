//go:build windows

package valuelog

import (
	"errors"
	"os"
	"runtime"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"golang.org/x/sys/windows"
)

// openSegmentReadHandle allows another manager to quarantine a sealed segment
// while this read handle remains open. Identity leases still decide whether the
// rename is permitted; FILE_SHARE_DELETE only removes the Windows pathname
// restriction after that decision has been made.
func openSegmentReadHandle(path string) (*os.File, error) {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(
		pathp,
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: path, Err: errors.New("invalid Windows file handle")}
	}
	return file, nil
}

func segmentFileDeletePending(file *os.File) bool {
	if file == nil {
		return false
	}
	info := struct {
		AllocationSize int64
		EndOfFile      int64
		NumberOfLinks  uint32
		DeletePending  byte
		Directory      byte
		_              [2]byte
	}{}
	err := windows.GetFileInformationByHandleEx(windows.Handle(file.Fd()), windows.FileStandardInfo,
		(*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	runtime.KeepAlive(file)
	return err == nil && info.DeletePending != 0
}

// isPendingDeleteSegmentPath checks whether a path error observed during
// discovery on an already-tracked File reflects Windows DELETE_PENDING /
// mid-unlink teardown rather than a live failure.
func isPendingDeleteSegmentPath(existing *File, _ string, pathErr error, retired bool) bool {
	if errors.Is(pathErr, os.ErrNotExist) {
		return true
	}
	if errors.Is(pathErr, windows.ERROR_ACCESS_DENIED) || isWindowsSharingViolationError(pathErr) {
		return retired || segmentFileDeletePending(existing.File)
	}
	return false
}

// isPendingDeleteOrSameSegmentPath disambiguates a false-negative os.SameFile
// on Windows during discovery. On Windows, os.Stat uses FindFirstFile without
// opening the file, and os.SameFile lazily calls CreateFile to populate file
// IDs; if another manager concurrently unlinked/quarantined the segment or left
// it DELETE_PENDING while existing.File is still open with FILE_SHARE_DELETE,
// CreateFile fails with ENOENT or ERROR_ACCESS_DENIED and Go silently returns
// SameFile == false. Re-opening with FILE_SHARE_DELETE proves whether a genuine
// different replacement file now occupies path (fail closed) versus the same
// file or a pending deletion (benign discovery race).
func isPendingDeleteOrSameSegmentPath(existing *File, path string, retired bool) bool {
	if existing == nil || existing.File == nil {
		return false
	}
	opened, err := openSegmentReadHandle(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true
		}
		if errors.Is(err, windows.ERROR_ACCESS_DENIED) || isWindowsSharingViolationError(err) {
			return retired || segmentFileDeletePending(existing.File)
		}
		return false
	}
	defer func() { _ = opened.Close() }()
	owned, err1 := rootpublication.StableIdentityFromFile(existing.File)
	linked, err2 := rootpublication.StableIdentityFromFile(opened)
	if err1 != nil || err2 != nil {
		return false
	}
	return rootpublication.SamePhysicalIdentity(owned, linked)
}
