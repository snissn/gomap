//go:build windows

package rootpublication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/windows"
)

func stableRelativeNamespaceSupported() bool { return false }

func openStableChildFile(parent *os.File, name string, flags int, perm os.FileMode) (*os.File, error) {
	if parent == nil {
		return nil, os.ErrInvalid
	}
	_ = perm // Windows applies ACLs inherited from the exact parent namespace.

	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: name, Err: err}
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))

	access := uint32(windows.FILE_GENERIC_READ)
	switch flags & (os.O_WRONLY | os.O_RDWR) {
	case os.O_WRONLY:
		access = windows.FILE_GENERIC_WRITE
	case os.O_RDWR:
		access = windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE
	}
	if flags&os.O_CREATE != 0 {
		access |= windows.FILE_GENERIC_WRITE
	}
	if flags&os.O_APPEND != 0 {
		if flags&os.O_TRUNC == 0 {
			access &^= windows.FILE_GENERIC_WRITE
		}
		access |= windows.FILE_APPEND_DATA | windows.FILE_WRITE_ATTRIBUTES | windows.FILE_WRITE_EA |
			windows.STANDARD_RIGHTS_WRITE | windows.SYNCHRONIZE
	}
	disposition := uint32(windows.FILE_OPEN)
	switch {
	case flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0:
		disposition = windows.FILE_CREATE
	case flags&os.O_CREATE != 0 && flags&os.O_TRUNC != 0:
		disposition = windows.FILE_OVERWRITE_IF
	case flags&os.O_CREATE != 0:
		disposition = windows.FILE_OPEN_IF
	case flags&os.O_TRUNC != 0:
		disposition = windows.FILE_OVERWRITE
	}
	options := uint32(windows.FILE_NON_DIRECTORY_FILE | windows.FILE_SYNCHRONOUS_IO_NONALERT | windows.FILE_OPEN_REPARSE_POINT)
	if flags&os.O_SYNC != 0 {
		options |= windows.FILE_WRITE_THROUGH
	}
	var (
		handle windows.Handle
		iosb   windows.IO_STATUS_BLOCK
	)
	err = windows.NtCreateFile(
		&handle,
		access,
		&attributes,
		&iosb,
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition,
		options,
		0,
		0,
	)
	runtime.KeepAlive(parent)
	if err != nil {
		return nil, &os.PathError{Op: "openat", Path: name, Err: stableWindowsNTError(err)}
	}
	file := os.NewFile(uintptr(handle), parent.Name()+string(os.PathSeparator)+name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "openat", Path: name, Err: errors.New("invalid Windows file handle")}
	}
	return file, nil
}

func stableWindowsNTError(err error) error {
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}

func removeStableChildFile(*os.File, string) error {
	return fmt.Errorf("%w: relative directory-handle unlink is unavailable", ErrNamespacePersistenceUnsupported)
}

func renameStableChildFile(*os.File, string, string) error {
	return fmt.Errorf("%w: relative directory-handle rename is unavailable", ErrNamespacePersistenceUnsupported)
}

func stableCrossParentMoveNoReplaceSupported() bool { return false }

func moveStableChildFileNoReplace(*os.File, *os.File, string, *os.File, string) (bool, error) {
	return false, fmt.Errorf("%w: cross-parent no-replace move is unavailable", ErrNamespacePersistenceUnsupported)
}

type stableWindowsFileIDInfo struct {
	VolumeSerialNumber uint64
	FileID             [16]byte
}

func duplicateStableFile(file *os.File) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	process, err := windows.GetCurrentProcess()
	if err != nil {
		return nil, err
	}
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(process, windows.Handle(file.Fd()), process, &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(duplicate), file.Name()+"#stable-pin"), nil
}

func stableIdentityFromFile(file *os.File) (StableIdentity, error) {
	if file == nil {
		return StableIdentity{}, os.ErrInvalid
	}
	handle := windows.Handle(file.Fd())
	var native stableWindowsFileIDInfo
	err := windows.GetFileInformationByHandleEx(handle, windows.FileIdInfo, (*byte)(unsafe.Pointer(&native)), uint32(unsafe.Sizeof(native)))
	if err == nil {
		return StableIdentity{Platform: "windows", VolumeID: native.VolumeSerialNumber, ObjectID: native.FileID}, nil
	}
	var fallback windows.ByHandleFileInformation
	if fallbackErr := windows.GetFileInformationByHandle(handle, &fallback); fallbackErr != nil {
		return StableIdentity{}, errorsJoinStableWindows(err, fallbackErr)
	}
	var objectID [16]byte
	binary.LittleEndian.PutUint32(objectID[:4], fallback.VolumeSerialNumber)
	binary.LittleEndian.PutUint32(objectID[8:12], fallback.FileIndexHigh)
	binary.LittleEndian.PutUint32(objectID[12:], fallback.FileIndexLow)
	return StableIdentity{Platform: "windows", VolumeID: uint64(fallback.VolumeSerialNumber), ObjectID: objectID}, nil
}

func errorsJoinStableWindows(primary, fallback error) error {
	return fmt.Errorf("stable file identity unavailable: FILE_ID_INFO=%v BY_HANDLE_FILE_INFORMATION=%w", primary, fallback)
}

func syncStableNamespace(parent *os.File) error {
	if parent == nil {
		return os.ErrInvalid
	}
	// os.Open directory handles do not carry GENERIC_WRITE, which
	// FlushFileBuffers requires. ReOpenFile retains the exact file-system object
	// while adding the access needed to issue the namespace flush; reopening the
	// diagnostic pathname would lose the rename/recreate identity guarantee.
	handle, err := reopenStableWindowsDirectory(parent)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	err = windows.FlushFileBuffers(handle)
	if err == nil {
		return nil
	}
	if err == windows.ERROR_INVALID_HANDLE || err == windows.ERROR_ACCESS_DENIED || err == windows.ERROR_NOT_SUPPORTED {
		return fmt.Errorf("%w: FlushFileBuffers(directory): %v", ErrNamespacePersistenceUnsupported, err)
	}
	return err
}

func syncStableFile(file *os.File) error {
	if file == nil {
		return os.ErrInvalid
	}
	// Append-only handles intentionally carry FILE_APPEND_DATA instead of
	// FILE_WRITE_DATA so writes cannot overwrite an earlier frontier. Windows
	// nevertheless requires GENERIC_WRITE for FlushFileBuffers. ReOpenFile adds
	// that access on the exact file-system object retained by the pin; reopening
	// the diagnostic pathname would lose the rename/recreate identity guarantee.
	handle, err := reopenStableWindowsHandle(file, windows.FILE_FLAG_WRITE_THROUGH, "stable file")
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	return windows.FlushFileBuffers(handle)
}

var stableWindowsReOpenFile = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

func reopenStableWindowsDirectory(parent *os.File) (windows.Handle, error) {
	return reopenStableWindowsHandle(parent, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_WRITE_THROUGH, "namespace parent")
}

func reopenStableWindowsHandle(file *os.File, flags uint32, description string) (windows.Handle, error) {
	if file == nil {
		return 0, os.ErrInvalid
	}
	handle, _, callErr := stableWindowsReOpenFile.Call(
		file.Fd(),
		uintptr(windows.GENERIC_WRITE),
		uintptr(windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE),
		uintptr(flags),
	)
	runtime.KeepAlive(file)
	if handle == ^uintptr(0) {
		if callErr == nil {
			callErr = windows.ERROR_INVALID_HANDLE
		}
		return 0, fmt.Errorf("reopen exact %s: %w", description, callErr)
	}
	return windows.Handle(handle), nil
}
