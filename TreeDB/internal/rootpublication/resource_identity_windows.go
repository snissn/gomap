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

var stableReOpenFile = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

func openStableAnonymousFile(*os.File, os.FileMode) (*os.File, error) {
	return nil, ErrNamespacePersistenceUnsupported
}

func linkStableChildFileNoReplace(*os.File, string, string) error {
	return ErrNamespacePersistenceUnsupported
}

func stableRelativeNamespaceSupported() bool { return false }

// Windows does not provide the complete create/rename/remove plus parent-sync
// contract advertised by StableRelativeNamespaceSupported. It does provide a
// narrower create-only contract for append-only files: open the child relative
// to the retained parent and flush that exact child. Microsoft documents a file
// flush as persisting file metadata, which is sufficient for a creation debt
// without claiming rename or removal support.
func stableNamespaceCreationPersistsThroughChild() bool { return true }

func openStableParent(path string) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(
		name,
		windows.FILE_LIST_DIRECTORY|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: path, Err: errors.New("invalid Windows directory handle")}
	}
	info, statErr := file.Stat()
	if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		_ = file.Close()
		if statErr != nil {
			return nil, &os.PathError{Op: "open", Path: path, Err: statErr}
		}
		return nil, &os.PathError{Op: "open", Path: path, Err: errors.New("stable parent is not an exact directory")}
	}
	return file, nil
}

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

	access := uint32(windows.FILE_GENERIC_READ | windows.SYNCHRONIZE)
	switch flags & (os.O_WRONLY | os.O_RDWR) {
	case os.O_WRONLY:
		access = windows.FILE_GENERIC_WRITE | windows.SYNCHRONIZE
	case os.O_RDWR:
		access = windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE | windows.SYNCHRONIZE
	}
	if flags&os.O_CREATE != 0 {
		access |= windows.FILE_GENERIC_WRITE
	}
	if flags&os.O_APPEND != 0 && flags&os.O_TRUNC == 0 {
		access &^= windows.FILE_WRITE_DATA
		access |= windows.FILE_APPEND_DATA
	}
	disposition := uint32(windows.FILE_OPEN)
	switch {
	case flags&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL:
		disposition = windows.FILE_CREATE
	case flags&os.O_TRUNC != 0 && flags&os.O_CREATE != 0:
		disposition = windows.FILE_OVERWRITE_IF
	case flags&os.O_TRUNC != 0:
		disposition = windows.FILE_OVERWRITE
	case flags&os.O_CREATE != 0:
		disposition = windows.FILE_OPEN_IF
	}
	options := uint32(windows.FILE_NON_DIRECTORY_FILE | windows.FILE_SYNCHRONOUS_IO_NONALERT | windows.FILE_OPEN_REPARSE_POINT)
	if flags&os.O_SYNC != 0 {
		options |= windows.FILE_WRITE_THROUGH
	}
	var (
		handle windows.Handle
		status windows.IO_STATUS_BLOCK
	)
	err = windows.NtCreateFile(
		&handle,
		access,
		&attributes,
		&status,
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

func openOrCreateStableChildDirectory(parent *os.File, name string, perm os.FileMode) (*os.File, error) {
	if parent == nil {
		return nil, os.ErrInvalid
	}
	_ = perm // Windows applies ACLs inherited from the exact parent namespace.

	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, &os.PathError{Op: "mkdirat", Path: name, Err: err}
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var (
		handle windows.Handle
		status windows.IO_STATUS_BLOCK
	)
	err = windows.NtCreateFile(
		&handle,
		windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE,
		&attributes,
		&status,
		nil,
		windows.FILE_ATTRIBUTE_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN_IF,
		windows.FILE_DIRECTORY_FILE|windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_WRITE_THROUGH|windows.FILE_OPEN_REPARSE_POINT,
		0,
		0,
	)
	runtime.KeepAlive(parent)
	if err != nil {
		return nil, &os.PathError{Op: "mkdirat", Path: name, Err: stableWindowsNTError(err)}
	}
	file := os.NewFile(uintptr(handle), parent.Name()+string(os.PathSeparator)+name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "mkdirat", Path: name, Err: errors.New("invalid Windows directory handle")}
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

func installStableFileHandleNoReplace(*os.File, *os.File, string) (bool, error) {
	return false, fmt.Errorf("%w: exact-handle no-replace install is unavailable", ErrNamespacePersistenceUnsupported)
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

// duplicateStableSyncFile reopens the exact file object with GENERIC_WRITE.
// DuplicateHandle preserves the source access mask, but FlushFileBuffers
// requires GENERIC_WRITE and sealed value-log managers intentionally retain
// read-only handles. ReOpenFile upgrades only the private durability pin; it
// does not broaden the manager's ordinary read handle or re-resolve a path.
func duplicateStableSyncFile(file *os.File) (*os.File, error) {
	if file == nil {
		return nil, os.ErrInvalid
	}
	handle, _, callErr := stableReOpenFile.Call(
		file.Fd(),
		uintptr(windows.GENERIC_READ|windows.GENERIC_WRITE),
		uintptr(windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE),
		0,
	)
	runtime.KeepAlive(file)
	if windows.Handle(handle) == windows.InvalidHandle {
		if callErr != nil && callErr != windows.ERROR_SUCCESS {
			return nil, callErr
		}
		return nil, windows.ERROR_INVALID_HANDLE
	}
	pinned := os.NewFile(handle, file.Name()+"#stable-sync-pin")
	if pinned == nil {
		_ = windows.CloseHandle(windows.Handle(handle))
		return nil, errors.New("invalid Windows stable sync handle")
	}
	return pinned, nil
}

func platformStableIdentityFromFile(file *os.File) (StableIdentity, error) {
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

func syncStableNamespace(handle *os.File) error {
	if handle == nil {
		return os.ErrInvalid
	}
	err := windows.FlushFileBuffers(windows.Handle(handle.Fd()))
	if err == nil {
		return nil
	}
	if err == windows.ERROR_INVALID_HANDLE || err == windows.ERROR_ACCESS_DENIED || err == windows.ERROR_NOT_SUPPORTED {
		return fmt.Errorf("%w: FlushFileBuffers(namespace persistence handle): %v", ErrNamespacePersistenceUnsupported, err)
	}
	return err
}
