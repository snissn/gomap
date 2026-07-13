//go:build windows

package rootpublication

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var stableReOpenFile = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

type stableWindowsFileRenameInformation struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [1]uint16
}

func stableWindowsError(err error) error {
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}

func stableWindowsOpenRelative(parent *os.File, name string, access, disposition, options uint32) (windows.Handle, error) {
	if parent == nil {
		return windows.InvalidHandle, os.ErrInvalid
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return windows.InvalidHandle, err
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(parent.Fd()),
		ObjectName:    objectName,
		Attributes:    windows.OBJ_CASE_INSENSITIVE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var status windows.IO_STATUS_BLOCK
	var allocationSize int64
	var handle windows.Handle
	err = windows.NtCreateFile(
		&handle,
		access,
		&attributes,
		&status,
		&allocationSize,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition,
		options,
		0,
		0,
	)
	if err != nil {
		return windows.InvalidHandle, stableWindowsError(err)
	}
	return handle, nil
}

func stableWindowsOpenParameters(flags int) (access, disposition, options uint32) {
	access = windows.SYNCHRONIZE | windows.FILE_READ_ATTRIBUTES
	switch flags & (os.O_WRONLY | os.O_RDWR) {
	case os.O_WRONLY:
		access |= windows.FILE_GENERIC_WRITE
	case os.O_RDWR:
		access |= windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE
	default:
		access |= windows.FILE_GENERIC_READ
	}
	if flags&os.O_CREATE != 0 {
		access |= windows.FILE_GENERIC_WRITE
	}
	if flags&os.O_APPEND != 0 && flags&os.O_TRUNC == 0 {
		access &^= windows.FILE_WRITE_DATA
		access |= windows.FILE_APPEND_DATA
	}
	switch {
	case flags&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL:
		disposition = windows.FILE_CREATE
	case flags&os.O_TRUNC != 0 && flags&os.O_CREATE != 0:
		disposition = windows.FILE_OVERWRITE_IF
	case flags&os.O_TRUNC != 0:
		disposition = windows.FILE_OVERWRITE
	case flags&os.O_CREATE != 0:
		disposition = windows.FILE_OPEN_IF
	default:
		disposition = windows.FILE_OPEN
	}
	options = windows.FILE_NON_DIRECTORY_FILE | windows.FILE_SYNCHRONOUS_IO_NONALERT
	if flags&os.O_SYNC != 0 {
		options |= windows.FILE_WRITE_THROUGH
	}
	return access, disposition, options
}

func openStableChildFile(parent *os.File, name string, flags int, _ os.FileMode) (*os.File, error) {
	access, disposition, options := stableWindowsOpenParameters(flags)
	handle, err := stableWindowsOpenRelative(parent, name, access, disposition, options)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), parent.Name()+string(os.PathSeparator)+name), nil
}

func removeStableChildFile(parent *os.File, name string) error {
	handle, err := stableWindowsOpenRelative(
		parent,
		name,
		windows.DELETE|windows.SYNCHRONIZE|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_SYNCHRONOUS_IO_NONALERT,
	)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer windows.CloseHandle(handle)

	var status windows.IO_STATUS_BLOCK
	flags := uint32(windows.FILE_DISPOSITION_DELETE | windows.FILE_DISPOSITION_POSIX_SEMANTICS | windows.FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE)
	err = windows.NtSetInformationFile(handle, &status, (*byte)(unsafe.Pointer(&flags)), uint32(unsafe.Sizeof(flags)), windows.FileDispositionInformationEx)
	if err == windows.STATUS_INVALID_INFO_CLASS || err == windows.STATUS_INVALID_PARAMETER || err == windows.STATUS_NOT_SUPPORTED {
		deleteFile := byte(1)
		err = windows.NtSetInformationFile(handle, &status, &deleteFile, 1, windows.FileDispositionInformation)
	}
	return stableWindowsError(err)
}

func renameStableChildFile(parent *os.File, oldName, newName string) error {
	handle, err := stableWindowsOpenRelative(
		parent,
		oldName,
		windows.DELETE|windows.SYNCHRONIZE|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_OPEN,
		windows.FILE_NON_DIRECTORY_FILE|windows.FILE_SYNCHRONOUS_IO_NONALERT,
	)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)

	newNameUTF16, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	fileNameBytes := (len(newNameUTF16) - 1) * 2
	var layout stableWindowsFileRenameInformation
	bufferSize := int(unsafe.Offsetof(layout.FileName)) + fileNameBytes
	buffer := make([]byte, bufferSize)
	info := (*stableWindowsFileRenameInformation)(unsafe.Pointer(&buffer[0]))
	info.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	info.RootDirectory = windows.Handle(parent.Fd())
	info.FileNameLength = uint32(fileNameBytes)
	copy(unsafe.Slice(&info.FileName[0], fileNameBytes/2), newNameUTF16[:len(newNameUTF16)-1])

	var status windows.IO_STATUS_BLOCK
	err = windows.NtSetInformationFile(handle, &status, &buffer[0], uint32(bufferSize), windows.FileRenameInformation)
	if err == windows.STATUS_INVALID_PARAMETER || err == windows.STATUS_NOT_SUPPORTED {
		info.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS
		err = windows.NtSetInformationFile(handle, &status, &buffer[0], uint32(bufferSize), windows.FileRenameInformation)
	}
	return stableWindowsError(err)
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
	reopened, _, callErr := stableReOpenFile.Call(
		parent.Fd(),
		uintptr(windows.GENERIC_WRITE),
		uintptr(windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE),
		uintptr(windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_WRITE_THROUGH),
	)
	if windows.Handle(reopened) == windows.InvalidHandle {
		if callErr == nil || callErr == syscall.Errno(0) {
			callErr = windows.ERROR_INVALID_HANDLE
		}
		return fmt.Errorf("%w: ReOpenFile(directory): %v", ErrNamespacePersistenceUnsupported, callErr)
	}
	handle := windows.Handle(reopened)
	defer windows.CloseHandle(handle)
	err := windows.FlushFileBuffers(handle)
	if err == nil {
		return nil
	}
	if err == windows.ERROR_INVALID_HANDLE || err == windows.ERROR_ACCESS_DENIED || err == windows.ERROR_NOT_SUPPORTED {
		return fmt.Errorf("%w: FlushFileBuffers(directory): %v", ErrNamespacePersistenceUnsupported, err)
	}
	return err
}
