//go:build windows

package rootpublication

import (
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func stableRelativeNamespaceSupported() bool { return false }

func openStableChildFile(*os.File, string, int, os.FileMode) (*os.File, error) {
	return nil, fmt.Errorf("%w: relative directory-handle open is unavailable", ErrNamespacePersistenceUnsupported)
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

func syncStableNamespace(parent *os.File) error {
	if parent == nil {
		return os.ErrInvalid
	}
	err := windows.FlushFileBuffers(windows.Handle(parent.Fd()))
	if err == nil {
		return nil
	}
	if err == windows.ERROR_INVALID_HANDLE || err == windows.ERROR_ACCESS_DENIED || err == windows.ERROR_NOT_SUPPORTED {
		return fmt.Errorf("%w: FlushFileBuffers(directory): %v", ErrNamespacePersistenceUnsupported, err)
	}
	return err
}
