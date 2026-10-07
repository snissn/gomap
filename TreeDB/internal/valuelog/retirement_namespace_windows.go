//go:build windows

package valuelog

import (
	"os"
	"runtime"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"golang.org/x/sys/windows"
)

// The native layouts and ordinary rename fallback mirror Go's
// internal/syscall/windows/at_windows.go. These operations provide visibility,
// not the certified namespace-persistence contract in rootpublication.
type retirementWindowsNameInformationEx struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [260]uint16
}

type retirementWindowsNameInformation struct {
	ReplaceIfExists bool
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [260]uint16
}

func retirementWindowsError(err error) error {
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}

func retirementWindowsOpenChild(parent *os.File, name string, access, options uint32) (*os.File, error) {
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return nil, err
	}
	attributes := windows.OBJECT_ATTRIBUTES{
		RootDirectory: windows.Handle(parent.Fd()), ObjectName: objectName,
		Attributes: windows.OBJ_CASE_INSENSITIVE,
	}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(&handle, access|windows.SYNCHRONIZE|windows.FILE_READ_ATTRIBUTES, &attributes, &status, nil,
		windows.FILE_ATTRIBUTE_NORMAL, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.FILE_OPEN, options|windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_OPEN_REPARSE_POINT, 0, 0)
	runtime.KeepAlive(parent)
	if err != nil {
		return nil, retirementWindowsError(err)
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, os.ErrInvalid
	}
	return file, nil
}

func retirementWindowsNameOperation(source *os.File, old string, destination *os.File, name string, link bool) error {
	defer runtime.KeepAlive(source)
	defer runtime.KeepAlive(destination)
	access := uint32(windows.DELETE)
	if link {
		access = windows.FILE_WRITE_ATTRIBUTES
	}
	file, err := retirementWindowsOpenChild(source, old, access, windows.FILE_NON_DIRECTORY_FILE)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return rootpublication.ErrResourceConflict
	}
	p16, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	var native retirementWindowsNameInformation
	if len(p16) > len(native.FileName) {
		return windows.ERROR_INVALID_NAME
	}
	native.RootDirectory = windows.Handle(destination.Fd())
	native.FileNameLength = uint32((len(p16) - 1) * 2)
	copy(native.FileName[:], p16)
	var status windows.IO_STATUS_BLOCK
	// Native FILE_LINK_INFORMATION is class11. x/sys.FileLinkInformation names
	// class72 (extended information), which has a different Flags contract.
	if link {
		err = windows.NtSetInformationFile(windows.Handle(file.Fd()), &status, (*byte)(unsafe.Pointer(&native)), uint32(unsafe.Sizeof(native)), 11)
	} else {
		extended := retirementWindowsNameInformationEx{
			Flags:         windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS,
			RootDirectory: native.RootDirectory, FileNameLength: native.FileNameLength, FileName: native.FileName,
		}
		err = windows.NtSetInformationFile(windows.Handle(file.Fd()), &status, (*byte)(unsafe.Pointer(&extended)), uint32(unsafe.Sizeof(extended)), 65)
		if err != nil {
			native.ReplaceIfExists = true
			err = windows.NtSetInformationFile(windows.Handle(file.Fd()), &status, (*byte)(unsafe.Pointer(&native)), uint32(unsafe.Sizeof(native)), 10)
		}
	}
	runtime.KeepAlive(file)
	return retirementWindowsError(err)
}

func retirementRenameChild(source *os.File, old string, destination *os.File, name string) error {
	return retirementWindowsNameOperation(source, old, destination, name, false)
}

func retirementLinkChild(source *os.File, old string, destination *os.File, name string) error {
	return retirementWindowsNameOperation(source, old, destination, name, true)
}

func retirementRemoveDirectory(parent, expected *os.File, name string) error {
	defer runtime.KeepAlive(parent)
	defer runtime.KeepAlive(expected)
	file, err := retirementWindowsOpenChild(parent, name, windows.DELETE, windows.FILE_DIRECTORY_FILE)
	if err != nil {
		return err
	}
	defer file.Close()
	got, err := file.Stat()
	if err != nil {
		return err
	}
	want, err := expected.Stat()
	if err != nil {
		return err
	}
	if !got.IsDir() || got.Mode()&os.ModeSymlink != 0 || !os.SameFile(got, want) {
		return rootpublication.ErrResourceConflict
	}
	// Ordinary delete-on-close removes only this exact directory handle, and
	// retains sharing violations; this does not advertise durable removal.
	native := struct{ DeleteFile byte }{1}
	return windows.SetFileInformationByHandle(windows.Handle(file.Fd()), windows.FileDispositionInfo,
		(*byte)(unsafe.Pointer(&native)), uint32(unsafe.Sizeof(native)))
}
