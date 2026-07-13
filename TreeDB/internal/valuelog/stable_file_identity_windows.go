//go:build windows

package valuelog

import (
	"encoding/binary"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const stableFileIdentityPlatformWindows = 2

type stableWindowsFileIDInfo struct {
	volumeSerialNumber uint64
	fileID             [16]byte
}

func captureStableFileIdentity(f *os.File) (StableFileIdentity, error) {
	var info stableWindowsFileIDInfo
	if err := windows.GetFileInformationByHandleEx(
		windows.Handle(f.Fd()),
		windows.FileIdInfo,
		(*byte)(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		return StableFileIdentity{}, err
	}
	return StableFileIdentity{
		platform: stableFileIdentityPlatformWindows,
		volume:   info.volumeSerialNumber,
		fileHigh: binary.LittleEndian.Uint64(info.fileID[8:]),
		fileLow:  binary.LittleEndian.Uint64(info.fileID[:8]),
	}, nil
}
