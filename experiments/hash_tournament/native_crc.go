//go:build nativecrc && cgo

package hashtournament

/*
#cgo LDFLAGS: -lz
#include <zlib.h>
*/
import "C"

import (
	"unsafe"

	libdeflate "github.com/4kills/go-libdeflate/v2"
)

func zlibCRC32(data []byte) uint32 {
	if len(data) == 0 {
		return uint32(C.crc32(0, nil, 0))
	}
	return uint32(C.crc32_z(0, (*C.Bytef)(unsafe.Pointer(&data[0])), C.z_size_t(len(data))))
}

func libdeflateCRC32(data []byte) uint32 {
	return libdeflate.Crc32(0, data)
}
