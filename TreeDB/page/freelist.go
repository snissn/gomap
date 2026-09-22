package page

import (
	"encoding/binary"
	"errors"
)

var ErrFreelistFull = errors.New("freelist page is full")
var ErrFreelistEmpty = errors.New("freelist page is empty")

// FreelistPageBody represents the body of a Freelist Page.
// Layout:
// NextPageID (8 bytes)
// Count (2 bytes) -- Wait, Header has Count?
// Standard Header has `Count`. We can use that.
// But we need to encode the array.
// NextPageID (8 bytes) + Array[uint64]
type FreelistPageBody struct {
	NextPageID uint64
	FreeIDs    []uint64
}

// MaxFreeIDs per page.
// PageSize (4096) - Header (16) - NextPageID (8) = 4072 bytes.
// Each ID = 8 bytes.
// 4072 / 8 = 509.
const MaxFreeIDs = 509

func (f *FreelistPageBody) Encode(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], f.NextPageID)
	// We assume Header.Count tracks the number of items.
	// But Encode doesn't write Count (it's in Header).
	// We write the array.
	ptr := 8
	for _, id := range f.FreeIDs {
		binary.LittleEndian.PutUint64(buf[ptr:ptr+8], id)
		ptr += 8
	}
}

func DecodeFreelistBody(buf []byte, count uint16) FreelistPageBody {
	f := FreelistPageBody{
		NextPageID: binary.LittleEndian.Uint64(buf[0:8]),
		FreeIDs:    make([]uint64, count),
	}
	ptr := 8
	for i := uint16(0); i < count; i++ {
		f.FreeIDs[i] = binary.LittleEndian.Uint64(buf[ptr : ptr+8])
		ptr += 8
	}
	return f
}
