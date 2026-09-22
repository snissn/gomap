package caching

import "unsafe"

// ownedReadResult preserves Get's safe-copy contract while avoiding an extra
// clone when the read path already returned an owned tight slice.
func ownedReadResult(buf []byte, scratch *ownedReadScratch) []byte {
	if len(buf) == 0 {
		return nil
	}
	if scratch == nil || (!ownedReadAliasesScratch(buf, scratch.buf) && cap(buf) == len(buf)) {
		return buf
	}
	owned := make([]byte, len(buf))
	copy(owned, buf)
	return owned
}

func ownedReadAliasesScratch(out, scratchBuf []byte) bool {
	if len(out) == 0 || cap(scratchBuf) == 0 {
		return false
	}
	fullScratch := scratchBuf[:cap(scratchBuf)]
	scratchPtr := uintptr(unsafe.Pointer(unsafe.SliceData(fullScratch)))
	outPtr := uintptr(unsafe.Pointer(unsafe.SliceData(out)))
	if outPtr < scratchPtr {
		return false
	}
	offset := outPtr - scratchPtr
	if offset > uintptr(len(fullScratch)) {
		return false
	}
	return uintptr(len(out)) <= uintptr(len(fullScratch))-offset
}
