package gomap

import "unsafe"

func scanSpecial8Scalar(ptr *Hash, target Hash) uint64 {
	hashes := unsafe.Slice(ptr, 8)
	var mask uint64
	for i, h := range hashes {
		if h == 0 || h == HashTombstone || h == target {
			mask |= 1 << uint(i)
		}
	}
	return mask
}
