package nativewire

func growBytes(dst []byte, extra int) []byte {
	if extra <= 0 || cap(dst)-len(dst) >= extra {
		return dst
	}
	if extra > maxInt-len(dst) {
		return dst
	}
	next := make([]byte, len(dst), len(dst)+extra)
	copy(next, dst)
	return next
}
