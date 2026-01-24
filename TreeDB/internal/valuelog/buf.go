package valuelog

func grow(dst []byte, n int) []byte {
	if n <= 0 {
		return dst
	}
	oldLen := len(dst)
	newLen := oldLen + n
	if newLen < 0 {
		return dst[:0]
	}
	if cap(dst) < newLen {
		newCap := cap(dst) * 2
		if newCap < newLen {
			newCap = newLen
		}
		tmp := make([]byte, oldLen, newCap)
		copy(tmp, dst)
		dst = tmp
	}
	return dst[:newLen]
}
