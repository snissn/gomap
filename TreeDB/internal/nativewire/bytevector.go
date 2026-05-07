package nativewire

const maxInt = int(^uint(0) >> 1)

type ByteVector struct {
	count   int
	offsets []int
	lengths []int
	payload []byte
}

func AppendByteVector(dst []byte, items ...[]byte) []byte {
	dst = appendUvarint(dst, uint64(len(items)))
	for _, item := range items {
		dst = appendUvarint(dst, uint64(len(item)))
	}
	for _, item := range items {
		dst = append(dst, item...)
	}
	return dst
}

func DecodeByteVector(src []byte, limits Limits) (ByteVector, error) {
	limits = limits.withDefaults()
	count64, off, err := readUvarint(src)
	if err != nil {
		return ByteVector{}, err
	}
	if count64 > uint64(limits.MaxByteVectorItems) {
		return ByteVector{}, protocolError(ErrResourceExhausted, "byte-vector count %d exceeds limit %d", count64, limits.MaxByteVectorItems)
	}
	if count64 > uint64(maxInt) {
		return ByteVector{}, protocolError(ErrResourceExhausted, "byte-vector count exceeds int capacity")
	}
	if count64 > uint64(len(src)-off) {
		return ByteVector{}, protocolError(ErrMalformedFrame, "byte-vector count %d exceeds remaining length table bytes %d", count64, len(src)-off)
	}
	count := int(count64)
	offsets := make([]int, count)
	lengths := make([]int, count)

	total := uint64(0)
	for i := 0; i < count; i++ {
		length, n, err := readUvarint(src[off:])
		if err != nil {
			return ByteVector{}, err
		}
		off += n
		if length > limits.MaxByteVectorBytes {
			return ByteVector{}, protocolError(ErrResourceExhausted, "byte-vector item %d length %d exceeds limit %d", i, length, limits.MaxByteVectorBytes)
		}
		if total+length < total {
			return ByteVector{}, protocolError(ErrMalformedFrame, "byte-vector length overflow")
		}
		total += length
		if total > limits.MaxByteVectorBytes {
			return ByteVector{}, protocolError(ErrResourceExhausted, "byte-vector payload length %d exceeds limit %d", total, limits.MaxByteVectorBytes)
		}
		if length > uint64(maxInt) || total > uint64(maxInt) {
			return ByteVector{}, protocolError(ErrResourceExhausted, "byte-vector payload length exceeds int capacity")
		}
		lengths[i] = int(length)
	}

	remaining := len(src) - off
	if total != uint64(remaining) {
		return ByteVector{}, protocolError(ErrMalformedFrame, "byte-vector declared payload %d does not match remaining %d", total, remaining)
	}

	next := 0
	for i, length := range lengths {
		offsets[i] = next
		next += length
	}
	return ByteVector{
		count:   count,
		offsets: offsets,
		lengths: lengths,
		payload: src[off:],
	}, nil
}

func (v ByteVector) Len() int {
	return v.count
}

func (v ByteVector) Item(i int) ([]byte, bool) {
	if i < 0 || i >= v.count {
		return nil, false
	}
	start := v.offsets[i]
	end := start + v.lengths[i]
	return v.payload[start:end], true
}
