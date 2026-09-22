package nativewire

type ByteVector struct {
	count   int
	offsets []int
	lengths []int
	payload []byte
}

// ByteVectorScratch carries reusable decode buffers for DecodeByteVectorInto.
type ByteVectorScratch struct {
	offsets []int
	lengths []int
}

func AppendByteVector(dst []byte, items ...[]byte) []byte {
	return AppendByteVectorWithEncodedLen(dst, ByteVectorEncodedLen(items), items...)
}

func AppendByteVectorWithEncodedLen(dst []byte, encodedLen int, items ...[]byte) []byte {
	dst = growBytes(dst, encodedLen)
	dst = appendUvarint(dst, uint64(len(items)))
	for _, item := range items {
		dst = appendUvarint(dst, uint64(len(item)))
	}
	for _, item := range items {
		dst = append(dst, item...)
	}
	return dst
}

func ByteVectorEncodedLen(items [][]byte) int {
	n := uvarintLen(uint64(len(items)))
	for _, item := range items {
		add := uvarintLen(uint64(len(item))) + len(item)
		if add > maxInt-n {
			return maxInt
		}
		n += add
	}
	return n
}

func ByteVectorPayloadEncodedLen(lengths []int, payloadLen int) (int, error) {
	if payloadLen < 0 {
		return 0, protocolError(ErrMalformedFrame, "negative byte-vector payload length")
	}
	n := uvarintLen(uint64(len(lengths)))
	total := 0
	for i, length := range lengths {
		if length < 0 {
			return 0, protocolError(ErrMalformedFrame, "negative byte-vector item length at index %d", i)
		}
		if length > maxInt-total {
			return 0, protocolError(ErrResourceExhausted, "byte-vector payload length exceeds int capacity")
		}
		total += length
		if n > maxInt-uvarintLen(uint64(length)) {
			return 0, protocolError(ErrResourceExhausted, "byte-vector length table exceeds int capacity")
		}
		n += uvarintLen(uint64(length))
	}
	if total != payloadLen {
		return 0, protocolError(ErrMalformedFrame, "byte-vector payload length %d does not match declared lengths %d", payloadLen, total)
	}
	if payloadLen > maxInt-n {
		return 0, protocolError(ErrResourceExhausted, "byte-vector encoded length exceeds int capacity")
	}
	return n + payloadLen, nil
}

func AppendByteVectorPayload(dst []byte, lengths []int, payload []byte) ([]byte, error) {
	encodedLen, err := ByteVectorPayloadEncodedLen(lengths, len(payload))
	if err != nil {
		return nil, err
	}
	dst = growBytes(dst, encodedLen)
	dst = appendUvarint(dst, uint64(len(lengths)))
	for _, length := range lengths {
		dst = appendUvarint(dst, uint64(length))
	}
	dst = append(dst, payload...)
	return dst, nil
}

func DecodeByteVector(src []byte, limits Limits) (ByteVector, error) {
	return DecodeByteVectorInto(src, limits, nil)
}

// DecodeByteVectorItems decodes a byte-vector into item slices that borrow from
// src. It avoids offset/length table allocation for callers that only need a
// transient [][]byte view.
func DecodeByteVectorItems(src []byte, limits Limits) ([][]byte, error) {
	return DecodeByteVectorItemsInto(nil, src, limits)
}

// DecodeByteVectorItemsInto decodes a byte-vector into dst[:count] item slices
// that borrow from src. It avoids offset/length table allocation for callers
// that only need a transient [][]byte view.
func DecodeByteVectorItemsInto(dst [][]byte, src []byte, limits Limits) ([][]byte, error) {
	limits = limits.withDefaults()
	count64, lengthsOff, err := readUvarint(src)
	if err != nil {
		return nil, err
	}
	if count64 > uint64(limits.MaxByteVectorItems) {
		return nil, protocolError(ErrResourceExhausted, "byte-vector count %d exceeds limit %d", count64, limits.MaxByteVectorItems)
	}
	if count64 > uint64(maxInt) {
		return nil, protocolError(ErrResourceExhausted, "byte-vector count exceeds int capacity")
	}
	count := int(count64)

	off := lengthsOff
	total := uint64(0)
	for i := 0; i < count; i++ {
		length, n, err := readUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		if length > limits.MaxByteVectorBytes {
			return nil, protocolError(ErrResourceExhausted, "byte-vector item %d length %d exceeds limit %d", i, length, limits.MaxByteVectorBytes)
		}
		if total+length < total {
			return nil, protocolError(ErrMalformedFrame, "byte-vector length overflow")
		}
		total += length
		if total > limits.MaxByteVectorBytes {
			return nil, protocolError(ErrResourceExhausted, "byte-vector payload length %d exceeds limit %d", total, limits.MaxByteVectorBytes)
		}
		if length > uint64(maxInt) || total > uint64(maxInt) {
			return nil, protocolError(ErrResourceExhausted, "byte-vector payload length exceeds int capacity")
		}
	}
	if total != uint64(len(src)-off) {
		return nil, protocolError(ErrMalformedFrame, "byte-vector declared payload %d does not match remaining %d", total, len(src)-off)
	}

	var out [][]byte
	if count <= cap(dst) {
		if count < len(dst) {
			clear(dst[count:])
		}
		out = dst[:count]
	} else {
		out = make([][]byte, count)
	}
	payloadOff := off
	next := 0
	off = lengthsOff
	for i := 0; i < count; i++ {
		length, n, err := readUvarint(src[off:])
		if err != nil {
			return nil, err
		}
		off += n
		start := payloadOff + next
		next += int(length)
		out[i] = src[start : payloadOff+next]
	}
	return out, nil
}

// DecodeByteVectorInto decodes a byte-vector using reusable buffers when
// scratch is non-nil. The returned vector borrows from src and scratch.
func DecodeByteVectorInto(src []byte, limits Limits, scratch *ByteVectorScratch) (ByteVector, error) {
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
		return ByteVector{}, protocolError(ErrMalformedFrame, "byte-vector count %d exceeds remaining bytes %d", count64, len(src)-off)
	}
	count := int(count64)
	offsets, lengths := byteVectorBuffers(count, scratch)

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

func validateByteVector(src []byte, limits Limits) error {
	limits = limits.withDefaults()
	count64, off, err := readUvarint(src)
	if err != nil {
		return err
	}
	if count64 > uint64(limits.MaxByteVectorItems) {
		return protocolError(ErrResourceExhausted, "byte-vector count %d exceeds limit %d", count64, limits.MaxByteVectorItems)
	}
	if count64 > uint64(maxInt) {
		return protocolError(ErrResourceExhausted, "byte-vector count exceeds int capacity")
	}
	if count64 > uint64(len(src)-off) {
		return protocolError(ErrMalformedFrame, "byte-vector count %d exceeds remaining length table bytes %d", count64, len(src)-off)
	}

	total := uint64(0)
	for i := 0; i < int(count64); i++ {
		length, n, err := readUvarint(src[off:])
		if err != nil {
			return err
		}
		off += n
		if length > limits.MaxByteVectorBytes {
			return protocolError(ErrResourceExhausted, "byte-vector item %d length %d exceeds limit %d", i, length, limits.MaxByteVectorBytes)
		}
		if total+length < total {
			return protocolError(ErrMalformedFrame, "byte-vector length overflow")
		}
		total += length
		if total > limits.MaxByteVectorBytes {
			return protocolError(ErrResourceExhausted, "byte-vector payload length %d exceeds limit %d", total, limits.MaxByteVectorBytes)
		}
		if length > uint64(maxInt) || total > uint64(maxInt) {
			return protocolError(ErrResourceExhausted, "byte-vector payload length exceeds int capacity")
		}
	}
	if total != uint64(len(src)-off) {
		return protocolError(ErrMalformedFrame, "byte-vector declared payload %d does not match remaining %d", total, len(src)-off)
	}
	return nil
}

func byteVectorBuffers(count int, scratch *ByteVectorScratch) ([]int, []int) {
	if scratch == nil {
		return make([]int, count), make([]int, count)
	}
	if cap(scratch.offsets) < count {
		scratch.offsets = make([]int, count)
	}
	if cap(scratch.lengths) < count {
		scratch.lengths = make([]int, count)
	}
	return scratch.offsets[:count], scratch.lengths[:count]
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
