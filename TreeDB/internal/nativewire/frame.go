package nativewire

import "encoding/binary"

type Version struct {
	Major uint16
	Minor uint16
}

type Header struct {
	Version   Version
	Type      FrameType
	Flags     uint32
	StreamID  uint64
	RequestID uint64
	BodyLen   uint64
}

func AppendHeader(dst []byte, h Header) ([]byte, error) {
	if h.Version.Major == 0 {
		h.Version.Major = ProtocolMajorV1
	}
	if h.Version.Minor == 0 {
		h.Version.Minor = ProtocolMinorV0
	}
	if !validFrameType(h.Type) {
		return dst, protocolError(ErrInvalidCommand, "unknown frame type %d", h.Type)
	}
	if err := validateFrameFlags(h.Flags); err != nil {
		return dst, err
	}

	var buf [FrameHeaderLenV1]byte
	copy(buf[0:4], Magic)
	binary.LittleEndian.PutUint16(buf[4:6], FrameHeaderLenV1)
	binary.LittleEndian.PutUint16(buf[6:8], h.Version.Major)
	binary.LittleEndian.PutUint16(buf[8:10], h.Version.Minor)
	binary.LittleEndian.PutUint16(buf[10:12], uint16(h.Type))
	binary.LittleEndian.PutUint32(buf[12:16], h.Flags)
	binary.LittleEndian.PutUint64(buf[16:24], h.StreamID)
	binary.LittleEndian.PutUint64(buf[24:32], h.RequestID)
	binary.LittleEndian.PutUint64(buf[32:40], h.BodyLen)
	return append(dst, buf[:]...), nil
}

func DecodeHeader(src []byte, limits Limits) (Header, error) {
	limits = limits.withDefaults()
	if len(src) < int(FrameHeaderLenV1) {
		return Header{}, protocolError(ErrMalformedFrame, "short frame header: %d", len(src))
	}
	if src[0] != Magic[0] || src[1] != Magic[1] || src[2] != Magic[2] || src[3] != Magic[3] {
		return Header{}, protocolError(ErrMalformedFrame, "bad frame magic")
	}

	headerLen := binary.LittleEndian.Uint16(src[4:6])
	if headerLen < FrameHeaderLenV1 {
		return Header{}, protocolError(ErrMalformedFrame, "invalid header length %d", headerLen)
	}
	if headerLen > limits.MaxHeaderLen {
		return Header{}, protocolError(ErrResourceExhausted, "header length %d exceeds limit %d", headerLen, limits.MaxHeaderLen)
	}
	if len(src) < int(headerLen) {
		return Header{}, protocolError(ErrMalformedFrame, "truncated extended header: %d < %d", len(src), headerLen)
	}
	if headerLen > FrameHeaderLenV1 {
		return Header{}, protocolError(ErrUnsupportedFeature, "unnegotiated fixed-header extension")
	}

	h := Header{
		Version: Version{
			Major: binary.LittleEndian.Uint16(src[6:8]),
			Minor: binary.LittleEndian.Uint16(src[8:10]),
		},
		Type:      FrameType(binary.LittleEndian.Uint16(src[10:12])),
		Flags:     binary.LittleEndian.Uint32(src[12:16]),
		StreamID:  binary.LittleEndian.Uint64(src[16:24]),
		RequestID: binary.LittleEndian.Uint64(src[24:32]),
		BodyLen:   binary.LittleEndian.Uint64(src[32:40]),
	}
	if !validFrameType(h.Type) {
		return Header{}, protocolError(ErrInvalidCommand, "unknown frame type %d", h.Type)
	}
	if err := validateFrameFlags(h.Flags); err != nil {
		return Header{}, err
	}
	frameLen := uint64(headerLen) + h.BodyLen
	if frameLen < h.BodyLen {
		return Header{}, protocolError(ErrMalformedFrame, "frame length overflow")
	}
	if frameLen > limits.MaxFrameSize {
		return Header{}, protocolError(ErrResourceExhausted, "frame length %d exceeds limit %d", frameLen, limits.MaxFrameSize)
	}
	return h, nil
}

func ValidateHeaderVersion(h Header, selected Version) error {
	if h.Version.Major != selected.Major {
		return protocolError(ErrUnsupportedVersion, "major version %d is not selected major %d", h.Version.Major, selected.Major)
	}
	if h.Version.Minor != selected.Minor {
		return protocolError(ErrUnsupportedVersion, "minor version %d is not selected minor %d", h.Version.Minor, selected.Minor)
	}
	return nil
}

func validFrameType(typ FrameType) bool {
	return typ >= FrameHello && typ <= FrameGoaway
}

func validateFrameFlags(flags uint32) error {
	if flags&frameRequiredFlagsMask != 0 {
		return protocolError(ErrUnsupportedFeature, "unknown required frame flags 0x%08x", flags&frameRequiredFlagsMask)
	}
	return nil
}
