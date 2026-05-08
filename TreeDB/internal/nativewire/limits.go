package nativewire

const (
	defaultMaxFrameSize                       = uint64(64 << 20)
	defaultMaxSections                        = 1024
	defaultMaxSectionLen                      = uint64(16 << 20)
	defaultMaxByteVectorItems                 = 1 << 20
	defaultMaxByteVectorBytes                 = uint64(64 << 20)
	defaultMaxDeterministicOpaquePayloadBytes = uint64(1 << 20)
	defaultMaxDeterministicNameBytes          = uint64(128)
)

// maxInt is the maximum int value on the current platform. Decoders use it
// before converting wire uint64 lengths and counts to int.
const maxInt = int(^uint(0) >> 1)

type Limits struct {
	MaxFrameSize                       uint64
	MaxHeaderLen                       uint16
	MaxSections                        int
	MaxSectionLen                      uint64
	MaxByteVectorItems                 int
	MaxByteVectorBytes                 uint64
	MaxDeterministicOpaquePayloadBytes uint64
	MaxDeterministicNameBytes          uint64
}

func DefaultLimits() Limits {
	return Limits{
		MaxFrameSize:                       defaultMaxFrameSize,
		MaxHeaderLen:                       MaxFrameHeaderLen,
		MaxSections:                        defaultMaxSections,
		MaxSectionLen:                      defaultMaxSectionLen,
		MaxByteVectorItems:                 defaultMaxByteVectorItems,
		MaxByteVectorBytes:                 defaultMaxByteVectorBytes,
		MaxDeterministicOpaquePayloadBytes: defaultMaxDeterministicOpaquePayloadBytes,
		MaxDeterministicNameBytes:          defaultMaxDeterministicNameBytes,
	}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxFrameSize == 0 {
		l.MaxFrameSize = d.MaxFrameSize
	}
	if l.MaxHeaderLen == 0 {
		l.MaxHeaderLen = d.MaxHeaderLen
	}
	if l.MaxSections <= 0 {
		l.MaxSections = d.MaxSections
	}
	if l.MaxSectionLen == 0 {
		l.MaxSectionLen = d.MaxSectionLen
	}
	if l.MaxByteVectorItems <= 0 {
		l.MaxByteVectorItems = d.MaxByteVectorItems
	}
	if l.MaxByteVectorBytes == 0 {
		l.MaxByteVectorBytes = d.MaxByteVectorBytes
	}
	if l.MaxDeterministicOpaquePayloadBytes == 0 {
		l.MaxDeterministicOpaquePayloadBytes = d.MaxDeterministicOpaquePayloadBytes
	}
	if l.MaxDeterministicNameBytes == 0 {
		l.MaxDeterministicNameBytes = d.MaxDeterministicNameBytes
	}
	return l
}
