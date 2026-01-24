package compression

type Kind uint8

const (
	KindNone Kind = iota
	KindZSTD
)

type Options struct {
	Kind            Kind
	MinBytes        int
	MinSavingsBytes int
	Level           int
}

type TrainConfig struct {
	TrainBytes     int
	DictBytes      int
	MinRecords     int
	MaxRecordBytes int
	SampleStride   int
	DedupWindow    int
	Level          int
	// Deterministic cost modeling (tests/benches only). When >0, overrides
	// encode/decode timing estimates in training evaluation.
	EncodeNsPerRawByte float64
	DecodeNsPerRawByte float64
}
