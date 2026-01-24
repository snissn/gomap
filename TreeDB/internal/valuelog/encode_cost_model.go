package valuelog

// EncodeCostModel provides deterministic encode cost estimates (ns) for tests/benches.
// When set on a Writer, it overrides wall-time measurement for sampled frames.
type EncodeCostModel interface {
	EncodeNs(rawPayloadBytes int, records int) int64
}

// EncodeCostModelFunc adapts a function to the EncodeCostModel interface.
type EncodeCostModelFunc func(rawPayloadBytes int, records int) int64

func (f EncodeCostModelFunc) EncodeNs(rawPayloadBytes int, records int) int64 {
	if f == nil {
		return 0
	}
	return f(rawPayloadBytes, records)
}
