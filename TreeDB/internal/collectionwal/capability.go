package collectionwal

type DurableAckCapability uint8

const (
	DurableAckDisabled DurableAckCapability = iota
	DurableAckNoIndexRowInsertOnly
)

func (c DurableAckCapability) String() string {
	switch c {
	case DurableAckDisabled:
		return "disabled"
	case DurableAckNoIndexRowInsertOnly:
		return "NoIndexRowInsertOnly"
	default:
		return "unknown"
	}
}

func (c DurableAckCapability) Enabled() bool {
	return c != DurableAckDisabled
}
