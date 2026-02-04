package template

import "context"

// TemplateKind indicates how a template encodes its payload.
type TemplateKind uint8

const (
	TemplateAnchors TemplateKind = iota + 1
	TemplateMask
)

// Candidate represents a routing candidate from templatedb.
type Candidate struct {
	ID   uint64
	Size int
}

// Store provides access to template definitions and routing candidates.
type Store interface {
	GetCandidates(ctx context.Context, fp uint64, max int) ([]Candidate, error)
	GetTemplateDef(ctx context.Context, templateID uint64) ([]byte, error)
	PutTemplateDef(ctx context.Context, defBytes []byte, routeFPs []uint64) (uint64, error)
}

// PublishSpec describes a template definition publish request.
type PublishSpec struct {
	DefBytes []byte
	RouteFPs []uint64
}

// BatchPublisher is an optional extension to Store that allows publishing
// multiple templates in a single durable batch.
type BatchPublisher interface {
	PutTemplateDefs(ctx context.Context, defs []PublishSpec) ([]uint64, error)
}

// TemplateDef is an ordered list of anchors.
type TemplateDef struct {
	Kind           TemplateKind
	Anchors        [][]byte
	Mask           []byte
	Base           []byte
	VarPositions   []uint16
	ConstPositions []uint16

	maskVarSpans   []span
	maskConstSpans []span
}

type span struct {
	start int
	end   int
}

// DecodeOptions control TemplateValue decoding limits.
type DecodeOptions struct {
	MaxDecodedBytes int
	MaxGaps         int

	// DefCacheSize controls the maximum number of decoded template definitions
	// cached by value-log readers. Values <= 0 disable caching.
	DefCacheSize int
}

func uvarintLen(v uint64) int {
	switch {
	case v < 1<<7:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<21:
		return 3
	case v < 1<<28:
		return 4
	case v < 1<<35:
		return 5
	case v < 1<<42:
		return 6
	case v < 1<<49:
		return 7
	case v < 1<<56:
		return 8
	case v < 1<<63:
		return 9
	default:
		return 10
	}
}
