package collections

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// The maintained validator checks child spans, not their document minimum or
// terminator. Validate each borrowed document before slicing its element body.
func nativeTextBSONBody(document []byte) ([]byte, error) {
	if len(document) < 5 {
		return nil, fmt.Errorf("collections: text BSON document span is shorter than five bytes")
	}
	length, _, ok := bsoncore.ReadLength(document)
	if !ok || int64(length) != int64(len(document)) || document[len(document)-1] != 0 {
		return nil, fmt.Errorf("collections: text BSON document length or terminator is invalid")
	}
	return document[4 : len(document)-1], nil
}

// Count with the maintained element reader and admitted, borrowed-tail DFS.
// Grow each complete stack backing before allocation; discarded cohorts remain
// charged on the original operation lease. No implicit recursive census stack
// or whole-document-as-element bound is needed for a large string payload.
func nativeTextBSONElements(document []byte, admit func(uint64) error) (uint64, error) {
	remaining, err := nativeTextBSONBody(document)
	if err != nil {
		return 0, err
	}
	var tails [][]byte
	var count uint64
	for {
		if len(remaining) == 0 {
			if len(tails) == 0 {
				return count, nil
			}
			last := len(tails) - 1
			remaining = tails[last]
			tails[last] = nil
			tails = tails[:last]
			continue
		}
		element, rest, ok := bsoncore.ReadElement(remaining)
		if !ok {
			return 0, fmt.Errorf("collections: text BSON element span is invalid")
		}
		count++
		value := element.Value()
		var child []byte
		switch value.Type {
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			child = value.Data
		case bsoncore.TypeCodeWithScope:
			// ReadCodeWithScope owns a decoded code string. Admit its full
			// source bound before that maintained decoder; scope stays borrowed.
			if err = admit(memtable.COWAllocationCharge(uint64(len(value.Data)))); err != nil {
				return 0, err
			}
			_, child, _, ok = bsoncore.ReadCodeWithScope(value.Data)
			if !ok {
				return 0, fmt.Errorf("collections: text BSON code-with-scope is invalid")
			}
		default:
			remaining = rest
			continue
		}
		body, err := nativeTextBSONBody(child)
		if err != nil {
			return 0, err
		}
		if len(rest) != 0 {
			if len(tails) == cap(tails) {
				if cap(tails) > int(^uint(0)>>1)/2 {
					return 0, memtable.ErrCOWCapacity
				}
				next := 2 * cap(tails)
				if next == 0 {
					next = 4
				}
				width := uint64(unsafe.Sizeof([]byte{}))
				if uint64(next) > ^uint64(0)/width {
					return 0, memtable.ErrCOWCapacity
				}
				if err = admit(memtable.COWAllocationCharge(uint64(next) * width)); err != nil {
					return 0, err
				}
				grown := make([][]byte, len(tails), next)
				copy(grown, tails)
				tails = grown
			}
			tails = append(tails, rest)
		}
		remaining = body
	}
}

// Bounds the pinned v2.6.0 canonical ExtJSON codec, not just its final output.
// Escaping consumes at most six output bytes per input byte. Every BSON value
// has one element marker; 64 bytes per element covers the fixed EJSON wrappers
// (including canonical numeric/date/timestamp/DBPointer forms). Payload bytes,
// including code-with-scope and base64 binary, fit the six-byte term.
func nativeTextBSONConversionCharge(source, elements uint64) (uint64, error) {
	var total uint64
	add := func(n, unit uint64) bool {
		if n > ^uint64(0)/unit {
			return false
		}
		n *= unit
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	if source > (^uint64(0)-3)/6 || elements > (^uint64(0)-3-6*source)/64 {
		return 0, memtable.ErrCOWCapacity
	}
	output := 6*source + 64*elements + 3 // document delimiters/newline
	// Keep every serial allocation cohort until operation cleanup. The writer's
	// full-document append buffer has <=16 output-sized growth/rounding cohorts
	// under Go 1.26 growslice. Per-key/value bytes.Buffer cohorts consume <=8,
	// formatting strings/copies <=4, and the sole output write one further copy.
	// The fixed per-element allowance is two 64-byte minimum buffers, two
	// formatting/reader closures, and their small allocation rounding cohorts.
	if !add(output, 16+8+4+1) || !add(elements, 8*64) {
		return 0, memtable.ErrCOWCapacity
	}
	// extJSONValueWriter has <=2 frames per element, with 2*cap+1 growth.
	// Four frame cohorts and pointer-width slots dominate its one-byte mode.
	if elements > (^uint64(0)-5)/2 || !add(2*elements+5, 4*uint64(unsafe.Sizeof(uintptr(0)))) || !add(1, 1024) {
		return 0, memtable.ErrCOWCapacity
	}
	return memtable.COWAllocationCharge(total), nil
}

type nativeTextJSONOutput struct{ document []byte }

func (w *nativeTextJSONOutput) Write(p []byte) (int, error) {
	// The maintained writer flushes once, at the top-level document end.
	if w.document != nil {
		return 0, fmt.Errorf("collections: text BSON codec wrote multiple documents")
	}
	w.document = bytes.Clone(p)
	return len(p), nil
}

func nativeTextBSONJSON(raw bson.Raw, admit func(uint64) error) ([]byte, error) {
	elements, err := nativeTextBSONElements(raw, admit)
	if err != nil {
		return nil, err
	}
	if err = raw.Validate(); err != nil {
		return nil, fmt.Errorf("collections: text index backfill BSON document: %w", err)
	}
	charge, err := nativeTextBSONConversionCharge(uint64(len(raw)), elements)
	if err != nil {
		return nil, err
	}
	if err = admit(charge); err != nil {
		return nil, fmt.Errorf("collections: native text BSON conversion workspace: %w", err)
	}
	// MarshalExtJSON retains a dynamically growing writer stack in extjPool.
	// These public constructors use the same codec/options without that pool;
	// its stack and buffers become unreachable when this operation finishes.
	var output nativeTextJSONOutput
	writer := bson.NewExtJSONValueWriter(&output, true, false)
	if err = bson.NewEncoder(writer).Encode(raw); err != nil {
		return nil, err
	}
	// The public writer adds one newline; the maintained MarshalExtJSON does not.
	out := bytes.TrimSuffix(output.document, []byte{'\n'})
	return out[:len(out):len(out)], nil
}

// The embedded resolver retains the original captured/staged authority. Only
// lookup-by-ID participates in decoding stored TD1D values; the remaining
// maintained resolver methods keep their original behavior.
type nativeTextTemplateResolver struct {
	templateV1Resolver
	admit func(uint64) error
}

func (r *nativeTextTemplateResolver) lookupTemplateV1(id uint64) (*templateV1Template, error) {
	tpl, err := r.templateV1Resolver.lookupTemplateV1(id)
	if err != nil {
		return nil, err
	}
	// Names are not present in the compact document. Charge each occurrence
	// before the decoder constructs its map and the encoder escapes those keys.
	// Include empty names: every field still needs a map/encoding entry.
	n := uint64(len(tpl.fields))
	for _, name := range tpl.fields {
		if uint64(len(name)) > ^uint64(0)-n {
			return nil, memtable.ErrCOWCapacity
		}
		n += uint64(len(name))
	}
	amount, err := nativeTemplateMaterializationCharge(n)
	if err != nil {
		return nil, err
	}
	if err = r.admit(amount); err != nil {
		return nil, fmt.Errorf("collections: native text template dictionary workspace: %w", err)
	}
	return tpl, nil
}

func nativeTextTemplateJSON(document []byte, resolver templateV1Resolver, admit func(uint64) error) ([]byte, error) {
	if resolver == nil {
		return nil, errTemplateV1MissingResolver
	}
	amount, err := nativeTemplateMaterializationCharge(uint64(len(document)))
	if err != nil {
		return nil, err
	}
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTextTemplateResolver{})))
	if amount > ^uint64(0)-header {
		return nil, memtable.ErrCOWCapacity
	}
	if err = admit(amount + header); err != nil {
		return nil, fmt.Errorf("collections: native text template conversion workspace: %w", err)
	}
	wrapped := &nativeTextTemplateResolver{templateV1Resolver: resolver, admit: admit}
	// Keep the maintained decoder and json.Marshal semantics, including output
	// cloning. Decoded objects, resolver reads/names, output and growth cohorts
	// stay on this operation's lease. Only encoding/json's inaccessible cached
	// encodeState buffer, ptrSeen and type caches after return are inherited
	// library-cache storage, not application/source pools or a process RSS quota.
	return templateV1StoredDocumentJSON(document, wrapped)
}

func nativeTextCapturedTemplateResolver(read *NativeJSONRead, admit func(uint64) error) (templateV1Resolver, error) {
	// Header plus read/admission closure, before constructing either object.
	environment := uint64(unsafe.Sizeof(struct {
		entry uintptr
		read  *NativeJSONRead
		admit func(uint64) error
	}{}))
	amount := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeTemplateResolver{}))) + memtable.COWAllocationCharge(environment)
	if err := admit(amount); err != nil {
		return nil, err
	}
	return read.templateResolver(admit), nil
}
