package collections

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/buger/jsonparser"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
)

// Analyzer option normalization constructs stopword slices/maps on every
// analysis call. Bound those and field-name/runtime headers from the captured
// declaration independently of document/query bytes.
func nativeTextDefinitionCharge(def TextIndexDefinition, calls uint64) (uint64, error) {
	size := uint64(len(def.Fields)) + 1
	add := func(n uint64) bool {
		if n > ^uint64(0)-size {
			return false
		}
		size += n
		return true
	}
	for _, field := range def.Fields {
		if !add(uint64(len(field.Field))) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	if def.AnalyzerOptions != nil {
		for _, word := range def.AnalyzerOptions.StopWords {
			if !add(uint64(len(word)) + 1) {
				return 0, memtable.ErrCOWCapacity
			}
		}
	}
	unit := 16*uint64(unsafe.Sizeof(string(""))) + 2*uint64(unsafe.Sizeof(TextToken{}))
	if calls == 0 || calls > ^uint64(0)/unit || size > ^uint64(0)/(calls*unit) {
		return 0, memtable.ErrCOWCapacity
	}
	return memtable.COWAllocationCharge(size * calls * unit), nil
}

// nativeTextRootValueTokenBound bounds unfiltered token occurrences. The
// source-envelope fallback retains its byte bound. The handled root selector
// borrows string contents without JSON quotes. Without escapes, the maintained
// simple analyzer sees these exact UTF-8 runes and emits one token per maximal
// simpleTextTokenRune run before stopword filtering. No distinct-term claim is
// needed. Rune decoding retains no string or copy of the borrowed byte span.
// Arrays, escaped strings and all other analyzers keep the existing byte bound;
// nested fields never reach this helper (their decoder path remains unchanged).
func nativeTextRootValueTokenBound(def TextIndexDefinition, value jsonParserIndexValue) uint64 {
	if value.valueType != jsonparser.String || def.Analyzer != TextAnalyzerSimple || bytes.IndexByte(value.raw, '\\') >= 0 {
		return (uint64(len(value.raw)) + 1) / 2
	}
	var tokens uint64
	inToken := false
	for raw := value.raw; len(raw) > 0; {
		r, width := utf8.DecodeRune(raw)
		raw = raw[width:]
		if simpleTextTokenRune(r) {
			if !inToken {
				tokens++
			}
			inToken = true
		} else {
			inToken = false
		}
	}
	return tokens
}

// nativeTextRootValueTermBound counts the keys the maintained analyzer can
// produce for one eligible field. Repeated occurrences remain separately charged
// by the unfiltered run bound below; this distinct count bounds only per-term
// maps, descriptors and temporary table keys. Every additional census allocation is admitted
// first on the original transaction lease and remains charged through teardown.
func nativeTextRootValueTermBound(def TextIndexDefinition, value jsonParserIndexValue, admit func(uint64) error) (uint64, error) {
	if value.valueType != jsonparser.String || def.Analyzer != TextAnalyzerSimple || bytes.IndexByte(value.raw, '\\') >= 0 {
		return (uint64(len(value.raw)) + 1) / 2, nil
	}
	tokens := nativeTextRootValueTokenBound(def, value)
	definition, err := nativeTextDefinitionCharge(def, 1)
	if err != nil || tokens > ^uint64(0)/2 {
		return 0, memtable.ErrCOWCapacity
	}
	var charge nativeTextAdmissionSum
	charge.add(1, definition)
	charge.array(1, uint64(unsafe.Sizeof(textTermAccumulator{})))
	charge.array(tokens, uint64(unsafe.Sizeof(textAnalyzedTerm{})))
	charge.array(uint64(len(value.raw)), 1) // owned analyzer input string
	// Lowercased UTF-8 is at most twice the source bytes. Byte-slice growth is
	// at least1.25x; the final rounded capacity is at most4*output+16, so all
	// old capacities sum to at most5 times that. With AllocationCharge(c)
	// <=2*c+16 and at most output allocations,128*n+256*T covers every
	// current/discarded Builder backing, including repeated tokens.
	charge.add(uint64(len(value.raw)), 128)
	charge.add(tokens, 256)
	charge.mapEntries(2*tokens, uint64(unsafe.Sizeof(string("")))+uint64(unsafe.Sizeof(int(0))))
	if charge.bad {
		return 0, memtable.ErrCOWCapacity
	}
	if err = admit(charge.total); err != nil {
		return 0, fmt.Errorf("collections: native text term census workspace: %w", err)
	}
	// Preallocate the existing accumulator's term lane to the unfiltered run
	// bound: stopword filtering/lowercasing cannot add terms. No slice growth,
	// positions, offsets, output map or retained census state is constructed.
	acc := textTermAccumulator{terms: make([]textAnalyzedTerm, 0, int(tokens))}
	if err = analyzeTextToTermAccumulator(def.Analyzer, def.AnalyzerOptions, string(value.raw), &acc); err != nil {
		return 0, err
	}
	return uint64(len(acc.terms)), nil
}

// nativeTextSimpleFieldCharge separates byte storage from cardinality storage
// for the exact unescaped simple-analyzer input counted above. N is source
// bytes, T is unfiltered runs, and U is the maintained census's distinct terms.
// The caller retains the original byte envelope whenever it is smaller.
// All cohorts (including unreachable growth) remain on the operation lease.
func nativeTextSimpleFieldCharge(def TextIndexDefinition, source, tokens, terms, documentIDBytes, fieldBytes uint64) (uint64, error) {
	return nativeTextFieldCharge(def, source, tokens, terms, documentIDBytes, fieldBytes, false)
}

// Prepared input already owns analyzer/state backing. Here source counts the
// actual normalized term bytes, rather than a raw-source expansion bound.
func nativeTextFieldCharge(def TextIndexDefinition, source, tokens, terms, documentIDBytes, fieldBytes uint64, prepared bool) (uint64, error) {
	if terms > tokens || (!prepared && tokens > source) || terms > ^uint64(0)/2 || uint64(len(def.Fields)) > uint64(textV2PostingBlockMaxFieldCount) {
		return 0, memtable.ErrCOWCapacity
	}
	var charge nativeTextAdmissionSum
	// This is the SAME builder/input proof as the separately admitted census:
	// lowercasing emits <=2*N UTF-8 bytes, including discarded repeated terms.
	// Reset does not reuse a previous term's backing. It is not U-weighted.
	if !prepared {
		charge.array(source, 1)
		charge.add(source, 128)
		charge.add(tokens, 256)
	}
	// Append-only typed slices: geometric growth >=1.25, final capacity <=2*n
	// plus allocator rounding. 32*n*(element+1) covers all backing cohorts,
	// including discarded ones, independently for each lane below. Batch
	// lanes are amortized over cumulative admitted per-document U, not merely
	// charged to the row that happens to trigger their next growth.
	grow := func(count, element uint64) {
		if element == ^uint64(0) || element+1 > ^uint64(0)/32 {
			charge.bad = true
			return
		}
		charge.add(count, 32*(element+1))
	}
	if !prepared {
		grow(terms, uint64(unsafe.Sizeof(textAnalyzedTerm{})))
	}
	grow(terms, uint64(unsafe.Sizeof(textPostingFieldValue{})))
	grow(terms, uint64(unsafe.Sizeof(textV2DocumentPostingTerm{})))
	grow(terms, uint64(unsafe.Sizeof(textV2PostingBlockEntry{})))
	grow(terms, uint64(unsafe.Sizeof(textV2PositionFieldValue{})))
	grow(terms, uint64(unsafe.Sizeof(textV2PostingBlockKV{})))
	// Fixed-capacity state/sort clones, posting header clone and sorted term
	// lists. Per-element rounding also covers independently small arrays.
	if !prepared {
		charge.add(terms, 2*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textDocumentTermState{}))))
	}
	charge.add(terms, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textV2PostingBlockEntry{}))))
	charge.add(terms, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textV2PositionFieldValue{}))))
	charge.add(terms, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textPostingFieldValue{}))))
	charge.add(terms, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(textStatsTermValue{}))))
	charge.add(terms, 10*memtable.COWAllocationCharge(uint64(unsafe.Sizeof(string("")))))
	// Accumulator lookup (2*U hint), analysis map, V1 posting map, V2
	// document lookup (2*U hint), batch postings, position fields, stats seen,
	// term deltas, term union, block counts and block deltas. Adding both
	// version families is conservative; maps are never assumed to share storage.
	str, integer := uint64(unsafe.Sizeof(string(""))), uint64(unsafe.Sizeof(int(0)))
	if !prepared {
		charge.mapEntries(2*terms, str+integer)
		charge.mapEntries(terms, str+uint64(unsafe.Sizeof((*textAnalyzedTerm)(nil))))
	}
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof(textPostingValue{})))
	charge.mapEntries(2*terms, str+integer)
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof([]textV2PostingBlockEntry{})))
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof([]textV2PositionFieldValue{})))
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof(uint64(0))))
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof(textStatsTermDelta{})))
	charge.mapEntries(terms, str)
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof(uint64(0))))
	charge.mapEntries(terms, str+uint64(unsafe.Sizeof(int64(0))))
	// Positions/offsets are per occurrence, even with one repeated term.
	// Analysis append growth and both V2 copy+encoder-clone cohorts coexist;
	// state terms borrow these arrays. Charge flags independently.
	var occurrence uint64
	if def.StorePositions {
		occurrence += uint64(unsafe.Sizeof(uint32(0)))
	}
	if def.StoreOffsets {
		occurrence += uint64(unsafe.Sizeof(textTokenOffset{}))
	}
	if !prepared {
		grow(tokens, occurrence)
		charge.add(tokens, 4*occurrence)
		charge.add(terms, 4*16)
	} else {
		// State emission borrows original arrays; only the encoder clones.
		charge.add(tokens, 2*occurrence)
		charge.add(terms, 2*16)
	} // rounding for the two separately copied arrays
	// Each V2 document/term allocates one complete declared-field frequency
	// lane. Candidate summary lanes, including retries, are charged below.
	charge.add(terms, memtable.COWAllocationCharge(uint64(len(def.Fields))*4))
	// Encoded term-byte cohorts include string-to-byte conversions, V1 state/
	// posting/stats keys or V2 block-prefix growth/position/fingerprint/stats
	// keys. Sixteen normalized-byte cohorts dominate either version, including
	// both stats lookup/output keys. Normalized bytes <=2*N and Q(c)<=2*c+16;
	// fixed encoder capacities are charged separately below.
	if prepared {
		charge.add(source, 32)
	} else {
		charge.add(source, 64)
	}
	charge.add(terms, 16*16)
	// Maximum-varint posting wire capacities are the maintained encoder's
	// H(F)+c*E(F), including its upper-capacity hints, not just emitted bytes.
	// Source sizing also precedes sealed backfill (128), so bound that target,
	// which dominates mutation micro-blocks (32). If even the full target fits,
	// halving is impossible. Otherwise each of <=P output chunks may restart
	// at target: <=8*P attempts and <256*P entry visits, not merely 2*P visits.
	fields := uint64(len(def.Fields))
	header := uint64(1 + 2 + 10*binary.MaxVarintLen64 + textV2PostingBlockChecksumBytes)
	if fields > (^uint64(0)-header)/21 {
		return 0, memtable.ErrCOWCapacity
	}
	header += fields * 11
	entry := uint64(31) + fields*10
	target := uint64(textV2PostingBlockTargetPostings)
	attempts, visits := uint64(1), uint64(1)
	if entry > (^uint64(0)-header)/target || header+target*entry > uint64(min(textV2PostingBlockInlineTargetBytes, textV2PostingBlockMaxValueBytes)) {
		attempts, visits = 8, 2*target
	}
	// Q(c)<=2*c+16 for every candidate, and a separately allocated summary
	// F*uint32 lane for every attempt. All failed candidates remain charged.
	charge.add(terms, attempts*memtable.COWAllocationCharge(fields*4))
	if header > (^uint64(0)-16)/2 || 2*header+16 > ^uint64(0)/attempts || entry > (^uint64(0)-attempts*(2*header+16))/(2*visits) {
		return 0, memtable.ErrCOWCapacity
	}
	charge.add(terms, attempts*(2*header+16)+2*visits*entry)
	// Fixed wire capacities are separate from normalized term bytes and
	// occurrence varints. Pre-sized posting/state/position encoders allocate
	// once; append-based stats encoders have a bounded number of append calls.
	// For each such growth: cap<=2*required+16, Q(cap)<=4*required+48.
	// This also admits discarded backing and conversions, without assuming
	// string-to-byte conversion is compiler-borrowed.
	var wire nativeTextAdmissionSum
	if def.Version == TextIndexVersionV1 {
		// Posting key; posting and state value per-field/term headers. The
		// state's per-document zero-term headers remain in row controls.
		wire.add(1, 2*(1+binary.MaxVarintLen64)+16)
		wire.add(documentIDBytes, 2)
		wire.add(2, 2*(1+6*binary.MaxVarintLen64)+16)
		wire.add(fieldBytes, 4)
		// Two stats keys (read/write), each <=3 appends, and a two-varint
		// stats value (initial byte plus <=2 appends).
		wire.add(6, 4*(2+binary.MaxVarintLen64)+48)
		wire.add(3, 4*(1+2*binary.MaxVarintLen64)+48)
	} else {
		// Position key; position value envelope includes five top-level
		// varints/fingerprint plus four field/count varints per field.
		wire.add(1, 2*(2+8+binary.MaxVarintLen64)+16)
		wire.add(1, 2*(1+9*binary.MaxVarintLen64+textV2PositionTermFingerprintSize)+16)
		// Both stats keys; four-varint stats value; block prefix/suffix
		// backing (two allocations, each with <=28 fixed bytes).
		wire.add(6, 4*(2+binary.MaxVarintLen64)+48)
		wire.add(5, 4*(1+4*binary.MaxVarintLen64)+48)
		wire.add(2, 4*(2+binary.MaxVarintLen64+16)+48)
	}
	if wire.bad {
		return 0, memtable.ErrCOWCapacity
	}
	charge.add(terms, wire.total)
	// V1 posting+state and V2 positions serialize each occurrence at most
	// three times; a uint32 position takes <=5 bytes and two offsets <=10.
	charge.add(tokens, 6*uint64(3*binary.MaxVarintLen32))
	if !prepared {
		charge.array(1, uint64(unsafe.Sizeof(textTermAccumulator{}))+uint64(unsafe.Sizeof(textV2DocumentPostingAccumulator{})))
	} else {
		charge.array(1, uint64(unsafe.Sizeof(textV2DocumentPostingAccumulator{})))
	}
	if charge.bad {
		return 0, memtable.ErrCOWCapacity
	}
	return charge.total, nil
}

// Only this existing stored-JSON/simple representation is prepared here.
// Other formats, V1 and selected arrays/escapes retain their original planner.
func nativeTextCanPrepare(def TextIndexDefinition, opts collectionOptions, values []jsonParserIndexValue) bool {
	if def.Version != TextIndexVersionV2 || def.Analyzer != TextAnalyzerSimple || normalizedDocumentFormat(opts.documentFormat) != DocumentFormatJSON {
		return false
	}
	for _, field := range def.Fields {
		if field.Field == "" || strings.Contains(field.Field, ".") {
			return false
		}
	}
	for _, value := range values {
		if value.valueType == jsonparser.Array || value.valueType == jsonparser.String && bytes.IndexByte(value.raw, '\\') >= 0 {
			return false
		}
	}
	return true
}

// The caller has already admitted selection and definition normalization.
// Analyze once with the maintained sink and actual position/offset options.
// Retain every input/Builder/growth cohort through the original lease; state
// terms borrow those strings/occurrence arrays, never refunded analyzer credit.
func nativeTextPrepareCurrent(def TextIndexDefinition, values []jsonParserIndexValue, admit func(uint64) error) (textDocumentStateValue, error) {
	var headers nativeTextAdmissionSum
	headers.array(1, uint64(unsafe.Sizeof(textDocumentStateValue{})))
	headers.array(uint64(len(def.Fields)), uint64(unsafe.Sizeof(textDocumentFieldState{})))
	if headers.bad {
		return textDocumentStateValue{}, memtable.ErrCOWCapacity
	}
	if err := admit(headers.total); err != nil {
		return textDocumentStateValue{}, fmt.Errorf("collections: native text prepared headers: %w", err)
	}
	state := textDocumentStateValue{Fields: make([]textDocumentFieldState, 0, len(def.Fields))}
	for i, value := range values {
		if value.valueType != jsonparser.String {
			continue
		}
		n := uint64(len(value.raw))
		var input nativeTextAdmissionSum
		input.array(n, 1)
		// Preserve input conversion and owner admission. Options were admitted
		// by the original caller. This owner's private analyzer capability
		// admits each Builder backing before Grow, including stopped/duplicate
		// tokens and obsolete growth cohorts, on this SAME stage callback.
		input.array(1, uint64(unsafe.Sizeof(textTermAccumulator{})))
		if input.bad {
			return textDocumentStateValue{}, memtable.ErrCOWCapacity
		}
		if err := admit(input.total); err != nil {
			return textDocumentStateValue{}, fmt.Errorf("collections: native text prepared analysis: %w", err)
		}
		// This exact owner remains reachable through its inline term backing
		// when <=8 distinct terms are retained. Never copy/reuse the owner.
		acc := &textTermAccumulator{storePositions: def.StorePositions, storeOffsets: def.StoreOffsets, admit: admit}
		acc.terms = acc.inline[:0]
		if err := analyzeTextToTermAccumulator(def.Analyzer, def.AnalyzerOptions, string(value.raw), acc); err != nil {
			return textDocumentStateValue{}, fmt.Errorf("collections: native text prepared state: %w", err)
		}
		// No termsMap, analyzed-field adapter, or second term array: consume
		// the same owned lane after its last lookup; discard lookup aliases.
		state.Fields = append(state.Fields, acc.takeDocumentField(def.Fields[i].Field))
	}
	slices.SortFunc(state.Fields, func(a, b textDocumentFieldState) int { return compareTextStrings(a.Field, b.Field) })
	return state, nil
}

type nativeTextPreparation struct {
	states    []textDocumentStateValue
	mutations []textDocumentMutation
}

func nativeTextSourceCharge(meta CollectionMeta, rows []nativeJSONRow, opts collectionOptions, admit func(uint64) error) (uint64, error) {
	return nativeTextSourceChargePrepared(meta, rows, opts, admit, nil)
}

// With preparation, already executed analyzer phases are admitted immediately;
// the returned amount is ONLY the remaining downstream envelope. Without it,
// preserve the original full returned source charge for fallback/DDL callers.
func nativeTextSourceChargePrepared(meta CollectionMeta, rows []nativeJSONRow, opts collectionOptions, admit func(uint64) error, preparation *nativeTextPreparation) (uint64, error) {
	var total uint64
	add := func(n uint64) bool {
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	// Fallback: a source byte bounds one token/term, all state/posting/position headers,
	// map entries, and simultaneous analysis/encoded images. No token expansion
	// analyzer is eligible (synonyms/stemming are rejected by normalization).
	unit := 2 * (uint64(unsafe.Sizeof(TextToken{})) + uint64(unsafe.Sizeof(textAnalyzedTerm{})) + uint64(unsafe.Sizeof(textDocumentTermState{})) + uint64(unsafe.Sizeof(textV2PostingBlockEntry{})) + uint64(unsafe.Sizeof(textV2PositionFieldValue{})) + 8*uint64(unsafe.Sizeof(string(""))))
	for _, def := range meta.TextIndexes {
		calls := uint64(len(rows)) * 2
		if len(def.Fields) != 0 && calls > (^uint64(0)-1)/uint64(len(def.Fields)) {
			return 0, memtable.ErrCOWCapacity
		}
		definitionCharge, err := nativeTextDefinitionCharge(def, 1+calls*uint64(len(def.Fields)))
		if err != nil {
			return 0, err
		}
		if preparation != nil {
			// Analyzer option normalization must be admitted BEFORE preparation.
			// It is not charged again in the returned downstream envelope.
			if err := admit(definitionCharge); err != nil {
				return 0, err
			}
		} else if !add(definitionCharge) {
			return 0, memtable.ErrCOWCapacity
		}
		// Fixed formats/status/corpus plus field stats; each row emits at most
		// one docid, docmap and norm key. Term/posting/position keys are bounded
		// by analyzed tokens below; existing block decode has its own receipt.
		operations := uint64(9+len(def.Fields)) + 3*uint64(len(rows))
		for rowIndex, row := range rows {
			for imageIndex, doc := range [][]byte{row.opening, row.current} {
				if doc == nil {
					continue
				}
				// Size the same maintained stored-to-JSON representation that the
				// reducer analyzes. Conversion cohorts remain charged: the reducer
				// converts again while the sizing buffer may await collection.
				var err error
				doc, err = materializeTextBackfillDocumentJSON(doc, opts)
				if err != nil {
					return 0, err
				}
				// JSON selection borrows source spans; ObjectEach can decode
				// escaped keys. Reserve source-sized parser/conversion scratch
				// and selected-value headers
				// before selection; unrelated values incur no token multiplier.
				source := memtable.COWAllocationCharge(uint64(len(doc)))
				valueSize := uint64(unsafe.Sizeof(jsonParserIndexValue{}))
				if source > ^uint64(0)/2 || uint64(len(def.Fields)) > ^uint64(0)/valueSize {
					return 0, memtable.ErrCOWCapacity
				}
				refs := memtable.COWAllocationCharge(uint64(len(def.Fields)) * valueSize)
				if refs > ^uint64(0)-2*source {
					return 0, memtable.ErrCOWCapacity
				}
				parse := 2*source + refs
				if err := admit(parse); err != nil {
					return 0, err
				}
				var stack [8]jsonParserIndexValue
				values := stack[:]
				if len(def.Fields) > len(stack) {
					values = make([]jsonParserIndexValue, len(def.Fields))
				} else {
					values = values[:len(def.Fields)]
				}
				handled, err := selectTextIndexDocumentJSONRootValues(def, doc, values)
				if err != nil {
					return 0, err
				}
				if preparation != nil && imageIndex == 1 && handled && nativeTextCanPrepare(def, opts, values) {
					state, err := nativeTextPrepareCurrent(def, values, admit)
					if err != nil {
						return 0, err
					}
					preparation.states[rowIndex] = state
					preparation.mutations[rowIndex].preparedNew = &preparation.states[rowIndex]
					var distinct uint64
					for _, field := range state.Fields {
						var normalized, occurrences uint64
						for _, term := range field.Terms {
							count := max(uint64(term.Frequency), uint64(len(term.Positions)), uint64(len(term.Offsets)))
							if uint64(len(term.Term)) > ^uint64(0)-normalized || count > ^uint64(0)-occurrences {
								return 0, memtable.ErrCOWCapacity
							}
							normalized += uint64(len(term.Term))
							occurrences += count
						}
						u := uint64(len(field.Terms))
						part, err := nativeTextFieldCharge(def, normalized, occurrences, u, uint64(len(row.id)), uint64(len(field.Field)), true)
						if err != nil || !add(part) || u > ^uint64(0)-distinct {
							return 0, memtable.ErrCOWCapacity
						}
						distinct += u
					}
					overhead := uint64(len(row.id)) + uint64(len(def.Fields)) + 1
					if overhead > ^uint64(0)/unit || !add(memtable.COWAllocationCharge(overhead*unit)) || distinct > (^uint64(0)-operations)/3 {
						return 0, memtable.ErrCOWCapacity
					}
					operations += 3 * distinct
					continue
				}
				var selected, tokens, analyzed uint64
				refined := false
				if handled {
					for fieldIndex, value := range values {
						if value.valueType != jsonparser.String && value.valueType != jsonparser.Array {
							continue
						}
						n := uint64(len(value.raw))
						bound, err := nativeTextRootValueTermBound(def, value, admit)
						if err != nil {
							return 0, err
						}
						if n > ^uint64(0)-selected || bound > ^uint64(0)-tokens {
							return 0, memtable.ErrCOWCapacity
						}
						selected += n
						tokens += bound
						if n > ^uint64(0)/unit {
							return 0, memtable.ErrCOWCapacity
						}
						fieldCharge := memtable.COWAllocationCharge(n * unit)
						if value.valueType == jsonparser.String && def.Analyzer == TextAnalyzerSimple && bytes.IndexByte(value.raw, '\\') < 0 {
							precise, err := nativeTextSimpleFieldCharge(def, n, nativeTextRootValueTokenBound(def, value), bound, uint64(len(row.id)), uint64(len(def.Fields[fieldIndex].Field)))
							if err == nil {
								fieldCharge = min(fieldCharge, precise)
								refined = true
							} // Overflow retains the original, already-checked byte envelope.
						}
						if fieldCharge > ^uint64(0)-analyzed {
							return 0, memtable.ErrCOWCapacity
						}
						analyzed += fieldCharge
					}
				} else {
					// The sizing decode uses the exact maintained fallback. Admit
					// its source/path/object scratch before allocation on this operation's
					// original lease. A second cohort remains in the final envelope: the
					// reducer decodes again, and unreachable first objects may await GC.
					decoder, err := nativeTextNestedDecoderCharge(def, uint64(len(doc)))
					if err != nil {
						return 0, err
					}
					if err = admit(decoder); err != nil {
						return 0, fmt.Errorf("collections: native text JSON decode workspace: %w", err)
					}
					object, err := decodeTextIndexDocumentJSONObject(doc)
					if err != nil {
						return 0, err
					}
					if !add(decoder) {
						return 0, memtable.ErrCOWCapacity
					}
					for _, field := range def.Fields {
						value, found := extractIndexPathValue(object, splitIndexPath(field.Field))
						if !found || value == nil {
							continue
						}
						var n uint64
						switch value := value.(type) {
						case string:
							n = uint64(len(value))
						case []any:
							// textIndexFieldText allocates len(input) string descriptors,
							// even when only a few elements are strings. Count empty strings
							// as members: they still contribute join separators.
							if uint64(len(value)) > ^uint64(0)/uint64(unsafe.Sizeof(string(""))) || !add(memtable.COWAllocationCharge(uint64(len(value))*uint64(unsafe.Sizeof(string(""))))) {
								return 0, memtable.ErrCOWCapacity
							}
							var members uint64
							for _, item := range value {
								if part, ok := item.(string); ok {
									if uint64(len(part)) > ^uint64(0)-n {
										return 0, memtable.ErrCOWCapacity
									}
									n += uint64(len(part))
									members++
								}
							}
							if members > 0 {
								if members-1 > ^uint64(0)-n {
									return 0, memtable.ErrCOWCapacity
								}
								n += members - 1
							}
						default:
							continue
						}
						if n > ^uint64(0)-selected || (n+1)/2 > ^uint64(0)-tokens {
							return 0, memtable.ErrCOWCapacity
						}
						selected += n
						tokens += (n + 1) / 2
					}
				}
				overhead := uint64(len(row.id)) + uint64(len(def.Fields)) + 1
				if selected > ^uint64(0)-overhead {
					return 0, memtable.ErrCOWCapacity
				}
				size := selected + overhead
				if size > ^uint64(0)/unit {
					return 0, memtable.ErrCOWCapacity
				}
				analysisCharge := memtable.COWAllocationCharge(size * unit)
				if refined {
					control := memtable.COWAllocationCharge(overhead * unit)
					if control > ^uint64(0)-analyzed {
						return 0, memtable.ErrCOWCapacity
					}
					analysisCharge = min(analysisCharge, analyzed+control)
				}
				if !add(analysisCharge) || tokens > (^uint64(0)-operations)/3 {
					return 0, memtable.ErrCOWCapacity
				}
				operations += 3 * tokens
			}
		}
		roots := uint64(7)
		if def.Version == TextIndexVersionV1 {
			roots = 3
		}
		tables, err := nativeTextTableCharge(operations, roots)
		if err != nil {
			return 0, err
		}
		if !add(tables) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}

// One full-source fallback decode cohort. Containers, interface/map/slice/string
// headers and growth are bounded per source byte; parser/reader and the initial
// 512-byte decoder buffer are charged separately for even tiny documents. Path
// splitting allocates string descriptors once per declared field per decode.
func nativeTextNestedDecoderCharge(def TextIndexDefinition, source uint64) (uint64, error) {
	charge := memtable.COWAllocationCharge
	unit := 4 * (uint64(unsafe.Sizeof(any(nil))) + uint64(unsafe.Sizeof(string(""))) + uint64(unsafe.Sizeof([]any{})))
	if source > ^uint64(0)/unit {
		return 0, memtable.ErrCOWCapacity
	}
	total := charge(source * unit)
	add := func(n uint64) bool {
		if n > ^uint64(0)-total {
			return false
		}
		total += n
		return true
	}
	if !add(charge(uint64(unsafe.Sizeof(json.Decoder{})))) || !add(charge(uint64(unsafe.Sizeof(bytes.Reader{})))) || !add(charge(512)) {
		return 0, memtable.ErrCOWCapacity
	}
	for _, field := range def.Fields {
		count := uint64(strings.Count(field.Field, ".")) + 1
		if count > ^uint64(0)/uint64(unsafe.Sizeof(string(""))) || !add(charge(count*uint64(unsafe.Sizeof(string(""))))) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}

func nativeTextDomainNumber(meta CollectionMeta, name string) uint64 {
	for i, def := range meta.TextIndexes {
		for j, root := range collectionTextRootNamesForDefinition(meta.Name, def) {
			if name == strings.TrimPrefix(root, meta.Name+"/") {
				return (uint64(1) << 32) + uint64(i)*7 + uint64(j)
			}
		}
	}
	return 0
}
func nativeTextTables(names []string, policies []backenddb.OrderedRootStoragePolicy, tables []memtable.Table, admit func(uint64) error) ([]nativeJSONDomainChange, error) {
	if len(names) != len(policies) || len(names) != len(tables) {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if err := admit(memtable.COWAllocationCharge(uint64(len(names)) * uint64(unsafe.Sizeof(nativeJSONDomainChange{})))); err != nil {
		return nil, err
	}
	out := make([]nativeJSONDomainChange, len(names))
	for i, table := range tables {
		if err := admit(memtable.COWAllocationCharge(uint64(table.Len()) * uint64(unsafe.Sizeof(memtable.COWMutation{})))); err != nil {
			return nil, err
		}
		out[i] = nativeJSONDomainChange{name: names[i], policy: policies[i], entries: make([]memtable.COWMutation, 0, table.Len())}
		it := table.NewIterator(nil, nil)
		err := func() error {
			defer it.Close()
			for ; it.Valid(); it.Next() {
				key, value := it.UnsafeKey(), it.UnsafeValue()
				if err := admit(memtable.COWAllocationCharge(uint64(len(key))) + memtable.COWAllocationCharge(uint64(len(value)))); err != nil {
					return err
				}
				flags := byte(node.FlagInline)
				if it.IsDeleted() {
					flags = node.FlagTombstone
				}
				out[i].entries = append(out[i].entries, memtable.COWMutation{Key: bytes.Clone(key), Value: bytes.Clone(value), Flags: flags})
			}
			return it.Error()
		}()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func nativeTextInitialDomains(meta CollectionMeta, admit func(uint64) error) ([]nativeJSONDomainChange, error) {
	if len(meta.TextIndexes) == 0 {
		return nil, nil
	}
	opts, err := collectionPlannerOptions(meta)
	if err != nil {
		return nil, err
	}
	opts.textMaterializationAdmit = admit
	charge, err := nativeTextSourceCharge(meta, nil, opts, admit)
	if err != nil {
		return nil, err
	}
	if err = admit(charge); err != nil {
		return nil, err
	}
	opts.textTableFactory = nativeTextTable
	catalog := &collectionCatalog{meta: meta}
	var out []nativeJSONDomainChange
	for _, def := range meta.TextIndexes {
		var plan *createTextIndexBackfillPlan
		var err error
		if def.Version == TextIndexVersionV2 {
			plan, err = buildCreateTextV2IndexBackfillPlan(nil, catalog, def, opts)
		} else {
			plan, err = buildCreateTextV1IndexBackfillPlan(nil, catalog, def, opts)
		}
		if err != nil {
			return nil, err
		}
		part, err := nativeTextTables(plan.rootNames, plan.policies, plan.tables, admit)
		resetCollectionTables(plan.tables)
		if err != nil {
			return nil, err
		}
		// V1's maintained builder omits empty posting/state tables. Native
		// captures still require all three declared domains, including empty ones.
		if def.Version == TextIndexVersionV1 {
			if err := admit(memtable.COWAllocationCharge(3 * uint64(unsafe.Sizeof(nativeJSONDomainChange{})))); err != nil {
				return nil, err
			}
			complete := make([]nativeJSONDomainChange, 0, 3)
			for _, name := range collectionTextRootNamesForDefinition(meta.Name, def) {
				found := false
				for _, domain := range part {
					if domain.name == name {
						complete = append(complete, domain)
						found = true
						break
					}
				}
				if !found {
					policy, e := collectionRootStoragePolicy(meta, name)
					if e != nil {
						return nil, e
					}
					complete = append(complete, nativeJSONDomainChange{name: name, policy: policy})
				}
			}
			part = complete
		}
		for i := range part {
			part[i].name = strings.TrimPrefix(part[i].name, meta.Name+"/")
		}
		out = append(out, part...)
	}
	return out, nil
}
func nativeTextMutationDomains(snap *backenddb.Snapshot, catalog *collectionCatalog, rows []nativeJSONRow, resolver templateV1Resolver, admit func(uint64) error) ([]nativeJSONDomainChange, error) {
	if len(catalog.meta.TextIndexes) == 0 {
		return nil, nil
	}
	opts, err := collectionPlannerOptions(catalog.meta)
	if err != nil {
		return nil, err
	}
	opts.textMaterializationAdmit = admit
	opts.templateResolver = resolver
	opts.textTableFactory = nativeTextTable
	var names []string
	var policies []backenddb.OrderedRootStoragePolicy
	var tables []memtable.Table
	defer func() { resetCollectionTables(tables) }()
	var baseRootIDs map[string]uint64
	for _, def := range catalog.meta.TextIndexes {
		// Prepared state is local to this definition and this captured plan.
		// The maintained reducer receives no state prepared for another index.
		var descriptors nativeTextAdmissionSum
		descriptors.array(1, uint64(unsafe.Sizeof(CollectionMeta{}))+uint64(unsafe.Sizeof(TextIndexDefinition{})))
		descriptors.array(uint64(len(rows)), uint64(unsafe.Sizeof(textDocumentMutation{})))
		canPrepare := nativeTextCanPrepare(def, opts, nil)
		if canPrepare {
			descriptors.array(uint64(len(rows)), uint64(unsafe.Sizeof(textDocumentStateValue{})))
			descriptors.array(1, uint64(unsafe.Sizeof(nativeTextPreparation{})))
		}
		if descriptors.bad {
			return nil, memtable.ErrCOWCapacity
		}
		if err = admit(descriptors.total); err != nil {
			return nil, err
		}
		mutations := make([]textDocumentMutation, len(rows))
		for i, row := range rows {
			mutations[i] = textDocumentMutation{documentID: row.id, oldDocument: row.opening, deleteOld: row.opening != nil, newDocument: row.current, setNew: row.current != nil}
		}
		var preparation *nativeTextPreparation
		if canPrepare {
			preparation = &nativeTextPreparation{states: make([]textDocumentStateValue, len(rows)), mutations: mutations}
		}
		oneMeta := catalog.meta
		oneDefinition := [1]TextIndexDefinition{def}
		oneMeta.TextIndexes = oneDefinition[:]
		// In prepared mode the explicit internal variant admits executed
		// analyzer phases immediately and returns ONLY remaining downstream
		// work. The old wrapper/DDL continues to return its full source charge.
		charge, err := nativeTextSourceChargePrepared(oneMeta, rows, opts, admit, preparation)
		if err != nil {
			return nil, err
		}
		if err = admit(charge); err != nil {
			return nil, fmt.Errorf("collections: native text analysis workspace: %w", err)
		}
		active := 0
		for i := range mutations {
			if mutations[i].deleteOld || mutations[i].setNew {
				mutations[active] = mutations[i]
				active++
			}
		}
		if active == 0 {
			continue
		}
		if baseRootIDs == nil {
			// The admitted table/control envelope includes this existing map.
			baseRootIDs = make(map[string]uint64)
		}
		if err = appendSingleTextIndexMutationDeltas(snap, catalog, opts, def, mutations[:active], &names, baseRootIDs, &policies, &tables); err != nil {
			return nil, err
		}
	}
	out, err := nativeTextTables(names, policies, tables, admit)
	for i := range out {
		out[i].name = strings.TrimPrefix(out[i].name, catalog.meta.Name+"/")
	}
	return out, err
}
func (t *NativeJSONTxn) prepareNativeText(g *nativeJSONTxnBinding) ([]nativeJSONDomainChange, error) {
	if len(g.probe.meta.TextIndexes) == 0 {
		return nil, nil
	}
	for _, def := range g.probe.meta.TextIndexes {
		name := collectionTextV2GenerationsRootName(g.name, def.Name)
		kind, key := txnhistory.PointEffect, encodeTextV2StatusKey()
		if def.Version == TextIndexVersionV1 {
			// V1 has no ordinal/status record. Corpus, term and field counts
			// share one RMW basis even for otherwise independent document IDs.
			name, kind, key = collectionTextStatsRootName(g.name, def.Name), txnhistory.RangeEffect, nil
		}
		name = strings.TrimPrefix(name, g.name+"/")
		ns := txnhistory.Namespace{Kind: txnhistory.CollectionNamespace, Collection: g.incarnation, Domain: nativeTextDomainNumber(g.probe.meta, name), Interpretation: 1}
		if err := t.trackDependency(ns, kind, key, nil); err != nil {
			return nil, err
		}
	}
	// Dependency backing survives publication. The private phase starts only
	// after those dependencies, and returns before ended construction credit is
	// returned to the SAME original stage receipt.
	phase, err := t.buildNativeTextPhase(g)
	if err != nil {
		return nil, err
	}
	if err = t.completeNativeTextPhase(g, phase); err != nil {
		return nil, err
	}
	return phase.domains, nil
}

func nativeTextDocumentFormat(format DocumentFormat) bool {
	switch normalizedDocumentFormat(format) {
	case DocumentFormatJSON, DocumentFormatBSON, DocumentFormatTemplateV1:
		return true
	default:
		return false
	}
}

func nativeTextMeta(meta CollectionMeta) bool {
	for _, def := range meta.TextIndexes {
		if (def.Version != TextIndexVersionV1 && def.Version != TextIndexVersionV2) || meta.Options.ColumnStore != nil || !nativeTextDocumentFormat(meta.Options.DocumentFormat) {
			return false
		}
	}
	return true
}

func nativeTextDomainCount(meta CollectionMeta) int {
	count := 0
	for _, def := range meta.TextIndexes {
		if def.Version == TextIndexVersionV2 {
			count += 7
		} else {
			count += 3
		}
	}
	return count
}
