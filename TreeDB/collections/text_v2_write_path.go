package collections

import (
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

const (
	textV2DocumentPostingInlineTerms = 16
	textV2PostingBlockMutationIDBase = uint64(1) << 63
)

func textV2PostingBlockMutationBlockIDStart(generation uint64) (uint64, error) {
	if generation == 0 || generation >= textV2PostingBlockMutationIDBase {
		return 0, errMalformedTextStorage("text-v2 posting block mutation generation %d cannot be encoded in block id namespace", generation)
	}
	return textV2PostingBlockMutationIDBase | generation, nil
}

func textV2ScoringAnalysisDefinition(def TextIndexDefinition) TextIndexDefinition {
	def.StorePositions = false
	def.StoreOffsets = false
	return def
}

type textV2PostingBatchBuilder struct {
	fieldCount uint32
	byTerm     map[string][]textV2PostingBlockEntry
}

type textV2DocumentPostingTerm struct {
	Term             string
	TermFrequency    uint32
	FieldFrequencies []uint32
}

type textV2DocumentPostingAccumulator struct {
	fieldCount int
	inline     [textV2DocumentPostingInlineTerms]textV2DocumentPostingTerm
	terms      []textV2DocumentPostingTerm
	lookup     map[string]int
}

func newTextV2DocumentPostingAccumulator(fieldCount int) textV2DocumentPostingAccumulator {
	acc := textV2DocumentPostingAccumulator{fieldCount: fieldCount}
	acc.terms = acc.inline[:0]
	return acc
}

func (a *textV2DocumentPostingAccumulator) add(term string, fieldIndex int, frequency uint32) error {
	if frequency == 0 {
		return nil
	}
	if fieldIndex < 0 || fieldIndex >= a.fieldCount {
		return errMalformedTextStorage("text-v2 posting accumulator field index %d outside field count %d", fieldIndex, a.fieldCount)
	}
	idx := -1
	if a.lookup != nil {
		if found, ok := a.lookup[term]; ok {
			idx = found
		}
	} else {
		for i := range a.terms {
			if a.terms[i].Term == term {
				idx = i
				break
			}
		}
	}
	if idx < 0 {
		idx = len(a.terms)
		a.terms = append(a.terms, textV2DocumentPostingTerm{Term: term, FieldFrequencies: make([]uint32, a.fieldCount)})
		if a.lookup != nil {
			a.lookup[term] = idx
		} else if len(a.terms) > textV2DocumentPostingInlineTerms {
			a.lookup = make(map[string]int, len(a.terms)*2)
			for i := range a.terms {
				a.lookup[a.terms[i].Term] = i
			}
		}
	}
	entry := &a.terms[idx]
	entry.TermFrequency += frequency
	entry.FieldFrequencies[fieldIndex] += frequency
	return nil
}

func (b *textV2PostingBatchBuilder) addDocument(def TextIndexDefinition, ordinal, generation uint64, analysis textAnalyzedDocument) error {
	acc, err := b.beginDocument(def, ordinal, generation)
	if err != nil {
		return err
	}
	for _, field := range analysis.Fields {
		fieldIndex := textV2FieldIndex(def, field.Field)
		if fieldIndex < 0 {
			return errMalformedTextStorage("text-v2 posting field %q missing from definition", field.Field)
		}
		for _, term := range field.Terms {
			if term == nil {
				continue
			}
			if err := acc.add(term.Term, fieldIndex, term.Frequency); err != nil {
				return err
			}
		}
	}
	b.finishDocument(ordinal, generation, acc)
	return nil
}

func (b *textV2PostingBatchBuilder) addDocumentState(def TextIndexDefinition, ordinal, generation uint64, state textDocumentStateValue) error {
	acc, err := b.beginDocument(def, ordinal, generation)
	if err != nil {
		return err
	}
	for _, field := range state.Fields {
		fieldIndex := textV2FieldIndex(def, field.Field)
		if fieldIndex < 0 {
			return errMalformedTextStorage("text-v2 posting field %q missing from definition", field.Field)
		}
		for _, term := range field.Terms {
			if err := acc.add(term.Term, fieldIndex, term.Frequency); err != nil {
				return err
			}
		}
	}
	b.finishDocument(ordinal, generation, acc)
	return nil
}

func (b *textV2PostingBatchBuilder) beginDocument(def TextIndexDefinition, ordinal, generation uint64) (textV2DocumentPostingAccumulator, error) {
	if ordinal == 0 || generation == 0 {
		return textV2DocumentPostingAccumulator{}, errMalformedTextStorage("text-v2 posting document ordinal/generation cannot be zero")
	}
	fieldCount := len(def.Fields)
	if fieldCount == 0 || fieldCount > int(textV2PostingBlockMaxFieldCount) {
		return textV2DocumentPostingAccumulator{}, errMalformedTextStorage("text-v2 posting document field count %d invalid", fieldCount)
	}
	if b.fieldCount == 0 {
		b.fieldCount = uint32(fieldCount)
	} else if b.fieldCount != uint32(fieldCount) {
		return textV2DocumentPostingAccumulator{}, errMalformedTextStorage("text-v2 posting batch field count changed from %d to %d", b.fieldCount, fieldCount)
	}
	if b.byTerm == nil {
		b.byTerm = make(map[string][]textV2PostingBlockEntry)
	}
	return newTextV2DocumentPostingAccumulator(fieldCount), nil
}

func (b *textV2PostingBatchBuilder) finishDocument(ordinal, generation uint64, acc textV2DocumentPostingAccumulator) {
	for _, term := range acc.terms {
		if term.TermFrequency == 0 {
			continue
		}
		entry := textV2PostingBlockEntry{
			Ordinal:          ordinal,
			Generation:       generation,
			TermFrequency:    term.TermFrequency,
			FieldFrequencies: term.FieldFrequencies,
		}
		b.byTerm[term.Term] = append(b.byTerm[term.Term], entry)
	}
}

func (b *textV2PostingBatchBuilder) empty() bool {
	return b == nil || len(b.byTerm) == 0
}

func buildTextV2PostingBatchTable(table memtable.Table, builder *textV2PostingBatchBuilder, kind textV2PostingBlockKind, targetPostings uint32, blockIDStart uint64) (int, uint64, map[string]uint64, error) {
	if table == nil || builder == nil || len(builder.byTerm) == 0 {
		return 0, 0, nil, nil
	}
	terms := make([]string, 0, len(builder.byTerm))
	for term := range builder.byTerm {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	blockCounts := make(map[string]uint64, len(terms))
	var blocksWritten int
	var bytesWritten uint64
	for _, term := range terms {
		blocks, err := buildTextV2PostingBlockKVs(term, builder.byTerm[term], builder.fieldCount, textV2PostingBlockBuildOptions{Kind: kind, TargetPostings: targetPostings, BlockIDStart: blockIDStart, FixedBlockID: kind == textV2PostingBlockKindMicro})
		if err != nil {
			return 0, 0, nil, err
		}
		for _, kv := range blocks {
			table.SetSteal(kv.Key, kv.Value)
			blocksWritten++
			bytesWritten += uint64(len(kv.Key) + len(kv.Value))
		}
		if len(blocks) > 0 {
			blockCounts[term] = uint64(len(blocks))
		}
	}
	return blocksWritten, bytesWritten, blockCounts, nil
}

func textV2FieldIndex(def TextIndexDefinition, fieldName string) int {
	for i := range def.Fields {
		if def.Fields[i].Field == fieldName {
			return i
		}
	}
	return -1
}
