package typedkernel

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// DictionaryPredicateRequest is the concrete hot-loop input for one selected
// low-cardinality dictionary-code block. Codes are already dictionary IDs; this
// layer deliberately does not resolve or materialize strings per row.
type DictionaryPredicateRequest struct {
	Rows       int
	Selection  typedcolumn.RowSelection
	Granule    typedcolumn.EncodedGranule
	HasGranule bool
	Reader     *typedcolumn.GranuleReader
	Code       uint32
	Codes      []uint32
}

// CountDictionaryCode counts selected rows whose dictionary code equals Code.
func CountDictionaryCode(req DictionaryPredicateRequest) (int, error) {
	reader, err := dictionaryPredicateReader(req)
	if err != nil {
		return 0, err
	}
	return reader.CountUint32Code(req.Granule, req.Selection, req.Code)
}

// SelectDictionaryCode returns selected rows whose dictionary code equals Code.
func SelectDictionaryCode(req DictionaryPredicateRequest, scratch *Scratch) (typedcolumn.RowSelection, error) {
	reader, err := dictionaryPredicateReader(req)
	if err != nil {
		return typedcolumn.RowSelection{}, err
	}
	var local typedcolumn.Uint32CodeSelectionScratch
	codeScratch := &local
	if scratch != nil {
		codeScratch = &scratch.Dictionary
	}
	return reader.SelectUint32Code(req.Granule, req.Selection, req.Code, codeScratch)
}

// CountDictionaryCodesIn counts selected rows whose dictionary code is in
// Codes. Codes outside the granule cardinality are ignored as absent values.
func CountDictionaryCodesIn(req DictionaryPredicateRequest, scratch *Scratch) (int, error) {
	reader, err := dictionaryPredicateReader(req)
	if err != nil {
		return 0, err
	}
	var local typedcolumn.Uint32CodeSelectionScratch
	codeScratch := &local
	if scratch != nil {
		codeScratch = &scratch.Dictionary
	}
	return reader.CountUint32CodesIn(req.Granule, req.Selection, req.Codes, codeScratch)
}

// SelectDictionaryCodesIn returns selected rows whose dictionary code is in
// Codes. Codes outside the granule cardinality are ignored as absent values.
func SelectDictionaryCodesIn(req DictionaryPredicateRequest, scratch *Scratch) (typedcolumn.RowSelection, error) {
	reader, err := dictionaryPredicateReader(req)
	if err != nil {
		return typedcolumn.RowSelection{}, err
	}
	var local typedcolumn.Uint32CodeSelectionScratch
	codeScratch := &local
	if scratch != nil {
		codeScratch = &scratch.Dictionary
	}
	return reader.SelectUint32CodesIn(req.Granule, req.Selection, req.Codes, codeScratch)
}

func dictionaryPredicateReader(req DictionaryPredicateRequest) (*typedcolumn.GranuleReader, error) {
	rows, err := validateDictionaryPredicateRequest(req)
	if err != nil {
		return nil, err
	}
	if req.Granule.Rows != rows {
		return nil, fmt.Errorf("typedkernel: dictionary granule rows=%d want %d", req.Granule.Rows, rows)
	}
	reader := req.Reader
	if reader == nil {
		reader = &typedcolumn.GranuleReader{}
	}
	return reader, nil
}

func validateDictionaryPredicateRequest(req DictionaryPredicateRequest) (int, error) {
	if !req.HasGranule {
		return 0, fmt.Errorf("typedkernel: dictionary predicate requires code granule")
	}
	rows := req.Rows
	if rows == 0 && req.Selection.Rows() != 0 {
		rows = req.Selection.Rows()
	}
	if rows < 0 {
		return 0, fmt.Errorf("typedkernel: negative dictionary row domain %d", rows)
	}
	if req.Selection.Rows() != rows {
		return 0, fmt.Errorf("typedkernel: dictionary selection rows=%d want %d", req.Selection.Rows(), rows)
	}
	return rows, nil
}
