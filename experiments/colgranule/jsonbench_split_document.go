package colgranule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
)

var jsonBenchDeclaredColumnPaths = []string{
	"did",
	"time_us",
	"kind",
	"commit.operation",
	"commit.collection",
}

func JSONBenchDeclaredColumnPaths() []string {
	return append([]string(nil), jsonBenchDeclaredColumnPaths...)
}

type JSONBenchDeclaredColumnValues struct {
	TimeUS           int64  `json:"time_us"`
	Did              string `json:"did"`
	Kind             string `json:"kind"`
	CommitOperation  string `json:"commit_operation,omitempty"`
	CommitCollection string `json:"commit_collection,omitempty"`
}

func JSONBenchRetainedDocument(raw []byte) ([]byte, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("colgranule: decode JSONBench retained document: %w", err)
	}
	delete(doc, "did")
	delete(doc, "time_us")
	delete(doc, "kind")
	rawCommit, ok := doc["commit"]
	if ok {
		var commit map[string]json.RawMessage
		if err := json.Unmarshal(rawCommit, &commit); err != nil {
			return nil, fmt.Errorf("colgranule: decode JSONBench retained commit object: %w", err)
		}
		delete(commit, "operation")
		delete(commit, "collection")
		encodedCommit, err := json.Marshal(commit)
		if err != nil {
			return nil, fmt.Errorf("colgranule: encode JSONBench retained commit object: %w", err)
		}
		doc["commit"] = encodedCommit
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("colgranule: encode JSONBench retained document: %w", err)
	}
	return encoded, nil
}

func RestoreJSONBenchDeclaredColumns(doc map[string]any, values JSONBenchDeclaredColumnValues) {
	doc["did"] = values.Did
	doc["time_us"] = values.TimeUS
	doc["kind"] = values.Kind
	if values.Kind != "commit" && values.CommitOperation == "" && values.CommitCollection == "" {
		return
	}
	commit, ok := doc["commit"].(map[string]any)
	if !ok {
		commit = make(map[string]any, 2)
	}
	commit["operation"] = values.CommitOperation
	commit["collection"] = values.CommitCollection
	doc["commit"] = commit
}

func JSONBenchDeclaredColumnValuesFromPart(scanner *ColumnPartScanner, part *ColumnPart, dictionaries map[string]map[int64]string, primaryID int64) (JSONBenchDeclaredColumnValues, error) {
	if scanner == nil {
		return JSONBenchDeclaredColumnValues{}, fmt.Errorf("colgranule: nil column part scanner")
	}
	if part == nil {
		return JSONBenchDeclaredColumnValues{}, fmt.Errorf("colgranule: nil column part")
	}
	locator, ok := part.LocatePrimaryID(primaryID)
	if !ok {
		return JSONBenchDeclaredColumnValues{}, os.ErrNotExist
	}
	timeUS, err := scanner.ValueAt(locator, "time_us")
	if err != nil {
		return JSONBenchDeclaredColumnValues{}, err
	}
	did, err := jsonBenchDictionaryStringFromPart(scanner, locator, dictionaries, "did_code")
	if err != nil {
		return JSONBenchDeclaredColumnValues{}, err
	}
	kind, err := jsonBenchDictionaryStringFromPart(scanner, locator, dictionaries, "kind_code")
	if err != nil {
		return JSONBenchDeclaredColumnValues{}, err
	}
	operation, err := jsonBenchDictionaryStringFromPart(scanner, locator, dictionaries, "commit_operation_code")
	if err != nil {
		return JSONBenchDeclaredColumnValues{}, err
	}
	collection, err := jsonBenchDictionaryStringFromPart(scanner, locator, dictionaries, "commit_collection_code")
	if err != nil {
		return JSONBenchDeclaredColumnValues{}, err
	}
	return JSONBenchDeclaredColumnValues{
		TimeUS:           timeUS,
		Did:              did,
		Kind:             kind,
		CommitOperation:  operation,
		CommitCollection: collection,
	}, nil
}

func ReverseJSONBenchDictionaries(dictionaries map[string]map[string]int64) map[string]map[int64]string {
	out := make(map[string]map[int64]string, len(dictionaries))
	for name, values := range dictionaries {
		reversed := make(map[int64]string, len(values))
		for value, code := range values {
			reversed[code] = value
		}
		out[name] = reversed
	}
	return out
}

func DecodeJSONDocumentPreserveNumbers(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return normalizeJSONDocumentNumbers(v)
}

func jsonBenchDictionaryStringFromPart(scanner *ColumnPartScanner, locator RowLocator, dictionaries map[string]map[int64]string, column string) (string, error) {
	code, err := scanner.ValueAt(locator, column)
	if err != nil {
		return "", err
	}
	values := dictionaries[column]
	if values == nil {
		return "", fmt.Errorf("colgranule: missing JSONBench dictionary %s", column)
	}
	value, ok := values[code]
	if !ok {
		return "", fmt.Errorf("colgranule: missing JSONBench dictionary %s code %d", column, code)
	}
	return value, nil
}

func normalizeJSONDocumentNumbers(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			normalized, err := normalizeJSONDocumentNumberValue(value)
			if err != nil {
				return err
			}
			x[key] = normalized
		}
	case []any:
		for i, value := range x {
			normalized, err := normalizeJSONDocumentNumberValue(value)
			if err != nil {
				return err
			}
			x[i] = normalized
		}
	}
	return nil
}

func normalizeJSONDocumentNumberValue(value any) (any, error) {
	switch x := value.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, err
		}
		return f, nil
	case map[string]any:
		if err := normalizeJSONDocumentNumbers(x); err != nil {
			return nil, err
		}
		return x, nil
	case []any:
		if err := normalizeJSONDocumentNumbers(x); err != nil {
			return nil, err
		}
		return x, nil
	default:
		return value, nil
	}
}
