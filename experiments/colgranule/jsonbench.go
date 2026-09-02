package colgranule

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const DefaultJSONBenchPath = "experiments/colgranule/testdata/jsonbench_sample.jsonl"
const DefaultJSONBenchDir = "experiments/colgranule/testdata"

const jsonBenchHoursPerDay = 24

type JSONBenchDataset struct {
	Rows         int
	Files        []string
	Columns      map[string][]int64
	Dictionaries map[string]map[string]int64
}

func LoadJSONBenchColumns(path string, limit int) (JSONBenchDataset, error) {
	if path == "" {
		path = DefaultJSONBenchDir
	}
	files, err := jsonBenchInputFiles(path)
	if err != nil {
		return JSONBenchDataset{}, err
	}
	ds := JSONBenchDataset{Columns: map[string][]int64{
		"row_index":                   nil,
		"time_us":                     nil,
		"hour_of_day":                 nil,
		"line_bytes":                  nil,
		"did_code":                    nil,
		"did_bytes":                   nil,
		"kind_code":                   nil,
		"commit_operation_code":       nil,
		"commit_collection_code":      nil,
		"commit_rev_bytes":            nil,
		"commit_rkey_bytes":           nil,
		"cid_bytes":                   nil,
		"record_type_code":            nil,
		"record_created_at_unix_ms":   nil,
		"record_text_bytes":           nil,
		"record_langs_count":          nil,
		"record_has_reply":            nil,
		"record_has_subject":          nil,
		"record_subject_string_bytes": nil,
	}}
	dicts := map[string]*stringDictionary{
		"did_code":               &stringDictionary{},
		"kind_code":              &stringDictionary{},
		"commit_operation_code":  &stringDictionary{},
		"commit_collection_code": &stringDictionary{},
		"record_type_code":       &stringDictionary{},
	}

	for _, file := range files {
		if limit > 0 && ds.Rows >= limit {
			break
		}
		if err := loadJSONBenchFile(file, limit, &ds, dicts); err != nil {
			return JSONBenchDataset{}, err
		}
		ds.Files = append(ds.Files, file)
	}
	ds.Dictionaries = freezeStringDictionaries(dicts)
	return ds, nil
}

func jsonBenchInputFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}

	var files []string
	for _, pattern := range []string{"*.json.gz", "*.jsonl.gz", "*.json", "*.jsonl"} {
		matches, err := filepath.Glob(filepath.Join(path, pattern))
		if err != nil {
			return nil, err
		}
		files = append(files, matches...)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no JSONBench input files found in %s", path)
	}
	return files, nil
}

func loadJSONBenchFile(path string, limit int, ds *JSONBenchDataset, dicts map[string]*stringDictionary) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var r io.Reader = f
	if filepath.Ext(path) == ".gz" {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		r = gz
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 256<<10), 8<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		var ev jsonBenchEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("decode jsonbench row %d in %s: %w", ds.Rows, path, err)
		}
		if err := appendJSONBenchRow(ds, dicts, line, &ev); err != nil {
			return fmt.Errorf("load jsonbench row %d in %s: %w", ds.Rows, path, err)
		}
		ds.Rows++
		if limit > 0 && ds.Rows >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func (d JSONBenchDataset) ColumnNames() []string {
	names := make([]string, 0, len(d.Columns))
	for name := range d.Columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type jsonBenchEvent struct {
	Did    string          `json:"did"`
	TimeUS int64           `json:"time_us"`
	Kind   string          `json:"kind"`
	Commit jsonBenchCommit `json:"commit"`
}

type jsonBenchCommit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     jsonBenchRecord `json:"record"`
	CID        string          `json:"cid"`
}

type jsonBenchRecord struct {
	Type      string           `json:"$type"`
	CreatedAt string           `json:"createdAt"`
	Text      string           `json:"text"`
	Langs     []string         `json:"langs"`
	Reply     *json.RawMessage `json:"reply"`
	Subject   json.RawMessage  `json:"subject"`
}

type stringDictionary struct {
	values map[string]int64
}

func (d *stringDictionary) code(s string) int64 {
	if d.values == nil {
		d.values = make(map[string]int64)
	}
	if code, ok := d.values[s]; ok {
		return code
	}
	code := int64(len(d.values) + 1)
	d.values[s] = code
	return code
}

func freezeStringDictionaries(dicts map[string]*stringDictionary) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(dicts))
	for name, dict := range dicts {
		values := make(map[string]int64, len(dict.values))
		for value, code := range dict.values {
			values[value] = code
		}
		out[name] = values
	}
	return out
}

func appendJSONBenchRow(ds *JSONBenchDataset, dicts map[string]*stringDictionary, line []byte, ev *jsonBenchEvent) error {
	row := int64(ds.Rows)
	commit := ev.Commit
	record := commit.Record
	createdAtMillis, err := parseRFC3339Millis(record.CreatedAt)
	if err != nil {
		return err
	}
	ds.Columns["row_index"] = append(ds.Columns["row_index"], row)
	ds.Columns["time_us"] = append(ds.Columns["time_us"], ev.TimeUS)
	ds.Columns["hour_of_day"] = append(ds.Columns["hour_of_day"], unixMicroHour(ev.TimeUS))
	ds.Columns["line_bytes"] = append(ds.Columns["line_bytes"], int64(len(line)))
	ds.Columns["did_code"] = append(ds.Columns["did_code"], dicts["did_code"].code(ev.Did))
	ds.Columns["did_bytes"] = append(ds.Columns["did_bytes"], int64(len(ev.Did)))
	ds.Columns["kind_code"] = append(ds.Columns["kind_code"], dicts["kind_code"].code(ev.Kind))
	ds.Columns["commit_operation_code"] = append(ds.Columns["commit_operation_code"], dicts["commit_operation_code"].code(commit.Operation))
	ds.Columns["commit_collection_code"] = append(ds.Columns["commit_collection_code"], dicts["commit_collection_code"].code(commit.Collection))
	ds.Columns["commit_rev_bytes"] = append(ds.Columns["commit_rev_bytes"], int64(len(commit.Rev)))
	ds.Columns["commit_rkey_bytes"] = append(ds.Columns["commit_rkey_bytes"], int64(len(commit.RKey)))
	ds.Columns["cid_bytes"] = append(ds.Columns["cid_bytes"], int64(len(commit.CID)))
	ds.Columns["record_type_code"] = append(ds.Columns["record_type_code"], dicts["record_type_code"].code(record.Type))
	ds.Columns["record_created_at_unix_ms"] = append(ds.Columns["record_created_at_unix_ms"], createdAtMillis)
	ds.Columns["record_text_bytes"] = append(ds.Columns["record_text_bytes"], int64(len(record.Text)))
	ds.Columns["record_langs_count"] = append(ds.Columns["record_langs_count"], int64(len(record.Langs)))
	ds.Columns["record_has_reply"] = append(ds.Columns["record_has_reply"], boolInt(record.Reply != nil))
	ds.Columns["record_has_subject"] = append(ds.Columns["record_has_subject"], boolInt(len(record.Subject) > 0 && !jsonRawNull(record.Subject)))
	ds.Columns["record_subject_string_bytes"] = append(ds.Columns["record_subject_string_bytes"], subjectStringBytes(record.Subject))
	return nil
}

func parseRFC3339Millis(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

func subjectStringBytes(raw json.RawMessage) int64 {
	if len(raw) == 0 || jsonRawNull(raw) {
		return 0
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return int64(len(s))
	}
	return 0
}

func jsonRawNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
