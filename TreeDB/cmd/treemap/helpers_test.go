package main

import (
	"bytes"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestResolveJSONLEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		inputEncoding  string
		recordEncoding string
		want           string
		wantErr        string
	}{
		{name: "explicit_string", inputEncoding: "string", recordEncoding: "hex", want: "string"},
		{name: "explicit_b64_alias", inputEncoding: "b64", recordEncoding: "", want: "base64"},
		{name: "auto_uses_record_raw", inputEncoding: "auto", recordEncoding: "raw", want: "string"},
		{name: "blank_input_blank_record_defaults_string", inputEncoding: "", recordEncoding: "", want: "string"},
		{name: "unsupported", inputEncoding: "wat", recordEncoding: "", wantErr: "unsupported encoding"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveJSONLEncoding(tc.inputEncoding, tc.recordEncoding)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveJSONLEncoding error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveJSONLEncoding = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestValidateScanJSONLEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		encoding     string
		omitEncoding bool
		want         string
		wantErr      string
	}{
		{name: "omit_string_ok", encoding: "string", omitEncoding: true, want: "string"},
		{name: "omit_raw_ok", encoding: "raw", omitEncoding: true, want: "string"},
		{name: "omit_base64_rejected", encoding: "base64", omitEncoding: true, wantErr: "-omit-encoding requires -encoding string"},
		{name: "omit_hex_rejected", encoding: "hex", omitEncoding: true, wantErr: "-omit-encoding requires -encoding string"},
		{name: "no_omit_base64_ok", encoding: "base64", omitEncoding: false, want: "base64"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateScanJSONLEncoding(tc.encoding, tc.omitEncoding)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateScanJSONLEncoding error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("validateScanJSONLEncoding = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDecodeJSONLValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    string
		encoding string
		want     []byte
		wantErr  string
	}{
		{name: "string", value: "abc", encoding: "string", want: []byte("abc")},
		{name: "base64", value: "AQI=", encoding: "base64", want: []byte{0x01, 0x02}},
		{name: "hex", value: "0a0b", encoding: "hex", want: []byte{0x0a, 0x0b}},
		{name: "invalid_base64", value: "***", encoding: "base64", wantErr: "invalid base64"},
		{name: "invalid_hex", value: "zz", encoding: "hex", wantErr: "invalid hex"},
		{name: "unsupported", value: "abc", encoding: "wat", wantErr: "unsupported encoding"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := decodeJSONLValue(tc.value, tc.encoding)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeJSONLValue error: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("decodeJSONLValue = %x, want %x", got, tc.want)
			}
		})
	}
}

func TestParseInputBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		hexInput bool
		want     []byte
		wantErr  string
	}{
		{name: "raw_string", input: "abc", hexInput: false, want: []byte("abc")},
		{name: "hex_no_prefix", input: "4142", hexInput: true, want: []byte("AB")},
		{name: "hex_0x_prefix", input: "0x4142", hexInput: true, want: []byte("AB")},
		{name: "hex_0X_prefix", input: "0X4142", hexInput: true, want: []byte("AB")},
		{name: "hex_invalid", input: "0XGG", hexInput: true, wantErr: "invalid byte"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseInputBytes(tc.input, tc.hexInput)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseInputBytes error: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("parseInputBytes = %x, want %x", got, tc.want)
			}
		})
	}
}

func TestFormatOutput(t *testing.T) {
	t.Parallel()

	input := []byte{0x41, 0x42, 0x01}
	tests := []struct {
		name    string
		mode    string
		want    string
		wantErr string
	}{
		{name: "string", mode: "string", want: "AB\x01"},
		{name: "hex", mode: "hex", want: "414201"},
		{name: "base64", mode: "base64", want: "QUIB"},
		{name: "unknown", mode: "wat", wantErr: "unknown output mode"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := formatOutput(input, tc.mode)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("formatOutput error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("formatOutput = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPrefixEnd(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input []byte
		want  []byte
	}{
		{name: "empty", input: nil, want: nil},
		{name: "increment_last", input: []byte{0x61, 0x62}, want: []byte{0x61, 0x63}},
		{name: "carry", input: []byte{0x12, 0xff}, want: []byte{0x13}},
		{name: "all_ff", input: []byte{0xff, 0xff}, want: nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			orig := append([]byte(nil), tc.input...)
			got := prefixEnd(tc.input)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("prefixEnd = %x, want %x", got, tc.want)
			}
			if !bytes.Equal(tc.input, orig) {
				t.Fatalf("prefixEnd modified input: got %x, want %x", tc.input, orig)
			}
		})
	}
}

func TestImportJSONLMissingRequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		line    string
		wantErr string
	}{
		{name: "missing_key", line: "{\"val\":\"v\",\"encoding\":\"string\"}\n", wantErr: `missing required field "key"`},
		{name: "missing_val", line: "{\"key\":\"k\",\"encoding\":\"string\"}\n", wantErr: `missing required field "val"`},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := treedb.Open(treedb.Options{Dir: dir})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			defer func() { _ = db.Close() }()

			count, err := importJSONL(db, strings.NewReader(tc.line), "auto", 0)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if count != 0 {
				t.Fatalf("count = %d, want 0", count)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
