package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

const defaultDictVariants = "64k:24k:noent,64k:32k:noent,64k:40k:noent,64k:64k:noent,64k:96k:noent,64k:128k:noent,96k:40k:noent,96k:64k:noent,96k:96k:noent,96k:128k:noent,128k:24k:noent,128k:32k:noent,128k:40k:noent,128k:40k:ent,128k:64k:noent,128k:96k:noent,128k:128k:noent,192k:40k:noent,192k:64k:noent,192k:96k:noent,192k:128k:noent,256k:40k:noent,256k:64k:noent,256k:96k:noent,256k:128k:noent,256k:192k:noent,256k:256k:noent,512k:96k:noent,512k:128k:noent,512k:192k:noent,512k:256k:noent"

type kvRecord struct {
	Val      string `json:"val"`
	Encoding string `json:"encoding,omitempty"`
}

type dictVariant struct {
	Mode         string
	HistoryBytes int
	DictBytes    int
	NoEntropy    bool
}

type sweepRow struct {
	Mode          string  `json:"mode"`
	K             int     `json:"k"`
	Records       int     `json:"records"`
	RawBytes      int     `json:"raw_bytes"`
	TotalBytes    int     `json:"total_bytes"`
	TotalRatio    float64 `json:"total_ratio"`
	EncodeNsPerOp int64   `json:"encode_ns_per_op"`
	DecodeNsPerOp int64   `json:"decode_ns_per_op"`
}

type modeKind uint8

const (
	modeRaw modeKind = iota
	modeSnappy
	modeLZ4
	modeDict
)

type modeDef struct {
	Name      string
	Kind      modeKind
	Dict      []byte
	NoEntropy bool
}

func main() {
	input := flag.String("input", "", "Path to JSONL dataset with {val} records")
	inputEncoding := flag.String("input-encoding", "auto", "Input value encoding: auto|string|base64|hex (auto uses per-record encoding or defaults to string)")
	trainN := flag.Int("train", 15000, "Training sample count")
	evalN := flag.Int("eval", 5000, "Evaluation sample count")
	capBytes := flag.Int("cap", 0, "Maximum bytes kept per value (0 disables)")
	kList := flag.String("k", "1,2,4,8,16,32", "Comma-separated K values")
	levelName := flag.String("level", "fastest", "zstd encoder level: fastest|default")
	dictVariantsArg := flag.String("dict-variants", defaultDictVariants, "Comma-separated dict variants: h64k_s24k_noent or 64k:24k:noent")
	outPath := flag.String("out", "", "Output JSON path (required)")
	flag.Parse()

	if strings.TrimSpace(*input) == "" {
		failf("-input is required")
	}
	if strings.TrimSpace(*outPath) == "" {
		failf("-out is required")
	}
	if *trainN <= 0 || *evalN <= 0 {
		failf("-train and -eval must be > 0")
	}

	level := zstd.SpeedFastest
	switch strings.ToLower(strings.TrimSpace(*levelName)) {
	case "fastest":
		level = zstd.SpeedFastest
	case "default":
		level = zstd.SpeedDefault
	default:
		failf("unsupported -level=%q (expected fastest|default)", *levelName)
	}

	kValues, err := parseIntList(*kList)
	if err != nil {
		failf("parse -k: %v", err)
	}
	kValues = dedupeIntPreserve(kValues)
	if len(kValues) == 0 {
		failf("-k produced no values")
	}
	for _, k := range kValues {
		if k <= 0 || k > valuelog.MaxFrameK {
			failf("invalid K=%d (must be 1..%d)", k, valuelog.MaxFrameK)
		}
	}

	variants, err := parseDictVariants(*dictVariantsArg)
	if err != nil {
		failf("parse -dict-variants: %v", err)
	}

	train, eval, err := loadValues(*input, *trainN, *evalN, *capBytes, *inputEncoding)
	if err != nil {
		fail(err)
	}
	if len(train) < *trainN {
		failf("insufficient training samples: train=%d/%d", len(train), *trainN)
	}
	if len(eval) == 0 {
		failf("insufficient eval samples: eval=0/%d", *evalN)
	}
	if len(eval) < *evalN {
		fmt.Fprintf(os.Stderr, "warning: requested eval=%d, loaded eval=%d\n", *evalN, len(eval))
	}

	fmt.Fprintf(os.Stderr, "loaded: input=%s train=%d eval=%d variants=%d\n", *input, len(train), len(eval), len(variants))

	rawByHistory := make(map[int][]byte)
	dictByMode := make(map[string][]byte, len(variants))
	for _, v := range variants {
		raw := rawByHistory[v.HistoryBytes]
		if len(raw) == 0 {
			raw, err = buildRawDict(train, v.HistoryBytes, level)
			if err != nil {
				failf("train raw dict h=%s: %v", formatK(v.HistoryBytes), err)
			}
			rawByHistory[v.HistoryBytes] = raw
		}
		dict := fitDictSize(raw, v.DictBytes)
		if err := validateDict(dict, level, v.NoEntropy); err != nil {
			failf("validate dict %s: %v", v.Mode, err)
		}
		dictByMode[v.Mode] = dict
	}

	modes := make([]modeDef, 0, 3+len(variants))
	modes = append(modes,
		modeDef{Name: "raw", Kind: modeRaw},
		modeDef{Name: "snappy", Kind: modeSnappy},
		modeDef{Name: "lz4", Kind: modeLZ4},
	)
	for _, v := range variants {
		modes = append(modes, modeDef{Name: v.Mode, Kind: modeDict, Dict: dictByMode[v.Mode], NoEntropy: v.NoEntropy})
	}

	rows := make([]sweepRow, 0, len(modes)*len(kValues))
	for _, mode := range modes {
		fmt.Fprintf(os.Stderr, "evaluating mode=%s\n", mode.Name)
		for _, k := range kValues {
			row, err := evalModeK(mode, eval, k, level)
			if err != nil {
				failf("eval mode=%s k=%d: %v", mode.Name, k, err)
			}
			row.Mode = mode.Name
			rows = append(rows, row)
		}
	}

	if err := writeJSON(*outPath, rows); err != nil {
		fail(err)
	}
	fmt.Fprintf(os.Stderr, "wrote %d rows to %s\n", len(rows), *outPath)
}

func parseDictVariants(raw string) ([]dictVariant, error) {
	tokens := strings.Split(raw, ",")
	variants := make([]dictVariant, 0, len(tokens))
	seen := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		v, err := parseDictVariant(token)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[v.Mode]; ok {
			continue
		}
		seen[v.Mode] = struct{}{}
		variants = append(variants, v)
	}
	return variants, nil
}

func parseDictVariant(token string) (dictVariant, error) {
	if strings.HasPrefix(token, "dict_") {
		parts := strings.Split(token, "_")
		if len(parts) != 4 {
			return dictVariant{}, fmt.Errorf("invalid dict mode %q", token)
		}
		history, err := parseSizeToken(strings.TrimPrefix(parts[1], "h"))
		if err != nil {
			return dictVariant{}, fmt.Errorf("parse history in %q: %w", token, err)
		}
		dictBytes, err := parseSizeToken(strings.TrimPrefix(parts[2], "s"))
		if err != nil {
			return dictVariant{}, fmt.Errorf("parse dict bytes in %q: %w", token, err)
		}
		noEntropy, err := parseEntropyToken(parts[3])
		if err != nil {
			return dictVariant{}, fmt.Errorf("parse entropy in %q: %w", token, err)
		}
		return dictVariant{Mode: formatDictMode(history, dictBytes, noEntropy), HistoryBytes: history, DictBytes: dictBytes, NoEntropy: noEntropy}, nil
	}

	parts := strings.Split(token, ":")
	if len(parts) != 3 {
		return dictVariant{}, fmt.Errorf("invalid dict variant %q (expected dict_h64k_s24k_noent or 64k:24k:noent)", token)
	}
	history, err := parseSizeToken(parts[0])
	if err != nil {
		return dictVariant{}, fmt.Errorf("parse history in %q: %w", token, err)
	}
	dictBytes, err := parseSizeToken(parts[1])
	if err != nil {
		return dictVariant{}, fmt.Errorf("parse dict bytes in %q: %w", token, err)
	}
	noEntropy, err := parseEntropyToken(parts[2])
	if err != nil {
		return dictVariant{}, fmt.Errorf("parse entropy in %q: %w", token, err)
	}
	return dictVariant{Mode: formatDictMode(history, dictBytes, noEntropy), HistoryBytes: history, DictBytes: dictBytes, NoEntropy: noEntropy}, nil
}

func parseEntropyToken(s string) (bool, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "entropy_")
	s = strings.TrimPrefix(s, "frame_")
	s = strings.TrimPrefix(s, "with_")
	s = strings.TrimPrefix(s, "without_")
	s = strings.TrimPrefix(s, "no_")
	s = strings.TrimPrefix(s, "-")
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "entropy")
	s = strings.TrimPrefix(s, "_")
	s = strings.TrimPrefix(s, "-")
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "compression_")
	s = strings.TrimPrefix(s, "compression")
	s = strings.TrimPrefix(s, "_")
	s = strings.TrimSpace(s)
	switch s {
	case "noent", "off", "none", "no":
		return true, nil
	case "ent", "on", "yes":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported entropy token %q (expected noent|ent)", s)
	}
}

func formatDictMode(historyBytes, dictBytes int, noEntropy bool) string {
	ent := "ent"
	if noEntropy {
		ent = "noent"
	}
	return fmt.Sprintf("dict_h%s_s%s_%s", formatK(historyBytes), formatK(dictBytes), ent)
}

func formatK(n int) string {
	if n%(1<<10) == 0 {
		return fmt.Sprintf("%dk", n>>10)
	}
	return strconv.Itoa(n)
}

func parseSizeToken(s string) (int, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimSuffix(s, "ib")
	if s == "" {
		return 0, fmt.Errorf("empty size token")
	}
	mul := 1
	switch {
	case strings.HasSuffix(s, "k"):
		mul = 1 << 10
		s = strings.TrimSuffix(s, "k")
	case strings.HasSuffix(s, "m"):
		mul = 1 << 20
		s = strings.TrimSuffix(s, "m")
	case strings.HasSuffix(s, "g"):
		mul = 1 << 30
		s = strings.TrimSuffix(s, "g")
	}
	base, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, err
	}
	if base <= 0 {
		return 0, fmt.Errorf("size must be > 0")
	}
	return base * mul, nil
}

func dedupeIntPreserve(in []int) []int {
	out := make([]int, 0, len(in))
	seen := make(map[int]struct{}, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func parseIntList(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func loadValues(path string, trainN, evalN, capBytes int, inputEncoding string) ([][]byte, [][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	train := make([][]byte, 0, trainN)
	eval := make([][]byte, 0, evalN)

	reader := bufio.NewReaderSize(f, 1<<20)
	lineNum := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && readErr != io.EOF {
			return nil, nil, readErr
		}
		if len(line) > 0 {
			lineNum++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) == 0 {
				if readErr == io.EOF {
					break
				}
				continue
			}
			var rec kvRecord
			if err := json.Unmarshal(trimmed, &rec); err != nil {
				return nil, nil, fmt.Errorf("line %d: invalid json record: %w (snippet=%q)", lineNum, err, previewLine(trimmed, 160))
			}
			enc := resolveInputEncoding(inputEncoding, rec)
			val, err := decodeValue(rec.Val, enc)
			if err != nil {
				return nil, nil, fmt.Errorf("line %d: %w", lineNum, err)
			}
			if capBytes > 0 && len(val) > capBytes {
				val = val[:capBytes]
			}
			if len(val) > 0 {
				vv := make([]byte, len(val))
				copy(vv, val)
				if len(train) < trainN {
					train = append(train, vv)
				} else if len(eval) < evalN {
					eval = append(eval, vv)
				}
			}
		}
		if len(train) >= trainN && len(eval) >= evalN {
			break
		}
		if readErr == io.EOF {
			break
		}
	}
	return train, eval, nil
}

func resolveInputEncoding(flagValue string, rec kvRecord) string {
	enc := strings.ToLower(strings.TrimSpace(flagValue))
	if enc == "" || enc == "auto" {
		if recEnc := strings.ToLower(strings.TrimSpace(rec.Encoding)); recEnc != "" {
			return recEnc
		}
		return "string"
	}
	return enc
}

func previewLine(line []byte, max int) string {
	if max <= 0 {
		max = 1
	}
	s := strings.TrimSpace(string(line))
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func decodeValue(raw, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "string":
		return []byte(raw), nil
	case "base64":
		out, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
		if err != nil {
			return nil, err
		}
		return out, nil
	case "hex":
		out, err := hex.DecodeString(strings.TrimSpace(raw))
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported encoding %q", encoding)
	}
}

func buildRawDict(samples [][]byte, historyBytes int, level zstd.EncoderLevel) ([]byte, error) {
	history := make([]byte, 0, historyBytes)
	for _, sample := range samples {
		if len(history) >= historyBytes {
			break
		}
		need := historyBytes - len(history)
		if len(sample) > need {
			history = append(history, sample[:need]...)
		} else {
			history = append(history, sample...)
		}
	}
	if len(history) < 8 {
		return nil, fmt.Errorf("insufficient history bytes: got=%d", len(history))
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       1,
		Contents: samples,
		History:  history,
		Offsets:  [3]int{1, 4, 8},
		Level:    level,
	})
	if err != nil {
		return nil, err
	}
	if len(dict) == 0 {
		return nil, fmt.Errorf("empty dictionary")
	}
	return dict, nil
}

func fitDictSize(raw []byte, dictBytes int) []byte {
	if len(raw) == dictBytes {
		out := make([]byte, len(raw))
		copy(out, raw)
		return out
	}
	if len(raw) > dictBytes {
		out := make([]byte, dictBytes)
		copy(out, raw[:dictBytes])
		return out
	}
	out := make([]byte, dictBytes)
	copy(out, raw)
	return out
}

func validateDict(dict []byte, level zstd.EncoderLevel, noEntropy bool) error {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderDict(dict),
		zstd.WithEncoderConcurrency(1),
		zstd.WithNoEntropyCompression(noEntropy),
	)
	if err != nil {
		return err
	}
	defer enc.Close()

	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dict))
	if err != nil {
		return err
	}
	defer dec.Close()

	dummy := []byte("validation_payload")
	encoded := enc.EncodeAll(dummy, nil)
	decoded, err := dec.DecodeAll(encoded, nil)
	if err != nil {
		return err
	}
	if !bytes.Equal(dummy, decoded) {
		return fmt.Errorf("dictionary validation round-trip mismatch")
	}
	return nil
}

func evalModeK(mode modeDef, values [][]byte, k int, level zstd.EncoderLevel) (sweepRow, error) {
	n := (len(values) / k) * k
	row := sweepRow{K: k}
	if n == 0 {
		return row, nil
	}

	var (
		dictEnc *zstd.Encoder
		dictDec *zstd.Decoder
		err     error
	)
	if mode.Kind == modeDict {
		dictEnc, err = zstd.NewWriter(nil,
			zstd.WithEncoderDict(mode.Dict),
			zstd.WithEncoderLevel(level),
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderCRC(false),
			zstd.WithNoEntropyCompression(mode.NoEntropy),
		)
		if err != nil {
			return row, err
		}
		defer dictEnc.Close()

		dictDec, err = zstd.NewReader(nil, zstd.WithDecoderDicts(mode.Dict))
		if err != nil {
			return row, err
		}
		defer dictDec.Close()
	}

	var encodeNs int64
	var decodeNs int64

	for i := 0; i < n; i += k {
		group := values[i : i+k]
		raw := 0
		for _, v := range group {
			raw += len(v)
		}
		if raw == 0 {
			continue
		}
		payload := make([]byte, 0, raw)
		for _, v := range group {
			payload = append(payload, v...)
		}

		encStart := time.Now()
		stored, compressed, err := encodeForMode(mode, dictEnc, payload)
		encodeNs += time.Since(encStart).Nanoseconds()
		if err != nil {
			return row, err
		}

		decStart := time.Now()
		decoded, err := decodeForMode(mode, dictDec, payload, stored, compressed)
		decodeNs += time.Since(decStart).Nanoseconds()
		if err != nil {
			return row, err
		}
		if !bytes.Equal(decoded, payload) {
			return row, fmt.Errorf("round-trip mismatch")
		}

		row.Records += k
		row.RawBytes += raw
		meta := valuelog.FrameHeaderSize + (k * 8) + ((k + 1) * 4)
		total := valuelog.HeaderSize + meta + len(stored)
		row.TotalBytes += total
	}

	if row.RawBytes > 0 {
		row.TotalRatio = float64(row.TotalBytes) / float64(row.RawBytes)
	}
	if row.Records > 0 {
		row.EncodeNsPerOp = encodeNs / int64(row.Records)
		row.DecodeNsPerOp = decodeNs / int64(row.Records)
	}
	return row, nil
}

func encodeForMode(mode modeDef, dictEnc *zstd.Encoder, payload []byte) ([]byte, bool, error) {
	switch mode.Kind {
	case modeRaw:
		return payload, false, nil
	case modeSnappy:
		encoded := snappy.Encode(nil, payload)
		if len(encoded) >= len(payload) {
			return payload, false, nil
		}
		return encoded, true, nil
	case modeLZ4:
		bound := lz4.CompressBlockBound(len(payload))
		dst := make([]byte, bound)
		n, err := lz4.CompressBlock(payload, dst, nil)
		if err != nil {
			return nil, false, err
		}
		if n <= 0 || n >= len(payload) {
			return payload, false, nil
		}
		return dst[:n], true, nil
	case modeDict:
		encoded := dictEnc.EncodeAll(payload, nil)
		if len(encoded) >= len(payload) {
			return payload, false, nil
		}
		return encoded, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported mode")
	}
}

func decodeForMode(mode modeDef, dictDec *zstd.Decoder, raw []byte, stored []byte, compressed bool) ([]byte, error) {
	if !compressed {
		return stored, nil
	}
	switch mode.Kind {
	case modeSnappy:
		out, err := snappy.Decode(nil, stored)
		if err != nil {
			return nil, err
		}
		return out, nil
	case modeLZ4:
		dst := make([]byte, len(raw))
		n, err := lz4.UncompressBlock(stored, dst)
		if err != nil {
			return nil, err
		}
		if n != len(raw) {
			return nil, fmt.Errorf("lz4 decode size mismatch: got=%d want=%d", n, len(raw))
		}
		return dst, nil
	case modeDict:
		out, err := dictDec.DecodeAll(stored, nil)
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("decode unsupported for mode")
	}
}

func writeJSON(path string, rows []sweepRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func failf(format string, args ...any) {
	fail(fmt.Errorf(format, args...))
}
