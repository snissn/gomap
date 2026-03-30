package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	templ "github.com/snissn/gomap/TreeDB/template"
)

type corpusManifestSection struct {
	File    string `json:"file"`
	Records int    `json:"records"`
}

type corpusManifest struct {
	Pointer   corpusManifestSection `json:"pointer"`
	OuterLeaf corpusManifestSection `json:"outer_leaf"`
}

type runConfig struct {
	Mode                  string
	MinSavingsBytes       int
	FingerprintK          int
	MaxTemplateFetch      int
	OuterLeafPretransform string
	DisableMaskTemplates  bool
	TrainSampleStride     int
	SynthesizeEvery       int
	MinAnchorFreq         int
	MinPresenceRatio      float64
	MinPublishSavings     int
	MinPublishRatio       float64
	ColdSearchAfter       int
	ColdSearchProbeEvery  int
}

type runResult struct {
	Name               string            `json:"name"`
	Dataset            string            `json:"dataset"`
	Records            int               `json:"records"`
	RawBytes           int64             `json:"raw_bytes"`
	EncodedBytes       int64             `json:"encoded_bytes"`
	SavedBytes         int64             `json:"saved_bytes"`
	SavedRatio         float64           `json:"saved_ratio"`
	RawGzipBytes       int64             `json:"raw_gzip_bytes"`
	EncodedGzipBytes   int64             `json:"encoded_gzip_bytes"`
	EncodeNsPerByte    float64           `json:"encode_ns_per_byte"`
	DecodeNsPerByte    float64           `json:"decode_ns_per_byte"`
	Attempted          uint64            `json:"attempted"`
	Matched            uint64            `json:"matched"`
	Kept               uint64            `json:"kept"`
	TemplatesPublished uint64            `json:"templates_published"`
	Reasons            map[string]uint64 `json:"reasons,omitempty"`
	Config             runConfig         `json:"config"`
}

type report struct {
	GeneratedAt string      `json:"generated_at"`
	Results     []runResult `json:"results"`
}

type datasetSpec struct {
	Name    string
	Path    string
	Records [][]byte
}

const (
	outerLeafPretransformOff               = "off"
	outerLeafPretransformHeaderV1          = "header_v1"
	outerLeafPretransformHeaderDirDeltaV1  = "header_dir_delta_v1"
	outerLeafColumnarPrefixPackedLeafFlags = uint16(0xF004)
)

var (
	outerLeafPretransformMagicHeaderV1         = []byte{'T', 'L', 0x01}
	outerLeafPretransformMagicHeaderDirDeltaV1 = []byte{'T', 'L', 0x02}
)

type outerLeafColumnarPrefixLayout struct {
	count     int
	keyDirOff int
	valDirOff int
	flagsOff  int
	prefixOff int
	prefixEnd int
}

func detectOuterLeafColumnarPrefixLayout(page []byte) (outerLeafColumnarPrefixLayout, bool) {
	if len(page) < 16 {
		return outerLeafColumnarPrefixLayout{}, false
	}
	flags := binary.LittleEndian.Uint16(page[12:14])
	if flags != outerLeafColumnarPrefixPackedLeafFlags {
		return outerLeafColumnarPrefixLayout{}, false
	}
	count := int(binary.LittleEndian.Uint16(page[14:16]))
	if count <= 0 {
		return outerLeafColumnarPrefixLayout{}, false
	}
	keyDirOff := 16
	valDirOff := keyDirOff + count*2
	flagsOff := valDirOff + count*2
	prefixOff := flagsOff + count
	prefixEnd := prefixOff + count*2
	if prefixEnd > len(page) {
		return outerLeafColumnarPrefixLayout{}, false
	}
	keysBase := int(binary.LittleEndian.Uint16(page[keyDirOff : keyDirOff+2]))
	if keysBase < prefixEnd || keysBase > len(page) {
		return outerLeafColumnarPrefixLayout{}, false
	}
	return outerLeafColumnarPrefixLayout{
		count:     count,
		keyDirOff: keyDirOff,
		valDirOff: valDirOff,
		flagsOff:  flagsOff,
		prefixOff: prefixOff,
		prefixEnd: prefixEnd,
	}, true
}

func deltaEncodeU16LERegion(buf []byte, off, count int) {
	if count <= 1 || off < 0 || off+count*2 > len(buf) {
		return
	}
	for i := count - 1; i >= 1; i-- {
		curOff := off + i*2
		prevOff := off + (i-1)*2
		cur := binary.LittleEndian.Uint16(buf[curOff : curOff+2])
		prev := binary.LittleEndian.Uint16(buf[prevOff : prevOff+2])
		binary.LittleEndian.PutUint16(buf[curOff:curOff+2], cur-prev)
	}
}

func deltaDecodeU16LERegion(buf []byte, off, count int) {
	if count <= 1 || off < 0 || off+count*2 > len(buf) {
		return
	}
	for i := 1; i < count; i++ {
		curOff := off + i*2
		prevOff := off + (i-1)*2
		cur := binary.LittleEndian.Uint16(buf[curOff : curOff+2])
		prev := binary.LittleEndian.Uint16(buf[prevOff : prevOff+2])
		binary.LittleEndian.PutUint16(buf[curOff:curOff+2], cur+prev)
	}
}

func applyOuterLeafPretransform(value []byte, mode string) []byte {
	if mode == outerLeafPretransformOff {
		return value
	}
	if len(value) < 12 {
		return value
	}
	page := append([]byte(nil), value...)
	switch mode {
	case outerLeafPretransformHeaderV1:
		out := make([]byte, 0, len(outerLeafPretransformMagicHeaderV1)+12+len(value))
		out = append(out, outerLeafPretransformMagicHeaderV1...)
		out = append(out, value[:12]...)
		for i := 0; i < 12; i++ {
			page[i] = 0
		}
		out = append(out, page...)
		return out
	case outerLeafPretransformHeaderDirDeltaV1:
		out := make([]byte, 0, len(outerLeafPretransformMagicHeaderDirDeltaV1)+12+len(value))
		out = append(out, outerLeafPretransformMagicHeaderDirDeltaV1...)
		out = append(out, value[:12]...)
		for i := 0; i < 12; i++ {
			page[i] = 0
		}
		if layout, ok := detectOuterLeafColumnarPrefixLayout(page); ok {
			deltaEncodeU16LERegion(page, layout.keyDirOff, layout.count)
			deltaEncodeU16LERegion(page, layout.valDirOff, layout.count)
			deltaEncodeU16LERegion(page, layout.prefixOff, layout.count)
		}
		out = append(out, page...)
		return out
	default:
		return value
	}
}

func reverseHeaderSideTransform(payload []byte, magic []byte) ([]byte, bool, error) {
	if len(payload) < len(magic) {
		return payload, false, nil
	}
	if !bytes.Equal(payload[:len(magic)], magic) {
		return payload, false, nil
	}
	if len(payload) < len(magic)+12 {
		return nil, false, errors.New("pretransform: payload too short")
	}
	header := payload[len(magic) : len(magic)+12]
	page := append([]byte(nil), payload[len(magic)+12:]...)
	if len(page) < 12 {
		return nil, false, errors.New("pretransform: transformed page too short")
	}
	copy(page[:12], header)
	return page, true, nil
}

func reverseOuterLeafPretransform(payload []byte, mode string) ([]byte, error) {
	switch mode {
	case outerLeafPretransformOff:
		return payload, nil
	case outerLeafPretransformHeaderV1:
		page, _, err := reverseHeaderSideTransform(payload, outerLeafPretransformMagicHeaderV1)
		return page, err
	case outerLeafPretransformHeaderDirDeltaV1:
		page, transformed, err := reverseHeaderSideTransform(payload, outerLeafPretransformMagicHeaderDirDeltaV1)
		if err != nil || !transformed {
			return page, err
		}
		if layout, ok := detectOuterLeafColumnarPrefixLayout(page); ok {
			deltaDecodeU16LERegion(page, layout.keyDirOff, layout.count)
			deltaDecodeU16LERegion(page, layout.valDirOff, layout.count)
			deltaDecodeU16LERegion(page, layout.prefixOff, layout.count)
		}
		return page, nil
	default:
		return payload, nil
	}
}

type memTemplateStore struct {
	mu      sync.RWMutex
	defs    map[uint64][]byte
	decoded map[uint64]templ.TemplateDef
	routes  map[uint64]map[uint64]int
}

func newMemTemplateStore() *memTemplateStore {
	return &memTemplateStore{
		defs:    make(map[uint64][]byte),
		decoded: make(map[uint64]templ.TemplateDef),
		routes:  make(map[uint64]map[uint64]int),
	}
}

func (s *memTemplateStore) GetCandidates(_ context.Context, fp uint64, max int) ([]templ.Candidate, error) {
	s.mu.RLock()
	route := s.routes[fp]
	if len(route) == 0 {
		s.mu.RUnlock()
		return nil, nil
	}
	ids := make([]uint64, 0, len(route))
	for id := range route {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if max > 0 && len(ids) > max {
		ids = ids[:max]
	}
	out := make([]templ.Candidate, 0, len(ids))
	s.mu.RLock()
	for _, id := range ids {
		out = append(out, templ.Candidate{ID: id, Size: len(s.defs[id])})
	}
	s.mu.RUnlock()
	return out, nil
}

func (s *memTemplateStore) GetTemplateDef(_ context.Context, templateID uint64) ([]byte, error) {
	s.mu.RLock()
	def, ok := s.defs[templateID]
	s.mu.RUnlock()
	if !ok {
		return nil, templ.ErrMissingTemplate
	}
	return append([]byte(nil), def...), nil
}

func (s *memTemplateStore) PutTemplateDef(_ context.Context, defBytes []byte, routeFPs []uint64) (uint64, error) {
	id := templ.TemplateID(defBytes, 0)
	defCopy := append([]byte(nil), defBytes...)
	decoded, err := templ.DecodeTemplateDef(defCopy)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.defs[id] = defCopy
	s.decoded[id] = decoded
	for _, fp := range routeFPs {
		if s.routes[fp] == nil {
			s.routes[fp] = make(map[uint64]int)
		}
		s.routes[fp][id] = len(defBytes)
	}
	s.mu.Unlock()
	return id, nil
}

func (s *memTemplateStore) lookupTemplate(id uint64) (templ.TemplateDef, error) {
	s.mu.RLock()
	def, ok := s.decoded[id]
	s.mu.RUnlock()
	if !ok {
		return templ.TemplateDef{}, templ.ErrMissingTemplate
	}
	return def, nil
}

type countingWriter struct {
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

type gzipRecordSizer struct {
	cw *countingWriter
	gw *gzip.Writer
}

func newGzipRecordSizer() *gzipRecordSizer {
	cw := &countingWriter{}
	gw, err := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	if err != nil {
		log.Printf("gzip.NewWriterLevel(%d) failed: %v; falling back to default level", gzip.BestSpeed, err)
		gw = gzip.NewWriter(cw)
	}
	return &gzipRecordSizer{cw: cw, gw: gw}
}

func (s *gzipRecordSizer) WriteRecord(payload []byte) error {
	if s == nil || s.gw == nil {
		return errors.New("nil gzip sizer")
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("record too large: %d", len(payload))
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := s.gw.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := s.gw.Write(payload)
	return err
}

func (s *gzipRecordSizer) Close() (int64, error) {
	if s == nil || s.gw == nil {
		return 0, nil
	}
	if err := s.gw.Close(); err != nil {
		return 0, err
	}
	return s.cw.n, nil
}

func readCorpusFile(path string, maxRecords int) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	out := make([][]byte, 0, 1024)
	for {
		if maxRecords > 0 && len(out) >= maxRecords {
			break
		}
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("truncated record header in %s", path)
			}
			return nil, err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if uint64(n) > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("record too large for platform int: %d", n)
		}
		buf := make([]byte, int(n))
		if n > 0 {
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
		}
		out = append(out, buf)
	}
	return out, nil
}

func parseIntList(raw string, fallback []int) ([]int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid int list %q: %w", raw, err)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return fallback, nil
	}
	sort.Ints(out)
	uniq := out[:0]
	prev := -1
	for _, n := range out {
		if len(uniq) == 0 || n != prev {
			uniq = append(uniq, n)
			prev = n
		}
	}
	return uniq, nil
}

func parseUintStat(stats map[string]string, key string) uint64 {
	if len(stats) == 0 {
		return 0
	}
	raw, ok := stats[key]
	if !ok {
		return 0
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseReasonStats(stats map[string]string) map[string]uint64 {
	if len(stats) == 0 {
		return nil
	}
	out := make(map[string]uint64)
	for key, value := range stats {
		if !strings.HasPrefix(key, "reason.") {
			continue
		}
		reason := strings.TrimPrefix(key, "reason.")
		if reason == "" {
			continue
		}
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil || n == 0 {
			continue
		}
		out[reason] = n
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func saturatingSub(curr, base uint64) uint64 {
	if curr <= base {
		return 0
	}
	return curr - base
}

func diffReasonStats(curr, base map[string]uint64) map[string]uint64 {
	if len(curr) == 0 {
		return nil
	}
	out := make(map[string]uint64)
	for reason, currCount := range curr {
		baseCount := uint64(0)
		if base != nil {
			baseCount = base[reason]
		}
		if currCount > baseCount {
			out[reason] = currCount - baseCount
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func waitForTrainer(engine *templ.Engine, maxWait time.Duration) {
	if engine == nil || maxWait <= 0 {
		return
	}
	deadline := time.Now().Add(maxWait)
	stable := 0
	var prevDefs uint64
	for {
		stats := engine.StatsSnapshot()
		enq := parseUintStat(stats, "train_enqueued_total")
		proc := parseUintStat(stats, "train_processed_total")
		defs := parseUintStat(stats, "publish_defs_total")
		if enq == proc && defs == prevDefs {
			stable++
		} else {
			stable = 0
		}
		prevDefs = defs
		if stable >= 4 || time.Now().After(deadline) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func buildRunName(cfg runConfig) string {
	suffix := ""
	if cfg.OuterLeafPretransform != "" && cfg.OuterLeafPretransform != outerLeafPretransformOff {
		suffix = "_pt_" + cfg.OuterLeafPretransform
	}
	if cfg.DisableMaskTemplates {
		suffix += "_nomask"
	}
	if cfg.Mode == "off" {
		return "off" + suffix
	}
	return fmt.Sprintf("tmpl_ms%d_fk%d_fetch%d%s", cfg.MinSavingsBytes, cfg.FingerprintK, cfg.MaxTemplateFetch, suffix)
}

func runTemplateLab(ds datasetSpec, cfg runConfig, warmupPasses, measurePasses int, waitAfterWarmup time.Duration) (runResult, error) {
	res := runResult{
		Name:    buildRunName(cfg),
		Dataset: ds.Name,
		Config:  cfg,
	}
	if len(ds.Records) == 0 {
		return res, nil
	}

	store := newMemTemplateStore()
	tcfg := templ.NormalizeConfig(templ.Config{})
	if cfg.Mode != "off" {
		tcfg.MinSavingsBytes = cfg.MinSavingsBytes
		if cfg.FingerprintK > 0 {
			tcfg.FingerprintK = cfg.FingerprintK
		}
		if cfg.MaxTemplateFetch > 0 {
			tcfg.MaxTemplateFetch = cfg.MaxTemplateFetch
		}
		tcfg.DisableMaskTemplates = cfg.DisableMaskTemplates
		if cfg.TrainSampleStride > 0 {
			tcfg.TrainSampleStride = cfg.TrainSampleStride
		}
		if cfg.SynthesizeEvery > 0 {
			tcfg.SynthesizeEverySamples = cfg.SynthesizeEvery
		}
		if cfg.MinAnchorFreq > 0 {
			tcfg.MinAnchorFreq = cfg.MinAnchorFreq
		}
		if cfg.MinPresenceRatio > 0 {
			tcfg.MinPresenceRatio = cfg.MinPresenceRatio
		}
		if cfg.MinPublishSavings > 0 {
			tcfg.MinPublishSavingsBytes = cfg.MinPublishSavings
		}
		if cfg.MinPublishRatio > 0 {
			tcfg.MinPublishRatio = cfg.MinPublishRatio
		}
		if cfg.ColdSearchAfter > 0 {
			tcfg.ColdSearchAfter = cfg.ColdSearchAfter
		}
		if cfg.ColdSearchProbeEvery > 0 {
			tcfg.ColdSearchProbeEvery = cfg.ColdSearchProbeEvery
		}
	}
	engine := templ.NewEngine(tcfg)
	defer engine.Close()

	encodeValue := func(value []byte) ([]byte, bool) {
		if cfg.Mode == "off" {
			return value, false
		}
		return engine.Encode(nil, value, store)
	}
	transformValue := func(value []byte) []byte {
		if ds.Name != "outer_leaf" {
			return value
		}
		return applyOuterLeafPretransform(value, cfg.OuterLeafPretransform)
	}

	if warmupPasses < 0 {
		warmupPasses = 0
	}
	for pass := 0; pass < warmupPasses; pass++ {
		for _, value := range ds.Records {
			_, _ = encodeValue(transformValue(value))
		}
	}
	waitForTrainer(engine, waitAfterWarmup)
	baselineStats := engine.StatsSnapshot()
	baselineAttempted := parseUintStat(baselineStats, "attempted")
	baselineMatched := parseUintStat(baselineStats, "matched")
	baselineKept := parseUintStat(baselineStats, "kept")
	baselinePublished := parseUintStat(baselineStats, "templates_published_total")
	baselineReasons := parseReasonStats(baselineStats)

	if measurePasses <= 0 {
		measurePasses = 1
	}
	rawSizer := newGzipRecordSizer()
	encSizer := newGzipRecordSizer()
	var encodeNS int64
	var decodeNS int64
	decodeOpts := templ.DecodeOptions{MaxDecodedBytes: tcfg.MaxDecodedBytes, MaxGaps: tcfg.MaxGaps, DefCacheSize: tcfg.DefCacheSize}

	for pass := 0; pass < measurePasses; pass++ {
		for idx, value := range ds.Records {
			workingValue := transformValue(value)
			res.Records++
			res.RawBytes += int64(len(workingValue))
			if err := rawSizer.WriteRecord(workingValue); err != nil {
				return res, err
			}

			// Keep dataset records immutable across modes/runs even if an
			// encoder path regresses and writes into the input slice.
			encodeInput := append([]byte(nil), workingValue...)
			encodeStart := time.Now()
			payload, encoded := encodeValue(encodeInput)
			encodeNS += time.Since(encodeStart).Nanoseconds()
			res.EncodedBytes += int64(len(payload))
			if err := encSizer.WriteRecord(payload); err != nil {
				return res, err
			}

			decodeStart := time.Now()
			var decoded []byte
			if encoded {
				var err error
				decoded, err = templ.DecodePayloadAppend(nil, payload, store.lookupTemplate, decodeOpts)
				if err != nil {
					decodeNS += time.Since(decodeStart).Nanoseconds()
					return res, fmt.Errorf("decode failed: %w", err)
				}
			} else {
				decoded = payload
			}
			decodeNS += time.Since(decodeStart).Nanoseconds()
			recovered := decoded
			if ds.Name == "outer_leaf" {
				var err error
				recovered, err = reverseOuterLeafPretransform(decoded, cfg.OuterLeafPretransform)
				if err != nil {
					return res, fmt.Errorf("reverse pretransform failed: %w", err)
				}
			}
			if !bytes.Equal(recovered, value) {
				preview := payload
				if len(preview) > 16 {
					preview = preview[:16]
				}
				return res, fmt.Errorf("decode mismatch pass=%d idx=%d encoded=%v raw_len=%d payload_len=%d payload_prefix=%x",
					pass, idx, encoded, len(value), len(payload), preview)
			}
		}
	}

	rawGzip, err := rawSizer.Close()
	if err != nil {
		return res, err
	}
	encGzip, err := encSizer.Close()
	if err != nil {
		return res, err
	}
	res.RawGzipBytes = rawGzip
	res.EncodedGzipBytes = encGzip

	if res.RawBytes > 0 {
		res.SavedBytes = res.RawBytes - res.EncodedBytes
		res.SavedRatio = float64(res.SavedBytes) / float64(res.RawBytes)
		res.EncodeNsPerByte = float64(encodeNS) / float64(res.RawBytes)
		res.DecodeNsPerByte = float64(decodeNS) / float64(res.RawBytes)
	}

	// Publish runs asynchronously in trainer workers; wait before reading
	// final published/matched counters for measured output.
	waitForTrainer(engine, waitAfterWarmup)
	stats := engine.StatsSnapshot()
	res.Attempted = saturatingSub(parseUintStat(stats, "attempted"), baselineAttempted)
	res.Matched = saturatingSub(parseUintStat(stats, "matched"), baselineMatched)
	res.Kept = saturatingSub(parseUintStat(stats, "kept"), baselineKept)
	res.TemplatesPublished = saturatingSub(parseUintStat(stats, "templates_published_total"), baselinePublished)
	res.Reasons = diffReasonStats(parseReasonStats(stats), baselineReasons)
	return res, nil
}

func writeJSON(path string, rep report) error {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

func writeMarkdown(path string, rep report) error {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, _ = fmt.Fprintln(f, "| Name | Dataset | Records | Raw Bytes | Encoded Bytes | Saved Ratio | Raw Gzip | Enc Gzip | Encode ns/B | Decode ns/B | Kept/Attempted |")
	_, _ = fmt.Fprintln(f, "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, r := range rep.Results {
		kept := "-"
		if r.Attempted > 0 {
			kept = fmt.Sprintf("%d/%d", r.Kept, r.Attempted)
		}
		_, _ = fmt.Fprintf(f, "| %s | %s | %d | %d | %d | %.4f | %d | %d | %.2f | %.2f | %s |\n",
			r.Name,
			r.Dataset,
			r.Records,
			r.RawBytes,
			r.EncodedBytes,
			r.SavedRatio,
			r.RawGzipBytes,
			r.EncodedGzipBytes,
			r.EncodeNsPerByte,
			r.DecodeNsPerByte,
			kept,
		)
	}
	return nil
}

func loadManifest(path string) (corpusManifest, error) {
	var m corpusManifest
	data, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(data, &m)
	return m, err
}

func datasetOrder(name string) int {
	switch name {
	case "pointer":
		return 0
	case "outer_leaf":
		return 1
	default:
		return 2
	}
}

func main() {
	corpusDir := flag.String("corpus-dir", "", "directory containing pointer_values.bin, outer_leaf_pages.bin, manifest.json")
	pointerFile := flag.String("pointer-file", "", "path to pointer_values.bin")
	outerLeafFile := flag.String("outer-leaf-file", "", "path to outer_leaf_pages.bin")
	datasetMode := flag.String("dataset", "both", "dataset mode: pointer|outer_leaf|both")
	maxRecords := flag.Int("max-records", 0, "max records per dataset (0=all)")
	warmupPasses := flag.Int("warmup-passes", 1, "warmup encode passes before measured pass")
	measurePasses := flag.Int("measure-passes", 1, "measured passes")
	waitAfterWarmupMs := flag.Int("wait-after-warmup-ms", 3000, "max wait for async trainer before measured pass")
	sweepMinSavings := flag.String("sweep-min-savings", "1,4,8", "comma-separated MinSavingsBytes sweep")
	sweepFingerprintK := flag.String("sweep-fingerprint-k", "8", "comma-separated FingerprintK sweep")
	sweepMaxFetch := flag.String("sweep-max-fetch", "8,16", "comma-separated MaxTemplateFetch sweep")
	outerLeafPretransform := flag.String("outer-leaf-pretransform", outerLeafPretransformOff, "outer leaf pretransform: off|header_v1|header_dir_delta_v1")
	disableMaskTemplates := flag.Bool("disable-mask-templates", false, "disable mask templates (anchor-only template mode)")
	trainSampleStride := flag.Int("template-train-sample-stride", 0, "template training sample stride override (0=default)")
	synthesizeEvery := flag.Int("template-synthesize-every", 0, "template synthesize-every override (0=default)")
	minAnchorFreq := flag.Int("template-min-anchor-freq", 0, "template min-anchor-freq override (0=default)")
	minPresenceRatio := flag.Float64("template-min-presence-ratio", 0, "template min-presence-ratio override (0=default)")
	minPublishSavings := flag.Int("template-min-publish-savings", 0, "template min-publish-savings-bytes override (0=default)")
	minPublishRatio := flag.Float64("template-min-publish-ratio", 0, "template min-publish-ratio override (0=default)")
	coldSearchAfter := flag.Int("template-cold-search-after", 0, "template cold-search-after override (0=default)")
	coldSearchProbeEvery := flag.Int("template-cold-search-probe-every", 0, "template cold-search-probe-every override (0=default)")
	includeOff := flag.Bool("include-off", true, "include off baseline")
	outJSON := flag.String("out-json", "", "optional JSON report path")
	outMD := flag.String("out-md", "", "optional markdown report path")
	flag.Parse()
	pointerFileExplicit := strings.TrimSpace(*pointerFile) != ""
	outerLeafFileExplicit := strings.TrimSpace(*outerLeafFile) != ""

	switch strings.ToLower(strings.TrimSpace(*outerLeafPretransform)) {
	case outerLeafPretransformOff, outerLeafPretransformHeaderV1, outerLeafPretransformHeaderDirDeltaV1:
	default:
		log.Fatalf("invalid -outer-leaf-pretransform=%q (expected off|header_v1|header_dir_delta_v1)", *outerLeafPretransform)
	}

	if *corpusDir != "" {
		if *pointerFile == "" {
			*pointerFile = filepath.Join(*corpusDir, "pointer_values.bin")
		}
		if *outerLeafFile == "" {
			*outerLeafFile = filepath.Join(*corpusDir, "outer_leaf_pages.bin")
		}
		manifestPath := filepath.Join(*corpusDir, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			if manifest, err := loadManifest(manifestPath); err == nil {
				if !pointerFileExplicit && manifest.Pointer.File != "" {
					*pointerFile = filepath.Join(*corpusDir, manifest.Pointer.File)
				}
				if !outerLeafFileExplicit && manifest.OuterLeaf.File != "" {
					*outerLeafFile = filepath.Join(*corpusDir, manifest.OuterLeaf.File)
				}
			}
		}
	}

	datasets := make([]datasetSpec, 0, 2)
	switch strings.ToLower(strings.TrimSpace(*datasetMode)) {
	case "pointer":
		datasets = append(datasets, datasetSpec{Name: "pointer", Path: *pointerFile})
	case "outer_leaf":
		datasets = append(datasets, datasetSpec{Name: "outer_leaf", Path: *outerLeafFile})
	case "both":
		datasets = append(datasets,
			datasetSpec{Name: "pointer", Path: *pointerFile},
			datasetSpec{Name: "outer_leaf", Path: *outerLeafFile},
		)
	default:
		log.Fatalf("invalid -dataset=%q (expected pointer|outer_leaf|both)", *datasetMode)
	}

	loaded := make([]datasetSpec, 0, len(datasets))
	for _, ds := range datasets {
		if strings.TrimSpace(ds.Path) == "" {
			log.Fatalf("dataset %s has empty path", ds.Name)
		}
		records, err := readCorpusFile(ds.Path, *maxRecords)
		if err != nil {
			log.Fatalf("read corpus %s: %v", ds.Path, err)
		}
		ds.Records = records
		loaded = append(loaded, ds)
	}

	minSavingsList, err := parseIntList(*sweepMinSavings, []int{1})
	if err != nil {
		log.Fatalf("parse -sweep-min-savings: %v", err)
	}
	fingerprintKList, err := parseIntList(*sweepFingerprintK, []int{8})
	if err != nil {
		log.Fatalf("parse -sweep-fingerprint-k: %v", err)
	}
	maxFetchList, err := parseIntList(*sweepMaxFetch, []int{8})
	if err != nil {
		log.Fatalf("parse -sweep-max-fetch: %v", err)
	}

	configs := make([]runConfig, 0, len(minSavingsList)*len(fingerprintKList)*len(maxFetchList)+1)
	pretransformMode := strings.ToLower(strings.TrimSpace(*outerLeafPretransform))
	tuning := runConfig{
		TrainSampleStride:    *trainSampleStride,
		SynthesizeEvery:      *synthesizeEvery,
		MinAnchorFreq:        *minAnchorFreq,
		MinPresenceRatio:     *minPresenceRatio,
		MinPublishSavings:    *minPublishSavings,
		MinPublishRatio:      *minPublishRatio,
		ColdSearchAfter:      *coldSearchAfter,
		ColdSearchProbeEvery: *coldSearchProbeEvery,
	}
	if *includeOff {
		configs = append(configs, runConfig{
			Mode:                  "off",
			OuterLeafPretransform: pretransformMode,
			DisableMaskTemplates:  *disableMaskTemplates,
			TrainSampleStride:     tuning.TrainSampleStride,
			SynthesizeEvery:       tuning.SynthesizeEvery,
			MinAnchorFreq:         tuning.MinAnchorFreq,
			MinPresenceRatio:      tuning.MinPresenceRatio,
			MinPublishSavings:     tuning.MinPublishSavings,
			MinPublishRatio:       tuning.MinPublishRatio,
			ColdSearchAfter:       tuning.ColdSearchAfter,
			ColdSearchProbeEvery:  tuning.ColdSearchProbeEvery,
		})
	}
	for _, minSavings := range minSavingsList {
		for _, fk := range fingerprintKList {
			for _, fetch := range maxFetchList {
				configs = append(configs, runConfig{
					Mode:                  "template",
					MinSavingsBytes:       minSavings,
					FingerprintK:          fk,
					MaxTemplateFetch:      fetch,
					OuterLeafPretransform: pretransformMode,
					DisableMaskTemplates:  *disableMaskTemplates,
					TrainSampleStride:     tuning.TrainSampleStride,
					SynthesizeEvery:       tuning.SynthesizeEvery,
					MinAnchorFreq:         tuning.MinAnchorFreq,
					MinPresenceRatio:      tuning.MinPresenceRatio,
					MinPublishSavings:     tuning.MinPublishSavings,
					MinPublishRatio:       tuning.MinPublishRatio,
					ColdSearchAfter:       tuning.ColdSearchAfter,
					ColdSearchProbeEvery:  tuning.ColdSearchProbeEvery,
				})
			}
		}
	}

	results := make([]runResult, 0, len(configs)*len(loaded))
	for _, ds := range loaded {
		for _, cfg := range configs {
			res, err := runTemplateLab(ds, cfg, *warmupPasses, *measurePasses, time.Duration(*waitAfterWarmupMs)*time.Millisecond)
			if err != nil {
				log.Fatalf("run failed dataset=%s cfg=%s: %v", ds.Name, buildRunName(cfg), err)
			}
			results = append(results, res)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		di := datasetOrder(results[i].Dataset)
		dj := datasetOrder(results[j].Dataset)
		if di != dj {
			return di < dj
		}
		if results[i].Name != results[j].Name {
			return results[i].Name < results[j].Name
		}
		return results[i].Records < results[j].Records
	})

	rep := report{GeneratedAt: time.Now().UTC().Format(time.RFC3339), Results: results}
	if err := writeJSON(*outJSON, rep); err != nil {
		log.Fatalf("write json: %v", err)
	}
	if err := writeMarkdown(*outMD, rep); err != nil {
		log.Fatalf("write markdown: %v", err)
	}

	for _, res := range results {
		fmt.Printf("template-lab: name=%s dataset=%s records=%d raw_bytes=%d encoded_bytes=%d saved_ratio=%.4f raw_gzip=%d encoded_gzip=%d kept=%d attempted=%d\n",
			res.Name,
			res.Dataset,
			res.Records,
			res.RawBytes,
			res.EncodedBytes,
			res.SavedRatio,
			res.RawGzipBytes,
			res.EncodedGzipBytes,
			res.Kept,
			res.Attempted,
		)
	}
}
