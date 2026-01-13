package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

type kvRecord struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type datasetStats struct {
	count int
	total int
	min   int
	max   int
	avg   float64
	p50   int
	p95   int
}

type metaFormat int

const (
	metaOffsets32 metaFormat = iota
	metaOffsets32Len16
	metaOffsets32LenVarint
)

type slab struct {
	records      int
	rawBytes     int
	payloadBytes int
	metaBytes    int
	headerBytes  int
	totalBytes   int
}

func main() {
	input := flag.String("input", "", "Input JSONL with key/val (URL-escaped)")
	dictPath := flag.String("dict", "", "Dict path (optional for dict mode)")
	n := flag.Int("n", 1_000_000, "Max records")
	seed := flag.Int64("seed", 1, "Seed (currently unused, deterministic order)")
	sample := flag.Int("sample", 100_000, "Sample lookups for microbench (unused if omitted)")
	blocksFlag := flag.String("blocks", "4096,8192,16384", "Block sizes (bytes) comma-separated")
	crcFlag := flag.Bool("crc", true, "Include CRC bytes in metadata accounting")
	metaFmtFlag := flag.String("meta_format", "offsets32+len16", "Meta format: offsets32|offsets32+len16|offsets32+lenvarint")
	flag.Parse()

	if *input == "" {
		fmt.Println("input required")
		os.Exit(1)
	}
	_ = seed
	_ = sample

	values, stats := loadValues(*input, *n)
	fmt.Printf("dataset: count=%d avg=%.2f p50=%d p95=%d min=%d max=%d\n", stats.count, stats.avg, stats.p50, stats.p95, stats.min, stats.max)

	blockSizes := parseBlocks(*blocksFlag)
	metaFmt := parseMetaFmt(*metaFmtFlag)
	headerBytes := 32

	var dictBytes []byte
	if *dictPath != "" {
		b, err := os.ReadFile(*dictPath)
		if err != nil {
			fmt.Printf("dict load failed: %v (dict mode skipped)\n", err)
			dictBytes = nil
		} else {
			dictBytes = b
			fmt.Printf("dict: %s size=%d\n", *dictPath, len(dictBytes))
		}
	} else {
		fmt.Println("dict: <none> (dict mode skipped)")
	}

	// Encoding A: per-row dict frames (payload-only)
	if dictBytes != nil {
		encDict, _ := zstd.NewWriter(nil, zstd.WithEncoderDict(dictBytes), zstd.WithEncoderLevel(zstd.SpeedDefault))
		defer encDict.Close()
		for _, bs := range blockSizes {
			slabs := packPerRow(values, bs, encDict, metaFmt, *crcFlag, headerBytes)
			printSlabStats(fmt.Sprintf("dict_row b=%d", bs), slabs, values)
		}
	}

	// Encoding B: block zstd payload
	encPlain, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer encPlain.Close()
	for _, bs := range blockSizes {
		slabs := packBlock(values, bs, encPlain, metaFmt, *crcFlag, headerBytes)
		printSlabStats(fmt.Sprintf("block b=%d", bs), slabs, values)
	}
}

func parseBlocks(s string) []int {
	parts := strings.Split(s, ",")
	var out []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		out = []int{4096, 8192, 16384}
	}
	return out
}

func parseMetaFmt(s string) metaFormat {
	switch s {
	case "offsets32":
		return metaOffsets32
	case "offsets32+len16":
		return metaOffsets32Len16
	case "offsets32+lenvarint":
		return metaOffsets32LenVarint
	default:
		return metaOffsets32Len16
	}
}

func loadValues(path string, limit int) ([][]byte, datasetStats) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)
	var values [][]byte
	var sizes []int
	total := 0
	minv := math.MaxInt32
	maxv := 0
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			panic(err)
		}
		if len(line) > 0 {
			var rec kvRecord
			if err := json.Unmarshal(bytes.TrimSpace(line), &rec); err == nil {
				valStr, err := url.PathUnescape(rec.Val)
				if err == nil {
					b := []byte(valStr)
					values = append(values, b)
					sz := len(b)
					sizes = append(sizes, sz)
					total += sz
					if sz < minv {
						minv = sz
					}
					if sz > maxv {
						maxv = sz
					}
					if limit > 0 && len(values) >= limit {
						break
					}
				}
			}
		}
		if err == io.EOF {
			break
		}
	}
	if minv == math.MaxInt32 {
		minv = 0
	}
	sort.Ints(sizes)
	p50 := 0
	p95 := 0
	if len(sizes) > 0 {
		p50 = sizes[len(sizes)/2]
		p95 = sizes[(95*len(sizes))/100]
	}
	stats := datasetStats{
		count: len(values),
		total: total,
		min:   minv,
		max:   maxv,
		avg:   float64(total) / float64(len(values)),
		p50:   p50,
		p95:   p95,
	}
	return values, stats
}

func packPerRow(values [][]byte, target int, enc *zstd.Encoder, fmt metaFormat, crc bool, header int) []slab {
	var slabs []slab
	start := 0
	for start < len(values) {
		raw := 0
		end := start
		for end < len(values) && raw+len(values[end]) <= target {
			raw += len(values[end])
			end++
		}
		if end == start {
			end = start + 1
			raw = len(values[start])
		}
		payload := 0
		lengths := make([]int, end-start)
		for i := start; i < end; i++ {
			f := enc.EncodeAll(values[i], nil)
			payload += len(f)
			lengths[i-start] = len(f)
		}
		meta := metaBytes(fmt, lengths, crc)
		s := slab{
			records:      end - start,
			rawBytes:     raw,
			payloadBytes: payload,
			metaBytes:    meta,
			headerBytes:  header,
			totalBytes:   payload + meta + header,
		}
		slabs = append(slabs, s)
		start = end
	}
	return slabs
}

func packBlock(values [][]byte, target int, enc *zstd.Encoder, fmt metaFormat, crc bool, header int) []slab {
	var slabs []slab
	start := 0
	for start < len(values) {
		raw := 0
		end := start
		for end < len(values) && raw+len(values[end]) <= target {
			raw += len(values[end])
			end++
		}
		if end == start {
			end = start + 1
			raw = len(values[start])
		}
		combined := make([]byte, 0, raw)
		offsets := make([]int, end-start+1)
		pos := 0
		for i := start; i < end; i++ {
			offsets[i-start] = pos
			combined = append(combined, values[i]...)
			pos += len(values[i])
		}
		offsets[end-start] = pos
		comp := enc.EncodeAll(combined, nil)
		meta := metaBytes(fmt, offsetsToLens(offsets), crc)
		s := slab{
			records:      end - start,
			rawBytes:     raw,
			payloadBytes: len(comp),
			metaBytes:    meta,
			headerBytes:  header,
			totalBytes:   len(comp) + meta + header,
		}
		slabs = append(slabs, s)
		start = end
	}
	return slabs
}

func offsetsToLens(offsets []int) []int {
	// offsets len = n+1
	lens := make([]int, len(offsets)-1)
	for i := 0; i < len(lens); i++ {
		lens[i] = offsets[i+1] - offsets[i]
	}
	return lens
}

func metaBytes(fmt metaFormat, lens []int, crc bool) int {
	n := len(lens)
	switch fmt {
	case metaOffsets32:
		meta := (n + 1) * 4
		if crc {
			meta += n * 4
		}
		return meta
	case metaOffsets32Len16:
		meta := (n+1)*4 + n*2
		for _, l := range lens {
			if l > 0xFFFF {
				meta += 2 // upgrade to 4 bytes
			}
		}
		if crc {
			meta += n * 4
		}
		return meta
	case metaOffsets32LenVarint:
		meta := (n + 1) * 4
		for _, l := range lens {
			meta += varintLen(uint64(l))
		}
		if crc {
			meta += n * 4
		}
		return meta
	default:
		return 0
	}
}

func varintLen(v uint64) int {
	l := 1
	for v >= 0x80 {
		v >>= 7
		l++
	}
	return l
}

func printSlabStats(name string, slabs []slab, values [][]byte) {
	var rawTotal, payloadTotal, metaTotal, headerTotal int
	recTotal := 0
	for _, s := range slabs {
		rawTotal += s.rawBytes
		payloadTotal += s.payloadBytes
		metaTotal += s.metaBytes
		headerTotal += s.headerBytes
		recTotal += s.records
	}
	totalBytes := payloadTotal + metaTotal + headerTotal
	totalRatio := float64(totalBytes) / float64(rawTotal)
	payloadRatio := float64(payloadTotal) / float64(rawTotal)
	metaPerRec := float64(metaTotal) / float64(recTotal)
	fmt.Printf("%s slabs=%d avg_rec_slab=%.1f\n", name, len(slabs), float64(recTotal)/float64(len(slabs)))
	fmt.Printf("  raw_total=%d payload_total=%d payload_ratio=%.4f\n", rawTotal, payloadTotal, payloadRatio)
	fmt.Printf("  meta_total=%d meta_per_rec=%.2f header_total=%d\n", metaTotal, metaPerRec, headerTotal)
	fmt.Printf("  total_bytes=%d total_ratio=%.4f total_per_rec=%.2f\n", totalBytes, totalRatio, float64(totalBytes)/float64(recTotal))
}
