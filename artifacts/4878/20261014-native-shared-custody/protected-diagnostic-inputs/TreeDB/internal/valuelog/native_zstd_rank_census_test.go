package valuelog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/compress/zstd"
	"github.com/snissn/gomap/TreeDB/page"
)

// Diagnostic census only: this does not implement a native decoder or change work pricing.
func TestNativeZSTDPhysicalRankCensus(t *testing.T) {
	out := os.Getenv("NATIVE_ZSTD_RANK_ARTIFACT")
	if out == "" {
		t.Skip("set NATIVE_ZSTD_RANK_ARTIFACT for decision evidence")
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(out, "fixture")
	if err := os.Mkdir(fixture, 0755); err != nil {
		t.Fatal(err)
	}
	value := make([]byte, (2<<20)+4096)
	x := uint32(1)
	for i := range value {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		value[i] = byte(x & 63)
		if i%2048 >= 1024 {
			value[i] = value[i-1024]
		}
	}
	path := filepath.Join(fixture, "value-000001.log")
	writer, err := NewWriter(path, page.ValueLogFileID(1))
	if err != nil {
		t.Fatal(err)
	}
	writer.SetBlockCompression(BlockCodecZSTD, true)
	ptrs, stats, err := writer.AppendFrameWithStats(0, nil, []Record{{RID: 1, Value: value}})
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if !stats.Kept {
		t.Fatal("actual writer ZSTD was not kept")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	limits := COWReadLimits{MaxRecordBytes: 8 << 20, MaxRawBytes: 8 << 20, MaxValueBytes: 8 << 20}
	shape, err := InspectCOWRecord(f, ptrs[0], limits)
	if err != nil {
		t.Fatal(err)
	}
	if !shape.Compressed || shape.DictID != 0 || shape.frame.Reserved != byte(BlockCodecZSTD) {
		t.Fatal(shape)
	}
	got, err := ReadCOWRecord(f, ptrs[0], shape, true, make([]byte, shape.RecordBytes), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes), func(h FrameHeader, input, dst []byte) ([]byte, error) {
		return decodeBlockPayload(h.Reserved, input, uint32(shape.RawBytes), dst)
	})
	if err != nil || !bytes.Equal(got, value) {
		t.Fatal("ordinary selected value/CRC oracle", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	start := int(ptrs[0].Offset - 4)
	record := data[start : start+int(shape.RecordBytes)]
	prefix := FrameHeaderSize + int(shape.frame.K)*8 + (int(shape.frame.K)+1)*4
	payload := record[HeaderSize+prefix:]
	if os.Getenv("NATIVE_ZSTD_RANK_EXPORT") == "1" {
		for _, part := range []struct {
			name string
			data []byte
		}{{"pointed.zstd", payload}, {"pointed.raw", value}} {
			if err := os.WriteFile(filepath.Join(out, part.name), part.data, 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	var header zstd.Header
	if err = header.Decode(payload); err != nil || header.Skippable || !header.HasFCS || header.FrameContentSize != uint64(len(value)) {
		t.Fatal(header, err)
	}
	type block struct {
		Offset                 int
		Last                   bool
		Type                   int
		StoredBytes            int
		LiteralType            int
		LiteralHeaderBytes     int
		RegeneratedLiterals    int
		CompressedLiterals     int
		HuffmanProbeLowerBound int
	}
	blocks := []block{}
	at, total := header.HeaderSize, 0
	for {
		if at+3 > len(payload) {
			t.Fatal("block header truncated")
		}
		word := int(payload[at]) | int(payload[at+1])<<8 | int(payload[at+2])<<16
		b := block{Offset: at, Last: word&1 != 0, Type: (word >> 1) & 3, StoredBytes: word >> 3, LiteralType: -1}
		at += 3
		stored := b.StoredBytes
		if b.Type == 1 {
			stored = 1
		}
		if b.Type == 3 || at+stored > len(payload) {
			t.Fatal("invalid block")
		}
		if b.Type == 2 {
			in := payload[at : at+stored]
			if len(in) < 5 {
				t.Fatal("literal header short")
			}
			b.LiteralType = int(in[0] & 3)
			sf := int(in[0] >> 2 & 3)
			if b.LiteralType < 2 {
				switch sf {
				case 0, 2:
					b.LiteralHeaderBytes = 1
					b.RegeneratedLiterals = int(in[0] >> 3)
				case 1:
					b.LiteralHeaderBytes = 2
					b.RegeneratedLiterals = int(in[0]>>4) | int(in[1])<<4
				case 3:
					b.LiteralHeaderBytes = 3
					b.RegeneratedLiterals = int(in[0]>>4) | int(in[1])<<4 | int(in[2])<<12
				}
			} else {
				sizes := []int{3, 3, 4, 5}
				bits := []uint{10, 10, 14, 18}
				b.LiteralHeaderBytes = sizes[sf]
				n := uint64(in[0] >> 4)
				for j := 1; j < b.LiteralHeaderBytes; j++ {
					n |= uint64(in[j]) << uint(4+8*(j-1))
				}
				b.RegeneratedLiterals = int(n & ((1 << bits[sf]) - 1))
				b.CompressedLiterals = int(n >> bits[sf])
				if b.LiteralHeaderBytes+b.CompressedLiterals >= len(in) {
					t.Fatal("literal payload bounds")
				}
				b.HuffmanProbeLowerBound = b.RegeneratedLiterals
				total += b.HuffmanProbeLowerBound
			}
		}
		blocks = append(blocks, b)
		at += stored
		if b.Last {
			break
		}
	}
	if header.HasCheckSum {
		at += 4
	}
	if at != len(payload) {
		t.Fatal("frame closure", at, len(payload))
	}
	if total <= 4096*32 {
		t.Fatal("not discriminating", total)
	}
	// Integrity oracle: actual record CRC rejects a flipped final stored byte.
	bad, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	corruptAt := int64(start + len(record) - 1)
	old := data[corruptAt]
	if _, err = bad.WriteAt([]byte{old ^ 1}, corruptAt); err != nil {
		t.Fatal(err)
	}
	_, crcErr := ReadCOWRecord(f, ptrs[0], shape, true, make([]byte, shape.RecordBytes), make([]byte, shape.RawBytes), make([]byte, shape.ValueBytes), nil)
	if !errors.Is(crcErr, ErrCorrupt) {
		t.Fatal("corrupt CRC oracle", crcErr)
	}
	if _, err = bad.WriteAt([]byte{old}, corruptAt); err != nil {
		t.Fatal(err)
	}
	if err = bad.Close(); err != nil {
		t.Fatal(err)
	}
	digest := func(p []byte) string { h := sha256.Sum256(p); return hex.EncodeToString(h[:]) }
	report := map[string]any{"source": "075ae77487778a872384a935c6d550359ca76452", "dependency": "87fb149e472104f0c2fa7a961b2493c4372603dc", "value_bytes": len(value), "value_sha256": digest(value), "generator": "xorshift32 seed=1; x^=x<<13; x^=x>>17; x^=x<<5; byte(x&63); duplicate each 1024-byte half in 2048-byte chunks", "file_path": path, "file_bytes": len(data), "file_sha256": digest(data), "pointer": ptrs[0], "stats": stats, "record_bytes": shape.RecordBytes, "raw_bytes": shape.RawBytes, "outer_stored_crc32": binary.LittleEndian.Uint32(record[:4]), "zstd_payload_bytes": len(payload), "zstd_payload_sha256": digest(payload), "zstd_header": header, "blocks": blocks, "huffman_literal_probe_lower_bound": total, "minimum_q32_calls": (total + 31) / 32, "same_request_record_capacity": 4096 * 32, "ordinary_selected_value_crc_oracle": "PASS exact bytes", "corrupt_crc_oracle": "PASS ErrCorrupt", "frame_and_block_closure": "PASS exact end"}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "census.json"), append(encoded, '\n'), 0644); err != nil {
		t.Fatal(err)
	}
	t.Logf("value=%d payload=%d blocks=%d huffman_probes>=%d minimum_Q32_calls=%d SAME_Request_capacity=%d", len(value), len(payload), len(blocks), total, (total+31)/32, 4096*32)
}
