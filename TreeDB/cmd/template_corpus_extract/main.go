package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type corpusSectionManifest struct {
	File     string `json:"file"`
	Scanned  int    `json:"scanned"`
	Records  int    `json:"records"`
	RawBytes int64  `json:"raw_bytes"`
	Limit    int    `json:"limit"`
	Stride   int    `json:"stride"`
}

type corpusManifest struct {
	Version      int                   `json:"version"`
	SourceDir    string                `json:"source_dir"`
	GeneratedAt  string                `json:"generated_at"`
	RecordFormat string                `json:"record_format"`
	Pointer      corpusSectionManifest `json:"pointer"`
	OuterLeaf    corpusSectionManifest `json:"outer_leaf"`
}

type corpusWriter struct {
	path     string
	file     *os.File
	buf      *bufio.Writer
	records  int
	rawBytes int64
}

func newCorpusWriter(path string, overwrite bool) (*corpusWriter, error) {
	if !overwrite {
		if _, err := os.Stat(path); err == nil {
			return nil, fmt.Errorf("output exists: %s (use -overwrite)", path)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &corpusWriter{
		path: path,
		file: f,
		buf:  bufio.NewWriterSize(f, 1<<20),
	}, nil
}

func (w *corpusWriter) WriteRecord(payload []byte) error {
	if w == nil || w.buf == nil {
		return errors.New("nil corpus writer")
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("record too large: %d bytes", len(payload))
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.buf.Write(payload); err != nil {
			return err
		}
	}
	w.records++
	w.rawBytes += int64(len(payload))
	return nil
}

func (w *corpusWriter) Close() error {
	if w == nil {
		return nil
	}
	var first error
	if w.buf != nil {
		if err := w.buf.Flush(); err != nil && first == nil {
			first = err
		}
		w.buf = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && first == nil {
			first = err
		}
		w.file = nil
	}
	return first
}

type outerLeafKey struct {
	fileID uint32
	offset uint64
}

var errStopScan = errors.New("template-corpus-extract: stop scan")

func shouldSample(scanned, stride int) bool {
	if stride <= 1 {
		return true
	}
	return scanned%stride == 0
}

func extractPointerCorpus(backend *treedbdb.DB, writer *corpusWriter, limit, stride int) (scanned int, err error) {
	if backend == nil {
		return 0, errors.New("nil backend")
	}
	it, err := backend.Iterator(nil, nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()

	for ; it.Valid(); it.Next() {
		_, ptr, flags := it.UnsafeEntry()
		if flags&node.FlagPointer == 0 || !page.IsValueLogFileID(ptr.FileID) {
			continue
		}
		if limit > 0 && writer.records >= limit {
			break
		}
		if shouldSample(scanned, stride) {
			value := it.ValueCopy(nil)
			if len(value) > 0 {
				if err := writer.WriteRecord(value); err != nil {
					return scanned, err
				}
			}
		}
		scanned++
	}
	if err := it.Error(); err != nil {
		return scanned, err
	}
	return scanned, nil
}

func extractOuterLeafCorpus(backend *treedbdb.DB, writer *corpusWriter, limit, stride int) (scanned int, err error) {
	if backend == nil {
		return 0, errors.New("nil backend")
	}
	snap := backend.AcquireSnapshot()
	if snap == nil {
		return 0, errors.New("acquire snapshot: nil")
	}
	defer snap.Close()

	state := snap.State()
	if state == nil {
		return 0, errors.New("snapshot state unavailable")
	}
	pgr := snap.Pager()
	if pgr == nil {
		return 0, errors.New("snapshot pager unavailable")
	}
	reader := treedbdb.ValueReaderForState(state)
	if reader == nil {
		return 0, errors.New("value-log reader unavailable")
	}

	initialCap := 1 << 10
	if limit > 0 {
		if limit < (1 << 20) {
			initialCap = limit
		} else {
			initialCap = 1 << 20
		}
	}
	seen := make(map[outerLeafKey]struct{}, initialCap)
	visit := func(ptr page.LeafLogPtr) error {

		key := outerLeafKey{fileID: ptr.ValueLogFileID(), offset: ptr.Offset}
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		if limit > 0 && writer.records >= limit {
			return errStopScan
		}
		if shouldSample(scanned, stride) {
			payload, err := reader.ReadUnsafe(ptr.ValuePtr())
			if err != nil {
				return err
			}
			if len(payload) > 0 {
				if err := writer.WriteRecord(payload); err != nil {
					return err
				}
			}
		}
		scanned++
		return nil
	}

	roots := []uint64{state.RootPageID, state.SystemRootPageID}
	for _, root := range roots {
		if root == 0 {
			continue
		}
		err := leafrefscan.Walk(context.Background(), root, pgr.Get, nil, visit)
		if err == nil {
			continue
		}
		if errors.Is(err, errStopScan) {
			break
		}
		return scanned, err
	}
	return scanned, nil
}

func writeManifest(path string, manifest corpusManifest) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(manifest)
}

func main() {
	appDir := flag.String("app-dir", "", "TreeDB app dir (root with maindb/dictdb/templatedb)")
	outDir := flag.String("out-dir", "", "output directory for extracted corpora")
	pointerLimit := flag.Int("pointer-limit", 200000, "max pointer-value records to write (0=all)")
	pointerStride := flag.Int("pointer-stride", 1, "pointer sampling stride")
	outerLeafLimit := flag.Int("outer-leaf-limit", 200000, "max outer-leaf page records to write (0=all)")
	outerLeafStride := flag.Int("outer-leaf-stride", 1, "outer-leaf sampling stride")
	skipPointer := flag.Bool("skip-pointer", false, "skip pointer-value corpus extraction")
	skipOuterLeaf := flag.Bool("skip-outer-leaf", false, "skip outer-leaf corpus extraction")
	overwrite := flag.Bool("overwrite", false, "overwrite existing output files")
	flag.Parse()

	if *appDir == "" {
		log.Fatal("-app-dir is required")
	}
	if *outDir == "" {
		log.Fatal("-out-dir is required")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir out-dir: %v", err)
	}

	backend, cleanup, err := treedb.OpenBackend(treedb.Options{Dir: *appDir, ReadOnly: true})
	if err != nil {
		log.Fatalf("open backend: %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			log.Printf("backend cleanup: %v", err)
		}
	}()

	pointerPath := filepath.Join(*outDir, "pointer_values.bin")
	outerLeafPath := filepath.Join(*outDir, "outer_leaf_pages.bin")
	manifestPath := filepath.Join(*outDir, "manifest.json")

	var pointerWriter *corpusWriter
	if !*skipPointer {
		pointerWriter, err = newCorpusWriter(pointerPath, *overwrite)
		if err != nil {
			log.Fatalf("open pointer corpus: %v", err)
		}
	}
	var outerWriter *corpusWriter
	if !*skipOuterLeaf {
		outerWriter, err = newCorpusWriter(outerLeafPath, *overwrite)
		if err != nil {
			log.Fatalf("open outer-leaf corpus: %v", err)
		}
	}

	pointerScanned := 0
	if !*skipPointer {
		pointerScanned, err = extractPointerCorpus(backend, pointerWriter, *pointerLimit, *pointerStride)
		if err != nil {
			log.Fatalf("extract pointer corpus: %v", err)
		}
		if err := pointerWriter.Close(); err != nil {
			log.Fatalf("close pointer corpus: %v", err)
		}
	}

	outerLeafScanned := 0
	if !*skipOuterLeaf {
		outerLeafScanned, err = extractOuterLeafCorpus(backend, outerWriter, *outerLeafLimit, *outerLeafStride)
		if err != nil {
			log.Fatalf("extract outer-leaf corpus: %v", err)
		}
		if err := outerWriter.Close(); err != nil {
			log.Fatalf("close outer-leaf corpus: %v", err)
		}
	}

	pointerRecords := 0
	pointerRawBytes := int64(0)
	if pointerWriter != nil {
		pointerRecords = pointerWriter.records
		pointerRawBytes = pointerWriter.rawBytes
	}
	outerLeafRecords := 0
	outerLeafRawBytes := int64(0)
	if outerWriter != nil {
		outerLeafRecords = outerWriter.records
		outerLeafRawBytes = outerWriter.rawBytes
	}

	manifest := corpusManifest{
		Version:      1,
		SourceDir:    *appDir,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		RecordFormat: "u32le_length + raw_bytes",
		Pointer: corpusSectionManifest{
			File:     filepath.Base(pointerPath),
			Scanned:  pointerScanned,
			Records:  pointerRecords,
			RawBytes: pointerRawBytes,
			Limit:    *pointerLimit,
			Stride:   *pointerStride,
		},
		OuterLeaf: corpusSectionManifest{
			File:     filepath.Base(outerLeafPath),
			Scanned:  outerLeafScanned,
			Records:  outerLeafRecords,
			RawBytes: outerLeafRawBytes,
			Limit:    *outerLeafLimit,
			Stride:   *outerLeafStride,
		},
	}
	if err := writeManifest(manifestPath, manifest); err != nil {
		log.Fatalf("write manifest: %v", err)
	}

	fmt.Printf("template-corpus-extract: out=%s pointer_records=%d pointer_bytes=%d outer_leaf_records=%d outer_leaf_bytes=%d manifest=%s\n",
		*outDir,
		pointerRecords,
		pointerRawBytes,
		outerLeafRecords,
		outerLeafRawBytes,
		manifestPath,
	)
}
