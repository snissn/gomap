package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/template"
)

type valueLogReplayExportRecord struct {
	Key      string `json:"key"`
	Val      string `json:"val"`
	Encoding string `json:"encoding,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
	RID      uint64 `json:"rid,omitempty"`
	File     string `json:"file,omitempty"`
	FileID   uint32 `json:"file_id,omitempty"`
	Offset   uint64 `json:"offset,omitempty"`
	ValueLen int    `json:"value_len,omitempty"`
}

type valueLogReplayExportReport struct {
	Dir               string `json:"dir"`
	MainDir           string `json:"main_dir"`
	ValueLogDir       string `json:"value_log_dir"`
	Output            string `json:"output"`
	Segments          int    `json:"segments"`
	Records           int64  `json:"records"`
	RawValueBytes     int64  `json:"raw_value_bytes"`
	TruncatedSegments int    `json:"truncated_segments,omitempty"`
}

type valueLogReadLookups struct {
	dictLookup         valuelog.DictLookup
	templateLookup     valuelog.TemplateLookup
	templateDecodeOpts template.DecodeOptions
}

func runVlogReplayExport(dir string, args []string) {
	fs := flag.NewFlagSet("vlog-replay-export", flag.ExitOnError)
	rw := fs.Bool("rw", false, "Open read-write (required; may replay WAL or repair files)")
	outPath := fs.String("out", "", "Output JSONL path ('-' or empty for stdout)")
	limit := fs.Int64("limit", 0, "Maximum logical records to export (0=all)")
	_ = fs.Parse(args)

	if !*rw {
		fatalf("vlog-replay-export requires -rw")
	}

	report, err := exportValueLogReplay(dir, valueLogReplayExportOptions{
		OutputPath: *outPath,
		Limit:      *limit,
	})
	if err != nil {
		fatalf("vlog-replay-export error: %v", err)
	}

	fmt.Fprintf(os.Stderr, "vlog-replay-export: output=%s segments=%d records=%d raw_value_bytes=%d truncated_segments=%d\n",
		report.Output,
		report.Segments,
		report.Records,
		report.RawValueBytes,
		report.TruncatedSegments,
	)
}

type valueLogReplayExportOptions struct {
	OutputPath string
	Limit      int64
}

func exportValueLogReplay(dir string, opts valueLogReplayExportOptions) (report valueLogReplayExportReport, err error) {
	report = valueLogReplayExportReport{Dir: dir}
	mainDir, err := resolveTreemapMainDir(dir)
	if err != nil {
		return report, err
	}
	rootDir := resolveTreemapRootDir(filepath.Clean(dir), mainDir)
	report.MainDir = mainDir
	report.ValueLogDir = filepath.Join(mainDir, "wal")

	segs, _, err := listValueLogSegments(report.ValueLogDir)
	if err != nil {
		return report, err
	}
	report.Segments = len(segs)

	lookups, cleanup, err := openValueLogReadLookups(rootDir)
	if err != nil {
		return report, err
	}
	defer func() {
		if cleanup == nil {
			return
		}
		if cerr := cleanup(); cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = errors.Join(err, cerr)
			}
		}
	}()

	out, outputPath, closeOutput, err := openReplayExportOutput(opts.OutputPath)
	if err != nil {
		return report, err
	}
	report.Output = outputPath
	defer func() {
		if closeOutput == nil {
			return
		}
		if cerr := closeOutput(); cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = errors.Join(err, cerr)
			}
		}
	}()

	writer := bufio.NewWriterSize(out, 1<<20)
	defer func() {
		if writer == nil {
			return
		}
		if ferr := writer.Flush(); ferr != nil {
			if err == nil {
				err = ferr
			} else {
				err = errors.Join(err, ferr)
			}
		}
	}()
	enc := json.NewEncoder(writer)

	var seq uint64
	for _, seg := range segs {
		if opts.Limit > 0 && report.Records >= opts.Limit {
			break
		}
		fileID, ok := parseValueLogAuditFileID(seg.Name)
		if !ok {
			continue
		}
		reader, openErr := valuelog.NewReader(seg.Path, fileID)
		if openErr != nil {
			return report, openErr
		}
		reader.SetDictLookup(lookups.dictLookup)
		reader.SetTemplateLookup(lookups.templateLookup, lookups.templateDecodeOpts)

		for {
			if opts.Limit > 0 && report.Records >= opts.Limit {
				break
			}
			rid, value, ptr, readErr := reader.ReadNext()
			if readErr == nil {
				seq++
				rec := valueLogReplayExportRecord{
					Key:      encodeReplayBase64(syntheticReplayKey(seq, rid, fileID)),
					Val:      encodeReplayBase64(value),
					Encoding: "base64",
					Seq:      seq,
					RID:      rid,
					File:     seg.Name,
					FileID:   fileID,
					Offset:   ptr.Offset,
					ValueLen: len(value),
				}
				if err := enc.Encode(rec); err != nil {
					_ = reader.Close()
					return report, err
				}
				report.Records++
				report.RawValueBytes += int64(len(value))
				continue
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if errors.Is(readErr, io.ErrUnexpectedEOF) {
				report.TruncatedSegments++
				break
			}
			_ = reader.Close()
			return report, readErr
		}
		if err := reader.Close(); err != nil {
			return report, err
		}
	}

	return report, nil
}

func openReplayExportOutput(path string) (io.Writer, string, func() error, error) {
	clean := path
	if clean == "" || clean == "-" {
		return os.Stdout, "-", nil, nil
	}
	f, err := os.Create(clean)
	if err != nil {
		return nil, "", nil, err
	}
	return f, clean, f.Close, nil
}

func openValueLogReadLookups(rootDir string) (_ valueLogReadLookups, cleanup func() error, err error) {
	var (
		lookups valueLogReadLookups
		closers []func() error
	)

	cleanup = func() error {
		var first error
		for i := len(closers) - 1; i >= 0; i-- {
			if cerr := closers[i](); cerr != nil && first == nil {
				first = cerr
			}
		}
		return first
	}
	defer func() {
		if err != nil && cleanup != nil {
			_ = cleanup()
			cleanup = nil
		}
	}()

	dictDir := filepath.Join(rootDir, "dictdb")
	dictIndexPath := filepath.Join(dictDir, "index.db")
	if _, statErr := os.Stat(dictIndexPath); statErr == nil {
		dictOpts := treedbdb.Options{Dir: dictDir, ReadOnly: true}
		applyPersistedFormatConfig(dictDir, &dictOpts)
		dictOpts.DisableBackgroundPrune = true
		dictOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
		dictBackend, openErr := treedbdb.Open(dictOpts)
		if openErr != nil {
			return lookups, cleanup, fmt.Errorf("dictdb open: %w", openErr)
		}
		closers = append(closers, dictBackend.Close)
		store := dictdb.New(dictBackend)
		lookups.dictLookup = func(dictID uint64) ([]byte, error) {
			return store.GetDictBytes(context.Background(), dictID)
		}
	} else if !os.IsNotExist(statErr) {
		return lookups, cleanup, fmt.Errorf("stat dictdb index: %w", statErr)
	}

	templateDir := filepath.Join(rootDir, "templatedb")
	templateIndexPath := filepath.Join(templateDir, "index.db")
	if _, statErr := os.Stat(templateIndexPath); statErr == nil {
		templateOpts := treedbdb.Options{Dir: templateDir, ReadOnly: true}
		applyPersistedFormatConfig(templateDir, &templateOpts)
		templateOpts.DisableBackgroundPrune = true
		templateOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
		templateOpts.ValueLog.TemplateMode = template.TemplateOff
		templateOpts.ValueLog.TemplateLookup = nil
		templateOpts.ValueLog.TemplateStore = nil
		templateBackend, openErr := treedbdb.Open(templateOpts)
		if openErr != nil {
			return lookups, cleanup, fmt.Errorf("templatedb open: %w", openErr)
		}
		closers = append(closers, templateBackend.Close)
		store := templatedb.New(templateBackendKV{db: templateBackend}, templatedb.Config{})
		lookups.templateLookup = func(templateID uint64) ([]byte, error) {
			return store.GetTemplateDef(context.Background(), templateID)
		}
		tcfg := template.NormalizeConfig(template.Config{})
		lookups.templateDecodeOpts = template.DecodeOptions{
			MaxDecodedBytes: tcfg.MaxDecodedBytes,
			MaxGaps:         tcfg.MaxGaps,
			DefCacheSize:    tcfg.DefCacheSize,
		}
	} else if !os.IsNotExist(statErr) {
		return lookups, cleanup, fmt.Errorf("stat templatedb index: %w", statErr)
	}

	return lookups, cleanup, nil
}

func syntheticReplayKey(seq, rid uint64, fileID uint32) []byte {
	return []byte(fmt.Sprintf("vlog-replay/%012d/rid-%020d/file-%010d", seq, rid, fileID))
}

func encodeReplayBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
