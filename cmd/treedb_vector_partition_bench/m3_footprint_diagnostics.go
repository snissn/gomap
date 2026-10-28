package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const m3DiagnosticsDirEnv = "GOMAP_M3_DIAGNOSTICS_DIR"

type m3FootprintFile struct {
	Path  string `json:"path"`
	Bytes int64  `json:"bytes"`
}

type m3FootprintDiagnostic struct {
	DatabaseDirectory string                                  `json:"database_directory"`
	Boundary          string                                  `json:"boundary"`
	Ratio             float64                                 `json:"ratio"`
	Generation        uint64                                  `json:"generation"`
	HeadSHA           string                                  `json:"head_sha"`
	Command           []string                                `json:"command"`
	TotalBytes        int64                                   `json:"total_bytes"`
	Files             []m3FootprintFile                       `json:"files"`
	WAL               backenddb.CommandWALClosedDiagnosticsV1 `json:"wal"`
	Error             string                                  `json:"error,omitempty"`
}

// The inventory uses the same logical lengths and includes every file, including
// initialized WAL reserve. Diagnostic artifacts are always outside the DB root.
func m3FootprintInventory(root string) ([]m3FootprintFile, int64, error) {
	var files []m3FootprintFile
	var total int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() < 0 || info.Size() > math.MaxInt64-total {
			return errors.New("M3 inventory byte accounting overflow")
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, m3FootprintFile{Path: filepath.ToSlash(rel), Bytes: info.Size()})
		total += info.Size()
		return nil
	})
	return files, total, err
}

func m3CaptureClosedFootprint(cfg config, db *backenddb.DB, dir, boundary string, ratio float64, generation uint64, measuredBytes int64) (out m3FootprintDiagnostic, resultErr error) {
	defer func() {
		if resultErr != nil {
			out.Error = resultErr.Error()
		}
	}()
	out = m3FootprintDiagnostic{DatabaseDirectory: dir, Boundary: boundary, Ratio: ratio, Generation: generation, HeadSHA: cfg.headSHA, Command: append([]string(nil), cfg.command...), TotalBytes: measuredBytes}
	var inventoryBytes int64
	var inventoryErr, walErr error
	out.Files, inventoryBytes, inventoryErr = m3FootprintInventory(dir)
	out.WAL, walErr = db.InspectClosedCommandWALV1()
	if inventoryErr == nil && inventoryBytes != measuredBytes {
		inventoryErr = fmt.Errorf("closed %s inventory bytes=%d measured=%d", boundary, inventoryBytes, measuredBytes)
	}
	captureErr := errors.Join(inventoryErr, walErr)
	if captureErr != nil {
		out.Error = captureErr.Error()
	}
	raw, marshalErr := json.MarshalIndent(out, "", "  ")
	if marshalErr != nil {
		return out, errors.Join(captureErr, marshalErr)
	}
	retainErr := m3RetainDiagnostic("footprint-"+boundary, raw, dir)
	if captureErr != nil {
		return out, errors.Join(fmt.Errorf("M3 footprint capture incomplete: %w", captureErr), retainErr)
	}
	return out, retainErr
}

// A unique file per attempt preserves all observations. Sync and close before
// returning so test cleanup or a subsequent report-validation failure cannot
// discard this evidence. Unset means the report itself owns the diagnostics.
func m3RetainDiagnostic(kind string, raw []byte, excludedRoots ...string) (resultErr error) {
	root := os.Getenv(m3DiagnosticsDirEnv)
	if root == "" {
		return nil
	}
	destination, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	for _, excluded := range excludedRoots {
		measuredRoot, err := filepath.Abs(excluded)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(measuredRoot, destination)
		if err != nil {
			return err
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return errors.New("M3 diagnostics destination must be outside the measured database")
		}
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(root, "m3-"+kind+"-*.json")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	if _, err := file.Write(append(raw, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func m3FootprintDiagnosticErrors(report m3PartitionIndexReport) error {
	var errs []error
	for _, row := range report.Rows {
		for _, diagnostic := range []m3FootprintDiagnostic{row.SourceFootprintDiagnostic, row.FinalFootprintDiagnostic} {
			if diagnostic.Error != "" {
				errs = append(errs, fmt.Errorf("M3 ratio %.4f %s diagnostics: %s", row.Ratio, diagnostic.Boundary, diagnostic.Error))
			}
		}
	}
	return errors.Join(errs...)
}
