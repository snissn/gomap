package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestNativeMVCCOriginalFilePhysicalClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value.log")
	writer, err := NewWriter(path, 1)
	if err != nil { t.Fatal(err) }
	if _, err = writer.Append(0, nil, 1, bytes.Repeat([]byte("real-original-file"), 150000)); err != nil { t.Fatal(err) }
	if err = writer.Close(); err != nil { t.Fatal(err) }
	file, err := openFile(path, 1, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil { t.Fatal(err) }
	defer file.Close()
	if !file.remapToFileSize() { t.Fatal("original constructor did not map persisted file") }
	if done, err := file.CloseNativeMVCCStep(&iterator.OrdinalScanWork{}); done || err != nil || file.closeAttempted || file.closed.Load() { t.Fatal("zero-work close effect") }
	if done, err := file.CloseNativeMVCCStep(&iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1<<20}); done || err != nil || !file.closed.Load() { t.Fatal("close did not pause", err) }
	if err := file.Close(); !errors.Is(err, ErrNativeMVCCFileWork) { t.Fatal("ordinary close stole native custody", err) }
	if _, err := file.File.Stat(); err != nil { t.Fatal("OS close preceded admitted physical phases", err) }
	turns := 0
	for ; turns < 10000; turns++ {
		work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 8192}
		done, err := file.CloseNativeMVCCStep(work)
		if err != nil || work.Records > 32 || work.Bytes > 8192 { t.Fatal("physical quantum", err) }
		if done { break }
	}
	if turns == 10000 || file.currentMappingV6.remainingV6() != 0 || len(file.deadMappings) != 0 { t.Fatal("physical mapping survived cleanup") }
	if _, err := file.File.Stat(); !errors.Is(err, os.ErrClosed) { t.Fatal("original handle not closed", err) }
	if done, err := file.CloseNativeMVCCStep(&iterator.OrdinalScanWork{}); !done || err != nil { t.Fatal("terminal cleanup replay", done, err) }
}
