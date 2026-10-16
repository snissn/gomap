package valuelog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestNativeMVCCOriginalFilePhysicalClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value.log")
	writer, err := NewWriter(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Append(0, nil, 1, bytes.Repeat([]byte("real-original-file"), 150000)); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := openFile(path, 1, nil, nil, templ.DecodeOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	mapped := file.remapToFileSize()
	if !mapped && (runtime.GOOS != "windows") {
		t.Fatal("original constructor did not map persisted file")
	}

	if done, err := file.CloseNativeMVCCStep(&iterator.OrdinalScanWork{}); done || err != nil || file.closeAttempted || file.closed.Load() {
		t.Fatal("zero-work close effect")
	}
	if done, err := file.CloseNativeMVCCStep(&iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}); done || err != nil || !file.closed.Load() {
		t.Fatal("close did not pause", err)
	}
	if err := file.Close(); !errors.Is(err, ErrNativeMVCCFileWork) {
		t.Fatal("ordinary close stole native custody", err)
	}
	if _, err := file.File.Stat(); err != nil {
		t.Fatal("OS close preceded admitted physical phases", err)
	}
	turns := 0
	for ; turns < 10000; turns++ {
		work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: uint64(max(8192, 2*os.Getpagesize()))}
		done, err := file.CloseNativeMVCCStep(work)
		if err != nil || work.Records > 32 || work.Bytes > work.ByteLimit {
			t.Fatal("physical quantum", err)
		}
		if done {
			break
		}
	}
	if turns == 10000 || file.currentMappingV6.remainingV6() != 0 || len(file.deadMappings) != 0 {
		t.Fatal("physical mapping survived cleanup")
	}
	if err := file.File.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("original handle not closed", err)
	}
	if done, err := file.CloseNativeMVCCStep(&iterator.OrdinalScanWork{}); !done || err != nil {
		t.Fatal("terminal cleanup replay", done, err)
	}
}

func TestNativeOriginalMappingFreshQuantumTooSmall(t *testing.T) {
	if !(runtime.GOOS != "windows") {
		t.Skip("platform uses retained file reads without mmap")
	}
	path := filepath.Join(t.TempDir(), "mapping.log")
	if err := os.WriteFile(path, make([]byte, os.Getpagesize()), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	mapping, err := mapOwnedReadOnlyV6(f, os.Getpagesize())
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.closeV6()
	minimum := 2*uint64(unsafe.Sizeof(mapping)) + uint64(os.Getpagesize())
	before := mapping.remainingV6()
	_, err = closeNativeMVCCMappingV6(&mapping, true, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: minimum - 1})
	var large *iterator.OrdinalUnitTooLarge
	if !errors.As(err, &large) || large.Bytes != minimum || mapping.remainingV6() != before {
		t.Fatal("fresh impossible mapping unit", err)
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: minimum, Bytes: 1}
	if done, err := closeNativeMVCCMappingV6(&mapping, true, work); done || err != nil || mapping.remainingV6() != before {
		t.Fatal("residual grant should yield", done, err)
	}
	if done, err := closeNativeMVCCMappingV6(&mapping, true, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: minimum}); !done || err != nil {
		t.Fatal("fresh fitting grant", done, err)
	}
}
