package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCommandWALCleanupParallelStreamEligibility(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("requires two schedulable processors")
	}
	dir := t.TempDir()
	segments := []logSegment{
		{path: filepath.Join(dir, commitlog.CommandSegmentName(0, 1)), seq: 1, size: minParallelWALCleanupStreamBytes},
		{path: filepath.Join(dir, commitlog.CommandSegmentName(0, 2)), seq: 2, size: minParallelWALCleanupStreamBytes},
		{path: filepath.Join(dir, commitlog.CommandSegmentName(0, 3)), seq: 3, size: 123},
	}
	active := commitlog.CommandJournalCleanupSnapshot{SegmentSeq: 3, ActivePath: segments[2].path}
	for _, test := range []struct {
		name         string
		lower, upper uint64
		maxFrame     int64
		small        bool
		want         bool
	}{
		{name: "default bound", lower: 7, upper: 7 + maxParallelWALCleanupReplayLSNs, want: true},
		{name: "zero debt", lower: 7, upper: 7, want: true},
		{name: "profile frame cap", lower: 7, upper: 8, maxFrame: maxParallelWALCleanupPrimaryFrameBytes, want: true},
		{name: "small predecessor", lower: 7, upper: 8, small: true},
		{name: "zero frontier", upper: 7},
		{name: "descending frontier", lower: 7, upper: 6},
		{name: "large replay map", lower: 7, upper: 8 + maxParallelWALCleanupReplayLSNs},
		{name: "unlimited frame", lower: 7, upper: 8, maxFrame: -1},
		{name: "large frame", lower: 7, upper: 8, maxFrame: maxParallelWALCleanupPrimaryFrameBytes + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			scan := newCommandWALCleanupScan(test.lower, test.upper, test.maxFrame, nil)
			files := append([]logSegment(nil), segments...)
			if test.small {
				files[1].size--
			}
			if got := scan.canScanClosedStreamPair(files, active, 3); got != test.want {
				t.Fatalf("eligible=%t want=%t", got, test.want)
			}
		})
	}
}

func requireExt4ForParallelWALCleanup(t *testing.T, dir string) {
	t.Helper()
	file, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(file)
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires ext4")
	}
}

func TestCommandWALCleanupParallelStreamOrderedProof(t *testing.T) {
	for _, test := range []struct {
		name          string
		frames        [4][]uint64
		lower, upper  uint64
		corrupt       []int
		partial       []int
		want          error
		workerRefusal string
	}{
		{
			name:   "covered pair and sealed replay suffix",
			frames: [4][]uint64{{1, 2}, {3, 4}, {5}, {6}},
			lower:  4, upper: 5, partial: []int{3},
		},
		{
			name:   "disjoint replay sets",
			frames: [4][]uint64{{1, 3}, {2, 4}, {5}, {6}},
			lower:  2, upper: 5,
		},
		{
			name:   "duplicate precedes later corruption",
			frames: [4][]uint64{{1, 3}, {2, 3, 4}, {5}, {6}},
			lower:  2, upper: 5, corrupt: []int{1},
			want: commitlog.ErrCommandWALDuplicateLSN,
		},
		{
			name:   "both hard errors maintain file order",
			frames: [4][]uint64{{1, 2}, {3, 4}, {5}, {6}},
			lower:  4, upper: 5, corrupt: []int{0, 1},
			want: commitlog.ErrCorrupt,
		},
		{
			name:   "missing replay LSN",
			frames: [4][]uint64{{1, 2}, {3, 4}, {6}, {7}},
			lower:  4, upper: 6,
			want: ErrCommandWALAppliedLSNNonContig,
		},
		{
			name:   "closed terminal tail is a hard error",
			frames: [4][]uint64{{1, 2}, {3, 4}, {5}, {6}},
			lower:  4, upper: 5, partial: []int{1},
			want: commitlog.ErrCommandWALTerminalTail,
		},
		{
			name:   "compressed predecessor is validated in order",
			frames: [4][]uint64{{1, 2}, {3, 4}, {5}, {6}},
			lower:  4, upper: 5, workerRefusal: "compressed",
		},
		{
			name:   "large frame after valid prefix is validated in order",
			frames: [4][]uint64{{1, 2}, {3, 4}, {5}, {6}},
			lower:  4, upper: 5, workerRefusal: "large",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			requireExt4ForParallelWALCleanup(t, dir)
			for index, lsns := range test.frames {
				if test.workerRefusal != "" && index == 1 {
					writeParallelCleanupWorkerRefusalFrame(t, dir, lsns, test.workerRefusal == "compressed")
				} else {
					writeCommandWALSegmentFrames(t, dir, uint64(index+1), lsns...)
				}
			}
			segments, err := listWALSegments(dir)
			if err != nil || len(segments) != 4 {
				t.Fatalf("segments=%v err=%v", segments, err)
			}
			for _, index := range test.corrupt {
				path := segments[index].path
				payload, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				payload[len(payload)-1] ^= 1
				if err := os.WriteFile(path, payload, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for _, index := range test.partial {
				path := segments[index].path
				if err := os.Truncate(path, segments[index].size-1); err != nil {
					t.Fatal(err)
				}
			}
			segments, err = listWALSegments(dir)
			if err != nil || len(segments) != 4 {
				t.Fatalf("segments=%v err=%v", segments, err)
			}

			serial := newCommandWALCleanupScan(test.lower, test.upper, 0, nil)
			serial.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
			want, serialErr := serial.finish(serial.scanLockedDirectory(dir))
			if err := closeCommandWALCleanupDecisions(want); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(serialErr, test.want) {
				t.Fatalf("serial error=%v want=%v", serialErr, test.want)
			}

			split := newCommandWALCleanupScan(test.lower, test.upper, 0, nil)
			split.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
			if !split.scanClosedStreamPair(segments[0], segments[1], rootpublication.StableIdentity{}) {
				t.Fatal("ext4 stream pair was declined")
			}
			split.scanSegment(segments[2], false, false, nil)
			split.scanSegment(segments[3], true, false, nil)
			got, splitErr := split.finish(nil)
			if err := closeCommandWALCleanupDecisions(got); err != nil {
				t.Fatal(err)
			}
			if !errors.Is(splitErr, test.want) || fmt.Sprint(splitErr) != fmt.Sprint(serialErr) ||
				!reflect.DeepEqual(got, want) || !reflect.DeepEqual(split.options.seenLSNs, serial.options.seenLSNs) {
				t.Fatalf("split decisions=%+v error=%v\nserial decisions=%+v error=%v", got, splitErr, want, serialErr)
			}
		})
	}
}

func writeParallelCleanupWorkerRefusalFrame(t *testing.T, dir string, lsns []uint64, compressed bool) {
	t.Helper()
	dir = WALDirPath(dir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, commitlog.CommandSegmentName(0, 2))
	writer, err := commitlog.NewWriterWithOptions(path, commitlog.Options{Compress: compressed})
	if err != nil {
		t.Fatal(err)
	}
	valueSize := maxParallelWALCleanupWorkerFrameBytes
	if compressed {
		valueSize = 128 << 10
	}
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{
		{Op: commitlog.RawKVOpSet, Key: []byte("key"), Value: bytes.Repeat([]byte("b"), valueSize)},
	})
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	envelope := commitlog.CommandEnvelope{
		LSN: lsns[0], Kind: commitlog.CommandKindRawKVBatch, Scope: commitlog.CommandScopeRawKV,
		PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
	}
	if err := writer.AppendCommand(envelope); err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	envelope.LSN, envelope.Payload = lsns[1], payload
	if err := errors.Join(writer.AppendCommand(envelope), writer.Close()); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	buffer := new(commitlog.CleanupReadBuffer)
	reader, err := buffer.Open(file, commitlog.Options{})
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	first, firstErr := buffer.ReadValidatedBoundedCommandFrameLSN(reader, maxParallelWALCleanupWorkerFrameBytes)
	_, err = buffer.ReadValidatedBoundedCommandFrameLSN(reader, maxParallelWALCleanupWorkerFrameBytes)
	if closeErr := errors.Join(reader.Close(), file.Close()); first != lsns[0] || firstErr != nil ||
		!errors.Is(err, commitlog.ErrCommandWALCleanupReadLimit) || closeErr != nil {
		t.Fatalf("fixture first=%d err=%v, later bounded reader error=%v close=%v", first, firstErr, err, closeErr)
	}
}

func TestCommandWALCleanupParallelStreamRejectsAliases(t *testing.T) {
	dir := t.TempDir()
	requireExt4ForParallelWALCleanup(t, dir)
	writeCommandWALFrame(t, dir, 1, 1)
	first := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 1))
	second := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2))
	if err := os.Link(first, second); err != nil {
		t.Fatal(err)
	}
	segments, err := listWALSegments(dir)
	if err != nil || len(segments) != 2 {
		t.Fatalf("segments=%v err=%v", segments, err)
	}
	scan := newCommandWALCleanupScan(1, 1, 0, nil)
	scan.closedStreamBuffer = new(commitlog.CleanupReadBuffer)
	if scan.scanClosedStreamPair(segments[0], segments[1], rootpublication.StableIdentity{}) || scan.hardErr != nil {
		t.Fatalf("closed alias selected or failed: %v", scan.hardErr)
	}
	file, err := os.Open(first)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err := errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	if scan.scanClosedStreamPair(segments[0], segments[1], identity) || scan.hardErr != nil {
		t.Fatalf("active alias selected or failed: %v", scan.hardErr)
	}
}
