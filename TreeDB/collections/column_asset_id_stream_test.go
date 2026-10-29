package collections

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

func TestColumnAssetSegmentIDStreamingNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		columnAssetSegmentFileName(1), columnAssetSegmentFileName(4),
		columnAssetSegmentFileName(columnAssetDirectViewSegmentFileIDBase),
		columnAssetSegmentFileName(^uint32(0)),
		"unrelated", "segment-000000.tca", "segment-0000100.tca",
		"segment-000099.tca.extra", "segment-4294967296.tca",
		"segment-" + strings.Repeat("9", 200) + ".tca",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, columnAssetSegmentFileName(7)), 0700); err != nil {
		t.Fatal(err)
	}
	// A dangling symlink still occupies its name; no child stat is necessary.
	if err := os.Symlink("missing-target", filepath.Join(dir, columnAssetSegmentFileName(9))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(8)), nil, 0000); err != nil {
		t.Fatal(err)
	}
	id, err := nextColumnAssetSegmentFileID(columnAssetManagerNamespace{SegmentDir: dir})
	if err != nil || id != 10 {
		t.Fatalf("id=%d err=%v, want high-water 10 including nonregular names", id, err)
	}
}

func TestColumnAssetSegmentIDStreamingMatchesSortedPolicy(t *testing.T) {
	const upper = uint32(6)
	// Exhaustively compare all low-band occupancies with the existing sorted
	// policy, including full exhaustion and gaps below the maximum occupied ID.
	for mask := uint32(0); mask < 1<<(upper-2); mask++ {
		dir := t.TempDir()
		segments := []columnAssetReachabilitySegment{{fileID: 1}, {fileID: upper}}
		for id := upper - 1; id >= 2; id-- {
			if mask&(1<<(id-2)) != 0 {
				segments = append(segments, columnAssetReachabilitySegment{fileID: id})
				if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(id)), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, id := range []uint32{1, upper} {
			if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(id)), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		sort.Slice(segments, func(i, j int) bool { return segments[i].fileID < segments[j].fileID })
		want, wantErr := nextColumnAssetSegmentFileIDFromSorted(segments, upper)
		got, err := nextColumnAssetSegmentFileIDWithUpper(columnAssetManagerNamespace{SegmentDir: dir}, upper)
		if got != want || (err != nil) != (wantErr != nil) {
			t.Fatalf("mask=%d got=%d err=%v want=%d err=%v", mask, got, err, want, wantErr)
		}
	}
}

func TestColumnAssetSegmentIDStreamingWindowBoundary(t *testing.T) {
	dir := t.TempDir()
	upper := columnAssetSegmentIDWindowSize + 4
	for id := uint32(2); id < columnAssetSegmentIDWindowSize+2; id++ {
		if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(id)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Occupy the very end to force gap search, then require a second window.
	if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(upper-1)), nil, 0600); err != nil {
		t.Fatal(err)
	}
	ns := columnAssetManagerNamespace{SegmentDir: dir}
	want := columnAssetSegmentIDWindowSize + 2
	if got, err := nextColumnAssetSegmentFileIDWithUpper(ns, upper); err != nil || got != want {
		t.Fatalf("second-window gap: id=%d err=%v want=%d", got, err, want)
	}
	if err := os.WriteFile(filepath.Join(dir, columnAssetSegmentFileName(want)), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := nextColumnAssetSegmentFileIDWithUpper(ns, upper); err == nil || got != 0 {
		t.Fatalf("fully occupied: id=%d err=%v", got, err)
	}
}

func TestColumnAssetSegmentIDStreamingMissingAndOpenErrors(t *testing.T) {
	dir := t.TempDir()
	missing := columnAssetManagerNamespace{SegmentDir: filepath.Join(dir, "missing")}
	if got, err := nextColumnAssetSegmentFileID(missing); err != nil || got != columnAssetM12ASegmentFileID+1 {
		t.Fatalf("missing directory: id=%d err=%v", got, err)
	}
	if got, err := nextColumnAssetSegmentFileIDWithUpper(missing, 2); err == nil || got != 0 {
		t.Fatalf("empty low band: id=%d err=%v", got, err)
	}
	file := filepath.Join(dir, "regular-file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := nextColumnAssetSegmentFileID(columnAssetManagerNamespace{SegmentDir: file}); err == nil || got != 0 {
		t.Fatalf("non-directory: id=%d err=%v", got, err)
	}
}

type columnAssetIDScanRead struct {
	names []string
	err   error
}

type columnAssetIDScanReader struct {
	t        *testing.T
	reads    []columnAssetIDScanRead
	closeErr error
	closed   int
}

func (r *columnAssetIDScanReader) Readdirnames(n int) ([]string, error) {
	r.t.Helper()
	if n != columnAssetSegmentIDNamesPerRead || n <= 0 {
		r.t.Fatalf("unbounded directory read: n=%d", n)
	}
	if len(r.reads) == 0 {
		r.t.Fatal("unexpected extra directory read")
	}
	read := r.reads[0]
	r.reads = r.reads[1:]
	if len(read.names) > n {
		r.t.Fatalf("bad test chunk: %d names exceeds n=%d", len(read.names), n)
	}
	return read.names, read.err
}

func (r *columnAssetIDScanReader) Close() error {
	r.closed++
	return r.closeErr
}

func TestColumnAssetSegmentIDStreamingReadAndCloseErrors(t *testing.T) {
	readErr := errors.New("read failure")
	closeErr := errors.New("close failure")
	for _, tc := range []struct {
		name     string
		readErr  error
		closeErr error
		wantErr  error
	}{
		{"eof", io.EOF, nil, nil},
		{"close", io.EOF, closeErr, closeErr},
		{"read-and-close", readErr, closeErr, readErr},
		{"joined-eof-is-error", errors.Join(io.EOF, readErr), nil, readErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &columnAssetIDScanReader{t: t, reads: []columnAssetIDScanRead{
				{names: []string{"junk"}},
				{names: []string{columnAssetSegmentFileName(17)}, err: tc.readErr},
			}, closeErr: tc.closeErr}
			var visited []uint32
			err := scanColumnAssetSegmentFileIDNames(r, func(id uint32) { visited = append(visited, id) })
			if (err != nil) != (tc.wantErr != nil) || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("err=%v want=%v", err, tc.wantErr)
			}
			if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
				t.Fatalf("close error was lost: %v", err)
			}
			if r.closed != 1 || len(r.reads) != 0 || len(visited) != 1 || visited[0] != 17 {
				t.Fatalf("closed=%d unread=%d visited=%v", r.closed, len(r.reads), visited)
			}
		})
	}
	r := &columnAssetIDScanReader{t: t, reads: []columnAssetIDScanRead{{}}}
	if err := scanColumnAssetSegmentFileIDNames(r, func(uint32) { t.Fatal("visited empty read") }); !errors.Is(err, io.ErrNoProgress) || r.closed != 1 {
		t.Fatalf("empty nonterminal read: err=%v closed=%d", err, r.closed)
	}
}

func TestColumnAssetSegmentIDStreamingStableParent(t *testing.T) {
	if !rootpublication.StableNamespaceCreationSupported() {
		t.Skip("exact parent opening is unsupported")
	}
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "linked-parent")
	if err := os.Symlink(dir, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if got, err := nextColumnAssetSegmentFileID(columnAssetManagerNamespace{SegmentDir: link}); err == nil || got != 0 {
		t.Fatalf("symlink parent was accepted: id=%d err=%v", got, err)
	}
}
