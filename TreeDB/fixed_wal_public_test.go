package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

// This uses ordinary Open and SetSync plus an independent pre-Close image.
// Container witnesses are transport; unchanged V2 and persistent vlog evidence
// remain the replay/durability authority.
func TestFixedWALPublicStableImage(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires production fixed-container publication")
	}
	source := t.TempDir()
	opts := commandWALDurabilityProofOptions(source)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	first := bytes.Repeat([]byte("a"), 66<<10)
	if err = database.SetSync([]byte("a"), first); err != nil {
		t.Fatal(err)
	}
	if err = database.SetSync([]byte("b"), []byte("accepted-b")); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(source), "commit-l*.log"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("WAL paths=%v err=%v", paths, err)
	}
	image, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(image) < 4096 || string(image[:6]) != "TDBWAL" {
		t.Fatal("ordinary production journal did not select fixed container")
	}
	var ends []int64
	for off := int64(4096); off+96 < int64(len(image)) && string(image[off:off+6]) == "TDBEXT"; {
		end := int64(binary.LittleEndian.Uint64(image[off+48 : off+56]))
		ends = append(ends, end)
		off = end
	}
	if len(ends) < 2 {
		t.Fatalf("extents=%d", len(ends))
	}
	lastEnd := ends[len(ends)-1]
	for _, tc := range []struct {
		name    string
		mutate  func([]byte)
		corrupt bool
	}{
		{"intact", func([]byte) {}, false},
		{"torn_new_redundant_footer", func(p []byte) { clear(p[lastEnd-96 : lastEnd-16]) }, false},
		{"zero_ack_header", func(p []byte) { clear(p[4096 : 4096+96]) }, true},
		{"crc_damage_before_frontier", func(p []byte) { p[4096+96+20] ^= 1 }, true},
		{"unknown_reserved_suffix", func(p []byte) { p[lastEnd+64] = 1 }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "image")
			if err := copyPublicCommandWALCrashImage(source, target); err != nil {
				t.Fatal(err)
			}
			wal := filepath.Join(backenddb.WALDirPath(target), filepath.Base(paths[0]))
			changed := bytes.Clone(image)
			tc.mutate(changed)
			if err := os.WriteFile(wal, changed, 0600); err != nil {
				t.Fatal(err)
			}
			recovered, err := Open(commandWALDurabilityProofOptions(target))
			if tc.corrupt {
				if recovered != nil {
					_ = recovered.Close()
				}
				if !errors.Is(err, commitlog.ErrCorrupt) {
					t.Fatalf("Open err=%v want corruption", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			for key, want := range map[string][]byte{"a": first, "b": []byte("accepted-b")} {
				got, e := recovered.Get([]byte(key))
				if e != nil || !bytes.Equal(got, want) {
					t.Fatalf("recovered %s bytes=%d err=%v", key, len(got), e)
				}
			}
		})
	}
}

func TestFixedWALPublicIncompleteRelaxedExtentHorizon(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires production fixed-container publication")
	}
	for _, tc := range []struct {
		name                                     string
		cover, rotate, footerPresent, tornFooter bool
		corrupt                                  bool
	}{
		{name: "relaxed_suffix_above_ack_frontier"},
		{name: "later_durable_barrier", cover: true, corrupt: true},
		{name: "later_segment_durable_barrier", cover: true, rotate: true, corrupt: true},
		{name: "positive_footer_crc_damage", footerPresent: true, corrupt: true},
		{name: "torn_nonzero_footer_bad_data", tornFooter: true, corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := t.TempDir()
			opts := relaxedCommandWALDurablePrefixOptions(source)
			if tc.rotate {
				opts.CommandWALSegmentTargetBytes = 1
			}
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			if err = database.SetSync([]byte("a"), []byte("accepted-a")); err != nil {
				t.Fatal(err)
			}
			if err = database.Set([]byte("b"), []byte("relaxed-body-that-will-tear")); err != nil {
				t.Fatal(err)
			}
			if err = database.backend.FlushCommandWAL(false); err != nil {
				t.Fatal(err)
			}
			if tc.cover {
				if err = writeEmptyPublicCommandWALSync(database); err != nil {
					t.Fatal(err)
				}
			}
			crash := filepath.Join(t.TempDir(), "image")
			if err = copyPublicCommandWALCrashImage(source, crash); err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(crash), "commit-l*.log"))
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, path := range paths {
				image, e := os.ReadFile(path)
				if e != nil {
					t.Fatal(e)
				}
				for off := int64(4096); off+96 < int64(len(image)) && string(image[off:off+6]) == "TDBEXT"; {
					length := int64(binary.LittleEndian.Uint64(image[off+40 : off+48]))
					end := int64(binary.LittleEndian.Uint64(image[off+48 : off+56]))
					body := image[off+96 : off+96+length]
					if bytes.Contains(body, []byte("relaxed-body-that-will-tear")) {
						// Keep the outer bound and stable 56-byte V2 identity. Replace the
						// unwritten body/end range with its exact initialized reserve bytes.
						clear(image[off+96+8+56 : off+96+length])
						if !tc.footerPresent {
							clear(image[end-96 : end])
						}
						if tc.tornFooter {
							image[end-96] = 'T'
						}
						found = true
					}
					off = end
				}
				if e = os.WriteFile(path, image, 0600); e != nil {
					t.Fatal(e)
				}
			}
			if !found {
				t.Fatal("did not locate relaxed B extent")
			}
			recovered, err := Open(relaxedCommandWALDurablePrefixOptions(crash))
			if tc.corrupt {
				if recovered != nil {
					_ = recovered.Close()
				}
				if !errors.Is(err, commitlog.ErrCorrupt) {
					t.Fatalf("Open err=%v want corruption", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			got, e := recovered.Get([]byte("a"))
			if e != nil || string(got) != "accepted-a" {
				t.Fatalf("A=%q err=%v", got, e)
			}
			got, e = recovered.Get([]byte("b"))
			if e != nil || got != nil {
				t.Fatalf("discarded B=%q err=%v", got, e)
			}
			// A repair retry uses the normal reopened directory and must stay usable.
			if err = recovered.SetSync([]byte("c"), []byte("after-repair")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
