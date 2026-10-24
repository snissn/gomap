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
