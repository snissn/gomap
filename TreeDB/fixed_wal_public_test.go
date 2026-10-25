package treedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// seedPublicFixedSuccessor constructs the actual next canonical segment. The
// existing backend rotation opens it, retains ownership and inspects its format.
func seedPublicFixedSuccessor(t *testing.T, database *DB) {
	t.Helper()
	active := database.Stats()["treedb.command_wal.writer.path"]
	var seq uint64
	if n, err := fmt.Sscanf(filepath.Base(active), "commit-l0-%d.log", &seq); err != nil || n != 1 || seq == ^uint64(0) {
		t.Fatalf("active command segment=%q: %v", active, err)
	}
	w, err := commitlog.NewFixedWriter(filepath.Join(filepath.Dir(active), commitlog.CommandSegmentName(0, seq+1)), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
}

func requirePublicFixedWAL(t *testing.T, database *DB) {
	t.Helper()
	if database.Stats()["treedb.command_wal.writer.container"] != "fixed" {
		seedPublicFixedSuccessor(t, database)
		if err := database.backend.RotateCommandWALActiveSegment(false); err != nil {
			t.Fatal(err)
		}
	}
	if database.Stats()["treedb.command_wal.writer.container"] != "fixed" {
		t.Fatal("actual selected segment is not fixed")
	}
}

func publicFixedWALPaths(t *testing.T, dir string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(dir), "commit-l*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var fixed []string
	for _, path := range paths {
		data, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if len(data) >= 6 && string(data[:6]) == "TDBWAL" {
			fixed = append(fixed, path)
		}
	}
	return fixed
}

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
	requirePublicFixedWAL(t, database)
	first := bytes.Repeat([]byte("a"), 66<<10)
	if err = database.SetSync([]byte("a"), first); err != nil {
		t.Fatal(err)
	}
	if err = database.SetSync([]byte("b"), []byte("accepted-b")); err != nil {
		t.Fatal(err)
	}
	paths := publicFixedWALPaths(t, source)
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

func TestFixedWALPublicRepairInterruptedOpenRetries(t *testing.T) {
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container atomic publication")
	}
	source := t.TempDir()
	database, err := Open(relaxedCommandWALDurablePrefixOptions(source))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	requirePublicFixedWAL(t, database)
	if err = database.SetSync([]byte("a"), []byte("accepted-a")); err != nil {
		t.Fatal(err)
	}
	const marker = "relaxed-repair-body"
	if err = database.Set([]byte("b"), []byte(marker)); err != nil {
		t.Fatal(err)
	}
	if err = database.backend.FlushCommandWAL(false); err != nil {
		t.Fatal(err)
	}
	template := filepath.Join(t.TempDir(), "template")
	if err = copyPublicCommandWALCrashImage(source, template); err != nil {
		t.Fatal(err)
	}
	paths := publicFixedWALPaths(t, template)
	if err != nil || len(paths) != 1 {
		t.Fatalf("WAL paths=%v err=%v", paths, err)
	}
	original, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	keptEnd := int64(0)
	for off := int64(4096); off+96 < int64(len(original)) && string(original[off:off+6]) == "TDBEXT"; {
		length := int64(binary.LittleEndian.Uint64(original[off+40 : off+48]))
		end := int64(binary.LittleEndian.Uint64(original[off+48 : off+56]))
		if bytes.Contains(original[off+96:off+96+length], []byte(marker)) {
			keptEnd = off
			clear(original[off+96+8+56 : off+96+length])
			clear(original[end-96 : end])
			break
		}
		off = end
	}
	if keptEnd == 0 {
		t.Fatal("missing classifiable relaxed suffix")
	}
	for _, cut := range []string{"create", "before-private-sync", "after-private-sync", "rename", "before-directory-sync", "after-directory-sync"} {
		t.Run(cut, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "image")
			if err := copyPublicCommandWALCrashImage(template, dir); err != nil {
				t.Fatal(err)
			}
			wal := filepath.Join(backenddb.WALDirPath(dir), filepath.Base(paths[0]))
			if err := os.WriteFile(wal, original, 0600); err != nil {
				t.Fatal(err)
			}
			retained, err := os.Open(wal)
			if err != nil {
				t.Fatal(err)
			}
			defer retained.Close()
			parent, err := rootpublication.OpenStableParent(filepath.Dir(wal))
			if err != nil {
				t.Fatal(err)
			}
			defer parent.Close()
			parentInfo, err := parent.Stat()
			if err != nil {
				t.Fatal(err)
			}
			var privateInfo os.FileInfo
			injected := errors.New("interrupted fixed-WAL startup repair")
			fired := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Resource != durabilitycut.ResourceCommandWAL {
					return nil
				}
				selected := ""
				if event.Namespace == durabilitycut.NamespaceCreate && strings.HasPrefix(event.StableNewName, ".wal-repair-") {
					if event.StableParent == nil || event.StablePath == nil {
						t.Fatal("private creation lacks retained handles")
					}
					actualParent, e := event.StableParent.Stat()
					if e != nil || !os.SameFile(actualParent, parentInfo) {
						t.Fatalf("repair used another exact parent: %v", e)
					}
					if e = rootpublication.ValidateStableChildLink(event.StableParent, event.StablePath, event.StableNewName); e != nil {
						t.Fatal(e)
					}
					privateInfo, e = event.StablePath.Stat()
					if e != nil {
						t.Fatal(e)
					}
					selected = "create"
				}
				if privateInfo == nil {
					return nil
				}
				if event.Point == durabilitycut.BeforeDependencyFileSync || event.Point == durabilitycut.AfterDependencyFileSync {
					if event.StablePath == nil {
						return nil
					}
					actual, e := event.StablePath.Stat()
					if e != nil || !os.SameFile(actual, privateInfo) {
						t.Fatalf("repair file barrier changed exact identity: %v", e)
					}
					selected = "before-private-sync"
					if event.Point == durabilitycut.AfterDependencyFileSync {
						selected = "after-private-sync"
					}
				}
				if event.Namespace == durabilitycut.NamespaceRename && strings.HasPrefix(event.StableOldName, ".wal-repair-") {
					current, e := rootpublication.OpenStableChildFile(event.StableParent, event.StableNewName, os.O_RDONLY, 0)
					if e != nil {
						t.Fatal(e)
					}
					actual, e := current.Stat()
					_ = current.Close()
					if e != nil || !os.SameFile(actual, privateInfo) || event.NewPath != wal {
						t.Fatalf("repair publication changed exact identity/name: %v", e)
					}
					selected = "rename"
				}
				if event.StablePath != nil && (event.Point == durabilitycut.BeforeNewFileDirectorySync || event.Point == durabilitycut.AfterNewFileDirectorySync) {
					actual, e := event.StablePath.Stat()
					if e != nil || !os.SameFile(actual, parentInfo) {
						t.Fatalf("repair namespace barrier changed exact parent: %v", e)
					}
					selected = "before-directory-sync"
					if event.Point == durabilitycut.AfterNewFileDirectorySync {
						selected = "after-directory-sync"
					}
				}
				if selected == cut {
					fired = true
					return injected
				}
				return nil
			})
			defer func() {
				if restore != nil {
					restore()
				}
			}()
			failed, openErr := Open(relaxedCommandWALDurablePrefixOptions(dir))
			restore()
			restore = nil
			if failed != nil {
				_ = failed.Close()
			}
			if !fired || !errors.Is(openErr, injected) {
				t.Fatalf("first Open cut=%t err=%v", fired, openErr)
			}
			heldImage := make([]byte, len(original))
			if _, e := retained.ReadAt(heldImage, 0); e != nil || !bytes.Equal(heldImage, original) {
				t.Fatalf("repair changed retained original object: %v", e)
			}
			image, err := os.ReadFile(wal)
			if err != nil || len(image) != len(original) || !bytes.Equal(image[:keptEnd], original[:keptEnd]) {
				t.Fatalf("repair changed acknowledged witnesses/prefix/capacity: %v", err)
			}
			recovered, err := Open(relaxedCommandWALDurablePrefixOptions(dir))
			if err != nil {
				t.Fatalf("ordinary Open retry: %v", err)
			}
			for key, want := range map[string]string{"a": "accepted-a", "b": ""} {
				got, e := recovered.Get([]byte(key))
				if e != nil || string(got) != want || (key == "b" && got != nil) {
					t.Fatalf("recovered %s=%q want%q err=%v", key, got, want, e)
				}
			}
			if err = recovered.SetSync([]byte("c"), []byte("after-repair")); err != nil {
				t.Fatal(err)
			}
			if err = recovered.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err = Open(relaxedCommandWALDurablePrefixOptions(dir))
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			got, err := recovered.Get([]byte("c"))
			if err != nil || string(got) != "after-repair" {
				t.Fatalf("post-repair ACK=%q err=%v", got, err)
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
			requirePublicFixedWAL(t, database)
			if err = database.SetSync([]byte("a"), []byte("accepted-a")); err != nil {
				t.Fatal(err)
			}
			if tc.rotate {
				// Stable automatic rotation requires a fresh O_EXCL successor.
				// Select the real explicit-format successor with ordinary rotation
				// before accepting b; the later barrier still crosses segments.
				seedPublicFixedSuccessor(t, database)
				if err = database.backend.RotateCommandWALActiveSegment(false); err != nil {
					t.Fatal(err)
				}
				if database.Stats()["treedb.command_wal.writer.container"] != "fixed" {
					t.Fatal("horizon fixture did not select fixed successor")
				}
			}
			if err = database.Set([]byte("b"), []byte("relaxed-body-that-will-tear")); err != nil {
				t.Fatal(err)
			}
			if err = database.backend.FlushCommandWAL(false); err != nil {
				t.Fatal(err)
			}
			relaxedSegment := database.Stats()["treedb.command_wal.writer.path"]
			if tc.cover {
				if err = writeEmptyPublicCommandWALSync(database); err != nil {
					t.Fatal(err)
				}
			}
			if tc.rotate && database.Stats()["treedb.command_wal.writer.path"] == relaxedSegment {
				t.Fatal("durable barrier did not move beyond the relaxed extent's physical segment")
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
						if filepath.Base(path) != filepath.Base(relaxedSegment) {
							t.Fatal("tear target differs from selected relaxed segment")
						}
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
