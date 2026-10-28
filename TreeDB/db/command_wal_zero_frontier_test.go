package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// A single real durable append leaves the initial recovery-selectable root at
// LSN zero. Empty predecessors contain no commands needed by that fallback.
func TestCommandWALZeroFrontierRetiresEmptyReopenSegments(t *testing.T) {
	for _, coalesced := range []bool{false, true} {
		name := "single"
		if coalesced {
			name = "coalesced"
		}
		t.Run(name, func(t *testing.T) { testCommandWALZeroFrontierReopens(t, coalesced) })
	}
}

func testCommandWALZeroFrontierReopens(t *testing.T, coalesced bool) {
	dir := t.TempDir()
	options := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	wantLSN := uint64(1)
	if coalesced {
		// An admitted extra builder holds the real coordinator's publication gate.
		// Three ordinary relaxed appends therefore share one physical root publish,
		// independent of timer/scheduler speed; no root or WAL state is fabricated.
		builder, err := database.rootPublication.coordinator.AcquireBuilder(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer builder.Release()
		for i := 0; i < 3; i++ {
			if err := database.Set([]byte("retained"), []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		builder.Release()
		wantLSN = 3
	} else if err := database.SetSync([]byte("retained"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := zeroFrontierSnapshotForTest(database)
	if err != nil || source.ProofError != "" || source.CleanupThrough != 0 || source.DurableWALLSN != wantLSN {
		t.Fatalf("source is not a valid zero-frontier fixture: %+v; %v", source, err)
	}
	if raw, err := json.Marshal(source); err == nil {
		t.Logf("source: %s", raw)
	}
	if len(source.Segments) != 2 {
		t.Fatalf("source segments=%+v, want framed predecessor and active empty", source.Segments)
	}
	var framedPath, emptyPath string
	for _, segment := range source.Segments {
		if segment.Frames == wantLSN && segment.MaxLSN == wantLSN {
			framedPath = segment.Path
		}
		if segment.Frames == 0 && segment.Active {
			emptyPath = segment.Path
		}
	}
	if framedPath == "" || emptyPath == "" {
		t.Fatalf("unexpected source layout: %+v", source.Segments)
	}
	raw, err := os.ReadFile(framedPath)
	if err != nil {
		t.Fatal(err)
	}
	framedDigest := sha256.Sum256(raw)
	for cycle := 0; cycle < 3; cycle++ {
		database, err = Open(options)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := database.Get([]byte("retained")); err != nil || string(got) != "value" {
			t.Fatalf("cycle %d reopen value=%q: %v", cycle, got, err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		final, err := zeroFrontierSnapshotForTest(database)
		if err != nil || final.ProofError != "" || final.CleanupThrough != 0 {
			t.Fatalf("cycle %d proof=%+v: %v", cycle, final, err)
		}
		if raw, err := json.Marshal(final); err == nil {
			t.Logf("cycle %d final: %s", cycle, raw)
		}
		if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
			t.Fatalf("cycle %d empty predecessor retained with valid zero-frontier proof: %s; %v", cycle, emptyPath, err)
		}
		if len(final.Segments) != len(source.Segments) {
			t.Fatalf("cycle %d WAL grew: source=%+v final=%+v", cycle, source.Segments, final.Segments)
		}
		raw, err := os.ReadFile(framedPath)
		if err != nil || sha256.Sum256(raw) != framedDigest {
			t.Fatalf("cycle %d replay predecessor changed: %v", cycle, err)
		}
		for _, segment := range final.Segments {
			if segment.Active && segment.Frames == 0 {
				emptyPath = segment.Path
			}
		}
	}
	// Force actual selection of the older LSN-zero root. The retained frame is
	// the only source for recovering the acknowledged key from that fallback.
	if database.durableRoot.slotRecord[database.durableRoot.slot^1].AppliedCommandLSN != 0 {
		t.Fatal("fixture no longer has the initial fallback root")
	}
	corruptIndexPageByte(t, dir, database.metaPageID)
	database, err = Open(options)
	if err != nil {
		t.Fatalf("open older root and replay retained frame: %v", err)
	}
	if got, err := database.Get([]byte("retained")); err != nil || string(got) != "value" {
		t.Fatalf("fallback replay value=%q: %v", got, err)
	}
	if database.State().AppliedCommandLSN != wantLSN {
		t.Fatalf("fallback replay applied LSN=%d want %d", database.State().AppliedCommandLSN, wantLSN)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

// Snapshot reads the closed backend and the existing read-only cleanup scanner;
// it needs no production diagnostic API or altered cleanup authority.
type zeroFrontierSnapshot struct {
	Roots          [2]uint64
	DurableWALLSN  uint64
	CleanupThrough uint64
	ProofError     string
	Segments       []commandWALSegmentCleanupDecision
}

func zeroFrontierSnapshotForTest(database *DB) (zeroFrontierSnapshot, error) {
	out := zeroFrontierSnapshot{DurableWALLSN: database.commandWALDurableLSN.Load()}
	for i := range out.Roots {
		out.Roots[i] = database.durableRoot.slotRecord[i].AppliedCommandLSN
	}
	proof, err := captureDurableWALCleanupProofFromRuntimeV1(database.durableRoot, out.DurableWALLSN)
	if err != nil {
		out.ProofError = err.Error()
		return out, err
	}
	out.CleanupThrough = proof.cleanupThrough
	out.Segments, err = scanCommandWALSegmentsForCleanupProof(database.dir, proof.cleanupThrough, proof.durableWALLSN, database.walMaxSegmentBytes)
	closeCommandWALCleanupDecisions(out.Segments)
	return out, err
}

func TestCommandWALZeroFrontierRetainsProtectedFiles(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.SetSync([]byte("retained"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	emptyPath, _ := database.commandWALActiveSegmentSnapshot()
	if emptyPath == "" || database.CommandWALActiveBytes() != 0 {
		t.Fatal("source has no empty active segment")
	}
	proof, err := database.captureDurableWALCleanupProofV1()
	if err != nil || proof.cleanupThrough != 0 {
		t.Fatalf("proof=%+v: %v", proof, err)
	}
	file, err := os.Open(emptyPath)
	if err != nil {
		t.Fatal(err)
	}
	identity, identityErr := rootpublication.StableIdentityFromFile(file)
	closeErr := file.Close()
	if err := errors.Join(identityErr, closeErr); err != nil {
		t.Fatal(err)
	}
	var pin *rootpublication.IdentityPin
	err = database.commandJournal.WithCleanupSnapshot(proof.journal, func(registry *rootpublication.IdentityPinRegistry, _ commitlog.CommandJournalCleanupSnapshot) (bool, error) {
		if err := registry.Observe(identity); err != nil {
			return false, err
		}
		var pinErr error
		pin, pinErr = registry.Pin(identity)
		return false, errors.Join(pinErr, registry.Unobserve(identity))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	if err := database.RotateCommandWALActiveSegment(true); err != nil {
		t.Fatal(err)
	}
	// A zero-byte legacy/unknown stream is not a positively classified empty
	// fixed container, even though it has the command-lane filename shape.
	unknown := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(17, 1))
	if err := os.WriteFile(unknown, nil, 0600); err != nil {
		t.Fatal(err)
	}
	before := make(map[string][32]byte)
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, segment := range segments {
		raw, err := os.ReadFile(segment.path)
		if err != nil {
			t.Fatal(err)
		}
		before[segment.path] = sha256.Sum256(raw)
	}
	if err := database.CleanupCommandWALCoveredSegments(false); !errors.Is(err, rootpublication.ErrResourcePinned) {
		t.Fatalf("pinned empty predecessor cleanup=%v", err)
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(raw) != digest {
			t.Fatalf("pinned cleanup changed %s: %v", path, err)
		}
	}
	pin.Release()
	if err := database.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(emptyPath); !os.IsNotExist(err) {
		t.Fatalf("unpinned empty predecessor remains: %v", err)
	}
	delete(before, emptyPath)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(raw) != digest {
			t.Fatalf("cleanup changed protected file %s: %v", path, err)
		}
	}
}

// A large relaxed append rotates the initially empty fixed container while
// retaining its producer dependency pin. Opportunistic Close must retain that
// legitimate pin, and the actual command must remain recoverable.
func TestCommandWALZeroFrontierCloseRetainsProducerPin(t *testing.T) {
	dir := t.TempDir()
	options := Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true}
	database, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	value := bytes.Repeat([]byte("x"), commandWALDeferredPointBufferRetainSize+8192)
	if _, err := database.AppendRawKVPointCommandWALTrusted(commitlog.RawKVOpSet, []byte("large"), value, false); err != nil {
		t.Fatal(err)
	}
	if err := database.FlushCommandWAL(false); err != nil {
		t.Fatal(err)
	}
	if err := database.CleanupCommandWALCoveredSegments(false); !errors.Is(err, rootpublication.ErrResourcePinned) || !errors.Is(err, errDurableWALCleanupProofUnavailable) {
		t.Fatalf("producer pin cleanup=%v", err)
	}
	segments, err := listWALSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("rotation fixture has %d segments", len(segments))
	}
	before := make(map[string][32]byte)
	for _, segment := range segments {
		raw, err := os.ReadFile(segment.path)
		if err != nil {
			t.Fatal(err)
		}
		before[segment.path] = sha256.Sum256(raw)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(raw) != digest {
			t.Fatalf("pinned Close changed %s: %v", path, err)
		}
	}
	database, err = Open(options)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := database.Get([]byte("large")); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("recovered value bytes=%d: %v", len(got), err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommandWALZeroFrontierNamespaceAbsenceFilter(t *testing.T) {
	for _, variant := range []string{"sole_active", "command_stream", "legacy_commit", "legacy_wal", "zero_sequence", "directory", "dangling_candidate", "missing_active", "replaced_active", "unreadable_active", "unreadable_directory"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			walDir := WALDirPath(dir)
			if err := os.MkdirAll(walDir, 0700); err != nil {
				t.Fatal(err)
			}
			active := filepath.Join(walDir, commitlog.CommandSegmentName(0, 1))
			if err := os.WriteFile(active, nil, 0600); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(active)
			if err != nil {
				t.Fatal(err)
			}
			identity, identityErr := rootpublication.StableIdentityFromFile(file)
			if err := errors.Join(identityErr, file.Close()); err != nil {
				t.Fatal(err)
			}
			snapshot := commitlog.CommandJournalCleanupSnapshot{ActivePath: active, ActiveIdentity: identity}
			candidate := filepath.Join(walDir, commitlog.CommandSegmentName(17, 1))
			wantErr := false
			switch variant {
			case "command_stream", "legacy_commit", "legacy_wal", "zero_sequence":
				if variant == "legacy_commit" {
					candidate = filepath.Join(walDir, "commit-000002.log")
				}
				if variant == "legacy_wal" {
					candidate = filepath.Join(walDir, "wal-000002.log")
				}
				if variant == "zero_sequence" {
					candidate = filepath.Join(walDir, "commit-l0-000000.log")
				}
				if err := os.WriteFile(candidate, []byte("unknown or corrupt payload"), 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(candidate, 0700); err != nil {
					t.Fatal(err)
				}
			case "dangling_candidate":
				if err := os.Symlink("missing", candidate); err != nil {
					t.Skipf("symlinks unavailable: %v", err)
				}
			case "missing_active", "replaced_active", "unreadable_active":
				// Keep the original inode alive so replacement cannot reuse it.
				if err := os.Rename(active, active+".old"); err != nil {
					t.Fatal(err)
				}
				if variant == "replaced_active" {
					if err := os.WriteFile(active, nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if variant == "unreadable_active" {
					if err := os.Symlink("missing", active); err != nil {
						t.Skipf("symlinks unavailable: %v", err)
					}
					wantErr = true
				}
			case "unreadable_directory":
				if err := os.Rename(walDir, walDir+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(walDir, nil, 0600); err != nil {
					t.Fatal(err)
				}
				wantErr = true
			}
			inactive, err := commandWALHasInactiveCleanupFiles(dir, snapshot)
			if wantErr {
				if err == nil {
					t.Fatal("filesystem error became a no-candidate result")
				}
				return
			}
			if err != nil || inactive != (variant != "sole_active") {
				t.Fatalf("inactive=%t err=%v", inactive, err)
			}
		})
	}
}

// Counters, rather than timings, verify the common empty database avoids
// physical reserve scans and a larger real rotated set still takes the complete
// scanner and converges to the sole protected active container.
func TestCommandWALZeroFrontierEmptyNamespaceScanBounds(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	before := database.commandWALCleanupScans.Load()
	if err := database.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatal(err)
	}
	if got := database.commandWALCleanupScans.Load(); got != before {
		t.Fatalf("sole active scanned: %d -> %d", before, got)
	}
	const rotations = 32
	for i := 0; i < rotations; i++ {
		if err := database.RotateCommandWALActiveSegment(true); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.CleanupCommandWALCoveredSegments(false); err != nil {
		t.Fatal(err)
	}
	if got := database.commandWALCleanupScans.Load(); got != before+1 {
		t.Fatalf("rotated namespace scans=%d want %d", got, before+1)
	}
	segments, err := listWALSegments(dir)
	if err != nil || len(segments) != 1 {
		t.Fatalf("rotated namespace did not converge: %d; %v", len(segments), err)
	}
	if got := database.commandWALCleanupRemoved.Load(); got != rotations {
		t.Fatalf("removed=%d want %d", got, rotations)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := database.commandWALCleanupScans.Load(); got != before+1 {
		t.Fatalf("sole active Close scanned: %d", got)
	}
}
