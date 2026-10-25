package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func writeEmptyFixedCleanupSegment(t *testing.T, dir string, seq uint64, capacity int64) string {
	t.Helper()
	if !commitlog.FixedWALCreationSupported() {
		t.Skip("requires fixed-container creation")
	}
	if err := os.MkdirAll(WALDirPath(dir), 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, seq))
	w, err := commitlog.NewFixedWriterWithCapacity(path, capacity)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func emptyFixedCleanupDecision(t *testing.T, decisions []commandWALSegmentCleanupDecision, path string) commandWALSegmentCleanupDecision {
	t.Helper()
	for _, decision := range decisions {
		if decision.Path == path {
			return decision
		}
	}
	t.Fatalf("missing positive empty-fixed decision for %s: %+v", path, decisions)
	return commandWALSegmentCleanupDecision{}
}

func TestCommandWALCleanupEmptyFixedFirstFrameRotation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity int64
		valueLen int
	}{
		{"small_initial_reserve", 4352, 128},
		{"oversized_first_atomic_frame", 4_199_424, 5 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			empty := writeEmptyFixedCleanupSegment(t, dir, 1, tc.capacity)
			info, err := os.Stat(empty)
			if err != nil {
				t.Fatal(err)
			}
			j, err := commitlog.OpenCommandJournal(WALDirPath(dir), commitlog.CommandJournalOptions{
				SegmentSeq: 1, InitialLSN: 1, SegmentTargetBytes: 16 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			key, value := []byte("retained"), bytes.Repeat([]byte("v"), tc.valueLen)
			payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{Op: commitlog.RawKVOpSet, Key: key, Value: value}})
			if err != nil {
				t.Fatal(err)
			}
			lsn, err := j.AppendCommand(commitlog.CommandEnvelope{
				Version: commitlog.CommandFrameVersionV2, Kind: commitlog.CommandKindRawKVBatch,
				Scope: commitlog.CommandScopeRawKV, PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
				DurabilityClass: commitlog.CommandDurabilityDurable, Payload: payload,
			})
			if err != nil || lsn != 2 {
				t.Fatalf("first append lsn=%d err=%v", lsn, err)
			}
			if err = j.Sync(); err != nil {
				t.Fatal(err)
			}
			if active := j.WriterBufferStats().ActivePath; active == empty {
				t.Fatal("actual first-frame physical pressure did not rotate the empty predecessor")
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 1, 2, 0)
			if err != nil {
				t.Fatal(err)
			}
			d := emptyFixedCleanupDecision(t, decisions, empty)
			if d.Size != info.Size() || d.LogicalSize != 0 || d.Frames != 0 || d.MinLSN != 0 || d.MaxLSN != 0 || !d.Covered || d.Active || d.file == nil {
				closeCommandWALCleanupDecisions(decisions)
				t.Fatalf("empty decision lost physical/logical/identity custody: %+v", d)
			}
			decisions, err = removeCoveredCommandWALSegments(decisions)
			if err != nil || !emptyFixedCleanupDecision(t, decisions, empty).Removed {
				t.Fatalf("empty predecessor removal: err=%v decisions=%+v", err, decisions)
			}
			// Reopen the retained exact replay segment; the frame above cleanup
			// frontier 1 must remain complete at its original timestamp/LSN 2.
			frames, err := commitlog.ScanCommandFramesV2(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2)), commitlog.Options{})
			if err != nil || len(frames) != 1 || frames[0].LSN != 2 {
				t.Fatalf("retained replay after reader reopen: frames=%d err=%v", len(frames), err)
			}
			ops, err := commitlog.DecodeRawKVBatchPayload(frames[0].Payload)
			if err != nil || len(ops) != 1 || !bytes.Equal(ops[0].Key, key) || !bytes.Equal(ops[0].Value, value) {
				t.Fatalf("retained value after reader reopen: ops=%d err=%v", len(ops), err)
			}
		})
	}
}

func TestCommandWALCleanupEmptyFixedEligibility(t *testing.T) {
	for _, variant := range []string{"fixed", "stream", "corrupt_reserve", "truncated", "partial_extent"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			path := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
			if variant != "fixed" {
				image, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				switch variant {
				case "stream":
					image = nil
				case "corrupt_reserve":
					image[len(image)-1] = 1
				case "truncated":
					image = image[:len(image)-1]
				case "partial_extent":
					image[4096] = 1
				}
				if err = os.WriteFile(path, image, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			writeEmptyFixedCleanupSegment(t, dir, 2, 8192)
			decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 1, 1, 0)
			defer closeCommandWALCleanupDecisions(decisions)
			if variant != "fixed" && variant != "stream" {
				if !errors.Is(err, commitlog.ErrCorrupt) {
					t.Fatalf("invalid container accepted: %v", err)
				}
				if _, statErr := os.Stat(path); statErr != nil {
					t.Fatalf("invalid container changed: %v", statErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if variant == "fixed" {
				d := emptyFixedCleanupDecision(t, decisions, path)
				if !d.Covered || d.Active || d.LogicalSize != 0 || d.Frames != 0 {
					t.Fatalf("empty fixed not eligible: %+v", d)
				}
			} else {
				for _, d := range decisions {
					if d.Path == path {
						t.Fatalf("empty stream gained deletion eligibility: %+v", d)
					}
				}
			}
			active := emptyFixedCleanupDecision(t, decisions, filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2)))
			if !active.Active || active.file != nil {
				t.Fatalf("empty active container not protected: %+v", active)
			}
			// Captured/current generation protection is independent of the
			// directory's latest-sequence inference, even for empty candidates.
			if err = retainCommandWALCleanupAuthoritySegments(decisions, commitlog.CommandJournalCleanupSnapshot{Lane: 0, SegmentSeq: 1}, commitlog.CommandJournalCleanupSnapshot{}); err != nil {
				t.Fatal(err)
			}
			for _, d := range decisions {
				if !d.Active {
					t.Fatalf("captured/future generation lost protection: %+v", d)
				}
			}
		})
	}
}

func TestCommandWALCleanupEmptyFixedPinAndReplacement(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "pin"
		if replace {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			if replace && runtime.GOOS == "windows" {
				t.Skip("Windows cannot rebind the retained open scan handle")
			}
			dir := t.TempDir()
			path := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
			writeCommandWALFrame(t, dir, 2, 2)
			decisions, err := scanCommandWALSegmentsForCleanupProof(dir, 1, 2, 0)
			if err != nil {
				t.Fatal(err)
			}
			identity := emptyFixedCleanupDecision(t, decisions, path).identity
			registry := rootpublication.NewIdentityPinRegistry()
			if err = registry.Observe(identity); err != nil {
				t.Fatal(err)
			}
			if replace {
				if err = os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
				_, err = removeCoveredCommandWALSegmentsWithRegistry(decisions, registry)
				if !errors.Is(err, ErrRecoveryRequired) {
					t.Fatalf("replacement not refused: %v", err)
				}
			} else {
				pin, e := registry.Pin(identity)
				if e != nil {
					t.Fatal(e)
				}
				_, err = removeCoveredCommandWALSegmentsWithRegistry(decisions, registry)
				pin.Release()
				if !errors.Is(err, rootpublication.ErrResourcePinned) {
					t.Fatalf("pinned empty identity not refused: %v", err)
				}
				decisions, err = scanCommandWALSegmentsForCleanupProof(dir, 1, 2, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = removeCoveredCommandWALSegmentsWithRegistry(decisions, registry); err != nil {
					t.Fatalf("released pin did not converge: %v", err)
				}
				if _, err = os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("released empty container not removed: %v", err)
				}
				return
			}
			if _, err = os.Stat(path); err != nil {
				t.Fatalf("replacement path removed: %v", err)
			}
		})
	}
}

func TestCommandWALCleanupEmptyFixedPhysicalAccountingAndNamespaceDebt(t *testing.T) {
	for _, point := range []durabilitycut.Point{durabilitycut.AfterWALOrAssetUnlink, durabilitycut.BeforeDeletionDirectorySync} {
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			path := writeEmptyFixedCleanupSegment(t, dir, 1, 8192)
			writeCommandWALFrame(t, dir, 2, 2)
			database := &DB{dir: dir, commandWAL: true, durability: DurabilityDurable}
			installCommandWALCleanupRootForTest(t, database, 1, 1)
			database.commandWALDurableLSN.Store(2)
			retained, err := os.Open(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2)))
			if err != nil {
				t.Fatal(err)
			}
			logical, err := commitlog.LogicalFileSize(retained)
			_ = retained.Close()
			if err != nil {
				t.Fatal(err)
			}
			database.commandWALClosedBytes.Store(logical)
			cutErr := errors.New("empty fixed namespace cut")
			fired := false
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if !fired && event.Resource == durabilitycut.ResourceCommandWAL && event.Point == point {
					fired = true
					return cutErr
				}
				return nil
			})
			err = database.CleanupCommandWALCoveredSegments(true)
			restore()
			if !errors.Is(err, cutErr) || !errors.Is(err, ErrRecoveryRequired) || !fired {
				t.Fatalf("namespace cut: fired=%v err=%v", fired, err)
			}
			if !database.commandWALCleanupNamespaceDirty.Load() || database.commandWALCleanupRemoved.Load() != 0 || database.commandWALCleanupUnlinkedPending.Load() != 1 {
				t.Fatal("empty unlink lost conservative namespace debt/accounting")
			}
			if got := database.commandWALClosedBytes.Load(); got != logical {
				t.Fatalf("empty physical reserve debited logical debt: got=%d want=%d", got, logical)
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("completed unlink not retained as mutation: %v", err)
			}
			if err = database.CleanupCommandWALCoveredSegments(true); err != nil {
				t.Fatal(err)
			}
			if database.commandWALCleanupNamespaceDirty.Load() || database.commandWALCleanupRemoved.Load() != 1 || database.commandWALCleanupBytes.Load() != 8192 {
				t.Fatal("empty physical bytes/segment did not settle after directory sync")
			}
			r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), commitlog.CommandSegmentName(0, 2)))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if env, err := r.ReadCommandFrame(); err != nil || env.LSN != 2 {
				t.Fatalf("retained replay after namespace failure: lsn=%d err=%v", env.LSN, err)
			}
			if _, err = r.ReadCommandFrame(); !errors.Is(err, io.EOF) {
				t.Fatalf("retained replay EOF: %v", err)
			}
		})
	}
}
