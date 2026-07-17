package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCompatibleLeafGenerationManifestPostRenameFailurePoisonsAndAdvancesRevision(t *testing.T) {
	leafDir := t.TempDir()
	store := newLeafGenerationManifestStore(leafDir, nil, leafGenerationManifestCompatibility, nil)
	defer store.Close()
	manifest := newLeafGenerationManifest(1)
	cut := errors.New("post-rename observation cut")
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Namespace == durabilitycut.NamespaceRename && event.NewPath == leafGenerationManifestPath(leafDir) {
			return cut
		}
		return nil
	})
	_, err := store.Replace(manifest)
	restore()
	if !errors.Is(err, cut) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Replace error=%v, want cut + ErrRecoveryRequired", err)
	}
	if manifest.ManifestRevision != 1 {
		t.Fatalf("ManifestRevision=%d, want committed revision 1", manifest.ManifestRevision)
	}
	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil || !ok || loaded.ManifestRevision != 1 {
		t.Fatalf("persisted manifest: revision=%v ok=%v err=%v", func() uint64 {
			if loaded == nil {
				return 0
			}
			return loaded.ManifestRevision
		}(), ok, err)
	}
	if _, err := store.Replace(manifest.clone()); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("poisoned retry error=%v, want ErrRecoveryRequired", err)
	}
}

type manifestTestLeafPageLog struct {
	path   string
	fileID uint32
}

func (l *manifestTestLeafPageLog) AppendLeafPage([]byte) (page.LeafLogPtr, error) {
	return page.LeafLogPtr{}, errors.New("unused in test")
}

func (l *manifestTestLeafPageLog) Flush() error { return nil }

func (l *manifestTestLeafPageLog) Sync() error { return nil }

func (l *manifestTestLeafPageLog) CurrentValueLogSegment() (string, uint32, bool) {
	if l == nil || l.path == "" || l.fileID == 0 {
		return "", 0, false
	}
	return l.path, l.fileID, true
}

func createLeafGenerationTestSegment(t *testing.T, leafDir string, lane, seq uint32) (string, uint32) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(leafDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return path, fileID
}

func TestLeafGenerationManifest_SaveLoadRoundTrip(t *testing.T) {
	leafDir := t.TempDir()
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{
				GenerationID:       1,
				State:              leafGenerationStateSealed,
				FileIDs:            []uint32{101, 102},
				CreatedCommitSeq:   11,
				SealedCommitSeq:    19,
				PublishedCommitSeq: 19,
			},
			{
				GenerationID:       2,
				State:              leafGenerationStateWritable,
				FileIDs:            []uint32{201},
				CreatedCommitSeq:   20,
				PublishedCommitSeq: 20,
			},
		},
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatalf("saveLeafGenerationManifest: %v", err)
	}

	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest to exist")
	}
	if loaded.CurrentGenerationID != manifest.CurrentGenerationID {
		t.Fatalf("CurrentGenerationID=%d, want %d", loaded.CurrentGenerationID, manifest.CurrentGenerationID)
	}
	if loaded.NextGenerationID != manifest.NextGenerationID {
		t.Fatalf("NextGenerationID=%d, want %d", loaded.NextGenerationID, manifest.NextGenerationID)
	}
	if len(loaded.Generations) != len(manifest.Generations) {
		t.Fatalf("len(Generations)=%d, want %d", len(loaded.Generations), len(manifest.Generations))
	}
	if got, want := loaded.Generations[0].FileIDs[1], manifest.Generations[0].FileIDs[1]; got != want {
		t.Fatalf("generation[0].FileIDs[1]=%d, want %d", got, want)
	}
}

func TestLeafGenerationManifest_LoadRejectsUnsupportedVersion(t *testing.T) {
	leafDir := t.TempDir()
	path := leafGenerationManifestPath(leafDir)
	if err := os.WriteFile(path, []byte(`{"version":999,"current_generation_id":1,"next_generation_id":2,"generations":[{"generation_id":1,"state":"writable"}]}`), 0o600); err != nil {
		t.Fatalf("WriteFile(manifest): %v", err)
	}
	_, ok, err := loadLeafGenerationManifest(leafDir)
	if !errors.Is(err, ErrLeafGenerationManifestIncompatible) {
		t.Fatalf("load error=%v want ErrLeafGenerationManifestIncompatible", err)
	}
	if ok {
		t.Fatalf("expected ok=false on unsupported version")
	}
}

func TestLeafGenerationManifest_LoadRejectsVersionOneWithoutMigration(t *testing.T) {
	leafDir := t.TempDir()
	path := leafGenerationManifestPath(leafDir)
	data := []byte(`{"version":1,"current_generation_id":1,"next_generation_id":2,"generations":[{"generation_id":1,"state":"writable"}]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := loadLeafGenerationManifest(leafDir); !errors.Is(err, ErrLeafGenerationManifestIncompatible) || ok {
		t.Fatalf("load v1 ok=%v error=%v want typed incompatibility", ok, err)
	}
}

func TestLeafGenerationManifest_LoadClassifiesPersistedDocumentCorruption(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantSyntax bool
	}{
		{
			name:       "malformed_json",
			data:       `{"version":2,"manifest_revision":1,`,
			wantSyntax: true,
		},
		{
			name: "structurally_invalid_v2",
			data: `{"version":2,"manifest_revision":1,"current_generation_id":1,"next_generation_id":2,"generations":[]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leafDir := t.TempDir()
			if err := os.WriteFile(leafGenerationManifestPath(leafDir), []byte(tt.data), 0o600); err != nil {
				t.Fatal(err)
			}
			_, ok, err := loadLeafGenerationManifest(leafDir)
			if ok || !errors.Is(err, ErrLeafGenerationManifestIncompatible) {
				t.Fatalf("load ok=%v error=%v want false, ErrLeafGenerationManifestIncompatible", ok, err)
			}
			if tt.wantSyntax {
				var syntaxErr *json.SyntaxError
				if !errors.As(err, &syntaxErr) {
					t.Fatalf("load error=%v does not preserve json.SyntaxError", err)
				}
			}
		})
	}
}

func TestCompatibleLeafGenerationManifestReplacementPreservesPersistedSyntaxError(t *testing.T) {
	leafDir := t.TempDir()
	if err := os.WriteFile(leafGenerationManifestPath(leafDir), []byte(`{"version":2,"manifest_revision":1,`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newLeafGenerationManifestStore(leafDir, nil, leafGenerationManifestCompatibility, nil)
	defer store.Close()

	_, err := store.Replace(newLeafGenerationManifest(1))
	if !errors.Is(err, ErrLeafGenerationManifestIncompatible) {
		t.Fatalf("Replace error=%v want ErrLeafGenerationManifestIncompatible", err)
	}
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("Replace error=%v does not preserve json.SyntaxError", err)
	}
}

func TestLeafGenerationManifest_LoadDoesNotClassifyFilesystemIOErrorAsIncompatible(t *testing.T) {
	leafDir := t.TempDir()
	if err := os.Mkdir(leafGenerationManifestPath(leafDir), 0o700); err != nil {
		t.Fatal(err)
	}
	_, ok, err := loadLeafGenerationManifest(leafDir)
	if err == nil || ok {
		t.Fatalf("load directory as manifest ok=%v error=%v want filesystem error", ok, err)
	}
	if errors.Is(err, ErrLeafGenerationManifestIncompatible) {
		t.Fatalf("filesystem error=%v must not be classified as ErrLeafGenerationManifestIncompatible", err)
	}
}

func TestLeafGenerationManifest_RegisterCurrentGenerationFileID(t *testing.T) {
	manifest := newLeafGenerationManifest(10)
	changed, err := manifest.registerCurrentGenerationFileID(77, 44)
	if err != nil {
		t.Fatalf("registerCurrentGenerationFileID: %v", err)
	}
	if !changed {
		t.Fatalf("expected first file registration to change manifest")
	}
	if got, want := len(manifest.Generations), 1; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	if got, want := len(manifest.Generations[0].FileIDs), 1; got != want {
		t.Fatalf("len(FileIDs)=%d, want %d", got, want)
	}
	if got, want := manifest.Generations[0].FileIDs[0], uint32(77); got != want {
		t.Fatalf("FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := manifest.Generations[0].PublishedCommitSeq, uint64(44); got != want {
		t.Fatalf("PublishedCommitSeq=%d, want %d", got, want)
	}
	changed, err = manifest.registerCurrentGenerationFileID(77, 99)
	if err != nil {
		t.Fatalf("registerCurrentGenerationFileID duplicate: %v", err)
	}
	if changed {
		t.Fatalf("expected duplicate file registration to be a no-op")
	}
	if got, want := manifest.Generations[0].PublishedCommitSeq, uint64(44); got != want {
		t.Fatalf("PublishedCommitSeq after duplicate=%d, want %d", got, want)
	}
}

func TestLeafGenerationManifest_RegisterCurrentGenerationFileID_DedupesAcrossGenerations(t *testing.T) {
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{77}, CreatedCommitSeq: 10, SealedCommitSeq: 20, PublishedCommitSeq: 20},
			{GenerationID: 2, State: leafGenerationStateWritable, CreatedCommitSeq: 30, PublishedCommitSeq: 30},
		},
	}

	changed, err := manifest.registerCurrentGenerationFileID(77, 99)
	if err != nil {
		t.Fatalf("registerCurrentGenerationFileID duplicate across generations: %v", err)
	}
	if changed {
		t.Fatalf("expected duplicate file registration across generations to be a no-op")
	}
	if got, want := manifest.CurrentGenerationID, uint64(2); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if got, want := manifest.NextGenerationID, uint64(3); got != want {
		t.Fatalf("NextGenerationID=%d, want %d", got, want)
	}
	if got := manifest.Generations[1].FileIDs; len(got) != 0 {
		t.Fatalf("current FileIDs=%v, want empty after duplicate", got)
	}
}

func TestLeafGenerationManifest_RegisterCurrentGenerationFileID_IgnoresDeletedGenerationDuplicates(t *testing.T) {
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{77}, CreatedCommitSeq: 10, DeletedCommitSeq: 20, PublishedCommitSeq: 20},
			{GenerationID: 2, State: leafGenerationStateWritable, CreatedCommitSeq: 30, PublishedCommitSeq: 30},
		},
	}

	changed, err := manifest.registerCurrentGenerationFileID(77, 99)
	if err != nil {
		t.Fatalf("registerCurrentGenerationFileID deleted duplicate: %v", err)
	}
	if !changed {
		t.Fatalf("expected deleted duplicate file registration to update current generation")
	}
	if got, want := manifest.Generations[1].FileIDs, []uint32{77}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current FileIDs=%v, want %v", got, want)
	}
}

func TestLeafGenerationManifest_RegisterCurrentGenerationFileID_RollsGenerationOnNewFile(t *testing.T) {
	manifest := newLeafGenerationManifest(10)
	if _, err := manifest.registerCurrentGenerationFileID(77, 44); err != nil {
		t.Fatalf("registerCurrentGenerationFileID first: %v", err)
	}
	changed, err := manifest.registerCurrentGenerationFileID(88, 55)
	if err != nil {
		t.Fatalf("registerCurrentGenerationFileID rollover: %v", err)
	}
	if !changed {
		t.Fatalf("expected rollover registration to change manifest")
	}
	if got, want := manifest.CurrentGenerationID, uint64(2); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if got, want := manifest.NextGenerationID, uint64(3); got != want {
		t.Fatalf("NextGenerationID=%d, want %d", got, want)
	}
	if got, want := len(manifest.Generations), 2; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	sealed := manifest.Generations[0]
	if got, want := sealed.State, leafGenerationStateSealed; got != want {
		t.Fatalf("sealed.State=%q, want %q", got, want)
	}
	if got, want := sealed.SealedCommitSeq, uint64(55); got != want {
		t.Fatalf("sealed.SealedCommitSeq=%d, want %d", got, want)
	}
	current := manifest.Generations[1]
	if got, want := current.State, leafGenerationStateWritable; got != want {
		t.Fatalf("current.State=%q, want %q", got, want)
	}
	if got, want := current.FileIDs[0], uint32(88); got != want {
		t.Fatalf("current.FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := current.CreatedCommitSeq, uint64(55); got != want {
		t.Fatalf("current.CreatedCommitSeq=%d, want %d", got, want)
	}
}

func TestLoadOrCreateLeafGenerationManifest_ReadOnlyMissingReturnsSynthetic(t *testing.T) {
	leafDir := t.TempDir()
	manifest, err := loadOrCreateLeafGenerationManifest(leafDir, 77, true)
	if err != nil {
		t.Fatalf("loadOrCreateLeafGenerationManifest: %v", err)
	}
	if manifest == nil {
		t.Fatalf("expected manifest")
	}
	if got, want := manifest.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("expected no manifest file written for read-only open, got %v", err)
	}
}

func TestParseLeafGenerationBootstrapFileName(t *testing.T) {
	cases := []struct {
		name string
		lane uint32
		seq  uint32
		ok   bool
	}{
		{name: "value-l3-12.log", lane: 3, seq: 12, ok: true},
		{name: "value-l3-12.log.tmp", ok: false},
		{name: "value-l3-12.log~", ok: false},
		{name: "value-l3-12", ok: false},
		{name: "value-l3-x.log", ok: false},
		{name: "value-l3-12-extra.log", ok: false},
	}
	for _, tc := range cases {
		lane, seq, ok := parseLeafGenerationBootstrapFileName(tc.name)
		if ok != tc.ok || lane != tc.lane || seq != tc.seq {
			t.Fatalf("parseLeafGenerationBootstrapFileName(%q)=(%d,%d,%v), want (%d,%d,%v)", tc.name, lane, seq, ok, tc.lane, tc.seq, tc.ok)
		}
	}
}

func TestLoadOrCreateLeafGenerationManifest_BootstrapsExistingLeafFiles(t *testing.T) {
	leafDir := t.TempDir()
	_, fileID1 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 1)
	_, fileID2 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 2)
	_, fileID3 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 3)
	createLeafGenerationTestSegment(t, leafDir, 0, 9)

	manifest, err := loadOrCreateLeafGenerationManifest(leafDir, 77, true)
	if err != nil {
		t.Fatalf("loadOrCreateLeafGenerationManifest(read-only): %v", err)
	}
	if manifest == nil {
		t.Fatalf("expected manifest")
	}
	if got, want := manifest.CurrentGenerationID, uint64(3); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if got, want := manifest.NextGenerationID, uint64(4); got != want {
		t.Fatalf("NextGenerationID=%d, want %d", got, want)
	}
	if got, want := len(manifest.Generations), 3; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	for i, wantRaw := range []uint32{
		page.ValueLogSegmentID(fileID1),
		page.ValueLogSegmentID(fileID2),
		page.ValueLogSegmentID(fileID3),
	} {
		gen := manifest.Generations[i]
		if got, want := gen.GenerationID, uint64(i+1); got != want {
			t.Fatalf("generation[%d].GenerationID=%d, want %d", i, got, want)
		}
		wantState := leafGenerationStateSealed
		wantSealedCommitSeq := uint64(77)
		if i == 2 {
			wantState = leafGenerationStateWritable
			wantSealedCommitSeq = 0
		}
		if got := gen.State; got != wantState {
			t.Fatalf("generation[%d].State=%q, want %q", i, got, wantState)
		}
		if got, want := len(gen.FileIDs), 1; got != want {
			t.Fatalf("generation[%d].len(FileIDs)=%d, want %d", i, got, want)
		}
		if got := gen.FileIDs[0]; got != wantRaw {
			t.Fatalf("generation[%d].FileIDs[0]=%d, want %d", i, got, wantRaw)
		}
		if got, want := gen.CreatedCommitSeq, uint64(77); got != want {
			t.Fatalf("generation[%d].CreatedCommitSeq=%d, want %d", i, got, want)
		}
		if got := gen.SealedCommitSeq; got != wantSealedCommitSeq {
			t.Fatalf("generation[%d].SealedCommitSeq=%d, want %d", i, got, wantSealedCommitSeq)
		}
	}
	if _, err := os.Stat(leafGenerationManifestPath(leafDir)); !os.IsNotExist(err) {
		t.Fatalf("expected read-only bootstrap to avoid writing manifest, got %v", err)
	}

	writeManifest, err := loadOrCreateLeafGenerationManifest(leafDir, 88, false)
	if err != nil {
		t.Fatalf("loadOrCreateLeafGenerationManifest(writeable): %v", err)
	}
	if writeManifest == nil {
		t.Fatalf("expected persisted manifest")
	}
	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest file to exist after writeable bootstrap")
	}
	if got, want := loaded.CurrentGenerationID, uint64(3); got != want {
		t.Fatalf("persisted CurrentGenerationID=%d, want %d", got, want)
	}
}

func TestReconcileLeafGenerationManifestWithDir_PreservesDeletedGenerationFilePresentOnDisk(t *testing.T) {
	leafDir := t.TempDir()
	_, fileID := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 1)
	rawFileID := page.ValueLogSegmentID(fileID)
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{rawFileID}, CreatedCommitSeq: 10, DeletedCommitSeq: 20, PublishedCommitSeq: 20},
			{GenerationID: 2, State: leafGenerationStateWritable, CreatedCommitSeq: 30, PublishedCommitSeq: 30},
		},
	}

	reconciled, changed, err := reconcileLeafGenerationManifestWithDir(leafDir, manifest, 99)
	if err != nil {
		t.Fatalf("reconcileLeafGenerationManifestWithDir: %v", err)
	}
	if changed {
		t.Fatal("expected reconcile to preserve pending-deleted on-disk file without re-registering it")
	}
	if reconciled != manifest {
		t.Fatal("expected unchanged reconcile to return original manifest")
	}
	current := manifest.Generations[manifest.currentGenerationIndex()]
	if len(current.FileIDs) != 0 {
		t.Fatalf("current FileIDs=%v, want empty while deleted file is pending cleanup", current.FileIDs)
	}
}

func TestReconcileLeafGenerationManifestWithDir_DropsMissingCurrentWritableFile(t *testing.T) {
	leafDir := t.TempDir()
	_, fileID18 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 18)
	_, fileID19 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 19)
	_, fileID20 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 20)
	fileID17, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 17)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	raw17 := page.ValueLogSegmentID(fileID17)
	raw18 := page.ValueLogSegmentID(fileID18)
	raw19 := page.ValueLogSegmentID(fileID19)
	raw20 := page.ValueLogSegmentID(fileID20)
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 3,
		NextGenerationID:    4,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateSealed, FileIDs: []uint32{raw18}, CreatedCommitSeq: 10, SealedCommitSeq: 20, PublishedCommitSeq: 20},
			{GenerationID: 2, State: leafGenerationStateSealed, FileIDs: []uint32{raw19}, CreatedCommitSeq: 21, SealedCommitSeq: 30, PublishedCommitSeq: 30},
			{GenerationID: 3, State: leafGenerationStateWritable, FileIDs: []uint32{raw17}, CreatedCommitSeq: 31, PublishedCommitSeq: 31},
		},
	}

	reconciled, changed, err := reconcileLeafGenerationManifestWithDir(leafDir, manifest, 99)
	if err != nil {
		t.Fatalf("reconcileLeafGenerationManifestWithDir: %v", err)
	}
	if !changed {
		t.Fatal("expected reconcile to drop missing current file and recover writable tail")
	}
	view := newLeafGenerationView(reconciled)
	if _, ok := view.FileToGeneration[raw17]; ok {
		t.Fatalf("missing raw file %d still present in generation view", raw17)
	}
	if got, want := view.FileToGeneration[raw18], uint64(1); got != want {
		t.Fatalf("raw18 generation=%d, want %d", got, want)
	}
	if got, want := view.FileToGeneration[raw19], uint64(2); got != want {
		t.Fatalf("raw19 generation=%d, want %d", got, want)
	}
	if got, want := view.FileToGeneration[raw20], uint64(3); got != want {
		t.Fatalf("raw20 generation=%d, want %d", got, want)
	}
	current := reconciled.Generations[reconciled.currentGenerationIndex()]
	if got, want := current.FileIDs, []uint32{raw20}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current FileIDs=%v, want %v", got, want)
	}
}

func TestLeafGenerationManifest_AppendRecoveredSealedGenerationFileID_IgnoresDeletedGenerationDuplicates(t *testing.T) {
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 2,
		NextGenerationID:    3,
		Generations: []leafGenerationRecord{
			{GenerationID: 1, State: leafGenerationStateDeleted, FileIDs: []uint32{77}, CreatedCommitSeq: 10, DeletedCommitSeq: 20, PublishedCommitSeq: 20},
			{GenerationID: 2, State: leafGenerationStateWritable, CreatedCommitSeq: 30, PublishedCommitSeq: 30},
		},
	}

	changed, err := manifest.appendRecoveredSealedGenerationFileID(77, 99)
	if err != nil {
		t.Fatalf("appendRecoveredSealedGenerationFileID: %v", err)
	}
	if !changed {
		t.Fatal("expected deleted duplicate file recovery to append a sealed generation")
	}
	if got, want := len(manifest.Generations), 3; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	recovered := manifest.Generations[2]
	if got, want := recovered.State, leafGenerationStateSealed; got != want {
		t.Fatalf("recovered.State=%q, want %q", got, want)
	}
	if got, want := recovered.FileIDs, []uint32{77}; !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered.FileIDs=%v, want %v", got, want)
	}
}

func TestLoadOrCreateLeafGenerationManifest_RecoversUnknownLeafFilesPreservesWritableTail(t *testing.T) {
	leafDir := t.TempDir()
	_, fileID1 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 1)
	_, fileID2 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 2)
	_, fileID3 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 3)

	manifest := newLeafGenerationManifest(55)
	if _, err := manifest.registerCurrentGenerationFileID(page.ValueLogSegmentID(fileID1), 55); err != nil {
		t.Fatalf("registerCurrentGenerationFileID: %v", err)
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		t.Fatalf("saveLeafGenerationManifest: %v", err)
	}

	loadedManifest, err := loadOrCreateLeafGenerationManifest(leafDir, 89, false)
	if err != nil {
		t.Fatalf("loadOrCreateLeafGenerationManifest: %v", err)
	}
	if loadedManifest == nil {
		t.Fatal("expected manifest")
	}
	if got, want := len(loadedManifest.Generations), 3; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	if got, want := loadedManifest.Generations[0].FileIDs[0], page.ValueLogSegmentID(fileID1); got != want {
		t.Fatalf("generation[0].FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := loadedManifest.Generations[0].State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation[0].State=%q, want %q", got, want)
	}
	if got, want := loadedManifest.Generations[1].State, leafGenerationStateSealed; got != want {
		t.Fatalf("generation[1].State=%q, want %q", got, want)
	}
	if got, want := loadedManifest.Generations[1].FileIDs[0], page.ValueLogSegmentID(fileID2); got != want {
		t.Fatalf("generation[1].FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := loadedManifest.Generations[2].State, leafGenerationStateWritable; got != want {
		t.Fatalf("generation[2].State=%q, want %q", got, want)
	}
	if got, want := loadedManifest.Generations[2].FileIDs[0], page.ValueLogSegmentID(fileID3); got != want {
		t.Fatalf("generation[2].FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := loadedManifest.CurrentGenerationID, loadedManifest.Generations[2].GenerationID; got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}

	loaded, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatal("expected persisted manifest")
	}
	if got, want := len(loaded.Generations), 3; got != want {
		t.Fatalf("persisted len(Generations)=%d, want %d", got, want)
	}
	if got, want := loaded.Generations[0].FileIDs[0], page.ValueLogSegmentID(fileID1); got != want {
		t.Fatalf("persisted generation[0].FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := loaded.Generations[0].State, leafGenerationStateSealed; got != want {
		t.Fatalf("persisted generation[0].State=%q, want %q", got, want)
	}
	if got, want := loaded.Generations[1].State, leafGenerationStateSealed; got != want {
		t.Fatalf("persisted generation[1].State=%q, want %q", got, want)
	}
	if got, want := loaded.Generations[1].FileIDs[0], page.ValueLogSegmentID(fileID2); got != want {
		t.Fatalf("persisted generation[1].FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := loaded.Generations[2].State, leafGenerationStateWritable; got != want {
		t.Fatalf("persisted generation[2].State=%q, want %q", got, want)
	}
	if got, want := loaded.Generations[2].FileIDs[0], page.ValueLogSegmentID(fileID3); got != want {
		t.Fatalf("persisted generation[2].FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := loaded.CurrentGenerationID, loaded.Generations[2].GenerationID; got != want {
		t.Fatalf("persisted CurrentGenerationID=%d, want %d", got, want)
	}
}

func TestOpen_IndexOuterLeavesInValueLog_CreatesLeafGenerationManifest(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	loaded, ok, err := loadLeafGenerationManifest(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest file to exist")
	}
	if got, want := loaded.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
}

func TestCloseWaitsForWritersBeforeLeafGenerationManifestStoreTeardown(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := db.leafGenerationManifestStore
	if store == nil {
		t.Fatal("leaf generation manifest store is nil")
	}

	teardownStarted := make(chan struct{})
	db.registerInternalTeardownHook(func() error {
		close(teardownStarted)
		return nil
	})
	db.writeMu.RLock()
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case <-teardownStarted:
		db.writeMu.RUnlock()
		t.Fatal("Close began internal teardown before writers drained")
	case <-time.After(100 * time.Millisecond):
	}

	manifest := db.leafGenerationManifest.clone()
	token, err := store.Replace(manifest)
	if err != nil {
		db.writeMu.RUnlock()
		t.Fatalf("manifest replacement while Close waits for writer: %v", err)
	}
	if token != nil {
		token.Release()
	}
	db.writeMu.RUnlock()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after writer drained")
	}
	store.mu.Lock()
	closed := store.closed
	store.mu.Unlock()
	if !closed {
		t.Fatal("leaf generation manifest store remained open after Close")
	}
}

func TestCloseReleasesWriteMuBeforeLeafGenerationManifestStoreTeardown(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := db.leafGenerationManifestStore
	if store == nil {
		t.Fatal("leaf generation manifest store is nil")
	}

	teardownStarted := make(chan struct{})
	db.registerInternalTeardownHook(func() error {
		close(teardownStarted)
		return nil
	})
	store.mu.Lock()
	storeLocked := true
	defer func() {
		if storeLocked {
			store.mu.Unlock()
		}
	}()
	db.writeMu.RLock()
	closeDone := make(chan error, 1)
	go func() { closeDone <- db.Close() }()
	select {
	case <-teardownStarted:
		db.writeMu.RUnlock()
		t.Fatal("Close began internal teardown before writers drained")
	case <-time.After(100 * time.Millisecond):
	}

	writeMuAcquired := make(chan struct{})
	go func() {
		db.writeMu.RLock()
		close(writeMuAcquired)
		db.writeMu.RUnlock()
	}()
	db.writeMu.RUnlock()

	select {
	case <-writeMuAcquired:
	case <-time.After(5 * time.Second):
		store.mu.Unlock()
		storeLocked = false
		<-closeDone
		t.Fatal("writeMu remained held while manifest store teardown was blocked")
	}
	store.mu.Unlock()
	storeLocked = false

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after manifest store teardown unblocked")
	}
}

func TestCommitRejectsClosingDBAfterWriteMuAcquisition(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		db.closing.Store(false)
		_ = db.Close()
	})

	db.mu.RLock()
	rootID := db.meta.UserRootPageID
	commitSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	db.writeMu.Lock()
	db.closing.Store(true)
	commitDone := make(chan error, 1)
	go func() { commitDone <- db.ForceCommit(rootID) }()
	db.writeMu.Unlock()

	select {
	case err := <-commitDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Commit error=%v, want %v", err, ErrClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Commit remained blocked after writeMu became available")
	}
	db.mu.RLock()
	gotCommitSeq := db.meta.CommitSeq
	db.mu.RUnlock()
	if gotCommitSeq != commitSeq {
		t.Fatalf("CommitSeq=%d after closing Commit, want %d", gotCommitSeq, commitSeq)
	}
}

func TestEnsureLeafPageLogSegmentRegistered_AddsWritableFileToManifest(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	path, fileID := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), 255, 1)
	db.SetLeafPageLog(&manifestTestLeafPageLog{path: path, fileID: fileID})

	registered, err := db.ensureLeafPageLogSegmentRegistered(55)
	if err != nil {
		t.Fatalf("ensureLeafPageLogSegmentRegistered: %v", err)
	}
	if !registered {
		t.Fatalf("expected current leaf segment registration to succeed")
	}
	registered, err = db.ensureLeafPageLogSegmentRegistered(99)
	if err != nil {
		t.Fatalf("ensureLeafPageLogSegmentRegistered second call: %v", err)
	}
	if !registered {
		t.Fatalf("expected second registration call to confirm current segment")
	}

	path2, fileID2 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), 255, 2)
	db.SetLeafPageLog(&manifestTestLeafPageLog{path: path2, fileID: fileID2})
	registered, err = db.ensureLeafPageLogSegmentRegistered(144)
	if err != nil {
		t.Fatalf("ensureLeafPageLogSegmentRegistered rollover: %v", err)
	}
	if !registered {
		t.Fatalf("expected rollover registration to succeed")
	}

	wantPending := []uint32{page.ValueLogSegmentID(fileID), page.ValueLogSegmentID(fileID2)}
	if got := append([]uint32(nil), db.leafGenerationPendingFileIDs...); !reflect.DeepEqual(got, wantPending) {
		t.Fatalf("pending file ids=%v want %v", got, wantPending)
	}
	if got, want := db.leafGenerationPendingCommitSeq[wantPending[0]], uint64(55); got != want {
		t.Fatalf("pending commit seq for first file=%d want %d", got, want)
	}
	if got, want := db.leafGenerationPendingCommitSeq[wantPending[1]], uint64(144); got != want {
		t.Fatalf("pending commit seq for second file=%d want %d", got, want)
	}

	if err := db.noteLeafGenerationPendingFileIDs(0, 144); err != nil {
		t.Fatalf("noteLeafGenerationPendingFileIDs: %v", err)
	}

	loaded, ok, err := loadLeafGenerationManifest(LeafLogDirPath(dir))
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest: %v", err)
	}
	if !ok {
		t.Fatalf("expected manifest file to exist")
	}
	if got, want := len(loaded.Generations), 2; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	sealed := loaded.Generations[0]
	if got, want := sealed.State, leafGenerationStateSealed; got != want {
		t.Fatalf("sealed.State=%q, want %q", got, want)
	}
	if got, want := len(sealed.FileIDs), 1; got != want {
		t.Fatalf("len(sealed.FileIDs)=%d, want %d", got, want)
	}
	if got, want := sealed.FileIDs[0], page.ValueLogSegmentID(fileID); got != want {
		t.Fatalf("sealed.FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := sealed.SealedCommitSeq, uint64(144); got != want {
		t.Fatalf("sealed.SealedCommitSeq=%d, want %d", got, want)
	}
	current := loaded.Generations[loaded.currentGenerationIndex()]
	if got, want := current.GenerationID, uint64(2); got != want {
		t.Fatalf("current.GenerationID=%d, want %d", got, want)
	}
	if got, want := len(current.FileIDs), 1; got != want {
		t.Fatalf("len(current.FileIDs)=%d, want %d", got, want)
	}
	if got, want := current.FileIDs[0], page.ValueLogSegmentID(fileID2); got != want {
		t.Fatalf("current.FileIDs[0]=%d, want %d", got, want)
	}
	if got, want := current.CreatedCommitSeq, uint64(144); got != want {
		t.Fatalf("current.CreatedCommitSeq=%d, want %d", got, want)
	}
}

func TestNoteLeafGenerationWritableFileIDs_PersistsSealedRecordLengthIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, fileID1 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 1)
	_, fileID2 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 2)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	if err := db.noteLeafGenerationWritableFileID(fileID1, 55); err != nil {
		t.Fatalf("noteLeafGenerationWritableFileID first: %v", err)
	}
	db.storeLeafGenerationRecordLengthIndex(rawFileID1, &leafGenerationRecordLengthIndex{
		offsets: []uint32{4, 128},
		lengths: []uint32{96, 104},
	})
	if err := db.noteLeafGenerationWritableFileID(fileID2, 89); err != nil {
		t.Fatalf("noteLeafGenerationWritableFileID rollover: %v", err)
	}

	indexPath := leafGenerationRecordLengthIndexPath(dir, rawFileID1)
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("Stat(%q): %v", indexPath, err)
	}
	idx, ok, err := loadLeafGenerationRecordLengthIndexFile(indexPath, rawFileID1)
	if err != nil {
		t.Fatalf("loadLeafGenerationRecordLengthIndexFile: %v", err)
	}
	if !ok {
		t.Fatal("expected persisted record-length index")
	}
	if got, found := idx.lookup(4); !found || got != 96 {
		t.Fatalf("lookup(4)=(%d,%v), want (96,true)", got, found)
	}
	if got, found := idx.lookup(128); !found || got != 104 {
		t.Fatalf("lookup(128)=(%d,%v), want (104,true)", got, found)
	}
}

func TestNoteLeafGenerationWritableFileID_SaveFailureLeavesManifestRetryable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if db.leafGenerationManifestStore == nil || db.leafGenerationManifestStore.mode != leafGenerationManifestStable {
		t.Skip("stable manifest replacement hooks are unavailable")
	}

	leafDir := LeafLogDirPath(dir)
	_, fileID1 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 1)
	_, fileID2 := createLeafGenerationTestSegment(t, leafDir, rewriteLeafLogLaneID, 2)
	rawFileID1 := page.ValueLogSegmentID(fileID1)
	rawFileID2 := page.ValueLogSegmentID(fileID2)
	if err := db.noteLeafGenerationWritableFileID(fileID1, 55); err != nil {
		t.Fatalf("noteLeafGenerationWritableFileID first: %v", err)
	}
	cut := errors.New("manifest save cut")
	db.leafGenerationManifestStore.hooks.BeforeTempCreate = func() error { return cut }
	if err := db.noteLeafGenerationWritableFileID(fileID2, 89); !errors.Is(err, cut) {
		t.Fatalf("manifest save error=%v, want %v", err, cut)
	}
	current := db.leafGenerationManifest.Generations[db.leafGenerationManifest.currentGenerationIndex()]
	if got, want := current.FileIDs, []uint32{rawFileID1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("in-memory current FileIDs=%v, want %v", got, want)
	}
	loadedBefore, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest before retry: %v", err)
	}
	if !ok {
		t.Fatal("expected manifest file before retry")
	}
	if got, want := loadedBefore.Generations[loadedBefore.currentGenerationIndex()].FileIDs, []uint32{rawFileID1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted current FileIDs before retry=%v, want %v", got, want)
	}
	db.leafGenerationManifestStore.hooks.BeforeTempCreate = nil
	if err := db.noteLeafGenerationWritableFileID(fileID2, 89); err != nil {
		t.Fatalf("noteLeafGenerationWritableFileID retry: %v", err)
	}
	loadedAfter, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		t.Fatalf("loadLeafGenerationManifest after retry: %v", err)
	}
	if !ok {
		t.Fatal("expected manifest file after retry")
	}
	if got, want := len(loadedAfter.Generations), 2; got != want {
		t.Fatalf("len(Generations)=%d, want %d", got, want)
	}
	if got, want := loadedAfter.Generations[0].FileIDs, []uint32{rawFileID1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sealed FileIDs=%v, want %v", got, want)
	}
	if got, want := loadedAfter.Generations[1].FileIDs, []uint32{rawFileID2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current FileIDs=%v, want %v", got, want)
	}
}

func TestStagedLeafGenerationManifestWithPending_ReflectsQueuedWritableFiles(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	path1, fileID1 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 1)
	db.SetLeafPageLog(&manifestTestLeafPageLog{path: path1, fileID: fileID1})
	if registered, err := db.ensureLeafPageLogSegmentRegistered(55); err != nil || !registered {
		t.Fatalf("ensureLeafPageLogSegmentRegistered first: registered=%v err=%v", registered, err)
	}
	if err := db.noteLeafGenerationPendingFileIDs(0, 55); err != nil {
		t.Fatalf("noteLeafGenerationPendingFileIDs first: %v", err)
	}

	path2, fileID2 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 2)
	db.SetLeafPageLog(&manifestTestLeafPageLog{path: path2, fileID: fileID2})
	if registered, err := db.ensureLeafPageLogSegmentRegistered(144); err != nil || !registered {
		t.Fatalf("ensureLeafPageLogSegmentRegistered rollover: registered=%v err=%v", registered, err)
	}
	before := db.currentLeafGenerationView()
	if _, ok := before.FileToGeneration[page.ValueLogSegmentID(fileID2)]; ok {
		t.Fatalf("pending file %d already visible before staging", page.ValueLogSegmentID(fileID2))
	}
	staged, changed, err := db.stagedLeafGenerationManifestWithPending(db.leafGenerationManifest, 0, 144)
	if err != nil {
		t.Fatalf("stagedLeafGenerationManifestWithPending: %v", err)
	}
	if !changed {
		t.Fatal("expected staged manifest change")
	}
	if staged == db.leafGenerationManifest {
		t.Fatal("expected staged manifest clone, got shared pointer")
	}
	view := newLeafGenerationView(staged)
	if got, want := len(view.GenerationOrder), 2; got != want {
		t.Fatalf("len(GenerationOrder)=%d, want %d", got, want)
	}
	if got, want := view.FileToGeneration[page.ValueLogSegmentID(fileID1)], uint64(1); got != want {
		t.Fatalf("file1 generation=%d, want %d", got, want)
	}
	if got, want := view.FileToGeneration[page.ValueLogSegmentID(fileID2)], uint64(2); got != want {
		t.Fatalf("file2 generation=%d, want %d", got, want)
	}
	current := staged.Generations[staged.currentGenerationIndex()]
	if got, want := current.FileIDs, []uint32{page.ValueLogSegmentID(fileID2)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged current FileIDs=%v, want %v", got, want)
	}
	liveCurrent := db.leafGenerationManifest.Generations[db.leafGenerationManifest.currentGenerationIndex()]
	if got, want := liveCurrent.FileIDs, []uint32{page.ValueLogSegmentID(fileID1)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("live current FileIDs=%v, want %v", got, want)
	}
}

func TestStagedLeafGenerationManifestWithPending_IgnoresStaleMissingOlderFile(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	_, fileID18 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 18)
	_, fileID19 := createLeafGenerationTestSegment(t, LeafLogDirPath(dir), rewriteLeafLogLaneID, 19)
	if err := db.noteLeafGenerationWritableFileID(fileID18, 55); err != nil {
		t.Fatalf("noteLeafGenerationWritableFileID first: %v", err)
	}
	if err := db.noteLeafGenerationWritableFileID(fileID19, 89); err != nil {
		t.Fatalf("noteLeafGenerationWritableFileID second: %v", err)
	}

	fileID17, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, 17)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	raw17 := page.ValueLogSegmentID(fileID17)
	raw19 := page.ValueLogSegmentID(fileID19)
	db.queueLeafGenerationWritableFileID(fileID17)

	staged, changed, err := db.stagedLeafGenerationManifestWithPending(db.leafGenerationManifest, 0, 144)
	if err != nil {
		t.Fatalf("stagedLeafGenerationManifestWithPending: %v", err)
	}
	if changed {
		t.Fatalf("expected stale missing file %d to be ignored", raw17)
	}
	if staged != db.leafGenerationManifest {
		t.Fatal("expected unchanged staging to return the live manifest")
	}
	if err := db.noteLeafGenerationPendingFileIDs(0, 144); err != nil {
		t.Fatalf("noteLeafGenerationPendingFileIDs: %v", err)
	}
	if len(db.leafGenerationPendingFileIDs) != 0 {
		t.Fatalf("pending file IDs after stale drain=%v, want empty", db.leafGenerationPendingFileIDs)
	}
	current := db.leafGenerationManifest.Generations[db.leafGenerationManifest.currentGenerationIndex()]
	if got, want := current.FileIDs, []uint32{raw19}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current FileIDs=%v, want %v", got, want)
	}
	if view := db.currentLeafGenerationView(); view.FileToGeneration[raw17] != 0 {
		t.Fatalf("stale raw file %d became visible in current view", raw17)
	}
}

func TestOpen_WithoutLeafVLog_DoesNotCreateLeafGenerationManifest(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.leafGenerationManifest != nil {
		t.Fatalf("expected no leaf generation manifest when outer leaves stay inline")
	}
	if _, err := os.Stat(leafGenerationManifestPath(LeafLogDirPath(dir))); !os.IsNotExist(err) {
		t.Fatalf("expected no manifest file, got %v", err)
	}
}

func TestOpenReadOnly_IndexOuterLeavesInValueLog_MissingManifestUsesSynthetic(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatalf("Open writeable: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close writeable: %v", err)
	}
	manifestPath := leafGenerationManifestPath(LeafLogDirPath(dir))
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("Remove(manifest): %v", err)
	}

	ro, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer func() { _ = ro.Close() }()

	if ro.leafGenerationManifest == nil {
		t.Fatalf("expected synthetic in-memory manifest on read-only open")
	}
	if got, want := ro.leafGenerationManifest.CurrentGenerationID, uint64(1); got != want {
		t.Fatalf("CurrentGenerationID=%d, want %d", got, want)
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("expected read-only open to leave manifest absent, got %v", err)
	}
}
