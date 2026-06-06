package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	leafGenerationManifestFileName = "manifest.json"
	leafGenerationManifestVersion  = 1

	leafGenerationStateWritable = "writable"
	leafGenerationStateSealed   = "sealed"
	leafGenerationStateRetiring = "retiring"
	leafGenerationStateDeleted  = "deleted"
)

type leafGenerationManifest struct {
	Version             int                    `json:"version"`
	CurrentGenerationID uint64                 `json:"current_generation_id"`
	NextGenerationID    uint64                 `json:"next_generation_id"`
	Generations         []leafGenerationRecord `json:"generations"`
}

type leafGenerationRecord struct {
	GenerationID       uint64   `json:"generation_id"`
	State              string   `json:"state"`
	FileIDs            []uint32 `json:"file_ids,omitempty"`
	CreatedCommitSeq   uint64   `json:"created_commit_seq,omitempty"`
	SealedCommitSeq    uint64   `json:"sealed_commit_seq,omitempty"`
	RetiredCommitSeq   uint64   `json:"retired_commit_seq,omitempty"`
	DeletedCommitSeq   uint64   `json:"deleted_commit_seq,omitempty"`
	PublishedCommitSeq uint64   `json:"published_commit_seq,omitempty"`
}

func leafGenerationManifestPath(leafDir string) string {
	if leafDir == "" {
		return ""
	}
	return filepath.Join(leafDir, leafGenerationManifestFileName)
}

func newLeafGenerationManifest(commitSeq uint64) *leafGenerationManifest {
	return &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: 1,
		NextGenerationID:    2,
		Generations: []leafGenerationRecord{{
			GenerationID:       1,
			State:              leafGenerationStateWritable,
			CreatedCommitSeq:   commitSeq,
			PublishedCommitSeq: commitSeq,
		}},
	}
}

func (m *leafGenerationManifest) currentGenerationIndex() int {
	if m == nil {
		return -1
	}
	for i := range m.Generations {
		if m.Generations[i].GenerationID == m.CurrentGenerationID {
			return i
		}
	}
	return -1
}

func (m *leafGenerationManifest) registerCurrentGenerationFileID(fileID uint32, commitSeq uint64) (bool, error) {
	if m == nil {
		return false, errors.New("treedb: leaf generation manifest is nil")
	}
	if fileID == 0 {
		return false, errors.New("treedb: leaf generation file_id must be non-zero")
	}
	idx := m.currentGenerationIndex()
	if idx < 0 {
		return false, fmt.Errorf("treedb: %s current_generation_id %d not found in generations", leafGenerationManifestFileName, m.CurrentGenerationID)
	}
	gen := &m.Generations[idx]
	if gen.State != leafGenerationStateWritable {
		return false, fmt.Errorf("treedb: current generation %d is not writable (state=%q)", gen.GenerationID, gen.State)
	}
	for _, existingGen := range m.Generations {
		if existingGen.State == leafGenerationStateDeleted {
			continue
		}
		for _, existing := range existingGen.FileIDs {
			if existing == fileID {
				return false, nil
			}
		}
	}
	if len(gen.FileIDs) == 0 {
		gen.FileIDs = append(gen.FileIDs, fileID)
		if commitSeq > gen.PublishedCommitSeq {
			gen.PublishedCommitSeq = commitSeq
		}
		return true, nil
	}

	maxID := uint64(0)
	for i := range m.Generations {
		if m.Generations[i].GenerationID > maxID {
			maxID = m.Generations[i].GenerationID
		}
	}
	if m.NextGenerationID <= maxID {
		m.NextGenerationID = maxID + 1
	}
	newID := m.NextGenerationID
	m.NextGenerationID++
	gen.State = leafGenerationStateSealed
	if commitSeq > gen.SealedCommitSeq {
		gen.SealedCommitSeq = commitSeq
	}
	if commitSeq > gen.PublishedCommitSeq {
		gen.PublishedCommitSeq = commitSeq
	}
	m.CurrentGenerationID = newID
	m.Generations = append(m.Generations, leafGenerationRecord{
		GenerationID:       newID,
		State:              leafGenerationStateWritable,
		FileIDs:            []uint32{fileID},
		CreatedCommitSeq:   commitSeq,
		PublishedCommitSeq: commitSeq,
	})
	return true, nil
}

func (m *leafGenerationManifest) sealCurrentGeneration(commitSeq uint64) ([]uint32, bool, error) {
	if m == nil {
		return nil, false, errors.New("treedb: leaf generation manifest is nil")
	}
	idx := m.currentGenerationIndex()
	if idx < 0 {
		return nil, false, fmt.Errorf("treedb: %s current_generation_id %d not found in generations", leafGenerationManifestFileName, m.CurrentGenerationID)
	}
	gen := &m.Generations[idx]
	if gen.State != leafGenerationStateWritable {
		return nil, false, fmt.Errorf("treedb: current generation %d is not writable (state=%q)", gen.GenerationID, gen.State)
	}
	if len(gen.FileIDs) == 0 {
		return nil, false, nil
	}
	maxID := uint64(0)
	for i := range m.Generations {
		if m.Generations[i].GenerationID > maxID {
			maxID = m.Generations[i].GenerationID
		}
	}
	if m.NextGenerationID <= maxID {
		m.NextGenerationID = maxID + 1
	}
	newID := m.NextGenerationID
	m.NextGenerationID++
	gen.State = leafGenerationStateSealed
	if commitSeq > gen.SealedCommitSeq {
		gen.SealedCommitSeq = commitSeq
	}
	if commitSeq > gen.PublishedCommitSeq {
		gen.PublishedCommitSeq = commitSeq
	}
	m.CurrentGenerationID = newID
	m.Generations = append(m.Generations, leafGenerationRecord{
		GenerationID:       newID,
		State:              leafGenerationStateWritable,
		CreatedCommitSeq:   commitSeq,
		PublishedCommitSeq: commitSeq,
	})
	return append([]uint32(nil), gen.FileIDs...), true, nil
}

func (m *leafGenerationManifest) appendRecoveredSealedGenerationFileID(fileID uint32, commitSeq uint64) (bool, error) {
	if m == nil {
		return false, errors.New("treedb: leaf generation manifest is nil")
	}
	if fileID == 0 {
		return false, errors.New("treedb: leaf generation file_id must be non-zero")
	}
	for _, gen := range m.Generations {
		for _, existing := range gen.FileIDs {
			if existing == fileID {
				return false, nil
			}
		}
	}
	maxID := uint64(0)
	for i := range m.Generations {
		if m.Generations[i].GenerationID > maxID {
			maxID = m.Generations[i].GenerationID
		}
	}
	if m.NextGenerationID <= maxID {
		m.NextGenerationID = maxID + 1
	}
	newID := m.NextGenerationID
	m.NextGenerationID++
	m.Generations = append(m.Generations, leafGenerationRecord{
		GenerationID:       newID,
		State:              leafGenerationStateSealed,
		FileIDs:            []uint32{fileID},
		CreatedCommitSeq:   commitSeq,
		SealedCommitSeq:    commitSeq,
		PublishedCommitSeq: commitSeq,
	})
	return true, nil
}

func (m *leafGenerationManifest) currentGenerationMaxRecoveredSeq() uint32 {
	if m == nil {
		return 0
	}
	idx := m.currentGenerationIndex()
	if idx < 0 {
		return 0
	}
	maxSeq := uint32(0)
	for _, rawFileID := range m.Generations[idx].FileIDs {
		if rawFileID == 0 {
			continue
		}
		_, seq := valuelog.DecodeSegmentID(rawFileID)
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

func (m *leafGenerationManifest) clone() *leafGenerationManifest {
	if m == nil {
		return nil
	}
	out := &leafGenerationManifest{
		Version:             m.Version,
		CurrentGenerationID: m.CurrentGenerationID,
		NextGenerationID:    m.NextGenerationID,
		Generations:         make([]leafGenerationRecord, len(m.Generations)),
	}
	copy(out.Generations, m.Generations)
	for i := range out.Generations {
		if len(m.Generations[i].FileIDs) == 0 {
			continue
		}
		out.Generations[i].FileIDs = append([]uint32(nil), m.Generations[i].FileIDs...)
	}
	return out
}

func validateLeafGenerationManifest(m *leafGenerationManifest) error {
	if m == nil {
		return errors.New("treedb: leaf generation manifest is nil")
	}
	if m.Version != leafGenerationManifestVersion {
		return fmt.Errorf("treedb: unsupported %s version %d", leafGenerationManifestFileName, m.Version)
	}
	if m.CurrentGenerationID == 0 {
		return fmt.Errorf("treedb: %s current_generation_id must be non-zero", leafGenerationManifestFileName)
	}
	if m.NextGenerationID == 0 {
		return fmt.Errorf("treedb: %s next_generation_id must be non-zero", leafGenerationManifestFileName)
	}
	if len(m.Generations) == 0 {
		return fmt.Errorf("treedb: %s must contain at least one generation", leafGenerationManifestFileName)
	}

	seen := make(map[uint64]struct{}, len(m.Generations))
	currentFound := false
	writableCount := 0
	maxID := uint64(0)
	for i := range m.Generations {
		gen := m.Generations[i]
		if gen.GenerationID == 0 {
			return fmt.Errorf("treedb: %s generation[%d] has zero generation_id", leafGenerationManifestFileName, i)
		}
		if _, dup := seen[gen.GenerationID]; dup {
			return fmt.Errorf("treedb: %s generation_id %d appears more than once", leafGenerationManifestFileName, gen.GenerationID)
		}
		seen[gen.GenerationID] = struct{}{}
		if gen.GenerationID > maxID {
			maxID = gen.GenerationID
		}
		switch gen.State {
		case leafGenerationStateWritable, leafGenerationStateSealed, leafGenerationStateRetiring, leafGenerationStateDeleted:
		default:
			return fmt.Errorf("treedb: %s generation[%d] has unsupported state %q", leafGenerationManifestFileName, i, gen.State)
		}
		if gen.State == leafGenerationStateWritable {
			writableCount++
		}
		if gen.GenerationID == m.CurrentGenerationID {
			currentFound = true
			if gen.State != leafGenerationStateWritable {
				return fmt.Errorf("treedb: %s current_generation_id %d must be writable, got %q", leafGenerationManifestFileName, m.CurrentGenerationID, gen.State)
			}
		}
	}
	if !currentFound {
		return fmt.Errorf("treedb: %s current_generation_id %d not found in generations", leafGenerationManifestFileName, m.CurrentGenerationID)
	}
	if writableCount != 1 {
		return fmt.Errorf("treedb: %s must contain exactly one writable generation, got %d", leafGenerationManifestFileName, writableCount)
	}
	if m.NextGenerationID <= maxID {
		return fmt.Errorf("treedb: %s next_generation_id %d must be greater than existing generation ids (max=%d)", leafGenerationManifestFileName, m.NextGenerationID, maxID)
	}
	return nil
}

type leafGenerationPendingFile struct {
	rawFileID uint32
	commitSeq uint64
}

func rawLeafGenerationFileID(fileID uint32) (uint32, bool) {
	rawFileID := page.ValueLogSegmentID(fileID)
	if rawFileID == 0 {
		return 0, false
	}
	return rawFileID, true
}

func (db *DB) queueLeafGenerationWritableFileID(fileID uint32) {
	db.queueLeafGenerationWritableFileIDAtCommit(fileID, 0)
}

func (db *DB) queueLeafGenerationWritableFileIDAtCommit(fileID uint32, commitSeq uint64) {
	if db == nil || db.leafGenerationManifest == nil {
		return
	}
	rawFileID, ok := rawLeafGenerationFileID(fileID)
	if !ok {
		return
	}
	db.leafGenerationPendingMu.Lock()
	if db.leafGenerationPendingSet == nil {
		db.leafGenerationPendingSet = make(map[uint32]struct{})
	}
	if db.leafGenerationPendingCommitSeq == nil {
		db.leafGenerationPendingCommitSeq = make(map[uint32]uint64)
	}
	if _, exists := db.leafGenerationPendingSet[rawFileID]; !exists {
		db.leafGenerationPendingSet[rawFileID] = struct{}{}
		db.leafGenerationPendingFileIDs = append(db.leafGenerationPendingFileIDs, rawFileID)
	}
	if commitSeq > 0 {
		if existing := db.leafGenerationPendingCommitSeq[rawFileID]; existing == 0 || commitSeq < existing {
			db.leafGenerationPendingCommitSeq[rawFileID] = commitSeq
		}
	}
	db.leafGenerationPendingMu.Unlock()
}

func (db *DB) snapshotLeafGenerationPendingFileIDs(currentFileID uint32) []leafGenerationPendingFile {
	if db == nil || db.leafGenerationManifest == nil {
		return nil
	}
	currentRawFileID, _ := rawLeafGenerationFileID(currentFileID)
	db.leafGenerationPendingMu.Lock()
	defer db.leafGenerationPendingMu.Unlock()
	if len(db.leafGenerationPendingFileIDs) == 0 && currentRawFileID == 0 {
		return nil
	}
	out := make([]leafGenerationPendingFile, 0, len(db.leafGenerationPendingFileIDs)+1)
	for _, rawFileID := range db.leafGenerationPendingFileIDs {
		out = append(out, leafGenerationPendingFile{
			rawFileID: rawFileID,
			commitSeq: db.leafGenerationPendingCommitSeq[rawFileID],
		})
	}
	if currentRawFileID != 0 {
		if _, exists := db.leafGenerationPendingSet[currentRawFileID]; !exists {
			out = append(out, leafGenerationPendingFile{rawFileID: currentRawFileID})
		}
	}
	return out
}

func (db *DB) clearLeafGenerationPendingFileIDs(fileIDs []uint32) {
	if db == nil || len(fileIDs) == 0 {
		return
	}
	db.leafGenerationPendingMu.Lock()
	defer db.leafGenerationPendingMu.Unlock()
	if len(db.leafGenerationPendingFileIDs) == 0 || len(db.leafGenerationPendingSet) == 0 {
		return
	}
	remove := make(map[uint32]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID == 0 {
			continue
		}
		remove[fileID] = struct{}{}
		delete(db.leafGenerationPendingSet, fileID)
		delete(db.leafGenerationPendingCommitSeq, fileID)
	}
	dst := db.leafGenerationPendingFileIDs[:0]
	for _, fileID := range db.leafGenerationPendingFileIDs {
		if _, drop := remove[fileID]; drop {
			continue
		}
		if _, keep := db.leafGenerationPendingSet[fileID]; !keep {
			continue
		}
		dst = append(dst, fileID)
	}
	db.leafGenerationPendingFileIDs = dst
}

func (db *DB) noteLeafGenerationPendingFileIDs(currentFileID uint32, commitSeq uint64) error {
	pending := db.snapshotLeafGenerationPendingFileIDs(currentFileID)
	if len(pending) == 0 {
		return nil
	}
	batch := make([]uint32, 0, len(pending))
	batchCommitSeq := uint64(0)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		flushed := append([]uint32(nil), batch...)
		if err := db.noteLeafGenerationWritableFileIDs(flushed, batchCommitSeq); err != nil {
			return err
		}
		db.clearLeafGenerationPendingFileIDs(flushed)
		batch = batch[:0]
		batchCommitSeq = 0
		return nil
	}
	for _, item := range pending {
		itemCommitSeq := item.commitSeq
		if itemCommitSeq == 0 {
			itemCommitSeq = commitSeq
		}
		if itemCommitSeq == 0 {
			continue
		}
		if batchCommitSeq != 0 && itemCommitSeq != batchCommitSeq {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		if batchCommitSeq == 0 {
			batchCommitSeq = itemCommitSeq
		}
		batch = append(batch, item.rawFileID)
	}
	if err := flushBatch(); err != nil {
		return err
	}
	return nil
}

func noteLeafGenerationWritableFileIDsInManifest(manifest *leafGenerationManifest, fileIDs []uint32, commitSeq uint64) ([]uint32, bool, error) {
	if manifest == nil || commitSeq == 0 || len(fileIDs) == 0 {
		return nil, false, nil
	}
	changedAny := false
	sealedFileIDs := make(map[uint32]struct{}, len(fileIDs))
	for _, rawFileID := range fileIDs {
		if rawFileID == 0 {
			continue
		}
		idx := manifest.currentGenerationIndex()
		if idx >= 0 {
			current := manifest.Generations[idx]
			if current.State == leafGenerationStateWritable && len(current.FileIDs) > 0 {
				duplicate := false
				for _, existing := range current.FileIDs {
					if existing == rawFileID {
						duplicate = true
						break
					}
				}
				if !duplicate {
					for _, sealedRawFileID := range current.FileIDs {
						if sealedRawFileID != 0 {
							sealedFileIDs[sealedRawFileID] = struct{}{}
						}
					}
				}
			}
		}
		changed, err := manifest.registerCurrentGenerationFileID(rawFileID, commitSeq)
		if err != nil {
			return nil, false, err
		}
		if changed {
			changedAny = true
		}
	}
	if !changedAny {
		return nil, false, nil
	}
	out := make([]uint32, 0, len(sealedFileIDs))
	for rawFileID := range sealedFileIDs {
		out = append(out, rawFileID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out, true, nil
}

func (db *DB) stagedLeafGenerationManifestWithPending(base *leafGenerationManifest, currentFileID uint32, commitSeq uint64) (*leafGenerationManifest, bool, error) {
	if db == nil || base == nil {
		return base, false, nil
	}
	pending := db.snapshotLeafGenerationPendingFileIDs(currentFileID)
	if len(pending) == 0 {
		return base, false, nil
	}
	working := base.clone()
	changedAny := false
	batch := make([]uint32, 0, len(pending))
	batchCommitSeq := uint64(0)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, changed, err := noteLeafGenerationWritableFileIDsInManifest(working, batch, batchCommitSeq)
		if err != nil {
			return err
		}
		if changed {
			changedAny = true
		}
		batch = batch[:0]
		batchCommitSeq = 0
		return nil
	}
	for _, item := range pending {
		itemCommitSeq := item.commitSeq
		if itemCommitSeq == 0 {
			itemCommitSeq = commitSeq
		}
		if itemCommitSeq == 0 {
			continue
		}
		if batchCommitSeq != 0 && itemCommitSeq != batchCommitSeq {
			if err := flushBatch(); err != nil {
				return base, false, err
			}
		}
		if batchCommitSeq == 0 {
			batchCommitSeq = itemCommitSeq
		}
		batch = append(batch, item.rawFileID)
	}
	if err := flushBatch(); err != nil {
		return base, false, err
	}
	return working, changedAny, nil
}

func (db *DB) noteLeafGenerationWritableFileIDs(fileIDs []uint32, commitSeq uint64) error {
	if db == nil || db.leafGenerationManifest == nil || commitSeq == 0 || len(fileIDs) == 0 {
		return nil
	}
	db.mu.Lock()
	if db.leafGenerationManifest == nil {
		db.mu.Unlock()
		return nil
	}
	nextManifest := db.leafGenerationManifest.clone()
	db.mu.Unlock()
	sealedFileIDs, changedAny, err := noteLeafGenerationWritableFileIDsInManifest(nextManifest, fileIDs, commitSeq)
	if err != nil {
		return err
	}
	if !changedAny {
		return nil
	}
	if err := saveLeafGenerationManifest(LeafLogDirPath(db.dir), nextManifest); err != nil {
		return err
	}
	db.mu.Lock()
	db.leafGenerationManifest = nextManifest
	db.mu.Unlock()
	if err := db.publishLeafGenerationState(false); err != nil {
		return err
	}
	for _, rawFileID := range sealedFileIDs {
		if err := db.persistLeafGenerationRecordLengthIndex(rawFileID); err != nil {
			// Record-length sidecars are rebuildable optimization metadata. Keep
			// manifest publication authoritative even if the sidecar write fails.
			continue
		}
	}
	return nil
}

func (db *DB) noteLeafGenerationWritableFileID(fileID uint32, commitSeq uint64) error {
	if db == nil || db.leafGenerationManifest == nil {
		return nil
	}
	rawFileID, ok := rawLeafGenerationFileID(fileID)
	if !ok {
		return errors.New("treedb: leaf generation file_id must be non-zero")
	}
	return db.noteLeafGenerationWritableFileIDs([]uint32{rawFileID}, commitSeq)
}

func loadLeafGenerationManifest(leafDir string) (*leafGenerationManifest, bool, error) {
	path := leafGenerationManifestPath(leafDir)
	if path == "" {
		return nil, false, errors.New("missing leaf_vlog dir")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, fmt.Errorf("treedb: decode %s: empty file", filepath.Base(path))
	}
	var manifest leafGenerationManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, false, fmt.Errorf("treedb: decode %s: %w", filepath.Base(path), err)
	}
	if err := validateLeafGenerationManifest(&manifest); err != nil {
		return nil, false, err
	}
	return manifest.clone(), true, nil
}

func saveLeafGenerationManifest(leafDir string, manifest *leafGenerationManifest) error {
	path := leafGenerationManifestPath(leafDir)
	if path == "" {
		return errors.New("missing leaf_vlog dir")
	}
	if err := validateLeafGenerationManifest(manifest); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
}

type leafGenerationBootstrapFile struct {
	rawFileID uint32
	lane      uint32
	seq       uint32
}

func parseLeafGenerationBootstrapFileName(name string) (lane uint32, seq uint32, ok bool) {
	if !strings.HasPrefix(name, "value-l") || !strings.HasSuffix(name, ".log") {
		return 0, 0, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, "value-l"), ".log")
	lanePart, seqPart, found := strings.Cut(body, "-")
	if !found || lanePart == "" || seqPart == "" || strings.Contains(seqPart, "-") {
		return 0, 0, false
	}
	lane64, err := strconv.ParseUint(lanePart, 10, 32)
	if err != nil {
		return 0, 0, false
	}
	seq64, err := strconv.ParseUint(seqPart, 10, 32)
	if err != nil {
		return 0, 0, false
	}
	return uint32(lane64), uint32(seq64), true
}

func listLeafGenerationBootstrapFiles(leafDir string) ([]leafGenerationBootstrapFile, error) {
	entries, err := os.ReadDir(leafDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	files := make([]leafGenerationBootstrapFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		lane, seq, ok := parseLeafGenerationBootstrapFileName(entry.Name())
		if !ok {
			continue
		}
		if lane != rewriteLeafLogLaneID {
			continue
		}
		rawFileID, err := valuelog.EncodeSegmentID(lane, seq)
		if err != nil {
			return nil, err
		}
		files = append(files, leafGenerationBootstrapFile{rawFileID: rawFileID, lane: lane, seq: seq})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].lane != files[j].lane {
			return files[i].lane < files[j].lane
		}
		if files[i].seq != files[j].seq {
			return files[i].seq < files[j].seq
		}
		return files[i].rawFileID < files[j].rawFileID
	})
	return files, nil
}

func bootstrapLeafGenerationManifestFromDir(leafDir string, commitSeq uint64) (*leafGenerationManifest, error) {
	files, err := listLeafGenerationBootstrapFiles(leafDir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return newLeafGenerationManifest(commitSeq), nil
	}
	manifest := &leafGenerationManifest{
		Version:             leafGenerationManifestVersion,
		CurrentGenerationID: uint64(len(files)),
		NextGenerationID:    uint64(len(files) + 1),
		Generations:         make([]leafGenerationRecord, 0, len(files)),
	}
	for i, file := range files {
		generationID := uint64(i + 1)
		state := leafGenerationStateSealed
		sealedCommitSeq := commitSeq
		if i == len(files)-1 {
			state = leafGenerationStateWritable
			sealedCommitSeq = 0
		}
		manifest.Generations = append(manifest.Generations, leafGenerationRecord{
			GenerationID:       generationID,
			State:              state,
			FileIDs:            []uint32{file.rawFileID},
			CreatedCommitSeq:   commitSeq,
			SealedCommitSeq:    sealedCommitSeq,
			PublishedCommitSeq: commitSeq,
		})
	}
	return manifest, nil
}

func reconcileLeafGenerationManifestWithDir(leafDir string, manifest *leafGenerationManifest, commitSeq uint64) (*leafGenerationManifest, bool, error) {
	if manifest == nil {
		return nil, false, nil
	}
	files, err := listLeafGenerationBootstrapFiles(leafDir)
	if err != nil {
		return nil, false, err
	}
	if len(files) == 0 {
		return manifest, false, nil
	}
	seen := make(map[uint32]struct{}, len(manifest.Generations))
	for _, gen := range manifest.Generations {
		if gen.State == leafGenerationStateDeleted {
			continue
		}
		for _, rawFileID := range gen.FileIDs {
			if rawFileID == 0 {
				continue
			}
			seen[rawFileID] = struct{}{}
		}
	}
	reconciled := manifest.clone()
	changedAny := false
	currentSeq := reconciled.currentGenerationMaxRecoveredSeq()
	for _, file := range files {
		if _, ok := seen[file.rawFileID]; ok {
			continue
		}
		changed := false
		var err error
		if file.seq > currentSeq {
			changed, err = reconciled.registerCurrentGenerationFileID(file.rawFileID, commitSeq)
			if err == nil && file.seq > currentSeq {
				currentSeq = file.seq
			}
		} else {
			changed, err = reconciled.appendRecoveredSealedGenerationFileID(file.rawFileID, commitSeq)
		}
		if err != nil {
			return nil, false, err
		}
		if changed {
			changedAny = true
			seen[file.rawFileID] = struct{}{}
		}
	}
	if !changedAny {
		return manifest, false, nil
	}
	return reconciled, true, nil
}

func loadOrCreateLeafGenerationManifest(leafDir string, commitSeq uint64, readOnly bool) (*leafGenerationManifest, error) {
	manifest, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		return nil, err
	}
	if ok {
		reconciled, changed, err := reconcileLeafGenerationManifestWithDir(leafDir, manifest, commitSeq)
		if err != nil {
			return nil, err
		}
		if changed {
			if !readOnly {
				if err := saveLeafGenerationManifest(leafDir, reconciled); err != nil {
					return nil, err
				}
			}
			return reconciled, nil
		}
		return manifest, nil
	}
	manifest, err = bootstrapLeafGenerationManifestFromDir(leafDir, commitSeq)
	if err != nil {
		return nil, err
	}
	if readOnly {
		return manifest, nil
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		return nil, err
	}
	return manifest.clone(), nil
}
