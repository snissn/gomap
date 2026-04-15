package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	for i := range m.Generations {
		gen := m.Generations[i]
		if gen.GenerationID == 0 {
			return fmt.Errorf("treedb: %s generation[%d] has zero generation_id", leafGenerationManifestFileName, i)
		}
		if _, dup := seen[gen.GenerationID]; dup {
			return fmt.Errorf("treedb: %s generation_id %d appears more than once", leafGenerationManifestFileName, gen.GenerationID)
		}
		seen[gen.GenerationID] = struct{}{}
		switch gen.State {
		case leafGenerationStateWritable, leafGenerationStateSealed, leafGenerationStateRetiring, leafGenerationStateDeleted:
		default:
			return fmt.Errorf("treedb: %s generation[%d] has unsupported state %q", leafGenerationManifestFileName, i, gen.State)
		}
		if gen.GenerationID == m.CurrentGenerationID {
			currentFound = true
		}
	}
	if !currentFound {
		return fmt.Errorf("treedb: %s current_generation_id %d not found in generations", leafGenerationManifestFileName, m.CurrentGenerationID)
	}
	return nil
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

func loadOrCreateLeafGenerationManifest(leafDir string, commitSeq uint64, readOnly bool) (*leafGenerationManifest, error) {
	manifest, ok, err := loadLeafGenerationManifest(leafDir)
	if err != nil {
		return nil, err
	}
	if ok {
		return manifest, nil
	}
	manifest = newLeafGenerationManifest(commitSeq)
	if readOnly {
		return manifest, nil
	}
	if err := saveLeafGenerationManifest(leafDir, manifest); err != nil {
		return nil, err
	}
	return manifest.clone(), nil
}
