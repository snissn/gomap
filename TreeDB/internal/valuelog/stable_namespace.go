package valuelog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

var ErrStableNamespaceUnsupported = errors.New("valuelog: stable namespace persistence unsupported")

var captureStableNamespaceDirectory = captureStableNamespaceDirectoryPlatform

type stableNamespaceState struct {
	mu             sync.Mutex
	directory      *os.File
	parentIdentity StableFileIdentity
	targetIdentity StableFileIdentity
	targetName     string
	targetPath     string
	generation     uint64
	established    bool
	err            error
	refs           int
	syncDirectory  func(*os.File, string) error
}

func newStableNamespaceState(path string, target StableFileIdentity, generation uint64, syncDirectory func(*os.File, string) error) *stableNamespaceState {
	directory, parent, err := captureStableNamespaceDirectory(path)
	targetName := filepath.Base(path)
	if targetName == "." || targetName == string(filepath.Separator) {
		err = fmt.Errorf("invalid namespace target path %q", path)
	}
	return &stableNamespaceState{
		directory:      directory,
		parentIdentity: parent,
		targetIdentity: target,
		targetName:     targetName,
		targetPath:     path,
		generation:     generation,
		err:            err,
		refs:           1,
		syncDirectory:  syncDirectory,
	}
}

func (w *Writer) newStableNamespaceState(path string, f *os.File, generation uint64, created bool) *stableNamespaceState {
	if !created {
		return nil
	}
	target, err := StableFileIdentityFromFile(f)
	if err != nil {
		return &stableNamespaceState{targetPath: path, generation: generation, err: err, refs: 1}
	}
	return newStableNamespaceState(path, target, generation, w.syncStableNamespaceDirectory)
}

func (s *stableNamespaceState) markEstablished() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.established = true
	if s.directory != nil {
		_ = s.directory.Close()
		s.directory = nil
	}
	s.mu.Unlock()
}

func (s *stableNamespaceState) acquire(target StableFileIdentity) (*stableNamespaceState, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.established {
		return nil, nil
	}
	if s.targetIdentity != target {
		return nil, fmt.Errorf("valuelog: namespace target identity changed")
	}
	if s.err != nil || s.directory == nil || !s.parentIdentity.valid() {
		return nil, fmt.Errorf("%w: %v", ErrStableNamespaceUnsupported, s.err)
	}
	s.refs++
	return s, nil
}

func (s *stableNamespaceState) establish(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.established {
		return nil
	}
	if s.err != nil || s.directory == nil || s.syncDirectory == nil {
		return fmt.Errorf("%w: %v", ErrStableNamespaceUnsupported, s.err)
	}
	// Resolve the child through the retained parent descriptor. targetPath
	// is display-only and may now name a replacement directory or file.
	identity, err := stableNamespaceTargetIdentity(s.directory, s.targetName)
	if err != nil {
		return fmt.Errorf("valuelog: inspect stable namespace target: %w", err)
	}
	if identity != s.targetIdentity {
		return fmt.Errorf("valuelog: namespace target identity changed")
	}
	if err := s.syncDirectory(s.directory, s.targetPath); err != nil {
		return err
	}
	s.established = true
	_ = s.directory.Close()
	s.directory = nil
	return nil
}

func (s *stableNamespaceState) release() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.refs--
	if s.refs == 0 && s.directory != nil {
		_ = s.directory.Close()
		s.directory = nil
	}
	s.mu.Unlock()
}

func (w *Writer) syncStableNamespaceDirectory(directory *os.File, targetPath string) error {
	if w == nil || directory == nil {
		return ErrStableNamespaceUnsupported
	}
	dir := filepath.Dir(targetPath)
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceValueLog, "", dir); err != nil {
		return err
	}
	start := time.Now()
	err := directory.Sync()
	w.directorySyncCalls.Add(1)
	if ns := time.Since(start).Nanoseconds(); ns > 0 {
		w.directorySyncNs.Add(uint64(ns))
	}
	if err != nil {
		w.directorySyncErrors.Add(1)
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceValueLog, "", dir)
}
