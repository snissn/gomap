package slab

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap-gemini/TreeDB/page"
)

// SlabSet is an immutable list of SlabFiles active at a specific point in time.
type SlabSet struct {
	Files map[uint32]*SlabFile
}

type SlabManager struct {
	dir        string
	activeSlab *SlabFile
	slabs      map[uint32]*SlabFile // The master list of all live + zombie slabs
	mu         sync.RWMutex
}

func NewSlabManager(dir string) (*SlabManager, error) {
	sm := &SlabManager{
		dir:   dir,
		slabs: make(map[uint32]*SlabFile),
	}
	
	matches, err := filepath.Glob(filepath.Join(dir, "data-*.slab"))
	if err != nil {
		return nil, err
	}
	
	var maxID uint32
	found := false
	
	for _, path := range matches {
		var id uint32
		_, err := fmt.Sscanf(filepath.Base(path), "data-%04d.slab", &id)
		if err == nil {
			s, err := OpenSlab(path, id)
			if err != nil {
				return nil, err
			}
			sm.slabs[id] = s
			if id >= maxID {
				maxID = id
				found = true
			}
		}
	}
	
	if !found {
		s, err := OpenSlab(filepath.Join(dir, "data-0000.slab"), 0)
		if err != nil {
			return nil, err
		}
		sm.slabs[0] = s
		sm.activeSlab = s
	} else {
		sm.activeSlab = sm.slabs[maxID]
	}
	
	return sm, nil
}

func (sm *SlabManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	for _, s := range sm.slabs {
		_ = s.Close()
	}
	sm.slabs = nil
	return nil
}

// Read reads from the slab file identified by ptr.FileID.
// For Snapshot Isolation, the caller should ensure the file is pinned via a Snapshot.
// If accessing without snapshot (e.g. during Compaction or internal ops), care must be taken.
// Current impl uses RLock on the master map, so it's safe against concurrent Close() initiated by Prune/Compaction
// IF Prune/Compaction removes from map.
func (sm *SlabManager) Read(ptr page.ValuePtr) ([]byte, error) {
	sm.mu.RLock()
	s, ok := sm.slabs[ptr.FileID]
	sm.mu.RUnlock()
	
	if !ok {
		return nil, fmt.Errorf("slab file %d not found", ptr.FileID)
	}
	
	return s.Read(int64(ptr.Offset))
}

func (sm *SlabManager) Append(key, value []byte) (page.ValuePtr, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	offset, err := sm.activeSlab.Write(key, value)
	if err == ErrSlabFull {
		if err := sm.rotate(); err != nil {
			return page.ValuePtr{}, err
		}
		offset, err = sm.activeSlab.Write(key, value)
	}
	
	if err != nil {
		return page.ValuePtr{}, err
	}
	
	length := 2 + 4 + len(key) + len(value)
	
	return page.ValuePtr{
		Offset: uint64(offset),
		Length: uint32(length),
		FileID: sm.activeSlab.ID,
	}, nil
}

func (sm *SlabManager) rotate() error {
	if err := sm.activeSlab.Sync(); err != nil {
		return err
	}
	
	newID := sm.activeSlab.ID + 1
	filename := fmt.Sprintf("data-%04d.slab", newID)
	path := filepath.Join(sm.dir, filename)
	
	newSlab, err := OpenSlab(path, newID)
	if err != nil {
		return err
	}
	
	sm.slabs[newID] = newSlab
	sm.activeSlab = newSlab
	
	return nil
}

func (sm *SlabManager) Sync() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.activeSlab.Sync()
}

func (sm *SlabManager) ActiveSlabID() uint32 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.activeSlab.ID
}

func (sm *SlabManager) ActiveSlabTail() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return uint64(sm.activeSlab.Size)
}

func (sm *SlabManager) SetActiveSlab(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	s, ok := sm.slabs[id]
	if !ok {
		path := filepath.Join(sm.dir, fmt.Sprintf("data-%04d.slab", id))
		var err error
		s, err = OpenSlab(path, id)
		if err != nil {
			return err
		}
		sm.slabs[id] = s
	}
	sm.activeSlab = s
	return nil
}

func (sm *SlabManager) TruncateActiveSlab(offset uint64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.activeSlab.Truncate(int64(offset))
}

func (sm *SlabManager) PruneSlabs(maxID uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	for id, s := range sm.slabs {
		if id > maxID {
			if err := s.Close(); err != nil {
				return err
			}
			if err := os.Remove(s.Path); err != nil {
				return err
			}
			delete(sm.slabs, id)
		}
	}
	return nil
}

// CurrentSlabSet returns a snapshot of the current slabs.
func (sm *SlabManager) CurrentSlabSet() *SlabSet {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	files := make(map[uint32]*SlabFile, len(sm.slabs))
	for k, v := range sm.slabs {
		files[k] = v
	}
	return &SlabSet{Files: files}
}

// AcquireSlabs increments the RefCount for all slabs in the set.
func (sm *SlabManager) AcquireSlabs(set *SlabSet) {
	for _, s := range set.Files {
		s.RefCount.Add(1)
	}
}

// ReleaseSlabs decrements the RefCount. If 0 and Zombie, closes and removes.
func (sm *SlabManager) ReleaseSlabs(set *SlabSet) error {
	// We iterate the set. For each slab:
	// Decrement RefCount.
	// If RefCount == 0 && IsZombie -> Delete.
	
	// We need to lock SlabManager to remove from master map if we delete.
	// But we can check condition without lock first?
	// `IsZombie` is atomic. `RefCount` is atomic.
	// But `delete(sm.slabs, id)` needs lock.
	
	var err error
	
	for _, s := range set.Files {
		newRef := s.RefCount.Add(-1)
		if newRef == 0 && s.IsZombie.Load() {
			// Needs cleanup.
			// Lock manager.
			sm.mu.Lock()
			// Double check?
			// RefCount could have gone up?
			// No, once Zombie, new readers don't pick it up (unless they pick it from old snapshot?).
			// Wait, "Snapshot Safety Invariant".
			// If IsZombie is true, it means it was removed from ACTIVE set.
			// Only old snapshots hold it.
			// So no NEW snapshot can acquire it.
			// So RefCount cannot go up.
			
			// Double check ref count just in case?
			if s.RefCount.Load() == 0 {
				if _, exists := sm.slabs[s.ID]; exists {
					if e := s.Close(); e != nil {
						err = e // store error but continue?
					}
					if e := os.Remove(s.Path); e != nil {
						err = e
					}
					delete(sm.slabs, s.ID)
				}
			}
			sm.mu.Unlock()
		}
	}
	return err
}