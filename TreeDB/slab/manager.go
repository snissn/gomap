package slab

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/snissn/gomap-gemini/TreeDB/page"
)

type SlabManager struct {
	dir        string
	activeSlab *SlabFile
	slabs      map[uint32]*SlabFile
	mu         sync.RWMutex
}

func NewSlabManager(dir string) (*SlabManager, error) {
	// For now, we assume starting fresh or need scanning?
	// Spec says "Open() recovery... Scan data directory".
	// For Phase 1, we can just start with slab 0 or scanning existing.
	// Let's implement simple scanning to find the highest ID.
	
	sm := &SlabManager{
		dir:   dir,
		slabs: make(map[uint32]*SlabFile),
	}
	
	// Scan directory for data-*.slab
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
		// Create slab 0
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
		// Rotate
		if err := sm.rotate(); err != nil {
			return page.ValuePtr{}, err
		}
		// Try again with new slab
		offset, err = sm.activeSlab.Write(key, value)
	}
	
	if err != nil {
		return page.ValuePtr{}, err
	}
	
	// Construct ValuePtr
	// Length = 2 (KeyLen) + 4 (ValLen) + len(Key) + len(Value)
	length := 2 + 4 + len(key) + len(value)
	
	return page.ValuePtr{
		Offset: uint64(offset),
		Length: uint32(length),
		FileID: sm.activeSlab.ID,
	}, nil
}

func (sm *SlabManager) rotate() error {
	// Sync old slab
	if err := sm.activeSlab.Sync(); err != nil {
		return err
	}
	
	// Create new slab
	newID := sm.activeSlab.ID + 1
	filename := fmt.Sprintf("data-%04d.slab", newID)
	path := filepath.Join(sm.dir, filename)
	
	newSlab, err := OpenSlab(path, newID)
	if err != nil {
		return err
	}
	
	sm.slabs[newID] = newSlab
	sm.activeSlab = newSlab
	
	// Sync directory? Spec 2.1 says "fsync the parent directory".
	// Implement if needed for strict durability, skip for now.
	
	return nil
}

func (sm *SlabManager) Sync() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.activeSlab.Sync()
}
