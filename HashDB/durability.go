package hashdb

import "fmt"

// syncActiveSegment fsyncs the active slab segment. If a write rotated to a new
// slab segment, it also fsyncs the DB directory (where supported) so the new
// file name is durable.
func (h *DB) syncActiveSegment(prevSegmentID uint16) error {
	f, ok := h.slabFiles[h.activeSegmentID]
	if !ok || f == nil {
		return fmt.Errorf("sync active segment: missing slab-%d", h.activeSegmentID)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if h.activeSegmentID != prevSegmentID {
		if err := syncDir(h.dir); err != nil {
			return err
		}
	}
	return nil
}
