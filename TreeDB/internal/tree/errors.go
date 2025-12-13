package tree

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/page"
)

var (
	// ErrNotFound is returned when a key is absent from the tree.
	ErrNotFound = errors.New("tree: not found")

	// ErrEmptyTree is returned when operations require a root but the tree is empty.
	ErrEmptyTree = errors.New("tree: empty")

	// ErrCorrupt indicates an unexpected on-disk page layout.
	ErrCorrupt = errors.New("tree: corrupt page")
)

// LeafEntry mirrors the leaf payload semantics: inline, pointer, or tombstone.
// Key bytes are stored separately in the leaf page directory.
type LeafEntry struct {
	Flags       page.LeafFlags
	InlineValue []byte
	Ptr         page.ValuePtr
}

// IsTombstone reports whether this entry is a tombstone.
func (e LeafEntry) IsTombstone() bool { return e.Flags == page.LeafFlagTombstone }

