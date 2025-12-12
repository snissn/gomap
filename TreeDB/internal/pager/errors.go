package pager

import "errors"

var (
	// ErrInvalidChunkSize is returned when ChunkSize is not aligned to PageSize.
	ErrInvalidChunkSize = errors.New("pager: chunk size must be multiple of page size")
	// ErrFileCorrupt is returned when on-disk structures are invalid.
	ErrFileCorrupt = errors.New("pager: index file corrupt")
	// ErrPageOutOfBounds is returned when a PageID is outside the mapped region.
	ErrPageOutOfBounds = errors.New("pager: page out of bounds")
	// ErrShrinkForbidden is returned when an attempt is made to shrink index.db.
	ErrShrinkForbidden = errors.New("pager: shrinking index.db while mapped is forbidden")
	// ErrChunkPinned is returned when closing with pinned chunks.
	ErrChunkPinned = errors.New("pager: cannot unmap chunk with active references")
	// ErrMutablePageNotNew is returned when attempting to mutate a non-new page via WithMutablePage.
	ErrMutablePageNotNew = errors.New("pager: mutable access requires a newly allocated page")
)
