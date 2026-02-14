package pebblecompat

import "errors"

var (
	// ErrClosed indicates the compatibility wrapper has been closed.
	ErrClosed = errors.New("pebblecompat: db closed")
	// ErrReservedKeyPrefix indicates a user key collided with wrapper metadata.
	ErrReservedKeyPrefix = errors.New("pebblecompat: user key uses reserved internal prefix")
	// ErrInvalidRange indicates a malformed [start, end) span.
	ErrInvalidRange = errors.New("pebblecompat: invalid key range")
	// ErrSharedSSTUnsupported indicates a shared-ingest backing cannot be resolved by pebblecompat.
	ErrSharedSSTUnsupported = errors.New("pebblecompat: shared sst ingest unsupported")
	// ErrExternalFileUnsupported indicates an external file descriptor cannot be resolved locally.
	ErrExternalFileUnsupported = errors.New("pebblecompat: external file requires a local ObjName path")
	// ErrCheckpointOptionUnsupported indicates checkpoint options are not implemented by pebblecompat.
	ErrCheckpointOptionUnsupported = errors.New("pebblecompat: checkpoint options unsupported")
)
