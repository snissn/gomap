package collectionwal

import "errors"

var (
	ErrCollectionWALTerminalTail       = errors.New("collectionwal: terminal incomplete tail")
	ErrCollectionWALCorruptMiddle      = errors.New("collectionwal: corrupt middle")
	ErrCollectionWALBadChecksum        = errors.New("collectionwal: bad checksum")
	ErrCollectionWALUnsupportedVersion = errors.New("collectionwal: unsupported version")
	ErrCollectionWALResourceLimit      = errors.New("collectionwal: resource limit")
	ErrCollectionWALUnsafePath         = errors.New("collectionwal: unsafe path")
	ErrCollectionWALMissingSideRef     = errors.New("collectionwal: missing side ref")
	ErrCollectionWALIdentityMismatch   = errors.New("collectionwal: identity mismatch")
	ErrCollectionWALSequenceGap        = errors.New("collectionwal: sequence gap")
	ErrCollectionWALRedacted           = errors.New("collectionwal: redacted")
	ErrCollectionWALRecoveryRequired   = errors.New("collectionwal: recovery required")
	ErrCollectionWALUnsupportedMode    = errors.New("collectionwal: unsupported mode")
)
