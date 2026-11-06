package dictdb

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

var ErrReadDefinitionCapacity = errors.New("dictdb: bounded read definition capacity exhausted")
var ErrReadDefinitionIdentity = errors.New("dictdb: dictionary read identity mismatch")

// DictionaryReadAllocationSizes describes each separate admission stage in raw
// bytes. Round fields separately. Owner admission occurs before constructing
// the snapshot callback; Snapshot admission precedes capture; Bytes admission
// precedes definition/payload backing and the optional physical pin.
type DictionaryReadAllocationSizes struct {
	Owner, Callback, Definition, Payload, Pin uint64
	Snapshot                                  db.SnapshotAllocationSizes
	CaptureMetadata                           **retainedalloc.Enrollment
}

// DictionaryReadDefinition owns immutable validated bytes and the optional
// exact physical source pin. The index snapshot fences capture only and is
// released after validated copying. Bytes must not be mutated. Close is safe after readers have
// drained. Release uses the existing storage owner's cleanup; releasing its
// last old Set may delete zombies and sync their directory. Preparation callers
// must defer this cleanup until their writer/admission locks are released.
type DictionaryReadDefinition struct {
	Bytes                                       []byte
	snapshot                                    *db.Snapshot
	metadata                                    *retainedalloc.Enrollment
	pin                                         *rootpublication.IdentityPin
	releaseOwner, releaseSnapshot, releaseBytes func()
	keyScratch                                  [page.PageSize]byte
	leafScratch                                 [page.PageSize]byte
	key                                         [len("bytes/") + 8]byte
	once, captureOnce                           sync.Once
}

func (d *DictionaryReadDefinition) Close() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		d.ReleaseCapture()
		d.pin.Release()
		d.pin = nil
		d.Bytes = nil
		if d.releaseBytes != nil {
			d.releaseBytes()
			d.releaseBytes = nil
		}
		if d.releaseOwner != nil {
			d.releaseOwner()
			d.releaseOwner = nil
		}
	})
}

// ReleaseCapture retires the temporary index/Set capture after copying. Call
// only after preparation ownership/admission locks are released. Decoder users
// need only the independently owned bytes and optional physical source pin.
func (d *DictionaryReadDefinition) ReleaseCapture() {
	if d == nil {
		return
	}
	d.captureOnce.Do(func() {
		if d.snapshot != nil {
			_ = d.snapshot.Close()
			d.snapshot = nil
		}
		if d.metadata != nil {
			d.metadata.Close()
			d.metadata = nil
		}
		if d.releaseSnapshot != nil {
			d.releaseSnapshot()
			d.releaseSnapshot = nil
		}
	})
}

// PrepareDictionaryReadDefinition transfers a partial owner even on error after
// owner admission. The caller must call Close outside writer/admission locks on
// failure, or ReleaseCapture outside those locks after a successful preparation.
// It never drains temporary snapshot/resource callbacks inside those locks.
func (s *Store) PrepareDictionaryReadDefinition(id uint64, limits valuelog.COWReadLimits, maxResources int, admit func(DictionaryReadAllocationSizes) (func(), error)) (*DictionaryReadDefinition, error) {
	return s.acquireDictionaryReadDefinition(id, limits, maxResources, admit, true)
}

// AcquireDictionaryReadDefinition is the built-in provider's pure, bounded
// read capability. Its append-only bytes/<hash ID> entries and SHA identity
// check permit old cuts to resolve the same definition after marker changes.
// admit transfers one independently owned release function on each successful
// stage; all partial stages are released on refusal. No allocating GetDictBytes,
// durable CaptureDictionaryResources, checkpoint, or namespace proof is used.
func (s *Store) AcquireDictionaryReadDefinition(id uint64, limits valuelog.COWReadLimits, maxResources int, admit func(DictionaryReadAllocationSizes) (func(), error)) (*DictionaryReadDefinition, error) {
	return s.acquireDictionaryReadDefinition(id, limits, maxResources, admit, false)
}

func (s *Store) acquireDictionaryReadDefinition(id uint64, limits valuelog.COWReadLimits, maxResources int, admit func(DictionaryReadAllocationSizes) (func(), error), deferred bool) (*DictionaryReadDefinition, error) {
	if s == nil || s.backend == nil {
		return nil, errStoreUnavailable
	}
	if admit == nil || id == 0 || limits.MaxValueBytes == 0 || maxResources < 1 {
		return nil, ErrReadDefinitionCapacity
	}
	ownerRelease, err := admit(DictionaryReadAllocationSizes{Owner: uint64(unsafe.Sizeof(DictionaryReadDefinition{})), Callback: 6 * uint64(unsafe.Sizeof(uintptr(0)))})
	if err != nil {
		return nil, err
	}
	d := &DictionaryReadDefinition{releaseOwner: ownerRelease}
	d.snapshot, err = s.backend.AcquireStableSnapshotWithAllocationAdmission(func(sizes db.SnapshotAllocationSizes) error {
		if sizes.ValueLog.MapHint > maxResources || sizes.ValueLog.FileCount > maxResources {
			return ErrReadDefinitionCapacity
		}
		release, err := admit(DictionaryReadAllocationSizes{Snapshot: sizes, CaptureMetadata: &d.metadata})
		if err == nil {
			d.releaseSnapshot = release
		}
		return err
	})
	if err != nil {
		return failDictionaryReadDefinition(d, err, deferred)
	}
	if err = d.snapshot.AdoptPrimaryMetadataEnrollment(d.metadata); err != nil {
		return failDictionaryReadDefinition(d, err, deferred)
	}
	d.metadata = nil
	copy(d.key[:], "bytes/")
	binary.BigEndian.PutUint64(d.key[len("bytes/"):], id)
	entry, err := d.snapshot.GetEntryExactWithFixedScratch(d.key[:], d.keyScratch[:], d.leafScratch[:], nil)
	if err != nil {
		return failDictionaryReadDefinition(d, err, deferred)
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return failDictionaryReadDefinition(d, tree.ErrKeyNotFound, deferred)
	}
	if entry.Flags&node.FlagPointer == 0 {
		if uint64(len(entry.Value)) > limits.MaxValueBytes {
			return failDictionaryReadDefinition(d, ErrReadDefinitionCapacity, deferred)
		}
		d.releaseBytes, err = admit(DictionaryReadAllocationSizes{Definition: uint64(len(entry.Value))})
		if err != nil {
			return failDictionaryReadDefinition(d, err, deferred)
		}
		d.Bytes = append([]byte(nil), entry.Value...)
	} else {
		file, identity, err := d.snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
		if err != nil {
			return failDictionaryReadDefinition(d, err, deferred)
		}
		shape, err := valuelog.InspectCOWRecord(file, entry.ValuePtr, limits)
		if err != nil {
			return failDictionaryReadDefinition(d, err, deferred)
		}
		// Built-in dictionary producers write raw definitions. Reject recursive
		// compressed definitions instead of constructing an unadmitted decoder.
		if shape.Compressed {
			return failDictionaryReadDefinition(d, ErrReadDefinitionIdentity, deferred)
		}
		d.releaseBytes, err = admit(DictionaryReadAllocationSizes{Definition: shape.ValueBytes, Payload: shape.RecordBytes, Pin: uint64(unsafe.Sizeof(rootpublication.IdentityPin{}))})
		if err != nil {
			return failDictionaryReadDefinition(d, err, deferred)
		}
		d.pin, err = s.backend.ValueLogIdentityPinRegistry().Pin(identity)
		if err != nil {
			return failDictionaryReadDefinition(d, err, deferred)
		}
		payload := make([]byte, shape.RecordBytes)
		output := make([]byte, shape.ValueBytes)
		d.Bytes, err = valuelog.ReadCOWRecord(file, entry.ValuePtr, shape, true, payload, nil, output, nil)
		if err != nil {
			return failDictionaryReadDefinition(d, err, deferred)
		}
	}
	sum := sha256.Sum256(d.Bytes)
	if binary.BigEndian.Uint64(sum[:8]) != id {
		return failDictionaryReadDefinition(d, ErrReadDefinitionIdentity, deferred)
	}
	// The append-only content identity is now owned in Bytes. Namespace
	// stability is required only while reading the source, not while decoding
	// this independently copied definition. Keep the file pin until Close.
	if !deferred {
		d.ReleaseCapture()
	}
	return d, nil
}

func failDictionaryReadDefinition(d *DictionaryReadDefinition, err error, deferred bool) (*DictionaryReadDefinition, error) {
	if deferred {
		return d, err
	}
	d.Close()
	return nil, err
}
