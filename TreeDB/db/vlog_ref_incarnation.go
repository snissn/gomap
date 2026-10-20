package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Copied physical evidence belongs to the SAME generation as the counted roots.
// indexID is only an in-process discriminator; it is never persisted or used to
// accept a cache from another DB lifetime. No pager or generation alias is kept.
type valueLogRefIncarnation struct {
	indexID uint64
	data    rootpublication.StableIdentity
}

func (i valueLogRefIncarnation) samePhysical(other valueLogRefIncarnation) bool {
	return i.data == other.data
}

func captureValueLogRefIncarnation(idx *indexGen) (valueLogRefIncarnation, error) {
	var i valueLogRefIncarnation
	if idx == nil || idx.pager == nil {
		return i, errors.New("missing value-log ref index incarnation")
	}
	i.indexID = idx.id
	err := idx.pager.WithStableResourceFile(func(f *os.File) error {
		var e error
		i.data, e = rootpublication.StableIdentityFromFile(f)
		return e
	})
	if err != nil {
		return valueLogRefIncarnation{}, err
	}

	if !i.valid() {
		return valueLogRefIncarnation{}, errors.New("invalid value-log ref physical incarnation")
	}
	return i, nil
}

// One immutable view supplies both sequence and original generation. Ordinary
// deltas carry the already-captured pair; they do not stat files per commit.
func (db *DB) currentValueLogRefView() (uint64, uint64) {
	if db == nil {
		return 0, 0
	}
	view := db.snapshotViewRO.Load()
	if view == nil || view.idx == nil || view.state == nil {
		return 0, 0
	}
	return view.state.CommitSeq, view.idx.id
}

func (db *DB) canTrackValueLogRefs(seq uint64) bool {
	currentSeq, generation := db.currentValueLogRefView()
	return seq == currentSeq && db.valueLogRefTracker.canTrackGeneration(seq, generation)
}

func (t *valueLogRefTracker) canTrackGeneration(seq, generation uint64) bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.valid && t.commitSeq == seq && t.incarnation.indexID == generation
}

func (t *valueLogRefTracker) referencedSetGeneration(seq, generation uint64) (map[uint32]struct{}, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.valid || t.commitSeq != seq || t.incarnation.indexID != generation {
		return nil, false
	}
	out := t.counts.referencedSet()
	return out, true
}

func (db *DB) allocateValueLogRefTracker() error {
	if db.valueLogRefTracker != nil {
		return nil
	}
	owner := db.valueLogIdentityPins.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(valueLogRefTracker{})))
	if err := owner.AddPending(charge); err != nil {
		return err
	}
	db.valueLogRefTracker = &valueLogRefTracker{metadata: owner, metadataCharge: charge}
	return nil
}

// Counts and fixed evidence are plain copied values. Consume those payloads
// before refunding their original containing allocation, without waiting for an
// unrelated future physical file lifetime or adding another cleanup owner.
func (t *valueLogRefTracker) close() error {
	if t == nil {
		return nil
	}
	if err := t.finishCacheRead(); err != nil {
		return err
	}
	t.mu.Lock()
	owner, charge := t.metadata, t.metadataCharge
	t.counts.close()
	t.incarnation = valueLogRefIncarnation{}
	t.valid, t.dirty = false, false
	t.metadata, t.metadataCharge = nil, 0
	t.revision++
	t.mu.Unlock()
	if owner != nil {
		owner.RemovePending(charge)
	}
	return nil
}

// Fixed canonical identities and an explicit DATA-only/paired form are covered
// by the cache CRC. The process-local index ID is deliberately omitted.
const valueLogRefIncarnationDiskSize = 4 + 2*48 + 16

func appendValueLogRefIncarnation(buf []byte, i valueLogRefIncarnation) []byte {
	var image [valueLogRefIncarnationDiskSize]byte

	put := func(dst []byte, id rootpublication.StableIdentity) {
		copy(dst[:16], id.Platform)
		binary.LittleEndian.PutUint64(dst[16:24], id.VolumeID)
		copy(dst[24:40], id.ObjectID[:])
		binary.LittleEndian.PutUint64(dst[40:48], id.Generation)
	}
	put(image[4:52], i.data)
	return append(buf, image[:]...)
}

func decodeValueLogRefIncarnation(image []byte) (valueLogRefIncarnation, error) {
	var i valueLogRefIncarnation
	if len(image) != valueLogRefIncarnationDiskSize {
		return i, errValueLogRefCorrupt
	}
	get := func(src []byte) (rootpublication.StableIdentity, bool) {
		var id rootpublication.StableIdentity
		end := bytes.IndexByte(src[:16], 0)
		if end < 0 {
			return id, false
		}
		for _, b := range src[end:16] {
			if b != 0 {
				return id, false
			}
		}
		id.Platform = string(src[:end])
		id.VolumeID = binary.LittleEndian.Uint64(src[16:24])
		copy(id.ObjectID[:], src[24:40])
		id.Generation = binary.LittleEndian.Uint64(src[40:48])
		return id, true
	}
	var ok bool
	i.data, ok = get(image[4:52])
	if !ok {
		return i, errValueLogRefCorrupt
	}
	for _, reserved := range image[52:] {
		if reserved != 0 {
			return i, errValueLogRefCorrupt
		}
	}
	if binary.LittleEndian.Uint32(image[:4]) != 0 || !i.valid() {
		return valueLogRefIncarnation{}, errValueLogRefCorrupt
	}
	return i, nil
}

// Physical replacement preserves logical counts only under the caller's exact
// old-generation cutover proof. A new pair is captured before publication.
func (t *valueLogRefTracker) advanceIncarnation(nextSeq, oldGeneration uint64, next valueLogRefIncarnation) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.valid {
		return nil
	}
	if t.incarnation.indexID != oldGeneration || nextSeq != t.commitSeq+1 || !next.valid() {
		return errors.New("value-log ref physical transition mismatch")
	}
	t.commitSeq, t.incarnation = nextSeq, next
	t.dirty = true
	t.revision++
	return nil
}

func (i valueLogRefIncarnation) valid() bool {
	return rootpublication.SamePhysicalIdentity(i.data, i.data)
}
