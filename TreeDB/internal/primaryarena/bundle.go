package primaryarena

import (
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/pager"
)

// PublicationBundle has distinct adjacent physical pages. The record owns the
// directory's independent root handle after installation. Their allocator unit
// is reusable only after BOTH pages and outgoing edges have actually drained.
// Pairing permits one mapping/dirty/verification span operation; it does not
// merge either codec, physical identity, checksum or custody certificate.
type PublicationBundle struct{ Directory, Record Ref }
type BundleClaim struct {
	arena               *Arena
	epoch, local, count uint64
	growth              *pager.Growth
	chunk               *bankChunk
	ready, cancelled    bool
	output              PublicationBundle
}

func (a *Arena) PrepareBundleClaim(w *iterator.OrdinalScanWork) (*BundleClaim, bool, error) {
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(BundleClaim{}))+128) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.recovered {
		return nil, false, ErrUnrecovered
	}
	if a.metadataDecoder == nil {
		return nil, false, ErrFormat
	}
	local := a.freePair
	if local == 0 && a.free != 0 {
		done, e := a.repackFreePairLocked(w)
		if e != nil || !done {
			return nil, false, e
		}
		local = a.freePair
	}
	count := a.next
	if local == 0 {
		local = (a.next + 1) &^ uint64(1)
		count = local + 2
	}
	if local+1 >= maxChunks*chunkBanks {
		return nil, false, ErrFull
	}
	c := &BundleClaim{arena: a, epoch: a.epoch, local: local, count: count}
	keepChunk := false
	defer func() {
		if !keepChunk {
			a.discardChunk(&c.chunk)
		}
	}()

	if a.chunks[local/chunkBanks] == nil {
		if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bankChunk{}))+128) {
			return nil, false, nil
		}
		var err error
		c.chunk, err = a.newChunk()
		if err != nil {
			return nil, false, err
		}
	}
	if count > a.next {
		growth, ok, e := pager.NewGrowthForPager(a.pager, count, w)
		if !ok || e != nil {
			return nil, false, e
		}
		c.growth = growth
	}
	keepChunk = true
	return c, true, nil
}
func (c *BundleClaim) Step(w *iterator.OrdinalScanWork) (PublicationBundle, bool, error) {
	if c == nil || c.cancelled {
		return PublicationBundle{}, false, ErrStale
	}
	if c.ready {
		return c.output, true, nil
	}
	a := c.arena
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.epoch != c.epoch {
		return PublicationBundle{}, false, ErrStale
	}
	bytes := 4*uint64(unsafe.Sizeof(bank{})) + 128
	records := uint64(2)
	if c.count > a.next && c.local > a.next {
		records++
		bytes += 2 * uint64(unsafe.Sizeof(bank{}))
	}
	install := func() {
		if c.chunk != nil {
			a.chunks[c.local/chunkBanks] = c.chunk
		}
		dir := &a.chunks[c.local/chunkBanks][c.local%chunkBanks]
		record := &a.chunks[c.local/chunkBanks][c.local%chunkBanks+1]
		next := dir.next
		dl, rl, dn, rn := dir.genericListed, record.genericListed, dir.freeNext, record.freeNext
		di, ri := dir.incarnation+1, record.incarnation+1
		*dir = bank{incarnation: di, refs: 1, class: Directory, pair: c.local, privatePublication: true, genericListed: dl, freeNext: dn}
		*record = bank{incarnation: ri, refs: 1, class: Record, pair: c.local, privatePublication: true, genericListed: rl, freeNext: rn}
		if a.freePair != 0 {
			a.freePair = next
		} else {
			// Alignment padding is a real reusable bank, not an invisible extent gap.
			if c.local > a.next {
				if a.chunks[a.next/chunkBanks] == nil {
					panic("primary arena: missing admitted alignment chunk")
				}
				padding := &a.chunks[a.next/chunkBanks][a.next%chunkBanks]
				*padding = bank{reusable: true, freeNext: a.free, genericListed: true}
				a.free = a.next
			}
			a.next = c.count
		}
		a.epoch++
		c.output = PublicationBundle{Directory: Ref{Namespace + c.local, di}, Record: Ref{Namespace + c.local + 1, ri}}
		c.ready = true
		a.counters.Claims += 2
		a.counters.Bytes += bytes
	}
	if c.growth != nil {
		ok, e := c.growth.StepWithInstallAtCount(a.pager, a.next, w, records, bytes, install)
		return c.output, ok, e
	}
	if !reserve(w, records, bytes) {
		return PublicationBundle{}, false, nil
	}
	install()
	return c.output, true, nil
}
func (c *BundleClaim) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.cancelled {
		return true, nil
	}
	if c.ready {
		return false, ErrStale
	}
	if c.growth != nil {
		ok, e := c.growth.Cancel(w)
		if !ok || e != nil {
			return ok, e
		}
	}
	c.arena.discardChunk(&c.chunk)
	c.cancelled = true
	return true, nil
}

// RepackFreePairsStep observes actual released bank records. It makes bounded
// allocator progress without writers, and never infers availability from age.
// Generic free-list links remain separate; lazy removal cannot alias a pair.
func (a *Arena) RepackFreePairsStep(w *iterator.OrdinalScanWork) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.recovered {
		return false, ErrUnrecovered
	}
	return a.repackFreePairLocked(w)
}
func (a *Arena) repackFreePairLocked(w *iterator.OrdinalScanWork) (bool, error) {
	if a.freePair != 0 {
		return true, nil
	}
	if a.repackEpoch != a.epoch {
		a.repackCursor = a.firstBank
		a.repackEpoch = a.epoch
	}
	if a.repackCursor >= a.next {
		return true, nil
	}
	for a.repackCursor+1 < a.next {
		first := (a.repackCursor + 1) &^ uint64(1)
		if first+1 >= a.next {
			a.repackCursor = a.next
			break
		}
		if !reserve(w, 2, 4*uint64(unsafe.Sizeof(bank{}))+64) {
			return false, nil
		}
		a.repackCursor = first + 2
		c := a.chunks[first/chunkBanks]
		if c == nil {
			continue
		}
		dir, record := &c[first%chunkBanks], &c[first%chunkBanks+1]
		if dir.refs != 0 || record.refs != 0 || !dir.reusable || !record.reusable || dir.pair != 0 || record.pair != 0 {
			continue
		}
		dir.pair = first
		record.pair = first
		dir.next = a.freePair
		a.freePair = first
		a.epoch++
		return true, nil
	}
	a.repackCursor = a.next
	return true, nil
}

// BundleRecordEncoder constructs the actual complete record for the exact new
// directory digest while the fresh current-bank guard is held. It only encodes
// private bytes; it must not call back into the arena or inspect a mutable head.
// The record's canonical decoder derives its genuine ownership edges below.
type BundleRecordEncoder func(directoryDigest [32]byte) ([]byte, error)
type bundleRecord struct {
	ref     Ref
	encode  BundleRecordEncoder
	durable bool
}

func (a *Arena) SealPublicationBundle(bundle PublicationBundle, directory []byte, groups [groupCount]*Group, encode BundleRecordEncoder, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealDirectory(bundle.Directory, directory, groups, &bundleRecord{ref: bundle.Record, encode: encode}, w)
}
func (a *Arena) SealExactReplacementBundle(bundle PublicationBundle, current Ref, entry node.PrimaryDirectoryEntry, expected page.EntryRevision, scratch []byte, encode BundleRecordEncoder, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealExactReplacement(bundle.Directory, current, entry, expected, scratch, &bundleRecord{ref: bundle.Record, encode: encode}, w)
}

// SealExactReplacementDurableBundle consumes the same fresh immutable-bank
// guard as replacement sealing and completes the actual pair file fence before
// installing its custody certificate. Failed fences leave private ownership and
// dirty membership intact for explicit cancellation or retry.
func (a *Arena) SealExactReplacementDurableBundle(bundle PublicationBundle, current Ref, entry node.PrimaryDirectoryEntry, expected page.EntryRevision, scratch []byte, encode BundleRecordEncoder, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealExactReplacement(bundle.Directory, current, entry, expected, scratch, &bundleRecord{ref: bundle.Record, encode: encode, durable: true}, w)
}

// writeBundleLocked shares only the actual pager span operation. Directory and
// record remain distinct images with distinct hashes, codecs and incoming refs.
// All child validation and admission precede either physical write. The record
// owns the directory even if its caller subsequently drops the private handle.
func (a *Arena) writeBundleLocked(directory Ref, image []byte, record *bundleRecord, w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if record == nil {
		if Local(directory.PageID) >= pager.PrimaryReadRootLocalBaseV6 {
			if !reserve(w, 1, 2*page.PageSize) {
				return nil, false, nil
			}
			charge := retainedalloc.AllocationCharge(uint64(len(image)))
			if err := a.metadata.Add(charge); err != nil {
				return nil, false, err
			}
			owned := make([]byte, len(image))
			copy(owned, image)
			ok, e := a.pager.RegisterPrimaryReadRootV6(Local(directory.PageID), owned, w)
			if !ok || e != nil {
				a.metadata.Remove(charge)
				return nil, ok, e
			}
			a.valid(directory).metadataCharge += charge
			return owned, true, nil
		}
		return a.pager.WriteViewWithWork(Local(directory.PageID), image, w)
	}
	bytesCost := uint64(20*page.PageSize) + 2*uint64(unsafe.Sizeof(bank{}))
	if !reserve(w, 1, bytesCost) {
		return nil, false, nil
	}
	db, rb := a.valid(directory), a.valid(record.ref)
	if db == nil || rb == nil || db.class != Directory || rb.class != Record || db.sealed || rb.sealed || db.pair != Local(directory.PageID) || rb.pair != db.pair || record.ref.PageID != directory.PageID+1 || Local(directory.PageID)%2 != 0 || record.encode == nil || a.metadataDecoder == nil {
		return nil, false, ErrStale
	}
	digest := sha256.Sum256(image)
	recordImage, e := record.encode(digest)
	if e != nil {
		return nil, false, e
	}
	if len(recordImage) != page.PageSize || !page.VerifyChecksumNonMutating(recordImage) || page.DecodeHeader(recordImage).PageID != record.ref.PageID {
		return nil, false, ErrFormat
	}
	edges, scratch, e := a.decodeMetadata(Record, recordImage)
	if e != nil {
		return nil, false, e
	}
	if scratch != nil {
		defer scratch.Close()
	}
	if len(edges) == 0 || len(edges) > page.PageSize/8 || edges[0].PageID != directory.PageID || edges[0].Class != Directory || edges[0].Digest != digest {
		return nil, false, ErrFormat
	}
	edgeBytes := uint64(len(edges)) * (2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(Ref{})))
	if !reserve(w, uint64(len(edges)), edgeBytes) {
		return nil, false, nil
	}
	charge := a.edgeCharge(len(edges))
	if err := a.metadata.Add(charge); err != nil {
		return nil, false, err
	}
	adopted := false
	defer func() {
		if !adopted {
			a.metadata.Remove(charge)
		}
	}()
	refs := make([]Ref, len(edges))
	for i, edge := range edges {
		child := a.slot(edge.PageID)
		if !validMetadataEdge(Record, edge.Class) || child == nil || child.refs == 0 || child.class != edge.Class || child.refs == ^uint64(0) {
			return nil, false, ErrStale
		}
		if i != 0 && (!child.sealed || (edge.Digest != ([32]byte{}) && child.digest != edge.Digest)) {
			return nil, false, ErrStale
		}
		refs[i] = Ref{edge.PageID, child.incarnation}
	}
	var physical, recordPhysical []byte
	var ok bool
	if record.durable {
		physical, recordPhysical, ok, e = a.pager.WritePublicationBundleViewsWithWork(Local(directory.PageID), image, recordImage, w)
	} else {
		physical, recordPhysical, ok, e = a.pager.WriteAdjacentViewsWithWork(Local(directory.PageID), image, recordImage, w)
	}
	if !ok || e != nil {
		return nil, ok, e
	}
	for _, r := range refs {
		a.valid(r).refs++
	}
	rb.image = recordPhysical
	rb.digest = sha256.Sum256(recordPhysical)
	rb.edges = refs
	rb.metadataCharge += charge
	adopted = true
	rb.sealed = true
	rb.privatePublication = false
	db.privatePublication = false
	a.counters.EdgeReads += uint64(len(refs))
	a.counters.EdgeWrites += uint64(len(refs))
	a.counters.PagesWritten++
	a.counters.Bytes += bytesCost + edgeBytes
	return physical, true, nil
}
