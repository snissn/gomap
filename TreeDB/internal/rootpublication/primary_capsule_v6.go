package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	PrimaryCapsulePagesV6            = 3
	PrimaryCapsuleSizeV6             = PrimaryCapsulePagesV6 * page.PageSize
	PrimaryCapsuleFirstBankV6 uint64 = 8 // [2,5),[5,8) are never allocator banks.
	capsuleHeaderV6                  = 128
	capsuleMetadataV6                = 512
)

var ErrPrimaryCapsuleV6 = errors.New("primary capsule V6: invalid complete authority")

func PrimaryCapsulePageV6(slot uint64) uint64 {
	return primaryarena.Namespace + 2 + slot*PrimaryCapsulePagesV6
}

// PrimaryCapsuleViewV6 borrows an exact complete image. Published readers and
// deferred cuts must transfer CopyOwned(), never a mutable mapped slot view.
// Embedded parent metadata is finite frontier proof, never lookup ancestry.
type PrimaryCapsuleViewV6 struct {
	image           []byte
	slot            uint64
	current, parent DurablePrimaryRootRecordV5
}

func (v PrimaryCapsuleViewV6) Slot() uint64                        { return v.slot }
func (v PrimaryCapsuleViewV6) Image() []byte                       { return v.image }
func (v PrimaryCapsuleViewV6) Current() DurablePrimaryRootRecordV5 { return v.current }
func (v PrimaryCapsuleViewV6) Parent() DurablePrimaryRootRecordV5  { return v.parent }
func (v PrimaryCapsuleViewV6) Directory() []byte                   { return v.image[page.PageSize : 2*page.PageSize] }
func (v PrimaryCapsuleViewV6) ParentDirectory() []byte {
	if v.parent.Record.CommitSeq == 0 {
		return nil
	}
	return v.image[2*page.PageSize:]
}
func (v PrimaryCapsuleViewV6) CopyOwned() PrimaryCapsuleViewV6 {
	v.image = append([]byte(nil), v.image...)
	return v
}

// capsuleDigestV6 hashes all three pages with only the outer CRC/digest fields
// normalized. It neither allocates a second complete image nor mutates a view.
func capsuleDigestV6(image []byte) [32]byte {
	h := sha256.New()
	h.Write(image[:8])
	h.Write(make([]byte, 4))
	h.Write(image[12:48])
	h.Write(make([]byte, 32))
	h.Write(image[80:])
	var sum [32]byte
	h.Sum(sum[:0])
	return sum
}

// EncodePrimaryCapsuleV6 requires exclusive scratch. All independently complete
// metadata and directory operands are checked before dst is changed. Parent
// ancestry fields are intentionally not serialized: the single embedded parent
// owns its own complete data view, and there is no grandparent address.
func EncodePrimaryCapsuleV6(dst []byte, slot uint64, current DurablePrimaryRootRecordV5, directory []byte, parent *DurablePrimaryRootRecordV5, parentDirectory []byte) error {
	if len(dst) != PrimaryCapsuleSizeV6 || slot > 1 {
		return ErrPrimaryCapsuleV6
	}
	if e := validateCapsuleOperandV6(current, directory); e != nil {
		return e
	}
	if parent == nil {
		if current.Record.DurableSeq != 1 || current.Record.AppliedCommandLSN != 0 || len(parentDirectory) != 0 {
			return ErrPrimaryCapsuleV6
		}
	} else {
		if e := validateCapsuleOperandV6(*parent, parentDirectory); e != nil {
			return e
		}
		if parent.Primary.ArenaUUID != current.Primary.ArenaUUID || parent.Record.CommitSeq >= current.Record.CommitSeq || parent.Record.DurableSeq == ^uint64(0) || parent.Record.DurableSeq+1 != current.Record.DurableSeq || parent.Record.AppliedCommandLSN > current.Record.AppliedCommandLSN {
			return ErrPrimaryCapsuleV6
		}
	}
	_, e := writePrimaryCapsuleV6(dst, slot, current, directory, parent, parentDirectory)
	return e
}

// writePrimaryCapsuleV6 is the one physical codec core. Both the raw checked
// encoder and the constructor-certified ordinary/native path use it. Returned
// metadata is exactly what this core wrote; it is not immediately re-decoded.
func writePrimaryCapsuleV6(dst []byte, slot uint64, current DurablePrimaryRootRecordV5, directory []byte, parent *DurablePrimaryRootRecordV5, parentDirectory []byte) (PrimaryCapsuleViewV6, error) {
	clear(dst)
	id := PrimaryCapsulePageV6(slot)
	header := page.PageHeader{PageID: id, Flags: uint16(page.PageTypePrimaryCapsule)}
	header.Encode(dst)
	copy(dst[16:24], []byte("TDPRCAP6"))
	binary.LittleEndian.PutUint16(dst[24:26], 6)
	binary.LittleEndian.PutUint16(dst[26:28], capsuleHeaderV6)
	dst[28] = byte(slot)
	copy(dst[32:48], current.Primary.ArenaUUID[:])
	binary.LittleEndian.PutUint64(dst[80:88], current.Record.CommitSeq)
	binary.LittleEndian.PutUint64(dst[88:96], current.Record.DurableSeq)
	current = encodeCapsuleOperandV6(dst[128:640], dst[page.PageSize:2*page.PageSize], id, id+1, current, directory)
	view := PrimaryCapsuleViewV6{image: dst, slot: slot, current: current}
	if parent != nil {
		dst[29] = 1
		view.parent = encodeCapsuleOperandV6(dst[640:1152], dst[2*page.PageSize:], id, id+2, *parent, parentDirectory)
	}
	sum := capsuleDigestV6(dst)
	copy(dst[48:80], sum[:])
	page.UpdateChecksum(dst[:page.PageSize])
	return view, nil
}

func validateCapsuleOperandV6(v DurablePrimaryRootRecordV5, image []byte) error {
	d, e := node.DecodePrimaryDirectory(image)
	if e != nil {
		return e
	}
	r := v.Record
	if len(image) != page.PageSize || page.DecodeHeader(image).PageID != r.UserRootPageID || sha256.Sum256(image) != v.Primary.DirectoryDigest || v.Primary.ArenaHighWater < PrimaryCapsuleFirstBankV6 {
		return ErrPrimaryCapsuleV6
	}
	base, seq := d.Base()
	if base.Ref.Page != v.Primary.BaseRootPageID || base.Digest != v.Primary.BaseDigest || seq != v.Primary.BaseSequence {
		return ErrPrimaryCapsuleV6
	}
	// The input is an owned immutable read image, which may use a RAM-only read
	// address. Encoding binds its copy to the fixed capsule's physical extent;
	// no input read address becomes a recovery or allocator operand.
	v.Record.UserRootPageID = PrimaryCapsulePageV6(0) + 1
	// Capsule contains the parent by value, so no external parent record is an authority.
	v.Record.ParentRecordPageID = 0
	v.Record.ParentCommitSeq = 0
	v.Record.ParentRecordDigest = [32]byte{}
	if e := v.validate(PrimaryCapsulePageV6(0)); e != nil {
		return e
	}
	return nil
}
func encodeCapsuleOperandV6(meta, dir []byte, id, dirID uint64, v DurablePrimaryRootRecordV5, source []byte) DurablePrimaryRootRecordV5 {
	copy(dir, source)
	binary.LittleEndian.PutUint64(dir[:8], dirID)
	page.UpdateChecksum(dir)
	v.Record.UserRootPageID = dirID
	v.Record.ParentRecordPageID = 0
	v.Record.ParentCommitSeq = 0
	v.Record.ParentRecordDigest = [32]byte{}
	v.Primary.DirectoryDigest = sha256.Sum256(dir)
	encodePrimaryRootFieldsV5(meta, id, v)
	binary.LittleEndian.PutUint16(meta[24:26], 6)
	binary.LittleEndian.PutUint16(meta[26:28], capsuleMetadataV6)
	// Outer complete-capsule hash authenticates all metadata; no second record digest/CRC.
	return v
}
func decodeCapsuleOperandV6(meta, dir []byte, id, dirID uint64) (DurablePrimaryRootRecordV5, error) {
	if page.DecodeHeader(meta).PageID != id || page.DecodeHeader(meta).Flags != uint16(page.PageTypeDurableRootRecord) || page.DecodeHeader(meta).Count != 0 || !allZeroV1(meta[8:12]) || !bytes.Equal(meta[16:24], durableRootRecordMagicV1[:]) || binary.LittleEndian.Uint16(meta[24:26]) != 6 || binary.LittleEndian.Uint16(meta[26:28]) != capsuleMetadataV6 || (meta[28] != 1 && meta[28] != 2) || !allZeroV1(meta[29:32]) || !allZeroV1(meta[232:280]) || !allZeroV1(meta[312:344]) || !allZeroV1(meta[488:]) {
		return DurablePrimaryRootRecordV5{}, ErrPrimaryCapsuleV6
	}
	if meta[28] == 2 && !allZeroV1(meta[200:232]) {
		return DurablePrimaryRootRecordV5{}, ErrPrimaryCapsuleV6
	}
	v := decodePrimaryRootFieldsV5(meta)
	if v.Record.UserRootPageID != dirID || validateCapsuleOperandV6(v, dir) != nil {
		return DurablePrimaryRootRecordV5{}, ErrPrimaryCapsuleV6
	}
	return v, nil
}
func DecodePrimaryCapsuleV6(image []byte, slot uint64, uuid [16]byte) (PrimaryCapsuleViewV6, error) {
	bad := func() (PrimaryCapsuleViewV6, error) { return PrimaryCapsuleViewV6{}, ErrPrimaryCapsuleV6 }
	if len(image) != PrimaryCapsuleSizeV6 || slot > 1 || uuid == ([16]byte{}) || !page.VerifyChecksumNonMutating(image[:page.PageSize]) {
		return bad()
	}
	id := PrimaryCapsulePageV6(slot)
	h := page.DecodeHeader(image)
	if h.PageID != id || h.Flags != uint16(page.PageTypePrimaryCapsule) || h.Count != 0 || !bytes.Equal(image[16:24], []byte("TDPRCAP6")) || binary.LittleEndian.Uint16(image[24:26]) != 6 || binary.LittleEndian.Uint16(image[26:28]) != capsuleHeaderV6 || image[28] != byte(slot) || image[29] > 1 || !allZeroV1(image[30:32]) || !bytes.Equal(image[32:48], uuid[:]) || !allZeroV1(image[96:128]) || !allZeroV1(image[1152:page.PageSize]) {
		return bad()
	}
	sum := capsuleDigestV6(image)
	if !bytes.Equal(sum[:], image[48:80]) {
		return bad()
	}
	current, e := decodeCapsuleOperandV6(image[128:640], image[page.PageSize:2*page.PageSize], id, id+1)
	if e != nil {
		return bad()
	}
	if current.Primary.ArenaUUID != uuid || current.Record.CommitSeq != binary.LittleEndian.Uint64(image[80:88]) || current.Record.DurableSeq != binary.LittleEndian.Uint64(image[88:96]) {
		return bad()
	}
	v := PrimaryCapsuleViewV6{image: image, slot: slot, current: current}
	if image[29] == 0 {
		if current.Record.DurableSeq != 1 || current.Record.AppliedCommandLSN != 0 || !allZeroV1(image[640:1152]) || !allZeroV1(image[2*page.PageSize:]) {
			return bad()
		}
	} else {
		v.parent, e = decodeCapsuleOperandV6(image[640:1152], image[2*page.PageSize:], id, id+2)
		if e != nil || v.parent.Primary.ArenaUUID != uuid || v.parent.Record.CommitSeq >= current.Record.CommitSeq || v.parent.Record.DurableSeq == ^uint64(0) || v.parent.Record.DurableSeq+1 != current.Record.DurableSeq || v.parent.Record.AppliedCommandLSN > current.Record.AppliedCommandLSN {
			return bad()
		}
	}
	return v, nil
}

// SelectPrimaryCapsulesV6 examines exactly the two physically eligible fixed
// slots. validate must close EACH current and embedded-parent physical resource
// projection independently. A failed newer closure may select only the other
// actual slot; it can never promote an embedded parent into selector authority.
func SelectPrimaryCapsulesV6(images [2][]byte, uuid [16]byte, validate func(PrimaryCapsuleViewV6) error) (PrimaryCapsuleViewV6, error) {
	var selected PrimaryCapsuleViewV6
	if validate == nil {
		return selected, ErrPrimaryCapsuleV6
	}
	for slot, image := range images {
		v, e := DecodePrimaryCapsuleV6(image, uint64(slot), uuid)
		if e != nil {
			continue
		}
		if validate(v) != nil {
			continue
		}
		if selected.image != nil && selected.current.Record.CommitSeq == v.current.Record.CommitSeq {
			return PrimaryCapsuleViewV6{}, ErrPrimaryCapsuleV6
		}
		if selected.image == nil || selected.current.Record.CommitSeq < v.current.Record.CommitSeq {
			selected = v
		}
	}
	if selected.image == nil {
		return selected, ErrPrimaryCapsuleV6
	}
	return selected, nil
}

// ValidatePhysicalProjectionV6 checks the independently complete DATA and bank
// closures of BOTH embedded operands, using the captured directory bytes. It
// does not reread a mutable fixed slot, consult DATA META, or use parent lookup.
// Dependency/vlog validators remain additional required caller authorities.
func (v PrimaryCapsuleViewV6) ValidatePhysicalProjectionV6(data, banks freelist.PageSource, dataPages, bankPages uint64) (*freelist.FreelistGenerationV1, error) {
	var selected *freelist.FreelistGenerationV1
	for i, operand := range []DurablePrimaryRootRecordV5{v.current, v.parent} {
		if operand.Record.CommitSeq == 0 {
			continue
		}
		image := v.Directory()
		if i == 1 {
			image = v.ParentDirectory()
		}
		d, e := node.DecodePrimaryDirectory(image)
		if e != nil {
			return nil, e
		}
		for j := 0; j < d.Count(); j++ {
			entry, _ := d.Entry(j)
			if entry.InlineAbsence() {
				continue
			}
			if !primaryarena.IsPage(entry.Operand.Ref.Page) || primaryarena.Local(entry.Operand.Ref.Page) < PrimaryCapsuleFirstBankV6 {
				return nil, ErrPrimaryCapsuleV6
			}
		}
		if operand.Record.Directory.RootPageID != 0 && primaryarena.Local(operand.Record.Directory.RootPageID) < PrimaryCapsuleFirstBankV6 {
			return nil, ErrPrimaryCapsuleV6
		}
		if operand.Record.Manifest.FirstPageID != 0 && primaryarena.Local(operand.Record.Manifest.FirstPageID) < PrimaryCapsuleFirstBankV6 {
			return nil, ErrPrimaryCapsuleV6
		}
		source := capsuleDirectorySourceV6{banks: banks, id: primaryarena.Local(operand.Record.UserRootPageID), image: image}
		generation, e := operand.ValidatePhysicalProjectionV5(data, source, dataPages, bankPages, operand.Primary.ArenaUUID)
		if e != nil {
			return nil, e
		}
		if i == 0 {
			selected = generation
		}
	}
	return selected, nil
}

type capsuleDirectorySourceV6 struct {
	banks freelist.PageSource
	id    uint64
	image []byte
}

func (s capsuleDirectorySourceV6) ReadPage(id uint64) ([]byte, error) {
	if id == s.id {
		return s.image, nil
	}
	if s.banks == nil {
		return nil, ErrPrimaryCapsuleV6
	}
	return s.banks.ReadPage(id)
}

// ConstructPrimaryCapsuleV6 consumes constructor-owned root authority in the
// same guarded publication region as fresh record/parent admission. It does
// not grant durability or fixed-slot eligibility. The caller exclusively owns
// dst and retains prepared's directory/DATA and parent's closure throughout.
func ConstructPrimaryCapsuleV6(dst []byte, slot uint64, current DurablePrimaryRootRecordV5, prepared *PreparedPrimaryRootV5, parent *PrimaryCapsuleViewV6, w *iterator.OrdinalScanWork) (PrimaryCapsuleViewV6, bool, error) {
	bad := func() (PrimaryCapsuleViewV6, bool, error) { return PrimaryCapsuleViewV6{}, false, ErrPrimaryCapsuleV6 }
	if w != nil && !w.Reserve(1, 2*capsuleMetadataV6+256) {
		return PrimaryCapsuleViewV6{}, false, nil
	}
	if len(dst) != PrimaryCapsuleSizeV6 || slot > 1 || prepared == nil || prepared.readCertificate == nil || prepared.validateSequence(current.Record.CommitSeq) != nil {
		return bad()
	}
	cert := prepared.readCertificate
	p := prepared.projection
	if current.Record.UserRootPageID != cert.Reference().PageID || current.Record.SystemRootPageID != prepared.frontier.systemRootPageID || current.Record.TotalPages != prepared.generation.HighWater() ||
		current.Record.Freelist != prepared.generation.GenerationRef() || current.Record.FreelistFreeCount != prepared.generation.FreeCount() || current.Record.FreelistRetiredCount != prepared.generation.RetiredCount() ||
		current.Record.AppliedCommandLSN != prepared.frontier.appliedCommandLSN || current.Record.MaxEntryRevision != prepared.frontier.maxEntryRevision ||
		current.Primary.ArenaUUID != p.ArenaUUID || current.Primary.DirectoryDigest != p.DirectoryDigest || current.Primary.BaseRootPageID != p.BaseRootPageID || current.Primary.BaseSequence != p.BaseSequence || current.Primary.DataCommitSeq != p.DataCommitSeq || current.Primary.BaseDigest != p.BaseDigest || current.Primary.SystemDigest != p.SystemDigest {
		return bad()
	}
	normalized := current
	normalized.Record.UserRootPageID = PrimaryCapsulePageV6(0) + 1
	normalized.Record.ParentRecordPageID = 0
	normalized.Record.ParentCommitSeq = 0
	normalized.Record.ParentRecordDigest = [32]byte{}
	if normalized.validate(PrimaryCapsulePageV6(0)) != nil {
		return bad()
	}
	var previous *DurablePrimaryRootRecordV5
	var previousImage []byte
	if parent == nil {
		if current.Record.DurableSeq != 1 || current.Record.AppliedCommandLSN != 0 {
			return bad()
		}
	} else {
		// Fresh parent integrity and finite frontier guard; canonical decode was
		// performed at constructor/recovery, not again on every publication.
		if w != nil && !w.Reserve(1, PrimaryCapsuleSizeV6+page.PageSize+2*capsuleMetadataV6) {
			return PrimaryCapsuleViewV6{}, false, nil
		}
		if len(parent.image) != PrimaryCapsuleSizeV6 || parent.slot > 1 || !page.VerifyChecksumNonMutating(parent.image[:page.PageSize]) ||
			capsuleDigestV6(parent.image) != [32]byte(parent.image[48:80]) {
			return bad()
		}
		prior := parent.current
		if prior.Primary.ArenaUUID != current.Primary.ArenaUUID || prior.Record.CommitSeq >= current.Record.CommitSeq || prior.Record.DurableSeq == ^uint64(0) || prior.Record.DurableSeq+1 != current.Record.DurableSeq || prior.Record.AppliedCommandLSN > current.Record.AppliedCommandLSN {
			return bad()
		}
		previous = &prior
		previousImage = parent.Directory()
	}
	// Each distinct copied directory is one physical operand, with actual read,
	// write, CRC and digest bytes. Complete capsule framing/digest is another.
	if w != nil && !w.Reserve(1, 4*page.PageSize) {
		return PrimaryCapsuleViewV6{}, false, nil
	}
	if previous != nil && w != nil && !w.Reserve(1, 4*page.PageSize) {
		return PrimaryCapsuleViewV6{}, false, nil
	}
	if w != nil && !w.Reserve(1, 2*PrimaryCapsuleSizeV6+page.PageSize+2*capsuleMetadataV6) {
		return PrimaryCapsuleViewV6{}, false, nil
	}
	view, err := writePrimaryCapsuleV6(dst, slot, current, cert.Image(), previous, previousImage)
	return view, err == nil, err
}
