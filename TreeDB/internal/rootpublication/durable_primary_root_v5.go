package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

const durablePrimaryRootHeaderV5 = 640

// PrimaryProjectionV5 binds actual DATA ownership separately from exact
// independently complete primary banks. DataCommitSeq identifies the immutable
// Freelist generation, not the newer root publication. Its root/extent/digests
// must be checked against the actual base and system operands during recovery.
type PrimaryProjectionV5 struct {
	ArenaUUID                                   [16]byte
	ArenaHighWater                              uint64
	DirectoryDigest                             [32]byte
	BaseRootPageID, BaseSequence, DataCommitSeq uint64
	BaseDigest, SystemDigest                    [32]byte
}
type DurablePrimaryRootRecordV5 struct {
	Record  DurableRootRecordV1
	Primary PrimaryProjectionV5
}

func primaryPageInExtent(id, extent uint64) bool {
	return primaryarena.IsPage(id) && primaryarena.Local(id) >= 2 && primaryarena.Local(id) < extent
}
func (v DurablePrimaryRootRecordV5) validate(pageID uint64) error {
	r, p := v.Record, v.Primary
	if !primaryPageInExtent(pageID, p.ArenaHighWater) || p.ArenaUUID == ([16]byte{}) || p.ArenaHighWater < 3 || p.DirectoryDigest == ([32]byte{}) || p.BaseDigest == ([32]byte{}) || p.SystemDigest == ([32]byte{}) || p.BaseSequence == 0 || p.DataCommitSeq == 0 || p.DataCommitSeq == ^uint64(0) || p.DataCommitSeq > r.CommitSeq || r.CommitSeq == 0 || r.DurableSeq == 0 || r.DurableSeq > r.CommitSeq || !primaryPageInExtent(r.UserRootPageID, p.ArenaHighWater) || p.BaseRootPageID < 2 || p.BaseRootPageID >= r.TotalPages || r.SystemRootPageID < 2 || r.SystemRootPageID >= r.TotalPages || r.Freelist.HeaderPageID < 2 || r.Freelist.HeaderPageID >= r.TotalPages || r.Freelist.GenerationID == 0 || r.Freelist.CommitSeq != p.DataCommitSeq || r.Freelist.HighWater <= r.Freelist.HeaderPageID || r.Freelist.HighWater != r.TotalPages || r.Freelist.Digest == ([32]byte{}) || r.MetaProjectionDigest == ([32]byte{}) {
		return ErrDurableRootRecordFormat
	}
	if r.Directory != (DependencyDirectoryRefV2{}) {
		if r.Manifest != (DependencyManifestRefV1{}) || !primaryPageInExtent(r.Directory.RootPageID, p.ArenaHighWater) || r.Directory.PhysicalCount > ^uint64(0)-r.Directory.LogicalCount {
			return ErrDurableRootRecordFormat
		}
	} else {
		m := r.Manifest
		if !primaryPageInExtent(m.FirstPageID, p.ArenaHighWater) || m.PageCount == 0 || m.ByteLength < 16 || m.Digest == ([32]byte{}) || uint64(m.PageCount) > p.ArenaHighWater-primaryarena.Local(m.FirstPageID) {
			return ErrDurableRootRecordFormat
		}
	}
	if r.ParentRecordPageID == 0 {
		if r.ParentCommitSeq != 0 || r.ParentRecordDigest != ([32]byte{}) {
			return ErrDurableRootRecordFormat
		}
	} else if !primaryPageInExtent(r.ParentRecordPageID, p.ArenaHighWater) || r.ParentCommitSeq == 0 || r.ParentCommitSeq >= r.CommitSeq || r.ParentRecordDigest == ([32]byte{}) {
		return ErrDurableRootRecordFormat
	}
	return nil
}
func (v DurablePrimaryRootRecordV5) EncodePage(pageID uint64) ([]byte, [32]byte, error) {
	if e := v.validate(pageID); e != nil {
		return nil, [32]byte{}, e
	}
	image := make([]byte, page.PageSize)
	digest, e := v.EncodePageInto(image, pageID)
	return image, digest, e
}

// EncodePageInto consumes caller-admitted scratch through the same canonical
// codec. Refused shape or record validation leaves the destination unchanged.
func (v DurablePrimaryRootRecordV5) EncodePageInto(image []byte, pageID uint64) ([32]byte, error) {
	if len(image) != page.PageSize {
		return [32]byte{}, ErrDurableRootRecordFormat
	}
	if e := v.validate(pageID); e != nil {
		return [32]byte{}, e
	}
	clear(image)
	encodePrimaryRootFieldsV5(image, pageID, v)
	digest := durableRootRecordDigestV1(image)
	copy(image[312:344], digest[:])
	page.UpdateChecksum(image)
	return digest, nil
}
func DecodeDurablePrimaryRootRecordV5(image []byte, pageID uint64, expected [32]byte) (DurablePrimaryRootRecordV5, error) {
	if len(image) != page.PageSize || expected == ([32]byte{}) {
		return DurablePrimaryRootRecordV5{}, ErrDurableRootRecordFormat
	}
	if !page.VerifyChecksumNonMutating(image) {
		return DurablePrimaryRootRecordV5{}, ErrDurableRootRecordChecksum
	}
	header := page.DecodeHeader(image)
	if header.PageID != pageID || page.PageType(header.Flags) != page.PageTypeDurableRootRecord || header.Count != 0 || !bytes.Equal(image[16:24], durableRootRecordMagicV1[:]) || binary.LittleEndian.Uint16(image[24:26]) != 5 || binary.LittleEndian.Uint16(image[26:28]) != durablePrimaryRootHeaderV5 || (image[28] != 1 && image[28] != 2) || !allZeroV1(image[29:32]) || !allZeroV1(image[488:]) {
		return DurablePrimaryRootRecordV5{}, ErrDurableRootRecordFormat
	}
	digest := durableRootRecordDigestV1(image)
	if digest != expected || !bytes.Equal(image[312:344], digest[:]) {
		return DurablePrimaryRootRecordV5{}, ErrDurableRootRecordDigest
	}
	if image[28] == 2 && !allZeroV1(image[200:232]) {
		return DurablePrimaryRootRecordV5{}, ErrDurableRootRecordFormat
	}
	v := decodePrimaryRootFieldsV5(image)
	if e := v.validate(pageID); e != nil {
		return DurablePrimaryRootRecordV5{}, e
	}
	return v, nil
}

// ValidatePhysicalProjectionV5 checks the selected actual DATA generation and
// the exact complete bank projection. The V1 equality validator is unchanged;
// this version accepts separate generations only after their root/extent/hash
// claims agree with actual independently recoverable bytes.
func (v DurablePrimaryRootRecordV5) ValidatePhysicalProjectionV5(data, arena freelist.PageSource, physicalDataPages, physicalArenaPages uint64, arenaUUID [16]byte) (*freelist.FreelistGenerationV1, error) {
	r, p := v.Record, v.Primary
	if data == nil || arena == nil || p.ArenaUUID != arenaUUID || p.ArenaHighWater > physicalArenaPages || r.TotalPages > physicalDataPages {
		return nil, ErrDurableRootRecordFormat
	}
	generation, e := freelist.LoadGenerationV1(data, r.Freelist)
	if e != nil {
		return nil, e
	}
	if generation.CommitSeq() != p.DataCommitSeq || generation.HighWater() != r.TotalPages || generation.FreeCount() != r.FreelistFreeCount || generation.RetiredCount() != r.FreelistRetiredCount {
		return nil, ErrDurableRootRecordFormat
	}
	dirImage, e := arena.ReadPage(primaryarena.Local(r.UserRootPageID))
	if e != nil {
		return nil, e
	}
	if sha256.Sum256(dirImage) != p.DirectoryDigest || page.DecodeHeader(dirImage).PageID != r.UserRootPageID {
		return nil, ErrDurableRootRecordDigest
	}
	directory, e := node.DecodePrimaryDirectory(dirImage)
	if e != nil {
		return nil, e
	}
	base, sequence := directory.Base()
	if base.Ref.Kind != page.ChildRefPage || base.Ref.Page != p.BaseRootPageID || base.Digest != p.BaseDigest || sequence != p.BaseSequence {
		return nil, ErrDurableRootRecordFormat
	}
	for _, root := range []struct {
		id     uint64
		digest [32]byte
	}{{p.BaseRootPageID, p.BaseDigest}, {r.SystemRootPageID, p.SystemDigest}} {
		unused, e := generation.SnapshotPageUnusedV1(root.id, p.DataCommitSeq+1)
		if e != nil || unused {
			return nil, ErrDurableRootRecordFormat
		}
		image, e := data.ReadPage(root.id)
		if e != nil {
			return nil, e
		}
		if len(image) != page.PageSize || sha256.Sum256(image) != root.digest || !page.VerifyChecksumNonMutating(image) || page.DecodeHeader(image).PageID != root.id {
			return nil, ErrDurableRootRecordDigest
		}
		n := node.NewNode(image)
		if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
			return nil, ErrDurableRootRecordFormat
		}
	}
	for i := 0; i < directory.Count(); i++ {
		entry, _ := directory.Entry(i)
		if entry.InlineAbsence() {
			continue
		}
		if !primaryPageInExtent(entry.Operand.Ref.Page, p.ArenaHighWater) {
			return nil, ErrDurableRootRecordFormat
		}
		image, e := arena.ReadPage(primaryarena.Local(entry.Operand.Ref.Page))
		if e != nil {
			return nil, e
		}
		if e = node.ValidatePrimaryComponent(entry, image); e != nil {
			return nil, e
		}
	}
	return generation, nil
}

// decodePrimaryRootFieldsV5 borrows already framed bytes. Callers validate their own version.
func decodePrimaryRootFieldsV5(image []byte) DurablePrimaryRootRecordV5 {
	record := DurableRootRecordV1{
		CommitSeq: binary.LittleEndian.Uint64(image[32:40]), DurableSeq: binary.LittleEndian.Uint64(image[40:48]),
		UserRootPageID: binary.LittleEndian.Uint64(image[48:56]), SystemRootPageID: binary.LittleEndian.Uint64(image[56:64]),
		TotalPages: binary.LittleEndian.Uint64(image[64:72]), MaxEntryRevision: binary.LittleEndian.Uint64(image[72:80]),
		AppliedCommandLSN: binary.LittleEndian.Uint64(image[80:88]), LastCommitHeight: binary.LittleEndian.Uint64(image[88:96]),
		Freelist: freelist.GenerationRefV1{
			HeaderPageID: binary.LittleEndian.Uint64(image[96:104]), GenerationID: binary.LittleEndian.Uint64(image[104:112]),
			CommitSeq: binary.LittleEndian.Uint64(image[112:120]), HighWater: binary.LittleEndian.Uint64(image[120:128]),
		},
		FreelistFreeCount: binary.LittleEndian.Uint64(image[160:168]), FreelistRetiredCount: binary.LittleEndian.Uint64(image[168:176]),
		Manifest: DependencyManifestRefV1{
			FirstPageID: binary.LittleEndian.Uint64(image[176:184]), ByteLength: binary.LittleEndian.Uint64(image[184:192]),
			EntryCount: binary.LittleEndian.Uint32(image[192:196]), PageCount: binary.LittleEndian.Uint32(image[196:200]),
		},
		ParentRecordPageID: binary.LittleEndian.Uint64(image[232:240]), ParentCommitSeq: binary.LittleEndian.Uint64(image[240:248]),
	}
	copy(record.Freelist.Digest[:], image[128:160])
	copy(record.Manifest.Digest[:], image[200:232])
	if image[28] == 2 {
		record.Manifest = DependencyManifestRefV1{}
		record.Directory = DependencyDirectoryRefV2{
			RootPageID:    binary.LittleEndian.Uint64(image[176:184]),
			PhysicalCount: binary.LittleEndian.Uint64(image[184:192]),
			LogicalCount:  binary.LittleEndian.Uint64(image[192:200]),
		}
	}
	copy(record.ParentRecordDigest[:], image[248:280])
	copy(record.MetaProjectionDigest[:], image[280:312])

	p := PrimaryProjectionV5{ArenaHighWater: binary.LittleEndian.Uint64(image[360:368]), BaseRootPageID: binary.LittleEndian.Uint64(image[400:408]), BaseSequence: binary.LittleEndian.Uint64(image[408:416]), DataCommitSeq: binary.LittleEndian.Uint64(image[416:424])}
	copy(p.ArenaUUID[:], image[344:360])
	copy(p.DirectoryDigest[:], image[368:400])
	copy(p.BaseDigest[:], image[424:456])
	copy(p.SystemDigest[:], image[456:488])
	return DurablePrimaryRootRecordV5{Record: record, Primary: p}
}

func encodePrimaryRootFieldsV5(image []byte, pageID uint64, v DurablePrimaryRootRecordV5) {
	encodeDurableRootRecordFieldsV1(image, pageID, v.Record)
	binary.LittleEndian.PutUint16(image[24:26], 5)
	binary.LittleEndian.PutUint16(image[26:28], durablePrimaryRootHeaderV5)
	image[28] = 1
	if v.Record.Directory != (DependencyDirectoryRefV2{}) {
		image[28] = 2
	}
	p := v.Primary
	copy(image[344:360], p.ArenaUUID[:])
	binary.LittleEndian.PutUint64(image[360:368], p.ArenaHighWater)
	copy(image[368:400], p.DirectoryDigest[:])
	binary.LittleEndian.PutUint64(image[400:408], p.BaseRootPageID)
	binary.LittleEndian.PutUint64(image[408:416], p.BaseSequence)
	binary.LittleEndian.PutUint64(image[416:424], p.DataCommitSeq)
	copy(image[424:456], p.BaseDigest[:])
	copy(image[456:488], p.SystemDigest[:])
}
