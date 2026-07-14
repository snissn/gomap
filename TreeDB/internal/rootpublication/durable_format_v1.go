package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	dependencyManifestPageHeaderV1 = 96
	dependencyManifestPayloadV1    = page.PageSize - dependencyManifestPageHeaderV1
	durableRootRecordHeaderV1      = 384
	maxDependencyManifestBytesV1   = 64 << 20
)

var (
	dependencyManifestPageMagicV1 = [8]byte{'D', 'P', 'M', 'P', 'G', 'V', '1', 0}
	dependencyManifestBodyMagicV1 = [8]byte{'D', 'P', 'M', 'A', 'N', 'V', '1', 0}
	durableRootRecordMagicV1      = [8]byte{'D', 'R', 'O', 'O', 'T', 'V', '1', 0}

	ErrDependencyManifestFormat  = errors.New("dependency manifest V1 format invalid")
	ErrDurableRootRecordFormat   = errors.New("durable root record V1 format invalid")
	ErrDurableRootRecordChecksum = errors.New("durable root record V1 checksum mismatch")
	ErrDurableRootRecordDigest   = errors.New("durable root record V1 digest mismatch")
)

type DependencyManifestEntryV1 struct {
	Kind           ResourceKind
	LogicalLane    string
	ResourceID     string
	DiagnosticPath string
	Identity       StableIdentity
	Generation     uint64
	Frontier       DurableFrontier
	Reachability   []ReachabilityField
}

type DependencyManifestRefV1 struct {
	FirstPageID uint64
	ByteLength  uint64
	EntryCount  uint32
	PageCount   uint32
	Digest      [32]byte
}

type DependencyManifestV1 struct {
	entries []DependencyManifestEntryV1
	payload []byte
	digest  [32]byte
}

func NewDependencyManifestV1(entries []DependencyManifestEntryV1) (*DependencyManifestV1, error) {
	normalized := make([]DependencyManifestEntryV1, len(entries))
	encoded := make([][]byte, len(entries))
	for i := range entries {
		entry, err := normalizeDependencyManifestEntryV1(entries[i])
		if err != nil {
			return nil, err
		}
		normalized[i] = entry
		encoded[i] = encodeDependencyManifestEntryV1(entry)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return bytes.Compare(encodeDependencyManifestEntryV1(normalized[i]), encodeDependencyManifestEntryV1(normalized[j])) < 0
	})
	for i := range normalized {
		encoded[i] = encodeDependencyManifestEntryV1(normalized[i])
		if i > 0 && bytes.Equal(encoded[i-1], encoded[i]) {
			return nil, fmt.Errorf("%w: duplicate resource entry", ErrDependencyManifestFormat)
		}
	}
	payload := make([]byte, 16)
	copy(payload[0:8], dependencyManifestBodyMagicV1[:])
	binary.LittleEndian.PutUint16(payload[8:10], 1)
	binary.LittleEndian.PutUint16(payload[10:12], 16)
	binary.LittleEndian.PutUint32(payload[12:16], uint32(len(normalized)))
	for _, entry := range encoded {
		if len(entry) > int(^uint32(0)) {
			return nil, ErrDependencyManifestFormat
		}
		payload = appendU32V1(payload, uint32(len(entry)))
		payload = append(payload, entry...)
		if len(payload) > maxDependencyManifestBytesV1 {
			return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrDependencyManifestFormat, maxDependencyManifestBytesV1)
		}
	}
	return &DependencyManifestV1{entries: normalized, payload: payload, digest: sha256.Sum256(payload)}, nil
}

func normalizeDependencyManifestEntryV1(entry DependencyManifestEntryV1) (DependencyManifestEntryV1, error) {
	if entry.Kind == "" || entry.ResourceID == "" || entry.Generation == 0 || !entry.Identity.valid() || entry.Identity.Generation != entry.Generation {
		return DependencyManifestEntryV1{}, fmt.Errorf("%w: incomplete resource entry", ErrDependencyManifestFormat)
	}
	if err := validateDiagnosticPath(entry.DiagnosticPath); err != nil {
		return DependencyManifestEntryV1{}, fmt.Errorf("%w: %v", ErrDependencyManifestFormat, err)
	}
	if err := validateDurableFrontier(entry.Frontier); err != nil {
		return DependencyManifestEntryV1{}, fmt.Errorf("%w: %v", ErrDependencyManifestFormat, err)
	}
	entry.Frontier = cloneDurableFrontier(entry.Frontier)
	entry.Reachability = append([]ReachabilityField(nil), entry.Reachability...)
	sort.Slice(entry.Reachability, func(i, j int) bool { return entry.Reachability[i] < entry.Reachability[j] })
	if len(entry.Reachability) == 0 {
		return DependencyManifestEntryV1{}, fmt.Errorf("%w: resource has no reachability", ErrDependencyManifestFormat)
	}
	for i, field := range entry.Reachability {
		if field == "" || (i > 0 && field == entry.Reachability[i-1]) {
			return DependencyManifestEntryV1{}, fmt.Errorf("%w: invalid reachability", ErrDependencyManifestFormat)
		}
	}
	return entry, nil
}

func (manifest *DependencyManifestV1) Entries() []DependencyManifestEntryV1 {
	if manifest == nil {
		return nil
	}
	out := make([]DependencyManifestEntryV1, len(manifest.entries))
	for i, entry := range manifest.entries {
		entry.Frontier = cloneDurableFrontier(entry.Frontier)
		entry.Reachability = append([]ReachabilityField(nil), entry.Reachability...)
		out[i] = entry
	}
	return out
}

func (manifest *DependencyManifestV1) PageCount() uint32 {
	if manifest == nil || len(manifest.payload) == 0 {
		return 0
	}
	return uint32((len(manifest.payload) + dependencyManifestPayloadV1 - 1) / dependencyManifestPayloadV1)
}

func (manifest *DependencyManifestV1) Materialize(firstPageID uint64, sink freelist.AppendPageSink) (DependencyManifestRefV1, error) {
	if manifest == nil || sink == nil || firstPageID < 2 || manifest.PageCount() == 0 {
		return DependencyManifestRefV1{}, ErrDependencyManifestFormat
	}
	pageCount := manifest.PageCount()
	if firstPageID > ^uint64(0)-uint64(pageCount-1) {
		return DependencyManifestRefV1{}, ErrDependencyManifestFormat
	}
	ref := DependencyManifestRefV1{
		FirstPageID: firstPageID, ByteLength: uint64(len(manifest.payload)),
		EntryCount: uint32(len(manifest.entries)), PageCount: pageCount, Digest: manifest.digest,
	}
	for index := uint32(0); index < pageCount; index++ {
		pageID := firstPageID + uint64(index)
		start := int(index) * dependencyManifestPayloadV1
		end := start + dependencyManifestPayloadV1
		if end > len(manifest.payload) {
			end = len(manifest.payload)
		}
		image := make([]byte, page.PageSize)
		pageHeader := page.PageHeader{PageID: pageID, Flags: uint16(page.PageTypeDependencyManifest)}
		pageHeader.Encode(image)
		copy(image[16:24], dependencyManifestPageMagicV1[:])
		binary.LittleEndian.PutUint16(image[24:26], 1)
		binary.LittleEndian.PutUint16(image[26:28], dependencyManifestPageHeaderV1)
		binary.LittleEndian.PutUint32(image[28:32], index)
		binary.LittleEndian.PutUint32(image[32:36], pageCount)
		binary.LittleEndian.PutUint32(image[36:40], ref.EntryCount)
		binary.LittleEndian.PutUint64(image[40:48], ref.ByteLength)
		if index+1 < pageCount {
			binary.LittleEndian.PutUint64(image[48:56], pageID+1)
		}
		copy(image[56:88], ref.Digest[:])
		binary.LittleEndian.PutUint32(image[88:92], uint32(end-start))
		copy(image[dependencyManifestPageHeaderV1:], manifest.payload[start:end])
		page.UpdateChecksum(image)
		if err := sink.WritePage(pageID, image); err != nil {
			return DependencyManifestRefV1{}, fmt.Errorf("materialize dependency manifest page %d: %w", pageID, err)
		}
	}
	return ref, nil
}

func LoadDependencyManifestV1(source freelist.PageSource, ref DependencyManifestRefV1) (*DependencyManifestV1, error) {
	if source == nil || ref.FirstPageID < 2 || ref.ByteLength < 16 || ref.ByteLength > maxDependencyManifestBytesV1 || ref.PageCount == 0 ||
		ref.Digest == [32]byte{} || uint64(ref.PageCount) != (ref.ByteLength+dependencyManifestPayloadV1-1)/dependencyManifestPayloadV1 ||
		ref.FirstPageID > ^uint64(0)-uint64(ref.PageCount-1) {
		return nil, ErrDependencyManifestFormat
	}
	payload := make([]byte, 0, int(ref.ByteLength))
	for index := uint32(0); index < ref.PageCount; index++ {
		pageID := ref.FirstPageID + uint64(index)
		image, err := source.ReadPage(pageID)
		if err != nil {
			return nil, fmt.Errorf("%w: read page %d: %v", ErrDependencyManifestFormat, pageID, err)
		}
		if len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) {
			return nil, fmt.Errorf("%w: page %d checksum or size", ErrDependencyManifestFormat, pageID)
		}
		header := page.DecodeHeader(image)
		if header.PageID != pageID || page.PageType(header.Flags) != page.PageTypeDependencyManifest || header.Count != 0 ||
			!bytes.Equal(image[16:24], dependencyManifestPageMagicV1[:]) || binary.LittleEndian.Uint16(image[24:26]) != 1 ||
			binary.LittleEndian.Uint16(image[26:28]) != dependencyManifestPageHeaderV1 || binary.LittleEndian.Uint32(image[28:32]) != index ||
			binary.LittleEndian.Uint32(image[32:36]) != ref.PageCount || binary.LittleEndian.Uint32(image[36:40]) != ref.EntryCount ||
			binary.LittleEndian.Uint64(image[40:48]) != ref.ByteLength || !bytes.Equal(image[56:88], ref.Digest[:]) ||
			!allZeroV1(image[92:96]) {
			return nil, fmt.Errorf("%w: page %d header", ErrDependencyManifestFormat, pageID)
		}
		wantNext := uint64(0)
		if index+1 < ref.PageCount {
			wantNext = pageID + 1
		}
		chunkLength := binary.LittleEndian.Uint32(image[88:92])
		remaining := ref.ByteLength - uint64(len(payload))
		wantChunk := uint64(dependencyManifestPayloadV1)
		if remaining < wantChunk {
			wantChunk = remaining
		}
		if binary.LittleEndian.Uint64(image[48:56]) != wantNext || uint64(chunkLength) != wantChunk ||
			!allZeroV1(image[dependencyManifestPageHeaderV1+int(chunkLength):]) {
			return nil, fmt.Errorf("%w: page %d chain", ErrDependencyManifestFormat, pageID)
		}
		payload = append(payload, image[dependencyManifestPageHeaderV1:dependencyManifestPageHeaderV1+int(chunkLength)]...)
	}
	if uint64(len(payload)) != ref.ByteLength || sha256.Sum256(payload) != ref.Digest {
		return nil, ErrDependencyManifestFormat
	}
	entries, err := decodeDependencyManifestPayloadV1(payload, ref.EntryCount)
	if err != nil {
		return nil, err
	}
	manifest, err := NewDependencyManifestV1(entries)
	if err != nil || manifest.digest != ref.Digest || !bytes.Equal(manifest.payload, payload) {
		return nil, ErrDependencyManifestFormat
	}
	return manifest, nil
}

func encodeDependencyManifestEntryV1(entry DependencyManifestEntryV1) []byte {
	var out []byte
	out = appendStringV1(out, string(entry.Kind))
	out = appendStringV1(out, entry.LogicalLane)
	out = appendStringV1(out, entry.ResourceID)
	out = appendStringV1(out, entry.DiagnosticPath)
	out = appendStringV1(out, entry.Identity.Platform)
	out = appendU64V1(out, entry.Identity.VolumeID)
	out = append(out, entry.Identity.ObjectID[:]...)
	out = appendU64V1(out, entry.Identity.Generation)
	out = appendU64V1(out, entry.Generation)
	out = appendU64V1(out, entry.Frontier.Bytes)
	out = appendU64V1(out, entry.Frontier.MaxLSN)
	rids := entry.Frontier.RIDs()
	out = appendU32V1(out, uint32(len(rids)))
	for _, rid := range rids {
		out = appendU64V1(out, rid)
	}
	out = appendU32V1(out, uint32(len(entry.Reachability)))
	for _, field := range entry.Reachability {
		out = appendStringV1(out, string(field))
	}
	return out
}

func decodeDependencyManifestPayloadV1(payload []byte, expectedCount uint32) ([]DependencyManifestEntryV1, error) {
	if len(payload) < 16 || !bytes.Equal(payload[0:8], dependencyManifestBodyMagicV1[:]) ||
		binary.LittleEndian.Uint16(payload[8:10]) != 1 || binary.LittleEndian.Uint16(payload[10:12]) != 16 ||
		binary.LittleEndian.Uint32(payload[12:16]) != expectedCount {
		return nil, ErrDependencyManifestFormat
	}
	reader := manifestReaderV1{data: payload, offset: 16}
	entries := make([]DependencyManifestEntryV1, 0, expectedCount)
	for index := uint32(0); index < expectedCount; index++ {
		length, ok := reader.u32()
		if !ok || uint64(length) > uint64(reader.remaining()) {
			return nil, ErrDependencyManifestFormat
		}
		entryReader := manifestReaderV1{data: reader.data[reader.offset : reader.offset+int(length)]}
		reader.offset += int(length)
		entry, ok := entryReader.entry()
		if !ok || entryReader.remaining() != 0 {
			return nil, ErrDependencyManifestFormat
		}
		entries = append(entries, entry)
	}
	if reader.remaining() != 0 {
		return nil, ErrDependencyManifestFormat
	}
	return entries, nil
}

type manifestReaderV1 struct {
	data   []byte
	offset int
}

func (reader *manifestReaderV1) remaining() int { return len(reader.data) - reader.offset }
func (reader *manifestReaderV1) bytes(length int) ([]byte, bool) {
	if length < 0 || length > reader.remaining() {
		return nil, false
	}
	value := reader.data[reader.offset : reader.offset+length]
	reader.offset += length
	return value, true
}
func (reader *manifestReaderV1) u32() (uint32, bool) {
	value, ok := reader.bytes(4)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint32(value), true
}
func (reader *manifestReaderV1) u64() (uint64, bool) {
	value, ok := reader.bytes(8)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint64(value), true
}
func (reader *manifestReaderV1) str() (string, bool) {
	length, ok := reader.u32()
	if !ok || uint64(length) > uint64(reader.remaining()) {
		return "", false
	}
	value, _ := reader.bytes(int(length))
	return string(value), true
}
func (reader *manifestReaderV1) entry() (DependencyManifestEntryV1, bool) {
	kind, ok := reader.str()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	lane, ok := reader.str()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	resourceID, ok := reader.str()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	path, ok := reader.str()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	platform, ok := reader.str()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	volume, ok := reader.u64()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	object, ok := reader.bytes(16)
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	identityGeneration, ok := reader.u64()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	generation, ok := reader.u64()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	frontierBytes, ok := reader.u64()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	maxLSN, ok := reader.u64()
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	ridCount, ok := reader.u32()
	if !ok || uint64(ridCount) > uint64(reader.remaining()/8) {
		return DependencyManifestEntryV1{}, false
	}
	rids := make([]uint64, ridCount)
	for i := range rids {
		rids[i], ok = reader.u64()
		if !ok {
			return DependencyManifestEntryV1{}, false
		}
	}
	reachabilityCount, ok := reader.u32()
	if !ok || uint64(reachabilityCount) > uint64(reader.remaining()/4) {
		return DependencyManifestEntryV1{}, false
	}
	reachability := make([]ReachabilityField, reachabilityCount)
	for i := range reachability {
		field, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		reachability[i] = ReachabilityField(field)
	}
	identity := StableIdentity{Platform: platform, VolumeID: volume, Generation: identityGeneration}
	copy(identity.ObjectID[:], object)
	frontier := DurableFrontier{Bytes: frontierBytes, MaxLSN: maxLSN}
	if len(rids) != 0 {
		ridFrontier := NewRIDFrontier(rids)
		ridFrontier.Bytes, ridFrontier.MaxLSN = frontierBytes, maxLSN
		frontier = ridFrontier
	}
	return DependencyManifestEntryV1{
		Kind: ResourceKind(kind), LogicalLane: lane, ResourceID: resourceID, DiagnosticPath: path,
		Identity: identity, Generation: generation, Frontier: frontier, Reachability: reachability,
	}, true
}

func appendU32V1(dst []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(dst, encoded[:]...)
}
func appendU64V1(dst []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(dst, encoded[:]...)
}
func appendStringV1(dst []byte, value string) []byte {
	dst = appendU32V1(dst, uint32(len(value)))
	return append(dst, value...)
}
func allZeroV1(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

type DurableRootRecordV1 struct {
	CommitSeq         uint64
	DurableSeq        uint64
	UserRootPageID    uint64
	SystemRootPageID  uint64
	TotalPages        uint64
	MaxEntryRevision  uint64
	AppliedCommandLSN uint64
	LastCommitHeight  uint64

	Freelist             freelist.GenerationRefV1
	FreelistFreeCount    uint64
	FreelistRetiredCount uint64
	Manifest             DependencyManifestRefV1

	ParentRecordPageID   uint64
	ParentCommitSeq      uint64
	ParentRecordDigest   [32]byte
	MetaProjectionDigest [32]byte
}

func (record DurableRootRecordV1) validate(pageID uint64) error {
	if pageID < 2 || record.CommitSeq == 0 || record.DurableSeq > record.CommitSeq || record.UserRootPageID < 2 || record.SystemRootPageID < 2 ||
		record.TotalPages <= pageID || record.UserRootPageID >= record.TotalPages || record.SystemRootPageID >= record.TotalPages ||
		record.Freelist.HeaderPageID < 2 || record.Freelist.GenerationID == 0 || record.Freelist.CommitSeq != record.CommitSeq ||
		record.Freelist.HighWater <= record.Freelist.HeaderPageID || record.Freelist.HighWater > record.TotalPages || record.Freelist.Digest == [32]byte{} ||
		record.Manifest.FirstPageID < 2 || record.Manifest.PageCount == 0 || record.Manifest.ByteLength < 16 || record.Manifest.Digest == [32]byte{} ||
		record.Manifest.FirstPageID+uint64(record.Manifest.PageCount) > record.TotalPages || record.MetaProjectionDigest == [32]byte{} {
		return ErrDurableRootRecordFormat
	}
	if record.ParentRecordPageID == 0 {
		if record.ParentCommitSeq != 0 || record.ParentRecordDigest != [32]byte{} {
			return ErrDurableRootRecordFormat
		}
	} else if record.ParentRecordPageID < 2 || record.ParentRecordPageID >= record.TotalPages || record.ParentCommitSeq == 0 ||
		record.ParentCommitSeq >= record.CommitSeq || record.ParentRecordDigest == [32]byte{} {
		return ErrDurableRootRecordFormat
	}
	return nil
}

func (record DurableRootRecordV1) EncodePage(pageID uint64) ([]byte, [32]byte, error) {
	if err := record.validate(pageID); err != nil {
		return nil, [32]byte{}, err
	}
	image := make([]byte, page.PageSize)
	header := page.PageHeader{PageID: pageID, Flags: uint16(page.PageTypeDurableRootRecord)}
	header.Encode(image)
	copy(image[16:24], durableRootRecordMagicV1[:])
	binary.LittleEndian.PutUint16(image[24:26], 1)
	binary.LittleEndian.PutUint16(image[26:28], durableRootRecordHeaderV1)
	binary.LittleEndian.PutUint64(image[32:40], record.CommitSeq)
	binary.LittleEndian.PutUint64(image[40:48], record.DurableSeq)
	binary.LittleEndian.PutUint64(image[48:56], record.UserRootPageID)
	binary.LittleEndian.PutUint64(image[56:64], record.SystemRootPageID)
	binary.LittleEndian.PutUint64(image[64:72], record.TotalPages)
	binary.LittleEndian.PutUint64(image[72:80], record.MaxEntryRevision)
	binary.LittleEndian.PutUint64(image[80:88], record.AppliedCommandLSN)
	binary.LittleEndian.PutUint64(image[88:96], record.LastCommitHeight)
	binary.LittleEndian.PutUint64(image[96:104], record.Freelist.HeaderPageID)
	binary.LittleEndian.PutUint64(image[104:112], record.Freelist.GenerationID)
	binary.LittleEndian.PutUint64(image[112:120], record.Freelist.CommitSeq)
	binary.LittleEndian.PutUint64(image[120:128], record.Freelist.HighWater)
	copy(image[128:160], record.Freelist.Digest[:])
	binary.LittleEndian.PutUint64(image[160:168], record.FreelistFreeCount)
	binary.LittleEndian.PutUint64(image[168:176], record.FreelistRetiredCount)
	binary.LittleEndian.PutUint64(image[176:184], record.Manifest.FirstPageID)
	binary.LittleEndian.PutUint64(image[184:192], record.Manifest.ByteLength)
	binary.LittleEndian.PutUint32(image[192:196], record.Manifest.EntryCount)
	binary.LittleEndian.PutUint32(image[196:200], record.Manifest.PageCount)
	copy(image[200:232], record.Manifest.Digest[:])
	binary.LittleEndian.PutUint64(image[232:240], record.ParentRecordPageID)
	binary.LittleEndian.PutUint64(image[240:248], record.ParentCommitSeq)
	copy(image[248:280], record.ParentRecordDigest[:])
	copy(image[280:312], record.MetaProjectionDigest[:])
	digest := durableRootRecordDigestV1(image)
	copy(image[312:344], digest[:])
	page.UpdateChecksum(image)
	return image, digest, nil
}

func DecodeDurableRootRecordV1(image []byte, pageID uint64, expectedDigest [32]byte) (DurableRootRecordV1, error) {
	if len(image) != page.PageSize || pageID < 2 || expectedDigest == [32]byte{} {
		return DurableRootRecordV1{}, ErrDurableRootRecordFormat
	}
	if !page.VerifyChecksumNonMutating(image) {
		return DurableRootRecordV1{}, ErrDurableRootRecordChecksum
	}
	header := page.DecodeHeader(image)
	if header.PageID != pageID || page.PageType(header.Flags) != page.PageTypeDurableRootRecord || header.Count != 0 ||
		!bytes.Equal(image[16:24], durableRootRecordMagicV1[:]) || binary.LittleEndian.Uint16(image[24:26]) != 1 ||
		binary.LittleEndian.Uint16(image[26:28]) != durableRootRecordHeaderV1 || !allZeroV1(image[28:32]) ||
		!allZeroV1(image[344:durableRootRecordHeaderV1]) || !allZeroV1(image[durableRootRecordHeaderV1:]) {
		return DurableRootRecordV1{}, ErrDurableRootRecordFormat
	}
	digest := durableRootRecordDigestV1(image)
	if digest != expectedDigest || !bytes.Equal(image[312:344], digest[:]) {
		return DurableRootRecordV1{}, ErrDurableRootRecordDigest
	}
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
	copy(record.ParentRecordDigest[:], image[248:280])
	copy(record.MetaProjectionDigest[:], image[280:312])
	if err := record.validate(pageID); err != nil {
		return DurableRootRecordV1{}, err
	}
	return record, nil
}

func durableRootRecordDigestV1(image []byte) [32]byte {
	canonical := append([]byte(nil), image...)
	clear(canonical[8:12])
	clear(canonical[312:344])
	return sha256.Sum256(canonical)
}
