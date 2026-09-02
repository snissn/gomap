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
	Kind               ResourceKind
	LogicalLane        string
	ResourceID         string
	DiagnosticPath     string
	Identity           StableIdentity
	Generation         uint64
	Digest             [32]byte
	Frontier           DurableFrontier
	Reachability       []ReachabilityField
	LogicalObligations []StableLogicalObligation
	Namespace          *DependencyManifestNamespaceV1
}

// DependencyManifestNamespaceV1 binds a reachable resource to the exact
// parent namespace generation whose create/rename was made durable before the
// root. Names are diagnostics and bounded-open validation inputs, never the
// resource identity itself.
type DependencyManifestNamespaceV1 struct {
	ParentIdentity StableIdentity
	Operation      NamespaceOperation
	OldName        string
	NewName        string
	DiagnosticPath string
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

// DependencyManifestBuildWorkV1 reports canonical entry encoding work. Payload
// assembly remains a full V1 stream because the on-disk format is unchanged.
type DependencyManifestBuildWorkV1 struct {
	EntriesVisited uint64
	EntriesEncoded uint64
	BytesEncoded   uint64
}

func NewDependencyManifestV1(entries []DependencyManifestEntryV1) (*DependencyManifestV1, error) {
	manifest, _, err := NewDependencyManifestV1WithWork(entries)
	return manifest, err
}

// NewDependencyManifestV1WithWork is NewDependencyManifestV1 with entry-encoding counters.
func NewDependencyManifestV1WithWork(entries []DependencyManifestEntryV1) (*DependencyManifestV1, DependencyManifestBuildWorkV1, error) {
	work := DependencyManifestBuildWorkV1{EntriesVisited: uint64(len(entries))}
	encoded := make([]dependencyManifestEncodedEntryV1, len(entries))
	for i := range entries {
		entry, err := normalizeDependencyManifestEntryV1(entries[i])
		if err != nil {
			return nil, work, err
		}
		raw := encodeDependencyManifestEntryV1(entry)
		encoded[i] = dependencyManifestEncodedEntryV1{entry: entry, encoded: raw}
		work.EntriesEncoded++
		work.BytesEncoded += uint64(len(raw))
	}
	manifest, err := newDependencyManifestV1FromEncoded(encoded)
	return manifest, work, err
}

type dependencyManifestEncodedEntryV1 struct {
	entry   DependencyManifestEntryV1
	encoded []byte
}

func newDependencyManifestV1FromEncoded(encoded []dependencyManifestEncodedEntryV1) (*DependencyManifestV1, error) {
	sort.Slice(encoded, func(i, j int) bool {
		return bytes.Compare(encoded[i].encoded, encoded[j].encoded) < 0
	})
	payloadBytes := 16
	for i := range encoded {
		if len(encoded[i].encoded) > int(^uint32(0)) || payloadBytes > maxDependencyManifestBytesV1-4-len(encoded[i].encoded) {
			return nil, fmt.Errorf("%w: payload exceeds %d bytes", ErrDependencyManifestFormat, maxDependencyManifestBytesV1)
		}
		if i > 0 && bytes.Equal(encoded[i-1].encoded, encoded[i].encoded) {
			return nil, fmt.Errorf("%w: duplicate resource entry", ErrDependencyManifestFormat)
		}
		payloadBytes += 4 + len(encoded[i].encoded)
	}
	entries := make([]DependencyManifestEntryV1, len(encoded))
	payload := make([]byte, 16, payloadBytes)
	copy(payload[0:8], dependencyManifestBodyMagicV1[:])
	binary.LittleEndian.PutUint16(payload[8:10], 1)
	binary.LittleEndian.PutUint16(payload[10:12], 16)
	binary.LittleEndian.PutUint32(payload[12:16], uint32(len(encoded)))
	for i := range encoded {
		entries[i] = encoded[i].entry
		payload = appendU32V1(payload, uint32(len(encoded[i].encoded)))
		payload = append(payload, encoded[i].encoded...)
	}
	return &DependencyManifestV1{entries: entries, payload: payload, digest: sha256.Sum256(payload)}, nil
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
	logical, err := normalizeDependencyManifestLogicalObligationsV1(entry.LogicalObligations, entry.Reachability)
	if err != nil {
		return DependencyManifestEntryV1{}, err
	}
	entry.LogicalObligations = logical
	if entry.Namespace != nil {
		namespace := *entry.Namespace
		if !namespace.ParentIdentity.valid() || namespace.ParentIdentity.Generation == 0 ||
			(namespace.Operation != NamespaceCreate && namespace.Operation != NamespaceRename) ||
			!stableChildBaseName(namespace.NewName) ||
			(namespace.Operation == NamespaceRename && !stableChildBaseName(namespace.OldName)) {
			return DependencyManifestEntryV1{}, fmt.Errorf("%w: invalid namespace obligation", ErrDependencyManifestFormat)
		}
		if err := validateDiagnosticPath(namespace.DiagnosticPath); err != nil {
			return DependencyManifestEntryV1{}, fmt.Errorf("%w: invalid namespace diagnostic path: %v", ErrDependencyManifestFormat, err)
		}
		entry.Namespace = &namespace
	}
	return entry, nil
}

func normalizeDependencyManifestLogicalObligationsV1(obligations []StableLogicalObligation, reachability []ReachabilityField) ([]StableLogicalObligation, error) {
	if len(obligations) == 0 {
		return nil, nil
	}
	fields := make(map[ReachabilityField]struct{}, len(reachability))
	for _, field := range reachability {
		fields[field] = struct{}{}
	}
	normalized := append([]StableLogicalObligation(nil), obligations...)
	for _, obligation := range normalized {
		if _, ok := fields[obligation.Reachability]; !ok {
			return nil, fmt.Errorf("%w: logical obligation field %q is absent from resource reachability", ErrDependencyManifestFormat, obligation.Reachability)
		}
		if err := validateStableLogicalObligation(obligation, obligation.Reachability); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrDependencyManifestFormat, err)
		}
	}
	sort.Slice(normalized, func(i, j int) bool { return stableLogicalObligationLess(normalized[i], normalized[j]) })
	for i := 1; i < len(normalized); i++ {
		if stableLogicalObligationKey(normalized[i-1]) != stableLogicalObligationKey(normalized[i]) {
			continue
		}
		if normalized[i-1] != normalized[i] {
			return nil, fmt.Errorf("%w: conflicting logical obligation", ErrDependencyManifestFormat)
		}
		return nil, fmt.Errorf("%w: duplicate logical obligation", ErrDependencyManifestFormat)
	}
	return normalized, nil
}

func (manifest *DependencyManifestV1) Entries() []DependencyManifestEntryV1 {
	if manifest == nil {
		return nil
	}
	out := make([]DependencyManifestEntryV1, len(manifest.entries))
	for i, entry := range manifest.entries {
		entry.Frontier = cloneDurableFrontier(entry.Frontier)
		entry.Reachability = append([]ReachabilityField(nil), entry.Reachability...)
		entry.LogicalObligations = cloneStableLogicalObligations(entry.LogicalObligations)
		if entry.Namespace != nil {
			namespace := *entry.Namespace
			entry.Namespace = &namespace
		}
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
	out = append(out, entry.Digest[:]...)
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
	out = appendU32V1(out, uint32(len(entry.LogicalObligations)))
	for _, obligation := range entry.LogicalObligations {
		out = appendStringV1(out, obligation.Class)
		out = appendStringV1(out, obligation.Kind)
		out = appendStringV1(out, obligation.Namespace)
		out = appendU64V1(out, obligation.Generation)
		out = appendU64V1(out, obligation.PartID)
		out = appendU64V1(out, obligation.FileID)
		out = appendU64V1(out, uint64(obligation.Offset))
		out = appendU64V1(out, uint64(obligation.Length))
		out = appendU32V1(out, obligation.Checksum)
		out = appendStringV1(out, string(obligation.Reachability))
		out = append(out, obligation.Digest[:]...)
	}
	if entry.Namespace == nil {
		out = appendU32V1(out, 0)
	} else {
		out = appendU32V1(out, 1)
		namespace := entry.Namespace
		out = appendStringV1(out, namespace.ParentIdentity.Platform)
		out = appendU64V1(out, namespace.ParentIdentity.VolumeID)
		out = append(out, namespace.ParentIdentity.ObjectID[:]...)
		out = appendU64V1(out, namespace.ParentIdentity.Generation)
		out = appendU32V1(out, uint32(namespace.Operation))
		out = appendStringV1(out, namespace.OldName)
		out = appendStringV1(out, namespace.NewName)
		out = appendStringV1(out, namespace.DiagnosticPath)
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
	digestBytes, ok := reader.bytes(32)
	if !ok {
		return DependencyManifestEntryV1{}, false
	}
	var digest [32]byte
	copy(digest[:], digestBytes)
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
	logicalCount, ok := reader.u32()
	if !ok || uint64(logicalCount) > uint64(reader.remaining()/4) {
		return DependencyManifestEntryV1{}, false
	}
	logical := make([]StableLogicalObligation, logicalCount)
	for i := range logical {
		class, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		kind, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		namespace, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		logical[i].Generation, fieldOK = reader.u64()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		logical[i].PartID, fieldOK = reader.u64()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		logical[i].FileID, fieldOK = reader.u64()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		offset, fieldOK := reader.u64()
		if !fieldOK || offset > uint64(^uint64(0)>>1) {
			return DependencyManifestEntryV1{}, false
		}
		length, fieldOK := reader.u64()
		if !fieldOK || length > uint64(^uint64(0)>>1) {
			return DependencyManifestEntryV1{}, false
		}
		checksum, fieldOK := reader.u32()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		reachabilityField, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		logicalDigest, fieldOK := reader.bytes(32)
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		logical[i].Class, logical[i].Kind, logical[i].Namespace = class, kind, namespace
		logical[i].Offset, logical[i].Length, logical[i].Checksum = int64(offset), int64(length), checksum
		logical[i].Reachability = ReachabilityField(reachabilityField)
		copy(logical[i].Digest[:], logicalDigest)
	}
	namespacePresent, ok := reader.u32()
	if !ok || namespacePresent > 1 {
		return DependencyManifestEntryV1{}, false
	}
	var namespace *DependencyManifestNamespaceV1
	if namespacePresent == 1 {
		platform, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		volume, fieldOK := reader.u64()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		object, fieldOK := reader.bytes(16)
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		parentGeneration, fieldOK := reader.u64()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		operation, fieldOK := reader.u32()
		if !fieldOK || operation > uint32(^uint8(0)) {
			return DependencyManifestEntryV1{}, false
		}
		oldName, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		newName, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		diagnosticPath, fieldOK := reader.str()
		if !fieldOK {
			return DependencyManifestEntryV1{}, false
		}
		parentIdentity := StableIdentity{Platform: platform, VolumeID: volume, Generation: parentGeneration}
		copy(parentIdentity.ObjectID[:], object)
		namespace = &DependencyManifestNamespaceV1{ParentIdentity: parentIdentity, Operation: NamespaceOperation(operation), OldName: oldName, NewName: newName, DiagnosticPath: diagnosticPath}
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
		Identity: identity, Generation: generation, Digest: digest, Frontier: frontier, Reachability: reachability,
		LogicalObligations: logical, Namespace: namespace,
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
