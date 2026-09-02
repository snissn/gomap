package colgranule

import (
	"encoding/binary"
	"fmt"
	crc32 "github.com/snissn/go-crc32-asm"
)

const (
	tcs1Magic              uint32 = 0x31534354 // "TCS1", little-endian on disk.
	tcs1Version            uint16 = 1
	tcs1PartImageKind      uint16 = 1
	tcs1SupportedFlags     uint32 = 0
	tcs1MagicOffset               = 0
	tcs1VersionOffset             = tcs1MagicOffset + 4
	tcs1KindOffset                = tcs1VersionOffset + 2
	tcs1FlagsOffset               = tcs1KindOffset + 2
	tcs1HeaderBytesOffset         = tcs1FlagsOffset + 4
	tcs1PayloadBytesOffset        = tcs1HeaderBytesOffset + 4
	tcs1PartIDOffset              = tcs1PayloadBytesOffset + 8
	tcs1RowsOffset                = tcs1PartIDOffset + 8
	tcs1ImageVersionOffset        = tcs1RowsOffset + 8
	tcs1ReservedOffset            = tcs1ImageVersionOffset + 2
	tcs1PayloadCRC32Offset        = tcs1ReservedOffset + 2
	tcs1HeaderBytes               = tcs1PayloadCRC32Offset + 4
	tcs1PayloadOffset             = tcs1HeaderBytes
)

type TCS1PartRecord struct {
	Version      uint16         `json:"version"`
	Kind         uint16         `json:"kind"`
	Flags        uint32         `json:"flags"`
	PartID       uint64         `json:"part_id"`
	Rows         int            `json:"rows"`
	ImageVersion uint16         `json:"image_version"`
	PayloadBytes int            `json:"payload_bytes"`
	TotalBytes   int            `json:"total_bytes"`
	PayloadCRC32 uint32         `json:"payload_crc32"`
	AssetRef     ColumnAssetRef `json:"asset_ref,omitempty"`
}

func EncodeTCS1ColumnPartImage(image ColumnPartImage) ([]byte, TCS1PartRecord, error) {
	if image.TotalBytes() == 0 {
		return nil, TCS1PartRecord{}, fmt.Errorf("colgranule: empty column part image")
	}
	if image.Rows < 0 {
		return nil, TCS1PartRecord{}, fmt.Errorf("colgranule: negative image rows %d", image.Rows)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		return nil, TCS1PartRecord{}, fmt.Errorf("colgranule: validate image before TCS1 encode: %w", err)
	}
	if parsed.PartID != image.PartID {
		return nil, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 encode part id=%d parsed image part id=%d", image.PartID, parsed.PartID)
	}
	if parsed.Rows != image.Rows {
		return nil, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 encode rows=%d parsed image rows=%d", image.Rows, parsed.Rows)
	}
	if parsed.Version != image.Version {
		return nil, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 encode image version=%d parsed image version=%d", image.Version, parsed.Version)
	}
	payloadBytes := len(image.Bytes)
	totalBytes := tcs1HeaderBytes + payloadBytes
	out := make([]byte, totalBytes)
	binary.LittleEndian.PutUint32(out[tcs1MagicOffset:tcs1VersionOffset], tcs1Magic)
	binary.LittleEndian.PutUint16(out[tcs1VersionOffset:tcs1KindOffset], tcs1Version)
	binary.LittleEndian.PutUint16(out[tcs1KindOffset:tcs1FlagsOffset], tcs1PartImageKind)
	binary.LittleEndian.PutUint32(out[tcs1FlagsOffset:tcs1HeaderBytesOffset], 0)
	binary.LittleEndian.PutUint32(out[tcs1HeaderBytesOffset:tcs1PayloadBytesOffset], tcs1HeaderBytes)
	binary.LittleEndian.PutUint64(out[tcs1PayloadBytesOffset:tcs1PartIDOffset], uint64(payloadBytes))
	binary.LittleEndian.PutUint64(out[tcs1PartIDOffset:tcs1RowsOffset], image.PartID)
	binary.LittleEndian.PutUint64(out[tcs1RowsOffset:tcs1ImageVersionOffset], uint64(image.Rows))
	binary.LittleEndian.PutUint16(out[tcs1ImageVersionOffset:tcs1ReservedOffset], image.Version)
	binary.LittleEndian.PutUint16(out[tcs1ReservedOffset:tcs1PayloadCRC32Offset], 0)
	checksum := crc32.ChecksumIEEE(image.Bytes)
	binary.LittleEndian.PutUint32(out[tcs1PayloadCRC32Offset:tcs1PayloadOffset], checksum)
	copy(out[tcs1PayloadOffset:], image.Bytes)
	return out, TCS1PartRecord{
		Version:      tcs1Version,
		Kind:         tcs1PartImageKind,
		PartID:       image.PartID,
		Rows:         image.Rows,
		ImageVersion: image.Version,
		PayloadBytes: payloadBytes,
		TotalBytes:   totalBytes,
		PayloadCRC32: checksum,
	}, nil
}

func DecodeTCS1ColumnPartImage(data []byte) (ColumnPartImage, TCS1PartRecord, error) {
	record, payload, err := decodeTCS1Header(data)
	if err != nil {
		return ColumnPartImage{}, TCS1PartRecord{}, err
	}
	image, err := ParseColumnPartImage(payload)
	if err != nil {
		return ColumnPartImage{}, TCS1PartRecord{}, fmt.Errorf("colgranule: parse TCS1 column part image: %w", err)
	}
	if image.PartID != record.PartID {
		return ColumnPartImage{}, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 part id=%d image part id=%d", record.PartID, image.PartID)
	}
	if image.Rows != record.Rows {
		return ColumnPartImage{}, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 rows=%d image rows=%d", record.Rows, image.Rows)
	}
	if image.Version != record.ImageVersion {
		return ColumnPartImage{}, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 image version=%d parsed image version=%d", record.ImageVersion, image.Version)
	}
	return image, record, nil
}

func StoreTCS1ColumnPartImage(store ColumnAssetStore, image ColumnPartImage) (ColumnAssetRef, TCS1PartRecord, error) {
	if store == nil {
		return ColumnAssetRef{}, TCS1PartRecord{}, fmt.Errorf("colgranule: nil column asset store")
	}
	payload, record, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		return ColumnAssetRef{}, TCS1PartRecord{}, err
	}
	ref, err := putColumnAssetPayload(store, ColumnAssetKindTCS1PartImage, payload)
	if err != nil {
		return ColumnAssetRef{}, TCS1PartRecord{}, err
	}
	record.AssetRef = ref
	return ref, record, nil
}

func LoadTCS1ColumnPartImage(store ColumnAssetStore, ref ColumnAssetRef) (ColumnPartImage, TCS1PartRecord, error) {
	if store == nil {
		return ColumnPartImage{}, TCS1PartRecord{}, fmt.Errorf("colgranule: nil column asset store")
	}
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		return ColumnPartImage{}, TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 asset kind=%s want %s", ref.Kind, ColumnAssetKindTCS1PartImage)
	}
	payload, err := store.Read(ref)
	if err != nil {
		return ColumnPartImage{}, TCS1PartRecord{}, err
	}
	image, record, err := DecodeTCS1ColumnPartImage(payload)
	if err != nil {
		return ColumnPartImage{}, TCS1PartRecord{}, err
	}
	record.AssetRef = ref
	return image, record, nil
}

func LoadTCS1ColumnPartHeader(store ColumnAssetStore, ref ColumnAssetRef) (TCS1PartRecord, error) {
	if store == nil {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: nil column asset store")
	}
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 asset kind=%s want %s", ref.Kind, ColumnAssetKindTCS1PartImage)
	}
	var header []byte
	var err error
	if ranged, ok := store.(ColumnAssetRangeReader); ok {
		header, err = ranged.ReadRange(ref, 0, tcs1HeaderBytes)
	} else {
		var payload []byte
		payload, err = store.Read(ref)
		if err == nil {
			if len(payload) < tcs1HeaderBytes {
				err = fmt.Errorf("colgranule: truncated TCS1 header bytes=%d", len(payload))
			} else {
				header = payload[:tcs1HeaderBytes]
			}
		}
	}
	if err != nil {
		return TCS1PartRecord{}, err
	}
	record, err := DecodeTCS1ColumnPartHeader(header, ref.Length)
	if err != nil {
		return TCS1PartRecord{}, err
	}
	record.AssetRef = ref
	return record, nil
}

func ColumnPartFromTCS1Asset(store ColumnAssetStore, ref ColumnAssetRef) (*ColumnPart, TCS1PartRecord, error) {
	return ColumnPartFromTCS1AssetWithOptions(store, ref, ColumnPartImageReadOptions{
		IncludeRowLocators:       true,
		ValidateRowLocators:      true,
		IncludeAggregateMetadata: true,
	})
}

func ColumnPartFromTCS1AssetWithOptions(store ColumnAssetStore, ref ColumnAssetRef, opts ColumnPartImageReadOptions) (*ColumnPart, TCS1PartRecord, error) {
	image, record, err := LoadTCS1ColumnPartImage(store, ref)
	if err != nil {
		return nil, TCS1PartRecord{}, err
	}
	part, err := ColumnPartFromImageWithOptions(image, opts)
	if err != nil {
		return nil, TCS1PartRecord{}, err
	}
	return part, record, nil
}

func TCS1AssetBackedColumnPart(part *ColumnPart, dictionaries map[string]map[string]int64, store ColumnAssetStore) (*ColumnPart, ColumnAssetRef, TCS1PartRecord, error) {
	return TCS1AssetBackedColumnPartWithOptions(part, dictionaries, store, ColumnPartImageReadOptions{
		IncludeRowLocators:       true,
		ValidateRowLocators:      true,
		IncludeAggregateMetadata: true,
	})
}

func TCS1AssetBackedColumnPartWithOptions(part *ColumnPart, dictionaries map[string]map[string]int64, store ColumnAssetStore, opts ColumnPartImageReadOptions) (*ColumnPart, ColumnAssetRef, TCS1PartRecord, error) {
	if part == nil {
		return nil, ColumnAssetRef{}, TCS1PartRecord{}, fmt.Errorf("colgranule: nil part")
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: dictionaries})
	if err != nil {
		return nil, ColumnAssetRef{}, TCS1PartRecord{}, err
	}
	ref, record, err := StoreTCS1ColumnPartImage(store, image)
	if err != nil {
		return nil, ColumnAssetRef{}, TCS1PartRecord{}, err
	}
	reconstructed, record, err := ColumnPartFromTCS1AssetWithOptions(store, ref, opts)
	if err != nil {
		return nil, ColumnAssetRef{}, TCS1PartRecord{}, err
	}
	return reconstructed, ref, record, nil
}

func ScanOnlyTCS1AssetBackedColumnPart(part *ColumnPart, dictionaries map[string]map[string]int64, store ColumnAssetStore) (*ColumnPart, ColumnAssetRef, TCS1PartRecord, error) {
	return TCS1AssetBackedColumnPartWithOptions(part, dictionaries, store, ColumnPartImageReadOptions{})
}

func putColumnAssetPayload(store ColumnAssetStore, kind ColumnAssetKind, payload []byte) (ColumnAssetRef, error) {
	if owned, ok := store.(columnAssetOwnedStore); ok {
		return owned.PutOwned(kind, payload)
	}
	return store.Put(kind, payload)
}

func DecodeTCS1ColumnPartHeader(header []byte, totalBytes int64) (TCS1PartRecord, error) {
	if len(header) < tcs1HeaderBytes {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: truncated TCS1 header bytes=%d", len(header))
	}
	if totalBytes < tcs1HeaderBytes {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 total bytes=%d below header bytes=%d", totalBytes, tcs1HeaderBytes)
	}
	if totalBytes > int64(^uint(0)>>1) {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 total bytes=%d exceeds host int", totalBytes)
	}
	magic := binary.LittleEndian.Uint32(header[tcs1MagicOffset:tcs1VersionOffset])
	if magic != tcs1Magic {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: invalid TCS1 magic 0x%x", magic)
	}
	version := binary.LittleEndian.Uint16(header[tcs1VersionOffset:tcs1KindOffset])
	if version != tcs1Version {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: unsupported TCS1 version %d", version)
	}
	kind := binary.LittleEndian.Uint16(header[tcs1KindOffset:tcs1FlagsOffset])
	if kind != tcs1PartImageKind {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: unsupported TCS1 kind %d", kind)
	}
	flags := binary.LittleEndian.Uint32(header[tcs1FlagsOffset:tcs1HeaderBytesOffset])
	if flags&^tcs1SupportedFlags != 0 {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: unsupported TCS1 flags 0x%x", flags)
	}
	headerBytes := binary.LittleEndian.Uint32(header[tcs1HeaderBytesOffset:tcs1PayloadBytesOffset])
	if headerBytes != tcs1HeaderBytes {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 header bytes=%d want %d", headerBytes, tcs1HeaderBytes)
	}
	payloadBytes := binary.LittleEndian.Uint64(header[tcs1PayloadBytesOffset:tcs1PartIDOffset])
	if payloadBytes > uint64(totalBytes-int64(tcs1PayloadOffset)) {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 payload bytes=%d exceed available=%d", payloadBytes, totalBytes-int64(tcs1PayloadOffset))
	}
	if payloadBytes != uint64(totalBytes-int64(tcs1PayloadOffset)) {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 payload bytes=%d want payload bytes=%d", payloadBytes, totalBytes-int64(tcs1PayloadOffset))
	}
	partID := binary.LittleEndian.Uint64(header[tcs1PartIDOffset:tcs1RowsOffset])
	rows64 := binary.LittleEndian.Uint64(header[tcs1RowsOffset:tcs1ImageVersionOffset])
	if rows64 > uint64(^uint(0)>>1) {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 rows=%d exceeds host int", rows64)
	}
	imageVersion := binary.LittleEndian.Uint16(header[tcs1ImageVersionOffset:tcs1ReservedOffset])
	if reserved := binary.LittleEndian.Uint16(header[tcs1ReservedOffset:tcs1PayloadCRC32Offset]); reserved != 0 {
		return TCS1PartRecord{}, fmt.Errorf("colgranule: TCS1 reserved=%d want 0", reserved)
	}
	return TCS1PartRecord{
		Version:      version,
		Kind:         kind,
		Flags:        flags,
		PartID:       partID,
		Rows:         int(rows64),
		ImageVersion: imageVersion,
		PayloadBytes: int(payloadBytes),
		TotalBytes:   int(totalBytes),
		PayloadCRC32: binary.LittleEndian.Uint32(header[tcs1PayloadCRC32Offset:tcs1PayloadOffset]),
	}, nil
}

func decodeTCS1Header(data []byte) (TCS1PartRecord, []byte, error) {
	if len(data) < tcs1HeaderBytes {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: truncated TCS1 header bytes=%d", len(data))
	}
	magic := binary.LittleEndian.Uint32(data[tcs1MagicOffset:tcs1VersionOffset])
	if magic != tcs1Magic {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: invalid TCS1 magic 0x%x", magic)
	}
	version := binary.LittleEndian.Uint16(data[tcs1VersionOffset:tcs1KindOffset])
	if version != tcs1Version {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: unsupported TCS1 version %d", version)
	}
	kind := binary.LittleEndian.Uint16(data[tcs1KindOffset:tcs1FlagsOffset])
	if kind != tcs1PartImageKind {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: unsupported TCS1 kind %d", kind)
	}
	flags := binary.LittleEndian.Uint32(data[tcs1FlagsOffset:tcs1HeaderBytesOffset])
	if flags&^tcs1SupportedFlags != 0 {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: unsupported TCS1 flags 0x%x", flags)
	}
	headerBytes := binary.LittleEndian.Uint32(data[tcs1HeaderBytesOffset:tcs1PayloadBytesOffset])
	if headerBytes != tcs1HeaderBytes {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 header bytes=%d want %d", headerBytes, tcs1HeaderBytes)
	}
	payloadBytes := binary.LittleEndian.Uint64(data[tcs1PayloadBytesOffset:tcs1PartIDOffset])
	if payloadBytes > uint64(len(data)-tcs1PayloadOffset) {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 payload bytes=%d exceed available=%d", payloadBytes, len(data)-tcs1PayloadOffset)
	}
	if payloadBytes != uint64(len(data)-tcs1PayloadOffset) {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 payload bytes=%d want payload bytes=%d", payloadBytes, len(data)-tcs1PayloadOffset)
	}
	partID := binary.LittleEndian.Uint64(data[tcs1PartIDOffset:tcs1RowsOffset])
	rows64 := binary.LittleEndian.Uint64(data[tcs1RowsOffset:tcs1ImageVersionOffset])
	if rows64 > uint64(^uint(0)>>1) {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 rows=%d exceeds host int", rows64)
	}
	rows := int(rows64)
	imageVersion := binary.LittleEndian.Uint16(data[tcs1ImageVersionOffset:tcs1ReservedOffset])
	if reserved := binary.LittleEndian.Uint16(data[tcs1ReservedOffset:tcs1PayloadCRC32Offset]); reserved != 0 {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 reserved=%d want 0", reserved)
	}
	wantChecksum := binary.LittleEndian.Uint32(data[tcs1PayloadCRC32Offset:tcs1PayloadOffset])
	payload := data[tcs1PayloadOffset:]
	if checksum := crc32.ChecksumIEEE(payload); checksum != wantChecksum {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 payload checksum=%08x want %08x", checksum, wantChecksum)
	}
	return TCS1PartRecord{
		Version:      version,
		Kind:         kind,
		Flags:        flags,
		PartID:       partID,
		Rows:         rows,
		ImageVersion: imageVersion,
		PayloadBytes: int(payloadBytes),
		TotalBytes:   len(data),
		PayloadCRC32: wantChecksum,
	}, payload, nil
}
