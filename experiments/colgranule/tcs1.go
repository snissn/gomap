package colgranule

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	tcs1Magic          uint32 = 0x31534354 // "TCS1", little-endian on disk.
	tcs1Version        uint16 = 1
	tcs1PartImageKind  uint16 = 1
	tcs1HeaderBytes           = 48
	tcs1SupportedFlags uint32 = 0
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
	binary.LittleEndian.PutUint32(out[0:4], tcs1Magic)
	binary.LittleEndian.PutUint16(out[4:6], tcs1Version)
	binary.LittleEndian.PutUint16(out[6:8], tcs1PartImageKind)
	binary.LittleEndian.PutUint32(out[8:12], 0)
	binary.LittleEndian.PutUint32(out[12:16], tcs1HeaderBytes)
	binary.LittleEndian.PutUint64(out[16:24], uint64(payloadBytes))
	binary.LittleEndian.PutUint64(out[24:32], image.PartID)
	binary.LittleEndian.PutUint64(out[32:40], uint64(image.Rows))
	binary.LittleEndian.PutUint16(out[40:42], image.Version)
	binary.LittleEndian.PutUint16(out[42:44], 0)
	checksum := crc32.ChecksumIEEE(image.Bytes)
	binary.LittleEndian.PutUint32(out[44:48], checksum)
	copy(out[tcs1HeaderBytes:], image.Bytes)
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
	ref, err := store.Put(ColumnAssetKindTCS1PartImage, payload)
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

func ColumnPartFromTCS1Asset(store ColumnAssetStore, ref ColumnAssetRef) (*ColumnPart, TCS1PartRecord, error) {
	image, record, err := LoadTCS1ColumnPartImage(store, ref)
	if err != nil {
		return nil, TCS1PartRecord{}, err
	}
	part, err := ColumnPartFromImage(image)
	if err != nil {
		return nil, TCS1PartRecord{}, err
	}
	return part, record, nil
}

func TCS1AssetBackedColumnPart(part *ColumnPart, dictionaries map[string]map[string]int64, store ColumnAssetStore) (*ColumnPart, ColumnAssetRef, TCS1PartRecord, error) {
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
	reconstructed, record, err := ColumnPartFromTCS1Asset(store, ref)
	if err != nil {
		return nil, ColumnAssetRef{}, TCS1PartRecord{}, err
	}
	return reconstructed, ref, record, nil
}

func decodeTCS1Header(data []byte) (TCS1PartRecord, []byte, error) {
	if len(data) < tcs1HeaderBytes {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: truncated TCS1 header bytes=%d", len(data))
	}
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != tcs1Magic {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: invalid TCS1 magic 0x%x", magic)
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version != tcs1Version {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: unsupported TCS1 version %d", version)
	}
	kind := binary.LittleEndian.Uint16(data[6:8])
	if kind != tcs1PartImageKind {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: unsupported TCS1 kind %d", kind)
	}
	flags := binary.LittleEndian.Uint32(data[8:12])
	if flags&^tcs1SupportedFlags != 0 {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: unsupported TCS1 flags 0x%x", flags)
	}
	headerBytes := binary.LittleEndian.Uint32(data[12:16])
	if headerBytes != tcs1HeaderBytes {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 header bytes=%d want %d", headerBytes, tcs1HeaderBytes)
	}
	payloadBytes := binary.LittleEndian.Uint64(data[16:24])
	if payloadBytes > uint64(len(data)-tcs1HeaderBytes) {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 payload bytes=%d exceed available=%d", payloadBytes, len(data)-tcs1HeaderBytes)
	}
	if payloadBytes != uint64(len(data)-tcs1HeaderBytes) {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 payload bytes=%d want payload bytes=%d", payloadBytes, len(data)-tcs1HeaderBytes)
	}
	partID := binary.LittleEndian.Uint64(data[24:32])
	rows64 := binary.LittleEndian.Uint64(data[32:40])
	if rows64 > uint64(^uint(0)>>1) {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 rows=%d exceeds host int", rows64)
	}
	rows := int(rows64)
	imageVersion := binary.LittleEndian.Uint16(data[40:42])
	if reserved := binary.LittleEndian.Uint16(data[42:44]); reserved != 0 {
		return TCS1PartRecord{}, nil, fmt.Errorf("colgranule: TCS1 reserved=%d want 0", reserved)
	}
	wantChecksum := binary.LittleEndian.Uint32(data[44:48])
	payload := data[tcs1HeaderBytes:]
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
