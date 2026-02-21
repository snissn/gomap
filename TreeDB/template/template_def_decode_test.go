package template

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func rewriteTemplateDefCRC(buf []byte) {
	if len(buf) < 4 {
		return
	}
	payloadLen := len(buf) - 4
	crc := crc32.Checksum(buf[:payloadLen], crcTable)
	binary.LittleEndian.PutUint32(buf[payloadLen:], crc)
}

func TestDecodeTemplateDefAdditionalCorruptionAndV2Mask(t *testing.T) {
	cfg := NormalizeConfig(Config{
		MinAnchorLen:          2,
		MaxAnchorLen:          64,
		MaxAnchorsPerTemplate: 8,
		MaxAnchorBytesTotal:   256,
	})

	anchorDef := TemplateDef{
		Kind:    TemplateAnchors,
		Anchors: [][]byte{[]byte("aa"), []byte("bb")},
	}
	anchorBytes, err := EncodeTemplateDef(anchorDef, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(anchor): %v", err)
	}

	// Unknown version branch.
	unknown := append([]byte(nil), anchorBytes...)
	unknown[0] = 0xFF
	rewriteTemplateDefCRC(unknown)
	if _, err := DecodeTemplateDef(unknown); err != ErrCorruptTemplateDef {
		t.Fatalf("expected ErrCorruptTemplateDef for unknown version, got %v", err)
	}

	maskBase := []byte("ABCD1234")
	varPos := []uint16{4, 5, 6, 7}
	mask := buildMaskFromPositions(varPos, len(maskBase))
	maskDef := TemplateDef{
		Kind:           TemplateMask,
		Base:           maskBase,
		Mask:           mask,
		VarPositions:   varPos,
		ConstPositions: buildConstPositions(mask, len(maskBase)),
	}
	maskBytes, err := EncodeTemplateDef(maskDef, cfg)
	if err != nil {
		t.Fatalf("EncodeTemplateDef(mask): %v", err)
	}

	// templateDefVerMask with wrong kind byte.
	badMaskKind := append([]byte(nil), maskBytes...)
	badMaskKind[1] = byte(TemplateAnchors)
	rewriteTemplateDefCRC(badMaskKind)
	if _, err := DecodeTemplateDef(badMaskKind); err != ErrCorruptTemplateDef {
		t.Fatalf("expected ErrCorruptTemplateDef for wrong mask kind, got %v", err)
	}

	// templateDefVerMaskV2 decode path.
	// Format: [ver][kind][uvar baseLen][uvar maskLen][mask][base][crc32]
	v2 := make([]byte, 0, 64)
	v2 = append(v2, templateDefVerMaskV2)
	v2 = append(v2, byte(TemplateMask))
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(maskBase)))
	v2 = append(v2, scratch[:n]...)
	n = binary.PutUvarint(scratch[:], uint64(len(mask)))
	v2 = append(v2, scratch[:n]...)
	v2 = append(v2, mask...)
	v2 = append(v2, maskBase...)
	v2 = append(v2, 0, 0, 0, 0)
	rewriteTemplateDefCRC(v2)

	dec, err := DecodeTemplateDef(v2)
	if err != nil {
		t.Fatalf("DecodeTemplateDef(v2): %v", err)
	}
	if dec.Kind != TemplateMask {
		t.Fatalf("decoded v2 kind=%v, want TemplateMask", dec.Kind)
	}
	if len(dec.VarPositions) == 0 || len(dec.ConstPositions) == 0 {
		t.Fatalf("expected non-empty v2 var/const positions")
	}
}

func TestValidateVarPositionsAdditionalBranches(t *testing.T) {
	base := []byte("ABCDE")
	mask := buildMaskFromPositions([]uint16{1, 3}, len(base))

	if err := validateVarPositions(nil, mask, []uint16{1}); err != ErrCorruptTemplateDef {
		t.Fatalf("expected base-empty error, got %v", err)
	}
	if err := validateVarPositions(base, nil, []uint16{1}); err != ErrCorruptTemplateDef {
		t.Fatalf("expected mask-empty error, got %v", err)
	}
	if err := validateVarPositions(base, mask, []uint16{9}); err != ErrCorruptTemplateDef {
		t.Fatalf("expected out-of-range error, got %v", err)
	}
	if err := validateVarPositions(base, mask, []uint16{3, 1}); err != ErrCorruptTemplateDef {
		t.Fatalf("expected non-increasing error, got %v", err)
	}
	if err := validateVarPositions(base, mask, []uint16{0}); err != ErrCorruptTemplateDef {
		t.Fatalf("expected missing-mask-bit error, got %v", err)
	}
	if err := validateVarPositions(base, mask, []uint16{1, 3}); err != nil {
		t.Fatalf("expected valid positions, got %v", err)
	}
}
