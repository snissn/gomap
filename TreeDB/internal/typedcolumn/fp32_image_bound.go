package typedcolumn

import (
	"encoding/binary"
	"errors"
	"math"
)

// FP32ImageEncodedUpperBound bounds a single raw FP32 vector column plus the
// stats-disabled delta-varint primary ordinal column used by typed publication.
// It reads no values and allocates no output. Other layouts fail closed. This
// bounds encoded TCIM bytes only, not builder scratch, asset containers or pages.
func FP32ImageEncodedUpperBound(opts Options, rows int, imageOpts ColumnPartImageOptions) (int64, error) {
	if rows <= 0 || uint64(rows) > math.MaxUint32 || len(opts.Columns) != 2 || opts.SchemaMode != ColumnSchemaFixed || len(opts.AggregateMetadata) != 0 || opts.Compression.Default != CompressionNone || opts.PartPolicy.DefaultCodecBlockRows != 0 || opts.PartPolicy.AdaptiveMarkSizing != (ColumnAdaptiveMarkSizing{}) {
		return 0, errFP32ImageBoundLayout
	}
	p, v := opts.Columns[0], opts.Columns[1]
	if p.Name == "" || v.Name == "" || p.Name == v.Name || uint64(len(p.Name)) > math.MaxUint32 || uint64(len(v.Name)) > math.MaxUint32 || v.FixedWidthElements <= 0 || uint64(v.FixedWidthElements) > math.MaxUint32 {
		return 0, errFP32ImageBoundLayout
	}
	if (p.Compression != CompressionNone && p.Compression != CompressionLZ4) || p != (ColumnDefinition{Name: p.Name, Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: p.Compression, CompressionSet: p.CompressionSet, StatsDisabled: true}) || v != (ColumnDefinition{Name: v.Name, Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, CompressionSet: v.CompressionSet, FixedWidthElements: v.FixedWidthElements, StatsDisabled: v.StatsDisabled}) {
		return 0, errFP32ImageBoundLayout
	}
	if len(opts.LogicalPrimaryKey.Columns) != 1 || opts.LogicalPrimaryKey.Columns[0] != p.Name || len(opts.SortKey.Columns) != 1 {
		return 0, errFP32ImageBoundLayout
	}
	sort := opts.SortKey.Columns[0]
	if sort.Column != p.Name || (sort.Direction != "" && sort.Direction != SortKeyAsc) || sort.Nulls != SortKeyNullsDefault {
		return 0, errFP32ImageBoundLayout
	}
	if len(imageOpts.Dictionaries) > 1 || len(imageOpts.DictionaryOrder) != 0 || len(imageOpts.DictionaryCollation) != 0 {
		return 0, errFP32ImageBoundLayout
	}
	// admitCompressionInto retains LZ4/ZSTD only if smaller than raw, including the
	// whole locator-section caller. Encoder scratch is not part of this bound.
	for _, compression := range []Compression{imageOpts.SectionCompression, imageOpts.RowLocatorSectionCompression, imageOpts.DictionarySectionCompression, imageOpts.PruningMetadataSectionCompression} {
		if compression != CompressionNone && compression != CompressionLZ4 && compression != CompressionZSTD {
			return 0, errFP32ImageBoundLayout
		}
	}
	for name, logical := range imageOpts.LayoutLogicalTypes {
		if (name != p.Name && name != v.Name) || uint64(len(logical)) > math.MaxUint32 {
			return 0, errFP32ImageBoundLayout
		}
	}
	align, err := columnPartImageAlignment(imageOpts.SectionAlignment)
	if err != nil {
		return 0, err
	}
	rpg := opts.PartPolicy.RowsPerGranule
	if rpg == 0 {
		rpg = DefaultRowsPerGranule
	}
	if rpg < 1 {
		return 0, errFP32ImageBoundLayout
	}
	g := int64(0)
	if rows > 0 {
		g = 1 + int64((rows-1)/rpg)
	}
	pn, vn := int64(len(p.Name)), int64(len(v.Name))
	var n encodedByteBound
	// The collection adapter emits one metadata-only dictionary even for FP32.
	// Bound its raw encoding (count/name/count/code/string); compact selection
	// and optional compression cannot enlarge that representation. Actual
	// column dictionaries remain outside this two-column layout contract.
	for name, entries := range imageOpts.Dictionaries {
		if name == p.Name || name == v.Name || len(entries) != 1 || uint64(len(name)) > math.MaxUint32 {
			return 0, errFP32ImageBoundLayout
		}
		n.add(64 + int64(align-1) + int64(len("part_dictionaries")) + 4 + 4 + int64(len(name)) + 4)
		for value := range entries {
			if uint64(len(value)) > math.MaxUint32 {
				return 0, errFP32ImageBoundLayout
			}
			n.add(8 + 4 + int64(len(value)))
		}
	}
	// Manifest: fixed header, seven descriptors, their two framed strings, and
	// at most alignment-1 padding bytes before every section (no tail padding).
	n.add(32 + 7*(64+int64(align-1)) + int64(len("part_descriptor")+len("sort_key")+len("sort_key_marks")+len("primary_id_locators")+len("writer_layout_contract")) + pn + vn)
	// Descriptor header/primary key; 8 int64 fields per granule; each column
	// has 22 fixed/name-framing bytes and each block has 94 metadata bytes.
	n.add(46 + pn + 44 + pn + vn)
	n.product(g, 64+2*94)
	// One ascending ordinal sort key and one prefix per mark, two int64 bounds.
	n.add(16 + pn + int64(len(SortKeyAsc)) + 4)
	n.product(g, 60+2*pn)
	// Generic locators dominate contiguous representation for nonempty parts;
	// reserve its fixed payload too for tiny/empty parts.
	n.add(rowLocatorContiguousPayloadBytes + 4)
	n.product(int64(rows), 32)
	// Layout contract: fixed header; scalar/vector column metadata (no offset
	// list extensions/dictionaries); 88 bytes per block. Logical strings owned
	// by image options are included rather than assuming their spelling.
	n.add(56 + 2*152 + pn + vn + int64(len(imageOpts.LayoutLogicalTypes[p.Name])) + int64(len(imageOpts.LayoutLogicalTypes[v.Name])))
	n.product(g, 2*88)
	n.product(int64(rows), binary.MaxVarintLen64)
	n.product(int64(rows), 4*int64(v.FixedWidthElements))
	if n.overflow {
		return 0, errFP32ImageBoundLayout
	}
	return n.bytes, nil
}

var errFP32ImageBoundLayout = errors.New("typedcolumn: unsupported or overflowing FP32 image bound layout")

type encodedByteBound struct {
	bytes    int64
	overflow bool
}

func (b *encodedByteBound) add(n int64) {
	if n < 0 || n > math.MaxInt64-b.bytes {
		b.overflow = true
		return
	}
	b.bytes += n
}
func (b *encodedByteBound) product(a, c int64) {
	if a < 0 || c < 0 || (a != 0 && c > math.MaxInt64/a) {
		b.overflow = true
		return
	}
	b.add(a * c)
}
