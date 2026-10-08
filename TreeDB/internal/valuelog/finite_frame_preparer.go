package valuelog

import (
	"math"
	"unsafe"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// FiniteFramePreparer is the existing no-dictionary serial frame preparer with
// exact owned backing admitted before allocation. Its body remains borrowed
// until ReleaseBody. It neither changes grouping/policy nor runs workers.
type FiniteFramePreparer struct {
	preparer           FramePreparer
	body               []byte
	reserve            func(uint64) error
	maxRecords, maxRaw int
	backing            uint64
	borrowed, closed   bool
}

func NewFiniteFramePreparer(maxRecords, maxRaw int, reserve func(uint64) error) (*FiniteFramePreparer, error) {
	if reserve == nil || maxRecords < 1 || maxRecords > MaxFrameK || maxRaw < 0 || maxRaw > maxRecords*page.PageSize {
		return nil, ErrFiniteWriterLoan
	}
	n, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(FiniteFramePreparer{})), true)
	if err != nil {
		return nil, err
	}
	if err := reserve(n); err != nil {
		return nil, err
	}
	f := &FiniteFramePreparer{reserve: reserve, maxRecords: maxRecords, maxRaw: maxRaw, backing: n}
	f.preparer.ResetForReuse()
	return f, nil
}
func (f *FiniteFramePreparer) charge(n uint64) error {
	if f == nil || f.closed || n > math.MaxUint64-f.backing {
		return ErrFiniteWriterLoan
	}
	if n == 0 {
		return nil
	}
	if err := f.reserve(n); err != nil {
		return err
	}
	f.backing += n
	return nil
}
func (f *FiniteFramePreparer) chargeAllocation(n uint64, scan bool) error {
	class, err := rootpublication.StableBackingClassBytes(n, scan)
	if err != nil {
		return err
	}
	return f.charge(class)
}
func (f *FiniteFramePreparer) grow(dst *[]byte, n int) error {
	if n < 0 {
		return ErrFiniteWriterLoan
	}
	if cap(*dst) >= n {
		return nil
	}
	if err := f.chargeAllocation(uint64(n), false); err != nil {
		return err
	}
	next := make([]byte, len(*dst), n)
	copy(next, *dst)
	*dst = next
	return nil
}
func (f *FiniteFramePreparer) Prepare(records []Record, codec BlockCodec, ioNs, encodeNs, safety float64, resetHints bool) ([]byte, FrameStats, error) {
	if f == nil || f.closed || f.borrowed || len(records) == 0 || len(records) > f.maxRecords {
		return nil, FrameStats{}, ErrFiniteWriterLoan
	}
	raw := 0
	for _, r := range records {
		if r.RID == 0 || len(r.Value) > page.PageSize || len(r.Value) > f.maxRaw-raw {
			return nil, FrameStats{}, ErrFiniteWriterLoan
		}
		raw += len(r.Value)
	}
	if codec != BlockCodecSnappy && codec != BlockCodecLZ4 {
		return nil, FrameStats{}, ErrFiniteWriterLoan
	}
	// Ordinary keep policy can only retain a shorter compressed payload. Charge
	// the codec's full intrinsic destination too, including unsuccessful attempts.
	prefix := FrameHeaderSize + 8*len(records) + 4*(len(records)+1)
	bound := snappy.MaxEncodedLen(raw)
	if codec == BlockCodecLZ4 {
		bound = lz4.CompressBlockBound(raw)
	}
	if bound < 0 {
		return nil, FrameStats{}, ErrFiniteWriterLoan
	}
	if err := f.grow(&f.body, prefix+raw); err != nil {
		return nil, FrameStats{}, err
	}
	if len(records) > 1 {
		if err := f.grow(&f.preparer.rawScratch, raw); err != nil {
			return nil, FrameStats{}, err
		}
	}
	if err := f.grow(&f.preparer.blockScratch, bound); err != nil {
		return nil, FrameStats{}, err
	}
	if codec == BlockCodecLZ4 && f.preparer.blockCodecScratch.lz4Compressor == nil {
		if err := f.chargeAllocation(uint64(unsafe.Sizeof(lz4.Compressor{})), false); err != nil {
			return nil, FrameStats{}, err
		}
		f.preparer.blockCodecScratch.lz4Compressor = &lz4.Compressor{}
	}
	f.preparer.SetBlockCompression(codec, true)
	f.preparer.SetKeepPolicy(ioNs, encodeNs, safety)
	f.preparer.SetEncodeSampleStride(0)
	if resetHints {
		f.preparer.ResetCompressionHints()
	}
	body, stats, err := f.preparer.PrepareFrameInto(f.body[:0], 0, nil, records)
	if err != nil {
		return nil, FrameStats{}, err
	}
	if cap(body) != cap(f.body) || len(body) > prefix+raw {
		return nil, FrameStats{}, ErrFiniteWriterLoan
	}
	f.body = body
	f.borrowed = true
	return body, stats, nil
}
func (f *FiniteFramePreparer) ReleaseBody() {
	if f == nil {
		return
	}
	f.body = f.body[:0]
	f.preparer.encLimiter = limitedSliceWriter{}
	f.borrowed = false
}
func (f *FiniteFramePreparer) BackingBytes() uint64 {
	if f == nil {
		return 0
	}
	return f.backing
}
func (f *FiniteFramePreparer) Close() error {
	if f == nil || f.closed {
		return nil
	}
	if f.borrowed {
		return ErrFiniteWriterLoan
	}
	f.preparer.rawScratch = nil
	f.preparer.encScratch = nil
	f.preparer.blockScratch = nil
	f.preparer.blockCodecScratch = blockCodecScratch{}
	f.preparer.encLimiter = limitedSliceWriter{}
	f.body = nil
	f.reserve = nil
	f.closed = true
	return nil
}
