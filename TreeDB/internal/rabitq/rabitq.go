package rabitq

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	// CodecName is the durable quantized codec name for TreeDB RaBitQ v1.
	CodecName = "rabitq_1bit"
	// CodecVersion is the first TreeDB RaBitQ codec contract version.
	CodecVersion uint32 = 1
	// CodeWidthBits is the number of stored bits per code dimension.
	CodeWidthBits = 1

	// StorageRole is the quantizedasset role selected by the v1 contract.
	StorageRole = "packed_codes"
	// StorageLogicalType is the typed-column logical/physical code shape.
	StorageLogicalType = "packed_bit_vector"
	// StorageEncoding is the raw typed-column encoding for StorageLogicalType.
	StorageEncoding = "raw_packed_bit_vector"
	// BitOrder documents the TreeDB packed-code bit order used on disk.
	BitOrder = "lsb0"

	// RotationName is part of the canonical codec config identity.
	RotationName = "signed_permutation_fwht_padded_v1"
	// DefaultSeed is the deterministic v1 seed used when callers do not supply a
	// workload-specific seed. It is intentionally non-zero and stable.
	DefaultSeed uint64 = 0x7261626974710001
)

var (
	// ErrInvalidConfig reports an impossible or unsupported codec shape.
	ErrInvalidConfig = errors.New("rabitq: invalid config")
	// ErrDimensionMismatch reports vector/query/code shape mismatch.
	ErrDimensionMismatch = errors.New("rabitq: dimension mismatch")
	// ErrDegenerateVector reports zero, non-finite, or otherwise unencodable input.
	ErrDegenerateVector = errors.New("rabitq: degenerate vector")
)

// Config is the v1 codec configuration identity. Vector dimensions are schema
// shape rather than serialized config; NewPlan binds Config to dimensions.
type Config struct {
	Seed uint64
}

// DefaultConfig returns the stable TreeDB rabitq_1bit v1 config.
func DefaultConfig() Config { return Config{Seed: DefaultSeed} }

// CanonicalBytes returns the stable byte identity used for manifest config
// comparison and hashing. The format is line-oriented ASCII by design so docs,
// tests, and future non-Go implementations can reproduce it exactly.
func (c Config) CanonicalBytes() []byte {
	return []byte(fmt.Sprintf("codec=%s\nversion=%d\nmetric=cosine\nnormalization=unit_l2\nrotation=%s\nseed=0x%016x\nstorage_role=%s\nstorage_logical_type=%s\nstorage_encoding=%s\nbit_order=%s\npadding=zero\ncode_width_bits=%d\n", CodecName, CodecVersion, RotationName, c.Seed, StorageRole, StorageLogicalType, StorageEncoding, BitOrder, CodeWidthBits))
}

// Hash64 returns the FNV-1a hash of CanonicalBytes. Quantized manifests may use
// this as CodecDescriptor.ConfigHash while storing CanonicalBytes as Config.
func (c Config) Hash64() uint64 {
	h := fnv.New64a()
	_, _ = h.Write(c.CanonicalBytes())
	return h.Sum64()
}

// Plan binds a Config to a vector dimension and owns deterministic rotation
// metadata. Plan is immutable and safe for concurrent use; caller-provided
// Workspace values are not safe for concurrent sharing.
type Plan struct {
	cfg              Config
	vectorDimensions int
	codeDimensions   int
	bytesPerCode     int
	invSqrtCodeDims  float64
	perm             []int
	signs            []float64
}

// Workspace holds reusable scratch for Encode, EncodeQuery, and query byte-table
// preparation. Query values returned by EncodeQuery alias query-specific
// workspace buffers and remain valid until the next EncodeQuery call using the
// same Workspace.
type Workspace struct {
	work         []float64
	queryBits    []byte
	queryWeights []float32

	queryByteMismatchWeights    []float64
	queryByteMismatchSignBits   []byte
	queryByteMismatchAbsWeights []float32
	queryByteMismatchCodeDims   int
	queryByteMismatchWeightSum  float64
	queryByteMismatchCacheValid bool
}

// EncodedVector contains the data-code row and side-array values produced by
// Encode. Code is a packed_bit_vector row using TreeDB LSB-first layout.
type EncodedVector struct {
	Code                   []byte
	CodeCount              uint32
	QuantizedDotProductInv float32
}

// Query contains weighted-popcount inputs for ScoreCosine. SignBits uses the
// same LSB-first bit order as data codes. AbsWeights[i] is abs(rotated_query[i]).
type Query struct {
	SignBits       []byte
	AbsWeights     []float32
	WeightSum      float32
	CodeDimensions int
}

// ByteMismatchTableEntries is the number of per-byte XOR masks in a query-local
// mismatch-weight table.
const ByteMismatchTableEntries = 256

// PrepareQueryByteMismatchWeights builds query-local byte/XOR mask tables for
// the v1 weighted sign-dot scorer. The returned slice aliases ws and remains
// valid until the next operation that reuses the same workspace scratch.
func PrepareQueryByteMismatchWeights(query Query, ws *Workspace) ([]float64, float64, bool) {
	if query.CodeDimensions <= 0 || len(query.AbsWeights) < query.CodeDimensions || len(query.SignBits) == 0 {
		return nil, 0, false
	}
	bytesPerCode := (query.CodeDimensions + 7) / 8
	if len(query.SignBits) != bytesPerCode {
		return nil, 0, false
	}
	cacheable := ws != nil
	if ws == nil {
		ws = &Workspace{}
	}
	entries := bytesPerCode * ByteMismatchTableEntries
	if cacheable && ws.queryByteMismatchCacheMatches(query, bytesPerCode, entries) {
		return ws.queryByteMismatchWeights, ws.queryByteMismatchWeightSum, true
	}
	if cacheable {
		ws.queryByteMismatchCacheValid = false
	}
	if cap(ws.queryByteMismatchWeights) < entries {
		ws.queryByteMismatchWeights = make([]float64, entries)
	} else {
		ws.queryByteMismatchWeights = ws.queryByteMismatchWeights[:entries]
	}
	var queryWeightSum float64
	var byteWeights [8]float64
	for byteIdx := 0; byteIdx < bytesPerCode; byteIdx++ {
		validBits := query.CodeDimensions - byteIdx*8
		if validBits > 8 {
			validBits = 8
		}
		if validBits <= 0 {
			return nil, 0, false
		}
		for bit := 0; bit < validBits; bit++ {
			weight := float64(query.AbsWeights[byteIdx*8+bit])
			if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 {
				return nil, 0, false
			}
			byteWeights[bit] = weight
			queryWeightSum += weight
		}
		for bit := validBits; bit < len(byteWeights); bit++ {
			byteWeights[bit] = 0
		}
		base := byteIdx * ByteMismatchTableEntries
		ws.queryByteMismatchWeights[base] = 0
		for mask := 1; mask < ByteMismatchTableEntries; mask++ {
			leastBit := mask & -mask
			ws.queryByteMismatchWeights[base+mask] = ws.queryByteMismatchWeights[base+(mask^leastBit)] + byteWeights[bits.TrailingZeros8(uint8(leastBit))]
		}
	}
	if queryWeightSum <= 0 || math.IsNaN(queryWeightSum) || math.IsInf(queryWeightSum, 0) {
		return nil, 0, false
	}
	if cacheable {
		ws.rememberQueryByteMismatchCache(query, queryWeightSum)
	}
	return ws.queryByteMismatchWeights, queryWeightSum, true
}

func (ws *Workspace) queryByteMismatchCacheMatches(query Query, bytesPerCode, entries int) bool {
	return ws != nil &&
		ws.queryByteMismatchCacheValid &&
		ws.queryByteMismatchCodeDims == query.CodeDimensions &&
		len(ws.queryByteMismatchWeights) == entries &&
		len(ws.queryByteMismatchSignBits) == bytesPerCode &&
		len(ws.queryByteMismatchAbsWeights) == query.CodeDimensions &&
		bytes.Equal(ws.queryByteMismatchSignBits, query.SignBits) &&
		equalFloat32Slice(ws.queryByteMismatchAbsWeights, query.AbsWeights[:query.CodeDimensions]) &&
		ws.queryByteMismatchWeightSum > 0 &&
		!math.IsNaN(ws.queryByteMismatchWeightSum) &&
		!math.IsInf(ws.queryByteMismatchWeightSum, 0)
}

func (ws *Workspace) rememberQueryByteMismatchCache(query Query, queryWeightSum float64) {
	if cap(ws.queryByteMismatchSignBits) < len(query.SignBits) {
		ws.queryByteMismatchSignBits = make([]byte, len(query.SignBits))
	} else {
		ws.queryByteMismatchSignBits = ws.queryByteMismatchSignBits[:len(query.SignBits)]
	}
	copy(ws.queryByteMismatchSignBits, query.SignBits)
	if cap(ws.queryByteMismatchAbsWeights) < query.CodeDimensions {
		ws.queryByteMismatchAbsWeights = make([]float32, query.CodeDimensions)
	} else {
		ws.queryByteMismatchAbsWeights = ws.queryByteMismatchAbsWeights[:query.CodeDimensions]
	}
	copy(ws.queryByteMismatchAbsWeights, query.AbsWeights[:query.CodeDimensions])
	ws.queryByteMismatchCodeDims = query.CodeDimensions
	ws.queryByteMismatchWeightSum = queryWeightSum
	ws.queryByteMismatchCacheValid = true
}

func equalFloat32Slice(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// QueryByteMismatchWeightsValid reports whether weights has the shape returned
// by PrepareQueryByteMismatchWeights for query.
func QueryByteMismatchWeightsValid(query Query, weights []float64, queryWeightSum float64) bool {
	return query.CodeDimensions > 0 && len(query.SignBits) == (query.CodeDimensions+7)/8 && len(weights) == len(query.SignBits)*ByteMismatchTableEntries && queryWeightSum > 0 && !math.IsNaN(queryWeightSum) && !math.IsInf(queryWeightSum, 0)
}

// NewPlan validates dimensions, derives code shape, and precomputes the v1
// deterministic signed permutation. CodeDimensions is next_power_of_two(dims),
// matching the padded Walsh-Hadamard rotation length.
func NewPlan(vectorDimensions int, cfg Config) (*Plan, error) {
	if vectorDimensions <= 0 {
		return nil, fmt.Errorf("%w: vector_dimensions=%d must be positive", ErrInvalidConfig, vectorDimensions)
	}
	codeDimensions, err := nextPowerOfTwoInt(vectorDimensions)
	if err != nil {
		return nil, err
	}
	bytesPerCode := (codeDimensions + 7) / 8
	perm := make([]int, codeDimensions)
	for i := range perm {
		perm[i] = i
	}
	rng := splitmix64{state: rotationSeed(cfg.Seed, vectorDimensions, codeDimensions)}
	for i := codeDimensions - 1; i > 0; i-- {
		j := int(rng.next() % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}
	signs := make([]float64, codeDimensions)
	for i := range signs {
		if rng.next()&1 == 0 {
			signs[i] = 1
		} else {
			signs[i] = -1
		}
	}
	return &Plan{cfg: cfg, vectorDimensions: vectorDimensions, codeDimensions: codeDimensions, bytesPerCode: bytesPerCode, invSqrtCodeDims: 1 / math.Sqrt(float64(codeDimensions)), perm: perm, signs: signs}, nil
}

// Config returns the plan's codec config.
func (p *Plan) Config() Config {
	if p == nil {
		return Config{}
	}
	return p.cfg
}

// VectorDimensions returns the source float32 vector dimensions.
func (p *Plan) VectorDimensions() int {
	if p == nil {
		return 0
	}
	return p.vectorDimensions
}

// CodeDimensions returns the number of one-bit dimensions stored per row.
func (p *Plan) CodeDimensions() int {
	if p == nil {
		return 0
	}
	return p.codeDimensions
}

// BytesPerCode returns ceil(CodeDimensions/8), the packed row width.
func (p *Plan) BytesPerCode() int {
	if p == nil {
		return 0
	}
	return p.bytesPerCode
}

// Encode normalizes vector, rotates it, packs sign bits into dst, and returns
// code_count plus quantized_dot_product_inv side-array values. It allocates only
// when dst or workspace scratch lacks capacity.
func (p *Plan) Encode(dst []byte, vector []float32, ws *Workspace) (EncodedVector, error) {
	if p == nil || p.vectorDimensions <= 0 || p.codeDimensions <= 0 {
		return EncodedVector{}, fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	rotated, err := p.rotateUnit(vector, ws)
	if err != nil {
		return EncodedVector{}, err
	}
	code := ensureByteLen(dst, p.bytesPerCode)
	clear(code)
	var count uint32
	var qdp float64
	for i, value := range rotated[:p.codeDimensions] {
		if value >= 0 {
			code[i>>3] |= byte(1 << uint(i&7))
			count++
		}
		qdp += math.Abs(value)
	}
	if qdp <= 0 || math.IsNaN(qdp) || math.IsInf(qdp, 0) {
		return EncodedVector{}, fmt.Errorf("%w: quantized dot product is not positive", ErrDegenerateVector)
	}
	return EncodedVector{Code: code, CodeCount: count, QuantizedDotProductInv: float32(1 / qdp)}, nil
}

// EncodeQuery normalizes and rotates query, then prepares the sign bits and
// absolute weights consumed by ScoreCosine. Returned slices alias ws.
func (p *Plan) EncodeQuery(query []float32, ws *Workspace) (Query, error) {
	if p == nil || p.vectorDimensions <= 0 || p.codeDimensions <= 0 {
		return Query{}, fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	rotated, err := p.rotateUnit(query, ws)
	if err != nil {
		return Query{}, err
	}
	if ws == nil {
		ws = &Workspace{}
	}
	ws.queryBits = ensureByteLen(ws.queryBits, p.bytesPerCode)
	clear(ws.queryBits)
	if cap(ws.queryWeights) < p.codeDimensions {
		ws.queryWeights = make([]float32, p.codeDimensions)
	} else {
		ws.queryWeights = ws.queryWeights[:p.codeDimensions]
	}
	var weightSum float64
	for i, value := range rotated[:p.codeDimensions] {
		if value >= 0 {
			ws.queryBits[i>>3] |= byte(1 << uint(i&7))
		}
		weight := math.Abs(value)
		ws.queryWeights[i] = float32(weight)
		weightSum += weight
	}
	if weightSum <= 0 || math.IsNaN(weightSum) || math.IsInf(weightSum, 0) {
		return Query{}, fmt.Errorf("%w: query weights are not positive", ErrDegenerateVector)
	}
	return Query{SignBits: ws.queryBits, AbsWeights: ws.queryWeights, WeightSum: float32(weightSum), CodeDimensions: p.codeDimensions}, nil
}

// ScoreCosine validates side-array inputs and returns the v1 estimated cosine
// score for one encoded data vector. This is the reference oracle; accelerated
// implementations may skip repeated validation only after matching this result.
func (p *Plan) ScoreCosine(query Query, code []byte, codeCount uint32, quantizedDotProductInv float32) (float64, error) {
	if err := p.ValidateCode(code, codeCount); err != nil {
		return 0, err
	}
	if err := p.ValidateQuery(query); err != nil {
		return 0, err
	}
	if err := p.ValidateQuantizedDotProductInv(quantizedDotProductInv); err != nil {
		return 0, err
	}
	var weightedSignDot float64
	for i, weight32 := range query.AbsWeights[:p.codeDimensions] {
		weight := float64(weight32)
		if bitAt(code, i) == bitAt(query.SignBits, i) {
			weightedSignDot += weight
		} else {
			weightedSignDot -= weight
		}
	}
	return weightedSignDot / (float64(quantizedDotProductInv) * float64(p.codeDimensions)), nil
}

// ScoreEncoded is a convenience wrapper around ScoreCosine.
func (p *Plan) ScoreEncoded(query Query, encoded EncodedVector) (float64, error) {
	return p.ScoreCosine(query, encoded.Code, encoded.CodeCount, encoded.QuantizedDotProductInv)
}

// ValidateCode verifies row width, zero high padding bits, and code_count.
func (p *Plan) ValidateCode(code []byte, codeCount uint32) error {
	if p == nil || p.codeDimensions <= 0 || p.bytesPerCode <= 0 {
		return fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	if len(code) != p.bytesPerCode {
		return fmt.Errorf("%w: code bytes=%d want %d", ErrDimensionMismatch, len(code), p.bytesPerCode)
	}
	if err := validatePadding(code, p.codeDimensions); err != nil {
		return err
	}
	got := p.CountCodeBits(code)
	if got != codeCount {
		return fmt.Errorf("%w: code_count=%d want %d", ErrDimensionMismatch, codeCount, got)
	}
	return nil
}

// CountCodeBits returns the popcount over logical code dimensions, excluding
// any zero padding bits in the final byte.
func (p *Plan) CountCodeBits(code []byte) uint32 {
	if p == nil || len(code) != p.bytesPerCode || p.codeDimensions <= 0 {
		return 0
	}
	full := p.codeDimensions / 8
	var count int
	for i := 0; i < full; i++ {
		count += bits.OnesCount8(code[i])
	}
	if rem := p.codeDimensions & 7; rem != 0 {
		mask := byte((1 << uint(rem)) - 1)
		count += bits.OnesCount8(code[full] & mask)
	}
	return uint32(count)
}

// ValidateQuantizedDotProductInv verifies the finite side-array range implied by
// unit-L2 vectors and an orthonormal rotation: L1(rotated) is in
// [1, sqrt(CodeDimensions)], so its inverse is in [1/sqrt(CodeDimensions), 1]
// modulo float32 rounding tolerance.
func (p *Plan) ValidateQuantizedDotProductInv(value float32) error {
	if p == nil || p.codeDimensions <= 0 {
		return fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	fv := float64(value)
	if value <= 0 || math.IsNaN(fv) || math.IsInf(fv, 0) {
		return fmt.Errorf("%w: invalid quantized_dot_product_inv=%v", ErrDegenerateVector, value)
	}
	minValue := p.invSqrtCodeDims
	const tolerance = 1e-5
	if fv < minValue-tolerance || fv > 1+tolerance {
		return fmt.Errorf("%w: quantized_dot_product_inv=%v outside valid range [%v,%v]", ErrDegenerateVector, value, minValue, 1.0)
	}
	return nil
}

// ValidateQuery verifies query shape, sign-bit padding, finite non-negative
// weights, and finite positive WeightSum.
func (p *Plan) ValidateQuery(q Query) error {
	if p == nil || p.codeDimensions <= 0 || p.bytesPerCode <= 0 {
		return fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	if q.CodeDimensions != p.codeDimensions {
		return fmt.Errorf("%w: query code_dimensions=%d want %d", ErrDimensionMismatch, q.CodeDimensions, p.codeDimensions)
	}
	if len(q.SignBits) != p.bytesPerCode {
		return fmt.Errorf("%w: query sign bytes=%d want %d", ErrDimensionMismatch, len(q.SignBits), p.bytesPerCode)
	}
	if err := validatePadding(q.SignBits, p.codeDimensions); err != nil {
		return fmt.Errorf("%w: query sign bits: %v", ErrDimensionMismatch, err)
	}
	if len(q.AbsWeights) != p.codeDimensions {
		return fmt.Errorf("%w: query weights=%d want %d", ErrDimensionMismatch, len(q.AbsWeights), p.codeDimensions)
	}
	if q.WeightSum <= 0 || math.IsNaN(float64(q.WeightSum)) || math.IsInf(float64(q.WeightSum), 0) {
		return fmt.Errorf("%w: invalid query weight_sum=%v", ErrDegenerateVector, q.WeightSum)
	}
	var sum float64
	for i, weight := range q.AbsWeights {
		fw := float64(weight)
		if weight < 0 || math.IsNaN(fw) || math.IsInf(fw, 0) {
			return fmt.Errorf("%w: invalid query weight[%d]=%v", ErrDegenerateVector, i, weight)
		}
		sum += fw
	}
	if sum <= 0 || math.IsNaN(sum) || math.IsInf(sum, 0) {
		return fmt.Errorf("%w: query weights are not positive", ErrDegenerateVector)
	}
	if delta := math.Abs(float64(q.WeightSum) - sum); delta > 1e-4*math.Max(1, sum) {
		return fmt.Errorf("%w: query weight_sum=%v does not match weights sum=%v", ErrDimensionMismatch, q.WeightSum, sum)
	}
	return nil
}

// ValidFor reports whether q can be used with p without allocation or shape
// adaptation.
func (q Query) ValidFor(p *Plan) bool {
	return p != nil && p.ValidateQuery(q) == nil
}

func (p *Plan) rotateUnit(vector []float32, ws *Workspace) ([]float64, error) {
	if len(vector) != p.vectorDimensions {
		return nil, fmt.Errorf("%w: vector dimensions=%d want %d", ErrDimensionMismatch, len(vector), p.vectorDimensions)
	}
	var norm2 float64
	for i, v := range vector {
		fv := float64(v)
		if math.IsNaN(fv) || math.IsInf(fv, 0) {
			return nil, fmt.Errorf("%w: vector[%d]=%v is not finite", ErrDegenerateVector, i, v)
		}
		norm2 += fv * fv
	}
	if norm2 <= 0 || math.IsNaN(norm2) || math.IsInf(norm2, 0) {
		return nil, fmt.Errorf("%w: vector norm is not positive", ErrDegenerateVector)
	}
	if ws == nil {
		ws = &Workspace{}
	}
	need := p.codeDimensions * 2
	if cap(ws.work) < need {
		ws.work = make([]float64, need)
	} else {
		ws.work = ws.work[:need]
		clear(ws.work)
	}
	src := ws.work[:p.codeDimensions]
	permuted := ws.work[p.codeDimensions:need]
	invNorm := 1 / math.Sqrt(norm2)
	for i := 0; i < p.vectorDimensions; i++ {
		src[i] = float64(vector[i]) * invNorm
	}
	for i := p.vectorDimensions; i < p.codeDimensions; i++ {
		src[i] = 0
	}
	for i, source := range p.perm {
		permuted[i] = src[source] * p.signs[i]
	}
	fwhtInPlace(permuted)
	for i := range permuted {
		permuted[i] *= p.invSqrtCodeDims
	}
	return permuted, nil
}

func fwhtInPlace(values []float64) {
	for width := 1; width < len(values); width <<= 1 {
		for start := 0; start < len(values); start += width << 1 {
			for i := 0; i < width; i++ {
				a := values[start+i]
				b := values[start+i+width]
				values[start+i] = a + b
				values[start+i+width] = a - b
			}
		}
	}
}

func validatePadding(code []byte, codeDimensions int) error {
	rem := codeDimensions & 7
	if rem == 0 {
		return nil
	}
	mask := byte((1 << uint(rem)) - 1)
	if code[len(code)-1]&^mask != 0 {
		return fmt.Errorf("%w: non-zero padding bits final_byte=0x%02x valid_mask=0x%02x", ErrDimensionMismatch, code[len(code)-1], mask)
	}
	return nil
}

func bitAt(row []byte, i int) bool {
	return row[i>>3]&(1<<uint(i&7)) != 0
}

func ensureByteLen(dst []byte, n int) []byte {
	if cap(dst) < n {
		return make([]byte, n)
	}
	return dst[:n]
}

func nextPowerOfTwoInt(n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("%w: dimensions=%d must be positive", ErrInvalidConfig, n)
	}
	if n > (math.MaxInt>>1)+1 {
		return 0, fmt.Errorf("%w: dimensions=%d overflow", ErrInvalidConfig, n)
	}
	if n&(n-1) == 0 {
		return n, nil
	}
	return 1 << bits.Len(uint(n)), nil
}

type splitmix64 struct{ state uint64 }

func (s *splitmix64) next() uint64 {
	s.state += 0x9e3779b97f4a7c15
	z := s.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func rotationSeed(seed uint64, vectorDimensions, codeDimensions int) uint64 {
	z := seed ^ 0x8f3f73b5d8f24429
	z ^= uint64(vectorDimensions) * 0x9e3779b97f4a7c15
	z ^= bits.RotateLeft64(uint64(codeDimensions)*0xbf58476d1ce4e5b9, 17)
	return z
}
