package brq

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

const (
	// CodecName is the durable quantized codec name for TreeDB BRQ v1.
	CodecName = "brq_1bit"
	// CodecVersion is the first TreeDB BRQ codec contract version.
	CodecVersion uint32 = 1
	// CodeWidthBits is the number of stored bits per code dimension.
	CodeWidthBits = 1
	// QueryWeightBits is the runtime query-weight bit width.
	QueryWeightBits = 4

	// StorageRole is the quantizedasset role selected by the v1 contract.
	StorageRole = "packed_codes"
	// StorageLogicalType is the typed-column logical/physical code shape.
	StorageLogicalType = "packed_bit_vector"
	// StorageEncoding is the raw typed-column encoding for StorageLogicalType.
	StorageEncoding = "raw_packed_bit_vector"
	// BitOrder documents the TreeDB packed-code bit order used on disk.
	BitOrder = "lsb0"
	// WordOrder documents little-endian uint64 scratch views for scorer kernels.
	WordOrder = "little_endian_uint64"
	// Padding is the only valid padding policy for partial final bytes.
	Padding = "zero"
	// QueryWeightQuantizer is the runtime query-weight quantizer identity.
	QueryWeightQuantizer = "max_abs_uint4_round_half_up"
	// ScoreLabel is the public label for quantized-only approximate scores.
	ScoreLabel = "brq_1bit_estimated_cosine_q4"
	// DataScaleSideArray is the required data side-array name.
	DataScaleSideArray = "quantized_dot_product_inv"

	// RotationName is part of the canonical codec config identity.
	RotationName = "signed_permutation_fwht_padded_v1"
	// DefaultSeed is the deterministic v1 seed used when callers do not supply a
	// workload-specific seed. It intentionally differs from rabitq_1bit.
	DefaultSeed uint64 = 0x6272713162697401
)

var (
	// ErrInvalidConfig reports an impossible or unsupported codec shape.
	ErrInvalidConfig = errors.New("brq: invalid config")
	// ErrDimensionMismatch reports vector/query/code shape mismatch.
	ErrDimensionMismatch = errors.New("brq: dimension mismatch")
	// ErrDegenerateVector reports zero, non-finite, or otherwise unencodable input.
	ErrDegenerateVector = errors.New("brq: degenerate vector")
)

// Config is the v1 codec configuration identity. Vector dimensions are schema
// shape rather than serialized config; NewPlan binds Config to dimensions.
type Config struct {
	Seed uint64
}

// DefaultConfig returns the stable TreeDB brq_1bit v1 config.
func DefaultConfig() Config { return Config{Seed: DefaultSeed} }

// CanonicalBytes returns the stable byte identity used for manifest config
// comparison and hashing. The format is line-oriented ASCII by design so docs,
// tests, and future non-Go implementations can reproduce it exactly.
func (c Config) CanonicalBytes() []byte {
	return []byte(fmt.Sprintf("codec=%s\nversion=%d\nmetric=cosine\nnormalization=unit_l2\nrotation=%s\nseed=0x%016x\nstorage_role=%s\nstorage_logical_type=%s\nstorage_encoding=%s\nbit_order=%s\nword_order=%s\npadding=%s\ncode_width_bits=%d\nquery_weight_bits=%d\nquery_weight_quantizer=%s\nscore=%s\ndata_scale_side_array=%s\n", CodecName, CodecVersion, RotationName, c.Seed, StorageRole, StorageLogicalType, StorageEncoding, BitOrder, WordOrder, Padding, CodeWidthBits, QueryWeightBits, QueryWeightQuantizer, ScoreLabel, DataScaleSideArray))
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

// Workspace holds reusable scratch for Encode and EncodeQuery. Query values
// returned by EncodeQuery alias query-specific workspace buffers and remain
// valid until the next EncodeQuery call using the same Workspace.
type Workspace struct {
	work         []float64
	queryBits    []byte
	queryWeights []uint8
	posQ1        []byte
	posQ2        []byte
	posQ4        []byte
	posQ8        []byte
	negQ1        []byte
	negQ2        []byte
	negQ4        []byte
	negQ8        []byte
}

// EncodedVector contains the data-code row and side-array values produced by
// Encode. Code is a packed_bit_vector row using TreeDB LSB-first layout.
type EncodedVector struct {
	Code                   []byte
	CodeCount              uint32
	QuantizedDotProductInv float32
}

// Query contains the runtime brq_1bit query representation. SignBits and all
// q-planes use TreeDB LSB0 packed rows. Weights stores one uint4 value per
// logical code dimension for validation and slow oracle scoring.
type Query struct {
	SignBits             []byte
	Weights              []uint8
	PosQ1                []byte
	PosQ2                []byte
	PosQ4                []byte
	PosQ8                []byte
	NegQ1                []byte
	NegQ2                []byte
	NegQ4                []byte
	NegQ8                []byte
	QueryWeightScale     float64
	QueryWeightSumInt    uint32
	NegativeWeightSumInt uint32
	CodeDimensions       int
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
	var l1 float64
	for i, value := range rotated[:p.codeDimensions] {
		if value >= 0 {
			code[i>>3] |= byte(1 << uint(i&7))
			count++
		}
		l1 += math.Abs(value)
	}
	if l1 <= 0 || math.IsNaN(l1) || math.IsInf(l1, 0) {
		return EncodedVector{}, fmt.Errorf("%w: rotated L1 norm is not positive", ErrDegenerateVector)
	}
	return EncodedVector{Code: code, CodeCount: count, QuantizedDotProductInv: float32(1 / l1)}, nil
}

// EncodeQuery normalizes and rotates query, quantizes absolute rotated values to
// uint4 weights, then prepares sign bits and positive/negative q1/q2/q4/q8
// bit-planes consumed by ScoreCosine. Returned slices alias ws.
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
	ws.queryWeights = ensureUint8Len(ws.queryWeights, p.codeDimensions)
	ws.posQ1 = ensureByteLen(ws.posQ1, p.bytesPerCode)
	ws.posQ2 = ensureByteLen(ws.posQ2, p.bytesPerCode)
	ws.posQ4 = ensureByteLen(ws.posQ4, p.bytesPerCode)
	ws.posQ8 = ensureByteLen(ws.posQ8, p.bytesPerCode)
	ws.negQ1 = ensureByteLen(ws.negQ1, p.bytesPerCode)
	ws.negQ2 = ensureByteLen(ws.negQ2, p.bytesPerCode)
	ws.negQ4 = ensureByteLen(ws.negQ4, p.bytesPerCode)
	ws.negQ8 = ensureByteLen(ws.negQ8, p.bytesPerCode)
	clear(ws.queryBits)
	clear(ws.queryWeights)
	clear(ws.posQ1)
	clear(ws.posQ2)
	clear(ws.posQ4)
	clear(ws.posQ8)
	clear(ws.negQ1)
	clear(ws.negQ2)
	clear(ws.negQ4)
	clear(ws.negQ8)

	var maxAbs float64
	for _, value := range rotated[:p.codeDimensions] {
		abs := math.Abs(value)
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	if maxAbs <= 0 || math.IsNaN(maxAbs) || math.IsInf(maxAbs, 0) {
		return Query{}, fmt.Errorf("%w: query max_abs is not positive", ErrDegenerateVector)
	}

	var weightSum uint32
	var negativeWeightSum uint32
	for i, value := range rotated[:p.codeDimensions] {
		positive := value >= 0
		if positive {
			ws.queryBits[i>>3] |= byte(1 << uint(i&7))
		}
		weight := quantizeUint4(math.Abs(value), maxAbs)
		ws.queryWeights[i] = weight
		weightSum += uint32(weight)
		if !positive {
			negativeWeightSum += uint32(weight)
		}
		setQueryPlaneBit(i, positive, weight, ws)
	}
	if weightSum == 0 {
		return Query{}, fmt.Errorf("%w: query uint4 weights are not positive", ErrDegenerateVector)
	}
	return Query{
		SignBits:             ws.queryBits,
		Weights:              ws.queryWeights,
		PosQ1:                ws.posQ1,
		PosQ2:                ws.posQ2,
		PosQ4:                ws.posQ4,
		PosQ8:                ws.posQ8,
		NegQ1:                ws.negQ1,
		NegQ2:                ws.negQ2,
		NegQ4:                ws.negQ4,
		NegQ8:                ws.negQ8,
		QueryWeightScale:     maxAbs / 15,
		QueryWeightSumInt:    weightSum,
		NegativeWeightSumInt: negativeWeightSum,
		CodeDimensions:       p.codeDimensions,
	}, nil
}

// ScoreCosine validates side-array inputs and returns the v1 estimated cosine
// score for one encoded data vector. This is the reference bit-product oracle;
// accelerated implementations may skip repeated validation only after matching
// this result bit-for-bit under golden tests.
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
	return p.scoreCosineValidated(query, code, quantizedDotProductInv), nil
}

// ScoreEncoded is a convenience wrapper around ScoreCosine.
func (p *Plan) ScoreEncoded(query Query, encoded EncodedVector) (float64, error) {
	return p.ScoreCosine(query, encoded.Code, encoded.CodeCount, encoded.QuantizedDotProductInv)
}

// ScoreCosineSlow validates inputs and evaluates the same score by walking one
// logical dimension at a time. It is useful for parity tests of fused kernels.
func (p *Plan) ScoreCosineSlow(query Query, code []byte, codeCount uint32, quantizedDotProductInv float32) (float64, error) {
	if err := p.ValidateCode(code, codeCount); err != nil {
		return 0, err
	}
	if err := p.ValidateQuery(query); err != nil {
		return 0, err
	}
	if err := p.ValidateQuantizedDotProductInv(quantizedDotProductInv); err != nil {
		return 0, err
	}
	var matchWeight uint32
	for i, weight8 := range query.Weights[:p.codeDimensions] {
		weight := uint32(weight8)
		if bitAt(query.SignBits, i) == bitAt(code, i) {
			matchWeight += weight
		}
	}
	signedWeight := int64(2*matchWeight) - int64(query.QueryWeightSumInt)
	return float64(signedWeight) * float64(query.QueryWeightScale) / (float64(quantizedDotProductInv) * float64(p.codeDimensions)), nil
}

// BitProduct evaluates the weighted bit-product formula over q1/q2/q4/q8 masks.
func (p *Plan) BitProduct(code, q1, q2, q4, q8 []byte) (uint32, error) {
	if p == nil || p.codeDimensions <= 0 || p.bytesPerCode <= 0 {
		return 0, fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	for _, input := range []struct {
		name string
		row  []byte
	}{
		{"code", code}, {"q1", q1}, {"q2", q2}, {"q4", q4}, {"q8", q8},
	} {
		if len(input.row) != p.bytesPerCode {
			return 0, fmt.Errorf("%w: %s bytes=%d want %d", ErrDimensionMismatch, input.name, len(input.row), p.bytesPerCode)
		}
		if err := validatePadding(input.row, p.codeDimensions); err != nil {
			return 0, fmt.Errorf("%w: %s padding: %v", ErrDimensionMismatch, input.name, err)
		}
	}
	return bitProductNoValidate(code, q1, q2, q4, q8), nil
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

// ValidateQuery verifies query shape, sign-bit padding, uint4 weights, finite
// positive scale, integer sums, and exact q1/q2/q4/q8 plane consistency.
func (p *Plan) ValidateQuery(q Query) error {
	if p == nil || p.codeDimensions <= 0 || p.bytesPerCode <= 0 {
		return fmt.Errorf("%w: nil or invalid plan", ErrInvalidConfig)
	}
	if q.CodeDimensions != p.codeDimensions {
		return fmt.Errorf("%w: query code_dimensions=%d want %d", ErrDimensionMismatch, q.CodeDimensions, p.codeDimensions)
	}
	if err := p.validatePlane("sign_bits", q.SignBits); err != nil {
		return err
	}
	planes := []struct {
		name string
		row  []byte
	}{
		{"pos_q1", q.PosQ1}, {"pos_q2", q.PosQ2}, {"pos_q4", q.PosQ4}, {"pos_q8", q.PosQ8},
		{"neg_q1", q.NegQ1}, {"neg_q2", q.NegQ2}, {"neg_q4", q.NegQ4}, {"neg_q8", q.NegQ8},
	}
	for _, plane := range planes {
		if err := p.validatePlane(plane.name, plane.row); err != nil {
			return err
		}
	}
	if len(q.Weights) != p.codeDimensions {
		return fmt.Errorf("%w: query weights=%d want %d", ErrDimensionMismatch, len(q.Weights), p.codeDimensions)
	}
	if q.QueryWeightScale <= 0 || math.IsNaN(q.QueryWeightScale) || math.IsInf(q.QueryWeightScale, 0) {
		return fmt.Errorf("%w: invalid query_weight_scale=%v", ErrDegenerateVector, q.QueryWeightScale)
	}
	minScale := p.invSqrtCodeDims / 15
	maxScale := 1.0 / 15.0
	const scaleTolerance = 1e-12
	if q.QueryWeightScale < minScale-scaleTolerance || q.QueryWeightScale > maxScale+scaleTolerance {
		return fmt.Errorf("%w: query_weight_scale=%v outside valid range [%v,%v]", ErrDegenerateVector, q.QueryWeightScale, minScale, maxScale)
	}
	var sum uint32
	var negSum uint32
	for i, weight := range q.Weights {
		if weight > 15 {
			return fmt.Errorf("%w: query weight[%d]=%d exceeds uint4", ErrDimensionMismatch, i, weight)
		}
		sum += uint32(weight)
		positive := bitAt(q.SignBits, i)
		if !positive {
			negSum += uint32(weight)
		}
		if err := validateQueryPlaneBitsAt(q, i, positive, weight); err != nil {
			return err
		}
	}
	if sum == 0 {
		return fmt.Errorf("%w: query weights are not positive", ErrDegenerateVector)
	}
	if q.QueryWeightSumInt != sum {
		return fmt.Errorf("%w: query_weight_sum_int=%d want %d", ErrDimensionMismatch, q.QueryWeightSumInt, sum)
	}
	if q.NegativeWeightSumInt != negSum {
		return fmt.Errorf("%w: negative_weight_sum_int=%d want %d", ErrDimensionMismatch, q.NegativeWeightSumInt, negSum)
	}
	return nil
}

// ValidFor reports whether q can be used with p without allocation or shape
// adaptation.
func (q Query) ValidFor(p *Plan) bool {
	return p != nil && p.ValidateQuery(q) == nil
}

func (p *Plan) scoreCosineValidated(query Query, code []byte, quantizedDotProductInv float32) float64 {
	posSet := bitProductNoValidate(code, query.PosQ1, query.PosQ2, query.PosQ4, query.PosQ8)
	negSet := bitProductNoValidate(code, query.NegQ1, query.NegQ2, query.NegQ4, query.NegQ8)
	matchWeight := posSet + (query.NegativeWeightSumInt - negSet)
	signedWeight := int64(2*matchWeight) - int64(query.QueryWeightSumInt)
	return float64(signedWeight) * float64(query.QueryWeightScale) / (float64(quantizedDotProductInv) * float64(p.codeDimensions))
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

func quantizeUint4(abs, maxAbs float64) uint8 {
	raw := abs * 15 / maxAbs
	w := math.Floor(raw + 0.5)
	if w < 0 {
		return 0
	}
	if w > 15 {
		return 15
	}
	return uint8(w)
}

func setQueryPlaneBit(i int, positive bool, weight uint8, ws *Workspace) {
	bit := byte(1 << uint(i&7))
	idx := i >> 3
	if positive {
		if weight&1 != 0 {
			ws.posQ1[idx] |= bit
		}
		if weight&2 != 0 {
			ws.posQ2[idx] |= bit
		}
		if weight&4 != 0 {
			ws.posQ4[idx] |= bit
		}
		if weight&8 != 0 {
			ws.posQ8[idx] |= bit
		}
		return
	}
	if weight&1 != 0 {
		ws.negQ1[idx] |= bit
	}
	if weight&2 != 0 {
		ws.negQ2[idx] |= bit
	}
	if weight&4 != 0 {
		ws.negQ4[idx] |= bit
	}
	if weight&8 != 0 {
		ws.negQ8[idx] |= bit
	}
}

func validateQueryPlaneBitsAt(q Query, i int, positive bool, weight uint8) error {
	planes := []struct {
		name string
		row  []byte
		mask uint8
		pos  bool
	}{
		{"pos_q1", q.PosQ1, 1, true}, {"pos_q2", q.PosQ2, 2, true}, {"pos_q4", q.PosQ4, 4, true}, {"pos_q8", q.PosQ8, 8, true},
		{"neg_q1", q.NegQ1, 1, false}, {"neg_q2", q.NegQ2, 2, false}, {"neg_q4", q.NegQ4, 4, false}, {"neg_q8", q.NegQ8, 8, false},
	}
	for _, plane := range planes {
		want := positive == plane.pos && weight&plane.mask != 0
		if got := bitAt(plane.row, i); got != want {
			return fmt.Errorf("%w: %s bit[%d]=%t want %t", ErrDimensionMismatch, plane.name, i, got, want)
		}
	}
	return nil
}

func bitProductNoValidate(code, q1, q2, q4, q8 []byte) uint32 {
	var total uint32
	for i, b := range code {
		total += uint32(bits.OnesCount8(b & q1[i]))
		total += 2 * uint32(bits.OnesCount8(b&q2[i]))
		total += 4 * uint32(bits.OnesCount8(b&q4[i]))
		total += 8 * uint32(bits.OnesCount8(b&q8[i]))
	}
	return total
}

func (p *Plan) validatePlane(name string, row []byte) error {
	if len(row) != p.bytesPerCode {
		return fmt.Errorf("%w: %s bytes=%d want %d", ErrDimensionMismatch, name, len(row), p.bytesPerCode)
	}
	if err := validatePadding(row, p.codeDimensions); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrDimensionMismatch, name, err)
	}
	return nil
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

func ensureUint8Len(dst []uint8, n int) []uint8 {
	if cap(dst) < n {
		return make([]uint8, n)
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
