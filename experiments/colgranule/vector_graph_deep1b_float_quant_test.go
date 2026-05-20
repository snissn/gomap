package colgranule

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

type columnVectorGraphDeep1BFloatQuantGate struct {
	id          string
	label       string
	targetK     int
	candidateK  int
	wantOverlap int
}

func columnVectorGraphDeep1BEvaluateGroundtruthFloatQuantizationTournament(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, seed int64) []columnVectorGraphDeep1BGroundtruthMethodReport {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT") != "1" {
		return nil
	}
	gates := columnVectorGraphDeep1BFloatQuantGates(rows)
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, 180)
	for bits := 1; bits <= 32; bits++ {
		out = append(out, columnVectorGraphDeep1BReportAffineFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, bits, "per_dim", "reconstructed_norm"))
		out = append(out, columnVectorGraphDeep1BReportAffineFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, bits, "per_dim", "f16_explicit_norm"))
	}
	for _, bits := range []int{1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 32} {
		out = append(out, columnVectorGraphDeep1BReportAffineFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, bits, "per_row", "reconstructed_norm"))
		out = append(out, columnVectorGraphDeep1BReportAffineFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, bits, "global", "reconstructed_norm"))
		out = append(out, columnVectorGraphDeep1BReportBlockFloatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, bits, "per_dim"))
		out = append(out, columnVectorGraphDeep1BReportBlockFloatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, bits, "per_row"))
	}
	for mantissaBits := 0; mantissaBits <= 23; mantissaBits++ {
		out = append(out, columnVectorGraphDeep1BReportMantissaFloatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, mantissaBits))
	}
	out = append(out, columnVectorGraphDeep1BReportFloatFormatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, "bf16", 16, columnVectorGraphDeep1BFloat32ToBF16RoundTrip))
	out = append(out, columnVectorGraphDeep1BReportFloatFormatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, "fp16", 16, func(value float32) float32 {
		return columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}))
	out = append(out, columnVectorGraphDeep1BReportFloatFormatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, "fp32_reference", 32, func(value float32) float32 { return value }))
	out = append(out, columnVectorGraphDeep1BReportRandomRotationFloatQuant(vectors, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, seed)...)
	out = append(out, columnVectorGraphDeep1BReportMixedDimFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, gates)...)
	out = append(out, columnVectorGraphDeep1BReportMixedRowFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, gates, 1, "mixed_row_affine")...)
	out = append(out, columnVectorGraphDeep1BReportMixedRowFloatQuant(vectors, invNorms, query, queryInvNorm, exactScores, margins, rows, dims, scanIters, gates, 4, "base_u4_row_exceptions")...)
	return out
}

func columnVectorGraphDeep1BFloatQuantGates(rows int) []columnVectorGraphDeep1BFloatQuantGate {
	cases := []columnVectorGraphDeep1BFloatQuantGate{
		{id: "top10_at20_ge9", label: "exact top10 in approx@20 >= 9/10", targetK: 10, candidateK: 20, wantOverlap: 9},
		{id: "top10_at20_full", label: "exact top10 in approx@20 = 10/10", targetK: 10, candidateK: 20, wantOverlap: 10},
		{id: "top10_at50_full", label: "exact top10 in approx@50 = 10/10", targetK: 10, candidateK: 50, wantOverlap: 10},
		{id: "top10_at100_full", label: "exact top10 in approx@100 = 10/10", targetK: 10, candidateK: 100, wantOverlap: 10},
		{id: "top20_at50_ge19", label: "exact top20 in approx@50 >= 19/20", targetK: 20, candidateK: 50, wantOverlap: 19},
		{id: "top20_at50_full", label: "exact top20 in approx@50 = 20/20", targetK: 20, candidateK: 50, wantOverlap: 20},
		{id: "top20_at100_full", label: "exact top20 in approx@100 = 20/20", targetK: 20, candidateK: 100, wantOverlap: 20},
		{id: "compressed_top10_ge9", label: "compressed final top10 >= 9/10", targetK: 10, candidateK: 10, wantOverlap: 9},
		{id: "compressed_top10_full", label: "compressed final top10 = 10/10", targetK: 10, candidateK: 10, wantOverlap: 10},
		{id: "compressed_top20_ge19", label: "compressed final top20 >= 19/20", targetK: 20, candidateK: 20, wantOverlap: 19},
		{id: "compressed_top20_full", label: "compressed final top20 = 20/20", targetK: 20, candidateK: 20, wantOverlap: 20},
	}
	out := cases[:0]
	for _, gate := range cases {
		if rows >= gate.targetK && rows >= gate.candidateK {
			out = append(out, gate)
		}
	}
	return out
}

func columnVectorGraphDeep1BReportAffineFloatQuant(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, bits int, policy string, normMode string) columnVectorGraphDeep1BGroundtruthMethodReport {
	start := time.Now()
	reconstructed, metadataBytesPerVector := columnVectorGraphDeep1BReconstructAffineFloatQuant(vectors, rows, dims, bits, policy)
	scoreInvNorms := columnVectorGraphDeep1BScoreInvNormsForMode(reconstructed, invNorms, dims, normMode)
	buildNanos := time.Since(start).Nanoseconds()
	approx := make([]float32, rows)
	columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, approx)
	if normMode == "f16_explicit_norm" {
		metadataBytesPerVector += 2
	}
	payloadBytes := float64(dims*bits) / 8
	notes := "official top100 float-value codec probe; theoretical packed payload bits are primary, metadata is reported separately and is not production-amortized; affine codes are materialized as reconstructed scratch values for this research scorer"
	method := columnVectorGraphDeep1BNewGroundtruthMethodReport("float_quant_affine", fmt.Sprintf("float_quant_affine_u%d_%s_%s", bits, policy, normMode), payloadBytes, metadataBytesPerVector, buildNanos, notes)
	method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, dst)
	})
	columnVectorGraphDeep1BSetReconstructionErrors(&method, vectors, reconstructed, rows, dims)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
	return method
}

func columnVectorGraphDeep1BReportBlockFloatQuant(vectors []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, bits int, policy string) columnVectorGraphDeep1BGroundtruthMethodReport {
	start := time.Now()
	reconstructed, metadataBytesPerVector := columnVectorGraphDeep1BReconstructBlockFloatQuant(vectors, rows, dims, bits, policy)
	scoreInvNorms := columnVectorGraphDeep1BInvNorms(reconstructed, dims)
	buildNanos := time.Since(start).Nanoseconds()
	approx := make([]float32, rows)
	columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, approx)
	payloadBytes := float64(dims*bits) / 8
	method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
		"float_quant_block_float",
		fmt.Sprintf("float_quant_block_float_u%d_%s_reconstructed_norm", bits, policy),
		payloadBytes,
		metadataBytesPerVector,
		buildNanos,
		"official top100 float-value codec probe; block-floating here means symmetric signed low-bit payloads with one shared fp32 scale per row or dimension, not a production-packed exponent format",
	)
	method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, dst)
	})
	columnVectorGraphDeep1BSetReconstructionErrors(&method, vectors, reconstructed, rows, dims)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
	return method
}

func columnVectorGraphDeep1BReportMantissaFloatQuant(vectors []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, mantissaBits int) columnVectorGraphDeep1BGroundtruthMethodReport {
	start := time.Now()
	reconstructed := make([]float32, len(vectors))
	for i, value := range vectors {
		reconstructed[i] = columnVectorGraphDeep1BTruncateFloat32Mantissa(value, mantissaBits)
	}
	scoreInvNorms := columnVectorGraphDeep1BInvNorms(reconstructed, dims)
	buildNanos := time.Since(start).Nanoseconds()
	approx := make([]float32, rows)
	columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, approx)
	payloadBitsPerValue := 9 + mantissaBits
	method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
		"float_quant_mantissa_trunc",
		fmt.Sprintf("float_quant_fp32_mantissa_m%d_reconstructed_norm", mantissaBits),
		float64(dims*payloadBitsPerValue)/8,
		0,
		buildNanos,
		"official top100 float-value codec probe; payload counts sign plus fp32 exponent plus retained mantissa bits, with lower mantissa bits zeroed",
	)
	method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, dst)
	})
	columnVectorGraphDeep1BSetReconstructionErrors(&method, vectors, reconstructed, rows, dims)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
	return method
}

func columnVectorGraphDeep1BReportFloatFormatQuant(vectors []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, name string, payloadBitsPerValue int, roundTrip func(float32) float32) columnVectorGraphDeep1BGroundtruthMethodReport {
	start := time.Now()
	reconstructed := make([]float32, len(vectors))
	for i, value := range vectors {
		reconstructed[i] = roundTrip(value)
	}
	scoreInvNorms := columnVectorGraphDeep1BInvNorms(reconstructed, dims)
	buildNanos := time.Since(start).Nanoseconds()
	approx := make([]float32, rows)
	columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, approx)
	method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
		"float_quant_float_format",
		"float_quant_"+name+"_reconstructed_norm",
		float64(dims*payloadBitsPerValue)/8,
		0,
		buildNanos,
		"official top100 float-value codec probe; standalone floating-point format lane used as the original-fidelity ceiling comparison",
	)
	method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
		columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, query, queryInvNorm, rows, dims, dst)
	})
	columnVectorGraphDeep1BSetReconstructionErrors(&method, vectors, reconstructed, rows, dims)
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
	return method
}

func columnVectorGraphDeep1BReportRandomRotationFloatQuant(vectors []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, seed int64) []columnVectorGraphDeep1BGroundtruthMethodReport {
	rotationStart := time.Now()
	rotation := columnVectorGraphDeep1BRandomOrthogonalMatrix(dims, 0x7107_5eed+seed)
	rotatedVectors := columnVectorGraphDeep1BApplyRotation(vectors, rows, dims, rotation)
	rotatedQuery := columnVectorGraphDeep1BApplyRotation(query, 1, dims, rotation)
	rotationBuildNanos := time.Since(rotationStart).Nanoseconds()
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, 8)
	for _, bits := range []int{1, 2, 4, 8, 16, 32} {
		start := time.Now()
		reconstructed, metadataBytesPerVector := columnVectorGraphDeep1BReconstructAffineFloatQuant(rotatedVectors, rows, dims, bits, "per_dim")
		scoreInvNorms := columnVectorGraphDeep1BInvNorms(reconstructed, dims)
		buildNanos := rotationBuildNanos + time.Since(start).Nanoseconds()
		approx := make([]float32, rows)
		columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, rotatedQuery, queryInvNorm, rows, dims, approx)
		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
			"float_quant_random_rotation_affine",
			fmt.Sprintf("float_quant_random_rotation_affine_u%d_per_dim_reconstructed_norm", bits),
			float64(dims*bits)/8,
			metadataBytesPerVector+8/float64(rows),
			buildNanos,
			"official top100 float-value codec probe; deterministic random orthogonal rotation plus per-dim affine quantization, inspired by RaBitQ/TurboQuant-style distribution smoothing but not a full implementation of either paper",
		)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			columnVectorGraphDeep1BScoreReconstructedVectorsInto(reconstructed, scoreInvNorms, rotatedQuery, queryInvNorm, rows, dims, dst)
		})
		columnVectorGraphDeep1BSetReconstructionErrors(&method, rotatedVectors, reconstructed, rows, dims)
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
		out = append(out, method)
	}
	return out
}

func columnVectorGraphDeep1BReportMixedDimFloatQuant(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, gates []columnVectorGraphDeep1BFloatQuantGate) []columnVectorGraphDeep1BGroundtruthMethodReport {
	if len(gates) == 0 {
		return nil
	}
	pre := columnVectorGraphDeep1BPrecomputeDimAffineContributions(vectors, invNorms, query, queryInvNorm, rows, dims)
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, len(gates))
	for _, gate := range gates {
		start := time.Now()
		bitsByDim, approx := columnVectorGraphDeep1BGreedyMixedDimBitPlan(pre, exactScores, gate, rows, dims)
		buildNanos := time.Since(start).Nanoseconds()
		payloadBytes := float64(columnVectorGraphDeep1BSumInts(bitsByDim)) / 8
		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
			"float_quant_mixed_dim_precision",
			"float_quant_mixed_dim_affine_"+gate.id,
			payloadBytes,
			float64(dims*8+dims)/float64(rows),
			buildNanos,
			"official top100 float-value codec probe; greedy per-dimension bit plan optimizes boundary-weighted score error until the named recall gate passes or fp32-equivalent 32-bit integer precision is reached; scoring uses exact row norms to isolate value quantization",
		)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			copy(dst[:rows], approx)
		})
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
		out = append(out, method)
	}
	return out
}

func columnVectorGraphDeep1BReportMixedRowFloatQuant(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, rows int, dims int, scanIters int, gates []columnVectorGraphDeep1BFloatQuantGate, startBits int, family string) []columnVectorGraphDeep1BGroundtruthMethodReport {
	if len(gates) == 0 {
		return nil
	}
	pre := columnVectorGraphDeep1BPrecomputeRowAffineScores(vectors, invNorms, query, queryInvNorm, rows, dims)
	out := make([]columnVectorGraphDeep1BGroundtruthMethodReport, 0, len(gates))
	for _, gate := range gates {
		start := time.Now()
		bitsByRow, approx := columnVectorGraphDeep1BGreedyMixedRowBitPlan(pre, exactScores, gate, rows, startBits)
		buildNanos := time.Since(start).Nanoseconds()
		payloadBytes := float64(dims*columnVectorGraphDeep1BSumInts(bitsByRow)) / (8 * float64(rows))
		method := columnVectorGraphDeep1BNewGroundtruthMethodReport(
			"float_quant_"+family,
			"float_quant_"+family+"_"+gate.id,
			payloadBytes,
			8+float64(rows)/float64(rows),
			buildNanos,
			"official top100 float-value codec probe; greedy row-adaptive precision class optimizes boundary-weighted score error until the named recall gate passes or 32-bit integer precision is reached; scoring uses exact row norms to isolate value quantization",
		)
		method.ScanNanosPerVector = columnVectorGraphDeep1BMeasureGroundtruthScan(rows, scanIters, func(dst []float32) {
			copy(dst[:rows], approx)
		})
		columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, approx, margins)
		out = append(out, method)
	}
	return out
}

func columnVectorGraphDeep1BReconstructAffineFloatQuant(vectors []float32, rows int, dims int, bits int, policy string) ([]float32, float64) {
	reconstructed := make([]float32, len(vectors))
	switch policy {
	case "per_dim":
		mins, scales := columnVectorGraphDeep1BAffineParamsPerDim(vectors, rows, dims, bits)
		for row := 0; row < rows; row++ {
			for j := 0; j < dims; j++ {
				reconstructed[row*dims+j] = columnVectorGraphDeep1BQuantizeAffineValue(vectors[row*dims+j], mins[j], scales[j], bits)
			}
		}
		return reconstructed, float64(dims*8) / float64(rows)
	case "per_row":
		for row := 0; row < rows; row++ {
			base := row * dims
			minValue := vectors[base]
			maxValue := vectors[base]
			for j := 1; j < dims; j++ {
				value := vectors[base+j]
				if value < minValue {
					minValue = value
				}
				if value > maxValue {
					maxValue = value
				}
			}
			scale := columnVectorGraphDeep1BQuantRangeFloat64(minValue, maxValue, bits)
			for j := 0; j < dims; j++ {
				reconstructed[base+j] = columnVectorGraphDeep1BQuantizeAffineValue(vectors[base+j], minValue, scale, bits)
			}
		}
		return reconstructed, 8
	case "global":
		minValue := vectors[0]
		maxValue := vectors[0]
		for _, value := range vectors[1:] {
			if value < minValue {
				minValue = value
			}
			if value > maxValue {
				maxValue = value
			}
		}
		scale := columnVectorGraphDeep1BQuantRangeFloat64(minValue, maxValue, bits)
		for i, value := range vectors {
			reconstructed[i] = columnVectorGraphDeep1BQuantizeAffineValue(value, minValue, scale, bits)
		}
		return reconstructed, 8 / float64(rows)
	default:
		panic(fmt.Sprintf("unknown affine quant policy %q", policy))
	}
}

func columnVectorGraphDeep1BReconstructBlockFloatQuant(vectors []float32, rows int, dims int, bits int, policy string) ([]float32, float64) {
	reconstructed := make([]float32, len(vectors))
	if bits <= 1 {
		switch policy {
		case "per_dim":
			for j := 0; j < dims; j++ {
				var sumAbs float64
				for row := 0; row < rows; row++ {
					sumAbs += math.Abs(float64(vectors[row*dims+j]))
				}
				scale := columnVectorGraphDeep1BNonZeroScale(float32(sumAbs / float64(rows)))
				for row := 0; row < rows; row++ {
					if vectors[row*dims+j] >= 0 {
						reconstructed[row*dims+j] = scale
					} else {
						reconstructed[row*dims+j] = -scale
					}
				}
			}
			return reconstructed, float64(dims*4) / float64(rows)
		case "per_row":
			for row := 0; row < rows; row++ {
				base := row * dims
				var sumAbs float64
				for j := 0; j < dims; j++ {
					sumAbs += math.Abs(float64(vectors[base+j]))
				}
				scale := columnVectorGraphDeep1BNonZeroScale(float32(sumAbs / float64(dims)))
				for j := 0; j < dims; j++ {
					if vectors[base+j] >= 0 {
						reconstructed[base+j] = scale
					} else {
						reconstructed[base+j] = -scale
					}
				}
			}
			return reconstructed, 4
		default:
			panic(fmt.Sprintf("unknown block float policy %q", policy))
		}
	}
	maxCode := math.Ldexp(1, bits-1) - 1
	switch policy {
	case "per_dim":
		for j := 0; j < dims; j++ {
			var maxAbs float64
			for row := 0; row < rows; row++ {
				absValue := math.Abs(float64(vectors[row*dims+j]))
				if absValue > maxAbs {
					maxAbs = absValue
				}
			}
			scale := columnVectorGraphDeep1BNonZeroScale(float32(maxAbs / maxCode))
			for row := 0; row < rows; row++ {
				reconstructed[row*dims+j] = columnVectorGraphDeep1BQuantizeSymmetricValue(vectors[row*dims+j], scale, maxCode)
			}
		}
		return reconstructed, float64(dims*4) / float64(rows)
	case "per_row":
		for row := 0; row < rows; row++ {
			base := row * dims
			var maxAbs float64
			for j := 0; j < dims; j++ {
				absValue := math.Abs(float64(vectors[base+j]))
				if absValue > maxAbs {
					maxAbs = absValue
				}
			}
			scale := columnVectorGraphDeep1BNonZeroScale(float32(maxAbs / maxCode))
			for j := 0; j < dims; j++ {
				reconstructed[base+j] = columnVectorGraphDeep1BQuantizeSymmetricValue(vectors[base+j], scale, maxCode)
			}
		}
		return reconstructed, 4
	default:
		panic(fmt.Sprintf("unknown block float policy %q", policy))
	}
}

func columnVectorGraphDeep1BAffineParamsPerDim(vectors []float32, rows int, dims int, bits int) ([]float32, []float32) {
	mins := make([]float32, dims)
	scales := make([]float32, dims)
	for j := 0; j < dims; j++ {
		minValue := vectors[j]
		maxValue := vectors[j]
		for row := 1; row < rows; row++ {
			value := vectors[row*dims+j]
			if value < minValue {
				minValue = value
			}
			if value > maxValue {
				maxValue = value
			}
		}
		mins[j] = minValue
		scales[j] = columnVectorGraphDeep1BQuantRangeFloat64(minValue, maxValue, bits)
	}
	return mins, scales
}

func columnVectorGraphDeep1BQuantRangeFloat64(minValue float32, maxValue float32, bits int) float32 {
	if bits <= 0 {
		return 1
	}
	maxCode := math.Ldexp(1, bits) - 1
	if maxCode <= 0 {
		return 1
	}
	scale := float64(maxValue-minValue) / maxCode
	if scale == 0 || math.IsNaN(scale) || math.IsInf(scale, 0) {
		return 1
	}
	return float32(scale)
}

func columnVectorGraphDeep1BQuantizeAffineValue(value float32, minValue float32, scale float32, bits int) float32 {
	if bits >= 32 {
		return value
	}
	maxCode := math.Ldexp(1, bits) - 1
	if maxCode <= 0 || scale == 0 {
		return minValue
	}
	code := math.Round(float64((value - minValue) / scale))
	if code < 0 {
		code = 0
	} else if code > maxCode {
		code = maxCode
	}
	return float32(float64(minValue) + code*float64(scale))
}

func columnVectorGraphDeep1BQuantizeSymmetricValue(value float32, scale float32, maxCode float64) float32 {
	if maxCode <= 0 || scale == 0 {
		if value >= 0 {
			return scale
		}
		return -scale
	}
	code := math.Round(float64(value / scale))
	if code < -maxCode {
		code = -maxCode
	} else if code > maxCode {
		code = maxCode
	}
	return float32(code * float64(scale))
}

func columnVectorGraphDeep1BScoreInvNormsForMode(reconstructed []float32, originalInvNorms []float32, dims int, normMode string) []float32 {
	if normMode != "f16_explicit_norm" {
		return columnVectorGraphDeep1BInvNorms(reconstructed, dims)
	}
	scoreInvNorms := make([]float32, len(originalInvNorms))
	for row := range originalInvNorms {
		scoreInvNorms[row] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(originalInvNorms[row]))
	}
	return scoreInvNorms
}

func columnVectorGraphDeep1BScoreReconstructedVectorsInto(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, rows int, dims int, scores []float32) {
	if len(scores) < rows {
		panic(fmt.Sprintf("reconstructed score dst len=%d want at least %d", len(scores), rows))
	}
	for row := 0; row < rows; row++ {
		base := row * dims
		var dot float32
		for j := 0; j < dims; j++ {
			dot += query[j] * vectors[base+j]
		}
		scores[row] = dot * queryInvNorm * invNorms[row]
	}
}

func columnVectorGraphDeep1BSetReconstructionErrors(method *columnVectorGraphDeep1BGroundtruthMethodReport, original []float32, reconstructed []float32, rows int, dims int) {
	var relSum float64
	var maxRel float64
	for row := 0; row < rows; row++ {
		base := row * dims
		var errSquared float64
		var normSquared float64
		for j := 0; j < dims; j++ {
			diff := float64(original[base+j] - reconstructed[base+j])
			errSquared += diff * diff
			value := float64(original[base+j])
			normSquared += value * value
		}
		var rel float64
		if normSquared > 0 {
			rel = math.Sqrt(errSquared / normSquared)
		}
		relSum += rel
		if rel > maxRel {
			maxRel = rel
		}
	}
	method.MeanRelativeL2 = relSum / float64(rows)
	method.MaxRelativeL2 = maxRel
}

func columnVectorGraphDeep1BTruncateFloat32Mantissa(value float32, keepMantissaBits int) float32 {
	if keepMantissaBits >= 23 {
		return value
	}
	if keepMantissaBits < 0 {
		keepMantissaBits = 0
	}
	bits := math.Float32bits(value)
	exponent := bits & 0x7f800000
	if exponent == 0x7f800000 || value == 0 {
		return value
	}
	drop := uint(23 - keepMantissaBits)
	mask := ^uint32((uint32(1) << drop) - 1)
	return math.Float32frombits(bits & mask)
}

func columnVectorGraphDeep1BFloat32ToBF16RoundTrip(value float32) float32 {
	bits := math.Float32bits(value)
	round := uint32(0x7fff) + ((bits >> 16) & 1)
	return math.Float32frombits((bits + round) & 0xffff0000)
}

type columnVectorGraphDeep1BDimAffinePrecompute struct {
	contrib []float32
}

func columnVectorGraphDeep1BPrecomputeDimAffineContributions(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, rows int, dims int) columnVectorGraphDeep1BDimAffinePrecompute {
	contrib := make([]float32, dims*33*rows)
	for bits := 1; bits <= 32; bits++ {
		mins, scales := columnVectorGraphDeep1BAffineParamsPerDim(vectors, rows, dims, bits)
		for j := 0; j < dims; j++ {
			offset := (j*33 + bits) * rows
			scoreScale := query[j] * queryInvNorm
			for row := 0; row < rows; row++ {
				reconstructed := columnVectorGraphDeep1BQuantizeAffineValue(vectors[row*dims+j], mins[j], scales[j], bits)
				contrib[offset+row] = reconstructed * scoreScale * invNorms[row]
			}
		}
	}
	return columnVectorGraphDeep1BDimAffinePrecompute{contrib: contrib}
}

func columnVectorGraphDeep1BGreedyMixedDimBitPlan(pre columnVectorGraphDeep1BDimAffinePrecompute, exactScores []float32, gate columnVectorGraphDeep1BFloatQuantGate, rows int, dims int) ([]int, []float32) {
	bitsByDim := make([]int, dims)
	for j := range bitsByDim {
		bitsByDim[j] = 1
	}
	scores := make([]float32, rows)
	for j := 0; j < dims; j++ {
		base := (j*33 + 1) * rows
		for row := 0; row < rows; row++ {
			scores[row] += pre.contrib[base+row]
		}
	}
	weights := columnVectorGraphDeep1BBoundaryWeights(exactScores, gate)
	for !columnVectorGraphDeep1BFloatQuantGatePasses(exactScores, scores, gate) {
		bestDim := -1
		bestImprovement := 0.0
		for j := 0; j < dims; j++ {
			if bitsByDim[j] >= 32 {
				continue
			}
			oldBase := (j*33 + bitsByDim[j]) * rows
			newBase := (j*33 + bitsByDim[j] + 1) * rows
			improvement := 0.0
			for row := 0; row < rows; row++ {
				oldErr := math.Abs(float64(exactScores[row] - scores[row]))
				nextScore := scores[row] - pre.contrib[oldBase+row] + pre.contrib[newBase+row]
				newErr := math.Abs(float64(exactScores[row] - nextScore))
				improvement += weights[row] * (oldErr - newErr)
			}
			if improvement > bestImprovement || bestDim < 0 {
				bestImprovement = improvement
				bestDim = j
			}
		}
		if bestDim < 0 {
			break
		}
		oldBase := (bestDim*33 + bitsByDim[bestDim]) * rows
		bitsByDim[bestDim]++
		newBase := (bestDim*33 + bitsByDim[bestDim]) * rows
		for row := 0; row < rows; row++ {
			scores[row] += pre.contrib[newBase+row] - pre.contrib[oldBase+row]
		}
		if columnVectorGraphDeep1BAllIntsAtLeast(bitsByDim, 32) {
			break
		}
	}
	return bitsByDim, scores
}

type columnVectorGraphDeep1BRowAffinePrecompute struct {
	scores []float32
}

func columnVectorGraphDeep1BPrecomputeRowAffineScores(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, rows int, dims int) columnVectorGraphDeep1BRowAffinePrecompute {
	scores := make([]float32, rows*33)
	for row := 0; row < rows; row++ {
		base := row * dims
		minValue := vectors[base]
		maxValue := vectors[base]
		for j := 1; j < dims; j++ {
			value := vectors[base+j]
			if value < minValue {
				minValue = value
			}
			if value > maxValue {
				maxValue = value
			}
		}
		for bits := 1; bits <= 32; bits++ {
			scale := columnVectorGraphDeep1BQuantRangeFloat64(minValue, maxValue, bits)
			var dot float32
			for j := 0; j < dims; j++ {
				reconstructed := columnVectorGraphDeep1BQuantizeAffineValue(vectors[base+j], minValue, scale, bits)
				dot += query[j] * reconstructed
			}
			scores[row*33+bits] = dot * queryInvNorm * invNorms[row]
		}
	}
	return columnVectorGraphDeep1BRowAffinePrecompute{scores: scores}
}

func columnVectorGraphDeep1BGreedyMixedRowBitPlan(pre columnVectorGraphDeep1BRowAffinePrecompute, exactScores []float32, gate columnVectorGraphDeep1BFloatQuantGate, rows int, startBits int) ([]int, []float32) {
	if startBits < 1 {
		startBits = 1
	}
	if startBits > 32 {
		startBits = 32
	}
	bitsByRow := make([]int, rows)
	scores := make([]float32, rows)
	for row := 0; row < rows; row++ {
		bitsByRow[row] = startBits
		scores[row] = pre.scores[row*33+startBits]
	}
	weights := columnVectorGraphDeep1BBoundaryWeights(exactScores, gate)
	for !columnVectorGraphDeep1BFloatQuantGatePasses(exactScores, scores, gate) {
		bestRow := -1
		bestPriority := 0.0
		for row := 0; row < rows; row++ {
			if bitsByRow[row] >= 32 {
				continue
			}
			oldErr := math.Abs(float64(exactScores[row] - scores[row]))
			nextScore := pre.scores[row*33+bitsByRow[row]+1]
			newErr := math.Abs(float64(exactScores[row] - nextScore))
			priority := weights[row] * (oldErr - newErr)
			if priority > bestPriority || bestRow < 0 {
				bestPriority = priority
				bestRow = row
			}
		}
		if bestRow < 0 {
			break
		}
		bitsByRow[bestRow]++
		scores[bestRow] = pre.scores[bestRow*33+bitsByRow[bestRow]]
		if columnVectorGraphDeep1BAllIntsAtLeast(bitsByRow, 32) {
			break
		}
	}
	return bitsByRow, scores
}

func columnVectorGraphDeep1BBoundaryWeights(exactScores []float32, gate columnVectorGraphDeep1BFloatQuantGate) []float64 {
	weights := make([]float64, len(exactScores))
	for i := range weights {
		weights[i] = 1
	}
	order := columnVectorGraphDeep1BScoreOrderDesc(exactScores)
	for rank, row := range order {
		switch {
		case rank < gate.targetK:
			weights[row] = 8
		case rank < gate.candidateK:
			weights[row] = 4
		case rank < min(len(order), max(gate.candidateK*2, gate.targetK+10)):
			weights[row] = 2
		}
	}
	return weights
}

func columnVectorGraphDeep1BFloatQuantGatePasses(exactScores []float32, approximate []float32, gate columnVectorGraphDeep1BFloatQuantGate) bool {
	overlap, _ := columnVectorGraphDeep1BCandidateRecall(exactScores, approximate, gate.targetK, gate.candidateK)
	return overlap >= gate.wantOverlap
}

func columnVectorGraphDeep1BSumInts(values []int) int {
	var sum int
	for _, value := range values {
		sum += value
	}
	return sum
}

func columnVectorGraphDeep1BAllIntsAtLeast(values []int, target int) bool {
	for _, value := range values {
		if value < target {
			return false
		}
	}
	return true
}

func columnVectorGraphDeep1BRenderFloatQuantGateSummaryMarkdown(b *strings.Builder, report columnVectorGraphDeep1BGroundtruthLocalityReport) {
	type methodGateResult struct {
		name    string
		family  string
		payload float64
		meta    float64
		count   int
		passed  int
		values  []float64
	}
	type gateSummary struct {
		label   string
		results map[string]*methodGateResult
	}
	gates := []struct {
		id     string
		label  string
		passes func(columnVectorGraphDeep1BGroundtruthMethodReport) bool
	}{
		{id: "compressed_top10_ge9", label: "compressed final top10 >= 9/10", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top10Overlap >= 9 }},
		{id: "compressed_top10_full", label: "compressed final top10 = 10/10", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top10Overlap >= 10 }},
		{id: "compressed_top20_ge19", label: "compressed final top20 >= 19/20", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top20Overlap >= 19 }},
		{id: "compressed_top20_full", label: "compressed final top20 = 20/20", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top20Overlap >= 20 }},
		{id: "top10_at20_ge9", label: "exact top10 in approx@20 >= 9/10", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top10InApproxTop20 >= 9 }},
		{id: "top10_at20_full", label: "exact top10 in approx@20 = 10/10", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top10InApproxTop20 >= 10 }},
		{id: "top10_at50_full", label: "exact top10 in approx@50 = 10/10", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top10InApproxTop50 >= 10 }},
		{id: "top10_at100_full", label: "exact top10 in approx@100 = 10/10", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top10InApproxTop100 >= 10 }},
		{id: "top20_at50_ge19", label: "exact top20 in approx@50 >= 19/20", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top20InApproxTop50 >= 19 }},
		{id: "top20_at50_full", label: "exact top20 in approx@50 = 20/20", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top20InApproxTop50 >= 20 }},
		{id: "top20_at100_full", label: "exact top20 in approx@100 = 20/20", passes: func(m columnVectorGraphDeep1BGroundtruthMethodReport) bool { return m.Top20InApproxTop100 >= 20 }},
	}
	summaries := make([]gateSummary, len(gates))
	for i, gate := range gates {
		summaries[i] = gateSummary{label: gate.label, results: make(map[string]*methodGateResult)}
		for _, q := range report.Queries {
			for _, method := range q.Methods {
				if !strings.HasPrefix(method.Family, "float_quant_") {
					continue
				}
				result := summaries[i].results[method.Name]
				if result == nil {
					result = &methodGateResult{name: method.Name, family: method.Family}
					summaries[i].results[method.Name] = result
				}
				result.count++
				result.payload += method.RowCodeBytesPerVector * 8
				result.meta += method.MetadataBytesPerVector
				if gate.passes(method) {
					result.passed++
					result.values = append(result.values, method.RowCodeBytesPerVector*8)
				}
			}
		}
	}
	hasFloatQuant := false
	for _, summary := range summaries {
		if len(summary.results) > 0 {
			hasFloatQuant = true
			break
		}
	}
	if !hasFloatQuant {
		return
	}
	fmt.Fprintf(b, "\n## Float Quantization Minimum Payload Gates\n\n")
	fmt.Fprintf(b, "This section inverts the codec tournament: for each recall gate it asks which float-value codec reaches the gate with the smallest vector payload. Payload is reported in **bits/vector** and excludes metadata by design; metadata bytes/vector are shown only as secondary accounting. These are still official top100 codec probes, not production granule claims.\n\n")
	fmt.Fprintf(b, "| Gate | Winner | Family | Payload bits/vector | Metadata B/vector | Passed queries | Failed queries |\n")
	fmt.Fprintf(b, "| --- | --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, summary := range summaries {
		var candidates []*methodGateResult
		for _, result := range summary.results {
			if result.count == 0 || result.passed != result.count {
				continue
			}
			candidates = append(candidates, result)
		}
		sort.Slice(candidates, func(i, j int) bool {
			left := candidates[i].payload / float64(candidates[i].count)
			right := candidates[j].payload / float64(candidates[j].count)
			if left != right {
				return left < right
			}
			return candidates[i].name < candidates[j].name
		})
		if len(candidates) == 0 {
			fmt.Fprintf(b, "| %s | n/a | n/a | n/a | n/a | 0 | %d |\n", summary.label, len(report.Queries))
			continue
		}
		winner := candidates[0]
		count := float64(winner.count)
		fmt.Fprintf(b, "| %s | `%s` | `%s` | %.1f | %.2f | %d | %d |\n",
			summary.label,
			winner.name,
			winner.family,
			winner.payload/count,
			winner.meta/count,
			winner.passed,
			winner.count-winner.passed,
		)
	}
	fmt.Fprintf(b, "\n### Per-Query Minimum Payload Distribution\n\n")
	fmt.Fprintf(b, "This table allows each query to choose its cheapest passing float-quant method independently. It is the adaptive lower-bound view: p50/p90/worst describe the payload needed per query before requiring one single method to pass every query.\n\n")
	fmt.Fprintf(b, "| Gate | Passing queries | Failed queries | p50 payload bits/vector | p90 payload bits/vector | Worst passing payload bits/vector |\n")
	fmt.Fprintf(b, "| --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, gate := range gates {
		var minimums []float64
		for _, q := range report.Queries {
			best := math.Inf(1)
			for _, method := range q.Methods {
				if !strings.HasPrefix(method.Family, "float_quant_") {
					continue
				}
				if !gate.passes(method) {
					continue
				}
				payload := method.RowCodeBytesPerVector * 8
				if payload < best {
					best = payload
				}
			}
			if !math.IsInf(best, 1) {
				minimums = append(minimums, best)
			}
		}
		sort.Float64s(minimums)
		if len(minimums) == 0 {
			fmt.Fprintf(b, "| %s | 0 | %d | n/a | n/a | n/a |\n", gate.label, len(report.Queries))
			continue
		}
		fmt.Fprintf(b, "| %s | %d | %d | %.1f | %.1f | %.1f |\n",
			gate.label,
			len(minimums),
			len(report.Queries)-len(minimums),
			columnVectorGraphDeep1BFloatQuantile(minimums, 0.50),
			columnVectorGraphDeep1BFloatQuantile(minimums, 0.90),
			columnVectorGraphDeep1BFloatQuantile(minimums, 1.0),
		)
	}
}
