package colgranule

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"
)

const columnVectorGraphDeep1BPQCodebookSize = 256

type columnVectorGraphDeep1BPQModel struct {
	rowCodeBytes          int
	dims                  int
	subquantizers         int
	codebookSize          int
	subStarts             []int
	subDims               []int
	centroidOffsets       []int
	centroids             []float32
	trainRows             int
	trainIterations       int
	trainNanos            int64
	codebookMetadataBytes int
	amortizeRows          int
}

type columnVectorGraphDeep1BPQEncoding struct {
	codes          []byte
	invNorms       []float32
	approxScores   []float32
	encodeNanos    int64
	meanRelativeL2 float64
	maxRelativeL2  float64
}

type columnVectorGraphDeep1BPQTrainingReport struct {
	RowCodeBytes                 int     `json:"row_code_bytes"`
	Subquantizers                int     `json:"subquantizers"`
	CodebookSize                 int     `json:"codebook_size"`
	TrainRows                    int     `json:"train_rows"`
	TrainIterations              int     `json:"train_iterations"`
	TrainNanos                   int64   `json:"train_nanos"`
	CodebookMetadataBytes        int     `json:"codebook_metadata_bytes"`
	CodebookMetadataBytesPerEval float64 `json:"codebook_metadata_bytes_per_eval_vector"`
	Notes                        string  `json:"notes,omitempty"`
}

func columnVectorGraphDeep1BFitBuildablePQModels(tb testing.TB, trainVectors []float32, rowCodeBytes []int, trainRows int, amortizeRows int, dims int, iterations int) []columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if len(rowCodeBytes) == 0 {
		return nil
	}
	if len(trainVectors) != trainRows*dims {
		tb.Fatalf("PQ train vectors=%d want=%d", len(trainVectors), trainRows*dims)
	}
	seen := make(map[int]bool, len(rowCodeBytes))
	budgets := make([]int, 0, len(rowCodeBytes))
	for _, budget := range rowCodeBytes {
		if budget <= 0 {
			tb.Fatalf("PQ row-code budget=%d must be positive", budget)
		}
		if budget > dims {
			tb.Fatalf("PQ row-code budget=%d exceeds dims=%d; this scout uses one 8-bit subcode per subquantizer", budget, dims)
		}
		if !seen[budget] {
			seen[budget] = true
			budgets = append(budgets, budget)
		}
	}
	sort.Ints(budgets)
	models := make([]columnVectorGraphDeep1BPQModel, 0, len(budgets))
	for _, budget := range budgets {
		models = append(models, columnVectorGraphDeep1BFitPQModel(tb, trainVectors, trainRows, dims, budget, iterations, amortizeRows))
	}
	return models
}

func columnVectorGraphDeep1BPQTrainingReports(models []columnVectorGraphDeep1BPQModel) []columnVectorGraphDeep1BPQTrainingReport {
	if len(models) == 0 {
		return nil
	}
	out := make([]columnVectorGraphDeep1BPQTrainingReport, 0, len(models))
	for _, model := range models {
		amortizeRows := max(1, model.amortizeRows)
		out = append(out, columnVectorGraphDeep1BPQTrainingReport{
			RowCodeBytes:                 model.rowCodeBytes,
			Subquantizers:                model.subquantizers,
			CodebookSize:                 model.codebookSize,
			TrainRows:                    model.trainRows,
			TrainIterations:              model.trainIterations,
			TrainNanos:                   model.trainNanos,
			CodebookMetadataBytes:        model.codebookMetadataBytes,
			CodebookMetadataBytesPerEval: float64(model.codebookMetadataBytes) / float64(amortizeRows),
			Notes:                        "global 8-bit PQ codebooks trained on held-out base-prefix rows and evaluated on a disjoint eval slice; this is a production/buildable codebook lane, not a top100 oracle fit",
		})
	}
	return out
}

func columnVectorGraphDeep1BFitPQModel(tb testing.TB, vectors []float32, rows int, dims int, rowCodeBytes int, iterations int, amortizeRows int) columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if rows < columnVectorGraphDeep1BPQCodebookSize {
		tb.Fatalf("PQ train rows=%d must be at least codebook size=%d", rows, columnVectorGraphDeep1BPQCodebookSize)
	}
	subStarts, subDims := columnVectorGraphDeep1BPQSubspaces(dims, rowCodeBytes)
	centroidOffsets := make([]int, rowCodeBytes)
	var centroidCount int
	for i, subDim := range subDims {
		centroidOffsets[i] = centroidCount
		centroidCount += columnVectorGraphDeep1BPQCodebookSize * subDim
	}
	centroids := make([]float32, centroidCount)
	start := time.Now()
	for sub := 0; sub < rowCodeBytes; sub++ {
		centroidOffset := centroidOffsets[sub]
		subCentroids := centroids[centroidOffset : centroidOffset+columnVectorGraphDeep1BPQCodebookSize*subDims[sub]]
		columnVectorGraphDeep1BTrainPQSubspace(vectors, rows, dims, subStarts[sub], subDims[sub], sub, iterations, subCentroids)
	}
	trainNanos := time.Since(start).Nanoseconds()
	for i, value := range centroids {
		centroids[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	return columnVectorGraphDeep1BPQModel{
		rowCodeBytes:          rowCodeBytes,
		dims:                  dims,
		subquantizers:         rowCodeBytes,
		codebookSize:          columnVectorGraphDeep1BPQCodebookSize,
		subStarts:             subStarts,
		subDims:               subDims,
		centroidOffsets:       centroidOffsets,
		centroids:             centroids,
		trainRows:             rows,
		trainIterations:       iterations,
		trainNanos:            trainNanos,
		codebookMetadataBytes: dims * columnVectorGraphDeep1BPQCodebookSize * 2,
		amortizeRows:          amortizeRows,
	}
}

func columnVectorGraphDeep1BPQSubspaces(dims int, subquantizers int) ([]int, []int) {
	subStarts := make([]int, subquantizers)
	subDims := make([]int, subquantizers)
	base := dims / subquantizers
	remainder := dims % subquantizers
	start := 0
	for sub := 0; sub < subquantizers; sub++ {
		subDim := base
		if sub < remainder {
			subDim++
		}
		subStarts[sub] = start
		subDims[sub] = subDim
		start += subDim
	}
	return subStarts, subDims
}

func columnVectorGraphDeep1BTrainPQSubspace(vectors []float32, rows int, dims int, subStart int, subDim int, subIndex int, iterations int, centroids []float32) {
	for code := 0; code < columnVectorGraphDeep1BPQCodebookSize; code++ {
		row := (code*9973 + subIndex*7919) % rows
		src := row*dims + subStart
		copy(centroids[code*subDim:(code+1)*subDim], vectors[src:src+subDim])
	}
	counts := make([]int, columnVectorGraphDeep1BPQCodebookSize)
	sums := make([]float64, columnVectorGraphDeep1BPQCodebookSize*subDim)
	for iter := 0; iter < iterations; iter++ {
		for i := range counts {
			counts[i] = 0
		}
		for i := range sums {
			sums[i] = 0
		}
		for row := 0; row < rows; row++ {
			src := vectors[row*dims+subStart : row*dims+subStart+subDim]
			bestCode := 0
			bestDistance := float32(math.MaxFloat32)
			for code := 0; code < columnVectorGraphDeep1BPQCodebookSize; code++ {
				centroid := centroids[code*subDim : (code+1)*subDim]
				var distance float32
				for j := 0; j < subDim; j++ {
					diff := src[j] - centroid[j]
					distance += diff * diff
				}
				if distance < bestDistance {
					bestDistance = distance
					bestCode = code
				}
			}
			counts[bestCode]++
			sumBase := bestCode * subDim
			for j := 0; j < subDim; j++ {
				sums[sumBase+j] += float64(src[j])
			}
		}
		for code := 0; code < columnVectorGraphDeep1BPQCodebookSize; code++ {
			centroid := centroids[code*subDim : (code+1)*subDim]
			if counts[code] == 0 {
				row := (iter*104729 + code*1543 + subIndex*313) % rows
				src := vectors[row*dims+subStart : row*dims+subStart+subDim]
				copy(centroid, src)
				continue
			}
			invCount := 1 / float64(counts[code])
			sumBase := code * subDim
			for j := 0; j < subDim; j++ {
				centroid[j] = float32(sums[sumBase+j] * invCount)
			}
		}
	}
}

func columnVectorGraphDeep1BEvaluateBuildablePQMethod(vectors []float32, invNorms []float32, query []float32, queryInvNorm float32, exactScores []float32, margins map[string]float64, selected []columnVectorGraphDeep1BBuildableGranule, builder string, model columnVectorGraphDeep1BPQModel, scanIters int) columnVectorGraphDeep1BGroundtruthMethodReport {
	dims := columnVectorGraphDeep1BDims
	rowIDs := columnVectorGraphDeep1BSelectedRowIDs(selected)
	start := time.Now()
	encoding := columnVectorGraphDeep1BEncodePQRows(vectors, invNorms, rowIDs, model, dims)
	encoding.encodeNanos = time.Since(start).Nanoseconds()
	scorer := columnVectorGraphDeep1BPreparePQScorer(model, query)
	scorer.scoreInto(encoding, queryInvNorm, len(rowIDs), encoding.approxScores)
	scanNanos := columnVectorGraphDeep1BMeasureGroundtruthScan(len(rowIDs), scanIters, func(dst []float32) {
		scorer.scoreInto(encoding, queryInvNorm, len(rowIDs), dst)
	})
	metadataBytesPerVector := 2 + float64(model.codebookMetadataBytes)/float64(max(1, model.amortizeRows))
	method := columnVectorGraphDeep1BNewCompressionMethodReport(
		"buildable_granule_scout",
		"global_pq_codebooks_amortized_over_eval_rows_plus_f16_inv_norm_per_row",
		"product_quantization",
		fmt.Sprintf("buildable_%s_global_pq_%dB_x8", builder, model.rowCodeBytes),
		float64(model.rowCodeBytes),
		metadataBytesPerVector,
		encoding.encodeNanos,
		fmt.Sprintf("production/buildable scout over %s granules; global PQ codebooks trained on %d held-out rows for %d iterations and evaluated on disjoint eval rows; codec recall is conditional on centroid-routed candidate union", builder, model.trainRows, model.trainIterations),
	)
	method.ScanNanosPerVector = scanNanos
	method.MeanRelativeL2 = encoding.meanRelativeL2
	method.MaxRelativeL2 = encoding.maxRelativeL2
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, encoding.approxScores, margins)
	return method
}

func columnVectorGraphDeep1BSelectedRowIDs(selected []columnVectorGraphDeep1BBuildableGranule) []int {
	var rows int
	for _, granule := range selected {
		rows += granule.Rows
	}
	rowIDs := make([]int, 0, rows)
	for _, granule := range selected {
		rowIDs = append(rowIDs, granule.RowIDs...)
	}
	return rowIDs
}

func columnVectorGraphDeep1BEncodePQRows(vectors []float32, invNorms []float32, rowIDs []int, model columnVectorGraphDeep1BPQModel, dims int) columnVectorGraphDeep1BPQEncoding {
	rows := len(rowIDs)
	codes := make([]byte, rows*model.subquantizers)
	invNormsStored := make([]float32, rows)
	recon := make([]float32, dims)
	var relSum float64
	var maxRel float64
	for outRow, rowID := range rowIDs {
		srcBase := rowID * dims
		for sub := 0; sub < model.subquantizers; sub++ {
			start := model.subStarts[sub]
			subDim := model.subDims[sub]
			code := columnVectorGraphDeep1BPQNearestCode(vectors[srcBase+start:srcBase+start+subDim], model, sub)
			codes[outRow*model.subquantizers+sub] = byte(code)
			centroid := columnVectorGraphDeep1BPQCentroid(model, sub, code)
			copy(recon[start:start+subDim], centroid)
		}
		invNormsStored[outRow] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(invNorms[rowID]))
		var errSquared float64
		var normSquared float64
		for j := 0; j < dims; j++ {
			diff := float64(vectors[srcBase+j] - recon[j])
			errSquared += diff * diff
			value := float64(vectors[srcBase+j])
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
	if rows == 0 {
		rows = 1
	}
	return columnVectorGraphDeep1BPQEncoding{
		codes:          codes,
		invNorms:       invNormsStored,
		approxScores:   make([]float32, len(rowIDs)),
		meanRelativeL2: relSum / float64(rows),
		maxRelativeL2:  maxRel,
	}
}

func columnVectorGraphDeep1BPQNearestCode(vector []float32, model columnVectorGraphDeep1BPQModel, sub int) int {
	subDim := model.subDims[sub]
	bestCode := 0
	bestDistance := float32(math.MaxFloat32)
	for code := 0; code < model.codebookSize; code++ {
		centroid := columnVectorGraphDeep1BPQCentroid(model, sub, code)
		var distance float32
		for j := 0; j < subDim; j++ {
			diff := vector[j] - centroid[j]
			distance += diff * diff
		}
		if distance < bestDistance {
			bestDistance = distance
			bestCode = code
		}
	}
	return bestCode
}

func columnVectorGraphDeep1BPQCentroid(model columnVectorGraphDeep1BPQModel, sub int, code int) []float32 {
	subDim := model.subDims[sub]
	offset := model.centroidOffsets[sub] + code*subDim
	return model.centroids[offset : offset+subDim]
}

type columnVectorGraphDeep1BPQScorer struct {
	table []float32
}

func columnVectorGraphDeep1BPreparePQScorer(model columnVectorGraphDeep1BPQModel, query []float32) columnVectorGraphDeep1BPQScorer {
	table := make([]float32, model.subquantizers*model.codebookSize)
	for sub := 0; sub < model.subquantizers; sub++ {
		start := model.subStarts[sub]
		subDim := model.subDims[sub]
		querySub := query[start : start+subDim]
		for code := 0; code < model.codebookSize; code++ {
			centroid := columnVectorGraphDeep1BPQCentroid(model, sub, code)
			var dot float32
			for j := 0; j < subDim; j++ {
				dot += querySub[j] * centroid[j]
			}
			table[sub*model.codebookSize+code] = dot
		}
	}
	return columnVectorGraphDeep1BPQScorer{table: table}
}

func (scorer columnVectorGraphDeep1BPQScorer) scoreInto(encoding columnVectorGraphDeep1BPQEncoding, queryInvNorm float32, rows int, scores []float32) {
	if len(scores) < rows {
		panic(fmt.Sprintf("PQ score dst len=%d want at least %d", len(scores), rows))
	}
	if rows == 0 {
		return
	}
	subquantizers := len(encoding.codes) / rows
	codebookSize := columnVectorGraphDeep1BPQCodebookSize
	for row := 0; row < rows; row++ {
		var dot float32
		codeBase := row * subquantizers
		for sub := 0; sub < subquantizers; sub++ {
			dot += scorer.table[sub*codebookSize+int(encoding.codes[codeBase+sub])]
		}
		scores[row] = dot * queryInvNorm * encoding.invNorms[row]
	}
}
