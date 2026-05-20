package colgranule

import (
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"gonum.org/v1/gonum/mat"
)

const columnVectorGraphDeep1BPQCodebookSize = 256

type columnVectorGraphDeep1BPQModel struct {
	method                string
	family                string
	rowCodeBytes          int
	dims                  int
	subquantizers         int
	codebookSize          int
	subStarts             []int
	subDims               []int
	centroidOffsets       []int
	centroids             []float32
	residualCenter        []float32
	rotation              []float32
	trainRows             int
	trainIterations       int
	opqIterations         int
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
	Method                       string  `json:"method"`
	RowCodeBytes                 int     `json:"row_code_bytes"`
	Subquantizers                int     `json:"subquantizers"`
	CodebookSize                 int     `json:"codebook_size"`
	TrainRows                    int     `json:"train_rows"`
	TrainIterations              int     `json:"train_iterations"`
	OPQIterations                int     `json:"opq_iterations,omitempty"`
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
		notes := "global 8-bit PQ codebooks trained on held-out base-prefix rows and evaluated on a disjoint eval slice; this is a production/buildable codebook lane, not a top100 oracle fit"
		switch model.method {
		case "global_opq":
			notes = "global OPQ-style learned rotation plus 8-bit PQ codebooks trained on held-out base-prefix rows and evaluated on a disjoint eval slice; this is a production/buildable codebook lane, not a top100 oracle fit"
		case "global_residual_pq":
			notes = "global centroid-residual 8-bit PQ codebooks trained on held-out base-prefix rows and evaluated on a disjoint eval slice; this is a production/buildable residual-codebook lane, not a top100 oracle fit and not full LOPQ"
		}
		out = append(out, columnVectorGraphDeep1BPQTrainingReport{
			Method:                       model.method,
			RowCodeBytes:                 model.rowCodeBytes,
			Subquantizers:                model.subquantizers,
			CodebookSize:                 model.codebookSize,
			TrainRows:                    model.trainRows,
			TrainIterations:              model.trainIterations,
			OPQIterations:                model.opqIterations,
			TrainNanos:                   model.trainNanos,
			CodebookMetadataBytes:        model.codebookMetadataBytes,
			CodebookMetadataBytesPerEval: float64(model.codebookMetadataBytes) / float64(amortizeRows),
			Notes:                        notes,
		})
	}
	return out
}

func columnVectorGraphDeep1BFitBuildableResidualPQModels(tb testing.TB, trainVectors []float32, rowCodeBytes []int, trainRows int, amortizeRows int, dims int, iterations int) []columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if len(rowCodeBytes) == 0 {
		return nil
	}
	if len(trainVectors) != trainRows*dims {
		tb.Fatalf("residual PQ train vectors=%d want=%d", len(trainVectors), trainRows*dims)
	}
	seen := make(map[int]bool, len(rowCodeBytes))
	budgets := make([]int, 0, len(rowCodeBytes))
	for _, budget := range rowCodeBytes {
		if budget <= 0 {
			tb.Fatalf("residual PQ row-code budget=%d must be positive", budget)
		}
		if budget > dims {
			tb.Fatalf("residual PQ row-code budget=%d exceeds dims=%d; this scout uses one 8-bit subcode per subquantizer", budget, dims)
		}
		if !seen[budget] {
			seen[budget] = true
			budgets = append(budgets, budget)
		}
	}
	sort.Ints(budgets)
	models := make([]columnVectorGraphDeep1BPQModel, 0, len(budgets))
	for _, budget := range budgets {
		models = append(models, columnVectorGraphDeep1BFitResidualPQModel(tb, trainVectors, trainRows, dims, budget, iterations, amortizeRows))
	}
	return models
}

func columnVectorGraphDeep1BFitBuildableOPQModels(tb testing.TB, trainVectors []float32, rowCodeBytes []int, trainRows int, amortizeRows int, dims int, pqIterations int, opqIterations int) []columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if len(rowCodeBytes) == 0 {
		return nil
	}
	if opqIterations <= 0 {
		tb.Fatalf("OPQ iterations=%d must be positive", opqIterations)
	}
	if len(trainVectors) != trainRows*dims {
		tb.Fatalf("OPQ train vectors=%d want=%d", len(trainVectors), trainRows*dims)
	}
	seen := make(map[int]bool, len(rowCodeBytes))
	budgets := make([]int, 0, len(rowCodeBytes))
	for _, budget := range rowCodeBytes {
		if budget <= 0 {
			tb.Fatalf("OPQ row-code budget=%d must be positive", budget)
		}
		if budget > dims {
			tb.Fatalf("OPQ row-code budget=%d exceeds dims=%d; this scout uses one 8-bit subcode per subquantizer", budget, dims)
		}
		if !seen[budget] {
			seen[budget] = true
			budgets = append(budgets, budget)
		}
	}
	sort.Ints(budgets)
	models := make([]columnVectorGraphDeep1BPQModel, 0, len(budgets))
	for _, budget := range budgets {
		models = append(models, columnVectorGraphDeep1BFitOPQModel(tb, trainVectors, trainRows, dims, budget, pqIterations, opqIterations, amortizeRows))
	}
	return models
}

func columnVectorGraphDeep1BFitPQModel(tb testing.TB, vectors []float32, rows int, dims int, rowCodeBytes int, iterations int, amortizeRows int) columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if rows < columnVectorGraphDeep1BPQCodebookSize {
		tb.Fatalf("PQ train rows=%d must be at least codebook size=%d", rows, columnVectorGraphDeep1BPQCodebookSize)
	}
	start := time.Now()
	subStarts, subDims, centroidOffsets, centroids := columnVectorGraphDeep1BTrainPQCentroids(tb, vectors, rows, dims, rowCodeBytes, iterations)
	trainNanos := time.Since(start).Nanoseconds()
	for i, value := range centroids {
		centroids[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	return columnVectorGraphDeep1BPQModel{
		method:                "global_pq",
		family:                "product_quantization",
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

func columnVectorGraphDeep1BFitResidualPQModel(tb testing.TB, vectors []float32, rows int, dims int, rowCodeBytes int, iterations int, amortizeRows int) columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if rows < columnVectorGraphDeep1BPQCodebookSize {
		tb.Fatalf("residual PQ train rows=%d must be at least codebook size=%d", rows, columnVectorGraphDeep1BPQCodebookSize)
	}
	start := time.Now()
	center := columnVectorGraphDeep1BMeanVector(vectors, rows, dims)
	for i, value := range center {
		center[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	residuals := columnVectorGraphDeep1BSubtractCenterRows(vectors, rows, dims, center)
	subStarts, subDims, centroidOffsets, centroids := columnVectorGraphDeep1BTrainPQCentroids(tb, residuals, rows, dims, rowCodeBytes, iterations)
	trainNanos := time.Since(start).Nanoseconds()
	for i, value := range centroids {
		centroids[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	return columnVectorGraphDeep1BPQModel{
		method:                "global_residual_pq",
		family:                "residual_product_quantization",
		rowCodeBytes:          rowCodeBytes,
		dims:                  dims,
		subquantizers:         rowCodeBytes,
		codebookSize:          columnVectorGraphDeep1BPQCodebookSize,
		subStarts:             subStarts,
		subDims:               subDims,
		centroidOffsets:       centroidOffsets,
		centroids:             centroids,
		residualCenter:        center,
		trainRows:             rows,
		trainIterations:       iterations,
		trainNanos:            trainNanos,
		codebookMetadataBytes: dims*columnVectorGraphDeep1BPQCodebookSize*2 + dims*2,
		amortizeRows:          amortizeRows,
	}
}

func columnVectorGraphDeep1BFitResidualOPQModel(tb testing.TB, vectors []float32, rows int, dims int, rowCodeBytes int, pqIterations int, opqIterations int, amortizeRows int) columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if rows < columnVectorGraphDeep1BPQCodebookSize {
		tb.Fatalf("residual OPQ train rows=%d must be at least codebook size=%d", rows, columnVectorGraphDeep1BPQCodebookSize)
	}
	start := time.Now()
	center := columnVectorGraphDeep1BMeanVector(vectors, rows, dims)
	for i, value := range center {
		center[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	residuals := columnVectorGraphDeep1BSubtractCenterRows(vectors, rows, dims, center)
	model := columnVectorGraphDeep1BFitOPQModel(tb, residuals, rows, dims, rowCodeBytes, pqIterations, opqIterations, amortizeRows)
	model.method = "global_residual_opq"
	model.family = "residual_optimized_product_quantization"
	model.residualCenter = center
	model.trainNanos = time.Since(start).Nanoseconds()
	model.codebookMetadataBytes += dims * 2
	return model
}

func columnVectorGraphDeep1BFitOPQModel(tb testing.TB, vectors []float32, rows int, dims int, rowCodeBytes int, pqIterations int, opqIterations int, amortizeRows int) columnVectorGraphDeep1BPQModel {
	tb.Helper()
	if rows < columnVectorGraphDeep1BPQCodebookSize {
		tb.Fatalf("OPQ train rows=%d must be at least codebook size=%d", rows, columnVectorGraphDeep1BPQCodebookSize)
	}
	rotation := columnVectorGraphDeep1BIdentityRotation(dims)
	var subStarts []int
	var subDims []int
	var centroidOffsets []int
	var centroids []float32
	start := time.Now()
	for iter := 0; iter < opqIterations; iter++ {
		rotated := columnVectorGraphDeep1BRotateRows(vectors, rows, dims, rotation)
		subStarts, subDims, centroidOffsets, centroids = columnVectorGraphDeep1BTrainPQCentroids(tb, rotated, rows, dims, rowCodeBytes, pqIterations)
		if iter+1 < opqIterations {
			tempModel := columnVectorGraphDeep1BPQModel{
				rowCodeBytes:    rowCodeBytes,
				dims:            dims,
				subquantizers:   rowCodeBytes,
				codebookSize:    columnVectorGraphDeep1BPQCodebookSize,
				subStarts:       subStarts,
				subDims:         subDims,
				centroidOffsets: centroidOffsets,
				centroids:       centroids,
			}
			reconstructed := columnVectorGraphDeep1BReconstructPQRows(rotated, rows, dims, tempModel)
			rotation = columnVectorGraphDeep1BOPQProcrustesRotation(tb, vectors, reconstructed, rows, dims)
		}
	}
	trainNanos := time.Since(start).Nanoseconds()
	for i, value := range centroids {
		centroids[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	for i, value := range rotation {
		rotation[i] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(value))
	}
	return columnVectorGraphDeep1BPQModel{
		method:                "global_opq",
		family:                "optimized_product_quantization",
		rowCodeBytes:          rowCodeBytes,
		dims:                  dims,
		subquantizers:         rowCodeBytes,
		codebookSize:          columnVectorGraphDeep1BPQCodebookSize,
		subStarts:             subStarts,
		subDims:               subDims,
		centroidOffsets:       centroidOffsets,
		centroids:             centroids,
		rotation:              rotation,
		trainRows:             rows,
		trainIterations:       pqIterations,
		opqIterations:         opqIterations,
		trainNanos:            trainNanos,
		codebookMetadataBytes: dims*columnVectorGraphDeep1BPQCodebookSize*2 + dims*dims*2,
		amortizeRows:          amortizeRows,
	}
}

func columnVectorGraphDeep1BMeanVector(vectors []float32, rows int, dims int) []float32 {
	center := make([]float32, dims)
	if rows == 0 {
		return center
	}
	for row := 0; row < rows; row++ {
		base := row * dims
		for j := 0; j < dims; j++ {
			center[j] += vectors[base+j]
		}
	}
	invRows := float32(1 / float64(rows))
	for j := 0; j < dims; j++ {
		center[j] *= invRows
	}
	return center
}

func columnVectorGraphDeep1BSubtractCenterRows(vectors []float32, rows int, dims int, center []float32) []float32 {
	residuals := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		for j := 0; j < dims; j++ {
			residuals[base+j] = vectors[base+j] - center[j]
		}
	}
	return residuals
}

func columnVectorGraphDeep1BTrainPQCentroids(tb testing.TB, vectors []float32, rows int, dims int, rowCodeBytes int, iterations int) ([]int, []int, []int, []float32) {
	tb.Helper()
	subStarts, subDims := columnVectorGraphDeep1BPQSubspaces(dims, rowCodeBytes)
	centroidOffsets := make([]int, rowCodeBytes)
	var centroidCount int
	for i, subDim := range subDims {
		centroidOffsets[i] = centroidCount
		centroidCount += columnVectorGraphDeep1BPQCodebookSize * subDim
	}
	centroids := make([]float32, centroidCount)
	for sub := 0; sub < rowCodeBytes; sub++ {
		centroidOffset := centroidOffsets[sub]
		subCentroids := centroids[centroidOffset : centroidOffset+columnVectorGraphDeep1BPQCodebookSize*subDims[sub]]
		columnVectorGraphDeep1BTrainPQSubspace(vectors, rows, dims, subStarts[sub], subDims[sub], sub, iterations, subCentroids)
	}
	return subStarts, subDims, centroidOffsets, centroids
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

func columnVectorGraphDeep1BIdentityRotation(dims int) []float32 {
	rotation := make([]float32, dims*dims)
	for i := 0; i < dims; i++ {
		rotation[i*dims+i] = 1
	}
	return rotation
}

func columnVectorGraphDeep1BRotateRows(vectors []float32, rows int, dims int, rotation []float32) []float32 {
	rotated := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		columnVectorGraphDeep1BRotateVectorInto(vectors[row*dims:(row+1)*dims], rotation, dims, rotated[row*dims:(row+1)*dims])
	}
	return rotated
}

func columnVectorGraphDeep1BRotateVectorInto(vector []float32, rotation []float32, dims int, dst []float32) {
	for out := 0; out < dims; out++ {
		var sum float32
		for in := 0; in < dims; in++ {
			sum += vector[in] * rotation[in*dims+out]
		}
		dst[out] = sum
	}
}

func columnVectorGraphDeep1BOPQProcrustesRotation(tb testing.TB, vectors []float32, reconstructed []float32, rows int, dims int) []float32 {
	tb.Helper()
	if len(vectors) != rows*dims || len(reconstructed) != rows*dims {
		tb.Fatalf("OPQ Procrustes shapes vectors=%d reconstructed=%d want=%d", len(vectors), len(reconstructed), rows*dims)
	}
	covariance := make([]float64, dims*dims)
	for row := 0; row < rows; row++ {
		xBase := row * dims
		yBase := row * dims
		for in := 0; in < dims; in++ {
			x := float64(vectors[xBase+in])
			dstBase := in * dims
			for out := 0; out < dims; out++ {
				covariance[dstBase+out] += x * float64(reconstructed[yBase+out])
			}
		}
	}
	var svd mat.SVD
	if ok := svd.Factorize(mat.NewDense(dims, dims, covariance), mat.SVDFull); !ok {
		tb.Fatalf("OPQ Procrustes SVD failed")
	}
	var u mat.Dense
	var v mat.Dense
	svd.UTo(&u)
	svd.VTo(&v)
	var rotation mat.Dense
	rotation.Mul(&u, v.T())
	out := make([]float32, dims*dims)
	for row := 0; row < dims; row++ {
		for col := 0; col < dims; col++ {
			out[row*dims+col] = float32(rotation.At(row, col))
		}
	}
	return out
}

func columnVectorGraphDeep1BReconstructPQRows(vectors []float32, rows int, dims int, model columnVectorGraphDeep1BPQModel) []float32 {
	reconstructed := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		srcBase := row * dims
		dstBase := row * dims
		for sub := 0; sub < model.subquantizers; sub++ {
			start := model.subStarts[sub]
			subDim := model.subDims[sub]
			code := columnVectorGraphDeep1BPQNearestCode(vectors[srcBase+start:srcBase+start+subDim], model, sub)
			copy(reconstructed[dstBase+start:dstBase+start+subDim], columnVectorGraphDeep1BPQCentroid(model, sub, code))
		}
	}
	return reconstructed
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
		columnVectorGraphDeep1BPQStorageNotes(model),
		model.family,
		fmt.Sprintf("buildable_%s_%s_%dB_x8", builder, model.method, model.rowCodeBytes),
		float64(model.rowCodeBytes),
		metadataBytesPerVector,
		encoding.encodeNanos,
		columnVectorGraphDeep1BPQMethodNotes(builder, model),
	)
	method.ScanNanosPerVector = scanNanos
	method.MeanRelativeL2 = encoding.meanRelativeL2
	method.MaxRelativeL2 = encoding.maxRelativeL2
	columnVectorGraphDeep1BSetEstimatedCandidateBytesRead(&method, len(rowIDs))
	columnVectorGraphDeep1BFillGroundtruthMethodMetrics(&method, exactScores, encoding.approxScores, margins)
	return method
}

func columnVectorGraphDeep1BPQStorageNotes(model columnVectorGraphDeep1BPQModel) string {
	if model.method == "global_opq" {
		return "global_opq_rotation_and_pq_codebooks_amortized_over_eval_rows_plus_f16_inv_norm_per_row"
	}
	if model.method == "global_residual_pq" {
		return "global_f16_centroid_and_residual_pq_codebooks_amortized_over_eval_rows_plus_f16_inv_norm_per_row"
	}
	return "global_pq_codebooks_amortized_over_eval_rows_plus_f16_inv_norm_per_row"
}

func columnVectorGraphDeep1BPQMethodNotes(builder string, model columnVectorGraphDeep1BPQModel) string {
	if model.method == "global_opq" {
		return fmt.Sprintf("production/buildable scout over %s granules; global OPQ-style rotation plus PQ codebooks trained on %d held-out rows for %d OPQ iterations and %d PQ k-means iterations, then evaluated on disjoint eval rows; codec recall is conditional on centroid-routed candidate union", builder, model.trainRows, model.opqIterations, model.trainIterations)
	}
	if model.method == "global_residual_pq" {
		return fmt.Sprintf("production/buildable scout over %s granules; global centroid-residual PQ codebooks trained on %d held-out rows for %d iterations and evaluated on disjoint eval rows; this is a residual-codebook baseline, not local LOPQ; codec recall is conditional on centroid-routed candidate union", builder, model.trainRows, model.trainIterations)
	}
	return fmt.Sprintf("production/buildable scout over %s granules; global PQ codebooks trained on %d held-out rows for %d iterations and evaluated on disjoint eval rows; codec recall is conditional on centroid-routed candidate union", builder, model.trainRows, model.trainIterations)
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
	working := make([]float32, dims)
	rotated := make([]float32, dims)
	approxOriginal := make([]float32, dims)
	var relSum float64
	var maxRel float64
	for outRow, rowID := range rowIDs {
		srcBase := rowID * dims
		original := vectors[srcBase : srcBase+dims]
		src := original
		if len(model.residualCenter) > 0 {
			for j := 0; j < dims; j++ {
				working[j] = original[j] - model.residualCenter[j]
			}
			src = working
		}
		if len(model.rotation) > 0 {
			columnVectorGraphDeep1BRotateVectorInto(src, model.rotation, dims, rotated)
			src = rotated
		}
		for sub := 0; sub < model.subquantizers; sub++ {
			start := model.subStarts[sub]
			subDim := model.subDims[sub]
			code := columnVectorGraphDeep1BPQNearestCode(src[start:start+subDim], model, sub)
			codes[outRow*model.subquantizers+sub] = byte(code)
			centroid := columnVectorGraphDeep1BPQCentroid(model, sub, code)
			copy(recon[start:start+subDim], centroid)
		}
		invNormsStored[outRow] = columnVectorGraphDeep1BFloat16BitsToFloat32(columnVectorGraphDeep1BFloat32ToFloat16Bits(invNorms[rowID]))
		reconForError := recon
		if len(model.residualCenter) > 0 && len(model.rotation) == 0 {
			for j := 0; j < dims; j++ {
				approxOriginal[j] = model.residualCenter[j] + recon[j]
			}
			reconForError = approxOriginal
			src = original
		}
		var errSquared float64
		var normSquared float64
		for j := 0; j < dims; j++ {
			diff := float64(src[j] - reconForError[j])
			errSquared += diff * diff
			value := float64(src[j])
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
	table   []float32
	baseDot float32
}

func columnVectorGraphDeep1BPreparePQScorer(model columnVectorGraphDeep1BPQModel, query []float32) columnVectorGraphDeep1BPQScorer {
	table := make([]float32, model.subquantizers*model.codebookSize)
	queryForScore := query
	var rotatedQuery []float32
	var baseDot float32
	if len(model.residualCenter) > 0 {
		for j := 0; j < model.dims; j++ {
			baseDot += query[j] * model.residualCenter[j]
		}
	}
	if len(model.rotation) > 0 {
		rotatedQuery = make([]float32, model.dims)
		columnVectorGraphDeep1BRotateVectorInto(query, model.rotation, model.dims, rotatedQuery)
		queryForScore = rotatedQuery
	}
	for sub := 0; sub < model.subquantizers; sub++ {
		start := model.subStarts[sub]
		subDim := model.subDims[sub]
		querySub := queryForScore[start : start+subDim]
		for code := 0; code < model.codebookSize; code++ {
			centroid := columnVectorGraphDeep1BPQCentroid(model, sub, code)
			var dot float32
			for j := 0; j < subDim; j++ {
				dot += querySub[j] * centroid[j]
			}
			table[sub*model.codebookSize+code] = dot
		}
	}
	return columnVectorGraphDeep1BPQScorer{table: table, baseDot: baseDot}
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
		dot := scorer.baseDot
		codeBase := row * subquantizers
		for sub := 0; sub < subquantizers; sub++ {
			dot += scorer.table[sub*codebookSize+int(encoding.codes[codeBase+sub])]
		}
		scores[row] = dot * queryInvNorm * encoding.invNorms[row]
	}
}
