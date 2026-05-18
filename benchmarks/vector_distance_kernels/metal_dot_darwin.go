//go:build darwin && cgo

package vectordistancekernels

/*
#cgo darwin LDFLAGS: -framework Foundation -framework Metal -framework MetalPerformanceShaders
#include <stdint.h>
#include <stdlib.h>

typedef struct MetalDotContext MetalDotContext;

MetalDotContext *metal_dot_create(uint32_t dims, uint32_t candidate_count, char *err, int err_len);
void metal_dot_destroy(MetalDotContext *ctx);
int metal_dot_load_candidates(MetalDotContext *ctx, const float *candidates, char *err, int err_len);
int metal_dot_load_candidate_inv_norms(MetalDotContext *ctx, const float *candidate_inv_norms, char *err, int err_len);
int metal_dot_run_loaded(MetalDotContext *ctx, const float *query, float *dots, char *err, int err_len);
int metal_dot_run_loaded_no_copy_output(MetalDotContext *ctx, const float *query, float *dots, char *err, int err_len);
int metal_dot_run(MetalDotContext *ctx, const float *query, const float *candidates, float *dots, char *err, int err_len);
int metal_dot_run_no_copy(MetalDotContext *ctx, const float *query, const float *candidates, float *dots, char *err, int err_len);
int metal_dot_query_batch_loaded(MetalDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int metal_dot_query_batch_loaded_no_copy(MetalDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int metal_dot_query_batch_tiled_loaded_no_copy(MetalDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int metal_dot_query_batch_tiled_variant_loaded_no_copy(MetalDotContext *ctx, const float *queries, uint32_t query_count, uint32_t variant, float *dots, char *err, int err_len);
int metal_dot_query_batch_simd_loaded_no_copy(MetalDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int metal_dot_query_batch_fixed_dim_loaded_no_copy(MetalDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int metal_dot_query_batch_cosine_loaded_no_copy(MetalDotContext *ctx, const float *queries, const float *query_inv_norms, uint32_t query_count, float *distances, char *err, int err_len);
int metal_dot_query_batch_fixed_dim_cosine_loaded_no_copy(MetalDotContext *ctx, const float *queries, const float *query_inv_norms, uint32_t query_count, float *distances, char *err, int err_len);
int metal_dot_query_batch_mps_loaded_no_copy(MetalDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int metal_dot_query_batch_topk_loaded_no_copy(MetalDotContext *ctx, const float *queries, const float *query_inv_norms, uint32_t query_count, uint32_t top_k, float *scores, uint32_t *indices, char *err, int err_len);
int metal_dot_query_batch_loaded_async_double_buffered(MetalDotContext *ctx, const float *queries, uint32_t query_count, uint32_t iterations, float *dots, char *err, int err_len);
int metal_dot_query_batch_loaded_async_buffered(MetalDotContext *ctx, const float *queries, uint32_t query_count, uint32_t query_batches, uint32_t depth, uint32_t iterations, float *dots, char *err, int err_len);
int metal_dot_query_batch_no_copy(MetalDotContext *ctx, const float *queries, const float *candidates, uint32_t query_count, float *dots, char *err, int err_len);
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

const metalDotErrorLen = 1024
const metalDotTopKBlockCandidates = 1024
const metalDotTiledVariantCount = 6

var metalDotTiledVariantNames = [...]string{
	"tile_q4_c32_d64",
	"tile_q4_c32_d128",
	"tile_q8_c16_d64",
	"tile_q8_c16_d128",
	"tile_q16_c8_d64",
	"tile_q16_c8_d128",
}

type metalDotKernel struct {
	ctx            *C.MetalDotContext
	err            *C.char
	dims           int
	candidateCount int
}

func newMetalDotKernel(dims, candidateCount int) (*metalDotKernel, error) {
	if dims <= 0 {
		return nil, fmt.Errorf("metal dot: dims must be positive, got %d", dims)
	}
	if candidateCount <= 0 {
		return nil, fmt.Errorf("metal dot: candidate count must be positive, got %d", candidateCount)
	}
	err := (*C.char)(C.malloc(C.size_t(metalDotErrorLen)))
	if err == nil {
		return nil, errors.New("metal dot: failed to allocate error buffer")
	}
	ctx := C.metal_dot_create(C.uint32_t(dims), C.uint32_t(candidateCount), err, C.int(metalDotErrorLen))
	if ctx == nil {
		createErr := metalDotError(err)
		C.free(unsafe.Pointer(err))
		return nil, createErr
	}
	return &metalDotKernel{
		ctx:            ctx,
		err:            err,
		dims:           dims,
		candidateCount: candidateCount,
	}, nil
}

func (k *metalDotKernel) Close() {
	if k == nil || k.ctx == nil {
		return
	}
	C.metal_dot_destroy(k.ctx)
	k.ctx = nil
	C.free(unsafe.Pointer(k.err))
	k.err = nil
}

func (k *metalDotKernel) LoadCandidates(candidates []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	ok := C.metal_dot_load_candidates(k.ctx, (*C.float)(unsafe.Pointer(&candidates[0])), k.err, C.int(metalDotErrorLen))
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) LoadCandidateInvNorms(candidateInvNorms []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("metal dot: closed kernel")
	}
	if len(candidateInvNorms) != k.candidateCount {
		return fmt.Errorf("metal dot: candidate inverse norms length=%d want %d", len(candidateInvNorms), k.candidateCount)
	}
	ok := C.metal_dot_load_candidate_inv_norms(k.ctx, (*C.float)(unsafe.Pointer(&candidateInvNorms[0])), k.err, C.int(metalDotErrorLen))
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) DotLoaded(query []float32, dots []float32) error {
	if err := k.validateQueryAndDots(query, dots); err != nil {
		return err
	}
	ok := C.metal_dot_run_loaded(
		k.ctx,
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) DotLoadedNoCopyOutput(query []float32, dots []float32) error {
	if err := k.validateQueryAndDots(query, dots); err != nil {
		return err
	}
	ok := C.metal_dot_run_loaded_no_copy_output(
		k.ctx,
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) Dot(query, candidates, dots []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	if err := k.validateQueryAndDots(query, dots); err != nil {
		return err
	}
	ok := C.metal_dot_run(
		k.ctx,
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&candidates[0])),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) DotNoCopy(query, candidates, dots []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	if err := k.validateQueryAndDots(query, dots); err != nil {
		return err
	}
	ok := C.metal_dot_run_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&query[0])),
		(*C.float)(unsafe.Pointer(&candidates[0])),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchLoaded(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_loaded(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatch(queries, candidates []float32, queryCount int, dots []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	if err := k.LoadCandidates(candidates); err != nil {
		return err
	}
	return k.QueryBatchLoaded(queries, queryCount, dots)
}

func (k *metalDotKernel) QueryBatchLoadedNoCopy(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchTiledLoadedNoCopy(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_tiled_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchTiledVariantLoadedNoCopy(queries []float32, queryCount, variant int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	if variant < 0 || variant >= metalDotTiledVariantCount {
		return fmt.Errorf("metal dot: tiled variant=%d out of range [0,%d)", variant, metalDotTiledVariantCount)
	}
	ok := C.metal_dot_query_batch_tiled_variant_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		C.uint32_t(variant),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchSIMDLoadedNoCopy(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_simd_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchFixedDimLoadedNoCopy(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_fixed_dim_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchCosineLoadedNoCopy(queries, queryInvNorms []float32, queryCount int, distances []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, distances); err != nil {
		return err
	}
	if len(queryInvNorms) != queryCount {
		return fmt.Errorf("metal dot: query inverse norms length=%d want %d", len(queryInvNorms), queryCount)
	}
	ok := C.metal_dot_query_batch_cosine_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		(*C.float)(unsafe.Pointer(&queryInvNorms[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&distances[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchFixedDimCosineLoadedNoCopy(queries, queryInvNorms []float32, queryCount int, distances []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, distances); err != nil {
		return err
	}
	if len(queryInvNorms) != queryCount {
		return fmt.Errorf("metal dot: query inverse norms length=%d want %d", len(queryInvNorms), queryCount)
	}
	ok := C.metal_dot_query_batch_fixed_dim_cosine_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		(*C.float)(unsafe.Pointer(&queryInvNorms[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&distances[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchMPSLoadedNoCopy(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_mps_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchCosineBlockTopKLoadedNoCopy(queries, queryInvNorms []float32, queryCount, topK int, scores []float32, indices []uint32) (int, error) {
	if err := k.validateQueryBatchAndTopK(queries, queryInvNorms, queryCount, topK, scores, indices); err != nil {
		return 0, err
	}
	blockCount := k.topKBlockCount()
	ok := C.metal_dot_query_batch_topk_loaded_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		(*C.float)(unsafe.Pointer(&queryInvNorms[0])),
		C.uint32_t(queryCount),
		C.uint32_t(topK),
		(*C.float)(unsafe.Pointer(&scores[0])),
		(*C.uint32_t)(unsafe.Pointer(&indices[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return 0, metalDotError(k.err)
	}
	return blockCount, nil
}

func (k *metalDotKernel) QueryBatchLoadedAsyncDoubleBuffered(queries []float32, queryCount, iterations int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	if iterations <= 0 {
		return fmt.Errorf("metal dot: async iterations must be positive, got %d", iterations)
	}
	if uint64(iterations) > uint64(^uint32(0)) {
		return fmt.Errorf("metal dot: async iterations=%d exceeds uint32 max", iterations)
	}
	ok := C.metal_dot_query_batch_loaded_async_double_buffered(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		C.uint32_t(iterations),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchLoadedAsyncBuffered(queries []float32, queryCount, queryBatches, depth, iterations int, dots []float32) error {
	if err := k.validateQueryBatchRingAndDots(queries, queryCount, queryBatches, dots); err != nil {
		return err
	}
	if depth <= 0 {
		return fmt.Errorf("metal dot: async depth must be positive, got %d", depth)
	}
	if iterations <= 0 {
		return fmt.Errorf("metal dot: async iterations must be positive, got %d", iterations)
	}
	if uint64(queryBatches) > uint64(^uint32(0)) || uint64(depth) > uint64(^uint32(0)) || uint64(iterations) > uint64(^uint32(0)) {
		return fmt.Errorf("metal dot: async query_batches=%d depth=%d iterations=%d exceeds uint32 max", queryBatches, depth, iterations)
	}
	ok := C.metal_dot_query_batch_loaded_async_buffered(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		C.uint32_t(queryBatches),
		C.uint32_t(depth),
		C.uint32_t(iterations),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) QueryBatchNoCopy(queries, candidates []float32, queryCount int, dots []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.metal_dot_query_batch_no_copy(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		(*C.float)(unsafe.Pointer(&candidates[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(metalDotErrorLen),
	)
	if ok == 0 {
		return metalDotError(k.err)
	}
	return nil
}

func (k *metalDotKernel) topKBlockCount() int {
	return (k.candidateCount + metalDotTopKBlockCandidates - 1) / metalDotTopKBlockCandidates
}

func (k *metalDotKernel) validateCandidates(candidates []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("metal dot: closed kernel")
	}
	want := k.candidateCount * k.dims
	if len(candidates) != want {
		return fmt.Errorf("metal dot: candidates length=%d want %d", len(candidates), want)
	}
	return nil
}

func (k *metalDotKernel) validateQueryAndDots(query, dots []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("metal dot: closed kernel")
	}
	if len(query) != k.dims {
		return fmt.Errorf("metal dot: query length=%d want %d", len(query), k.dims)
	}
	if len(dots) != k.candidateCount {
		return fmt.Errorf("metal dot: dots length=%d want %d", len(dots), k.candidateCount)
	}
	return nil
}

func (k *metalDotKernel) validateQueryBatchRingAndDots(queries []float32, queryCount, queryBatches int, dots []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("metal dot: closed kernel")
	}
	if queryCount <= 0 {
		return fmt.Errorf("metal dot: query count must be positive, got %d", queryCount)
	}
	if queryBatches <= 0 {
		return fmt.Errorf("metal dot: query batches must be positive, got %d", queryBatches)
	}
	wantQueries := queryBatches * queryCount * k.dims
	if len(queries) != wantQueries {
		return fmt.Errorf("metal dot: query ring length=%d want %d", len(queries), wantQueries)
	}
	wantDots := queryCount * k.candidateCount
	if len(dots) != wantDots {
		return fmt.Errorf("metal dot: query-batch dots length=%d want %d", len(dots), wantDots)
	}
	return nil
}

func (k *metalDotKernel) validateQueryBatchAndDots(queries []float32, queryCount int, dots []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("metal dot: closed kernel")
	}
	if queryCount <= 0 {
		return fmt.Errorf("metal dot: query count must be positive, got %d", queryCount)
	}
	wantQueries := queryCount * k.dims
	if len(queries) != wantQueries {
		return fmt.Errorf("metal dot: queries length=%d want %d", len(queries), wantQueries)
	}
	wantDots := queryCount * k.candidateCount
	if len(dots) != wantDots {
		return fmt.Errorf("metal dot: query-batch dots length=%d want %d", len(dots), wantDots)
	}
	return nil
}

func (k *metalDotKernel) validateQueryBatchAndTopK(queries, queryInvNorms []float32, queryCount, topK int, scores []float32, indices []uint32) error {
	if k == nil || k.ctx == nil {
		return errors.New("metal dot: closed kernel")
	}
	if queryCount <= 0 {
		return fmt.Errorf("metal dot: query count must be positive, got %d", queryCount)
	}
	if topK <= 0 || topK > 16 {
		return fmt.Errorf("metal dot: topK must be in [1,16], got %d", topK)
	}
	wantQueries := queryCount * k.dims
	if len(queries) != wantQueries {
		return fmt.Errorf("metal dot: queries length=%d want %d", len(queries), wantQueries)
	}
	if len(queryInvNorms) != queryCount {
		return fmt.Errorf("metal dot: query inverse norms length=%d want %d", len(queryInvNorms), queryCount)
	}
	wantResults := queryCount * k.topKBlockCount() * topK
	if len(scores) != wantResults {
		return fmt.Errorf("metal dot: block top-k scores length=%d want %d", len(scores), wantResults)
	}
	if len(indices) != wantResults {
		return fmt.Errorf("metal dot: block top-k indices length=%d want %d", len(indices), wantResults)
	}
	return nil
}

func metalDotError(err *C.char) error {
	msg := C.GoString(err)
	if msg == "" {
		msg = "unknown Metal error"
	}
	return errors.New(msg)
}
