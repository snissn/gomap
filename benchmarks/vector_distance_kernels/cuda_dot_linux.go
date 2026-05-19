//go:build linux && cgo && cuda
// +build linux,cgo,cuda

package vectordistancekernels

/*
#cgo linux CFLAGS: -I/usr/local/cuda/include
#cgo linux LDFLAGS: -L/usr/local/cuda/lib64 -lcublas -lcudart
#include <stdint.h>
#include <stdlib.h>

typedef struct CudaDotContext CudaDotContext;

CudaDotContext *cuda_dot_create(uint32_t dims, uint32_t candidate_count, char *err, int err_len);
void cuda_dot_destroy(CudaDotContext *ctx);
int cuda_dot_device_name(char *name, int name_len, char *err, int err_len);
int cuda_dot_load_candidates(CudaDotContext *ctx, const float *candidates, char *err, int err_len);
int cuda_dot_query_batch_loaded(CudaDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len);
int cuda_dot_query_batch(CudaDotContext *ctx, const float *queries, const float *candidates, uint32_t query_count, float *dots, char *err, int err_len);
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	cudaDotErrorLen      = 1024
	cudaDotDeviceNameLen = 256
)

type cudaDotKernel struct {
	ctx            *C.CudaDotContext
	err            *C.char
	dims           int
	candidateCount int
}

func cudaDotDeviceName() (string, error) {
	name := (*C.char)(C.malloc(C.size_t(cudaDotDeviceNameLen)))
	if name == nil {
		return "", errors.New("cuda dot: failed to allocate device-name buffer")
	}
	defer C.free(unsafe.Pointer(name))

	err := (*C.char)(C.malloc(C.size_t(cudaDotErrorLen)))
	if err == nil {
		return "", errors.New("cuda dot: failed to allocate error buffer")
	}
	defer C.free(unsafe.Pointer(err))

	ok := C.cuda_dot_device_name(name, C.int(cudaDotDeviceNameLen), err, C.int(cudaDotErrorLen))
	if ok == 0 {
		return "", cudaDotError(err)
	}
	return C.GoString(name), nil
}

func newCUDADotKernel(dims, candidateCount int) (*cudaDotKernel, error) {
	if dims <= 0 {
		return nil, fmt.Errorf("cuda dot: dims must be positive, got %d", dims)
	}
	if candidateCount <= 0 {
		return nil, fmt.Errorf("cuda dot: candidate count must be positive, got %d", candidateCount)
	}
	if uint64(dims) > uint64(^uint32(0)) || uint64(candidateCount) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("cuda dot: dims=%d candidateCount=%d exceed uint32 max", dims, candidateCount)
	}
	err := (*C.char)(C.malloc(C.size_t(cudaDotErrorLen)))
	if err == nil {
		return nil, errors.New("cuda dot: failed to allocate error buffer")
	}
	ctx := C.cuda_dot_create(C.uint32_t(dims), C.uint32_t(candidateCount), err, C.int(cudaDotErrorLen))
	if ctx == nil {
		createErr := cudaDotError(err)
		C.free(unsafe.Pointer(err))
		return nil, createErr
	}
	return &cudaDotKernel{
		ctx:            ctx,
		err:            err,
		dims:           dims,
		candidateCount: candidateCount,
	}, nil
}

func (k *cudaDotKernel) Close() {
	if k == nil || k.ctx == nil {
		return
	}
	C.cuda_dot_destroy(k.ctx)
	k.ctx = nil
	C.free(unsafe.Pointer(k.err))
	k.err = nil
}

func (k *cudaDotKernel) LoadCandidates(candidates []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	ok := C.cuda_dot_load_candidates(k.ctx, (*C.float)(unsafe.Pointer(&candidates[0])), k.err, C.int(cudaDotErrorLen))
	if ok == 0 {
		return cudaDotError(k.err)
	}
	return nil
}

func (k *cudaDotKernel) QueryBatchLoaded(queries []float32, queryCount int, dots []float32) error {
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.cuda_dot_query_batch_loaded(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(cudaDotErrorLen),
	)
	if ok == 0 {
		return cudaDotError(k.err)
	}
	return nil
}

func (k *cudaDotKernel) QueryBatch(queries, candidates []float32, queryCount int, dots []float32) error {
	if err := k.validateCandidates(candidates); err != nil {
		return err
	}
	if err := k.validateQueryBatchAndDots(queries, queryCount, dots); err != nil {
		return err
	}
	ok := C.cuda_dot_query_batch(
		k.ctx,
		(*C.float)(unsafe.Pointer(&queries[0])),
		(*C.float)(unsafe.Pointer(&candidates[0])),
		C.uint32_t(queryCount),
		(*C.float)(unsafe.Pointer(&dots[0])),
		k.err,
		C.int(cudaDotErrorLen),
	)
	if ok == 0 {
		return cudaDotError(k.err)
	}
	return nil
}

func (k *cudaDotKernel) validateCandidates(candidates []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("cuda dot: closed kernel")
	}
	want := k.candidateCount * k.dims
	if len(candidates) != want {
		return fmt.Errorf("cuda dot: candidates length=%d want %d", len(candidates), want)
	}
	return nil
}

func (k *cudaDotKernel) validateQueryBatchAndDots(queries []float32, queryCount int, dots []float32) error {
	if k == nil || k.ctx == nil {
		return errors.New("cuda dot: closed kernel")
	}
	if queryCount <= 0 {
		return fmt.Errorf("cuda dot: query count must be positive, got %d", queryCount)
	}
	if uint64(queryCount) > uint64(^uint32(0)) {
		return fmt.Errorf("cuda dot: query count=%d exceeds uint32 max", queryCount)
	}
	wantQueries := queryCount * k.dims
	if len(queries) != wantQueries {
		return fmt.Errorf("cuda dot: queries length=%d want %d", len(queries), wantQueries)
	}
	wantDots := queryCount * k.candidateCount
	if len(dots) != wantDots {
		return fmt.Errorf("cuda dot: query-batch dots length=%d want %d", len(dots), wantDots)
	}
	return nil
}

func cudaDotError(err *C.char) error {
	msg := C.GoString(err)
	if msg == "" {
		msg = "unknown CUDA error"
	}
	return errors.New(msg)
}
