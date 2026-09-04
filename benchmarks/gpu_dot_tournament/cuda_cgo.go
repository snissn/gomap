//go:build cuda

package gpudottournament

/*
#cgo LDFLAGS: -lcudart -lcublas
#include <stdlib.h>
#include <cuda_runtime.h>
#include <cublas_v2.h>

static const char* gd_cuda_err(cudaError_t err) { return cudaGetErrorString(err); }
static const char* gd_cublas_err(cublasStatus_t st) {
    switch (st) {
    case CUBLAS_STATUS_SUCCESS: return "CUBLAS_STATUS_SUCCESS";
    case CUBLAS_STATUS_NOT_INITIALIZED: return "CUBLAS_STATUS_NOT_INITIALIZED";
    case CUBLAS_STATUS_ALLOC_FAILED: return "CUBLAS_STATUS_ALLOC_FAILED";
    case CUBLAS_STATUS_INVALID_VALUE: return "CUBLAS_STATUS_INVALID_VALUE";
    case CUBLAS_STATUS_ARCH_MISMATCH: return "CUBLAS_STATUS_ARCH_MISMATCH";
    case CUBLAS_STATUS_MAPPING_ERROR: return "CUBLAS_STATUS_MAPPING_ERROR";
    case CUBLAS_STATUS_EXECUTION_FAILED: return "CUBLAS_STATUS_EXECUTION_FAILED";
    case CUBLAS_STATUS_INTERNAL_ERROR: return "CUBLAS_STATUS_INTERNAL_ERROR";
    case CUBLAS_STATUS_NOT_SUPPORTED: return "CUBLAS_STATUS_NOT_SUPPORTED";
    case CUBLAS_STATUS_LICENSE_ERROR: return "CUBLAS_STATUS_LICENSE_ERROR";
    default: return "CUBLAS_STATUS_UNKNOWN";
    }
}
static cudaError_t gd_malloc(void **p, size_t n) { return cudaMalloc(p, n); }
static cudaError_t gd_free(void *p) { return cudaFree(p); }
static cudaError_t gd_h2d(void *dst, const void *src, size_t n) { return cudaMemcpy(dst, src, n, cudaMemcpyHostToDevice); }
static cudaError_t gd_d2h(void *dst, const void *src, size_t n) { return cudaMemcpy(dst, src, n, cudaMemcpyDeviceToHost); }
static cudaError_t gd_device_count(int *n) { return cudaGetDeviceCount(n); }
static cudaError_t gd_sync() { return cudaDeviceSynchronize(); }
static cublasStatus_t gd_create(cublasHandle_t *h) { return cublasCreate(h); }
static cublasStatus_t gd_destroy(cublasHandle_t h) { return cublasDestroy(h); }
static cublasStatus_t gd_sgemv_t(cublasHandle_t h, int dim, int rows, const float *matrix, const float *query, float *out) {
    const float alpha = 1.0f;
    const float beta = 0.0f;
    return cublasSgemv(h, CUBLAS_OP_T, dim, rows, &alpha, matrix, dim, query, 1, &beta, out, 1);
}
static cublasStatus_t gd_sgemm_t(cublasHandle_t h, int dim, int rows, const float *matrix, const float *query, float *out) {
    const float alpha = 1.0f;
    const float beta = 0.0f;
    return cublasSgemm(h, CUBLAS_OP_T, CUBLAS_OP_N, 1, rows, dim, &alpha, query, dim, matrix, dim, &beta, out, 1);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type cudaHandle = C.cublasHandle_t

func cudaDeviceCount() (int, error) {
	var n C.int
	if err := C.gd_device_count(&n); err != C.cudaSuccess {
		return 0, fmt.Errorf("cudaGetDeviceCount: %s", C.GoString(C.gd_cuda_err(err)))
	}
	return int(n), nil
}

func cudaCreate() (cudaHandle, error) {
	var h C.cublasHandle_t
	if st := C.gd_create(&h); st != C.CUBLAS_STATUS_SUCCESS {
		return nil, fmt.Errorf("cublasCreate: %s", C.GoString(C.gd_cublas_err(st)))
	}
	return h, nil
}

func cudaDestroy(h cudaHandle) error {
	if st := C.gd_destroy(h); st != C.CUBLAS_STATUS_SUCCESS {
		return fmt.Errorf("cublasDestroy: %s", C.GoString(C.gd_cublas_err(st)))
	}
	return nil
}

func cudaMallocRaw(bytes int) (unsafe.Pointer, error) {
	var p unsafe.Pointer
	if err := C.gd_malloc(&p, C.size_t(bytes)); err != C.cudaSuccess {
		return nil, fmt.Errorf("cudaMalloc(%d): %s", bytes, C.GoString(C.gd_cuda_err(err)))
	}
	return p, nil
}

func cudaFreeRaw(p unsafe.Pointer) error {
	if err := C.gd_free(p); err != C.cudaSuccess {
		return fmt.Errorf("cudaFree: %s", C.GoString(C.gd_cuda_err(err)))
	}
	return nil
}

func cudaH2DRaw(dst unsafe.Pointer, src []float32) error {
	if len(src) == 0 {
		return nil
	}
	if err := C.gd_h2d(dst, unsafe.Pointer(&src[0]), C.size_t(len(src)*4)); err != C.cudaSuccess {
		return fmt.Errorf("cudaMemcpy H2D: %s", C.GoString(C.gd_cuda_err(err)))
	}
	return nil
}

func cudaD2HRaw(dst []float32, src unsafe.Pointer) error {
	if len(dst) == 0 {
		return nil
	}
	if err := C.gd_d2h(unsafe.Pointer(&dst[0]), src, C.size_t(len(dst)*4)); err != C.cudaSuccess {
		return fmt.Errorf("cudaMemcpy D2H: %s", C.GoString(C.gd_cuda_err(err)))
	}
	return nil
}

func cudaSyncRaw() error {
	if err := C.gd_sync(); err != C.cudaSuccess {
		return fmt.Errorf("cudaDeviceSynchronize: %s", C.GoString(C.gd_cuda_err(err)))
	}
	return nil
}

func cudaSgemvT(h cudaHandle, dim, rows int, matrix, query, out unsafe.Pointer) error {
	if st := C.gd_sgemv_t(h, C.int(dim), C.int(rows), (*C.float)(matrix), (*C.float)(query), (*C.float)(out)); st != C.CUBLAS_STATUS_SUCCESS {
		return fmt.Errorf("cublasSgemv: %s", C.GoString(C.gd_cublas_err(st)))
	}
	return nil
}

func cudaSgemmT(h cudaHandle, dim, rows int, matrix, query, out unsafe.Pointer) error {
	if st := C.gd_sgemm_t(h, C.int(dim), C.int(rows), (*C.float)(matrix), (*C.float)(query), (*C.float)(out)); st != C.CUBLAS_STATUS_SUCCESS {
		return fmt.Errorf("cublasSgemm: %s", C.GoString(C.gd_cublas_err(st)))
	}
	return nil
}
