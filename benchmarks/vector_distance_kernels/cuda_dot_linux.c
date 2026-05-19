//go:build linux && cgo && cuda
// +build linux,cgo,cuda

#include <cublas_v2.h>
#include <cuda_runtime_api.h>

#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct CudaDotContext {
	cublasHandle_t handle;
	float *d_candidates;
	float *d_queries;
	float *d_dots;
	uint32_t dims;
	uint32_t candidate_count;
	uint32_t query_capacity;
} CudaDotContext;

static void cuda_dot_set_error(char *err, int err_len, const char *message) {
	if (err == NULL || err_len <= 0) {
		return;
	}
	if (message == NULL) {
		message = "unknown CUDA error";
	}
	strncpy(err, message, (size_t)err_len - 1);
	err[err_len - 1] = '\0';
}

static void cuda_dot_set_errorf(char *err, int err_len, const char *format, ...) {
	if (err == NULL || err_len <= 0) {
		return;
	}
	va_list args;
	va_start(args, format);
	vsnprintf(err, (size_t)err_len, format, args);
	va_end(args);
	err[err_len - 1] = '\0';
}

static const char *cuda_dot_cublas_status(cublasStatus_t status) {
	switch (status) {
	case CUBLAS_STATUS_SUCCESS:
		return "CUBLAS_STATUS_SUCCESS";
	case CUBLAS_STATUS_NOT_INITIALIZED:
		return "CUBLAS_STATUS_NOT_INITIALIZED";
	case CUBLAS_STATUS_ALLOC_FAILED:
		return "CUBLAS_STATUS_ALLOC_FAILED";
	case CUBLAS_STATUS_INVALID_VALUE:
		return "CUBLAS_STATUS_INVALID_VALUE";
	case CUBLAS_STATUS_ARCH_MISMATCH:
		return "CUBLAS_STATUS_ARCH_MISMATCH";
	case CUBLAS_STATUS_MAPPING_ERROR:
		return "CUBLAS_STATUS_MAPPING_ERROR";
	case CUBLAS_STATUS_EXECUTION_FAILED:
		return "CUBLAS_STATUS_EXECUTION_FAILED";
	case CUBLAS_STATUS_INTERNAL_ERROR:
		return "CUBLAS_STATUS_INTERNAL_ERROR";
	case CUBLAS_STATUS_NOT_SUPPORTED:
		return "CUBLAS_STATUS_NOT_SUPPORTED";
	case CUBLAS_STATUS_LICENSE_ERROR:
		return "CUBLAS_STATUS_LICENSE_ERROR";
	default:
		return "unknown cuBLAS status";
	}
}

static int cuda_dot_float_bytes(uint64_t count, size_t *bytes) {
	if (count > SIZE_MAX / sizeof(float)) {
		return 0;
	}
	*bytes = (size_t)count * sizeof(float);
	return 1;
}

static int cuda_dot_check_dims(uint32_t dims, uint32_t candidate_count, uint32_t query_count, char *err, int err_len) {
	if (dims == 0) {
		cuda_dot_set_error(err, err_len, "CUDA dot dims must be positive");
		return 0;
	}
	if (candidate_count == 0) {
		cuda_dot_set_error(err, err_len, "CUDA dot candidate count must be positive");
		return 0;
	}
	if (query_count == 0) {
		cuda_dot_set_error(err, err_len, "CUDA dot query count must be positive");
		return 0;
	}
	if (dims > (uint32_t)INT_MAX || candidate_count > (uint32_t)INT_MAX || query_count > (uint32_t)INT_MAX) {
		cuda_dot_set_error(err, err_len, "CUDA dot dimensions exceed cuBLAS int limits");
		return 0;
	}
	return 1;
}

static int cuda_dot_reserve_queries(CudaDotContext *ctx, uint32_t query_count, char *err, int err_len) {
	if (query_count <= ctx->query_capacity) {
		return 1;
	}

	size_t query_bytes = 0;
	size_t dot_bytes = 0;
	if (!cuda_dot_float_bytes((uint64_t)query_count * ctx->dims, &query_bytes) ||
	    !cuda_dot_float_bytes((uint64_t)query_count * ctx->candidate_count, &dot_bytes)) {
		cuda_dot_set_error(err, err_len, "CUDA dot query/output buffer size overflows size_t");
		return 0;
	}

	float *new_queries = NULL;
	float *new_dots = NULL;
	cudaError_t cuda_status = cudaMalloc((void **)&new_queries, query_bytes);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaMalloc queries failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}
	cuda_status = cudaMalloc((void **)&new_dots, dot_bytes);
	if (cuda_status != cudaSuccess) {
		cudaFree(new_queries);
		cuda_dot_set_errorf(err, err_len, "cudaMalloc dots failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}

	cudaFree(ctx->d_queries);
	cudaFree(ctx->d_dots);
	ctx->d_queries = new_queries;
	ctx->d_dots = new_dots;
	ctx->query_capacity = query_count;
	return 1;
}

int cuda_dot_device_name(char *name, int name_len, char *err, int err_len) {
	if (name == NULL || name_len <= 0) {
		cuda_dot_set_error(err, err_len, "CUDA dot device-name buffer is nil");
		return 0;
	}
	int device_count = 0;
	cudaError_t cuda_status = cudaGetDeviceCount(&device_count);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaGetDeviceCount failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}
	if (device_count <= 0) {
		cuda_dot_set_error(err, err_len, "no CUDA device found");
		return 0;
	}

	int device = 0;
	cuda_status = cudaGetDevice(&device);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaGetDevice failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}

	struct cudaDeviceProp prop;
	cuda_status = cudaGetDeviceProperties(&prop, device);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaGetDeviceProperties failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}
	snprintf(name, (size_t)name_len, "%s (compute capability %d.%d)", prop.name, prop.major, prop.minor);
	name[name_len - 1] = '\0';
	return 1;
}

CudaDotContext *cuda_dot_create(uint32_t dims, uint32_t candidate_count, char *err, int err_len) {
	if (!cuda_dot_check_dims(dims, candidate_count, 1, err, err_len)) {
		return NULL;
	}

	int device_count = 0;
	cudaError_t cuda_status = cudaGetDeviceCount(&device_count);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaGetDeviceCount failed: %s", cudaGetErrorString(cuda_status));
		return NULL;
	}
	if (device_count <= 0) {
		cuda_dot_set_error(err, err_len, "no CUDA device found");
		return NULL;
	}

	CudaDotContext *ctx = (CudaDotContext *)calloc(1, sizeof(CudaDotContext));
	if (ctx == NULL) {
		cuda_dot_set_error(err, err_len, "failed to allocate CUDA dot context");
		return NULL;
	}
	ctx->dims = dims;
	ctx->candidate_count = candidate_count;

	cublasStatus_t cublas_status = cublasCreate(&ctx->handle);
	if (cublas_status != CUBLAS_STATUS_SUCCESS) {
		cuda_dot_set_errorf(err, err_len, "cublasCreate failed: %s", cuda_dot_cublas_status(cublas_status));
		free(ctx);
		return NULL;
	}

	size_t candidate_bytes = 0;
	if (!cuda_dot_float_bytes((uint64_t)dims * candidate_count, &candidate_bytes)) {
		cuda_dot_set_error(err, err_len, "CUDA dot candidate buffer size overflows size_t");
		cublasDestroy(ctx->handle);
		free(ctx);
		return NULL;
	}
	cuda_status = cudaMalloc((void **)&ctx->d_candidates, candidate_bytes);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaMalloc candidates failed: %s", cudaGetErrorString(cuda_status));
		cublasDestroy(ctx->handle);
		free(ctx);
		return NULL;
	}

	return ctx;
}

void cuda_dot_destroy(CudaDotContext *ctx) {
	if (ctx == NULL) {
		return;
	}
	cudaFree(ctx->d_candidates);
	cudaFree(ctx->d_queries);
	cudaFree(ctx->d_dots);
	if (ctx->handle != NULL) {
		cublasDestroy(ctx->handle);
	}
	free(ctx);
}

int cuda_dot_load_candidates(CudaDotContext *ctx, const float *candidates, char *err, int err_len) {
	if (ctx == NULL) {
		cuda_dot_set_error(err, err_len, "CUDA dot context is nil");
		return 0;
	}
	if (candidates == NULL) {
		cuda_dot_set_error(err, err_len, "CUDA dot candidates are nil");
		return 0;
	}
	size_t candidate_bytes = 0;
	if (!cuda_dot_float_bytes((uint64_t)ctx->dims * ctx->candidate_count, &candidate_bytes)) {
		cuda_dot_set_error(err, err_len, "CUDA dot candidate buffer size overflows size_t");
		return 0;
	}
	cudaError_t cuda_status = cudaMemcpy(ctx->d_candidates, candidates, candidate_bytes, cudaMemcpyHostToDevice);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaMemcpy candidates failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}
	return 1;
}

int cuda_dot_query_batch_loaded(CudaDotContext *ctx, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	if (ctx == NULL) {
		cuda_dot_set_error(err, err_len, "CUDA dot context is nil");
		return 0;
	}
	if (queries == NULL) {
		cuda_dot_set_error(err, err_len, "CUDA dot queries are nil");
		return 0;
	}
	if (dots == NULL) {
		cuda_dot_set_error(err, err_len, "CUDA dot output is nil");
		return 0;
	}
	if (!cuda_dot_check_dims(ctx->dims, ctx->candidate_count, query_count, err, err_len)) {
		return 0;
	}
	if (!cuda_dot_reserve_queries(ctx, query_count, err, err_len)) {
		return 0;
	}

	size_t query_bytes = 0;
	size_t dot_bytes = 0;
	if (!cuda_dot_float_bytes((uint64_t)query_count * ctx->dims, &query_bytes) ||
	    !cuda_dot_float_bytes((uint64_t)query_count * ctx->candidate_count, &dot_bytes)) {
		cuda_dot_set_error(err, err_len, "CUDA dot query/output buffer size overflows size_t");
		return 0;
	}

	cudaError_t cuda_status = cudaMemcpy(ctx->d_queries, queries, query_bytes, cudaMemcpyHostToDevice);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaMemcpy queries failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}

	const float alpha = 1.0f;
	const float beta = 0.0f;
	cublasStatus_t cublas_status = cublasSgemm(
		ctx->handle,
		CUBLAS_OP_T,
		CUBLAS_OP_N,
		(int)ctx->candidate_count,
		(int)query_count,
		(int)ctx->dims,
		&alpha,
		ctx->d_candidates,
		(int)ctx->dims,
		ctx->d_queries,
		(int)ctx->dims,
		&beta,
		ctx->d_dots,
		(int)ctx->candidate_count
	);
	if (cublas_status != CUBLAS_STATUS_SUCCESS) {
		cuda_dot_set_errorf(err, err_len, "cublasSgemm failed: %s", cuda_dot_cublas_status(cublas_status));
		return 0;
	}

	cuda_status = cudaMemcpy(dots, ctx->d_dots, dot_bytes, cudaMemcpyDeviceToHost);
	if (cuda_status != cudaSuccess) {
		cuda_dot_set_errorf(err, err_len, "cudaMemcpy dots failed: %s", cudaGetErrorString(cuda_status));
		return 0;
	}
	return 1;
}

int cuda_dot_query_batch(CudaDotContext *ctx, const float *queries, const float *candidates, uint32_t query_count, float *dots, char *err, int err_len) {
	if (!cuda_dot_load_candidates(ctx, candidates, err, err_len)) {
		return 0;
	}
	return cuda_dot_query_batch_loaded(ctx, queries, query_count, dots, err, err_len);
}
