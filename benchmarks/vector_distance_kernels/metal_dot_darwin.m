//go:build darwin && cgo

#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#import <MetalPerformanceShaders/MetalPerformanceShaders.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct MetalDotContext MetalDotContext;

#define METAL_DOT_TILED_VARIANT_COUNT 6

@interface GMMetalDotContext : NSObject {
@public
	id<MTLDevice> device;
	id<MTLCommandQueue> queue;
	id<MTLComputePipelineState> pipeline;
	id<MTLComputePipelineState> queryBatchPipeline;
	id<MTLComputePipelineState> queryBatchTiledPipeline;
	id<MTLComputePipelineState> queryBatchTiledVariantPipelines[METAL_DOT_TILED_VARIANT_COUNT];
	id<MTLComputePipelineState> queryBatchSIMDPipeline;
	id<MTLComputePipelineState> queryBatchCosinePipeline;
	id<MTLComputePipelineState> queryBatchFixedDimPipeline;
	id<MTLComputePipelineState> queryBatchFixedDimCosinePipeline;
	id<MTLComputePipelineState> queryBatchTopKPipeline;
	id<MTLBuffer> queryBuffer;
	id<MTLBuffer> candidatesBuffer;
	id<MTLBuffer> candidateInvNormsBuffer;
	id<MTLBuffer> dotsBuffer;
	id<MTLBuffer> dimsBuffer;
	id<MTLBuffer> candidateCountBuffer;
	uint32_t dims;
	uint32_t candidateCount;
}

@end

@implementation GMMetalDotContext

- (void)dealloc {
	[device release];
	[queue release];
	[pipeline release];
	[queryBatchPipeline release];
	[queryBatchTiledPipeline release];
	for (int i = 0; i < METAL_DOT_TILED_VARIANT_COUNT; i++) {
		[queryBatchTiledVariantPipelines[i] release];
	}
	[queryBatchSIMDPipeline release];
	[queryBatchCosinePipeline release];
	[queryBatchFixedDimPipeline release];
	[queryBatchFixedDimCosinePipeline release];
	[queryBatchTopKPipeline release];
	[queryBuffer release];
	[candidatesBuffer release];
	[candidateInvNormsBuffer release];
	[dotsBuffer release];
	[dimsBuffer release];
	[candidateCountBuffer release];
	[super dealloc];
}

@end

static void metal_dot_set_error(char *err, int err_len, NSString *message) {
	if (err == NULL || err_len <= 0) {
		return;
	}
	const char *utf8 = message == nil ? "unknown Metal error" : [message UTF8String];
	if (utf8 == NULL) {
		utf8 = "unknown Metal error";
	}
	strncpy(err, utf8, (size_t)err_len - 1);
	err[err_len - 1] = '\0';
}

static void metal_dot_set_error_c(char *err, int err_len, const char *message) {
	if (err == NULL || err_len <= 0) {
		return;
	}
	if (message == NULL) {
		message = "unknown Metal error";
	}
	strncpy(err, message, (size_t)err_len - 1);
	err[err_len - 1] = '\0';
}

static int metal_dot_check_completed_command(id<MTLCommandBuffer> command_buffer, char *err, int err_len) {
	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static NSString *metal_dot_fixed_dim_function_name(uint32_t dims) {
	switch (dims) {
		case 64:
			return @"dot_query_batch_d64_f32";
		case 128:
			return @"dot_query_batch_d128_f32";
		case 256:
			return @"dot_query_batch_d256_f32";
		case 768:
			return @"dot_query_batch_d768_f32";
		case 1536:
			return @"dot_query_batch_d1536_f32";
		default:
			return nil;
	}
}

static NSString *metal_dot_fixed_dim_cosine_function_name(uint32_t dims) {
	switch (dims) {
		case 64:
			return @"cosine_query_batch_d64_f32";
		case 128:
			return @"cosine_query_batch_d128_f32";
		case 256:
			return @"cosine_query_batch_d256_f32";
		case 768:
			return @"cosine_query_batch_d768_f32";
		case 1536:
			return @"cosine_query_batch_d1536_f32";
		default:
			return nil;
	}
}

static NSString *metal_dot_tiled_variant_function_name(uint32_t variant) {
	switch (variant) {
		case 0:
			return @"dot_query_batch_tiled_q4_c32_d64_f32";
		case 1:
			return @"dot_query_batch_tiled_q4_c32_d128_f32";
		case 2:
			return @"dot_query_batch_tiled_q8_c16_d64_f32";
		case 3:
			return @"dot_query_batch_tiled_q8_c16_d128_f32";
		case 4:
			return @"dot_query_batch_tiled_q16_c8_d64_f32";
		case 5:
			return @"dot_query_batch_tiled_q16_c8_d128_f32";
		default:
			return nil;
	}
}

static uint32_t metal_dot_tiled_variant_queries(uint32_t variant) {
	switch (variant) {
		case 0:
		case 1:
			return 4;
		case 2:
		case 3:
			return 8;
		case 4:
		case 5:
			return 16;
		default:
			return 0;
	}
}

static uint32_t metal_dot_tiled_variant_candidates(uint32_t variant) {
	switch (variant) {
		case 0:
		case 1:
			return 32;
		case 2:
		case 3:
			return 16;
		case 4:
		case 5:
			return 8;
		default:
			return 0;
	}
}

static const char *metal_dot_shader_source =
	"#include <metal_stdlib>\n"
	"using namespace metal;\n"
	"kernel void dot_batch_f32(device const float *query [[buffer(0)]],\n"
	"                          device const float *candidates [[buffer(1)]],\n"
	"                          device float *dots [[buffer(2)]],\n"
	"                          constant uint &dims [[buffer(3)]],\n"
	"                          uint gid [[thread_position_in_grid]]) {\n"
	"    float sum = 0.0f;\n"
	"    uint base = gid * dims;\n"
	"    for (uint i = 0; i < dims; i++) {\n"
	"        sum += query[i] * candidates[base + i];\n"
	"    }\n"
	"    dots[gid] = sum;\n"
	"}\n"
	"kernel void dot_query_batch_f32(device const float *queries [[buffer(0)]],\n"
	"                                device const float *candidates [[buffer(1)]],\n"
	"                                device float *dots [[buffer(2)]],\n"
	"                                constant uint &dims [[buffer(3)]],\n"
	"                                constant uint &candidate_count [[buffer(4)]],\n"
	"                                uint2 gid [[thread_position_in_grid]]) {\n"
	"    uint candidate = gid.x;\n"
	"    uint query = gid.y;\n"
	"    float sum = 0.0f;\n"
	"    uint query_base = query * dims;\n"
	"    uint candidate_base = candidate * dims;\n"
	"    for (uint i = 0; i < dims; i++) {\n"
	"        sum += queries[query_base + i] * candidates[candidate_base + i];\n"
	"    }\n"
	"    dots[query * candidate_count + candidate] = sum;\n"
	"}\n"
	"#define DOT_TILE_Q 8u\n"
	"#define DOT_TILE_C 16u\n"
	"#define DOT_TILE_D 64u\n"
	"kernel void dot_query_batch_tiled_f32(device const float *queries [[buffer(0)]],\n"
	"                                      device const float *candidates [[buffer(1)]],\n"
	"                                      device float *dots [[buffer(2)]],\n"
	"                                      constant uint &dims [[buffer(3)]],\n"
	"                                      constant uint &candidate_count [[buffer(4)]],\n"
	"                                      constant uint &query_count [[buffer(5)]],\n"
	"                                      uint2 tid [[thread_position_in_threadgroup]],\n"
	"                                      uint2 tg [[threadgroup_position_in_grid]]) {\n"
	"    threadgroup float q_tile[DOT_TILE_Q][DOT_TILE_D];\n"
	"    threadgroup float c_tile[DOT_TILE_C][DOT_TILE_D];\n"
	"    uint local_c = tid.x;\n"
	"    uint local_q = tid.y;\n"
	"    uint lane = local_q * DOT_TILE_C + local_c;\n"
	"    uint candidate = tg.x * DOT_TILE_C + local_c;\n"
	"    uint query = tg.y * DOT_TILE_Q + local_q;\n"
	"    bool valid = query < query_count && candidate < candidate_count;\n"
	"    float sum = 0.0f;\n"
	"    for (uint d0 = 0; d0 < dims; d0 += DOT_TILE_D) {\n"
	"        uint chunk_dims = min(DOT_TILE_D, dims - d0);\n"
	"        for (uint idx = lane; idx < DOT_TILE_Q * DOT_TILE_D; idx += DOT_TILE_Q * DOT_TILE_C) {\n"
	"            uint q = idx / DOT_TILE_D;\n"
	"            uint d = idx - q * DOT_TILE_D;\n"
	"            uint global_q = tg.y * DOT_TILE_Q + q;\n"
	"            q_tile[q][d] = (global_q < query_count && d < chunk_dims) ? queries[global_q * dims + d0 + d] : 0.0f;\n"
	"        }\n"
	"        for (uint idx = lane; idx < DOT_TILE_C * DOT_TILE_D; idx += DOT_TILE_Q * DOT_TILE_C) {\n"
	"            uint c = idx / DOT_TILE_D;\n"
	"            uint d = idx - c * DOT_TILE_D;\n"
	"            uint global_c = tg.x * DOT_TILE_C + c;\n"
	"            c_tile[c][d] = (global_c < candidate_count && d < chunk_dims) ? candidates[global_c * dims + d0 + d] : 0.0f;\n"
	"        }\n"
	"        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
	"        if (valid) {\n"
	"            for (uint d = 0; d < chunk_dims; d++) {\n"
	"                sum += q_tile[local_q][d] * c_tile[local_c][d];\n"
	"            }\n"
	"        }\n"
	"        threadgroup_barrier(mem_flags::mem_threadgroup);\n"
	"    }\n"
	"    if (valid) {\n"
	"        dots[query * candidate_count + candidate] = sum;\n"
	"    }\n"
	"}\n"
	"#define DOT_QUERY_BATCH_TILED_VARIANT(NAME, TILE_Q_VALUE, TILE_C_VALUE, TILE_D_VALUE) \\\n"
	"kernel void NAME(device const float *queries [[buffer(0)]], \\\n"
	"                 device const float *candidates [[buffer(1)]], \\\n"
	"                 device float *dots [[buffer(2)]], \\\n"
	"                 constant uint &dims [[buffer(3)]], \\\n"
	"                 constant uint &candidate_count [[buffer(4)]], \\\n"
	"                 constant uint &query_count [[buffer(5)]], \\\n"
	"                 uint2 tid [[thread_position_in_threadgroup]], \\\n"
	"                 uint2 tg [[threadgroup_position_in_grid]]) { \\\n"
	"    threadgroup float q_tile[TILE_Q_VALUE][TILE_D_VALUE]; \\\n"
	"    threadgroup float c_tile[TILE_C_VALUE][TILE_D_VALUE]; \\\n"
	"    uint local_c = tid.x; \\\n"
	"    uint local_q = tid.y; \\\n"
	"    uint lane = local_q * TILE_C_VALUE + local_c; \\\n"
	"    uint candidate = tg.x * TILE_C_VALUE + local_c; \\\n"
	"    uint query = tg.y * TILE_Q_VALUE + local_q; \\\n"
	"    bool valid = query < query_count && candidate < candidate_count; \\\n"
	"    float sum = 0.0f; \\\n"
	"    for (uint d0 = 0; d0 < dims; d0 += TILE_D_VALUE) { \\\n"
	"        uint chunk_dims = min(TILE_D_VALUE, dims - d0); \\\n"
	"        for (uint idx = lane; idx < TILE_Q_VALUE * TILE_D_VALUE; idx += TILE_Q_VALUE * TILE_C_VALUE) { \\\n"
	"            uint q = idx / TILE_D_VALUE; \\\n"
	"            uint d = idx - q * TILE_D_VALUE; \\\n"
	"            uint global_q = tg.y * TILE_Q_VALUE + q; \\\n"
	"            q_tile[q][d] = (global_q < query_count && d < chunk_dims) ? queries[global_q * dims + d0 + d] : 0.0f; \\\n"
	"        } \\\n"
	"        for (uint idx = lane; idx < TILE_C_VALUE * TILE_D_VALUE; idx += TILE_Q_VALUE * TILE_C_VALUE) { \\\n"
	"            uint c = idx / TILE_D_VALUE; \\\n"
	"            uint d = idx - c * TILE_D_VALUE; \\\n"
	"            uint global_c = tg.x * TILE_C_VALUE + c; \\\n"
	"            c_tile[c][d] = (global_c < candidate_count && d < chunk_dims) ? candidates[global_c * dims + d0 + d] : 0.0f; \\\n"
	"        } \\\n"
	"        threadgroup_barrier(mem_flags::mem_threadgroup); \\\n"
	"        if (valid) { \\\n"
	"            for (uint d = 0; d < chunk_dims; d++) { \\\n"
	"                sum += q_tile[local_q][d] * c_tile[local_c][d]; \\\n"
	"            } \\\n"
	"        } \\\n"
	"        threadgroup_barrier(mem_flags::mem_threadgroup); \\\n"
	"    } \\\n"
	"    if (valid) { \\\n"
	"        dots[query * candidate_count + candidate] = sum; \\\n"
	"    } \\\n"
	"}\n"
	"DOT_QUERY_BATCH_TILED_VARIANT(dot_query_batch_tiled_q4_c32_d64_f32, 4u, 32u, 64u)\n"
	"DOT_QUERY_BATCH_TILED_VARIANT(dot_query_batch_tiled_q4_c32_d128_f32, 4u, 32u, 128u)\n"
	"DOT_QUERY_BATCH_TILED_VARIANT(dot_query_batch_tiled_q8_c16_d64_f32, 8u, 16u, 64u)\n"
	"DOT_QUERY_BATCH_TILED_VARIANT(dot_query_batch_tiled_q8_c16_d128_f32, 8u, 16u, 128u)\n"
	"DOT_QUERY_BATCH_TILED_VARIANT(dot_query_batch_tiled_q16_c8_d64_f32, 16u, 8u, 64u)\n"
	"DOT_QUERY_BATCH_TILED_VARIANT(dot_query_batch_tiled_q16_c8_d128_f32, 16u, 8u, 128u)\n"
	"kernel void dot_query_batch_simd_f32(device const float *queries [[buffer(0)]],\n"
	"                                     device const float *candidates [[buffer(1)]],\n"
	"                                     device float *dots [[buffer(2)]],\n"
	"                                     constant uint &dims [[buffer(3)]],\n"
	"                                     constant uint &candidate_count [[buffer(4)]],\n"
	"                                     constant uint &query_count [[buffer(5)]],\n"
	"                                     constant uint &simdgroups_per_threadgroup [[buffer(6)]],\n"
	"                                     uint3 tg [[threadgroup_position_in_grid]],\n"
	"                                     uint simd_group [[simdgroup_index_in_threadgroup]],\n"
	"                                     uint simd_lane [[thread_index_in_simdgroup]],\n"
	"                                     uint simd_width [[threads_per_simdgroup]]) {\n"
	"    uint dot = tg.x * simdgroups_per_threadgroup + simd_group;\n"
	"    uint dot_count = query_count * candidate_count;\n"
	"    float partial = 0.0f;\n"
	"    if (dot < dot_count) {\n"
	"        uint query = dot / candidate_count;\n"
	"        uint candidate = dot - query * candidate_count;\n"
	"        uint query_base = query * dims;\n"
	"        uint candidate_base = candidate * dims;\n"
	"        for (uint d = simd_lane; d < dims; d += simd_width) {\n"
	"            partial += queries[query_base + d] * candidates[candidate_base + d];\n"
	"        }\n"
	"    }\n"
	"    float sum = simd_sum(partial);\n"
	"    if (simd_lane == 0 && dot < dot_count) {\n"
	"        dots[dot] = sum;\n"
	"    }\n"
	"}\n"
	"#define DOT_QUERY_BATCH_FIXED(NAME, DIMS_VALUE) \\\n"
	"kernel void NAME(device const float *queries [[buffer(0)]], \\\n"
	"                 device const float *candidates [[buffer(1)]], \\\n"
	"                 device float *dots [[buffer(2)]], \\\n"
	"                 constant uint &candidate_count [[buffer(3)]], \\\n"
	"                 uint2 gid [[thread_position_in_grid]]) { \\\n"
	"    uint candidate = gid.x; \\\n"
	"    uint query = gid.y; \\\n"
	"    float sum = 0.0f; \\\n"
	"    uint query_base = query * DIMS_VALUE; \\\n"
	"    uint candidate_base = candidate * DIMS_VALUE; \\\n"
	"    for (uint i = 0; i < DIMS_VALUE; i++) { \\\n"
	"        sum += queries[query_base + i] * candidates[candidate_base + i]; \\\n"
	"    } \\\n"
	"    dots[query * candidate_count + candidate] = sum; \\\n"
	"}\n"
	"DOT_QUERY_BATCH_FIXED(dot_query_batch_d64_f32, 64u)\n"
		"DOT_QUERY_BATCH_FIXED(dot_query_batch_d128_f32, 128u)\n"
		"DOT_QUERY_BATCH_FIXED(dot_query_batch_d256_f32, 256u)\n"
		"DOT_QUERY_BATCH_FIXED(dot_query_batch_d768_f32, 768u)\n"
		"DOT_QUERY_BATCH_FIXED(dot_query_batch_d1536_f32, 1536u)\n"
		"#define COSINE_QUERY_BATCH_FIXED(NAME, DIMS_VALUE) \\\n"
		"kernel void NAME(device const float *queries [[buffer(0)]], \\\n"
		"                 device const float *candidates [[buffer(1)]], \\\n"
		"                 device const float *query_inv_norms [[buffer(2)]], \\\n"
		"                 device const float *candidate_inv_norms [[buffer(3)]], \\\n"
		"                 device float *distances [[buffer(4)]], \\\n"
		"                 constant uint &candidate_count [[buffer(5)]], \\\n"
		"                 uint2 gid [[thread_position_in_grid]]) { \\\n"
		"    uint candidate = gid.x; \\\n"
		"    uint query = gid.y; \\\n"
		"    float sum = 0.0f; \\\n"
		"    uint query_base = query * DIMS_VALUE; \\\n"
		"    uint candidate_base = candidate * DIMS_VALUE; \\\n"
		"    for (uint i = 0; i < DIMS_VALUE; i++) { \\\n"
		"        sum += queries[query_base + i] * candidates[candidate_base + i]; \\\n"
		"    } \\\n"
		"    distances[query * candidate_count + candidate] = 1.0f - sum * query_inv_norms[query] * candidate_inv_norms[candidate]; \\\n"
		"}\n"
		"COSINE_QUERY_BATCH_FIXED(cosine_query_batch_d64_f32, 64u)\n"
		"COSINE_QUERY_BATCH_FIXED(cosine_query_batch_d128_f32, 128u)\n"
		"COSINE_QUERY_BATCH_FIXED(cosine_query_batch_d256_f32, 256u)\n"
		"COSINE_QUERY_BATCH_FIXED(cosine_query_batch_d768_f32, 768u)\n"
		"COSINE_QUERY_BATCH_FIXED(cosine_query_batch_d1536_f32, 1536u)\n"
		"kernel void cosine_query_batch_f32(device const float *queries [[buffer(0)]],\n"
		"                                  device const float *candidates [[buffer(1)]],\n"
		"                                  device const float *query_inv_norms [[buffer(2)]],\n"
	"                                  device const float *candidate_inv_norms [[buffer(3)]],\n"
	"                                  device float *distances [[buffer(4)]],\n"
	"                                  constant uint &dims [[buffer(5)]],\n"
	"                                  constant uint &candidate_count [[buffer(6)]],\n"
	"                                  uint2 gid [[thread_position_in_grid]]) {\n"
	"    uint candidate = gid.x;\n"
	"    uint query = gid.y;\n"
	"    float sum = 0.0f;\n"
	"    uint query_base = query * dims;\n"
	"    uint candidate_base = candidate * dims;\n"
		"    for (uint i = 0; i < dims; i++) {\n"
		"        sum += queries[query_base + i] * candidates[candidate_base + i];\n"
		"    }\n"
		"    distances[query * candidate_count + candidate] = 1.0f - sum * query_inv_norms[query] * candidate_inv_norms[candidate];\n"
		"}\n"
		"#define TOPK_MAX 16u\n"
		"#define TOPK_THREADS 256u\n"
		"#define TOPK_BLOCK_CANDIDATES 1024u\n"
		"#define TOPK_EMPTY_INDEX 0xffffffffu\n"
		"static inline void topk_insert(thread float scores[TOPK_MAX], thread uint indices[TOPK_MAX], uint top_k, float score, uint index) {\n"
		"    if (score >= scores[top_k - 1]) {\n"
		"        return;\n"
		"    }\n"
		"    uint pos = top_k - 1;\n"
		"    while (pos > 0 && score < scores[pos - 1]) {\n"
		"        scores[pos] = scores[pos - 1];\n"
		"        indices[pos] = indices[pos - 1];\n"
		"        pos--;\n"
		"    }\n"
		"    scores[pos] = score;\n"
		"    indices[pos] = index;\n"
		"}\n"
		"kernel void cosine_query_batch_block_topk_f32(device const float *queries [[buffer(0)]],\n"
		"                                             device const float *candidates [[buffer(1)]],\n"
		"                                             device const float *query_inv_norms [[buffer(2)]],\n"
		"                                             device const float *candidate_inv_norms [[buffer(3)]],\n"
		"                                             device float *out_scores [[buffer(4)]],\n"
		"                                             device uint *out_indices [[buffer(5)]],\n"
		"                                             constant uint &dims [[buffer(6)]],\n"
		"                                             constant uint &candidate_count [[buffer(7)]],\n"
		"                                             constant uint &top_k [[buffer(8)]],\n"
		"                                             constant uint &block_count [[buffer(9)]],\n"
		"                                             uint2 tg [[threadgroup_position_in_grid]],\n"
		"                                             uint lane [[thread_index_in_threadgroup]]) {\n"
		"    threadgroup float local_scores[TOPK_THREADS][TOPK_MAX];\n"
		"    threadgroup uint local_indices[TOPK_THREADS][TOPK_MAX];\n"
		"    float best_scores[TOPK_MAX];\n"
		"    uint best_indices[TOPK_MAX];\n"
		"    for (uint k = 0; k < TOPK_MAX; k++) {\n"
		"        best_scores[k] = 3.402823466e+38f;\n"
		"        best_indices[k] = TOPK_EMPTY_INDEX;\n"
		"    }\n"
		"    uint block = tg.x;\n"
		"    uint query = tg.y;\n"
		"    uint block_start = block * TOPK_BLOCK_CANDIDATES;\n"
		"    uint block_end = min(block_start + TOPK_BLOCK_CANDIDATES, candidate_count);\n"
		"    uint query_base = query * dims;\n"
		"    float query_inv_norm = query_inv_norms[query];\n"
		"    for (uint candidate = block_start + lane; candidate < block_end; candidate += TOPK_THREADS) {\n"
		"        uint candidate_base = candidate * dims;\n"
		"        float sum = 0.0f;\n"
		"        for (uint d = 0; d < dims; d++) {\n"
		"            sum += queries[query_base + d] * candidates[candidate_base + d];\n"
		"        }\n"
		"        float distance = 1.0f - sum * query_inv_norm * candidate_inv_norms[candidate];\n"
		"        topk_insert(best_scores, best_indices, top_k, distance, candidate);\n"
		"    }\n"
		"    for (uint k = 0; k < TOPK_MAX; k++) {\n"
		"        local_scores[lane][k] = best_scores[k];\n"
		"        local_indices[lane][k] = best_indices[k];\n"
		"    }\n"
		"    threadgroup_barrier(mem_flags::mem_threadgroup);\n"
		"    if (lane == 0) {\n"
		"        float merged_scores[TOPK_MAX];\n"
		"        uint merged_indices[TOPK_MAX];\n"
		"        for (uint k = 0; k < TOPK_MAX; k++) {\n"
		"            merged_scores[k] = 3.402823466e+38f;\n"
		"            merged_indices[k] = TOPK_EMPTY_INDEX;\n"
		"        }\n"
		"        for (uint l = 0; l < TOPK_THREADS; l++) {\n"
		"            for (uint k = 0; k < top_k; k++) {\n"
		"                topk_insert(merged_scores, merged_indices, top_k, local_scores[l][k], local_indices[l][k]);\n"
		"            }\n"
		"        }\n"
		"        uint out_base = (query * block_count + block) * top_k;\n"
		"        for (uint k = 0; k < top_k; k++) {\n"
		"            out_scores[out_base + k] = merged_scores[k];\n"
		"            out_indices[out_base + k] = merged_indices[k];\n"
		"        }\n"
		"    }\n"
		"}\n";

MetalDotContext *metal_dot_create(uint32_t dims, uint32_t candidate_count, char *err, int err_len) {
	@autoreleasepool {
		if (dims == 0) {
			metal_dot_set_error_c(err, err_len, "Metal dot dims must be positive");
			return NULL;
		}
		if (candidate_count == 0) {
			metal_dot_set_error_c(err, err_len, "Metal dot candidate count must be positive");
			return NULL;
		}

		id<MTLDevice> device = MTLCreateSystemDefaultDevice();
		if (device == nil) {
			metal_dot_set_error_c(err, err_len, "no default Metal device");
			return NULL;
		}

		GMMetalDotContext *ctx = [[GMMetalDotContext alloc] init];
		ctx->device = [device retain];
		ctx->dims = dims;
		ctx->candidateCount = candidate_count;

		ctx->queue = [ctx->device newCommandQueue];
		if (ctx->queue == nil) {
			metal_dot_set_error_c(err, err_len, "failed to create Metal command queue");
			[ctx release];
			return NULL;
		}

		NSError *library_error = nil;
		NSString *source = [NSString stringWithUTF8String:metal_dot_shader_source];
		id<MTLLibrary> library = [ctx->device newLibraryWithSource:source options:nil error:&library_error];
		if (library == nil) {
			metal_dot_set_error(err, err_len, library_error.localizedDescription);
			[ctx release];
			return NULL;
		}

		id<MTLFunction> function = [library newFunctionWithName:@"dot_batch_f32"];
		if (function == nil) {
			metal_dot_set_error_c(err, err_len, "failed to load Metal dot_batch_f32 function");
			[library release];
			[ctx release];
			return NULL;
		}

		NSError *pipeline_error = nil;
		ctx->pipeline = [ctx->device newComputePipelineStateWithFunction:function error:&pipeline_error];
		[function release];
		if (ctx->pipeline == nil) {
			metal_dot_set_error(err, err_len, pipeline_error.localizedDescription);
			[library release];
			[ctx release];
			return NULL;
		}

		id<MTLFunction> query_batch_function = [library newFunctionWithName:@"dot_query_batch_f32"];
		if (query_batch_function == nil) {
			metal_dot_set_error_c(err, err_len, "failed to load Metal dot_query_batch_f32 function");
			[library release];
			[ctx release];
			return NULL;
		}

		NSError *query_batch_pipeline_error = nil;
		ctx->queryBatchPipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_function error:&query_batch_pipeline_error];
		[query_batch_function release];
		if (ctx->queryBatchPipeline == nil) {
			metal_dot_set_error(err, err_len, query_batch_pipeline_error.localizedDescription);
			[library release];
			[ctx release];
			return NULL;
		}

		id<MTLFunction> query_batch_tiled_function = [library newFunctionWithName:@"dot_query_batch_tiled_f32"];
		if (query_batch_tiled_function == nil) {
			metal_dot_set_error_c(err, err_len, "failed to load Metal dot_query_batch_tiled_f32 function");
			[library release];
			[ctx release];
			return NULL;
		}

		NSError *query_batch_tiled_pipeline_error = nil;
		ctx->queryBatchTiledPipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_tiled_function error:&query_batch_tiled_pipeline_error];
		[query_batch_tiled_function release];
		if (ctx->queryBatchTiledPipeline == nil) {
			metal_dot_set_error(err, err_len, query_batch_tiled_pipeline_error.localizedDescription);
			[library release];
			[ctx release];
			return NULL;
		}

		for (uint32_t variant = 0; variant < METAL_DOT_TILED_VARIANT_COUNT; variant++) {
			NSString *variant_function_name = metal_dot_tiled_variant_function_name(variant);
			id<MTLFunction> query_batch_tiled_variant_function = [library newFunctionWithName:variant_function_name];
			if (query_batch_tiled_variant_function == nil) {
				metal_dot_set_error_c(err, err_len, "failed to load Metal tiled query-batch variant function");
				[library release];
				[ctx release];
				return NULL;
			}

			NSError *query_batch_tiled_variant_pipeline_error = nil;
			ctx->queryBatchTiledVariantPipelines[variant] = [ctx->device newComputePipelineStateWithFunction:query_batch_tiled_variant_function error:&query_batch_tiled_variant_pipeline_error];
			[query_batch_tiled_variant_function release];
			if (ctx->queryBatchTiledVariantPipelines[variant] == nil) {
				metal_dot_set_error(err, err_len, query_batch_tiled_variant_pipeline_error.localizedDescription);
				[library release];
				[ctx release];
				return NULL;
			}
		}

		id<MTLFunction> query_batch_simd_function = [library newFunctionWithName:@"dot_query_batch_simd_f32"];
		if (query_batch_simd_function == nil) {
			metal_dot_set_error_c(err, err_len, "failed to load Metal dot_query_batch_simd_f32 function");
			[library release];
			[ctx release];
			return NULL;
		}

		NSError *query_batch_simd_pipeline_error = nil;
		ctx->queryBatchSIMDPipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_simd_function error:&query_batch_simd_pipeline_error];
		[query_batch_simd_function release];
		if (ctx->queryBatchSIMDPipeline == nil) {
			metal_dot_set_error(err, err_len, query_batch_simd_pipeline_error.localizedDescription);
			[library release];
			[ctx release];
			return NULL;
		}

		id<MTLFunction> query_batch_cosine_function = [library newFunctionWithName:@"cosine_query_batch_f32"];
		if (query_batch_cosine_function == nil) {
			metal_dot_set_error_c(err, err_len, "failed to load Metal cosine_query_batch_f32 function");
			[library release];
			[ctx release];
			return NULL;
		}

		NSError *query_batch_cosine_pipeline_error = nil;
		ctx->queryBatchCosinePipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_cosine_function error:&query_batch_cosine_pipeline_error];
		[query_batch_cosine_function release];
			if (ctx->queryBatchCosinePipeline == nil) {
				metal_dot_set_error(err, err_len, query_batch_cosine_pipeline_error.localizedDescription);
				[library release];
				[ctx release];
				return NULL;
			}

			id<MTLFunction> query_batch_topk_function = [library newFunctionWithName:@"cosine_query_batch_block_topk_f32"];
			if (query_batch_topk_function == nil) {
				metal_dot_set_error_c(err, err_len, "failed to load Metal cosine_query_batch_block_topk_f32 function");
				[library release];
				[ctx release];
				return NULL;
			}

			NSError *query_batch_topk_pipeline_error = nil;
			ctx->queryBatchTopKPipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_topk_function error:&query_batch_topk_pipeline_error];
			[query_batch_topk_function release];
			if (ctx->queryBatchTopKPipeline == nil) {
				metal_dot_set_error(err, err_len, query_batch_topk_pipeline_error.localizedDescription);
				[library release];
				[ctx release];
				return NULL;
			}

			NSString *fixed_dim_function_name = metal_dot_fixed_dim_function_name(dims);
			if (fixed_dim_function_name != nil) {
				id<MTLFunction> query_batch_fixed_dim_function = [library newFunctionWithName:fixed_dim_function_name];
			if (query_batch_fixed_dim_function == nil) {
				metal_dot_set_error_c(err, err_len, "failed to load fixed-dimension Metal query-batch function");
				[library release];
				[ctx release];
				return NULL;
			}

			NSError *query_batch_fixed_dim_pipeline_error = nil;
			ctx->queryBatchFixedDimPipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_fixed_dim_function error:&query_batch_fixed_dim_pipeline_error];
			[query_batch_fixed_dim_function release];
				if (ctx->queryBatchFixedDimPipeline == nil) {
					metal_dot_set_error(err, err_len, query_batch_fixed_dim_pipeline_error.localizedDescription);
					[library release];
					[ctx release];
					return NULL;
				}

				NSString *fixed_dim_cosine_function_name = metal_dot_fixed_dim_cosine_function_name(dims);
				id<MTLFunction> query_batch_fixed_dim_cosine_function = [library newFunctionWithName:fixed_dim_cosine_function_name];
				if (query_batch_fixed_dim_cosine_function == nil) {
					metal_dot_set_error_c(err, err_len, "failed to load fixed-dimension Metal cosine query-batch function");
					[library release];
					[ctx release];
					return NULL;
				}

				NSError *query_batch_fixed_dim_cosine_pipeline_error = nil;
				ctx->queryBatchFixedDimCosinePipeline = [ctx->device newComputePipelineStateWithFunction:query_batch_fixed_dim_cosine_function error:&query_batch_fixed_dim_cosine_pipeline_error];
				[query_batch_fixed_dim_cosine_function release];
				if (ctx->queryBatchFixedDimCosinePipeline == nil) {
					metal_dot_set_error(err, err_len, query_batch_fixed_dim_cosine_pipeline_error.localizedDescription);
					[library release];
					[ctx release];
					return NULL;
				}
			}
		[library release];

		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;
		NSUInteger query_bytes = (NSUInteger)dims * sizeof(float);
		NSUInteger candidates_bytes = (NSUInteger)dims * (NSUInteger)candidate_count * sizeof(float);
		NSUInteger candidate_inv_norms_bytes = (NSUInteger)candidate_count * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)candidate_count * sizeof(float);

		ctx->queryBuffer = [ctx->device newBufferWithLength:query_bytes options:buffer_options];
		ctx->candidatesBuffer = [ctx->device newBufferWithLength:candidates_bytes options:buffer_options];
		ctx->candidateInvNormsBuffer = [ctx->device newBufferWithLength:candidate_inv_norms_bytes options:buffer_options];
		ctx->dotsBuffer = [ctx->device newBufferWithLength:dots_bytes options:buffer_options];
		ctx->dimsBuffer = [ctx->device newBufferWithBytes:&dims length:sizeof(dims) options:buffer_options];
		ctx->candidateCountBuffer = [ctx->device newBufferWithBytes:&candidate_count length:sizeof(candidate_count) options:buffer_options];
		if (ctx->queryBuffer == nil || ctx->candidatesBuffer == nil || ctx->candidateInvNormsBuffer == nil || ctx->dotsBuffer == nil || ctx->dimsBuffer == nil || ctx->candidateCountBuffer == nil) {
			metal_dot_set_error_c(err, err_len, "failed to create Metal buffers");
			[ctx release];
			return NULL;
		}

		return (MetalDotContext *)ctx;
	}
}

void metal_dot_destroy(MetalDotContext *opaque) {
	if (opaque == NULL) {
		return;
	}
	GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
	[ctx release];
}

int metal_dot_load_candidates(MetalDotContext *opaque, const float *candidates, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (candidates == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot candidates are nil");
			return 0;
		}
		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger candidates_bytes = (NSUInteger)ctx->dims * (NSUInteger)ctx->candidateCount * sizeof(float);
		memcpy([ctx->candidatesBuffer contents], candidates, candidates_bytes);
		[ctx->candidatesBuffer didModifyRange:NSMakeRange(0, candidates_bytes)];
		return 1;
	}
}

int metal_dot_load_candidate_inv_norms(MetalDotContext *opaque, const float *candidate_inv_norms, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (candidate_inv_norms == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot candidate inverse norms are nil");
			return 0;
		}
		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger candidate_inv_norms_bytes = (NSUInteger)ctx->candidateCount * sizeof(float);
		memcpy([ctx->candidateInvNormsBuffer contents], candidate_inv_norms, candidate_inv_norms_bytes);
		[ctx->candidateInvNormsBuffer didModifyRange:NSMakeRange(0, candidate_inv_norms_bytes)];
		return 1;
	}
}

int metal_dot_run_loaded(MetalDotContext *opaque, const float *query, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (query == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot query is nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger query_bytes = (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)ctx->candidateCount * sizeof(float);

		memcpy([ctx->queryBuffer contents], query, query_bytes);
		[ctx->queryBuffer didModifyRange:NSMakeRange(0, query_bytes)];

		id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
		if (command_buffer == nil) {
			metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
			return 0;
		}

		id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
		if (encoder == nil) {
			metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
			return 0;
		}

		[encoder setComputePipelineState:ctx->pipeline];
		[encoder setBuffer:ctx->queryBuffer offset:0 atIndex:0];
		[encoder setBuffer:ctx->candidatesBuffer offset:0 atIndex:1];
		[encoder setBuffer:ctx->dotsBuffer offset:0 atIndex:2];
		[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];

		NSUInteger threads_per_group = ctx->pipeline.threadExecutionWidth;
		if (threads_per_group == 0) {
			threads_per_group = 1;
		}
		if (threads_per_group > ctx->candidateCount) {
			threads_per_group = ctx->candidateCount;
		}
		MTLSize grid_size = MTLSizeMake(ctx->candidateCount, 1, 1);
		MTLSize threadgroup_size = MTLSizeMake(threads_per_group, 1, 1);
		[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
		[encoder endEncoding];

		[command_buffer commit];
		[command_buffer waitUntilCompleted];

		if (command_buffer.status == MTLCommandBufferStatusError) {
			NSString *message = command_buffer.error.localizedDescription;
			metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
			return 0;
		}

		memcpy(dots, [ctx->dotsBuffer contents], dots_bytes);
		return 1;
	}
}

int metal_dot_run_loaded_no_copy_output(MetalDotContext *opaque, const float *query, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (query == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot query is nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger query_bytes = (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)ctx->candidateCount * sizeof(float);

		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:MTLResourceStorageModeShared deallocator:nil];
		if (dots_buffer == nil) {
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal output buffer");
			return 0;
		}

		memcpy([ctx->queryBuffer contents], query, query_bytes);
		[ctx->queryBuffer didModifyRange:NSMakeRange(0, query_bytes)];

		id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
		if (command_buffer == nil) {
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
			return 0;
		}

		id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
		if (encoder == nil) {
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
			return 0;
		}

		[encoder setComputePipelineState:ctx->pipeline];
		[encoder setBuffer:ctx->queryBuffer offset:0 atIndex:0];
		[encoder setBuffer:ctx->candidatesBuffer offset:0 atIndex:1];
		[encoder setBuffer:dots_buffer offset:0 atIndex:2];
		[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];

		NSUInteger threads_per_group = ctx->pipeline.threadExecutionWidth;
		if (threads_per_group == 0) {
			threads_per_group = 1;
		}
		if (threads_per_group > ctx->candidateCount) {
			threads_per_group = ctx->candidateCount;
		}
		MTLSize grid_size = MTLSizeMake(ctx->candidateCount, 1, 1);
		MTLSize threadgroup_size = MTLSizeMake(threads_per_group, 1, 1);
		[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
		[encoder endEncoding];

		[command_buffer commit];
		[command_buffer waitUntilCompleted];

		[dots_buffer release];

		if (command_buffer.status == MTLCommandBufferStatusError) {
			NSString *message = command_buffer.error.localizedDescription;
			metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
			return 0;
		}
		return 1;
	}
}

int metal_dot_run_no_copy(MetalDotContext *opaque, const float *query, const float *candidates, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (query == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot query is nil");
			return 0;
		}
		if (candidates == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot candidates are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger query_bytes = (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger candidates_bytes = (NSUInteger)ctx->dims * (NSUInteger)ctx->candidateCount * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> query_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)query length:query_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> candidates_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)candidates length:candidates_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (query_buffer == nil || candidates_buffer == nil || dots_buffer == nil) {
			[query_buffer release];
			[candidates_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal buffers");
			return 0;
		}

		id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
		if (command_buffer == nil) {
			[query_buffer release];
			[candidates_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
			return 0;
		}

		id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
		if (encoder == nil) {
			[query_buffer release];
			[candidates_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
			return 0;
		}

		[encoder setComputePipelineState:ctx->pipeline];
		[encoder setBuffer:query_buffer offset:0 atIndex:0];
		[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
		[encoder setBuffer:dots_buffer offset:0 atIndex:2];
		[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];

		NSUInteger threads_per_group = ctx->pipeline.threadExecutionWidth;
		if (threads_per_group == 0) {
			threads_per_group = 1;
		}
		if (threads_per_group > ctx->candidateCount) {
			threads_per_group = ctx->candidateCount;
		}
		MTLSize grid_size = MTLSizeMake(ctx->candidateCount, 1, 1);
		MTLSize threadgroup_size = MTLSizeMake(threads_per_group, 1, 1);
		[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
		[encoder endEncoding];

		[command_buffer commit];
		[command_buffer waitUntilCompleted];

		[query_buffer release];
		[candidates_buffer release];
		[dots_buffer release];

		if (command_buffer.status == MTLCommandBufferStatusError) {
			NSString *message = command_buffer.error.localizedDescription;
			metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
			return 0;
		}
		return 1;
	}
}

static int metal_dot_dispatch_query_batch(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> candidates_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchPipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
	[encoder setBuffer:dots_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:4];

	NSUInteger width = ctx->queryBatchPipeline.threadExecutionWidth;
	if (width == 0) {
		width = 1;
	}
	if (width > ctx->candidateCount) {
		width = ctx->candidateCount;
	}
	NSUInteger max_threads = ctx->queryBatchPipeline.maxTotalThreadsPerThreadgroup;
	if (max_threads == 0) {
		max_threads = width;
	}
	NSUInteger height = max_threads / width;
	if (height == 0) {
		height = 1;
	}
	if (height > query_count) {
		height = query_count;
	}

	MTLSize grid_size = MTLSizeMake(ctx->candidateCount, query_count, 1);
	MTLSize threadgroup_size = MTLSizeMake(width, height, 1);
	[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_tiled(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> candidates_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}

	static const NSUInteger tile_candidates = 16;
	static const NSUInteger tile_queries = 8;
	static const NSUInteger threads_per_tile = tile_candidates * tile_queries;
	if (ctx->queryBatchTiledPipeline.maxTotalThreadsPerThreadgroup < threads_per_tile) {
		metal_dot_set_error_c(err, err_len, "Metal dot tiled query-batch pipeline does not support the required threadgroup size");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchTiledPipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
	[encoder setBuffer:dots_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:4];
	[encoder setBytes:&query_count length:sizeof(query_count) atIndex:5];

	MTLSize threadgroups = MTLSizeMake(
		((NSUInteger)ctx->candidateCount + tile_candidates - 1) / tile_candidates,
		((NSUInteger)query_count + tile_queries - 1) / tile_queries,
		1
	);
	MTLSize threadgroup_size = MTLSizeMake(tile_candidates, tile_queries, 1);
	[encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_tiled_variant(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> candidates_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, uint32_t variant, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}
	if (variant >= METAL_DOT_TILED_VARIANT_COUNT || ctx->queryBatchTiledVariantPipelines[variant] == nil) {
		metal_dot_set_error_c(err, err_len, "Metal dot tiled query-batch variant is unavailable");
		return 0;
	}

	uint32_t tile_queries_u32 = metal_dot_tiled_variant_queries(variant);
	uint32_t tile_candidates_u32 = metal_dot_tiled_variant_candidates(variant);
	if (tile_queries_u32 == 0 || tile_candidates_u32 == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot tiled query-batch variant has invalid tile shape");
		return 0;
	}
	NSUInteger tile_queries = tile_queries_u32;
	NSUInteger tile_candidates = tile_candidates_u32;
	NSUInteger threads_per_tile = tile_candidates * tile_queries;
	id<MTLComputePipelineState> variant_pipeline = ctx->queryBatchTiledVariantPipelines[variant];
	if (variant_pipeline.maxTotalThreadsPerThreadgroup < threads_per_tile) {
		metal_dot_set_error_c(err, err_len, "Metal dot tiled query-batch variant pipeline does not support the required threadgroup size");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:variant_pipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
	[encoder setBuffer:dots_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:4];
	[encoder setBytes:&query_count length:sizeof(query_count) atIndex:5];

	MTLSize threadgroups = MTLSizeMake(
		((NSUInteger)ctx->candidateCount + tile_candidates - 1) / tile_candidates,
		((NSUInteger)query_count + tile_queries - 1) / tile_queries,
		1
	);
	MTLSize threadgroup_size = MTLSizeMake(tile_candidates, tile_queries, 1);
	[encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_simd(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> candidates_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}

	NSUInteger simd_width = ctx->queryBatchSIMDPipeline.threadExecutionWidth;
	if (simd_width == 0) {
		simd_width = 1;
	}
	NSUInteger max_threads = ctx->queryBatchSIMDPipeline.maxTotalThreadsPerThreadgroup;
	if (max_threads < simd_width) {
		metal_dot_set_error_c(err, err_len, "Metal dot SIMD query-batch pipeline does not support the required threadgroup size");
		return 0;
	}

	NSUInteger simdgroups_per_threadgroup_ns = max_threads / simd_width;
	if (simdgroups_per_threadgroup_ns > 8) {
		simdgroups_per_threadgroup_ns = 8;
	}
	if (simdgroups_per_threadgroup_ns == 0) {
		simdgroups_per_threadgroup_ns = 1;
	}
	uint32_t simdgroups_per_threadgroup = (uint32_t)simdgroups_per_threadgroup_ns;
	NSUInteger threads_per_threadgroup = simd_width * simdgroups_per_threadgroup_ns;
	NSUInteger dot_count = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount;

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchSIMDPipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
	[encoder setBuffer:dots_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:4];
	[encoder setBytes:&query_count length:sizeof(query_count) atIndex:5];
	[encoder setBytes:&simdgroups_per_threadgroup length:sizeof(simdgroups_per_threadgroup) atIndex:6];

	MTLSize threadgroups = MTLSizeMake(
		(dot_count + simdgroups_per_threadgroup_ns - 1) / simdgroups_per_threadgroup_ns,
		1,
		1
	);
	MTLSize threadgroup_size = MTLSizeMake(threads_per_threadgroup, 1, 1);
	[encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_fixed_dim(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> candidates_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}
	if (ctx->queryBatchFixedDimPipeline == nil) {
		metal_dot_set_error_c(err, err_len, "Metal dot fixed-dimension query-batch pipeline is unavailable for this dimension");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchFixedDimPipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
	[encoder setBuffer:dots_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:3];

	NSUInteger width = ctx->queryBatchFixedDimPipeline.threadExecutionWidth;
	if (width == 0) {
		width = 1;
	}
	if (width > ctx->candidateCount) {
		width = ctx->candidateCount;
	}
	NSUInteger max_threads = ctx->queryBatchFixedDimPipeline.maxTotalThreadsPerThreadgroup;
	if (max_threads == 0) {
		max_threads = width;
	}
	NSUInteger height = max_threads / width;
	if (height == 0) {
		height = 1;
	}
	if (height > query_count) {
		height = query_count;
	}

	MTLSize grid_size = MTLSizeMake(ctx->candidateCount, query_count, 1);
	MTLSize threadgroup_size = MTLSizeMake(width, height, 1);
	[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_cosine(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> query_inv_norms_buffer, id<MTLBuffer> distances_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchCosinePipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:ctx->candidatesBuffer offset:0 atIndex:1];
	[encoder setBuffer:query_inv_norms_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->candidateInvNormsBuffer offset:0 atIndex:3];
	[encoder setBuffer:distances_buffer offset:0 atIndex:4];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:5];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:6];

	NSUInteger width = ctx->queryBatchCosinePipeline.threadExecutionWidth;
	if (width == 0) {
		width = 1;
	}
	if (width > ctx->candidateCount) {
		width = ctx->candidateCount;
	}
	NSUInteger max_threads = ctx->queryBatchCosinePipeline.maxTotalThreadsPerThreadgroup;
	if (max_threads == 0) {
		max_threads = width;
	}
	NSUInteger height = max_threads / width;
	if (height == 0) {
		height = 1;
	}
	if (height > query_count) {
		height = query_count;
	}

	MTLSize grid_size = MTLSizeMake(ctx->candidateCount, query_count, 1);
	MTLSize threadgroup_size = MTLSizeMake(width, height, 1);
	[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_fixed_dim_cosine(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> query_inv_norms_buffer, id<MTLBuffer> distances_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}
	if (ctx->queryBatchFixedDimCosinePipeline == nil) {
		metal_dot_set_error_c(err, err_len, "Metal dot fixed-dimension cosine query-batch pipeline is unavailable for this dimension");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchFixedDimCosinePipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:ctx->candidatesBuffer offset:0 atIndex:1];
	[encoder setBuffer:query_inv_norms_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->candidateInvNormsBuffer offset:0 atIndex:3];
	[encoder setBuffer:distances_buffer offset:0 atIndex:4];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:5];

	NSUInteger width = ctx->queryBatchFixedDimCosinePipeline.threadExecutionWidth;
	if (width == 0) {
		width = 1;
	}
	if (width > ctx->candidateCount) {
		width = ctx->candidateCount;
	}
	NSUInteger max_threads = ctx->queryBatchFixedDimCosinePipeline.maxTotalThreadsPerThreadgroup;
	if (max_threads == 0) {
		max_threads = width;
	}
	NSUInteger height = max_threads / width;
	if (height == 0) {
		height = 1;
	}
	if (height > query_count) {
		height = query_count;
	}

	MTLSize grid_size = MTLSizeMake(ctx->candidateCount, query_count, 1);
	MTLSize threadgroup_size = MTLSizeMake(width, height, 1);
	[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_topk(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> query_inv_norms_buffer, id<MTLBuffer> scores_buffer, id<MTLBuffer> indices_buffer, uint32_t query_count, uint32_t top_k, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}
	if (top_k == 0 || top_k > 16) {
		metal_dot_set_error_c(err, err_len, "Metal dot top-k must be in [1,16]");
		return 0;
	}
	static const uint32_t block_candidates = 1024;
	static const NSUInteger topk_threads = 256;
	uint32_t block_count = (ctx->candidateCount + block_candidates - 1) / block_candidates;
	if (ctx->queryBatchTopKPipeline.maxTotalThreadsPerThreadgroup < topk_threads) {
		metal_dot_set_error_c(err, err_len, "Metal dot top-k pipeline does not support the required threadgroup size");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchTopKPipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:ctx->candidatesBuffer offset:0 atIndex:1];
	[encoder setBuffer:query_inv_norms_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->candidateInvNormsBuffer offset:0 atIndex:3];
	[encoder setBuffer:scores_buffer offset:0 atIndex:4];
	[encoder setBuffer:indices_buffer offset:0 atIndex:5];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:6];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:7];
	[encoder setBytes:&top_k length:sizeof(top_k) atIndex:8];
	[encoder setBytes:&block_count length:sizeof(block_count) atIndex:9];

	MTLSize threadgroups = MTLSizeMake(block_count, query_count, 1);
	MTLSize threadgroup_size = MTLSizeMake(topk_threads, 1, 1);
	[encoder dispatchThreadgroups:threadgroups threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];

	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_dispatch_query_batch_mps(GMMetalDotContext *ctx, id<MTLBuffer> queries_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, char *err, int err_len) {
	if (query_count == 0) {
		metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
		return 0;
	}

	MPSMatrixDescriptor *queries_desc = [MPSMatrixDescriptor matrixDescriptorWithRows:query_count
	                                                                          columns:ctx->dims
	                                                                         rowBytes:(NSUInteger)ctx->dims * sizeof(float)
	                                                                         dataType:MPSDataTypeFloat32];
	MPSMatrixDescriptor *candidates_desc = [MPSMatrixDescriptor matrixDescriptorWithRows:ctx->candidateCount
	                                                                             columns:ctx->dims
	                                                                            rowBytes:(NSUInteger)ctx->dims * sizeof(float)
	                                                                            dataType:MPSDataTypeFloat32];
	MPSMatrixDescriptor *dots_desc = [MPSMatrixDescriptor matrixDescriptorWithRows:query_count
	                                                                       columns:ctx->candidateCount
	                                                                      rowBytes:(NSUInteger)ctx->candidateCount * sizeof(float)
	                                                                      dataType:MPSDataTypeFloat32];
	if (queries_desc == nil || candidates_desc == nil || dots_desc == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create MPS matrix descriptors");
		return 0;
	}

	MPSMatrix *queries_matrix = [[MPSMatrix alloc] initWithBuffer:queries_buffer descriptor:queries_desc];
	MPSMatrix *candidates_matrix = [[MPSMatrix alloc] initWithBuffer:ctx->candidatesBuffer descriptor:candidates_desc];
	MPSMatrix *dots_matrix = [[MPSMatrix alloc] initWithBuffer:dots_buffer descriptor:dots_desc];
	if (queries_matrix == nil || candidates_matrix == nil || dots_matrix == nil) {
		[queries_matrix release];
		[candidates_matrix release];
		[dots_matrix release];
		metal_dot_set_error_c(err, err_len, "failed to create MPS matrices");
		return 0;
	}

	MPSMatrixMultiplication *multiply = [[MPSMatrixMultiplication alloc] initWithDevice:ctx->device
	                                                                     transposeLeft:NO
	                                                                    transposeRight:YES
	                                                                        resultRows:query_count
	                                                                     resultColumns:ctx->candidateCount
	                                                                   interiorColumns:ctx->dims
	                                                                             alpha:1.0
	                                                                              beta:0.0];
	if (multiply == nil) {
		[queries_matrix release];
		[candidates_matrix release];
		[dots_matrix release];
		metal_dot_set_error_c(err, err_len, "failed to create MPS matrix multiplication");
		return 0;
	}

	id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
	if (command_buffer == nil) {
		[multiply release];
		[queries_matrix release];
		[candidates_matrix release];
		[dots_matrix release];
		metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
		return 0;
	}

	[multiply encodeToCommandBuffer:command_buffer leftMatrix:queries_matrix rightMatrix:candidates_matrix resultMatrix:dots_matrix];
	[command_buffer commit];
	[command_buffer waitUntilCompleted];

	[multiply release];
	[queries_matrix release];
	[candidates_matrix release];
	[dots_matrix release];

	if (command_buffer.status == MTLCommandBufferStatusError) {
		NSString *message = command_buffer.error.localizedDescription;
		metal_dot_set_error(err, err_len, message == nil ? @"Metal MPS command failed" : message);
		return 0;
	}
	return 1;
}

static int metal_dot_encode_query_batch(GMMetalDotContext *ctx, id<MTLCommandBuffer> command_buffer, id<MTLBuffer> queries_buffer, id<MTLBuffer> candidates_buffer, id<MTLBuffer> dots_buffer, uint32_t query_count, char *err, int err_len) {
	id<MTLComputeCommandEncoder> encoder = [command_buffer computeCommandEncoder];
	if (encoder == nil) {
		metal_dot_set_error_c(err, err_len, "failed to create Metal compute encoder");
		return 0;
	}

	[encoder setComputePipelineState:ctx->queryBatchPipeline];
	[encoder setBuffer:queries_buffer offset:0 atIndex:0];
	[encoder setBuffer:candidates_buffer offset:0 atIndex:1];
	[encoder setBuffer:dots_buffer offset:0 atIndex:2];
	[encoder setBuffer:ctx->dimsBuffer offset:0 atIndex:3];
	[encoder setBuffer:ctx->candidateCountBuffer offset:0 atIndex:4];

	NSUInteger width = ctx->queryBatchPipeline.threadExecutionWidth;
	if (width == 0) {
		width = 1;
	}
	if (width > ctx->candidateCount) {
		width = ctx->candidateCount;
	}
	NSUInteger max_threads = ctx->queryBatchPipeline.maxTotalThreadsPerThreadgroup;
	if (max_threads == 0) {
		max_threads = width;
	}
	NSUInteger height = max_threads / width;
	if (height == 0) {
		height = 1;
	}
	if (height > query_count) {
		height = query_count;
	}

	MTLSize grid_size = MTLSizeMake(ctx->candidateCount, query_count, 1);
	MTLSize threadgroup_size = MTLSizeMake(width, height, 1);
	[encoder dispatchThreads:grid_size threadsPerThreadgroup:threadgroup_size];
	[encoder endEncoding];
	return 1;
}

int metal_dot_query_batch_loaded(MetalDotContext *opaque, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytes:queries length:queries_bytes options:buffer_options];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithLength:dots_bytes options:buffer_options];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create Metal query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch(ctx, queries_buffer, ctx->candidatesBuffer, dots_buffer, query_count, err, err_len);
		if (ok) {
			memcpy(dots, [dots_buffer contents], dots_bytes);
		}

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_loaded_no_copy(MetalDotContext *opaque, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch(ctx, queries_buffer, ctx->candidatesBuffer, dots_buffer, query_count, err, err_len);

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_tiled_loaded_no_copy(MetalDotContext *opaque, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal tiled query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_tiled(ctx, queries_buffer, ctx->candidatesBuffer, dots_buffer, query_count, err, err_len);

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_tiled_variant_loaded_no_copy(MetalDotContext *opaque, const float *queries, uint32_t query_count, uint32_t variant, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal tiled query-batch variant buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_tiled_variant(ctx, queries_buffer, ctx->candidatesBuffer, dots_buffer, query_count, variant, err, err_len);

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_simd_loaded_no_copy(MetalDotContext *opaque, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal SIMD query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_simd(ctx, queries_buffer, ctx->candidatesBuffer, dots_buffer, query_count, err, err_len);

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_fixed_dim_loaded_no_copy(MetalDotContext *opaque, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal fixed-dimension query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_fixed_dim(ctx, queries_buffer, ctx->candidatesBuffer, dots_buffer, query_count, err, err_len);

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_cosine_loaded_no_copy(MetalDotContext *opaque, const float *queries, const float *query_inv_norms, uint32_t query_count, float *distances, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (query_inv_norms == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot query inverse norms are nil");
			return 0;
		}
		if (distances == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot distances output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger query_inv_norms_bytes = (NSUInteger)query_count * sizeof(float);
		NSUInteger distances_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> query_inv_norms_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)query_inv_norms length:query_inv_norms_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> distances_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)distances length:distances_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || query_inv_norms_buffer == nil || distances_buffer == nil) {
			[queries_buffer release];
			[query_inv_norms_buffer release];
			[distances_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal cosine query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_cosine(ctx, queries_buffer, query_inv_norms_buffer, distances_buffer, query_count, err, err_len);

		[queries_buffer release];
		[query_inv_norms_buffer release];
		[distances_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_fixed_dim_cosine_loaded_no_copy(MetalDotContext *opaque, const float *queries, const float *query_inv_norms, uint32_t query_count, float *distances, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (query_inv_norms == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot query inverse norms are nil");
			return 0;
		}
		if (distances == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot distances output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger query_inv_norms_bytes = (NSUInteger)query_count * sizeof(float);
		NSUInteger distances_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> query_inv_norms_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)query_inv_norms length:query_inv_norms_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> distances_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)distances length:distances_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || query_inv_norms_buffer == nil || distances_buffer == nil) {
			[queries_buffer release];
			[query_inv_norms_buffer release];
			[distances_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal fixed-dimension cosine query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_fixed_dim_cosine(ctx, queries_buffer, query_inv_norms_buffer, distances_buffer, query_count, err, err_len);

		[queries_buffer release];
		[query_inv_norms_buffer release];
		[distances_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_mps_loaded_no_copy(MetalDotContext *opaque, const float *queries, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal MPS query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_mps(ctx, queries_buffer, dots_buffer, query_count, err, err_len);

		[queries_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_topk_loaded_no_copy(MetalDotContext *opaque, const float *queries, const float *query_inv_norms, uint32_t query_count, uint32_t top_k, float *scores, uint32_t *indices, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (query_inv_norms == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot query inverse norms are nil");
			return 0;
		}
		if (scores == NULL || indices == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot top-k output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		uint32_t block_count = (ctx->candidateCount + 1023u) / 1024u;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger query_inv_norms_bytes = (NSUInteger)query_count * sizeof(float);
		NSUInteger result_count = (NSUInteger)query_count * (NSUInteger)block_count * (NSUInteger)top_k;
		NSUInteger scores_bytes = result_count * sizeof(float);
		NSUInteger indices_bytes = result_count * sizeof(uint32_t);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> query_inv_norms_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)query_inv_norms length:query_inv_norms_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> scores_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)scores length:scores_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> indices_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)indices length:indices_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || query_inv_norms_buffer == nil || scores_buffer == nil || indices_buffer == nil) {
			[queries_buffer release];
			[query_inv_norms_buffer release];
			[scores_buffer release];
			[indices_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal top-k query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch_topk(ctx, queries_buffer, query_inv_norms_buffer, scores_buffer, indices_buffer, query_count, top_k, err, err_len);

		[queries_buffer release];
		[query_inv_norms_buffer release];
		[scores_buffer release];
		[indices_buffer release];
		return ok;
	}
}

int metal_dot_query_batch_loaded_async_double_buffered(MetalDotContext *opaque, const float *queries, uint32_t query_count, uint32_t iterations, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}
		if (query_count == 0) {
			metal_dot_set_error_c(err, err_len, "Metal dot query count must be positive");
			return 0;
		}
		if (iterations == 0) {
			metal_dot_set_error_c(err, err_len, "Metal dot async iterations must be positive");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffers[2] = { nil, nil };
		id<MTLBuffer> dots_buffers[2] = { nil, nil };
		id<MTLCommandBuffer> pending[2] = { nil, nil };
		int ok = 1;
		uint32_t last_submitted_slot = 0;

		for (int slot = 0; slot < 2; slot++) {
			queries_buffers[slot] = [ctx->device newBufferWithLength:queries_bytes options:buffer_options];
			dots_buffers[slot] = [ctx->device newBufferWithLength:dots_bytes options:buffer_options];
			if (queries_buffers[slot] == nil || dots_buffers[slot] == nil) {
				metal_dot_set_error_c(err, err_len, "failed to create async Metal query-batch buffers");
				ok = 0;
				goto cleanup;
			}
		}

		for (uint32_t i = 0; i < iterations; i++) {
			uint32_t slot = i & 1u;
			last_submitted_slot = slot;

			if (pending[slot] != nil) {
				[pending[slot] waitUntilCompleted];
				if (!metal_dot_check_completed_command(pending[slot], err, err_len)) {
					ok = 0;
					goto cleanup;
				}
				memcpy(dots, [dots_buffers[slot] contents], dots_bytes);
				[pending[slot] release];
				pending[slot] = nil;
			}

			memcpy([queries_buffers[slot] contents], queries, queries_bytes);
			[queries_buffers[slot] didModifyRange:NSMakeRange(0, queries_bytes)];

			id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
			if (command_buffer == nil) {
				metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
				ok = 0;
				goto cleanup;
			}
			[command_buffer retain];

			if (!metal_dot_encode_query_batch(ctx, command_buffer, queries_buffers[slot], ctx->candidatesBuffer, dots_buffers[slot], query_count, err, err_len)) {
				[command_buffer release];
				ok = 0;
				goto cleanup;
			}
			[command_buffer commit];
			pending[slot] = command_buffer;
		}

		for (int slot = 0; slot < 2; slot++) {
			if (pending[slot] != nil) {
				[pending[slot] waitUntilCompleted];
				if (!metal_dot_check_completed_command(pending[slot], err, err_len)) {
					ok = 0;
					goto cleanup;
				}
				memcpy(dots, [dots_buffers[slot] contents], dots_bytes);
				[pending[slot] release];
				pending[slot] = nil;
			}
		}

		memcpy(dots, [dots_buffers[last_submitted_slot] contents], dots_bytes);

cleanup:
		for (int slot = 0; slot < 2; slot++) {
			if (pending[slot] != nil) {
				[pending[slot] waitUntilCompleted];
				if (ok && !metal_dot_check_completed_command(pending[slot], err, err_len)) {
					ok = 0;
				}
				[pending[slot] release];
			}
			[queries_buffers[slot] release];
			[dots_buffers[slot] release];
		}
		return ok;
	}
}

int metal_dot_query_batch_loaded_async_buffered(MetalDotContext *opaque, const float *queries, uint32_t query_count, uint32_t query_batches, uint32_t depth, uint32_t iterations, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}
		if (query_count == 0 || query_batches == 0 || depth == 0 || iterations == 0) {
			metal_dot_set_error_c(err, err_len, "Metal dot async query count, batches, depth, and iterations must be positive");
			return 0;
		}
		if (depth > 8) {
			metal_dot_set_error_c(err, err_len, "Metal dot async depth must be <= 8");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		NSUInteger query_values_per_batch = (NSUInteger)query_count * (NSUInteger)ctx->dims;
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> *queries_buffers = calloc(depth, sizeof(id<MTLBuffer>));
		id<MTLBuffer> *dots_buffers = calloc(depth, sizeof(id<MTLBuffer>));
		id<MTLCommandBuffer> *pending = calloc(depth, sizeof(id<MTLCommandBuffer>));
		if (queries_buffers == NULL || dots_buffers == NULL || pending == NULL) {
			free(queries_buffers);
			free(dots_buffers);
			free(pending);
			metal_dot_set_error_c(err, err_len, "failed to allocate async Metal buffer arrays");
			return 0;
		}

		int ok = 1;
		uint32_t last_submitted_slot = 0;

		for (uint32_t slot = 0; slot < depth; slot++) {
			queries_buffers[slot] = [ctx->device newBufferWithLength:queries_bytes options:buffer_options];
			dots_buffers[slot] = [ctx->device newBufferWithLength:dots_bytes options:buffer_options];
			if (queries_buffers[slot] == nil || dots_buffers[slot] == nil) {
				metal_dot_set_error_c(err, err_len, "failed to create async Metal query-batch buffers");
				ok = 0;
				goto cleanup;
			}
		}

		for (uint32_t i = 0; i < iterations; i++) {
			uint32_t slot = i % depth;
			uint32_t batch = i % query_batches;
			last_submitted_slot = slot;

			if (pending[slot] != nil) {
				[pending[slot] waitUntilCompleted];
				if (!metal_dot_check_completed_command(pending[slot], err, err_len)) {
					ok = 0;
					goto cleanup;
				}
				memcpy(dots, [dots_buffers[slot] contents], dots_bytes);
				[pending[slot] release];
				pending[slot] = nil;
			}

			const float *query_batch = queries + ((NSUInteger)batch * query_values_per_batch);
			memcpy([queries_buffers[slot] contents], query_batch, queries_bytes);
			[queries_buffers[slot] didModifyRange:NSMakeRange(0, queries_bytes)];

			id<MTLCommandBuffer> command_buffer = [ctx->queue commandBuffer];
			if (command_buffer == nil) {
				metal_dot_set_error_c(err, err_len, "failed to create Metal command buffer");
				ok = 0;
				goto cleanup;
			}
			[command_buffer retain];

			if (!metal_dot_encode_query_batch(ctx, command_buffer, queries_buffers[slot], ctx->candidatesBuffer, dots_buffers[slot], query_count, err, err_len)) {
				[command_buffer release];
				ok = 0;
				goto cleanup;
			}
			[command_buffer commit];
			pending[slot] = command_buffer;
		}

		for (uint32_t slot = 0; slot < depth; slot++) {
			if (pending[slot] != nil) {
				[pending[slot] waitUntilCompleted];
				if (!metal_dot_check_completed_command(pending[slot], err, err_len)) {
					ok = 0;
					goto cleanup;
				}
				memcpy(dots, [dots_buffers[slot] contents], dots_bytes);
				[pending[slot] release];
				pending[slot] = nil;
			}
		}

		memcpy(dots, [dots_buffers[last_submitted_slot] contents], dots_bytes);

cleanup:
		for (uint32_t slot = 0; slot < depth; slot++) {
			if (pending[slot] != nil) {
				[pending[slot] waitUntilCompleted];
				if (ok && !metal_dot_check_completed_command(pending[slot], err, err_len)) {
					ok = 0;
				}
				[pending[slot] release];
			}
			[queries_buffers[slot] release];
			[dots_buffers[slot] release];
		}
		free(queries_buffers);
		free(dots_buffers);
		free(pending);
		return ok;
	}
}

int metal_dot_query_batch_no_copy(MetalDotContext *opaque, const float *queries, const float *candidates, uint32_t query_count, float *dots, char *err, int err_len) {
	@autoreleasepool {
		if (opaque == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot context is nil");
			return 0;
		}
		if (queries == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot queries are nil");
			return 0;
		}
		if (candidates == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot candidates are nil");
			return 0;
		}
		if (dots == NULL) {
			metal_dot_set_error_c(err, err_len, "Metal dot output is nil");
			return 0;
		}

		GMMetalDotContext *ctx = (GMMetalDotContext *)opaque;
		NSUInteger queries_bytes = (NSUInteger)query_count * (NSUInteger)ctx->dims * sizeof(float);
		NSUInteger candidates_bytes = (NSUInteger)ctx->dims * (NSUInteger)ctx->candidateCount * sizeof(float);
		NSUInteger dots_bytes = (NSUInteger)query_count * (NSUInteger)ctx->candidateCount * sizeof(float);
		MTLResourceOptions buffer_options = MTLResourceStorageModeShared;

		id<MTLBuffer> queries_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)queries length:queries_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> candidates_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)candidates length:candidates_bytes options:buffer_options deallocator:nil];
		id<MTLBuffer> dots_buffer = [ctx->device newBufferWithBytesNoCopy:(void *)dots length:dots_bytes options:buffer_options deallocator:nil];
		if (queries_buffer == nil || candidates_buffer == nil || dots_buffer == nil) {
			[queries_buffer release];
			[candidates_buffer release];
			[dots_buffer release];
			metal_dot_set_error_c(err, err_len, "failed to create no-copy Metal query-batch buffers");
			return 0;
		}

		int ok = metal_dot_dispatch_query_batch(ctx, queries_buffer, candidates_buffer, dots_buffer, query_count, err, err_len);

		[queries_buffer release];
		[candidates_buffer release];
		[dots_buffer release];
		return ok;
	}
}

int metal_dot_run(MetalDotContext *ctx, const float *query, const float *candidates, float *dots, char *err, int err_len) {
	if (!metal_dot_load_candidates(ctx, candidates, err, err_len)) {
		return 0;
	}
	return metal_dot_run_loaded(ctx, query, dots, err, err_len);
}
