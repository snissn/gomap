#define _POSIX_C_SOURCE 200809L

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
  PAGE_SIZE = 4096,
  NODE_HEADER_SIZE = 16,
  DIRECTORY_ENTRY_SIZE = 2,
  VALUE_PTR_SIZE = 16,
  LEAF_PREFIX_RESTART_INTERVAL = 16,
  BENCH_KEY_SIZE = 32,
  BENCH_VALUE_SIZE = 128,
  BENCH_KEY_COUNT = 1 << 12,
  SMALL_SEARCH_THRESHOLD = 16,

  FLAG_INLINE = 0x00,
  FLAG_POINTER = 0x01,
  FLAG_TOMBSTONE = 0x02,

  LEAF_COLUMNAR_V2_META_SIZE = 3,
  LEAF_COLUMNAR_PREFIX_V2_META_SIZE = 7
};

typedef struct {
  int prefix;
  int columnar;
} Options;

typedef struct {
  size_t key_off;
  size_t key_len;
  size_t value_off;
  size_t value_len;
  size_t prefix_len;
  uint8_t flags;
} ColumnarEntry;

typedef struct {
  uint8_t data[PAGE_SIZE];
  Options opts;
  int count;
  int heap_start;
  int dir_end;
  int leaf_index;
  uint8_t prev_key[BENCH_KEY_SIZE];
  size_t prev_key_len;
  uint8_t arena[PAGE_SIZE];
  uint8_t value_arena[PAGE_SIZE];
  size_t arena_len;
  size_t value_arena_len;
  ColumnarEntry entries[BENCH_KEY_COUNT];
  size_t key_bytes;
  size_t value_bytes;
} Builder;

typedef struct {
  uint8_t data[PAGE_SIZE];
  Options opts;
  int count;
} Page;

typedef struct {
  uint8_t *data;
  size_t count;
  size_t len;
} BytesTable;

typedef struct {
  size_t prefixLen;
  size_t suffixLen;
  size_t keyOff;
} prefixLayout;

typedef struct {
  uint64_t state;
} SplitMix64;

typedef struct {
  int iters;
  int prefix;
  BytesTable keys;
  BytesTable values;
} BuildCtx;

typedef struct {
  int iters;
  Page *page;
  BytesTable queries;
} SearchCtx;

typedef uint64_t (*BenchFn)(void *);

static volatile uint64_t sink;

static int env_int_any(const char **names, int n_names, int def);
static void run_case(const char *name, int iters, BenchFn fn, void *ctx);
static uint64_t splitmix_next(SplitMix64 *rng);
static void splitmix_fill(SplitMix64 *rng, uint8_t *dst, size_t len);
static BytesTable make_bench_keys(size_t count, size_t prefix_bytes);
static BytesTable make_bench_values(size_t count);
static void free_table(BytesTable *table);
static uint8_t *table_at(BytesTable table, size_t index);
static void fill_be8(uint8_t *dst, uint64_t v);
static void fill_be16(uint8_t *dst, uint64_t a, uint64_t b);
static uint64_t bench_builder_prepared(void *raw);
static void setup_search_columnar(int fixed_be8, Page *page, BytesTable *queries);
static void setup_search_prefix_variant(Options opts, Page *page, BytesTable *queries);
static uint64_t bench_search_prepared(void *raw);
static void builder_init(Builder *b, Options opts);
static int builder_add_leaf_entry(Builder *b, const uint8_t *key, size_t key_len,
                                  const uint8_t *value, size_t value_len, uint8_t flags);
static void builder_leaf_entry_size_with_prefix(const Builder *b, const uint8_t *key,
                                                size_t key_len, const uint8_t *value,
                                                size_t value_len, uint8_t flags,
                                                size_t *entry_size, size_t *prefix_len,
                                                size_t *suffix_len);
static int builder_add_leaf_entry_with_prefix(Builder *b, const uint8_t *key,
                                              size_t key_len, const uint8_t *value,
                                              size_t value_len, uint8_t flags,
                                              size_t entry_size, size_t prefix_len,
                                              size_t suffix_len);
static int builder_add_leaf_entry_columnar_v2(Builder *b, const uint8_t *key,
                                              size_t key_len, const uint8_t *value,
                                              size_t value_len, uint8_t flags);
static int builder_add_leaf_entry_columnar_prefix_v2(Builder *b, const uint8_t *key,
                                                     size_t key_len, const uint8_t *value,
                                                     size_t value_len, uint8_t flags,
                                                     size_t prefix_len, size_t suffix_len);
static void builder_finish(const Builder *b, Page *page);
static void finish_columnar_v2(const Builder *b, Page *page);
static void finish_columnar_prefix_v2(const Builder *b, Page *page);
static void page_search_leaf(Page *page, const uint8_t *key, size_t key_len, int *idx,
                             int *found);
static void search_plain(const Page *page, const uint8_t *key, size_t key_len, int *idx,
                         int *found);
static void search_columnar_v2(const Page *page, const uint8_t *key, size_t key_len,
                               int *idx, int *found);
static void search_prefix_v2(const Page *page, const uint8_t *key, size_t key_len, int *idx,
                             int *found);
static void search_prefix_block(const Page *page, int block_start, int block_end,
                                const uint8_t *target, size_t target_len, int *idx,
                                int *found);
static void search_columnar_prefix_v2(const Page *page, const uint8_t *key, size_t key_len,
                                      int *idx, int *found);
static void search_columnar_prefix_block(const Page *page, int block_start, int block_end,
                                         const uint8_t *target, size_t target_len, int *idx,
                                         int *found);

int main(void) {
  const char *build_envs[] = {"MATCHED_C_LEAF_BUILD_ITERS", "LEAF_BUILD_ITERS"};
  const char *search_envs[] = {"MATCHED_C_LEAF_SEARCH_ITERS", "LEAF_SEARCH_ITERS"};
  int build_iters = env_int_any(build_envs, 2, 500000);
  int search_iters = env_int_any(search_envs, 2, 2000000);

  BytesTable values = make_bench_values(BENCH_KEY_COUNT);
  BytesTable keys_no_prefix = make_bench_keys(BENCH_KEY_COUNT, 0);
  BuildCtx build_no_prefix = {build_iters, 0, keys_no_prefix, values};
  run_case("builder/no_prefix", build_iters, bench_builder_prepared, &build_no_prefix);

  BytesTable keys_prefix_heavy = make_bench_keys(BENCH_KEY_COUNT, 16);
  BuildCtx build_prefix_heavy = {build_iters, 1, keys_prefix_heavy, values};
  run_case("builder/prefix_heavy", build_iters, bench_builder_prepared, &build_prefix_heavy);

  BytesTable keys_prefix_light = make_bench_keys(BENCH_KEY_COUNT, 2);
  BuildCtx build_prefix_light = {build_iters, 1, keys_prefix_light, values};
  run_case("builder/prefix_light", build_iters, bench_builder_prepared, &build_prefix_light);

  Page columnar_fixed_page;
  BytesTable columnar_fixed_queries;
  setup_search_columnar(1, &columnar_fixed_page, &columnar_fixed_queries);
  SearchCtx columnar_fixed = {search_iters, &columnar_fixed_page, columnar_fixed_queries};
  run_case("search/columnar_fixed_be8", search_iters, bench_search_prepared, &columnar_fixed);

  Page columnar_variable_page;
  BytesTable columnar_variable_queries;
  setup_search_columnar(0, &columnar_variable_page, &columnar_variable_queries);
  SearchCtx columnar_variable = {search_iters, &columnar_variable_page, columnar_variable_queries};
  run_case("search/columnar_variable16", search_iters, bench_search_prepared, &columnar_variable);

  Page prefix_page;
  BytesTable prefix_queries;
  setup_search_prefix_variant((Options){1, 0}, &prefix_page, &prefix_queries);
  SearchCtx prefix = {search_iters, &prefix_page, prefix_queries};
  run_case("search/prefix_v2", search_iters, bench_search_prepared, &prefix);

  Page columnar_prefix_page;
  BytesTable columnar_prefix_queries;
  setup_search_prefix_variant((Options){1, 1}, &columnar_prefix_page, &columnar_prefix_queries);
  SearchCtx columnar_prefix = {search_iters, &columnar_prefix_page, columnar_prefix_queries};
  run_case("search/columnar_prefix_v2", search_iters, bench_search_prepared, &columnar_prefix);

  free_table(&values);
  free_table(&keys_no_prefix);
  free_table(&keys_prefix_heavy);
  free_table(&keys_prefix_light);
  free_table(&columnar_fixed_queries);
  free_table(&columnar_variable_queries);
  free_table(&prefix_queries);
  free_table(&columnar_prefix_queries);
  return (int)(sink & 0);
}

static int env_int_any(const char **names, int n_names, int def) {
  for (int i = 0; i < n_names; i++) {
    const char *raw = getenv(names[i]);
    if (raw == NULL || raw[0] == '\0') {
      continue;
    }
    char *end = NULL;
    long value = strtol(raw, &end, 10);
    if (end != raw && value > 0 && value <= 2147483647L) {
      return (int)value;
    }
  }
  return def;
}

static void run_case(const char *name, int iters, BenchFn fn, void *ctx) {
  struct timespec start;
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &start);
  uint64_t checksum = fn(ctx);
  clock_gettime(CLOCK_MONOTONIC, &end);
  uint64_t elapsed_ns = (uint64_t)(end.tv_sec - start.tv_sec) * 1000000000ULL +
                        (uint64_t)(end.tv_nsec - start.tv_nsec);
  double ns_op = (double)elapsed_ns / (double)iters;
  sink ^= checksum;
  printf("MATCHED_C\t%s\t%.2f\t%d\t%llu\n", name, ns_op, iters,
         (unsigned long long)checksum);
}

static uint64_t splitmix_next(SplitMix64 *rng) {
  rng->state += 0x9e3779b97f4a7c15ULL;
  uint64_t z = rng->state;
  z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
  z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
  return z ^ (z >> 31);
}

static void splitmix_fill(SplitMix64 *rng, uint8_t *dst, size_t len) {
  while (len > 0) {
    uint64_t v = splitmix_next(rng);
    size_t n = len < 8 ? len : 8;
    for (size_t i = 0; i < n; i++) {
      dst[i] = (uint8_t)(v >> (8 * i));
    }
    dst += n;
    len -= n;
  }
}

static int key_cmp_32(const void *a, const void *b) {
  return memcmp(a, b, BENCH_KEY_SIZE);
}

static BytesTable make_bench_keys(size_t count, size_t prefix_bytes) {
  BytesTable table = {calloc(count, BENCH_KEY_SIZE), count, BENCH_KEY_SIZE};
  if (table.data == NULL) {
    abort();
  }
  SplitMix64 rng = {1};
  for (size_t i = 0; i < count; i++) {
    uint8_t *key = table_at(table, i);
    size_t prefix = prefix_bytes < BENCH_KEY_SIZE ? prefix_bytes : BENCH_KEY_SIZE;
    memset(key, 0x42, prefix);
    splitmix_fill(&rng, key + prefix, BENCH_KEY_SIZE - prefix);
  }
  qsort(table.data, count, BENCH_KEY_SIZE, key_cmp_32);
  return table;
}

static BytesTable make_bench_values(size_t count) {
  BytesTable table = {malloc(count * BENCH_VALUE_SIZE), count, BENCH_VALUE_SIZE};
  if (table.data == NULL) {
    abort();
  }
  SplitMix64 rng = {2};
  for (size_t i = 0; i < count; i++) {
    splitmix_fill(&rng, table_at(table, i), BENCH_VALUE_SIZE);
  }
  return table;
}

static void free_table(BytesTable *table) {
  free(table->data);
  table->data = NULL;
  table->count = 0;
  table->len = 0;
}

static uint8_t *table_at(BytesTable table, size_t index) {
  return table.data + index * table.len;
}

static void fill_be8(uint8_t *dst, uint64_t v) {
  for (int i = 7; i >= 0; i--) {
    dst[7 - i] = (uint8_t)(v >> (8 * i));
  }
}

static void fill_be16(uint8_t *dst, uint64_t a, uint64_t b) {
  fill_be8(dst, a);
  fill_be8(dst + 8, b);
}

static uint64_t bench_builder_prepared(void *raw) {
  BuildCtx *ctx = (BuildCtx *)raw;
  Builder b;
  builder_init(&b, (Options){ctx->prefix, 0});
  uint64_t checksum = 0;
  for (int i = 0; i < ctx->iters;) {
    size_t k = (size_t)i & (ctx->keys.count - 1);
    size_t entry_size = 0;
    size_t prefix_len = 0;
    size_t suffix_len = 0;
    builder_leaf_entry_size_with_prefix(&b, table_at(ctx->keys, k), ctx->keys.len,
                                        table_at(ctx->values, k), ctx->values.len,
                                        FLAG_INLINE, &entry_size, &prefix_len, &suffix_len);
    if (builder_add_leaf_entry_with_prefix(&b, table_at(ctx->keys, k), ctx->keys.len,
                                           table_at(ctx->values, k), ctx->values.len,
                                           FLAG_INLINE, entry_size, prefix_len, suffix_len)) {
      checksum += (uint64_t)b.count;
      i++;
    } else {
      builder_init(&b, (Options){ctx->prefix, 0});
    }
  }
  return checksum;
}

static void setup_search_columnar(int fixed_be8, Page *page, BytesTable *queries) {
  Builder b;
  builder_init(&b, (Options){0, 1});
  int inserted = 0;
  uint8_t key[16];
  for (int i = 0; i < BENCH_KEY_COUNT; i++) {
    if (fixed_be8) {
      fill_be8(key, (uint64_t)i);
    } else {
      fill_be16(key, (uint64_t)i, (uint64_t)i * 17ULL + 3ULL);
    }
    if (!builder_add_leaf_entry(&b, key, fixed_be8 ? 8 : 16, NULL, 0, FLAG_POINTER)) {
      break;
    }
    inserted++;
  }
  builder_finish(&b, page);
  queries->count = (size_t)inserted;
  queries->len = fixed_be8 ? 8 : 16;
  queries->data = malloc(queries->count * queries->len);
  if (queries->data == NULL) {
    abort();
  }
  for (size_t i = 0; i < queries->count; i++) {
    if (fixed_be8) {
      fill_be8(table_at(*queries, i), (uint64_t)i);
    } else {
      fill_be16(table_at(*queries, i), (uint64_t)i, (uint64_t)i * 17ULL + 3ULL);
    }
  }
}

static void setup_search_prefix_variant(Options opts, Page *page, BytesTable *queries) {
  BytesTable keys = make_bench_keys(128, 24);
  Builder b;
  builder_init(&b, opts);
  for (size_t i = 0; i < keys.count; i++) {
    uint8_t flags = FLAG_INLINE;
    uint8_t value[2] = {(uint8_t)i, (uint8_t)(i + 1)};
    const uint8_t *value_ptr = value;
    size_t value_len = 2;
    if (i % 3 == 1) {
      flags = FLAG_POINTER;
      value_ptr = NULL;
      value_len = 0;
    } else if (i % 3 == 2) {
      flags = FLAG_TOMBSTONE;
      value_ptr = NULL;
      value_len = 0;
    }
    if (!builder_add_leaf_entry(&b, table_at(keys, i), keys.len, value_ptr, value_len, flags)) {
      abort();
    }
  }
  builder_finish(&b, page);
  queries->count = 4096;
  queries->len = BENCH_KEY_SIZE;
  queries->data = malloc(queries->count * queries->len);
  if (queries->data == NULL) {
    abort();
  }
  for (size_t i = 0; i < queries->count; i++) {
    uint8_t *query = table_at(*queries, i);
    memcpy(query, table_at(keys, i % keys.count), BENCH_KEY_SIZE);
    if ((i & 1U) != 0) {
      query[BENCH_KEY_SIZE - 1] ^= 0x01;
    }
  }
  free_table(&keys);
}

static uint64_t bench_search_prepared(void *raw) {
  SearchCtx *ctx = (SearchCtx *)raw;
  uint64_t checksum = 0;
  for (int i = 0; i < ctx->iters; i++) {
    int idx = 0;
    int found = 0;
    uint8_t *query = table_at(ctx->queries, (size_t)i % ctx->queries.count);
    page_search_leaf(ctx->page, query, ctx->queries.len, &idx, &found);
    checksum += (uint64_t)idx + (uint64_t)found;
  }
  return checksum;
}

static void builder_init(Builder *b, Options opts) {
  b->opts = opts;
  b->count = 0;
  b->heap_start = PAGE_SIZE;
  b->dir_end = NODE_HEADER_SIZE;
  b->leaf_index = 0;
  b->prev_key_len = 0;
  b->arena_len = 0;
  b->value_arena_len = 0;
  b->key_bytes = 0;
  b->value_bytes = 0;
}

static size_t shared_prefix_len(const uint8_t *a, size_t a_len, const uint8_t *b, size_t b_len) {
  size_t n = a_len < b_len ? a_len : b_len;
  for (size_t i = 0; i < n; i++) {
    if (a[i] != b[i]) {
      return i;
    }
  }
  return n;
}

static size_t value_size(size_t value_len, uint8_t flags) {
  if ((flags & FLAG_POINTER) != 0) {
    return VALUE_PTR_SIZE;
  }
  if ((flags & FLAG_TOMBSTONE) != 0) {
    return 0;
  }
  return value_len;
}

static size_t uvarint_len(uint64_t x) {
  size_t n = 1;
  while (x >= 0x80) {
    x >>= 7;
    n++;
  }
  return n;
}

static size_t leaf_prefix_header_size_v2(size_t prefix_len, size_t suffix_len, uint8_t flags,
                                         size_t value_len) {
  size_t header_size = 3;
  if (prefix_len > 254 || suffix_len > 254) {
    header_size += 4;
  }
  if ((flags & FLAG_POINTER) == 0 && (flags & FLAG_TOMBSTONE) == 0) {
    header_size += uvarint_len(value_len);
  }
  return header_size;
}

static void builder_leaf_entry_size_with_prefix(const Builder *b, const uint8_t *key,
                                                size_t key_len, const uint8_t *value,
                                                size_t value_len, uint8_t flags,
                                                size_t *entry_size, size_t *prefix_len,
                                                size_t *suffix_len) {
  (void)value;
  *prefix_len = 0;
  *suffix_len = key_len;
  if (b->opts.prefix && b->leaf_index % LEAF_PREFIX_RESTART_INTERVAL != 0 &&
      b->prev_key_len > 0) {
    *prefix_len = shared_prefix_len(key, key_len, b->prev_key, b->prev_key_len);
    *suffix_len = key_len - *prefix_len;
  }
  size_t val_size = value_size(value_len, flags);
  if (b->opts.columnar && b->opts.prefix) {
    *entry_size = *suffix_len + val_size + LEAF_COLUMNAR_PREFIX_V2_META_SIZE;
    return;
  }
  if (b->opts.columnar) {
    *prefix_len = 0;
    *suffix_len = key_len;
    *entry_size = *suffix_len + val_size + LEAF_COLUMNAR_V2_META_SIZE;
    return;
  }
  size_t header_size = 7;
  if (b->opts.prefix) {
    header_size = leaf_prefix_header_size_v2(*prefix_len, *suffix_len, flags, value_len);
  }
  *entry_size = header_size + *suffix_len + val_size;
}

static void put_u16(uint8_t *dst, uint16_t v) {
  dst[0] = (uint8_t)v;
  dst[1] = (uint8_t)(v >> 8);
}

static void put_u32(uint8_t *dst, uint32_t v) {
  dst[0] = (uint8_t)v;
  dst[1] = (uint8_t)(v >> 8);
  dst[2] = (uint8_t)(v >> 16);
  dst[3] = (uint8_t)(v >> 24);
}

static uint16_t get_u16(const uint8_t *src) {
  return (uint16_t)src[0] | ((uint16_t)src[1] << 8);
}

static size_t put_uvarint(uint8_t *dst, uint64_t x) {
  size_t i = 0;
  while (x >= 0x80) {
    dst[i++] = (uint8_t)x | 0x80;
    x >>= 7;
  }
  dst[i++] = (uint8_t)x;
  return i;
}

static size_t read_uvarint(const uint8_t *src, uint64_t *out) {
  uint64_t x = 0;
  unsigned shift = 0;
  for (size_t i = 0; i < 10; i++) {
    uint8_t b = src[i];
    if (b < 0x80) {
      *out = x | ((uint64_t)b << shift);
      return i + 1;
    }
    x |= (uint64_t)(b & 0x7f) << shift;
    shift += 7;
  }
  *out = 0;
  return 0;
}

static int builder_add_leaf_entry(Builder *b, const uint8_t *key, size_t key_len,
                                  const uint8_t *value, size_t value_len, uint8_t flags) {
  size_t entry_size = 0;
  size_t prefix_len = 0;
  size_t suffix_len = 0;
  builder_leaf_entry_size_with_prefix(b, key, key_len, value, value_len, flags, &entry_size,
                                      &prefix_len, &suffix_len);
  return builder_add_leaf_entry_with_prefix(b, key, key_len, value, value_len, flags, entry_size,
                                            prefix_len, suffix_len);
}

static int builder_add_leaf_entry_with_prefix(Builder *b, const uint8_t *key,
                                              size_t key_len, const uint8_t *value,
                                              size_t value_len, uint8_t flags,
                                              size_t entry_size, size_t prefix_len,
                                              size_t suffix_len) {
  if (b->opts.columnar && b->opts.prefix) {
    return builder_add_leaf_entry_columnar_prefix_v2(b, key, key_len, value, value_len, flags,
                                                     prefix_len, suffix_len);
  }
  if (b->opts.columnar) {
    return builder_add_leaf_entry_columnar_v2(b, key, key_len, value, value_len, flags);
  }
  if (b->opts.prefix && b->leaf_index % LEAF_PREFIX_RESTART_INTERVAL == 0) {
    prefix_len = 0;
    suffix_len = key_len;
  }
  size_t required = entry_size + DIRECTORY_ENTRY_SIZE;
  if (b->heap_start < b->dir_end + (int)required) {
    return 0;
  }
  int entry_start = b->heap_start - (int)entry_size;
  int off = entry_start;
  size_t header_size = 7;
  if (b->opts.prefix) {
    header_size = leaf_prefix_header_size_v2(prefix_len, suffix_len, flags, value_len);
    int extended = prefix_len > 254 || suffix_len > 254;
    if (extended) {
      b->data[off] = 0xff;
      b->data[off + 1] = 0xff;
    } else {
      b->data[off] = (uint8_t)prefix_len;
      b->data[off + 1] = (uint8_t)suffix_len;
    }
    b->data[off + 2] = flags;
    off += 3;
    if (extended) {
      put_u16(&b->data[off], (uint16_t)prefix_len);
      put_u16(&b->data[off + 2], (uint16_t)suffix_len);
      off += 4;
    }
    if ((flags & FLAG_POINTER) == 0 && (flags & FLAG_TOMBSTONE) == 0) {
      (void)put_uvarint(&b->data[off], value_len);
    }
  } else {
    put_u16(&b->data[off], (uint16_t)key_len);
    put_u32(&b->data[off + 2], (uint32_t)value_len);
    b->data[off + 6] = flags;
  }

  int key_start = entry_start + (int)header_size;
  memcpy(&b->data[key_start], key + prefix_len, suffix_len);
  int value_start = key_start + (int)suffix_len;
  if ((flags & FLAG_POINTER) != 0) {
    memset(&b->data[value_start], 0, VALUE_PTR_SIZE);
  } else if ((flags & FLAG_TOMBSTONE) == 0 && value_len > 0) {
    memcpy(&b->data[value_start], value, value_len);
  }
  put_u16(&b->data[b->dir_end], (uint16_t)entry_start);
  b->heap_start = entry_start;
  b->dir_end += DIRECTORY_ENTRY_SIZE;
  b->count++;
  b->leaf_index++;
  if (b->opts.prefix) {
    memcpy(b->prev_key, key, key_len);
    b->prev_key_len = key_len;
  }
  return 1;
}

static int builder_add_leaf_entry_columnar_v2(Builder *b, const uint8_t *key,
                                              size_t key_len, const uint8_t *value,
                                              size_t value_len, uint8_t flags) {
  size_t val_size = value_size(value_len, flags);
  size_t entry_size = LEAF_COLUMNAR_V2_META_SIZE + key_len + val_size;
  if (b->heap_start < b->dir_end + (int)entry_size + DIRECTORY_ENTRY_SIZE) {
    return 0;
  }
  size_t key_off = b->arena_len;
  memcpy(&b->arena[b->arena_len], key, key_len);
  b->arena_len += key_len;
  size_t value_off = b->arena_len;
  if ((flags & FLAG_POINTER) != 0) {
    memset(&b->arena[b->arena_len], 0, VALUE_PTR_SIZE);
    b->arena_len += VALUE_PTR_SIZE;
  } else if ((flags & FLAG_TOMBSTONE) == 0 && value_len > 0) {
    memcpy(&b->arena[b->arena_len], value, value_len);
    b->arena_len += value_len;
  }
  b->entries[b->count] = (ColumnarEntry){key_off, key_len, value_off, value_len, 0, flags};
  b->key_bytes += key_len;
  b->value_bytes += val_size;
  b->dir_end += DIRECTORY_ENTRY_SIZE + LEAF_COLUMNAR_V2_META_SIZE;
  b->heap_start -= (int)(key_len + val_size);
  b->count++;
  b->leaf_index++;
  return 1;
}

static int builder_add_leaf_entry_columnar_prefix_v2(Builder *b, const uint8_t *key,
                                                     size_t key_len, const uint8_t *value,
                                                     size_t value_len, uint8_t flags,
                                                     size_t prefix_len, size_t suffix_len) {
  if (b->leaf_index % LEAF_PREFIX_RESTART_INTERVAL == 0) {
    prefix_len = 0;
    suffix_len = key_len;
  }
  size_t val_size = value_size(value_len, flags);
  int next_count = b->count + 1;
  size_t next_key_bytes = b->key_bytes + suffix_len;
  size_t next_value_bytes = b->value_bytes + val_size;
  int dir_end = NODE_HEADER_SIZE + next_count * LEAF_COLUMNAR_PREFIX_V2_META_SIZE;
  int heap_start = PAGE_SIZE - (int)(next_key_bytes + next_value_bytes);
  if (heap_start < dir_end) {
    return 0;
  }
  size_t key_off = b->arena_len;
  memcpy(&b->arena[b->arena_len], key + prefix_len, suffix_len);
  b->arena_len += suffix_len;
  size_t value_off = b->value_arena_len;
  if ((flags & FLAG_POINTER) != 0) {
    memset(&b->value_arena[b->value_arena_len], 0, VALUE_PTR_SIZE);
    b->value_arena_len += VALUE_PTR_SIZE;
  } else if ((flags & FLAG_TOMBSTONE) == 0 && value_len > 0) {
    memcpy(&b->value_arena[b->value_arena_len], value, value_len);
    b->value_arena_len += value_len;
  }
  b->entries[b->count] =
      (ColumnarEntry){key_off, suffix_len, value_off, value_len, prefix_len, flags};
  b->key_bytes = next_key_bytes;
  b->value_bytes = next_value_bytes;
  b->count = next_count;
  b->leaf_index++;
  b->dir_end = dir_end;
  b->heap_start = heap_start;
  memcpy(b->prev_key, key, key_len);
  b->prev_key_len = key_len;
  return 1;
}

static void builder_finish(const Builder *b, Page *page) {
  memcpy(page->data, b->data, PAGE_SIZE);
  page->opts = b->opts;
  page->count = b->count;
  if (b->opts.columnar && b->opts.prefix) {
    finish_columnar_prefix_v2(b, page);
  } else if (b->opts.columnar) {
    finish_columnar_v2(b, page);
  }
}

static void finish_columnar_v2(const Builder *b, Page *page) {
  int count = b->count;
  int key_dir_start = NODE_HEADER_SIZE;
  int val_dir_start = key_dir_start + count * DIRECTORY_ENTRY_SIZE;
  int flags_start = val_dir_start + count * DIRECTORY_ENTRY_SIZE;
  int keys_start = PAGE_SIZE - (int)b->key_bytes;
  int values_start = keys_start - (int)b->value_bytes;
  int key_off = keys_start;
  int val_off = values_start;
  for (int i = 0; i < count; i++) {
    const ColumnarEntry *e = &b->entries[i];
    put_u16(&page->data[key_dir_start + i * 2], (uint16_t)key_off);
    put_u16(&page->data[val_dir_start + i * 2], (uint16_t)val_off);
    page->data[flags_start + i] = e->flags;
    size_t val_size = e->value_len;
    if ((e->flags & FLAG_POINTER) != 0) {
      val_size = VALUE_PTR_SIZE;
    } else if ((e->flags & FLAG_TOMBSTONE) != 0) {
      val_size = 0;
    }
    if (val_size > 0) {
      memcpy(&page->data[val_off], &b->arena[e->value_off], val_size);
      val_off += (int)val_size;
    }
    memcpy(&page->data[key_off], &b->arena[e->key_off], e->key_len);
    key_off += (int)e->key_len;
  }
}

static void finish_columnar_prefix_v2(const Builder *b, Page *page) {
  int count = b->count;
  int key_dir_start = NODE_HEADER_SIZE;
  int val_dir_start = key_dir_start + count * DIRECTORY_ENTRY_SIZE;
  int flags_start = val_dir_start + count * DIRECTORY_ENTRY_SIZE;
  int prefix_start = flags_start + count;
  int suffix_start = PAGE_SIZE - (int)b->key_bytes;
  int values_start = suffix_start - (int)b->value_bytes;
  memcpy(&page->data[values_start], b->value_arena, b->value_arena_len);
  memcpy(&page->data[suffix_start], b->arena, b->arena_len);
  for (int i = 0; i < count; i++) {
    const ColumnarEntry *e = &b->entries[i];
    put_u16(&page->data[key_dir_start + i * 2], (uint16_t)(suffix_start + (int)e->key_off));
    put_u16(&page->data[val_dir_start + i * 2], (uint16_t)(values_start + (int)e->value_off));
    page->data[flags_start + i] = e->flags;
    put_u16(&page->data[prefix_start + i * 2], (uint16_t)e->prefix_len);
  }
}

static int compare_leaf_key(const uint8_t *a, size_t a_len, const uint8_t *b, size_t b_len) {
  if (a_len == 8 && b_len == 8) {
    uint64_t av = 0;
    uint64_t bv = 0;
    for (int i = 0; i < 8; i++) {
      av = (av << 8) | a[i];
      bv = (bv << 8) | b[i];
    }
    return (av > bv) - (av < bv);
  }
  size_t n = a_len < b_len ? a_len : b_len;
  int cmp = memcmp(a, b, n);
  if (cmp != 0) {
    return cmp < 0 ? -1 : 1;
  }
  return (a_len > b_len) - (a_len < b_len);
}

static void page_search_leaf(Page *page, const uint8_t *key, size_t key_len, int *idx,
                             int *found) {
  if (page->opts.columnar && page->opts.prefix) {
    search_columnar_prefix_v2(page, key, key_len, idx, found);
  } else if (page->opts.columnar) {
    search_columnar_v2(page, key, key_len, idx, found);
  } else if (page->opts.prefix) {
    search_prefix_v2(page, key, key_len, idx, found);
  } else {
    search_plain(page, key, key_len, idx, found);
  }
}

static int page_offset_at(const Page *page, int index) {
  return (int)get_u16(&page->data[NODE_HEADER_SIZE + index * 2]);
}

static void plain_key_at(const Page *page, int index, const uint8_t **key, size_t *key_len) {
  int off = page_offset_at(page, index);
  *key_len = get_u16(&page->data[off]);
  *key = &page->data[off + 7];
}

static void search_plain(const Page *page, const uint8_t *key, size_t key_len, int *idx,
                         int *found) {
  int lo = 0;
  int hi = page->count;
  while (lo < hi) {
    int mid = (int)((unsigned)(lo + hi) >> 1);
    const uint8_t *k = NULL;
    size_t k_len = 0;
    plain_key_at(page, mid, &k, &k_len);
    if (compare_leaf_key(k, k_len, key, key_len) < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  *idx = lo;
  *found = 0;
  if (lo < page->count) {
    const uint8_t *k = NULL;
    size_t k_len = 0;
    plain_key_at(page, lo, &k, &k_len);
    *found = compare_leaf_key(k, k_len, key, key_len) == 0;
  }
}

static void columnar_key_at(const Page *page, int index, const uint8_t **key, size_t *key_len) {
  int key_start = page_offset_at(page, index);
  int key_end = index + 1 < page->count ? page_offset_at(page, index + 1) : PAGE_SIZE;
  *key = &page->data[key_start];
  *key_len = (size_t)(key_end - key_start);
}

static void search_columnar_v2(const Page *page, const uint8_t *key, size_t key_len,
                               int *idx, int *found) {
  if (page->count <= SMALL_SEARCH_THRESHOLD) {
    for (int i = 0; i < page->count; i++) {
      const uint8_t *k = NULL;
      size_t k_len = 0;
      columnar_key_at(page, i, &k, &k_len);
      int cmp = compare_leaf_key(k, k_len, key, key_len);
      if (cmp >= 0) {
        *idx = i;
        *found = cmp == 0;
        return;
      }
    }
    *idx = page->count;
    *found = 0;
    return;
  }
  int lo = 0;
  int hi = page->count;
  while (lo < hi) {
    int mid = (int)((unsigned)(lo + hi) >> 1);
    const uint8_t *k = NULL;
    size_t k_len = 0;
    columnar_key_at(page, mid, &k, &k_len);
    if (compare_leaf_key(k, k_len, key, key_len) < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  *idx = lo;
  *found = 0;
  if (lo < page->count) {
    const uint8_t *k = NULL;
    size_t k_len = 0;
    columnar_key_at(page, lo, &k, &k_len);
    *found = compare_leaf_key(k, k_len, key, key_len) == 0;
  }
}

static prefixLayout parse_prefix_layout(const uint8_t *data, int off) {
  uint8_t shared8 = data[off];
  uint8_t suffix8 = data[off + 1];
  uint8_t flags = data[off + 2];
  size_t header = 3;
  size_t prefix_len = shared8;
  size_t suffix_len = suffix8;
  if (shared8 == 0xff && suffix8 == 0xff) {
    header += 4;
    prefix_len = get_u16(&data[off + 3]);
    suffix_len = get_u16(&data[off + 5]);
  }
  if ((flags & FLAG_POINTER) == 0 && (flags & FLAG_TOMBSTONE) == 0) {
    uint64_t ignored = 0;
    header += read_uvarint(&data[off + header], &ignored);
  }
  return (prefixLayout){prefix_len, suffix_len, header};
}

static void prefix_restart_key(const Page *page, int index, const uint8_t **key, size_t *key_len) {
  int off = page_offset_at(page, index);
  prefixLayout layout = parse_prefix_layout(page->data, off);
  *key = &page->data[off + (int)layout.keyOff];
  *key_len = layout.suffixLen;
}

static int compare_prefix_virtual_key(const uint8_t *prev, size_t prev_len, size_t prefix_len,
                                      const uint8_t *suffix, size_t suffix_len,
                                      const uint8_t *target, size_t target_len) {
  (void)prev_len;
  size_t n = prefix_len < target_len ? prefix_len : target_len;
  if (n > 0) {
    int cmp = memcmp(prev, target, n);
    if (cmp != 0) {
      return cmp < 0 ? -1 : 1;
    }
  }
  if (target_len < prefix_len) {
    return 1;
  }
  const uint8_t *tail = target + prefix_len;
  size_t tail_len = target_len - prefix_len;
  n = suffix_len < tail_len ? suffix_len : tail_len;
  if (n > 0) {
    int cmp = memcmp(suffix, tail, n);
    if (cmp != 0) {
      return cmp < 0 ? -1 : 1;
    }
  }
  return (suffix_len > tail_len) - (suffix_len < tail_len);
}

static void search_prefix_v2(const Page *page, const uint8_t *key, size_t key_len, int *idx,
                             int *found) {
  if (page->count == 0) {
    *idx = 0;
    *found = 0;
    return;
  }
  if (page->count <= SMALL_SEARCH_THRESHOLD) {
    search_prefix_block(page, 0, page->count, key, key_len, idx, found);
    return;
  }
  int restarts = (page->count + LEAF_PREFIX_RESTART_INTERVAL - 1) / LEAF_PREFIX_RESTART_INTERVAL;
  int lo = 0;
  int hi = restarts;
  while (lo < hi) {
    int mid = (int)((unsigned)(lo + hi) >> 1);
    int pos = mid * LEAF_PREFIX_RESTART_INTERVAL;
    if (pos >= page->count) {
      hi = mid;
      continue;
    }
    const uint8_t *restart = NULL;
    size_t restart_len = 0;
    prefix_restart_key(page, pos, &restart, &restart_len);
    if (compare_leaf_key(restart, restart_len, key, key_len) <= 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  int block_start = lo > 0 ? (lo - 1) * LEAF_PREFIX_RESTART_INTERVAL : 0;
  int block_end = block_start + LEAF_PREFIX_RESTART_INTERVAL;
  if (block_end > page->count) {
    block_end = page->count;
  }
  search_prefix_block(page, block_start, block_end, key, key_len, idx, found);
}

static void search_prefix_block(const Page *page, int block_start, int block_end,
                                const uint8_t *target, size_t target_len, int *idx,
                                int *found) {
  if (block_start >= block_end) {
    *idx = block_end;
    *found = 0;
    return;
  }
  const uint8_t *restart = NULL;
  size_t restart_len = 0;
  prefix_restart_key(page, block_start, &restart, &restart_len);
  int cmp = compare_leaf_key(restart, restart_len, target, target_len);
  if (cmp >= 0) {
    *idx = block_start;
    *found = cmp == 0;
    return;
  }
  uint8_t prev[BENCH_KEY_SIZE];
  uint8_t next[BENCH_KEY_SIZE];
  memcpy(prev, restart, restart_len);
  size_t prev_len = restart_len;
  for (int i = block_start + 1; i < block_end; i++) {
    int off = page_offset_at(page, i);
    prefixLayout layout = parse_prefix_layout(page->data, off);
    const uint8_t *suffix = &page->data[off + (int)layout.keyOff];
    cmp = compare_prefix_virtual_key(prev, prev_len, layout.prefixLen, suffix, layout.suffixLen,
                                     target, target_len);
    if (cmp >= 0) {
      *idx = i;
      *found = cmp == 0;
      return;
    }
    size_t key_len = layout.prefixLen + layout.suffixLen;
    memcpy(next, prev, layout.prefixLen);
    memcpy(next + layout.prefixLen, suffix, layout.suffixLen);
    memcpy(prev, next, key_len);
    prev_len = key_len;
  }
  *idx = block_end;
  *found = 0;
}

static void columnar_prefix_suffix_at(const Page *page, int index, const uint8_t **suffix,
                                      size_t *suffix_len) {
  int key_start = page_offset_at(page, index);
  int key_end = index + 1 < page->count ? page_offset_at(page, index + 1) : PAGE_SIZE;
  *suffix = &page->data[key_start];
  *suffix_len = (size_t)(key_end - key_start);
}

static size_t columnar_prefix_len_at(const Page *page, int index) {
  int flags_start = NODE_HEADER_SIZE + page->count * 4;
  int prefix_start = flags_start + page->count;
  return get_u16(&page->data[prefix_start + index * 2]);
}

static void search_columnar_prefix_v2(const Page *page, const uint8_t *key, size_t key_len,
                                      int *idx, int *found) {
  if (page->count == 0) {
    *idx = 0;
    *found = 0;
    return;
  }
  if (page->count <= SMALL_SEARCH_THRESHOLD) {
    search_columnar_prefix_block(page, 0, page->count, key, key_len, idx, found);
    return;
  }
  int restarts = (page->count + LEAF_PREFIX_RESTART_INTERVAL - 1) / LEAF_PREFIX_RESTART_INTERVAL;
  int lo = 0;
  int hi = restarts;
  while (lo < hi) {
    int mid = (int)((unsigned)(lo + hi) >> 1);
    int pos = mid * LEAF_PREFIX_RESTART_INTERVAL;
    if (pos >= page->count) {
      hi = mid;
      continue;
    }
    const uint8_t *suffix = NULL;
    size_t suffix_len = 0;
    columnar_prefix_suffix_at(page, pos, &suffix, &suffix_len);
    if (compare_leaf_key(suffix, suffix_len, key, key_len) <= 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  int block_start = lo > 0 ? (lo - 1) * LEAF_PREFIX_RESTART_INTERVAL : 0;
  int block_end = block_start + LEAF_PREFIX_RESTART_INTERVAL;
  if (block_end > page->count) {
    block_end = page->count;
  }
  search_columnar_prefix_block(page, block_start, block_end, key, key_len, idx, found);
}

static void search_columnar_prefix_block(const Page *page, int block_start, int block_end,
                                         const uint8_t *target, size_t target_len, int *idx,
                                         int *found) {
  if (block_start >= block_end) {
    *idx = block_end;
    *found = 0;
    return;
  }
  const uint8_t *restart = NULL;
  size_t restart_len = 0;
  columnar_prefix_suffix_at(page, block_start, &restart, &restart_len);
  int cmp = compare_leaf_key(restart, restart_len, target, target_len);
  if (cmp >= 0) {
    *idx = block_start;
    *found = cmp == 0;
    return;
  }
  uint8_t prev[BENCH_KEY_SIZE];
  uint8_t next[BENCH_KEY_SIZE];
  memcpy(prev, restart, restart_len);
  size_t prev_len = restart_len;
  for (int i = block_start + 1; i < block_end; i++) {
    const uint8_t *suffix = NULL;
    size_t suffix_len = 0;
    columnar_prefix_suffix_at(page, i, &suffix, &suffix_len);
    size_t prefix_len = columnar_prefix_len_at(page, i);
    cmp = compare_prefix_virtual_key(prev, prev_len, prefix_len, suffix, suffix_len, target,
                                     target_len);
    if (cmp >= 0) {
      *idx = i;
      *found = cmp == 0;
      return;
    }
    size_t key_len = prefix_len + suffix_len;
    memcpy(next, prev, prefix_len);
    memcpy(next + prefix_len, suffix, suffix_len);
    memcpy(prev, next, key_len);
    prev_len = key_len;
  }
  *idx = block_end;
  *found = 0;
}
