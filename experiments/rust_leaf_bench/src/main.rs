use std::cmp::Ordering;
use std::env;
use std::ffi::{c_int, c_void};
use std::hint::black_box;
use std::time::Instant;

extern "C" {
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

const PAGE_SIZE: usize = 4096;
const NODE_HEADER_SIZE: usize = 16;
const DIRECTORY_ENTRY_SIZE: usize = 2;
const VALUE_PTR_SIZE: usize = 16;
const LEAF_PREFIX_RESTART_INTERVAL: usize = 16;
const BENCH_KEY_SIZE: usize = 32;
const BENCH_VALUE_SIZE: usize = 128;
const BENCH_KEY_COUNT: usize = 1 << 12;
const SMALL_SEARCH_THRESHOLD: usize = 16;

const FLAG_INLINE: u8 = 0x00;
const FLAG_POINTER: u8 = 0x01;
const FLAG_TOMBSTONE: u8 = 0x02;

const LEAF_COLUMNAR_V2_META_SIZE: usize = 3;
const LEAF_COLUMNAR_PREFIX_V2_META_SIZE: usize = 7;

#[derive(Clone, Copy)]
struct Options {
    prefix: bool,
    columnar: bool,
    key_kind: KeyKind,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Bytes,
    FixedBe8,
}

#[derive(Clone, Copy, Default)]
struct ColumnarEntry {
    key_off: usize,
    key_len: usize,
    value_off: usize,
    value_len: usize,
    flags: u8,
    prefix_len: usize,
}

struct Builder {
    data: [u8; PAGE_SIZE],
    opts: Options,
    count: usize,
    heap_start: usize,
    dir_end: usize,
    leaf_index: usize,
    prev_key: [u8; BENCH_KEY_SIZE],
    prev_key_len: usize,
    arena: [u8; PAGE_SIZE],
    value_arena: [u8; PAGE_SIZE],
    arena_len: usize,
    value_arena_len: usize,
    entries: [ColumnarEntry; BENCH_KEY_COUNT],
    entries_len: usize,
    key_bytes: usize,
    value_bytes: usize,
}

struct Page {
    data: [u8; PAGE_SIZE],
    opts: Options,
    count: usize,
}

struct BytesTable {
    data: Vec<u8>,
    count: usize,
    len: usize,
}

struct VarBytesTable {
    data: Vec<u8>,
    offsets: Vec<usize>,
}

impl VarBytesTable {
    fn with_capacity(count: usize, bytes: usize) -> Self {
        Self {
            data: Vec::with_capacity(bytes),
            offsets: Vec::with_capacity(count + 1),
        }
    }

    fn push(&mut self, value: &[u8]) {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        self.data.extend_from_slice(value);
        self.offsets.push(self.data.len());
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline(always)]
    fn at(&self, i: usize) -> &[u8] {
        &self.data[self.offsets[i]..self.offsets[i + 1]]
    }
}

impl BytesTable {
    #[inline(always)]
    fn at(&self, i: usize) -> &[u8] {
        let off = i * self.len;
        &self.data[off..off + self.len]
    }

    #[inline(always)]
    fn at_mut(&mut self, i: usize) -> &mut [u8] {
        let off = i * self.len;
        &mut self.data[off..off + self.len]
    }
}

#[derive(Clone, Copy)]
struct PrefixLayout {
    prefix_len: usize,
    suffix_len: usize,
    key_off: usize,
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn fill(&mut self, dst: &mut [u8]) {
        for chunk in dst.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

fn main() {
    let build_iters = env_usize_any(&["RUST_LEAF_BUILD_ITERS", "LEAF_BUILD_ITERS"], 500_000);
    let search_iters = env_usize_any(&["RUST_LEAF_SEARCH_ITERS", "LEAF_SEARCH_ITERS"], 2_000_000);
    let selected_case = env::var("LEAF_CASE").ok();
    let mut ran = false;

    if case_enabled(&selected_case, "builder/no_prefix") {
        let values = make_bench_values(BENCH_KEY_COUNT);
        let keys = make_bench_keys(BENCH_KEY_COUNT, 0);
        run_case("builder/no_prefix", build_iters, || {
            bench_builder_prepared(build_iters, false, &keys, &values)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "builder/prefix_heavy") {
        let values = make_bench_values(BENCH_KEY_COUNT);
        let keys = make_bench_keys(BENCH_KEY_COUNT, 16);
        run_case("builder/prefix_heavy", build_iters, || {
            bench_builder_prepared(build_iters, true, &keys, &values)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "builder/prefix_light") {
        let values = make_bench_values(BENCH_KEY_COUNT);
        let keys = make_bench_keys(BENCH_KEY_COUNT, 2);
        run_case("builder/prefix_light", build_iters, || {
            bench_builder_prepared(build_iters, true, &keys, &values)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "search/columnar_fixed_be8") {
        let (mut page, queries) = setup_search_columnar(true);
        run_case("search/columnar_fixed_be8", search_iters, || {
            bench_search_prepared(search_iters, &mut page, &queries)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "search/columnar_variable16") {
        let (mut page, queries) = setup_search_columnar(false);
        run_case("search/columnar_variable16", search_iters, || {
            bench_search_prepared(search_iters, &mut page, &queries)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "search/columnar_variable_len") {
        let (mut page, queries) = setup_search_columnar_variable_len();
        run_case("search/columnar_variable_len", search_iters, || {
            bench_search_prepared_var(search_iters, &mut page, &queries)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "search/prefix_v2") {
        let (mut page, queries) = setup_search_prefix_variant(Options {
            prefix: true,
            columnar: false,
            key_kind: KeyKind::Bytes,
        });
        run_case("search/prefix_v2", search_iters, || {
            bench_search_prepared(search_iters, &mut page, &queries)
        });
        ran = true;
    }
    if case_enabled(&selected_case, "search/columnar_prefix_v2") {
        let (mut page, queries) = setup_search_prefix_variant(Options {
            prefix: true,
            columnar: true,
            key_kind: KeyKind::Bytes,
        });
        run_case("search/columnar_prefix_v2", search_iters, || {
            bench_search_prepared(search_iters, &mut page, &queries)
        });
        ran = true;
    }

    if !ran {
        eprintln!("unknown LEAF_CASE={}", selected_case.unwrap_or_default());
        std::process::exit(2);
    }
}

fn case_enabled(selected: &Option<String>, case: &str) -> bool {
    selected.as_deref().map_or(true, |want| want == case)
}

fn env_usize_any(names: &[&str], default: usize) -> usize {
    for name in names {
        if let Some(value) = env::var(name)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
        {
            return value;
        }
    }
    default
}

fn run_case<F>(name: &str, iters: usize, mut f: F)
where
    F: FnMut() -> u64,
{
    let start = Instant::now();
    let checksum = f();
    let elapsed = start.elapsed();
    let ns = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "RESULT\t{}\t{:.2}\t{}\t{}",
        name,
        ns,
        iters,
        black_box(checksum)
    );
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn make_bench_keys(count: usize, prefix_bytes: usize) -> BytesTable {
    let mut keys = Vec::with_capacity(count);
    let mut rng = Rng::new(1);
    for _ in 0..count {
        let mut key = [0u8; BENCH_KEY_SIZE];
        let p = prefix_bytes.min(BENCH_KEY_SIZE);
        for b in &mut key[..p] {
            *b = 0x42;
        }
        rng.fill(&mut key[p..]);
        keys.push(key);
    }
    keys.sort();

    let mut data = Vec::with_capacity(count * BENCH_KEY_SIZE);
    for key in keys {
        data.extend_from_slice(&key);
    }
    BytesTable {
        data,
        count,
        len: BENCH_KEY_SIZE,
    }
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn make_bench_values(count: usize) -> BytesTable {
    let mut table = BytesTable {
        data: vec![0u8; count * BENCH_VALUE_SIZE],
        count,
        len: BENCH_VALUE_SIZE,
    };
    let mut rng = Rng::new(2);
    for i in 0..count {
        rng.fill(table.at_mut(i));
    }
    table
}

fn fill_be8(dst: &mut [u8], v: u64) {
    dst[..8].copy_from_slice(&v.to_be_bytes());
}

fn fill_be16(dst: &mut [u8], a: u64, b: u64) {
    fill_be8(&mut dst[..8], a);
    fill_be8(&mut dst[8..16], b);
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn bench_builder_prepared(
    iters: usize,
    prefix: bool,
    keys: &BytesTable,
    values: &BytesTable,
) -> u64 {
    let mut builder = Builder::new(Options {
        prefix,
        columnar: false,
        key_kind: KeyKind::Bytes,
    });
    let mut checksum = 0u64;
    let mut i = 0usize;
    while i < iters {
        let idx = i & (keys.count - 1);
        let key = keys.at(idx);
        let value = values.at(idx);
        let (entry_size, prefix_len, suffix_len) =
            builder.leaf_entry_size_with_prefix(key, value, FLAG_INLINE);
        match builder.add_leaf_entry_with_prefix(
            key,
            value,
            FLAG_INLINE,
            entry_size,
            prefix_len,
            suffix_len,
        ) {
            Ok(()) => {
                checksum = checksum.wrapping_add(builder.count as u64);
                i += 1;
            }
            Err(ErrFull) => {
                builder.reset(Options {
                    prefix,
                    columnar: false,
                    key_kind: KeyKind::Bytes,
                });
            }
        }
    }
    checksum
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn setup_search_columnar(fixed_be8: bool) -> (Page, BytesTable) {
    let mut builder = Builder::new(Options {
        prefix: false,
        columnar: true,
        key_kind: if fixed_be8 {
            KeyKind::FixedBe8
        } else {
            KeyKind::Bytes
        },
    });
    let mut inserted = 0usize;
    let mut key = [0u8; 16];
    for i in 0..BENCH_KEY_COUNT {
        let key_len = if fixed_be8 {
            fill_be8(&mut key[..8], i as u64);
            8
        } else {
            fill_be16(
                &mut key,
                i as u64,
                (i as u64).wrapping_mul(17).wrapping_add(3),
            );
            16
        };
        if builder
            .add_leaf_entry(&key[..key_len], &[], FLAG_POINTER)
            .is_err()
        {
            break;
        }
        inserted += 1;
    }
    let page = builder.finish();
    let query_len = if fixed_be8 { 8 } else { 16 };
    let mut queries = BytesTable {
        data: vec![0u8; inserted * query_len],
        count: inserted,
        len: query_len,
    };
    for i in 0..inserted {
        if fixed_be8 {
            fill_be8(queries.at_mut(i), i as u64);
        } else {
            fill_be16(
                queries.at_mut(i),
                i as u64,
                (i as u64).wrapping_mul(17).wrapping_add(3),
            );
        }
    }
    (page, queries)
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn setup_search_columnar_variable_len() -> (Page, VarBytesTable) {
    let mut builder = Builder::new(Options {
        prefix: false,
        columnar: true,
        key_kind: KeyKind::Bytes,
    });
    let mut inserted = 0usize;
    let mut key = [0u8; BENCH_KEY_SIZE];
    for i in 0..BENCH_KEY_COUNT {
        let key_len = fill_variable_len_key(&mut key, i);
        if builder
            .add_leaf_entry(&key[..key_len], &[], FLAG_POINTER)
            .is_err()
        {
            break;
        }
        inserted += 1;
    }

    let page = builder.finish();
    let mut queries = VarBytesTable::with_capacity(inserted, inserted * BENCH_KEY_SIZE);
    for i in 0..inserted {
        let key_len = fill_variable_len_key(&mut key, i);
        queries.push(&key[..key_len]);
    }
    (page, queries)
}

fn fill_variable_len_key(dst: &mut [u8; BENCH_KEY_SIZE], i: usize) -> usize {
    let key_len = 9 + (i % (BENCH_KEY_SIZE - 8));
    fill_be8(&mut dst[..8], i as u64);
    for (j, b) in dst[8..key_len].iter_mut().enumerate() {
        *b = i.wrapping_mul(31).wrapping_add(j) as u8;
    }
    key_len
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn bench_search_prepared(iters: usize, page: &mut Page, queries: &BytesTable) -> u64 {
    let mut checksum = 0u64;
    for i in 0..iters {
        let (idx, found) = page.search_leaf(black_box(queries.at(i % queries.count)));
        checksum = checksum.wrapping_add(idx as u64 + found as u64);
    }
    checksum
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn bench_search_prepared_var(iters: usize, page: &mut Page, queries: &VarBytesTable) -> u64 {
    let mut checksum = 0u64;
    for i in 0..iters {
        let (idx, found) = page.search_leaf(black_box(queries.at(i % queries.len())));
        checksum = checksum.wrapping_add(idx as u64 + found as u64);
    }
    checksum
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn setup_search_prefix_variant(opts: Options) -> (Page, BytesTable) {
    const KEY_COUNT: usize = 128;
    let keys = make_bench_keys(KEY_COUNT, 24);
    let mut builder = Builder::new(opts);
    for i in 0..keys.count {
        let key = keys.at(i);
        let flags = match i % 3 {
            0 => FLAG_INLINE,
            1 => FLAG_POINTER,
            _ => FLAG_TOMBSTONE,
        };
        let value = [i as u8, i.wrapping_add(1) as u8];
        let value = if flags == FLAG_INLINE {
            &value[..]
        } else {
            &[]
        };
        builder
            .add_leaf_entry(key, value, flags)
            .expect("prefix setup should fit");
    }
    let page = builder.finish();
    let mut queries = BytesTable {
        data: vec![0u8; 4096 * BENCH_KEY_SIZE],
        count: 4096,
        len: BENCH_KEY_SIZE,
    };
    for i in 0..4096 {
        let q = queries.at_mut(i);
        q.copy_from_slice(keys.at(i % keys.count));
        if i & 1 == 1 {
            q[BENCH_KEY_SIZE - 1] ^= 0x01;
        }
    }
    (page, queries)
}

#[derive(Debug)]
struct ErrFull;

impl Builder {
    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn new(opts: Options) -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
            opts,
            count: 0,
            heap_start: PAGE_SIZE,
            dir_end: NODE_HEADER_SIZE,
            leaf_index: 0,
            prev_key: [0u8; BENCH_KEY_SIZE],
            prev_key_len: 0,
            arena: [0u8; PAGE_SIZE],
            value_arena: [0u8; PAGE_SIZE],
            arena_len: 0,
            value_arena_len: 0,
            entries: [ColumnarEntry::default(); BENCH_KEY_COUNT],
            entries_len: 0,
            key_bytes: 0,
            value_bytes: 0,
        }
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn reset(&mut self, opts: Options) {
        self.opts = opts;
        self.count = 0;
        self.heap_start = PAGE_SIZE;
        self.dir_end = NODE_HEADER_SIZE;
        self.leaf_index = 0;
        self.prev_key_len = 0;
        self.arena_len = 0;
        self.value_arena_len = 0;
        self.entries_len = 0;
        self.key_bytes = 0;
        self.value_bytes = 0;
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn add_leaf_entry(&mut self, key: &[u8], value: &[u8], flags: u8) -> Result<(), ErrFull> {
        let (entry_size, prefix_len, suffix_len) =
            self.leaf_entry_size_with_prefix(key, value, flags);
        self.add_leaf_entry_with_prefix(key, value, flags, entry_size, prefix_len, suffix_len)
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn leaf_entry_size_with_prefix(
        &self,
        key: &[u8],
        value: &[u8],
        flags: u8,
    ) -> (usize, usize, usize) {
        let mut prefix_len = 0usize;
        let mut suffix_len = key.len();
        if self.opts.prefix
            && self.leaf_index % LEAF_PREFIX_RESTART_INTERVAL != 0
            && self.prev_key_len > 0
        {
            prefix_len = shared_prefix_len(key, &self.prev_key[..self.prev_key_len]).min(key.len());
            suffix_len = key.len() - prefix_len;
        }

        let val_size = value_size(value, flags);
        if self.opts.columnar && self.opts.prefix {
            return (
                suffix_len + val_size + LEAF_COLUMNAR_PREFIX_V2_META_SIZE,
                prefix_len,
                suffix_len,
            );
        }
        if self.opts.columnar {
            return (
                suffix_len + val_size + LEAF_COLUMNAR_V2_META_SIZE,
                0,
                suffix_len,
            );
        }

        let mut header_size = 7usize;
        if self.opts.prefix {
            header_size = leaf_prefix_header_size_v2(prefix_len, suffix_len, flags, value.len());
        }
        (header_size + suffix_len + val_size, prefix_len, suffix_len)
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn add_leaf_entry_with_prefix(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: u8,
        entry_size: usize,
        mut prefix_len: usize,
        mut suffix_len: usize,
    ) -> Result<(), ErrFull> {
        if self.opts.columnar && self.opts.prefix {
            return self
                .add_leaf_entry_columnar_prefix_v2(key, value, flags, prefix_len, suffix_len);
        }
        if self.opts.columnar {
            return self.add_leaf_entry_columnar_v2(key, value, flags);
        }

        if self.opts.prefix && self.leaf_index % LEAF_PREFIX_RESTART_INTERVAL == 0 {
            prefix_len = 0;
            suffix_len = key.len();
        }
        let required = entry_size + DIRECTORY_ENTRY_SIZE;
        if self.heap_start < self.dir_end + required {
            return Err(ErrFull);
        }

        let entry_start = self.heap_start - entry_size;
        let mut off = entry_start;
        let header_size;
        if self.opts.prefix {
            header_size = leaf_prefix_header_size_v2(prefix_len, suffix_len, flags, value.len());
            let extended = prefix_len > 254 || suffix_len > 254;
            if extended {
                self.data[off] = 0xff;
                self.data[off + 1] = 0xff;
            } else {
                self.data[off] = prefix_len as u8;
                self.data[off + 1] = suffix_len as u8;
            }
            self.data[off + 2] = flags;
            off += 3;
            if extended {
                put_u16(&mut self.data[off..off + 2], prefix_len as u16);
                put_u16(&mut self.data[off + 2..off + 4], suffix_len as u16);
                off += 4;
            }
            if flags & FLAG_POINTER == 0 && flags & FLAG_TOMBSTONE == 0 {
                let _ = put_uvarint(&mut self.data[off..], value.len() as u64);
            }
        } else {
            header_size = 7;
            put_u16(&mut self.data[off..off + 2], key.len() as u16);
            put_u32(&mut self.data[off + 2..off + 6], value.len() as u32);
            self.data[off + 6] = flags;
        }

        let key_start = entry_start + header_size;
        self.data[key_start..key_start + suffix_len].copy_from_slice(&key[prefix_len..]);
        let value_start = key_start + suffix_len;
        if flags & FLAG_POINTER != 0 {
            self.data[value_start..value_start + VALUE_PTR_SIZE].fill(0);
        } else if flags & FLAG_TOMBSTONE == 0 {
            self.data[value_start..value_start + value.len()].copy_from_slice(value);
        }

        put_u16(
            &mut self.data[self.dir_end..self.dir_end + DIRECTORY_ENTRY_SIZE],
            entry_start as u16,
        );
        self.heap_start = entry_start;
        self.dir_end += DIRECTORY_ENTRY_SIZE;
        self.count += 1;
        self.leaf_index += 1;
        if self.opts.prefix {
            self.prev_key[..key.len()].copy_from_slice(key);
            self.prev_key_len = key.len();
        }
        Ok(())
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn add_leaf_entry_columnar_v2(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: u8,
    ) -> Result<(), ErrFull> {
        let val_size = value_size(value, flags);
        let entry_size = LEAF_COLUMNAR_V2_META_SIZE + key.len() + val_size;
        if self.heap_start < self.dir_end + entry_size + DIRECTORY_ENTRY_SIZE {
            return Err(ErrFull);
        }

        let key_off = self.arena_len;
        self.arena[self.arena_len..self.arena_len + key.len()].copy_from_slice(key);
        self.arena_len += key.len();
        let value_off = self.arena_len;
        if flags & FLAG_POINTER != 0 {
            self.arena[self.arena_len..self.arena_len + VALUE_PTR_SIZE].fill(0);
            self.arena_len += VALUE_PTR_SIZE;
        } else if flags & FLAG_TOMBSTONE == 0 {
            self.arena[self.arena_len..self.arena_len + value.len()].copy_from_slice(value);
            self.arena_len += value.len();
        }
        self.entries[self.entries_len] = ColumnarEntry {
            key_off,
            key_len: key.len(),
            value_off,
            value_len: value.len(),
            flags,
            prefix_len: 0,
        };
        self.entries_len += 1;
        self.key_bytes += key.len();
        self.value_bytes += val_size;
        self.dir_end += DIRECTORY_ENTRY_SIZE + LEAF_COLUMNAR_V2_META_SIZE;
        self.heap_start -= key.len() + val_size;
        self.count += 1;
        self.leaf_index += 1;
        Ok(())
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn add_leaf_entry_columnar_prefix_v2(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: u8,
        mut prefix_len: usize,
        mut suffix_len: usize,
    ) -> Result<(), ErrFull> {
        if self.leaf_index % LEAF_PREFIX_RESTART_INTERVAL == 0 {
            prefix_len = 0;
            suffix_len = key.len();
        }
        let val_size = value_size(value, flags);
        let next_count = self.count + 1;
        let next_key_bytes = self.key_bytes + suffix_len;
        let next_value_bytes = self.value_bytes + val_size;
        let dir_end = NODE_HEADER_SIZE + next_count * LEAF_COLUMNAR_PREFIX_V2_META_SIZE;
        let heap_start = PAGE_SIZE - (next_key_bytes + next_value_bytes);
        if heap_start < dir_end {
            return Err(ErrFull);
        }

        let key_off = self.arena_len;
        self.arena[self.arena_len..self.arena_len + suffix_len].copy_from_slice(&key[prefix_len..]);
        self.arena_len += suffix_len;
        let value_off = self.value_arena_len;
        if flags & FLAG_POINTER != 0 {
            self.value_arena[self.value_arena_len..self.value_arena_len + VALUE_PTR_SIZE].fill(0);
            self.value_arena_len += VALUE_PTR_SIZE;
        } else if flags & FLAG_TOMBSTONE == 0 {
            self.value_arena[self.value_arena_len..self.value_arena_len + value.len()]
                .copy_from_slice(value);
            self.value_arena_len += value.len();
        }
        self.entries[self.entries_len] = ColumnarEntry {
            key_off,
            key_len: suffix_len,
            value_off,
            value_len: value.len(),
            flags,
            prefix_len,
        };
        self.entries_len += 1;
        self.key_bytes = next_key_bytes;
        self.value_bytes = next_value_bytes;
        self.count = next_count;
        self.leaf_index += 1;
        self.dir_end = dir_end;
        self.heap_start = heap_start;
        self.prev_key[..key.len()].copy_from_slice(key);
        self.prev_key_len = key.len();
        Ok(())
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn finish(mut self) -> Page {
        if self.opts.columnar && self.opts.prefix {
            self.finish_columnar_prefix_v2();
        } else if self.opts.columnar {
            self.finish_columnar_v2();
        }
        Page {
            data: self.data,
            opts: self.opts,
            count: self.count,
        }
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn finish_columnar_v2(&mut self) {
        let count = self.count;
        let key_dir_start = NODE_HEADER_SIZE;
        let val_dir_start = key_dir_start + count * DIRECTORY_ENTRY_SIZE;
        let flags_start = val_dir_start + count * DIRECTORY_ENTRY_SIZE;
        let keys_start = PAGE_SIZE - self.key_bytes;
        let values_start = keys_start - self.value_bytes;

        let mut key_off = keys_start;
        let mut val_off = values_start;
        for (i, e) in self.entries[..self.entries_len].iter().enumerate() {
            put_u16(
                &mut self.data[key_dir_start + i * 2..key_dir_start + i * 2 + 2],
                key_off as u16,
            );
            put_u16(
                &mut self.data[val_dir_start + i * 2..val_dir_start + i * 2 + 2],
                val_off as u16,
            );
            self.data[flags_start + i] = e.flags;

            let val_size = if e.flags & FLAG_POINTER != 0 {
                VALUE_PTR_SIZE
            } else if e.flags & FLAG_TOMBSTONE != 0 {
                0
            } else {
                e.value_len
            };
            if val_size > 0 {
                let src = e.value_off;
                self.data[val_off..val_off + val_size]
                    .copy_from_slice(&self.arena[src..src + val_size]);
                val_off += val_size;
            }

            let key_src = e.key_off;
            self.data[key_off..key_off + e.key_len]
                .copy_from_slice(&self.arena[key_src..key_src + e.key_len]);
            key_off += e.key_len;
        }
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn finish_columnar_prefix_v2(&mut self) {
        let count = self.count;
        let key_dir_start = NODE_HEADER_SIZE;
        let val_dir_start = key_dir_start + count * DIRECTORY_ENTRY_SIZE;
        let flags_start = val_dir_start + count * DIRECTORY_ENTRY_SIZE;
        let prefix_start = flags_start + count;
        let suffix_start = PAGE_SIZE - self.key_bytes;
        let values_start = suffix_start - self.value_bytes;

        self.data[values_start..suffix_start]
            .copy_from_slice(&self.value_arena[..self.value_arena_len]);
        self.data[suffix_start..suffix_start + self.arena_len]
            .copy_from_slice(&self.arena[..self.arena_len]);

        for (i, e) in self.entries[..self.entries_len].iter().enumerate() {
            put_u16(
                &mut self.data[key_dir_start + i * 2..key_dir_start + i * 2 + 2],
                (suffix_start + e.key_off) as u16,
            );
            put_u16(
                &mut self.data[val_dir_start + i * 2..val_dir_start + i * 2 + 2],
                (values_start + e.value_off) as u16,
            );
            self.data[flags_start + i] = e.flags;
            put_u16(
                &mut self.data[prefix_start + i * 2..prefix_start + i * 2 + 2],
                e.prefix_len as u16,
            );
        }
    }
}

impl Page {
    #[inline(always)]
    fn search_leaf(&mut self, key: &[u8]) -> (usize, bool) {
        if self.opts.columnar && self.opts.prefix {
            self.search_columnar_prefix_v2(key)
        } else if self.opts.columnar {
            match self.opts.key_kind {
                KeyKind::FixedBe8 => self.search_columnar_v2_fixed_be8(key),
                KeyKind::Bytes => self.search_columnar_v2(key),
            }
        } else if self.opts.prefix {
            self.search_prefix_v2(key)
        } else {
            self.search_plain(key)
        }
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn search_plain(&self, key: &[u8]) -> (usize, bool) {
        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            let k = self.plain_key_at(mid);
            if compare_leaf_key(k, key) == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < self.count {
            let k = self.plain_key_at(lo);
            (lo, compare_leaf_key(k, key) == Ordering::Equal)
        } else {
            (lo, false)
        }
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn plain_key_at(&self, index: usize) -> &[u8] {
        let off = self.offset_at(index);
        let key_len = get_u16(&self.data[off..off + 2]) as usize;
        &self.data[off + 7..off + 7 + key_len]
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn search_columnar_v2(&self, key: &[u8]) -> (usize, bool) {
        if self.count <= SMALL_SEARCH_THRESHOLD {
            for idx in 0..self.count {
                let cmp = compare_leaf_key(self.columnar_key_at(idx), key);
                if cmp != Ordering::Less {
                    return (idx, cmp == Ordering::Equal);
                }
            }
            return (self.count, false);
        }

        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if compare_leaf_key(self.columnar_key_at(mid), key) == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < self.count {
            (
                lo,
                compare_leaf_key(self.columnar_key_at(lo), key) == Ordering::Equal,
            )
        } else {
            (lo, false)
        }
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn columnar_key_at(&self, index: usize) -> &[u8] {
        let key_start = self.offset_at(index);
        let key_end = if index + 1 < self.count {
            self.offset_at(index + 1)
        } else {
            PAGE_SIZE
        };
        &self.data[key_start..key_end]
    }

    #[inline(always)]
    fn search_columnar_v2_fixed_be8(&self, key: &[u8]) -> (usize, bool) {
        debug_assert_eq!(key.len(), 8);
        let target = u64::from_be_bytes(key.try_into().unwrap());
        if self.count <= SMALL_SEARCH_THRESHOLD {
            for idx in 0..self.count {
                let entry = self.columnar_fixed_be8_at(idx);
                if entry >= target {
                    return (idx, entry == target);
                }
            }
            return (self.count, false);
        }

        let mut lo = 0usize;
        let mut hi = self.count;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.columnar_fixed_be8_at(mid) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < self.count {
            (lo, self.columnar_fixed_be8_at(lo) == target)
        } else {
            (lo, false)
        }
    }

    #[inline(always)]
    fn columnar_fixed_be8_at(&self, index: usize) -> u64 {
        let key_start = self.offset_at(index);
        u64::from_be_bytes(self.data[key_start..key_start + 8].try_into().unwrap())
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn search_prefix_v2(&mut self, key: &[u8]) -> (usize, bool) {
        if self.count == 0 {
            return (0, false);
        }
        if self.count <= SMALL_SEARCH_THRESHOLD {
            return self.search_prefix_block(0, self.count, key);
        }

        let restarts =
            (self.count + LEAF_PREFIX_RESTART_INTERVAL - 1) / LEAF_PREFIX_RESTART_INTERVAL;
        let mut lo = 0usize;
        let mut hi = restarts;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            let idx = mid * LEAF_PREFIX_RESTART_INTERVAL;
            if idx >= self.count {
                hi = mid;
                continue;
            }
            let restart = self.prefix_restart_key(idx);
            if compare_leaf_key(restart, key) != Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        let block_start = if lo > 0 {
            (lo - 1) * LEAF_PREFIX_RESTART_INTERVAL
        } else {
            0
        };
        let block_end = (block_start + LEAF_PREFIX_RESTART_INTERVAL).min(self.count);
        self.search_prefix_block(block_start, block_end, key)
    }

    #[inline(always)]
    fn search_prefix_block(
        &mut self,
        block_start: usize,
        block_end: usize,
        target: &[u8],
    ) -> (usize, bool) {
        if block_start >= block_end {
            return (block_end, false);
        }

        let restart = self.prefix_restart_key(block_start);
        let cmp = compare_leaf_key(restart, target);
        if cmp != Ordering::Less {
            return (block_start, cmp == Ordering::Equal);
        }

        let mut prev = [0u8; BENCH_KEY_SIZE];
        let mut prev_len = restart.len();
        prev[..prev_len].copy_from_slice(restart);
        for idx in block_start + 1..block_end {
            let off = self.offset_at(idx);
            let layout = parse_prefix_layout(&self.data, off);
            let suffix = &self.data[off + layout.key_off..off + layout.key_off + layout.suffix_len];
            let key_len = layout.prefix_len + layout.suffix_len;
            prev[layout.prefix_len..key_len].copy_from_slice(suffix);
            prev_len = key_len;
            let cmp = compare_leaf_key(&prev[..prev_len], target);
            if cmp != Ordering::Less {
                return (idx, cmp == Ordering::Equal);
            }
        }
        (block_end, false)
    }

    #[inline(always)]
    fn prefix_restart_key(&self, index: usize) -> &[u8] {
        let off = self.offset_at(index);
        let layout = parse_prefix_layout(&self.data, off);
        &self.data[off + layout.key_off..off + layout.key_off + layout.suffix_len]
    }

    #[inline(always)]
    fn search_columnar_prefix_v2(&mut self, key: &[u8]) -> (usize, bool) {
        if self.count == 0 {
            return (0, false);
        }
        if self.count <= SMALL_SEARCH_THRESHOLD {
            return self.search_columnar_prefix_block(0, self.count, key);
        }

        let restarts =
            (self.count + LEAF_PREFIX_RESTART_INTERVAL - 1) / LEAF_PREFIX_RESTART_INTERVAL;
        let mut lo = 0usize;
        let mut hi = restarts;
        while lo < hi {
            let mid = (lo + hi) >> 1;
            let idx = mid * LEAF_PREFIX_RESTART_INTERVAL;
            if idx >= self.count {
                hi = mid;
                continue;
            }
            let restart = self.columnar_prefix_suffix_at(idx);
            if compare_leaf_key(restart, key) != Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let block_start = if lo > 0 {
            (lo - 1) * LEAF_PREFIX_RESTART_INTERVAL
        } else {
            0
        };
        let block_end = (block_start + LEAF_PREFIX_RESTART_INTERVAL).min(self.count);
        self.search_columnar_prefix_block(block_start, block_end, key)
    }

    #[inline(always)]
    fn search_columnar_prefix_block(
        &mut self,
        block_start: usize,
        block_end: usize,
        target: &[u8],
    ) -> (usize, bool) {
        if block_start >= block_end {
            return (block_end, false);
        }
        let restart = self.columnar_prefix_suffix_at(block_start);
        let cmp = compare_leaf_key(restart, target);
        if cmp != Ordering::Less {
            return (block_start, cmp == Ordering::Equal);
        }

        let mut prev = [0u8; BENCH_KEY_SIZE];
        let mut prev_len = restart.len();
        prev[..prev_len].copy_from_slice(restart);
        for idx in block_start + 1..block_end {
            let prefix_len = self.columnar_prefix_len_at(idx);
            let suffix = self.columnar_prefix_suffix_at(idx);
            let key_len = prefix_len + suffix.len();
            prev[prefix_len..key_len].copy_from_slice(suffix);
            prev_len = key_len;
            let cmp = compare_leaf_key(&prev[..prev_len], target);
            if cmp != Ordering::Less {
                return (idx, cmp == Ordering::Equal);
            }
        }
        (block_end, false)
    }

    #[inline(always)]
    fn columnar_prefix_suffix_at(&self, index: usize) -> &[u8] {
        let key_start = self.offset_at(index);
        let key_end = if index + 1 < self.count {
            self.offset_at(index + 1)
        } else {
            PAGE_SIZE
        };
        &self.data[key_start..key_end]
    }

    #[inline(always)]
    fn columnar_prefix_len_at(&self, index: usize) -> usize {
        let flags_start = NODE_HEADER_SIZE + self.count * 4;
        let prefix_start = flags_start + self.count;
        get_u16(&self.data[prefix_start + index * 2..prefix_start + index * 2 + 2]) as usize
    }

    #[inline(always)]
    fn offset_at(&self, index: usize) -> usize {
        get_u16(&self.data[NODE_HEADER_SIZE + index * 2..NODE_HEADER_SIZE + index * 2 + 2]) as usize
    }
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn value_size(value: &[u8], flags: u8) -> usize {
    if flags & FLAG_POINTER != 0 {
        VALUE_PTR_SIZE
    } else if flags & FLAG_TOMBSTONE != 0 {
        0
    } else {
        value.len()
    }
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn leaf_prefix_header_size_v2(
    prefix_len: usize,
    suffix_len: usize,
    flags: u8,
    val_len: usize,
) -> usize {
    let mut size = 3;
    if prefix_len > 254 || suffix_len > 254 {
        size += 4;
    }
    if flags & FLAG_POINTER == 0 && flags & FLAG_TOMBSTONE == 0 {
        size += uvarint_len(val_len as u64);
    }
    size
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn uvarint_len(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn put_uvarint(dst: &mut [u8], mut x: u64) -> usize {
    let mut i = 0;
    while x >= 0x80 {
        dst[i] = x as u8 | 0x80;
        x >>= 7;
        i += 1;
    }
    dst[i] = x as u8;
    i + 1
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn read_uvarint(src: &[u8]) -> (u64, usize) {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, b) in src.iter().copied().enumerate() {
        if b < 0x80 {
            return (x | ((b as u64) << s), i + 1);
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    (0, 0)
}

#[inline(always)]
fn parse_prefix_layout(data: &[u8], off: usize) -> PrefixLayout {
    let shared8 = data[off];
    let suffix8 = data[off + 1];
    let flags = data[off + 2];
    let mut header = 3usize;
    let (prefix_len, suffix_len) = if shared8 == 0xff && suffix8 == 0xff {
        header += 4;
        (
            get_u16(&data[off + 3..off + 5]) as usize,
            get_u16(&data[off + 5..off + 7]) as usize,
        )
    } else {
        (shared8 as usize, suffix8 as usize)
    };
    if flags & FLAG_POINTER == 0 && flags & FLAG_TOMBSTONE == 0 {
        let (_, n) = read_uvarint(&data[off + header..]);
        header += n;
    }
    PrefixLayout {
        prefix_len,
        suffix_len,
        key_off: header,
    }
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn compare_leaf_key(a: &[u8], b: &[u8]) -> Ordering {
    compare_bytes(a, b)
}

#[inline(always)]
fn compare_bytes(a: &[u8], b: &[u8]) -> Ordering {
    let n = a.len().min(b.len());
    if n > 0 {
        let cmp = unsafe { memcmp(a.as_ptr().cast::<c_void>(), b.as_ptr().cast::<c_void>(), n) };
        if cmp < 0 {
            return Ordering::Less;
        }
        if cmp > 0 {
            return Ordering::Greater;
        }
    }
    a.len().cmp(&b.len())
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn put_u16(dst: &mut [u8], v: u16) {
    dst[0] = v as u8;
    dst[1] = (v >> 8) as u8;
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn put_u32(dst: &mut [u8], v: u32) {
    dst[0] = v as u8;
    dst[1] = (v >> 8) as u8;
    dst[2] = (v >> 16) as u8;
    dst[3] = (v >> 24) as u8;
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn get_u16(src: &[u8]) -> u16 {
    src[0] as u16 | ((src[1] as u16) << 8)
}
