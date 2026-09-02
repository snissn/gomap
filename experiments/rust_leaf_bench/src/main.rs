use std::cmp::Ordering;
use std::env;
use std::hint::black_box;
use std::time::Instant;

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
}

#[derive(Clone)]
struct ColumnarEntry {
    key_off: usize,
    key_len: usize,
    value_off: usize,
    value_len: usize,
    flags: u8,
    prefix_len: usize,
}

struct Builder {
    data: Vec<u8>,
    opts: Options,
    count: usize,
    heap_start: usize,
    dir_end: usize,
    leaf_index: usize,
    prev_key: Vec<u8>,
    arena: Vec<u8>,
    value_arena: Vec<u8>,
    entries: Vec<ColumnarEntry>,
    key_bytes: usize,
    value_bytes: usize,
}

struct Page {
    data: Vec<u8>,
    opts: Options,
    count: usize,
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
    if case_enabled(&selected_case, "search/prefix_v2") {
        let (mut page, queries) = setup_search_prefix_variant(Options {
            prefix: true,
            columnar: false,
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
fn make_bench_keys(count: usize, prefix_bytes: usize) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(count);
    let mut rng = Rng::new(1);
    for _ in 0..count {
        let mut key = vec![0u8; BENCH_KEY_SIZE];
        let p = prefix_bytes.min(BENCH_KEY_SIZE);
        for b in &mut key[..p] {
            *b = 0x42;
        }
        rng.fill(&mut key[p..]);
        keys.push(key);
    }
    keys.sort();
    keys
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn make_bench_values(count: usize) -> Vec<Vec<u8>> {
    let mut values = Vec::with_capacity(count);
    let mut rng = Rng::new(2);
    for _ in 0..count {
        let mut value = vec![0u8; BENCH_VALUE_SIZE];
        rng.fill(&mut value);
        values.push(value);
    }
    values
}

fn be8(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn be16(a: u64, b: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&a.to_be_bytes());
    out.extend_from_slice(&b.to_be_bytes());
    out
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn bench_builder_prepared(iters: usize, prefix: bool, keys: &[Vec<u8>], values: &[Vec<u8>]) -> u64 {
    let mut builder = Builder::new(Options {
        prefix,
        columnar: false,
    });
    let mut checksum = 0u64;
    let mut i = 0usize;
    while i < iters {
        let idx = i & (keys.len() - 1);
        let (entry_size, prefix_len, suffix_len) =
            builder.leaf_entry_size_with_prefix(&keys[idx], &values[idx], FLAG_INLINE);
        match builder.add_leaf_entry_with_prefix(
            &keys[idx],
            &values[idx],
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
                });
            }
        }
    }
    checksum
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn setup_search_columnar(fixed_be8: bool) -> (Page, Vec<Vec<u8>>) {
    let mut builder = Builder::new(Options {
        prefix: false,
        columnar: true,
    });
    let mut inserted = 0usize;
    for i in 0..BENCH_KEY_COUNT {
        let key = if fixed_be8 {
            be8(i as u64)
        } else {
            be16(i as u64, (i as u64).wrapping_mul(17).wrapping_add(3))
        };
        if builder.add_leaf_entry(&key, &[], FLAG_POINTER).is_err() {
            break;
        }
        inserted += 1;
    }
    let page = builder.finish();
    let mut queries = Vec::with_capacity(inserted);
    for i in 0..inserted {
        queries.push(if fixed_be8 {
            be8(i as u64)
        } else {
            be16(i as u64, (i as u64).wrapping_mul(17).wrapping_add(3))
        });
    }
    (page, queries)
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn bench_search_prepared(iters: usize, page: &mut Page, queries: &[Vec<u8>]) -> u64 {
    let mut checksum = 0u64;
    for i in 0..iters {
        let (idx, found) = page.search_leaf(black_box(&queries[i % queries.len()]));
        checksum = checksum.wrapping_add(idx as u64 + found as u64);
    }
    checksum
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn setup_search_prefix_variant(opts: Options) -> (Page, Vec<Vec<u8>>) {
    const KEY_COUNT: usize = 128;
    let keys = make_bench_keys(KEY_COUNT, 24);
    let mut builder = Builder::new(opts);
    for (i, key) in keys.iter().enumerate() {
        let flags = match i % 3 {
            0 => FLAG_INLINE,
            1 => FLAG_POINTER,
            _ => FLAG_TOMBSTONE,
        };
        let value = if flags == FLAG_INLINE {
            vec![i as u8, i.wrapping_add(1) as u8]
        } else {
            Vec::new()
        };
        builder
            .add_leaf_entry(key, &value, flags)
            .expect("prefix setup should fit");
    }
    let page = builder.finish();
    let mut queries = Vec::with_capacity(4096);
    for i in 0..4096 {
        let mut q = keys[i % keys.len()].clone();
        if i & 1 == 1 && !q.is_empty() {
            let last = q.len() - 1;
            q[last] ^= 0x01;
        }
        queries.push(q);
    }
    (page, queries)
}

#[derive(Debug)]
struct ErrFull;

impl Builder {
    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn new(opts: Options) -> Self {
        Self {
            data: vec![0u8; PAGE_SIZE],
            opts,
            count: 0,
            heap_start: PAGE_SIZE,
            dir_end: NODE_HEADER_SIZE,
            leaf_index: 0,
            prev_key: Vec::new(),
            arena: Vec::new(),
            value_arena: Vec::new(),
            entries: Vec::new(),
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
        self.prev_key.clear();
        self.arena.clear();
        self.value_arena.clear();
        self.entries.clear();
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
            && !self.prev_key.is_empty()
        {
            prefix_len = shared_prefix_len(key, &self.prev_key).min(key.len());
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
            self.prev_key.clear();
            self.prev_key.extend_from_slice(key);
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

        let key_off = self.arena.len();
        self.arena.extend_from_slice(key);
        let value_off = self.arena.len();
        if flags & FLAG_POINTER != 0 {
            self.arena.resize(self.arena.len() + VALUE_PTR_SIZE, 0);
        } else if flags & FLAG_TOMBSTONE == 0 {
            self.arena.extend_from_slice(value);
        }
        self.entries.push(ColumnarEntry {
            key_off,
            key_len: key.len(),
            value_off,
            value_len: value.len(),
            flags,
            prefix_len: 0,
        });
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

        let key_off = self.arena.len();
        self.arena.extend_from_slice(&key[prefix_len..]);
        let value_off = self.value_arena.len();
        if flags & FLAG_POINTER != 0 {
            self.value_arena
                .resize(self.value_arena.len() + VALUE_PTR_SIZE, 0);
        } else if flags & FLAG_TOMBSTONE == 0 {
            self.value_arena.extend_from_slice(value);
        }
        self.entries.push(ColumnarEntry {
            key_off,
            key_len: suffix_len,
            value_off,
            value_len: value.len(),
            flags,
            prefix_len,
        });
        self.key_bytes = next_key_bytes;
        self.value_bytes = next_value_bytes;
        self.count = next_count;
        self.leaf_index += 1;
        self.dir_end = dir_end;
        self.heap_start = heap_start;
        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);
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
        for (i, e) in self.entries.iter().enumerate() {
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

        self.data[values_start..suffix_start].copy_from_slice(&self.value_arena);
        self.data[suffix_start..].copy_from_slice(&self.arena);

        for (i, e) in self.entries.iter().enumerate() {
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
    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn search_leaf(&mut self, key: &[u8]) -> (usize, bool) {
        if self.opts.columnar && self.opts.prefix {
            self.search_columnar_prefix_v2(key)
        } else if self.opts.columnar {
            self.search_columnar_v2(key)
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

    #[cfg_attr(feature = "profile-attribution", inline(never))]
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
            let cmp =
                compare_prefix_virtual_key(&prev[..prev_len], layout.prefix_len, suffix, target);
            if cmp != Ordering::Less {
                return (idx, cmp == Ordering::Equal);
            }
            let key_len = layout.prefix_len + layout.suffix_len;
            prev[layout.prefix_len..key_len].copy_from_slice(suffix);
            prev_len = key_len;
        }
        (block_end, false)
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn prefix_restart_key(&self, index: usize) -> &[u8] {
        let off = self.offset_at(index);
        let layout = parse_prefix_layout(&self.data, off);
        &self.data[off + layout.key_off..off + layout.key_off + layout.suffix_len]
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
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

    #[cfg_attr(feature = "profile-attribution", inline(never))]
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
            let cmp = compare_prefix_virtual_key(&prev[..prev_len], prefix_len, suffix, target);
            if cmp != Ordering::Less {
                return (idx, cmp == Ordering::Equal);
            }
            let key_len = prefix_len + suffix.len();
            prev[prefix_len..key_len].copy_from_slice(suffix);
            prev_len = key_len;
        }
        (block_end, false)
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn columnar_prefix_suffix_at(&self, index: usize) -> &[u8] {
        let key_start = self.offset_at(index);
        let key_end = if index + 1 < self.count {
            self.offset_at(index + 1)
        } else {
            PAGE_SIZE
        };
        &self.data[key_start..key_end]
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
    fn columnar_prefix_len_at(&self, index: usize) -> usize {
        let flags_start = NODE_HEADER_SIZE + self.count * 4;
        let prefix_start = flags_start + self.count;
        get_u16(&self.data[prefix_start + index * 2..prefix_start + index * 2 + 2]) as usize
    }

    #[cfg_attr(feature = "profile-attribution", inline(never))]
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

#[cfg_attr(feature = "profile-attribution", inline(never))]
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
    if a.len() == 8 && b.len() == 8 {
        return u64::from_be_bytes(a.try_into().unwrap())
            .cmp(&u64::from_be_bytes(b.try_into().unwrap()));
    }
    a.cmp(b)
}

#[cfg_attr(feature = "profile-attribution", inline(never))]
fn compare_prefix_virtual_key(
    prev_key: &[u8],
    prefix_len: usize,
    suffix: &[u8],
    target: &[u8],
) -> Ordering {
    let prefix_cmp_len = prefix_len.min(target.len());
    if prefix_cmp_len > 0 {
        let cmp = prev_key[..prefix_cmp_len].cmp(&target[..prefix_cmp_len]);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    if target.len() < prefix_len {
        return Ordering::Greater;
    }
    let target_tail = &target[prefix_len..];
    let suffix_cmp_len = suffix.len().min(target_tail.len());
    if suffix_cmp_len > 0 {
        let cmp = suffix[..suffix_cmp_len].cmp(&target_tail[..suffix_cmp_len]);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    suffix.len().cmp(&target_tail.len())
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
