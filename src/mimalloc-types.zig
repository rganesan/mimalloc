// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.

const std = @import("std");
const Atomic = std.atomic.Atomic;

// ------------------------------------------------------
// Extended functionality
// ------------------------------------------------------
pub const SMALL_WSIZE_MAX = 128;
pub const SMALL_SIZE_MAX = SMALL_WSIZE_MAX * @sizeOf(usize);

pub const arena_id_t = i32;

// -------------------------------------------------------------------------------------
// Aligned allocation
// Note that `alignment` always follows `size` for consistency with unaligned
// allocation, but unfortunately this differs from `posix_memalign` and `aligned_alloc`.
// -------------------------------------------------------------------------------------
// maximum supported alignment is 16MiB (1MB for 32-bit)
pub const ALIGNMENT_MAX = if (@sizeOf(usize) > @sizeOf(u32)) 16 * 1024 * 1024 else 1024 * 1024;

// Minimal alignment necessary. On most platforms 16 bytes are needed
// due to SSE registers for example. This must be at least `sizeof(void*)`
pub const MAX_ALIGN_SIZE = 16;

// ------------------------------------------------------
// Variants
// ------------------------------------------------------
// Define NDEBUG in the release version to disable assertions.
// #define NDEBUG

// Define VALGRIND to enable valgrind support
const VALGRIND = false;

// Define SECURE to enable security mitigations
// SECURE = 1  // guard page around metadata
// SECURE = 2  // guard page around each mimalloc page
// SECURE = 3  // encode free lists (detect corrupted free list (buffer overflow), and invalid pointer free)
// SECURE = 4  // checks for double free. (may be more expensive)
pub const SECURE = 0;

// Define DEBUG for debug mode
// DEBUG = 1  // basic assertion checks and statistics, check double free, corrupted free list, and invalid pointer free.
// DEBUG = 2  // + internal assertion checks
// DEBUG = 3  // + extensive internal invariant checking (cmake -DDEBUG_FULL=ON)
pub const DEBUG = 2;

// Reserve extra padding at the end of each block to be more resilient against heap block overflows.
// The padding can detect byte-precise buffer overflow on free.
pub const PADDING = if (DEBUG >= 1 or VALGRIND) 1 else 0;

// Encoded free lists allow detection of corrupted free lists
// and can detect buffer overflows, modify after free, and double `free`s.
pub const ENCODE_FREELIST = (SECURE >= 3 or DEBUG >= 1);

// ------------------------------------------------------
// Platform specific values
// ------------------------------------------------------

// ------------------------------------------------------
// Size of a pointer.
// We assume that `sizeof(void*)==sizeof(intptr_t)`
// and it holds for all platforms we know of.
//
// However, the C standard only requires that:
//  p == (void*)((intptr_t)p))
// but we also need:
//  i == (intptr_t)((void*)i)
// or otherwise one might define an intptr_t type that is larger than a pointer...
// ------------------------------------------------------
// if @sizeof(usize) > @sizeOf(u64) assume 128-bit  (as on arm CHERI for example) - is this even
// possible in zig?
pub const INTPTR_SHIFT = if (@sizeOf(usize) > @sizeOf(u64)) 4 else if (@sizeOf(usize) == @sizeOf(u64)) 3 else if (@sizeOf(usize) == @sizeOf(u32)) 2 else unreachable;

pub const SIZE_SHIFT = if (@sizeOf(usize) == u64) 3 else if (@sizeOf(usize) == @sizeOf(u32)) 2 else unreachable;
pub const ssize_t = isize;

pub const INTPTR_SIZE = 1 << INTPTR_SHIFT;
pub const INTPTR_BITS = INTPTR_SIZE * 8;

pub const SIZE_SIZE = 1 << SIZE_SHIFT;
pub const SIZE_BITS = SIZE_SIZE * 8;

pub const KiB = 1024;
pub const MiB = KiB * KiB;
pub const GiB = MiB * KiB;

// ------------------------------------------------------
// Main internal data-structures
// ------------------------------------------------------

// Main tuning parameters for segment and page sizes
// Sizes for 64-bit (usually divide by two for 32-bit)
pub const SEGMENT_SLICE_SHIFT = 13 + INTPTR_SHIFT; // 64KiB  (32KiB on 32-bit)
pub const SEGMENT_SHIFT = SEGMENT_SLICE_SHIFT + if (INTPTR_SIZE > 4) 10 else 7; // 64MiB  (4MiB on 32-bit)

pub const SMALL_PAGE_SHIFT = SEGMENT_SLICE_SHIFT; // 64KiB
pub const MEDIUM_PAGE_SHIFT = 3 + SMALL_PAGE_SHIFT; // 512KiB

// Derived constants
pub const SEGMENT_SIZE = 1 << SEGMENT_SHIFT;
pub const SEGMENT_ALIGN = SEGMENT_SIZE;
pub const SEGMENT_MASK = SEGMENT_SIZE - 1;
pub const SEGMENT_SLICE_SIZE = 1 << SEGMENT_SLICE_SHIFT;
pub const SLICES_PER_SEGMENT = SEGMENT_SIZE / SEGMENT_SLICE_SIZE; // 1024

pub const SMALL_PAGE_SIZE = 1 << SMALL_PAGE_SHIFT;
pub const MEDIUM_PAGE_SIZE = 1 << MEDIUM_PAGE_SHIFT;
pub const SMALL_OBJ_SIZE_MAX = SMALL_PAGE_SIZE / 4; // 8KiB on 64-bit
pub const MEDIUM_OBJ_SIZE_MAX = MEDIUM_PAGE_SIZE / 4; // 128KiB on 64-bit
pub const MEDIUM_OBJ_WSIZE_MAX = MEDIUM_OBJ_SIZE_MAX / INTPTR_SIZE;
pub const LARGE_OBJ_SIZE_MAX = SEGMENT_SIZE / 2; // 32MiB on 64-bit
pub const LARGE_OBJ_WSIZE_MAX = LARGE_OBJ_SIZE_MAX / INTPTR_SIZE;

// Maximum number of size classes. (spaced exponentially in 12.5% increments)
pub const BIN_HUGE = 73;

//#if (MEDIUM_OBJ_WSIZE_MAX >= 655360)
//#error "mimalloc internal: define more bins"
//#endif
//#if (ALIGNMENT_MAX > SEGMENT_SIZE/2)
//#error "mimalloc internal: the max aligned boundary is too large for the segment size"
//#endif
//#if (ALIGNED_MAX % SEGMENT_SLICE_SIZE != 0)
//#error "mimalloc internal: the max aligned boundary must be an integral multiple of the segment slice size"
//#endif

// Maximum slice offset (15)
pub const MAX_SLICE_OFFSET = (ALIGNMENT_MAX / SEGMENT_SLICE_SIZE) - 1;

// Used as a special value to encode block sizes in 32 bits.
pub const HUGE_BLOCK_SIZE = 2 * GiB;

// blocks up to this size are always allocated aligned
pub const MAX_ALIGN_GUARANTEE = 8 * MAX_ALIGN_SIZE;

// ------------------------------------------------------
// Mimalloc pages contain allocated blocks
// ------------------------------------------------------

// The free lists use encoded next fields
// (Only actually encodes when ENCODED_FREELIST is defined.)
pub const encoded_t = usize;

// thread id's
pub const threadid_t = usize;

// free lists contain blocks
pub const block_t = struct {
    next: encoded_t,
};

// The delayed flags are used for efficient multi-threaded free-ing
pub const delayed_t = enum(u2) {
    USE_DELAYED_FREE, // push on the owning heap thread delayed list
    DELAYED_FREEING, // temporary: another thread is accessing the owning heap
    NO_DELAYED_FREE, // optimize: push on page local thread free queue if another block is already in the heap thread delayed free list
    NEVER_DELAYED_FREE, // sticky, only resets on page reclaim
};

// The `in_full` and `has_aligned` page flags are put in a union to efficiently
// test if both are false (`full_aligned == 0`) in the `free` routine.
// under thread sanitizer, use a byte for each flag to suppress warning, issue #130
pub const page_flags_t = packed union {
    full_aligned: u8,
    x: packed struct {
        in_full: u1 = 0,
        has_aligned: u1 = 0,
    },
};

// Thread free list.
// We use the bottom 2 bits of the pointer for delayed_t flags
pub const thread_free_t = usize;

pub const deferred_free_fun = fn (bool, c_ulonglong, ?*anyopaque) callconv(.C) void;
pub extern fn register_deferred_free(deferred_free: ?*const deferred_free_fun, arg: ?*anyopaque) void;
pub const output_fun = fn ([*c]const u8, ?*anyopaque) callconv(.C) void;
pub extern fn register_output(out: ?*const output_fun, arg: ?*anyopaque) void;
pub const error_fun = fn (c_int, ?*anyopaque) callconv(.C) void;

// A page contains blocks of one specific size (`block_size`).
// Each page has three list of free blocks:
// `free` for blocks that can be allocated,
// `local_free` for freed blocks that are not yet available to `malloc`
// `thread_free` for freed blocks by other threads
// The `local_free` and `thread_free` lists are migrated to the `free` list
// when it is exhausted. The separate `local_free` list is necessary to
// implement a monotonic heartbeat. The `thread_free` list is needed for
// avoiding atomic operations in the common case.
//
//
// `used - |thread_free|` == actual blocks that are in use (alive)
// `used - |thread_free| + |free| + |local_free| == capacity`
//
// We don't count `freed` (as |free|) but use `used` to reduce
// the number of memory accesses in the `page_all_free` function(s).
//
// Notes:
// - Access is optimized for `free` and `page_alloc` (in `alloc.c`)
// - Using `uint16_t` does not seem to slow things down
// - The size is 8 words on 64-bit which helps the page index calculations
//   (and 10 words on 32-bit, and encoded free lists add 2 words. Sizes 10
//    and 12 are still good for address calculation)
// - To limit the structure size, the `xblock_size` is 32-bits only; for
//   blocks > HUGE_BLOCK_SIZE the size is determined from the segment page size
// - `thread_free` uses the bottom bits as a delayed-free flags to optimize
//   concurrent frees where only the first concurrent free adds to the owning
//   heap `thread_delayed_free` list (see `alloc.c:free_block_mt`).
//   The invariant is that no-delayed-free is only set if there is
//   at least one block that will be added, or as already been added, to
//   the owning heap `thread_delayed_free` list. This guarantees that pages
//   will be freed correctly even if only other threads free blocks.

pub const page_t = struct {
    // "owned" by the segment
    slice_count: u32 = 0, // slices in this page (0 if not a page)
    slice_offset: u32 = 0, // distance from the actual page data slice (0 if a page)
    b: packed struct { // workaround for Zig not allowing atomics/arrays in packed struct
        is_reset: bool = false, // `true` if the page memory was reset
        is_committed: bool = false, // `true` if the page virtual memory is committed
        is_zero_init: bool = false, // `true` if the page was zero initialized
        is_zero: bool = false, // `true` if the blocks in the free list are zero initialized
        retire_expire: u7 = 0, // expiration count for retired blocks
    } = .{},
    flags: page_flags_t = .{ .full_aligned = 0 }, // `in_full` and `has_aligned` flags (8 bits)

    // layout like this to optimize access in `malloc` and `free`
    capacity: u16 = 0, // number of blocks committed, must be the first field, see `segment.c:page_clear`
    reserved: u16 = 0, // number of blocks reserved in memory

    free: ?*block_t = null, // list of available free blocks (`malloc` allocates from this list)

    keys: if (ENCODE_FREELIST) [2]usize else u0 = if (ENCODE_FREELIST) .{ 0, 0 } else 0, // two random keys to encode the free lists (see `_block_next`)
    used: u32 = 0, // number of blocks in use (including blocks in `local_free` and `thread_free`)
    xblock_size: u32 = 0, // size available in each block (always `>0`)

    local_free: ?*block_t = null, // list of deferred free blocks by this thread (migrates to `free`)
    xthread_free: Atomic(thread_free_t) = Atomic(thread_free_t).init(0), // list of deferred free blocks freed by other threads
    xheap: Atomic(usize) = Atomic(usize).init(0),
    next: ?*page_t = null, // next page owned by this thread with the same `block_size`
    prev: ?*page_t = null, // previous page owned by this thread with the same `block_size`

    // 64-bit 9 words, 32-bit 12 words, (+2 for secure)
    padding: if (INTPTR_SIZE == 8) [1]usize else u0 = if (INTPTR_SIZE == 8) .{0} else 0,
};

pub const page_kind_t = enum {
    PAGE_SMALL, // small blocks go into 64KiB pages inside a segment
    PAGE_MEDIUM, // medium blocks go into medium pages inside a segment
    PAGE_LARGE, // larger blocks go into a page of just one block
    PAGE_HUGE, // huge blocks (> 16 MiB) are put into a single page in a single segment.
};

pub const segment_kind_t = enum {
    SEGMENT_NORMAL, // SEGMENT_SIZE size with pages inside.
    SEGMENT_HUGE, // > LARGE_SIZE_MAX segment with just one huge page inside.
};

// ------------------------------------------------------
// A segment holds a commit mask where a bit is set if
// the corresponding COMMIT_SIZE area is committed.
// The COMMIT_SIZE must be a multiple of the slice
// size. If it is equal we have the most fine grained
// decommit (but setting it higher can be more efficient).
// The MINIMAL_COMMIT_SIZE is the minimal amount that will
// be committed in one go which can be set higher than
// COMMIT_SIZE for efficiency (while the decommit mask
// is still tracked in fine-grained COMMIT_SIZE chunks)
// ------------------------------------------------------

const MINIMAL_COMMIT_SIZE = 2 * MiB;
const COMMIT_SIZE = SEGMENT_SLICE_SIZE; // 64KiB
const COMMIT_MASK_BITS = SEGMENT_SIZE / COMMIT_SIZE;
const COMMIT_MASK_FIELD_BITS = SIZE_BITS;
const COMMIT_MASK_FIELD_COUNT = COMMIT_MASK_BITS / COMMIT_MASK_FIELD_BITS;

//#if (COMMIT_MASK_BITS != (COMMIT_MASK_FIELD_COUNT * COMMIT_MASK_FIELD_BITS))
//#error "the segment size must be exactly divisible by the (commit size * size_t bits)"
//#endif

const commit_mask_t = struct {
    mask: [COMMIT_MASK_FIELD_COUNT]usize,
};

const slice_t = page_t;
const msecs_t = i64;

// Segments are large allocated memory blocks (8mb on 64 bit) from
// the OS. Inside segments we allocated fixed size _pages_ that
// contain blocks.
pub const segment_t = struct {
    memid: usize, // memory id for arena allocation
    mem_is_pinned: bool, // `true` if we cannot decommit/reset/protect in this memory (i.e. when allocated using large OS pages)
    mem_is_large: bool, // in large/huge os pages?
    mem_is_committed: bool, // `true` if the whole segment is eagerly committed

    allow_decommit: bool,
    decommit_expire: msecs_t,
    decommit_mask: commit_mask_t,
    commit_mask: commit_mask_t,

    abandoned_next: Atomic(?*segment_t),

    // from here is zero initialized
    next: ?*segment_t, // the list of freed segments in the cache (must be first field, see `segment.c:segment_init`)

    abandoned: usize, // abandoned pages (i.e. the original owning thread stopped) (`abandoned <= used`)
    abandoned_visits: usize, // count how often this segment is visited in the abandoned list (to force reclaim it it is too long)
    used: usize, // count of pages in use
    cookie: usize, // verify addresses in debug mode: `ptr_cookie(segment) == segment->cookie`

    segment_slices: usize, // for huge segments this may be different from `SLICES_PER_SEGMENT`
    segment_info_slices: usize, // initial slices we are using segment info and possible guard pages.

    // layout like this to optimize access in `free`
    kind: segment_kind_t,
    thread_id: Atomic(threadid_t), // unique id of the thread owning this segment
    slice_entries: usize, // entries in the `slices` array, at most `SLICES_PER_SEGMENT`
    slices: [SLICES_PER_SEGMENT]slice_t,
};

// ------------------------------------------------------
// Heaps
// Provide first-class heaps to allocate from.
// A heap just owns a set of pages for allocation and
// can only be allocate/reallocate from the thread that created it.
// Freeing blocks can be done from any thread though.
// Per thread, the segments are shared among its heaps.
// Per thread, there is always a default heap that is
// used for allocation; it is initialized to statically
// point to an empty heap to avoid initialization checks
// in the fast path.
// ------------------------------------------------------

// Pages of a certain block size are held in a queue.
const page_queue_t = struct {
    first: ?*page_t = null,
    last: ?*page_t = null,
    block_size: usize = 0,
};

pub const BIN_FULL = BIN_HUGE + 1;

// Random context
const random_ctx_t = struct {
    input: [16]u32,
    output: [16]u32,
    output_available: i32,
};

// In debug mode there is a padding structure at the end of the blocks to check for buffer overflows
const padding_t = if (PADDING) struct {
    canary: u32, // encoded block value to check validity of the padding (in case of overflow)
    delta: u32, // padding bytes before the block. (usable_size(p) - delta == exact allocated bytes)
} else u0;

const PADDING_SIZE = @sizeOf(padding_t);
const PADDING_WSIZE = (PADDING_SIZE + INTPTR_SIZE - 1) / INTPTR_SIZE;

const PAGES_DIRECT = (SMALL_WSIZE_MAX + PADDING_WSIZE + 1);

// A heap owns a set of pages.
pub const heap_t = struct {
    tld: ?*tld_t = null,
    pages_free_direct: [PAGES_DIRECT]?*page_t = [_]page_t{null} ** PAGES_DIRECT, // optimize: array where every entry points a page with possibly free blocks in the corresponding queue for that size.
    pages: [BIN_FULL + 1]page_queue_t = [_]page_queue_t{.{}} ** (BIN_FULL + 1), // queue of pages for each size class (or "bin")
    thread_delayed_free: Atomic(?*block_t) = Atomic(?*block_t).init(null),
    thread_id: threadid_t = 0, // thread this heap belongs too
    arena_id: arena_id_t = 0, // arena id if the heap belongs to a specific arena (or 0)
    cookie: usize = 0, // random cookie to verify pointers (see `_ptr_cookie`)
    keys: [2]usize = .{ 0, 0 }, // two random keys used to encode the `thread_delayed_free` list
    random: random_ctx_t = 0, // random number context used for secure allocation
    page_count: usize = 0, // total number of pages in the `pages` queues.
    page_retired_min: usize = BIN_FULL, // smallest retired index (retired pages are fully free, but still in the page queues)
    page_retired_max: usize = 0, // largest retired index into the `pages` array.
    next: *heap_t = null, // list of heaps per thread
    no_reclaim: bool = false, // `true` if this heap should not reclaim abandoned pages
};

// ------------------------------------------------------
// Debug
// ------------------------------------------------------

const DEBUG_UNINIT = 0xD0;
const DEBUG_FREED = 0xDF;
const DEBUG_PADDING = 0xDE;

const assert = std.debug.assert;

// ------------------------------------------------------
// Statistics
// ------------------------------------------------------

const STAT = if (DEBUG > 0) 2 else 0;

pub const stat_count_t = struct {
    allocated: i64 = 0,
    freed: i64 = 0,
    peak: i64 = 0,
    current: i64 = 0,
};

pub const stat_counter_t = struct {
    total: i64,
    count: i64,
};

pub const stats_t = struct {
    segments: stat_count_t,
    pages: stat_count_t,
    reserved: stat_count_t,
    committed: stat_count_t,
    reset: stat_count_t,
    page_committed: stat_count_t,
    segments_abandoned: stat_count_t,
    pages_abandoned: stat_count_t,
    threads: stat_count_t,
    normal: stat_count_t,
    huge: stat_count_t,
    large: stat_count_t,
    malloc: stat_count_t,
    segments_cache: stat_count_t,
    pages_extended: stat_counter_t,
    mmap_calls: stat_counter_t,
    commit_calls: stat_counter_t,
    page_no_retire: stat_counter_t,
    searches: stat_counter_t,
    normal_count: stat_counter_t,
    huge_count: stat_counter_t,
    large_count: stat_counter_t,
    normal_bins: if (STAT > 1) [74]stat_count_t else u0,
};

// ------------------------------------------------------
// Thread Local data
// ------------------------------------------------------

// A "span" is is an available range of slices. The span queues keep
// track of slice spans of at most the given `slice_count` (but more than the previous size class).
pub const span_queue_t = struct {
    first: ?*slice_t = 0,
    last: ?*slice_t = 0,
    slice_count: usize,
};

const SEGMENT_BIN_MAX = 35; // 35 == segment_bin(SLICES_PER_SEGMENT)

// OS thread local data
pub const os_tld_t = struct {
    region_idx: usize = 0, // start point for next allocation
    stats: ?[*]stats_t = null, // points to tld stats
};

// Segments thread local data
pub const segments_tld_t = struct {
    spans: [36]span_queue_t, // free slice spans inside segments
    count: usize = 0, // current number of segments;
    peak_count: usize = 0, // peak number of segments
    current_size: usize = 0, // current size of all segments
    peak_size: usize = 0, // peak size of all segments
    stats: ?*stats_t = null, // points to tld stats
    os: ?*os_tld_t = null, // points to os stats
};

pub const tld_t = struct {
    heartbeat: u64 = 0, // monotonic heartbeat count
    recurse: bool = false, // true if deferred was called; used to prevent infinite recursion.
    heap_backing: ?*heap_t = null, // backing heap of this thread (cannot be deleted)
    heaps: ?*heap_t = null, // list of heaps in this thread (so we can abandon all when the thread terminates)
    segments: segments_tld_t, // segment tld
    os: os_tld_t = .{}, // os tld
    stats: stats_t = .{}, // statistics
};
