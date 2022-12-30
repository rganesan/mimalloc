// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.

const std = @import("std");
const builtin = std.builtin;
const mem = std.mem;
const os = std.os;
const assert = std.debug.assert;
const AtomicOrder = builtin.AtomicOrder;
const Atomic = std.atomic.Atomic;
const Random = std.rand.Random;
const Prng = std.rand.DefaultPrng;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("heap.zig");
};

const mi_assert = std.debug.assert;

const _mi_heap_random_next = mi._mi_heap_random_next;
const mi_get_default_heap = mi.mi_get_default_heap;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

// Align a byte size to a size in _machine words_,
// i.e. byte size == `wsize*sizeof(void*)`.
pub inline fn _mi_wsize_from_size(size: usize) usize {
    mi_assert(size <= std.math.maxInt(usize) - @sizeOf(usize));
    return (size + @sizeOf(usize) - 1) / @sizeOf(usize);
}

// ------------------------------------------------------
// Extended functionality
// ------------------------------------------------------
pub const MI_SMALL_WSIZE_MAX = 128;
pub const MI_SMALL_SIZE_MAX = MI_SMALL_WSIZE_MAX * @sizeOf(usize);

pub const mi_arena_id_t = i32;
pub const MI_ARENA_ID_NONE = 0;

// -------------------------------------------------------------------------------------
// Aligned allocation
// Note that `alignment` always follows `size` for consistency with unaligned
// allocation, but unfortunately this differs from `posix_memalign` and `aligned_alloc`.
// -------------------------------------------------------------------------------------

// Minimal alignment necessary. On most platforms 16 bytes are needed
// due to SSE registers for example. This must be at least `sizeof(void*)`
pub const MI_MAX_ALIGN_SIZE = 16;

// ------------------------------------------------------
// Variants
// ------------------------------------------------------
// Define NDEBUG in the release version to disable assertions.
// #define NDEBUG

// Define VALGRIND to enable valgrind support
const MI_VALGRIND = false;

// Define MI_SECURE to enable security mitigations
// MI_SECURE = 1  // guard page around metadata
// MI_SECURE = 2  // guard page around each mimalloc page
// MI_SECURE = 3  // encode free lists (detect corrupted free list (buffer overflow), and invalid pointer free)
// MI_SECURE = 4  // checks for double free. (may be more expensive)
pub const MI_SECURE = 0;

// Define MI_DEBUG for debug mode
// MI_DEBUG = 1  // basic assertion checks and statistics, check double free, corrupted free list, and invalid pointer free.
// MI_DEBUG = 2  // + internal assertion checks
// MI_DEBUG = 3  // + extensive internal invariant checking (cmake -DDEBUG_FULL=ON)
pub const MI_DEBUG = 2;

// Reserve extra padding at the end of each block to be more resilient against heap block overflows.
// The padding can detect byte-precise buffer overflow on free.
pub const MI_PADDING = if (MI_DEBUG >= 1 or MI_VALGRIND) 1 else 0;

// Encoded free lists allow detection of corrupted free lists
// and can detect buffer overflows, modify after free, and double `free`s.
pub const MI_ENCODE_FREELIST = (MI_SECURE >= 3 or MI_DEBUG >= 1);

// We used to abandon huge pages but to eagerly deallocate if freed from another thread,
// but that makes it not possible to visit them during a heap walk or include them in a
// `mi_heap_destroy`. We therefore instead reset/decommit the huge blocks if freed from
// another thread so most memory is available until it gets properly freed by the owning thread.
pub const MI_HUGE_PAGE_ABANDON = false;

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
pub const MI_INTPTR_SHIFT = if (@sizeOf(usize) > @sizeOf(u64)) 4 else if (@sizeOf(usize) == @sizeOf(u64)) 3 else if (@sizeOf(usize) == @sizeOf(u32)) 2 else unreachable;

pub const MI_SIZE_SHIFT = if (@sizeOf(usize) == @sizeOf(u64)) 3 else if (@sizeOf(usize) == @sizeOf(u32)) 2 else unreachable;
pub const ssize_t = isize;

pub const MI_INTPTR_SIZE = 1 << MI_INTPTR_SHIFT;
pub const MI_INTPTR_BITS = MI_INTPTR_SIZE * 8;

pub const MI_SIZE_SIZE = 1 << MI_SIZE_SHIFT;
pub const MI_SIZE_BITS = MI_SIZE_SIZE * 8;

pub const MI_KiB = 1024;
pub const MI_MiB = MI_KiB * MI_KiB;
pub const MI_GiB = MI_MiB * MI_KiB;

// ------------------------------------------------------
// Main internal data-structures
// ------------------------------------------------------

// Main tuning parameters for segment and page sizes
// Sizes for 64-bit (usually divide by two for 32-bit)
pub const MI_SEGMENT_SLICE_SHIFT = 13 + MI_INTPTR_SHIFT; // 64KiB  (32KiB on 32-bit)
pub const MI_SEGMENT_SHIFT = MI_SEGMENT_SLICE_SHIFT + if (MI_INTPTR_SIZE > 4) 9 else 7; // 32MiB  (4MiB on 32-bit)

pub const MI_SMALL_PAGE_SHIFT = MI_SEGMENT_SLICE_SHIFT; // 64KiB
pub const MI_MEDIUM_PAGE_SHIFT = 3 + MI_SMALL_PAGE_SHIFT; // 512KiB

// Derived constants
pub const MI_SEGMENT_SIZE = 1 << MI_SEGMENT_SHIFT;
pub const MI_SEGMENT_ALIGN = MI_SEGMENT_SIZE;
pub const MI_SEGMENT_MASK = @intCast(usize, MI_SEGMENT_ALIGN - 1);
pub const MI_SEGMENT_SLICE_SIZE = 1 << MI_SEGMENT_SLICE_SHIFT;
pub const MI_SLICES_PER_SEGMENT = MI_SEGMENT_SIZE / MI_SEGMENT_SLICE_SIZE; // 1024

pub const MI_SMALL_PAGE_SIZE = 1 << MI_SMALL_PAGE_SHIFT;
pub const MI_MEDIUM_PAGE_SIZE = 1 << MI_MEDIUM_PAGE_SHIFT;
pub const MI_SMALL_OBJ_SIZE_MAX = MI_SMALL_PAGE_SIZE / 4; // 8KiB on 64-bit
pub const MI_MEDIUM_OBJ_SIZE_MAX = MI_MEDIUM_PAGE_SIZE / 4; // 128KiB on 64-bit
pub const MI_MEDIUM_OBJ_WSIZE_MAX = MI_MEDIUM_OBJ_SIZE_MAX / MI_INTPTR_SIZE;
pub const MI_LARGE_OBJ_SIZE_MAX = MI_SEGMENT_SIZE / 2; // 32MiB on 64-bit
pub const MI_LARGE_OBJ_WSIZE_MAX = MI_LARGE_OBJ_SIZE_MAX / MI_INTPTR_SIZE;

// Maximum number of size classes. (spaced exponentially in 12.5% increments)
pub const MI_BIN_HUGE = 73;

//#if (MI_MEDIUM_OBJ_WSIZE_MAX >= 655360)
//#error "mimalloc internal: define more bins"
//#endif

// Maximum slice offset (15)
pub const MI_MAX_SLICE_OFFSET = (MI_ALIGNMENT_MAX / MI_SEGMENT_SLICE_SIZE) - 1;

// Used as a special value to encode block sizes in 32 bits.
pub const MI_HUGE_BLOCK_SIZE = 2 * MI_GiB;

// blocks up to this size are always allocated aligned
pub const MI_MAX_ALIGN_GUARANTEE = 8 * MI_MAX_ALIGN_SIZE;

// Alignments over MI_ALIGNMENT_MAX are allocated in dedicated huge page segments
pub const MI_ALIGNMENT_MAX = (MI_SEGMENT_SIZE >> 1);

// ------------------------------------------------------
// Mimalloc pages contain allocated blocks
// ------------------------------------------------------

// The free lists use encoded next fields
// (Only actually encodes when ENCODED_FREELIST is defined.)
pub const mi_encoded_t = usize;

// thread id's
pub const mi_threadid_t = u64;

// free lists contain blocks
pub const mi_block_t = struct {
    next: mi_encoded_t,
};

// The delayed flags are used for efficient multi-threaded free-ing
pub const mi_delayed_t = enum(u2) {
    MI_USE_DELAYED_FREE, // push on the owning heap thread delayed list
    MI_DELAYED_FREEING, // temporary: another thread is accessing the owning heap
    MI_NO_DELAYED_FREE, // optimize: push on page local thread free queue if another block is already in the heap thread delayed free list
    MI_NEVER_DELAYED_FREE, // sticky, only resets on page reclaim
};

// The `in_full` and `has_aligned` page flags are put in a union to efficiently
// test if both are false (`full_aligned == 0`) in the `free` routine.
// under thread sanitizer, use a byte for each flag to suppress warning, issue #130
pub const mi_page_flags_t = packed union {
    full_aligned: u8,
    x: packed struct {
        in_full: u1 = 0,
        has_aligned: u1 = 0,
    },
};

// Thread free list.
// We use the bottom 2 bits of the pointer for delayed_t flags
pub const mi_thread_free_t = usize;

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

pub const mi_page_t = struct {
    const Self = @This();

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
    flags: mi_page_flags_t = .{ .full_aligned = 0 }, // `in_full` and `has_aligned` flags (8 bits)

    // layout like this to optimize access in `malloc` and `free`
    capacity: u16 = 0, // number of blocks committed, must be the first field, see `segment.c:page_clear`
    reserved: u16 = 0, // number of blocks reserved in memory

    free: ?*mi_block_t = null, // list of available free blocks (`malloc` allocates from this list)
    used: u32 = 0, // number of blocks in use (including blocks in `local_free` and `thread_free`)
    xblock_size: u32 = 0, // size available in each block (always `>0`)

    local_free: ?*mi_block_t = null, // list of deferred free blocks by this thread (migrates to `free`)

    keys: if (MI_ENCODE_FREELIST) [2]usize else u0 = if (MI_ENCODE_FREELIST) .{ 0, 0 } else 0, // two random keys to encode the free lists (see `_block_next`)
    xthread_free: Atomic(mi_thread_free_t) = Atomic(mi_thread_free_t).init(0), // list of deferred free blocks freed by other threads
    xheap: Atomic(?*mi_heap_t) = undefined,
    next: ?*mi_page_t = null, // next page owned by this thread with the same `block_size`
    prev: ?*mi_page_t = null, // previous page owned by this thread with the same `block_size`

    // 64-bit 9 words, 32-bit 12 words, (+2 for secure)
    padding: if (MI_INTPTR_SIZE == 8) [1]usize else u0 = if (MI_INTPTR_SIZE == 8) .{0} else 0,

    pub fn init() Self {
        return Self{ .xheap = Atomic(?*mi_heap_t).init(null) };
    }
};

pub const mi_page_kind_t = enum {
    MI_PAGE_SMALL, // small blocks go into 64KiB pages inside a segment
    MI_PAGE_MEDIUM, // medium blocks go into medium pages inside a segment
    MI_PAGE_LARGE, // larger blocks go into a page of just one block
    MI_PAGE_HUGE, // huge blocks (> 16 MiB) are put into a single page in a single segment.
};

pub const mi_segment_kind_t = enum {
    MI_SEGMENT_NORMAL, // MI_SEGMENT_SIZE size with pages inside.
    MI_SEGMENT_HUGE, // > MI_LARGE_SIZE_MAX segment with just one huge page inside.
};

// ------------------------------------------------------
// A segment holds a commit mask where a bit is set if
// the corresponding MI_COMMIT_SIZE area is committed.
// The MI_COMMIT_SIZE must be a multiple of the slice
// size. If it is equal we have the most fine grained
// decommit (but setting it higher can be more efficient).
// The MI_MINIMAL_COMMIT_SIZE is the minimal amount that will
// be committed in one go which can be set higher than
// MI_COMMIT_SIZE for efficiency (while the decommit mask
// is still tracked in fine-grained MI_COMMIT_SIZE chunks)
// ------------------------------------------------------

pub const MI_MINIMAL_COMMIT_SIZE = (16 * MI_SEGMENT_SLICE_SIZE); // 1MiB
pub const MI_COMMIT_SIZE = MI_SEGMENT_SLICE_SIZE; // 64KiB
pub const MI_COMMIT_MASK_BITS = MI_SEGMENT_SIZE / MI_COMMIT_SIZE;
pub const MI_COMMIT_MASK_FIELD_BITS = MI_SIZE_BITS;
pub const MI_COMMIT_MASK_FIELD_COUNT = MI_COMMIT_MASK_BITS / MI_COMMIT_MASK_FIELD_BITS;

//#if (MI_COMMIT_MASK_BITS != (MI_COMMIT_MASK_FIELD_COUNT * MI_COMMIT_MASK_FIELD_BITS))
//#error "the segment size must be exactly divisible by the (commit size * size_t bits)"
//#endif

pub const mi_commit_mask_t = struct {
    mask: [MI_COMMIT_MASK_FIELD_COUNT]usize,
};

pub const mi_slice_t = mi_page_t;
pub const mi_msecs_t = i64;

// Segments are large allocated memory blocks (8mb on 64 bit) from
// the OS. Inside segments we allocated fixed size _pages_ that
// contain blocks.
pub const mi_segment_t = struct {
    memid: usize, // memory id for arena allocation
    mem_is_pinned: bool, // `true` if we cannot decommit/reset/protect in this memory (i.e. when allocated using large OS pages)
    mem_is_large: bool, // in large/huge os pages?
    mem_is_committed: bool, // `true` if the whole segment is eagerly committed

    mem_alignment: usize, // page alignment for huge pages (only used for alignment > MI_ALIGNMENT_MAX)
    mem_align_offset: usize, // offset for huge page alignment (only used for alignment > MI_ALIGNMENT_MAX)
    allow_decommit: bool,
    decommit_expire: mi_msecs_t,
    decommit_mask: mi_commit_mask_t,
    commit_mask: mi_commit_mask_t,

    abandoned_next: Atomic(?*mi_segment_t),

    // from here is zero initialized
    next: ?*mi_segment_t, // the list of freed segments in the cache (must be first field, see `segment.c:segment_init`)

    abandoned: usize, // abandoned pages (i.e. the original owning thread stopped) (`abandoned <= used`)
    abandoned_visits: usize, // count how often this segment is visited in the abandoned list (to force reclaim it it is too long)
    used: usize, // count of pages in use
    cookie: usize, // verify addresses in debug mode: `ptr_cookie(segment) == segment->cookie`

    segment_slices: usize, // for huge segments this may be different from `MI_SLICES_PER_SEGMENT`
    segment_info_slices: usize, // initial slices we are using segment info and possible guard pages.

    // layout like this to optimize access in `free`
    kind: mi_segment_kind_t,
    slice_entries: usize, // entries in the `slices` array, at most `MI_SLICES_PER_SEGMENT`
    thread_id: Atomic(mi_threadid_t), // unique id of the thread owning this segment
    slices: [MI_SLICES_PER_SEGMENT]mi_slice_t,
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
pub const mi_page_queue_t = struct {
    const Self = @This();

    first: ?*mi_page_t = null,
    last: ?*mi_page_t = null,
    block_size: usize = 0,
};

pub const MI_BIN_FULL = MI_BIN_HUGE + 1;

// In debug mode there is a padding structure at the end of the blocks to check for buffer overflows
pub const mi_padding_t = if (MI_PADDING > 0) struct {
    canary: u32, // encoded block value to check validity of the padding (in case of overflow)
    delta: u32, // padding bytes before the block. (usable_size(p) - delta == exact allocated bytes)
} else u0;

pub const MI_PADDING_SIZE = @sizeOf(mi_padding_t);
const MI_PADDING_WSIZE = (MI_PADDING_SIZE + MI_INTPTR_SIZE - 1) / MI_INTPTR_SIZE;

pub const MI_PAGES_DIRECT = (MI_SMALL_WSIZE_MAX + MI_PADDING_WSIZE + 1);

// A heap owns a set of pages.
pub const mi_heap_t = struct {
    const Self = @This();

    tld: ?*mi_tld_t = null,
    pages_free_direct: [MI_PAGES_DIRECT]*mi_page_t = [_]*mi_page_t{&mi._mi_page_empty} ** MI_PAGES_DIRECT, // optimize: array where every entry points a page with possibly free blocks in the corresponding queue for that size.
    pages: [MI_BIN_FULL + 1]mi_page_queue_t = [_]mi_page_queue_t{.{}} ** (MI_BIN_FULL + 1), // queue of pages for each size class (or "bin")
    thread_delayed_free: Atomic(?*mi_block_t) = Atomic(?*mi_block_t).init(null),
    thread_id: mi_threadid_t = 0, // thread this heap belongs too
    arena_id: mi_arena_id_t = 0, // arena id if the heap belongs to a specific arena (or 0)
    cookie: usize = 0, // random cookie to verify pointers (see `_ptr_cookie`)
    keys: [2]usize = .{ 0, 0 }, // two random keys used to encode the `thread_delayed_free` list
    random: Prng = undefined, // random number context used for secure allocation
    page_count: usize = 0, // total number of pages in the `pages` queues.
    page_retired_min: usize = MI_BIN_FULL, // smallest retired index (retired pages are fully free, but still in the page queues)
    page_retired_max: usize = 0, // largest retired index into the `pages` array.
    next: ?*mi_heap_t = null, // list of heaps per thread
    no_reclaim: bool = false, // `true` if this heap should not reclaim abandoned pages
};

// ------------------------------------------------------
// Debug
// ------------------------------------------------------

pub const MI_DEBUG_UNINIT = 0xD0;
pub const MI_DEBUG_FREED = 0xDF;
pub const MI_DEBUG_PADDING = 0xDE;

// ------------------------------------------------------
// Statistics
// ------------------------------------------------------

pub const MI_STAT = if (MI_DEBUG > 0) 2 else 0;

pub const mi_stat_count_t = struct {
    allocated: i64 = 0,
    freed: i64 = 0,
    peak: i64 = 0,
    current: i64 = 0,
};

pub const mi_stat_counter_t = struct {
    total: i64 = 0,
    count: i64 = 0,
};

pub const mi_stats_t = struct {
    segments: mi_stat_count_t = .{},
    pages: mi_stat_count_t = .{},
    reserved: mi_stat_count_t = .{},
    committed: mi_stat_count_t = .{},
    reset: mi_stat_count_t = .{},
    page_committed: mi_stat_count_t = .{},
    segments_abandoned: mi_stat_count_t = .{},
    pages_abandoned: mi_stat_count_t = .{},
    threads: mi_stat_count_t = .{},
    normal: mi_stat_count_t = .{},
    huge: mi_stat_count_t = .{},
    large: mi_stat_count_t = .{},
    malloc: mi_stat_count_t = .{},
    segments_cache: mi_stat_count_t = .{},
    pages_extended: mi_stat_counter_t = .{},
    mmap_calls: mi_stat_counter_t = .{},
    commit_calls: mi_stat_counter_t = .{},
    page_no_retire: mi_stat_counter_t = .{},
    searches: mi_stat_counter_t = .{},
    normal_count: mi_stat_counter_t = .{},
    huge_count: mi_stat_counter_t = .{},
    large_count: mi_stat_counter_t = .{},
    normal_bins: if (MI_STAT > 1) [74]mi_stat_count_t else u0 = if (MI_STAT > 1) [_]mi_stat_count_t{.{}} ** 74 else 0,
};

// ------------------------------------------------------
// Thread Local data
// ------------------------------------------------------

// A "span" is is an available range of slices. The span queues keep
// track of slice spans of at most the given `slice_count` (but more than the previous size class).
pub const mi_span_queue_t = struct {
    first: ?*mi_slice_t = null,
    last: ?*mi_slice_t = null,
    slice_count: usize,
};

pub const MI_SEGMENT_BIN_MAX = 35; // 35 == segment_bin(SLICES_PER_SEGMENT)

// OS thread local data
pub const mi_os_tld_t = struct {
    region_idx: usize = 0, // start point for next allocation
    stats: ?*mi_stats_t = null, // points to tld stats
};

// Segments thread local data
pub const mi_segments_tld_t = struct {
    spans: [36]mi_span_queue_t, // free slice spans inside segments
    count: usize = 0, // current number of segments;
    peak_count: usize = 0, // peak number of segments
    current_size: usize = 0, // current size of all segments
    peak_size: usize = 0, // peak size of all segments
    stats: ?*mi_stats_t = null, // points to tld stats
    os: ?*mi_os_tld_t = null, // points to os stats
};

pub const mi_tld_t = struct {
    const Self = @This();
    heartbeat: u64 = 0, // monotonic heartbeat count
    recurse: bool = false, // true if deferred was called; used to prevent infinite recursion.
    heap_backing: ?*mi_heap_t = null, // backing heap of this thread (cannot be deleted)
    heaps: ?*mi_heap_t = null, // list of heaps in this thread (so we can abandon all when the thread terminates)
    segments: mi_segments_tld_t, // segment tld
    os: mi_os_tld_t = .{}, // os tld
    stats: mi_stats_t = .{}, // statistics
};

// ------------------------------------------------------
// Options
// ------------------------------------------------------

pub const mi_option_t = enum {
    // stable options
    mi_option_show_errors,
    mi_option_show_stats,
    mi_option_verbose,
    // some of the following options are experimental
    mi_option_eager_commit,
    mi_option_large_os_pages, // use large (2MiB) OS pages, implies eager commit
    mi_option_reserve_huge_os_pages, // reserve N huge OS pages (1GiB) at startup
    mi_option_reserve_huge_os_pages_at, // reserve huge OS pages at a specific NUMA node
    mi_option_reserve_os_memory, // reserve specified amount of OS memory at startup
    mi_option_page_reset,
    mi_option_abandoned_page_decommit,
    mi_option_eager_commit_delay,
    mi_option_decommit_delay,
    mi_option_use_numa_nodes, // 0 = use available numa nodes, otherwise use at most N nodes.
    mi_option_limit_os_alloc, // 1 = do not use OS memory for allocation (but only reserved arenas)
    mi_option_os_tag,
    mi_option_max_errors,
    mi_option_max_warnings,
    mi_option_max_segment_reclaim,
    mi_option_allow_decommit,
    mi_option_segment_decommit_delay,
    mi_option_decommit_extend_delay,
    _mi_option_last,
};

// inline functions from mimalloc-internal.h

inline fn noop(cond: bool) void {
    _ = cond;
}

const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else noop;

pub inline fn _mi_thread_id() u64 {
    return std.Thread.getCurrentId();
}

// "bit scan reverse": Return index of the highest bit (or MI_INTPTR_BITS if `x` is zero)
pub inline fn mi_bsr(x: usize) usize {
    return if (x == 0) mi.MI_INTPTR_BITS else mi.MI_INTPTR_BITS - 1 - @clz(x);
}

pub inline fn _mi_ptr_cookie(p: anytype) usize {
    mi_assert_internal(mi._mi_heap_main.cookie != 0);
    return (@ptrToInt(p) ^ mi._mi_heap_main.cookie);
}

// Align upwards
pub inline fn _mi_align_up(sz: usize, alignment: usize) usize {
    mi_assert_internal(alignment != 0);
    const mask = alignment - 1;
    if ((alignment & mask) == 0) { // power of two?
        return ((sz + mask) & ~mask);
    } else {
        return (((sz + mask) / alignment) * alignment);
    }
}

// Align downwards
pub inline fn _mi_align_down(sz: usize, alignment: usize) usize {
    mi_assert_internal(alignment != 0);
    const mask = alignment - 1;
    if ((alignment & mask) == 0) { // power of two?
        return (sz & ~mask);
    } else {
        return ((sz / alignment) * alignment);
    }
}

fn mi_align_up_ptr(p: [*]u8, alignment: usize) [*]u8 {
    return @intToPtr([*]u8, _mi_align_up(@ptrToInt(p), alignment));
}

// Divide upwards: `s <= _mi_divide_up(s,d)*d < s+d`.
pub inline fn _mi_divide_up(size: usize, divider: usize) usize {
    mi_assert_internal(divider != 0);
    return if (divider == 0) size else ((size + divider - 1) / divider);
}

// atomics

pub inline fn mi_atomic_load_relaxed(a: anytype) @TypeOf(a.value) {
    return a.load(AtomicOrder.Monotonic);
}

pub const mi_atomic_loadi64_relaxed = mi_atomic_load_relaxed;

pub inline fn mi_atomic_load_acquire(a: anytype) @TypeOf(a.value) {
    return a.load(AtomicOrder.Acquire);
}

pub const mi_atomic_loadi64_acquire = mi_atomic_load_acquire;

pub inline fn mi_atomic_store_release(a: anytype, x: @TypeOf(a.value)) void {
    return a.store(x, AtomicOrder.Release);
}

pub const mi_atomic_storei64_release = mi_atomic_store_release;

pub inline fn mi_atomic_store_relaxed(a: anytype, x: @TypeOf(a.value)) void {
    return a.store(x, AtomicOrder.Monotonic);
}

pub const mi_atomic_storei64_relaxed = mi_atomic_store_relaxed;

pub inline fn mi_atomic_increment_relaxed(a: anytype) void {
    _ = a.fetchAdd(1, AtomicOrder.Monotonic);
}

pub inline fn mi_atomic_decrement_relaxed(a: anytype) void {
    _ = a.fetchSub(1, AtomicOrder.Monotonic);
}

pub inline fn mi_atomic_and_acq_rel(a: anytype, x: @TypeOf(a.value)) @TypeOf(a.value) {
    return a.fetchAnd(x, AtomicOrder.AcqRel);
}

pub inline fn mi_atomic_or_acq_rel(a: anytype, x: @TypeOf(a.value)) @TypeOf(a.value) {
    return a.fetchOr(x, AtomicOrder.AcqRel);
}

pub inline fn mi_atomic_add_acq_rel(a: anytype, x: @TypeOf(a.value)) @TypeOf(a.value) {
    return a.fetchAdd(x, AtomicOrder.AcqRel);
}

pub inline fn mi_atomic_add_relaxed(a: anytype, x: @TypeOf(a.value)) void {
    _ = a.fetchAdd(x, AtomicOrder.Monotonic);
}

pub inline fn mi_atomic_sub_relaxed(a: anytype, x: @TypeOf(a.value)) void {
    _ = a.fetchSub(x, AtomicOrder.Monotonic);
}

pub inline fn mi_atomic_cas_weak_release(a: anytype, exp: *@TypeOf(a.value), des: @TypeOf(a.value)) bool {
    return a.tryCompareAndSwap(exp.*, des, AtomicOrder.Release, AtomicOrder.Monotonic) != null;
}

pub inline fn mi_atomic_cas_weak_acq_rel(a: anytype, exp: *@TypeOf(a.value), des: @TypeOf(a.value)) bool {
    return a.tryCompareAndSwap(exp.*, des, AtomicOrder.AcqRel, AtomicOrder.Acquire) != null;
}

pub inline fn mi_atomic_cas_strong_acq_rel(a: anytype, exp: *@TypeOf(a.value), des: @TypeOf(a.value)) bool {
    return a.compareAndSwap(exp.*, des, AtomicOrder.AcqRel, AtomicOrder.Acquire) != null;
}

pub inline fn mi_atomic_load_ptr_relaxed(comptime T: type, a: *Atomic(?*T)) ?*T {
    return a.load(AtomicOrder.Monotonic);
}

pub inline fn mi_atomic_load_ptr_acquire(comptime T: type, a: *Atomic(?*T)) ?*T {
    return a.load(AtomicOrder.Acquire);
}

pub inline fn mi_atomic_store_ptr_release(comptime T: type, a: *Atomic(?*T), val: ?*T) void {
    a.store(val, AtomicOrder.Release);
}

pub inline fn mi_atomic_cas_ptr_weak_acq_rel(comptime T: type, a: *Atomic(?*T), exp: *?*T, des: ?*T) bool {
    return a.tryCompareAndSwap(exp.*, des, AtomicOrder.AcqRel, AtomicOrder.Acquire) != null;
}

pub inline fn mi_atomic_exchange_ptr_acq_rel(comptime T: type, a: *Atomic(?*T), val: ?*T) ?*T {
    return a.swap(val, AtomicOrder.AcqRel);
}

pub inline fn mi_atomic_cas_ptr_strong_acq_rel(comptime T: type, a: *Atomic(?*T), exp: *?*T, des: ?*T) bool {
    return a.compareAndSwap(exp.*, des, AtomicOrder.AcqRel, AtomicOrder.Acquire) != null;
}

pub inline fn mi_atomic_cas_ptr_weak_release(comptime T: type, a: *Atomic(?*T), exp: *?*T, des: ?*T) bool {
    return a.tryCompareAndSwap(exp.*, des, AtomicOrder.Release, AtomicOrder.Monotonic) != null;
}

pub inline fn mi_atomic_yield() void {
    std.Thread.yield() catch return;
}

// VALGRIND tracking - disabled for now
pub fn mi_track_malloc(p: anytype, size: usize, zero: bool) void {
    _ = p;
    _ = size;
    _ = zero;
}

pub fn mi_track_free_size(p: anytype, size: usize) void {
    _ = p;
    _ = size;
}

pub fn mi_track_resize(p: anytype, oldsize: usize, newsize: usize) void {
    _ = p;
    _ = oldsize;
    _ = newsize;
}

pub fn mi_track_mem_free(p: anytype) void {
    _ = p;
}

pub const mi_track_free = mi_track_mem_free;

pub fn mi_track_mem_defined(p: anytype, size: usize) void {
    _ = p;
    _ = size;
}

pub fn mi_track_mem_undefined(p: anytype, size: usize) void {
    _ = p;
    _ = size;
}

pub fn mi_track_mem_noaccess(p: anytype, size: usize) void {
    _ = p;
    _ = size;
}

// arena

pub fn _mi_arena_id_none() mi_arena_id_t {
    return 0;
}

fn mi_arena_id_is_suitable(arena_id: mi_arena_id_t, arena_is_exclusive: bool, req_arena_id: mi_arena_id_t) bool {
    return ((!arena_is_exclusive and req_arena_id == _mi_arena_id_none()) or
        (arena_id == req_arena_id));
}

pub fn _mi_arena_memid_is_suitable(arena_memid: usize, request_arena_id: mi_arena_id_t) bool {
    const id = @intCast(mi_arena_id_t, arena_memid & 0x7F);
    const exclusive = ((arena_memid & 0x80) != 0);
    return mi_arena_id_is_suitable(id, exclusive, request_arena_id);
}

// Simplified version, directly get page OS. Can't use page_allocator because it ignores alignment
pub fn _mi_arena_alloc_aligned(size: usize, alignment: usize, commit: *bool, large: *bool, is_pinned: *bool, is_zero: *bool, req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?*anyopaque {
    _ = large;
    _ = is_pinned;
    _ = is_zero;
    _ = req_arena_id;
    _ = memid;
    _ = tld;

    const hint = mi_os_get_aligned_hint(alignment, size);
    var p = os.mmap(hint, size, os.PROT.WRITE | os.PROT.READ, os.MAP.PRIVATE | os.MAP.ANONYMOUS, -1, alignment) catch return null;

    // if not aligned, free it, overallocate, and unmap around it
    if (@ptrToInt(p.ptr) % alignment != 0) {
        os.munmap(p);
        std.log.warn("unable to allocate aligned OS memory directly, fall back to over-allocation ({} bytes, address: {*}, alignment: {}, commit: {})\n", .{ size, p.ptr, alignment, commit });
        if (size >= (std.math.maxInt(isize) - alignment)) return null; // overflow
        const over_size = size + alignment;
        p = os.mmap(null, over_size, os.PROT.WRITE | os.PROT.READ, os.MAP.PRIVATE | os.MAP.ANONYMOUS, -1, alignment) catch return null;
        // and selectively unmap parts around the over-allocated area. (noop on sbrk)
        const aligned_p = mi_align_up_ptr(p.ptr, alignment);
        const pre_size = @ptrToInt(aligned_p) - @ptrToInt(p.ptr);
        const mid_size = _mi_align_up(size, _mi_os_page_size());
        const post_size = over_size - pre_size - mid_size;
        mi_assert_internal(pre_size < over_size and post_size < over_size and mid_size >= size);
        if (pre_size > 0) os.munmap(p[0..pre_size]);
        if (post_size > 0) os.munmap(@alignCast(mem.page_size, p[(pre_size + mid_size)..post_size]));
        // we can return the aligned pointer on `mmap` (and sbrk) systems
        return aligned_p;
    }
    return p.ptr;
}

// Simplified version, directly release memory to page_allocator
pub fn _mi_arena_free(p: *anyopaque, size: usize, alignment: usize, align_offset: usize, memid: usize, all_committed: bool, tld: *mi_os_tld_t) void {
    _ = memid;
    _ = all_committed;
    _ = tld;
    _ = alignment;
    _ = align_offset;
    return os.munmap(@ptrCast([*]u8, @alignCast(mem.page_size, p))[0..size]);
}

// os

// Signal to the OS that the address range is no longer in use
// but may be used later again. This will release physical memory
// pages and reduce swapping while keeping the memory committed.
// We page align to a conservative area inside the range to reset.
pub fn _mi_os_reset(addr: *anyopaque, size: usize, tld_stats: *mi_stats_t) bool {
    _ = tld_stats;
    // mi_stats_t* stats = &_mi_stats_main;
    // return mi_os_resetx(addr, size, true, stats);
    std.os.madvise(@alignCast(mem.page_size, @ptrCast([*]u8, addr)), size, std.os.MADV.FREE) catch return false;
    return true;
}

pub fn _mi_os_commit(addr: *anyopaque, size: usize, is_zero: *bool, tld_stats: *mi_stats_t) bool {
    // MI_UNUSED(tld_stats);
    // mi_stats_t* stats = &_mi_stats_main;
    // return mi_os_commitx(addr, size, true, false /* liberal */, is_zero, stats);
    _ = tld_stats;
    is_zero.* = false; // TODO: What does is_zero mean?
    std.os.mprotect(@alignCast(mem.page_size, @ptrCast([*]u8, addr))[0..size], os.PROT.READ | os.PROT.WRITE) catch return false;
    return true;
}

pub fn _mi_os_decommit(addr: *anyopaque, size: usize, tld_stats: *mi_stats_t) bool {
    // mi_stats_t* stats = &_mi_stats_main;
    // bool is_zero;
    // return mi_os_commitx(addr, size, false, true /* conservative */, &is_zero, stats);
    return _mi_os_reset(addr, size, tld_stats);
}

// TODO:
pub fn _mi_os_protect(addr: *anyopaque, size: usize) bool {
    // return mi_os_protectx(addr, size, true);
    _ = addr;
    _ = size;
    return true;
}

pub fn _mi_os_unprotect(addr: *anyopaque, size: usize) bool {
    // return mi_os_protectx(addr, size, false);
    _ = addr;
    _ = size;
    return true;
}

pub fn _mi_os_numa_node(tld: ?*mi_os_tld_t) usize {
    _ = tld;
    return 0;
}

pub fn _mi_os_numa_node_count() usize {
    return 1;
}

pub fn _mi_os_page_size() usize {
    return std.mem.page_size;
}

//--------------------------------------------------------------
//  aligned hinting
//--------------------------------------------------------------

// On 64-bit systems, we can do efficient aligned allocation by using
// the 2TiB to 30TiB area to allocate those.

var aligned_base = Atomic(usize).init(0);

// Return a MI_SEGMENT_SIZE aligned address that is probably available.
// If this returns NULL, the OS will determine the address but on some OS's that may not be
// properly aligned which can be more costly as it needs to be adjusted afterwards.
// For a size > 1GiB this always returns NULL in order to guarantee good ASLR randomization;
// (otherwise an initial large allocation of say 2TiB has a 50% chance to include (known) addresses
//  in the middle of the 2TiB - 6TiB address range (see issue #372))

const MI_HINT_BASE = (2 << 40); // 2TiB start
const MI_HINT_AREA = (4 << 40); // upto 6TiB   (since before win8 there is "only" 8TiB available to processes)
const MI_HINT_MAX = (30 << 40); // wrap after 30TiB (area after 32TiB is used for huge OS pages)

fn mi_os_get_aligned_hint(try_alignment: usize, size_in: usize) ?[*]align(mem.page_size) u8 {
    if (MI_INTPTR_SIZE < 8 or try_alignment > MI_SEGMENT_SIZE) return null;
    var size = _mi_align_up(size_in, MI_SEGMENT_SIZE);
    if (size > 1 * MI_GiB) return null; // guarantee the chance of fixed valid address is at most 1/(MI_HINT_AREA / 1<<30) = 1/4096.
    if (MI_SECURE > 0)
        size += MI_SEGMENT_SIZE; // put in `MI_SEGMENT_SIZE` virtual gaps between hinted blocks; this splits VLA's but increases guarded areas.

    var hint = mi_atomic_add_acq_rel(&aligned_base, size);
    if (hint == 0 or hint > MI_HINT_MAX) { // wrap or initialize
        var init: usize = MI_HINT_BASE;
        if (MI_SECURE > 0 or MI_DEBUG == 0) { // security: randomize start of aligned allocations unless in debug mode
            const r = _mi_heap_random_next(mi_get_default_heap());
            init = init + ((MI_SEGMENT_SIZE * ((r >> 17) & 0xFFFFF)) % MI_HINT_AREA); // (randomly 20 bits)*4MiB == 0 to 4TiB
        }
        var expected = hint + size;
        _ = mi_atomic_cas_strong_acq_rel(&aligned_base, &expected, init);
        hint = mi_atomic_add_acq_rel(&aligned_base, size); // this may still give 0 or > MI_HINT_MAX but that is ok, it is a hint after all
    }
    if (hint % try_alignment != 0) return null;
    return @intToPtr([*]align(mem.page_size) u8, hint);
}

// -------------------------------------------------------------------
// Fast "random" shuffle
// -------------------------------------------------------------------

pub fn _mi_random_shuffle(x_in: usize) usize {
    var x = x_in;
    if (x == 0) {
        x = 17;
    } // ensure we don't get stuck in generating zeros
    if (MI_INTPTR_SIZE == 8) {
        // by Sebastiano Vigna, see: <http://xoshiro.di.unimi.it/splitmix64.c>
        x ^= x >> 30;
        x *%= 0xbf58476d1ce4e5b9;
        x ^= x >> 27;
        x *%= 0x94d049bb133111eb;
        x ^= x >> 31;
    } else if (MI_INTPTR_SIZE == 4) {
        // by Chris Wellons, see: <https://nullprogram.com/blog/2018/07/31/>
        x ^= x >> 16;
        x *%= 0x7feb352d;
        x ^= x >> 15;
        x *%= 0x846ca68b;
        x ^= x >> 16;
    }
    return x;
}
