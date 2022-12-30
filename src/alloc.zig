//----------------------------------------------------------------------------
// Copyright (c) 2018-2020, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//----------------------------------------------------------------------------

const std = @import("std");
const assert = std.debug.assert;
const builtin = @import("builtin");
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("heap.zig");
    usingnamespace @import("page.zig");
    usingnamespace @import("page-queue.zig");
    usingnamespace @import("stats.zig");
    usingnamespace @import("segment.zig");
    usingnamespace @import("segment-cache.zig");

    fn noop(cond: bool) void {
        _ = cond;
    }
};

// alises to avoid clutter

// type aliases
const mi_heap_t = mi.mi_heap_t;
const mi_page_t = mi.mi_page_t;
const mi_segment_t = mi.mi_segment_t;
const mi_block_t = mi.mi_block_t;
const mi_delayed_t = mi.mi_delayed_t;
const mi_padding_t = mi.mi_padding_t;
const mi_thread_free_t = mi.mi_thread_free_t;
const mi_encoded_t = mi.mi_encoded_t;

// #defines
const SIZE_MAX = mi.SIZE_MAX;
const MI_DEBUG = mi.MI_DEBUG;
const MI_DEBUG_UNINIT = mi.MI_DEBUG_UNINIT;
const MI_DEBUG_PADDING = mi.MI_DEBUG_PADDING;
const MI_DEBUG_FREED = mi.MI_DEBUG_FREED;

const MI_SECURE = mi.MI_SECURE;
const MI_STAT = mi.MI_STAT;
const MI_PADDING = mi.MI_PADDING;
const MI_PADDING_SIZE = mi.MI_PADDING_SIZE;
const MI_ENCODE_FREELIST = mi.MI_ENCODE_FREELIST;
const MI_TRACK_ENABLED = false;

const MI_HUGE_PAGE_ABANDON = mi.MI_HUGE_PAGE_ABANDON;

const MI_MAX_ALIGN_SIZE = mi.MI_MAX_ALIGN_SIZE;
const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;
const MI_SMALL_OBJ_SIZE_MAX = mi.MI_SMALL_OBJ_SIZE_MAX;
const MI_MEDIUM_OBJ_SIZE_MAX = mi.MI_MEDIUM_OBJ_SIZE_MAX;
const MI_LARGE_OBJ_SIZE_MAX = mi.MI_LARGE_OBJ_SIZE_MAX;
const MI_SMALL_SIZE_MAX = mi.MI_SMALL_SIZE_MAX;

// Function aliases
const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_load_acquire = mi.mi_atomic_load_acquire;
const mi_atomic_cas_weak_release = mi.mi_atomic_cas_weak_release;
const mi_atomic_load_ptr_relaxed = mi.mi_atomic_load_ptr_relaxed;
const mi_atomic_cas_ptr_weak_release = mi.mi_atomic_cas_ptr_weak_release;

const mi_get_default_heap = mi.mi_get_default_heap;
const _mi_bin = mi._mi_bin;
const _mi_thread_id = mi._mi_thread_id;
const _mi_malloc_generic = mi._mi_malloc_generic;

const _mi_heap_get_free_small_page = mi._mi_heap_get_free_small_page;
const mi_heap_get_default = mi.mi_heap_get_default;
const mi_heap_is_initialized = mi.mi_heap_is_initialized;
const mi_is_in_heap_region = mi.mi_is_in_heap_region;

const mi_page_is_huge = mi.mi_page_is_huge;

const mi_block_next = mi.mi_block_next;
const mi_block_nextx = mi.mi_block_nextx;
const mi_block_set_next = mi.mi_block_set_next;
const mi_block_set_nextx = mi.mi_block_set_nextx;

const _mi_is_aligned = mi._mi_is_aligned;

const mi_ptr_encode = mi.mi_ptr_encode;
const _mi_ptr_cookie = mi._mi_ptr_cookie;
const _mi_ptr_page = mi._mi_ptr_page;
const _mi_ptr_segment = mi._mi_ptr_segment;
const _mi_segment_page_of = mi._mi_segment_page_of;
const _mi_segment_page_start = mi._mi_segment_page_start;
const _mi_segment_huge_page_free = mi._mi_segment_huge_page_free;
const _mi_segment_huge_page_reset = mi._mi_segment_huge_page_reset;

const _mi_page_segment = mi._mi_page_segment;
const mi_page_all_free = mi.mi_page_all_free;
const _mi_page_retire = mi._mi_page_retire;
const mi_page_is_in_full = mi.mi_page_is_in_full;
const _mi_page_unfull = mi._mi_page_unfull;
const _mi_page_start = mi._mi_page_start;
const mi_page_thread_free = mi.mi_page_thread_free;
const mi_page_has_aligned = mi.mi_page_has_aligned;
const _mi_page_try_use_delayed_free = mi._mi_page_try_use_delayed_free;
const _mi_page_free_collect = mi._mi_page_free_collect;

const mi_page_block_size = mi.mi_page_block_size;
const mi_page_usable_block_size = mi.mi_page_usable_block_size;

const _mi_stat_increase = mi._mi_stat_increase;
const _mi_stat_decrease = mi._mi_stat_decrease;
const _mi_stat_counter_increase = mi._mi_stat_counter_increase;

const mi_track_mem_defined = mi.mi_track_mem_defined;
const mi_track_mem_undefined = mi.mi_track_mem_undefined;
const mi_track_mem_noaccess = mi.mi_track_mem_noaccess;
const mi_track_malloc = mi.mi_track_malloc;
const mi_track_free_size = mi.mi_track_free_size;
const mi_track_free = mi.mi_track_free;

const _mi_memzero_aligned = mi._mi_memzero_aligned;

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

pub fn mi_is_in_same_page(p: *const mi_block_t, q: *const mi_block_t) bool {
    const segment = _mi_ptr_segment(@ptrCast(*const u8, p));
    if (_mi_ptr_segment(@ptrCast(*const u8, q)) != segment) return false;
    // assume q may be invalid // return (_mi_segment_page_of(segment, p) == _mi_segment_page_of(segment, q));
    const page = _mi_segment_page_of(segment, @ptrCast(*const u8, p));
    var psize: usize = undefined;
    const start = _mi_segment_page_start(segment, page, &psize);
    return (@ptrToInt(start) <= @ptrToInt(q) and @ptrToInt(q) < @ptrToInt(start) + psize);
}

inline fn mi_mul_overflow(count: usize, size: usize, total: *usize) bool {
    return @mulWithOverflow(usize, count, size, total);
}

// Safe multiply `count*size` into `total`; return `true` on overflow.
fn mi_count_size_overflow(count: usize, size: usize, total: *usize) bool {
    if (count == 1) { // quick check for the case where count is one (common for C++ allocators)
        total.* = size;
        return false;
    } else if (mi_unlikely(mi_mul_overflow(count, size, total))) {
        if (MI_DEBUG > 0)
            std.log.err("allocation request is too large ({} * {} bytes)\n", count, size);
        total.* = SIZE_MAX;
        return true;
    } else return false;
}

// Thread free flag helpers
pub fn mi_tf_block(tf: mi_thread_free_t) *mi_block_t {
    return @intToPtr(*mi_block_t, (tf & ~@intCast(usize, 0x03)));
}

pub fn mi_tf_delayed(tf: mi_thread_free_t) mi_delayed_t {
    return @intToEnum(mi_delayed_t, tf & @intCast(usize, 0x03));
}

fn mi_tf_make(block: ?*mi_block_t, delayed: mi_delayed_t) mi_thread_free_t {
    return (@ptrToInt(block) | @enumToInt(delayed));
}

pub fn mi_tf_set_delayed(tf: mi_thread_free_t, delayed: mi_delayed_t) mi_thread_free_t {
    return mi_tf_make(mi_tf_block(tf), delayed);
}

pub fn mi_tf_set_block(tf: mi_thread_free_t, block: ?*mi_block_t) mi_thread_free_t {
    return mi_tf_make(block, mi_tf_delayed(tf));
}

// ------------------------------------------------------
// Allocation
// ------------------------------------------------------

// Fast allocation in a page: just pop from the free list.
// Fall back to generic allocation only if the list is empty.
pub fn _mi_page_malloc(heap: *mi_heap_t, page: *mi_page_t, size: usize, zero: bool) ?*u8 {
    mi_assert_internal(page.xblock_size == 0 or mi_page_block_size(page) >= size);
    var block = page.free;
    if (mi_unlikely(block == null)) {
        return _mi_malloc_generic(heap, size, zero, 0);
    }
    mi_assert_internal(block != null and _mi_ptr_page(@ptrCast(*const u8, block.?)) == page);
    // pop from the free list
    page.used += 1;
    page.free = mi_block_next(page, block.?);
    mi_assert_internal(page.free == null or _mi_ptr_page(@ptrCast(*const u8, page.free)) == page);

    // allow use of the block internally
    // note: when tracking we need to avoid ever touching the MI_PADDING since
    // that is tracked by valgrind etc. as non-accessible (through the red-zone, see `mimalloc-track.h`)
    mi_track_mem_undefined(@ptrCast(?*u8, block), mi_page_usable_block_size(page));

    // zero the block? note: we need to zero the full block size (issue #63)
    if (mi_unlikely(zero)) {
        mi_assert_internal(page.xblock_size != 0); // do not call with zero'ing for huge blocks (see _mi_malloc_generic)
        const zsize = if (page.b.is_zero) @sizeOf(mi_encoded_t) + MI_PADDING_SIZE else page.xblock_size;
        _mi_memzero_aligned(@ptrCast([*]u8, block), zsize - MI_PADDING_SIZE);
    }

    if ((MI_DEBUG > 0) and !MI_TRACK_ENABLED) {
        if (!page.b.is_zero and !zero and !mi_page_is_huge(page)) {
            @memset(@ptrCast([*]u8, block), MI_DEBUG_UNINIT, mi_page_usable_block_size(page));
        } else if (MI_SECURE != 0) {
            if (!zero) {
                block.?.next = 0;
            } // don't leak internal data
        }
    }

    if (MI_STAT > 0) {
        const bsize = mi_page_usable_block_size(page);
        if (bsize <= MI_MEDIUM_OBJ_SIZE_MAX) {
            _mi_stat_increase(&heap.tld.?.stats.normal, bsize);
            _mi_stat_counter_increase(&heap.tld.?.stats.normal_count, 1);
            if (MI_STAT > 1) {
                const bin = _mi_bin(bsize);
                _mi_stat_increase(&heap.tld.?.stats.normal_bins[bin], 1);
            }
        }
    }

    if (MI_PADDING > 0 and MI_ENCODE_FREELIST and !MI_TRACK_ENABLED) {
        const padding = @ptrCast(*mi_padding_t, @alignCast(@alignOf(*mi_padding_t), @ptrCast([*]u8, block) + mi_page_usable_block_size(page)));
        const delta = @ptrToInt(padding) - @ptrToInt(block) - (size - MI_PADDING_SIZE);
        if (MI_DEBUG > 1) {
            mi_assert_internal(delta >= 0 and mi_page_usable_block_size(page) >= (size - MI_PADDING_SIZE + delta));
            mi_track_mem_defined(@ptrCast(*u8, padding), @sizeOf(mi_padding_t)); // note: re-enable since mi_page_usable_block_size may set noaccess
        }
        padding.canary = @truncate(u32, mi_ptr_encode(page, block, &page.keys));
        padding.delta = @intCast(u32, delta);
        if (!mi_page_is_huge(page)) {
            const fill = @ptrCast([*]u8, padding) - delta;
            const maxpad = if (delta > MI_MAX_ALIGN_SIZE) MI_MAX_ALIGN_SIZE else delta; // set at most N initial padding bytes
            var i: usize = 0;
            while (i < maxpad) : (i += 1) {
                fill[i] = MI_DEBUG_PADDING;
            }
        }
    }

    return @ptrCast(*u8, block);
}

fn mi_heap_malloc_small_zero(heap_in: *mi_heap_t, size_in: usize, zero: bool) ?*u8 {
    var size = size_in;
    var heap = heap_in;
    if (MI_DEBUG > 0) {
        const tid = _mi_thread_id();
        mi_assert(heap.thread_id == 0 or heap.thread_id == tid); // heaps are thread local
    }
    mi_assert(size <= MI_SMALL_SIZE_MAX);
    if (MI_PADDING > 0 and size == 0) {
        size = @sizeOf(*u8);
    }
    const page = _mi_heap_get_free_small_page(heap, size + MI_PADDING_SIZE);
    const p = _mi_page_malloc(heap, page, size + MI_PADDING_SIZE, zero);
    mi_assert_internal(p == null or mi_usable_size(p.?) >= size);
    if (MI_STAT > 1 and p != null) {
        if (!mi_heap_is_initialized(heap)) {
            heap = mi_get_default_heap();
        }
        _mi_stat_increase(&heap.tld.?.stats.malloc, mi_usable_size(p.?));
    }
    mi_track_malloc(p, size, zero);
    return p;
}

// allocate a small block
pub fn mi_heap_malloc_small(heap: *mi_heap_t, size: usize) ?*u8 {
    return mi_heap_malloc_small_zero(heap, size, false);
}

pub fn mi_malloc_small(size: usize) ?*u8 {
    return mi_heap_malloc_small(mi_get_default_heap(), size);
}

// The main allocation function
pub fn _mi_heap_malloc_zero_ex(heap_in: *mi_heap_t, size: usize, zero: bool, huge_alignment: usize) ?*u8 {
    var heap = heap_in;
    if (mi_likely(size <= MI_SMALL_SIZE_MAX)) {
        mi_assert_internal(huge_alignment == 0);
        return mi_heap_malloc_small_zero(heap, size, zero);
    } else {
        mi_assert(heap.thread_id == 0 or heap.thread_id == _mi_thread_id()); // heaps are thread local
        const p = _mi_malloc_generic(heap, size + MI_PADDING_SIZE, zero, huge_alignment); // note: size can overflow but it is detected in malloc_generic
        mi_assert_internal(p == null or mi_usable_size(p.?) >= size);
        if (MI_STAT > 1) {
            if (p != null) {
                if (!mi_heap_is_initialized(heap)) {
                    heap = mi_get_default_heap();
                }
                _mi_stat_increase(&heap.tld.?.stats.malloc, mi_usable_size(p.?));
            }
        }
        mi_track_malloc(p, size, zero);
        return p;
    }
}

pub fn _mi_heap_malloc_zero(heap: *mi_heap_t, size: usize, zero: bool) ?*u8 {
    return _mi_heap_malloc_zero_ex(heap, size, zero, 0);
}

pub fn mi_heap_malloc(heap: *mi_heap_t, size: usize) ?*u8 {
    return _mi_heap_malloc_zero(heap, size, false);
}

pub fn mi_malloc(size: usize) ?*u8 {
    return mi_heap_malloc(mi_get_default_heap(), size);
}

// zero initialized small block
pub fn mi_zalloc_small(size: usize) ?*u8 {
    return mi_heap_malloc_small_zero(mi_get_default_heap(), size, true);
}

pub fn mi_heap_zalloc(heap: *mi_heap_t, size: usize) ?*u8 {
    return _mi_heap_malloc_zero(heap, size, true);
}

pub fn mi_zalloc(size: usize) ?*u8 {
    return mi_heap_zalloc(mi_get_default_heap(), size);
}

// ------------------------------------------------------
// Check for double free in secure and debug mode
// This is somewhat expensive so only enabled for secure mode 4
// ------------------------------------------------------

// linear check if the free list contains a specific element
fn mi_list_contains(page: *const mi_page_t, list_in: ?*const mi_block_t, elem: *const mi_block_t) bool {
    var list = list_in;
    while (list != null) {
        if (elem == list.?) return true;
        list = mi_block_next(page, list.?);
    }
    return false;
}

fn mi_check_is_double_freex(page: *const mi_page_t, block: *const mi_block_t) bool {
    // The decoded value is in the same page (or null).
    // Walk the free lists to verify positively if it is already freed
    if (mi_list_contains(page, page.free, block) or
        mi_list_contains(page, page.local_free, block) or
        mi_list_contains(page, mi_page_thread_free(page), block))
    {
        std.log.warn("double free detected of block {} with size {}\n", .{ block, mi_page_block_size(page) });
        return true;
    }
    return false;
}

fn mi_check_is_double_free(page: *const mi_page_t, block: *const mi_block_t) bool {
    if (!MI_ENCODE_FREELIST or (MI_SECURE < 4 and MI_DEBUG == 0)) return false;
    var is_double_free = false;
    const n = mi.mi_ptr_decode_raw(page, block.next, &page.keys); // pretend it is freed, and get the decoded first field
    if ((n & (MI_INTPTR_SIZE - 1)) == 0 and // quick check: aligned pointer?
        (n == 0 or mi_is_in_same_page(block, @intToPtr(*mi_block_t, n))))
    {
        // Suspicous: decoded value a in block is in the same page (or null) -- maybe a double free?
        // (continue in separate function to improve code generation)
        is_double_free = mi_check_is_double_freex(page, block);
    }
    return is_double_free;
}

// ---------------------------------------------------------------------------
// Check for heap block overflow by setting up padding at the end of the block
// ---------------------------------------------------------------------------

fn mi_page_decode_padding(page: *const mi_page_t, block: *const mi_block_t, delta: *usize, bsize: *usize) bool {
    bsize.* = mi_page_usable_block_size(page);
    const padding = @intToPtr(*mi_padding_t, @ptrToInt(block) + bsize.*);
    mi_track_mem_defined(padding, @sizeOf(mi_padding_t));
    delta.* = padding.delta;
    const canary = padding.canary;
    const ok = (@truncate(u32, mi_ptr_encode(page, block, &page.keys)) == canary and delta.* <= bsize.*);
    mi_track_mem_noaccess(padding, @sizeOf(mi_padding_t));
    return ok;
}

// Return the exact usable size of a block.
fn mi_page_usable_size_of(page: *const mi_page_t, block: *const mi_block_t) usize {
    if (MI_PADDING == 0) {
        return mi_page_usable_block_size(page);
    }
    var bsize: usize = undefined;
    var delta: usize = undefined;
    const ok = mi_page_decode_padding(page, block, &delta, &bsize);
    mi_assert_internal(ok);
    mi_assert_internal(delta <= bsize);
    return if (ok) bsize - delta else 0;
}

fn mi_verify_padding(page: *const mi_page_t, block: *const mi_block_t, size: *usize, wrong: *usize) bool {
    var bsize: usize = undefined;
    var delta: usize = undefined;
    var ok = mi_page_decode_padding(page, block, &delta, &bsize);
    size.* = bsize;
    wrong.* = bsize;
    if (!ok) return false;
    mi_assert_internal(bsize >= delta);
    size.* = bsize - delta;
    if (!mi_page_is_huge(page)) {
        const fill = @intToPtr([*]u8, @ptrToInt(block)) + bsize - delta; // TODO: ugly const cast
        const maxpad = if (delta > MI_MAX_ALIGN_SIZE) MI_MAX_ALIGN_SIZE else delta; // check at most the first N padding bytes
        mi_track_mem_defined(fill, maxpad);
        var i: usize = 0;
        while (i < maxpad) : (i += 1) {
            if (fill[i] != MI_DEBUG_PADDING) {
                wrong.* = bsize - delta + i;
                ok = false;
                break;
            }
        }
        mi_track_mem_noaccess(fill, maxpad);
    }
    return ok;
}

fn mi_check_padding(page: *const mi_page_t, block: *const mi_block_t) void {
    if (MI_PADDING == 0 or !MI_ENCODE_FREELIST or MI_TRACK_ENABLED) return;
    var size: usize = undefined;
    var wrong: usize = undefined;
    if (!mi_verify_padding(page, block, &size, &wrong)) {
        std.log.err("buffer overflow in heap block {} of size {}: write after {} bytes\n", .{ block, size, wrong });
    }
}

// When a non-thread-local block is freed, it becomes part of the thread delayed free
// list that is freed later by the owning heap. If the exact usable size is too small to
// contain the pointer for the delayed list, then shrink the padding (by decreasing delta)
// so it will later not trigger an overflow error in `mi_free_block`.
fn mi_padding_shrink(page: *const mi_page_t, block: *const mi_block_t, min_size: usize) void {
    if (MI_PADDING == 0 or !MI_ENCODE_FREELIST or MI_TRACK_ENABLED) return;
    var bsize: usize = undefined;
    var delta: usize = undefined;
    const ok = mi_page_decode_padding(page, block, &delta, &bsize);
    mi_assert_internal(ok);
    if (!ok or (bsize - delta) >= min_size) return; // usually already enough space
    mi_assert_internal(bsize >= min_size);
    if (bsize < min_size) return; // should never happen
    const new_delta = (bsize - min_size);
    mi_assert_internal(new_delta < bsize);
    const padding = @intToPtr(*mi_padding_t, @ptrToInt(block) + bsize); // TODO: casting away const
    padding.delta = @intCast(u32, new_delta);
}

// only maintain stats for smaller objects if requested
fn mi_stat_free(page: *const mi_page_t, block: *const mi_block_t) void {
    if (MI_STAT == 0) return;
    const heap = mi_heap_get_default();
    const bsize = mi_page_usable_block_size(page);
    if (MI_STAT > 1) {
        const usable_size = mi_page_usable_size_of(page, block);
        _mi_stat_decrease(&heap.tld.?.stats.malloc, usable_size);
    }
    if (bsize <= MI_MEDIUM_OBJ_SIZE_MAX) {
        _mi_stat_decrease(&heap.tld.?.stats.normal, bsize);
        if (MI_STAT > 1)
            _mi_stat_decrease(&heap.tld.?.stats.normal_bins[_mi_bin(bsize)], 1);
    } else if (bsize <= MI_LARGE_OBJ_SIZE_MAX) {
        _mi_stat_decrease(&heap.tld.?.stats.large, bsize);
    } else {
        _mi_stat_decrease(&heap.tld.?.stats.huge, bsize);
    }
}

// maintain stats for huge objects
fn mi_stat_huge_free(page: *const mi_page_t) void {
    if (!MI_HUGE_PAGE_ABANDON or MI_STAT == 0) return;
    const heap = mi_heap_get_default();
    const bsize = mi_page_block_size(page); // to match stats in `page.c:mi_page_huge_alloc`
    if (bsize <= MI_LARGE_OBJ_SIZE_MAX) {
        _mi_stat_decrease(&heap.tld.?.stats.large, bsize);
    } else {
        _mi_stat_decrease(&heap.tld.?.stats.huge, bsize);
    }
}

// ------------------------------------------------------
// Free
// ------------------------------------------------------

// multi-threaded free (or free in huge block if compiled with MI_HUGE_PAGE_ABANDON)
fn _mi_free_block_mt(page: *mi_page_t, block: *mi_block_t) void {
    // The padding check may access the non-thread-owned page for the key values.
    // that is safe as these are constant and the page won't be freed (as the block is not freed yet).
    mi_check_padding(page, block);
    mi_padding_shrink(page, block, @sizeOf(mi_block_t)); // for small size, ensure we can fit the delayed thread pointers without triggering overflow detection

    // huge page segments are always abandoned and can be freed immediately
    const segment = _mi_page_segment(page);
    if (segment.kind == .MI_SEGMENT_HUGE) {
        if (MI_HUGE_PAGE_ABANDON) {
            mi_stat_huge_free(page);
            // huge page segments are always abandoned and can be freed immediately
            _mi_segment_huge_page_free(segment, page, block);
            return;
        } else {
            // huge pages are special as they occupy the entire segment
            // as these are large we reset the memory occupied by the page so it is available to other threads
            // (as the owning thread needs to actually free the memory later).
            _mi_segment_huge_page_reset(segment, page, block);
        }
    }

    if (MI_DEBUG != 0 and !MI_TRACK_ENABLED and segment.kind != .MI_SEGMENT_HUGE) { // note: when tracking, cannot use mi_usable_size with multi-threading
        @memset(@ptrCast([*]u8, block), MI_DEBUG_FREED, mi_usable_size(block));
    }

    // Try to put the block on either the page-local thread free list, or the heap delayed free list.
    var tfreex: mi_thread_free_t = undefined;
    var use_delayed: bool = undefined;
    var tfree = mi_atomic_load_relaxed(&page.xthread_free);
    while (true) {
        use_delayed = (mi_tf_delayed(tfree) == .MI_USE_DELAYED_FREE);
        if (mi_unlikely(use_delayed)) {
            // unlikely: this only happens on the first concurrent free in a page that is in the full list
            tfreex = mi_tf_set_delayed(tfree, .MI_DELAYED_FREEING);
        } else {
            // usual: directly add to page thread_free list
            mi_block_set_next(page, block, mi_tf_block(tfree));
            tfreex = mi_tf_set_block(tfree, block);
        }
        if (mi_atomic_cas_weak_release(&page.xthread_free, &tfree, tfreex)) break;
    }

    if (mi_unlikely(use_delayed)) {
        // racy read on `heap`, but ok because MI_DELAYED_FREEING is set (see `mi_heap_delete` and `mi_heap_collect_abandon`)
        const heap = mi_atomic_load_acquire(&page.xheap); //mi_page_heap(page);
        mi_assert_internal(heap != null);
        if (heap != null) {
            // add to the delayed free list of this heap. (do this atomically as the lock only protects heap memory validity)
            var dfree = mi_atomic_load_ptr_relaxed(mi_block_t, &heap.?.thread_delayed_free);
            while (true) {
                mi_block_set_nextx(heap, block, dfree, &heap.?.keys);
                if (mi_atomic_cas_ptr_weak_release(mi_block_t, &heap.?.thread_delayed_free, &dfree, block)) break;
            }
        }

        // and reset the MI_DELAYED_FREEING flag
        tfree = mi_atomic_load_relaxed(&page.xthread_free);
        while (true) {
            tfreex = tfree;
            mi_assert_internal(mi_tf_delayed(tfree) == .MI_DELAYED_FREEING);
            tfreex = mi_tf_set_delayed(tfree, .MI_NO_DELAYED_FREE);
            if (mi_atomic_cas_weak_release(&page.xthread_free, &tfree, tfreex)) break;
        }
    }
}

// regular free
fn _mi_free_block(page: *mi_page_t, local: bool, block: *mi_block_t) void {
    // and push it on the free list
    //const size_t bsize = mi_page_block_size(page);
    if (mi_likely(local)) {
        // owning thread can free a block directly
        if (mi_unlikely(mi_check_is_double_free(page, block))) return;
        mi_check_padding(page, block);
        if (MI_DEBUG != 0 and !MI_TRACK_ENABLED and !mi_page_is_huge(page)) { // huge page content may be already decommitted
            @memset(@ptrCast([*]u8, block), MI_DEBUG_FREED, mi_page_block_size(page));
        }
        mi_block_set_next(page, block, page.local_free);
        page.local_free = block;
        page.used -= 1;
        if (mi_unlikely(mi_page_all_free(page))) {
            _mi_page_retire(page);
        } else if (mi_unlikely(mi_page_is_in_full(page))) {
            _mi_page_unfull(page);
        }
    } else {
        _mi_free_block_mt(page, block);
    }
}

// Adjust a block that was allocated aligned, to the actual start of the block in the page.
pub fn _mi_page_ptr_unalign(segment: *const mi_segment_t, page: *const mi_page_t, p: anytype) *mi_block_t {
    const diff = @ptrToInt(p) - @ptrToInt(_mi_page_start(segment, page, null));
    const adjust = (diff % mi_page_block_size(page));
    return @intToPtr(*mi_block_t, @ptrToInt(p) - adjust);
}

pub fn _mi_free_generic(segment: *const mi_segment_t, page: *mi_page_t, is_local: bool, p: *u8) void {
    const block = if (mi_page_has_aligned(page)) _mi_page_ptr_unalign(segment, page, p) else @ptrCast(*mi_block_t, @alignCast(@alignOf(mi_block_t), p));
    mi_stat_free(page, block); // stat_free may access the padding
    mi_track_free(p);
    _mi_free_block(page, is_local, block);
}

// Get the segment data belonging to a pointer
// This is just a single `and` in assembly but does further checks in debug mode
// (and secure mode) if this was a valid pointer.
fn mi_checked_ptr_segment(p: *anyopaque, msg: []const u8) ?*mi_segment_t {
    if (MI_DEBUG > 0 and mi_unlikely(@ptrToInt(p) & (MI_INTPTR_SIZE - 1) != 0)) {
        std.log.err("{s}: invalid (unaligned) pointer: {*}", .{ msg, p });
        return null;
    }

    const segment = _mi_ptr_segment(p);

    if (MI_DEBUG > 0) {
        if (mi_unlikely(!mi_is_in_heap_region(p))) {
            if (MI_INTPTR_SIZE != 8 or builtin.os.tag != .linux or @ptrToInt(p) >> 40 != 0x7F) { // linux tends to align large blocks above 0x7F000000000 (issue #640)
                std.log.warn("{s}: pointer might not point to a valid heap region: {*} (this may still be a valid very large allocation (over 64MiB))", .{ msg, p });
                if (mi_likely(_mi_ptr_cookie(segment) == segment.cookie)) {
                    std.log.warn("(yes, the previous pointer {*} was valid after all)", .{p});
                }
            }
        }
    }
    if (MI_DEBUG > 0 or MI_SECURE >= 4) {
        if (mi_unlikely(_mi_ptr_cookie(segment) != segment.cookie)) {
            std.log.warn("{s}: pointer does not point to a valid heap space: {*}", .{ msg, p });
            return null;
        }
    }
    return segment;
}

// Free a block
// fast path written carefully to prevent spilling on the stack
pub fn mi_free(p: *u8) void {
    const segment = mi_checked_ptr_segment(p, "mi_free") orelse return;
    const is_local = (_mi_thread_id() == mi_atomic_load_relaxed(&segment.thread_id));
    const page = _mi_segment_page_of(segment, p);

    if (mi_likely(is_local)) { // thread-local free?
        if (mi_likely(page.flags.full_aligned == 0)) { // and it is not a full page (full pages need to move from the full bin), nor has aligned blocks (aligned blocks need to be unaligned)
            const block = @ptrCast(*mi_block_t, @alignCast(@alignOf(mi_block_t), p));
            if (mi_unlikely(mi_check_is_double_free(page, block))) return;
            mi_check_padding(page, block);
            mi_stat_free(page, block);
            if (MI_DEBUG != 0 and !MI_TRACK_ENABLED)
                @memset(@ptrCast([*]u8, block), MI_DEBUG_FREED, mi_page_block_size(page));
            mi_track_free(p);
            mi_block_set_next(page, block, page.local_free);
            page.local_free = block;
            page.used -= 1;
            if (mi_unlikely(page.used == 0)) { // using this expression generates better code than: page.used--; if (mi_page_all_free(page))
                _mi_page_retire(page);
            }
        } else {
            // page is full or contains (inner) aligned blocks; use generic path
            _mi_free_generic(segment, page, true, p);
        }
    } else {
        // not thread-local; use generic path
        _mi_free_generic(segment, page, false, p);
    }
}

// return true if successful
pub fn _mi_free_delayed_block(block: *mi_block_t) bool {
    // get segment and page
    const segment = _mi_ptr_segment(block);
    mi_assert_internal(_mi_ptr_cookie(segment) == segment.cookie);
    mi_assert_internal(_mi_thread_id() == segment.thread_id.load(AtomicOrder.Monotonic));
    const page = _mi_segment_page_of(segment, block);

    // Clear the no-delayed flag so delayed freeing is used again for this page.
    // This must be done before collecting the free lists on this page -- otherwise
    // some blocks may end up in the page `thread_free` list with no blocks in the
    // heap `thread_delayed_free` list which may cause the page to be never freed!
    // (it would only be freed if we happen to scan it in `mi_page_queue_find_free_ex`)
    if (!_mi_page_try_use_delayed_free(page, mi_delayed_t.MI_USE_DELAYED_FREE, false // dont overwrite never delayed
    )) {
        return false;
    }

    // collect all other non-local frees to ensure up-to-date `used` count
    _mi_page_free_collect(page, false);

    // and free the block (possibly freeing the page as well since used is updated)
    _mi_free_block(page, true, block);
    return true;
}

// Bytes available in a block
fn mi_page_usable_aligned_size_of(segment: *mi_segment_t, page: *const mi_page_t, p: anytype) usize {
    const block = _mi_page_ptr_unalign(segment, page, p);
    const size = mi_page_usable_size_of(page, block);
    const adjust = @ptrToInt(p) - @ptrToInt(block);
    mi_assert_internal(adjust >= 0 and adjust <= size);
    return (size - adjust);
}

fn _mi_usable_size(p: *anyopaque, msg: []const u8) usize {
    const segment = mi_checked_ptr_segment(p, msg);
    const page = _mi_segment_page_of(segment.?, p);
    if (mi_likely(!mi_page_has_aligned(page))) {
        // TODO: Try avoiding alignCast
        const block = @ptrCast(*const mi_block_t, @alignCast(@alignOf(*const mi_block_t), p));
        return mi_page_usable_size_of(page, block);
    } else {
        // split out to separate routine for improved code generation
        return mi_page_usable_aligned_size_of(segment.?, page, p);
    }
}

pub fn mi_usable_size(p: anytype) usize {
    return _mi_usable_size(p, "mi_usable_size");
}

// ------------------------------------------------------
// ensure explicit external inline definitions are emitted!
// ------------------------------------------------------

// TODO: Do we need to do anything in zig, the functions are already pub?

// ------------------------------------------------------
// Allocation extensions
// ------------------------------------------------------

pub fn mi_free_size(p: *u8, size: usize) void {
    mi_assert(p == null or size <= _mi_usable_size(p, "mi_free_size"));
    mi_free(p);
}

pub fn mi_free_size_aligned(p: *u8, size: usize, alignment: usize) void {
    mi_assert((@ptrToInt(p) % alignment) == 0);
    mi_free_size(p, size);
}

pub fn mi_free_aligned(p: *u8, alignment: usize) void {
    mi_assert((@ptrToInt(p) % alignment) == 0);
    mi_free(p);
}

pub fn mi_heap_calloc(heap: *mi_heap_t, count: usize, size: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(count, size, &total)) return null;
    return mi_heap_zalloc(heap, total);
}

pub fn mi_calloc(count: usize, size: usize) ?*u8 {
    return mi_heap_calloc(mi_get_default_heap(), count, size);
}

// Uninitialized `calloc`
pub fn mi_heap_mallocn(heap: *mi_heap_t, count: usize, size: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(count, size, &total)) return null;
    return mi_heap_malloc(heap, total);
}

pub fn mi_mallocn(count: usize, size: usize) ?*u8 {
    return mi_heap_mallocn(mi_get_default_heap(), count, size);
}

// Expand (or shrink) in place (or fail)
pub fn mi_expand(p: *u8, newsize: usize) ?*u8 {
    if (MI_PADDING) {
        // we do not shrink/expand with padding enabled
        return null;
    }
    const size = _mi_usable_size(p, "mi_expand");
    if (newsize > size) return null;
    return p; // it fits
}

pub fn _mi_heap_realloc_zero(heap: *mi_heap_t, p: *u8, newsize: usize, zero: bool) ?*u8 {
    // if p == null then behave as malloc.
    // else if size == 0 then reallocate to a zero-sized block (and don't return null, just as mi_malloc(0)).
    // (this means that returning null always indicates an error, and `p` will not have been freed in that case.)
    const size = _mi_usable_size(p, "mi_realloc"); // also works if p == null (with size 0)
    if (mi_unlikely(newsize <= size and newsize >= (size / 2) and newsize > 0)) { // note: newsize must be > 0 or otherwise we return null for realloc(null,0)
        // todo: adjust potential padding to reflect the new size?
        mi_track_free_size(p, size);
        mi_track_malloc(p, newsize, true);
        return p; // reallocation still fits and not more than 50% waste
    }
    const newp = mi_heap_malloc(heap, newsize);
    if (mi_likely(newp != null)) {
        if (zero and newsize > size) {
            // also set last word in the previous allocation to zero to ensure any padding is zero-initialized
            const start = if (size >= @sizeOf(usize)) size - @sizeOf(usize) else 0;
            @memset(@ptrCast(*u8, newp) + start, 0, newsize - start);
        }
        if (mi_likely(p != null)) {
            if (mi_likely(_mi_is_aligned(p, @sizeOf(usize)))) { // a client may pass in an arbitrary pointer `p`..
                const copysize = if (newsize > size) size else newsize;
                mi_track_mem_defined(p, copysize); // _mi_useable_size may be too large for byte precise memory tracking..
                std.mem.copy(newp, p, copysize);
            }
            mi_free(p); // only free the original pointer if successful
        }
    }
    return newp;
}

pub fn mi_heap_realloc(heap: *mi_heap_t, p: *u8, newsize: usize) ?*u8 {
    return _mi_heap_realloc_zero(heap, p, newsize, false);
}

pub fn mi_heap_reallocn(heap: *mi_heap_t, p: *u8, count: usize, size: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(count, size, &total)) return null;
    return mi_heap_realloc(heap, p, total);
}

// Reallocate but free `p` on errors
pub fn mi_heap_reallocf(heap: *mi_heap_t, p: *u8, newsize: usize) ?*u8 {
    const newp = mi_heap_realloc(heap, p, newsize);
    if (newp == null and p != null) mi_free(p);
    return newp;
}

pub fn mi_heap_rezalloc(heap: *mi_heap_t, p: *u8, newsize: usize) ?*u8 {
    return _mi_heap_realloc_zero(heap, p, newsize, true);
}

pub fn mi_heap_recalloc(heap: *mi_heap_t, p: *u8, count: usize, size: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(count, size, &total)) return null;
    return mi_heap_rezalloc(heap, p, total);
}

pub fn mi_realloc(p: *u8, newsize: usize) ?*u8 {
    return mi_heap_realloc(mi_get_default_heap(), p, newsize);
}

pub fn mi_reallocn(p: *u8, count: usize, size: usize) ?*u8 {
    return mi_heap_reallocn(mi_get_default_heap(), p, count, size);
}

// Reallocate but free `p` on errors
pub fn mi_reallocf(p: *u8, newsize: usize) ?*u8 {
    return mi_heap_reallocf(mi_get_default_heap(), p, newsize);
}

pub fn mi_rezalloc(p: *u8, newsize: usize) ?*u8 {
    return mi_heap_rezalloc(mi_get_default_heap(), p, newsize);
}

pub fn mi_recalloc(p: *u8, count: usize, size: usize) ?*u8 {
    return mi_heap_recalloc(mi_get_default_heap(), p, count, size);
}

// The following are not relevant for zig and not ported.

// ------------------------------------------------------
// strdup, strndup, and realpath
// ------------------------------------------------------

//-------------------------------------------------------
// C++ new and new_aligned
// The standard requires calling into `get_new_handler` and
// throwing the bad_alloc exception on failure. If we compile
// with a C++ compiler we can implement this precisely. If we
// use a C compiler we cannot throw a `bad_alloc` exception
// but we call `exit` instead (i.e. not returning).
//-------------------------------------------------------*/

