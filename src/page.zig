//----------------------------------------------------------------------------
// Copyright (c) 2018-2020, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//----------------------------------------------------------------------------

// -----------------------------------------------------------
//  The core of the allocator. Every segment contains
//  pages of a certain block size. The main function
//  exported is `mi_malloc_generic`.
//------------------------------------------------------------

const std = @import("std");
const math = std.math;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("heap.zig");
    usingnamespace @import("alloc.zig");
    usingnamespace @import("page-queue.zig");
    usingnamespace @import("segment.zig");
    usingnamespace @import("stats.zig");

    fn noop(cond: bool) void {
        _ = cond;
    }
};

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

// alises to avoid clutter

// type aliases
const mi_heap_t = mi.mi_heap_t;
const mi_tld_t = mi.mi_tld_t;
const mi_page_t = mi.mi_page_t;
const mi_page_queue_t = mi.mi_page_queue_t;
const mi_block_t = mi.mi_block_t;
const mi_delayed_t = mi.mi_delayed_t;
const mi_thread_free_t = mi.mi_thread_free_t;
const mi_segment_t = mi.mi_segment_t;
const mi_stats_t = mi.mi_stats_t;
const mi_encoded_t = mi.mi_encoded_t;

// #defines
const MI_DEBUG = mi.MI_DEBUG;
const MI_SECURE = mi.MI_SECURE;

const MI_HUGE_PAGE_ABANDON = mi.MI_HUGE_PAGE_ABANDON;

const MI_KiB = mi.MI_KiB;
const MI_MiB = mi.MI_MiB;

const PTRDIFF_MAX = std.math.maxInt(isize);

const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;
const MI_INTPTR_BITS = mi.MI_INTPTR_BITS;

const MI_BIN_FULL = mi.MI_BIN_FULL;
const MI_BIN_HUGE = mi.MI_BIN_HUGE;
const MI_PADDING_SIZE = mi.MI_PADDING_SIZE;
const MI_MEDIUM_OBJ_SIZE_MAX = mi.MI_MEDIUM_OBJ_SIZE_MAX;
const MI_SMALL_OBJ_SIZE_MAX = mi.MI_SMALL_OBJ_SIZE_MAX;
const MI_LARGE_OBJ_SIZE_MAX = mi.MI_LARGE_OBJ_SIZE_MAX;
const MI_HUGE_BLOCK_SIZE = mi.MI_HUGE_BLOCK_SIZE;
const MI_SEGMENT_SLICE_SIZE = mi.MI_SEGMENT_SLICE_SIZE;

const MI_ENCODE_FREELIST = mi.MI_ENCODE_FREELIST;

// Function aliases

const mi_thread_init = mi.mi_thread_init;
const _mi_ptr_page = mi._mi_ptr_page;
const mi_atomic_yield = mi.mi_atomic_yield;
const mi_mem_is_zero = mi.mi_mem_is_zero;

const _mi_align_up = mi._mi_align_up;

const mi_atomic_cas_weak_release = mi.mi_atomic_cas_weak_release;
const mi_atomic_cas_weak_acq_rel = mi.mi_atomic_cas_weak_acq_rel;
const mi_atomic_load_ptr_relaxed = mi.mi_atomic_load_ptr_relaxed;
const mi_atomic_store_release = mi.mi_atomic_store_release;
const mi_atomic_cas_ptr_weak_release = mi.mi_atomic_cas_ptr_weak_release;
const mi_atomic_cas_ptr_weak_acq_rel = mi.mi_atomic_cas_ptr_weak_acq_rel;
const mi_atomic_load_acquire = mi.mi_atomic_load_acquire;
const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_store_ptr_release = mi.mi_atomic_store_ptr_release;

const mi_bin = mi.mi_bin;

const _mi_free_delayed_block = mi._mi_free_delayed_block;

const mi_heap_is_initialized = mi.mi_heap_is_initialized;
const mi_heap_page_queue_of = mi.mi_heap_page_queue_of;
const mi_heap_contains_queue = mi.mi_heap_contains_queue;
const _mi_heap_random_next = mi._mi_heap_random_next;
const mi_get_default_heap = mi.mi_get_default_heap;
const mi_heap_collect = mi.mi_heap_collect;

const mi_page_queue_is_huge = mi.mi_page_queue_is_huge;
const mi_page_queue_is_special = mi.mi_page_queue_is_special;

const _mi_random_shuffle = mi._mi_random_shuffle;

const mi_page_queue_of = mi.mi_page_queue_of;
const mi_page_thread_free = mi.mi_page_thread_free;
const mi_page_thread_free_flag = mi.mi_page_thread_free_flag;
const mi_page_block_size = mi.mi_page_block_size;
const mi_is_in_same_page = mi.mi_is_in_same_page;
const mi_page_heap = mi.mi_page_heap;
const _mi_ptr_segment = mi._mi_ptr_segment;
const _mi_page_start = mi._mi_page_start;
const _mi_page_malloc = mi._mi_page_malloc;

const mi_page_queue = mi.mi_page_queue;
const mi_page_queue_push = mi.mi_page_queue_push;
const mi_page_queue_remove = mi.mi_page_queue_remove;
const mi_page_queue_contains = mi.mi_page_queue_contains;
const mi_page_queue_enqueue_from = mi.mi_page_queue_enqueue_from;

const _mi_segment_page_start = mi._mi_segment_page_start;
const _mi_segment_page_alloc = mi._mi_segment_page_alloc;
const _mi_segment_page_free = mi._mi_segment_page_free;
const _mi_segment_page_abandon = mi._mi_segment_page_abandon;

const mi_tf_block = mi.mi_tf_block;
const mi_tf_delayed = mi.mi_tf_delayed;
const mi_tf_set_delayed = mi.mi_tf_set_delayed;
const mi_tf_set_block = mi.mi_tf_set_block;
const mi_tf_set_next = mi.mi_tf_set_next;

const mi_stat_counter_increase = mi._mi_stat_counter_increase;
const mi_stat_increase = mi._mi_stat_increase;

const mi_track_mem_noaccess = mi.mi_track_mem_noaccess;
const mi_track_mem_defined = mi.mi_track_mem_defined;
const mi_track_mem_undefined = mi.mi_track_mem_undefined;

const EFAULT = 0;
const ENOMEM = 0;
const EOVERFLOW = 0;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

// malloc-internal.h inlines

pub fn mi_page_has_aligned(page: *const mi_page_t) bool {
    return page.flags.x.has_aligned == 1;
}

pub fn mi_page_is_in_full(page: *const mi_page_t) bool {
    return page.flags.x.in_full == 1;
}

pub fn mi_page_set_in_full(page: *mi_page_t, in_full: bool) void {
    page.flags.x.in_full = if (in_full) 1 else 0;
}

pub fn mi_page_set_has_aligned(page: *mi_page_t, has_aligned: bool) void {
    page.flags.x.has_aligned = if (has_aligned) 1 else 0;
}

// are there immediately available blocks, i.e. blocks available on the free list.
pub fn mi_page_immediate_available(page: *const mi_page_t) bool {
    return (page.free != null);
}

fn mi_rotl(x: usize, shift: usize) usize {
    const shift_bits = shift % MI_INTPTR_BITS;
    if (shift_bits == 0) return x;
    return math.shl(usize, x, shift_bits) | math.shr(usize, x, MI_INTPTR_BITS - shift_bits);
}

fn mi_rotr(x: usize, shift: usize) usize {
    const shift_bits = shift % MI_INTPTR_BITS;
    if (shift_bits == 0) return x;
    return math.shr(usize, x, shift_bits) | math.shl(usize, x, MI_INTPTR_BITS - shift_bits);
}

// This is needed for double free check, zig does alignment checks can't use mi_ptr_decode
pub fn mi_ptr_decode_raw(ptr: anytype, x: mi_encoded_t, keys: []const usize) usize {
    const decoded = mi_rotr(x -% keys[0], keys[0]) ^ keys[1];
    return if (decoded == @ptrToInt(ptr)) 0 else decoded; // TODO: This looks weird
}

fn mi_ptr_decode(ptr: anytype, x: mi_encoded_t, keys: []const usize) ?*mi_block_t {
    const decoded = mi_ptr_decode_raw(ptr, x, keys);
    return @intToPtr(?*mi_block_t, decoded);
}

pub fn mi_ptr_encode(ptr: anytype, block: ?*const mi_block_t, keys: []const usize) mi_encoded_t {
    const x = if (block == null) @ptrToInt(ptr) else @ptrToInt(block);
    const encoded = mi_rotl(x ^ keys[1], keys[0]) +% keys[0];
    return encoded;
}

pub fn mi_block_nextx(ptr: anytype, block: *const mi_block_t, keys: ?[]const usize) ?*mi_block_t {
    mi_track_mem_defined(block, @sizeOf(mi_block_t));
    var next: ?*mi_block_t = if (MI_ENCODE_FREELIST) mi_ptr_decode(ptr, block.next, keys.?) else @intToPtr(*mi_block_t, block.next);
    mi_track_mem_noaccess(block, @sizeOf(mi_block_t));
    return next;
}

pub inline fn mi_block_next(page: *const mi_page_t, block: *const mi_block_t) ?*mi_block_t {
    if (MI_ENCODE_FREELIST) {
        var next = mi_block_nextx(page, block, &page.keys);
        // check for free list corruption: is `next` at least in the same page?
        // TODO: check if `next` is `page->block_size` aligned?
        if (mi_unlikely(next != null and !mi_is_in_same_page(block, next.?))) {
            std.log.err("corrupted free list entry of size {}b at {} value {?}\n", .{ mi_page_block_size(page), block, next });
            next = null;
        }
        return next;
    }
    return mi_block_nextx(page, block, null);
}

pub fn mi_page_is_huge(page: *const mi_page_t) bool {
    return (_mi_page_segment(page).kind == .MI_SEGMENT_HUGE);
}

pub fn mi_page_usable_block_size(page: *const mi_page_t) usize {
    return mi_page_block_size(page) - MI_PADDING_SIZE;
}

pub fn _mi_memzero_aligned(dest: [*]u8, n: usize) void {
    @memset(dest, 0, n);
}

// Segment belonging to a page
pub fn _mi_page_segment(page: *const mi_page_t) *mi_segment_t {
    const segment = _mi_ptr_segment(page);
    mi_assert_internal(@ptrToInt(page) >= @ptrToInt(&segment.slices) and @ptrToInt(page) < @ptrToInt((@ptrCast([*]mi_segment_t, &segment.slices) + segment.slice_entries)));
    return segment;
}

//-----------------------------------------------------------
// os.c
//-----------------------------------------------------------
// round to a good OS allocation size (bounded by max 12.5% waste)
fn _mi_os_good_alloc_size(size: usize) usize {
    var align_size: usize = if (size < 512 * MI_KiB) std.mem.page_size //
    else if (size < 2 * MI_MiB) 64 * MI_KiB //
    else if (size < 8 * MI_MiB) 256 * MI_KiB //
    else if (size < 32 * MI_MiB) 1 * MI_MiB //
    else 4 * MI_MiB;
    if (mi_unlikely(size >= (std.math.maxInt(isize) - align_size))) return size; // possible overflow?
    return _mi_align_up(size, align_size);
}

//-----------------------------------------------------------
//  Page helpers
//-----------------------------------------------------------

// are all blocks in a page freed?
// note: needs up-to-date used count, (as the `xthread_free` list may not be empty). see `_mi_page_collect_free`.
pub inline fn mi_page_all_free(page: *const mi_page_t) bool {
    return (page.used == 0);
}

pub inline fn mi_page_set_heap(page: *mi_page_t, heap: ?*mi_heap_t) void {
    mi_assert_internal(mi_page_thread_free_flag(page) != mi_delayed_t.MI_DELAYED_FREEING);
    mi_atomic_store_release(&page.xheap, heap);
}

// Index a block in a page
pub fn mi_page_block_at(page: *const mi_page_t, page_start: anytype, block_size: usize, i: usize) *mi_block_t {
    mi_assert_internal(i <= page.reserved);
    return @intToPtr(*mi_block_t, @ptrToInt(page_start) + i * block_size);
}

pub fn mi_block_set_nextx(ptr: anytype, block: *mi_block_t, next: ?*const mi_block_t, keys: ?[]const usize) void {
    mi_track_mem_undefined(block, @sizeOf(mi_block_t));
    block.next = if (MI_ENCODE_FREELIST) mi_ptr_encode(ptr, next, keys.?) else @ptrToInt(next);
    mi_track_mem_noaccess(block, @sizeOf(mi_block_t));
}

pub inline fn mi_block_set_next(page: *const mi_page_t, block: *mi_block_t, next: ?*const mi_block_t) void {
    if (MI_ENCODE_FREELIST) {
        mi_block_set_nextx(page, block, next, &page.keys);
    } else {
        mi_block_set_nextx(page, block, next, null);
    }
}

fn mi_page_list_count(page: *mi_page_t, head: ?*mi_block_t) usize {
    if (MI_DEBUG < 3) return 0;
    var count: usize = 0;
    var block = head;
    while (block != null) : (block = mi_block_next(page, block.?)) {
        mi_assert_internal(page == _mi_ptr_page(block.?));
        count += 1;
    }
    return count;
}

fn mi_page_list_is_valid(page: *mi_page_t, block: ?*mi_block_t) bool {
    if (MI_DEBUG < 3) return true;
    var psize: usize = undefined;
    const page_area = _mi_page_start(_mi_page_segment(page), page, &psize);
    const start = @ptrToInt(page_area);
    const end = @ptrToInt(page_area + psize);
    var p = block;
    while (p != null) : (p = mi_block_next(page, p.?)) {
        if (@ptrToInt(p) < start or @ptrToInt(p) >= end) return false;
    }
    return true;
}

fn mi_page_is_valid_init(page: *mi_page_t) bool {
    if (MI_DEBUG < 3) return true;
    mi_assert_internal(page.xblock_size > 0);
    mi_assert_internal(page.used <= page.capacity);
    mi_assert_internal(page.capacity <= page.reserved);

    const segment = _mi_page_segment(page);
    const start = _mi_page_start(segment, page, null);
    mi_assert_internal(start == _mi_segment_page_start(segment, page, null));
    //const bsize = mi_page_block_size(page);
    //mi_assert_internal(start + page.capacity*page.block_size == page.top);

    mi_assert_internal(mi_page_list_is_valid(page, page.free));
    mi_assert_internal(mi_page_list_is_valid(page, page.local_free));

    if (MI_DEBUG > 3) { // generally too expensive to check this
        if (page.is_zero) {
            const ubsize = mi_page_usable_block_size(page);
            var block = page.free;
            while (block != null) : (block = mi_block_next(page, block)) {
                mi_assert_expensive(mi_mem_is_zero(block + 1, ubsize - @sizeOf(mi_block_t)));
            }
        }
    }

    const tfree = mi_page_thread_free(page);
    mi_assert_internal(mi_page_list_is_valid(page, tfree));
    //const tfree_count = mi_page_list_count(page, tfree);
    //mi_assert_internal(tfree_count <= page.thread_freed + 1);

    const free_count = mi_page_list_count(page, page.free) + mi_page_list_count(page, page.local_free);
    mi_assert_internal(page.used + free_count == page.capacity);

    return true;
}

pub fn _mi_page_is_valid(page: *mi_page_t) bool {
    if (MI_DEBUG < 3) return true;
    mi_assert_internal(mi_page_is_valid_init(page));
    if (MI_SECURE > 0) {
        mi_assert_internal(page.keys[0] != 0);
    }
    if (mi_page_heap(page) != null) {
        const segment = _mi_page_segment(page);

        mi_assert_internal(!mi._mi_process_is_initialized or segment.thread_id.load(AtomicOrder.Monotonic) == 0 or segment.thread_id.load(AtomicOrder.Monotonic) == mi_page_heap(page).?.thread_id);
        if (MI_HUGE_PAGE_ABANDON or segment.kind != .MI_SEGMENT_HUGE) {
            const pq = mi_page_queue_of(page);
            mi_assert_internal(mi_page_queue_contains(pq, page));
            mi_assert_internal(pq.block_size == mi_page_block_size(page) or mi_page_block_size(page) > MI_MEDIUM_OBJ_SIZE_MAX or mi_page_is_in_full(page));
            mi_assert_internal(mi_heap_contains_queue(mi_page_heap(page).?, pq));
        }
    }
    return true;
}

pub fn _mi_page_use_delayed_free(page: *mi_page_t, delay: mi_delayed_t, override_never: bool) void {
    while (!_mi_page_try_use_delayed_free(page, delay, override_never)) {
        mi_atomic_yield();
    }
}

pub fn _mi_page_try_use_delayed_free(page: *mi_page_t, delay: mi_delayed_t, override_never: bool) bool {
    var tfreex: mi_thread_free_t = undefined;
    var old_delay: mi_delayed_t = .MI_DELAYED_FREEING;
    var tfree: mi_thread_free_t = undefined;
    var yield_count: usize = 0;
    while ((old_delay == .MI_DELAYED_FREEING) or
        !mi_atomic_cas_weak_release(&page.xthread_free, &tfree, tfreex))
    {
        tfree = mi_atomic_load_acquire(&page.xthread_free); // note: must acquire as we can break/repeat this loop and not do a CAS;
        tfreex = mi_tf_set_delayed(tfree, delay);
        old_delay = mi_tf_delayed(tfree);
        if (mi_unlikely(old_delay == .MI_DELAYED_FREEING)) {
            if (yield_count >= 4) return false; // give up after 4 tries
            yield_count += 1;
            mi_atomic_yield(); // delay until outstanding MI_DELAYED_FREEING are done.
            // tfree = mi_tf_set_delayed(tfree, MI_NO_DELAYED_FREE); // will cause CAS to busy fail
        } else if (delay == old_delay) {
            break; // avoid atomic operation if already equal
        } else if (!override_never and old_delay == .MI_NEVER_DELAYED_FREE) {
            break; // leave never-delayed flag set
        }
    }

    return true; // success
}

//-----------------------------------------------------------
// Page collect the `local_free` and `thread_free` lists
//-----------------------------------------------------------

// Collect the local `thread_free` list using an atomic exchange.
// Note: The exchange must be done atomically as this is used right after
// moving to the full list in `mi_page_collect_ex` and we need to
// ensure that there was no race where the page became unfull just before the move.
fn _mi_page_thread_free_collect(page: *mi_page_t) void {
    var head: ?*mi_block_t = undefined;
    var tfreex: mi_thread_free_t = undefined;
    var tfree = mi_atomic_load_relaxed(&page.xthread_free);
    while (true) {
        head = mi_tf_block(tfree);
        tfreex = mi_tf_set_block(tfree, null);
        if (mi_atomic_cas_weak_acq_rel(&page.xthread_free, &tfree, tfreex)) break;
    }

    // return if the list is empty
    if (head == null) return;

    // find the tail -- also to get a proper count (without data races)
    const max_count = page.capacity; // cannot collect more than capacity
    var count: usize = 1;
    var tail = head;
    var next: ?*mi_block_t = mi_block_next(page, tail.?);
    while (next != null and count <= max_count) : (next = mi_block_next(page, tail.?)) {
        count += 1;
        tail = next;
    }
    // if `count > max_count` there was a memory corruption (possibly infinite list due to double multi-threaded free)
    if (count > max_count) {
        std.log.err("corrupted thread-free list", .{});
        return; // the thread-free items cannot be freed
    }

    // and append the current local free list
    mi_block_set_next(page, tail.?, page.local_free);
    page.local_free = head;

    // update counts now
    page.used -= @intCast(u32, count);
}

pub fn _mi_page_free_collect(page: *mi_page_t, force: bool) void {
    // collect the thread free list
    if (force or mi_page_thread_free(page) != null) { // quick test to avoid an atomic operation
        _mi_page_thread_free_collect(page);
    }

    // and the local free list
    if (page.local_free != null) {
        if (mi_likely(page.free == null)) {
            // usual case
            page.free = page.local_free;
            page.local_free = null;
            page.b.is_zero = false;
        } else if (force) {
            // append -- only on shutdown (force) as this is a linear operation
            var tail = page.local_free;
            var next = mi_block_next(page, tail.?);
            while (next != null) : (next = mi_block_next(page, tail.?)) {
                tail = next;
            }
            mi_block_set_next(page, tail.?, page.free);
            page.free = page.local_free;
            page.local_free = null;
            page.b.is_zero = false;
        }
    }

    mi_assert_internal(!force or page.local_free == null);
}

//-----------------------------------------------------------
//  Page fresh and retire
//-----------------------------------------------------------

// called from segments when reclaiming abandoned pages
pub fn _mi_page_reclaim(heap: *mi_heap_t, page: *mi_page_t) void {
    mi_assert_expensive(mi_page_is_valid_init(page));
    mi_assert_internal(mi_page_heap(page) == heap);
    mi_assert_internal(mi_page_thread_free_flag(page) != .MI_NEVER_DELAYED_FREE);
    if (MI_HUGE_PAGE_ABANDON) {
        mi_assert_internal(_mi_page_segment(page).kind != .MI_SEGMENT_HUGE);
    }
    mi_assert_internal(!page.b.is_reset);
    // TODO: push on full queue immediately if it is full?
    const pq = mi_page_queue(heap, mi_page_block_size(page));
    mi_page_queue_push(heap, pq, page);
    mi_assert_expensive(_mi_page_is_valid(page));
}

// allocate a fresh page from a segment
fn mi_page_fresh_alloc(heap: *mi_heap_t, pq: ?*mi_page_queue_t, block_size: usize, page_alignment: usize) ?*mi_page_t {
    if (!MI_HUGE_PAGE_ABANDON) {
        mi_assert_internal(pq != null);
        mi_assert_internal(mi_heap_contains_queue(heap, pq.?));
        mi_assert_internal(page_alignment > 0 or block_size > MI_MEDIUM_OBJ_SIZE_MAX or block_size == pq.?.block_size);
    }

    var page = _mi_segment_page_alloc(heap, block_size, page_alignment, &heap.tld.?.segments, &heap.tld.?.os) orelse return null;
    // page null: this may be out-of-memory, or an abandoned page was reclaimed (and in our queue)
    mi_assert_internal(page_alignment > 0 or block_size > MI_MEDIUM_OBJ_SIZE_MAX or _mi_page_segment(page).kind != .MI_SEGMENT_HUGE);
    mi_assert_internal(pq != null or page.xblock_size != 0);
    mi_assert_internal(pq != null or mi_page_block_size(page) >= block_size);
    // a fresh page was found, initialize it
    const full_block_size = if (pq == null or mi_page_queue_is_huge(pq.?)) mi_page_block_size(page) else block_size; // see also: mi_segment_huge_page_alloc
    mi_assert_internal(full_block_size >= block_size);
    mi_page_init(heap, page, full_block_size, heap.tld.?);
    mi_stat_increase(&heap.tld.?.stats.pages, 1);
    if (pq != null) mi_page_queue_push(heap, pq.?, page); // huge pages use pq==null
    mi_assert_expensive(_mi_page_is_valid(page));
    return page;
}

// Get a fresh page to use
fn mi_page_fresh(heap: *mi_heap_t, pq: *mi_page_queue_t) ?*mi_page_t {
    mi_assert_internal(mi_heap_contains_queue(heap, pq));
    const page = mi_page_fresh_alloc(heap, pq, pq.block_size, 0) orelse return null;
    mi_assert_internal(pq.block_size == mi_page_block_size(page));
    mi_assert_internal(pq == mi_page_queue(heap, mi_page_block_size(page)));
    return page;
}

//-------------------------------------------------------------
//   Do any delayed frees
//   (put there by other threads if they deallocated in a full page)
//-------------------------------------------------------------
pub fn _mi_heap_delayed_free_all(heap: *mi_heap_t) void {
    while (!_mi_heap_delayed_free_partial(heap)) {
        mi_atomic_yield();
    }
}

// returns true if all delayed frees were processed
pub fn _mi_heap_delayed_free_partial(heap: *mi_heap_t) bool {
    // take over the list (note: no atomic exchange since it is often null)
    var block = mi_atomic_load_ptr_relaxed(mi_block_t, &heap.thread_delayed_free);
    while (block != null and !mi_atomic_cas_ptr_weak_acq_rel(mi_block_t, &heap.thread_delayed_free, &block, null)) { // nothing
    }
    var all_freed: bool = true;

    // and free them all
    while (block != null) {
        var next = mi_block_nextx(heap, block.?, &heap.keys);
        // use internal free instead of regular one to keep stats etc correct
        if (!_mi_free_delayed_block(block.?)) {
            // we might already start delayed freeing while another thread has not yet
            // reset the delayed_freeing flag; in that case delay it further by reinserting the current block
            // into the delayed free list
            all_freed = false;
            var dfree = mi_atomic_load_ptr_relaxed(mi_block_t, &heap.thread_delayed_free);
            while (true) {
                mi_block_set_nextx(heap, block.?, dfree, &heap.keys);
                if (mi_atomic_cas_ptr_weak_release(mi_block_t, &heap.thread_delayed_free, &dfree, block)) break;
            }
        }
        block = next;
    }
    return all_freed;
}

//-----------------------------------------------------------
//  Unfull, abandon, free and retire
//-----------------------------------------------------------

// Move a page from the full list back to a regular list
pub fn _mi_page_unfull(page: *mi_page_t) void {
    mi_assert_expensive(_mi_page_is_valid(page));
    mi_assert_internal(mi_page_is_in_full(page));
    if (!mi_page_is_in_full(page)) return;

    const heap = mi_page_heap(page).?;
    const pqfull = &heap.pages[MI_BIN_FULL];
    mi_page_set_in_full(page, false); // to get the right queue
    const pq = mi_heap_page_queue_of(heap, page);
    mi_page_set_in_full(page, true);
    mi_page_queue_enqueue_from(pq, pqfull, page);
}

fn mi_page_to_full(page: *mi_page_t, pq: *mi_page_queue_t) void {
    mi_assert_internal(pq == mi_page_queue_of(page));
    mi_assert_internal(!mi_page_immediate_available(page));
    mi_assert_internal(mi_page_is_in_full(page));

    if (mi_page_is_in_full(page)) return;
    mi_page_queue_enqueue_from(&mi_page_heap(page).?.pages[MI_BIN_FULL], pq, page);
    _mi_page_free_collect(page, false); // try to collect right away in case another thread freed just before MI_USE_DELAYED_FREE was set
}

// Abandon a page with used blocks at the end of a thread.
// Note: only call if it is ensured that no references exist from
// the `page.heap.thread_delayed_free` into this page.
// Currently only called through `mi_heap_collect_ex` which ensures this.
pub fn _mi_page_abandon(page: *mi_page_t, pq: *mi_page_queue_t) void {
    mi_assert_expensive(_mi_page_is_valid(page));
    mi_assert_internal(pq == mi_page_queue_of(page));
    mi_assert_internal(mi_page_heap(page) != null);

    const pheap = mi_page_heap(page).?;

    // remove from our page list
    const segments_tld = &pheap.tld.?.segments;
    mi_page_queue_remove(pq, page);

    // page is no longer associated with our heap
    mi_assert_internal(mi_page_thread_free_flag(page) == .MI_NEVER_DELAYED_FREE);
    mi_page_set_heap(page, null);

    if (MI_DEBUG > 1) {
        // check there are no references left..
        var block = pheap.thread_delayed_free.load(AtomicOrder.Monotonic);
        while (block != null) : (block = mi_block_nextx(pheap, block.?, &pheap.keys)) {
            mi_assert_internal(_mi_ptr_page(block.?) != page);
        }
    }

    // and abandon it
    mi_assert_internal(mi_page_heap(page) == null);
    _mi_segment_page_abandon(page, segments_tld);
}

// Free a page with no more free blocks
pub fn _mi_page_free(page: *mi_page_t, pq: *mi_page_queue_t, force: bool) void {
    mi_assert_expensive(_mi_page_is_valid(page));
    mi_assert_internal(pq == mi_page_queue_of(page));
    mi_assert_internal(mi_page_all_free(page));
    mi_assert_internal(mi_page_thread_free_flag(page) != .MI_DELAYED_FREEING);

    // no more aligned blocks in here
    mi_page_set_has_aligned(page, false);

    const heap = mi_page_heap(page).?;

    // remove from the page list
    // (no need to do _mi_heap_delayed_free first as all blocks are already free)
    const segments_tld = &heap.tld.?.segments;
    mi_page_queue_remove(pq, page);

    // and free it
    mi_page_set_heap(page, null);
    _mi_segment_page_free(page, force, segments_tld);
}

// Retire parameters
const MI_MAX_RETIRE_SIZE = MI_MEDIUM_OBJ_SIZE_MAX;
const MI_RETIRE_CYCLES = (8);

// Retire a page with no more used blocks
// Important to not retire too quickly though as new
// allocations might coming.
// Note: called from `mi_free` and benchmarks often
// trigger this due to freeing everything and then
// allocating again so careful when changing this.
pub fn _mi_page_retire(page: *mi_page_t) void {
    mi_assert_expensive(_mi_page_is_valid(page));
    mi_assert_internal(mi_page_all_free(page));

    mi_page_set_has_aligned(page, false);

    // don't retire too often..
    // (or we end up retiring and re-allocating most of the time)
    // NOTE: refine this more: we should not retire if this
    // is the only page left with free blocks. It is not clear
    // how to check this efficiently though...
    // for now, we don't retire if it is the only page left of this size class.
    const pq = mi_page_queue_of(page);
    if (mi_likely(page.xblock_size <= MI_MAX_RETIRE_SIZE and !mi_page_queue_is_special(pq))) { // not too large && not full or huge queue?
        if (pq.last == page and pq.first == page) { // the only page in the queue?
            mi_stat_counter_increase(&mi._mi_stats_main.page_no_retire, 1);
            page.b.retire_expire = 1 + if (page.xblock_size <= @intCast(usize, MI_SMALL_OBJ_SIZE_MAX)) @intCast(u7, MI_RETIRE_CYCLES) else @intCast(u7, MI_RETIRE_CYCLES / 4);
            const heap = mi_page_heap(page).?;
            mi_assert_internal(@ptrToInt(pq) >= @ptrToInt(&heap.pages));
            const index = (@ptrToInt(pq) - @ptrToInt(&heap.pages)) / @sizeOf(mi_page_queue_t);
            mi_assert_internal(index < MI_BIN_FULL and index < MI_BIN_HUGE);
            if (index < heap.page_retired_min) heap.page_retired_min = index;
            if (index > heap.page_retired_max) heap.page_retired_max = index;
            mi_assert_internal(mi_page_all_free(page));
            return; // dont't free after all
        }
    }
    _mi_page_free(page, pq, false);
}

// free retired pages: we don't need to look at the entire queues
// since we only retire pages that are at the head position in a queue.
pub fn _mi_heap_collect_retired(heap: *mi_heap_t, force: bool) void {
    var min: usize = MI_BIN_FULL;
    var max: usize = 0;
    var bin = heap.page_retired_min;
    while (bin <= heap.page_retired_max) : (bin += 1) {
        const pq = &heap.pages[bin];
        var page = pq.first;
        if (page != null and page.?.b.retire_expire != 0) {
            if (mi_page_all_free(page.?)) {
                page.?.b.retire_expire -= 1;
                if (force or page.?.b.retire_expire == 0) {
                    _mi_page_free(pq.first.?, pq, force);
                } else {
                    // keep retired, update min/max
                    if (bin < min) min = bin;
                    if (bin > max) max = bin;
                }
            } else {
                page.?.b.retire_expire = 0;
            }
        }
    }
    heap.page_retired_min = min;
    heap.page_retired_max = max;
}

//-----------------------------------------------------------
//  Initialize the initial free list in a page.
//  In secure mode we initialize a randomized list by
//  alternating between slices.
//-----------------------------------------------------------

const MI_MAX_SLICE_SHIFT = 6; // at most 64 slices
const MI_MAX_SLICES = (1 << MI_MAX_SLICE_SHIFT);
const MI_MIN_SLICES = 2;

fn mi_page_free_list_extend_secure(heap: *mi_heap_t, page: *mi_page_t, bsize: usize, extend: usize, stats: *const mi_stats_t) void {
    _ = stats;
    if (MI_SECURE <= 2) {
        mi_assert_internal(page.free == null);
        mi_assert_internal(page.local_free == null);
    }
    mi_assert_internal(page.capacity + extend <= page.reserved);
    mi_assert_internal(bsize == mi_page_block_size(page));
    const page_area = _mi_page_start(_mi_page_segment(page), page, null);

    // initialize a randomized free list
    // set up `slice_count` slices to alternate between
    var shift: std.math.Log2Int(usize) = MI_MAX_SLICE_SHIFT;
    while ((extend >> shift) == 0) {
        shift -= 1;
    }
    const slice_count = @intCast(usize, 1) << shift;
    const slice_extend = extend / slice_count;
    mi_assert_internal(slice_extend >= 1);
    var blocks: [MI_MAX_SLICES]*mi_block_t = undefined; // current start of the slice
    var counts: [MI_MAX_SLICES]usize = undefined; // available objects in the slice
    var i: usize = 0;
    while (i < slice_count) : (i += 1) {
        blocks[i] = mi_page_block_at(page, page_area, bsize, page.capacity + i * slice_extend);
        counts[i] = slice_extend;
    }
    counts[slice_count - 1] += (extend % slice_count); // final slice holds the modulus too (todo: distribute evenly?)

    // and initialize the free list by randomly threading through them
    // set up first element
    const r = _mi_heap_random_next(heap);
    var current = r % slice_count;
    counts[current] -= 1;
    const free_start = blocks[current];
    // and iterate through the rest; use `random_shuffle` for performance
    var rnd = _mi_random_shuffle(r | 1); // ensure not 0
    i = 1;
    while (i < extend) : (i += 1) {
        // call random_shuffle only every INTPTR_SIZE rounds
        const round = i % MI_INTPTR_SIZE;
        if (round == 0) rnd = _mi_random_shuffle(rnd);
        // select a random next slice index
        var next = (std.math.shr(usize, rnd, 8 * round) & (slice_count - 1));
        while (counts[next] == 0) { // ensure it still has space
            next += 1;
            if (next == slice_count) next = 0;
        }
        // and link the current block to it
        counts[next] -= 1;
        const block = blocks[current];
        blocks[current] = @intToPtr(*mi_block_t, @ptrToInt(block) + bsize); // bump to the following block
        mi_block_set_next(page, block, blocks[next]); // and set next; note: we may have `current == next`
        current = next;
    }
    // prepend to the free list (usually null)
    mi_block_set_next(page, blocks[current], page.free); // end of the list
    page.free = free_start;
}

fn mi_page_free_list_extend(page: *mi_page_t, bsize: usize, extend: usize, stats: *const mi_stats_t) void {
    _ = stats;
    if (MI_SECURE <= 2) {
        mi_assert_internal(page.free == null);
        mi_assert_internal(page.local_free == null);
    }
    mi_assert_internal(page.capacity + extend <= page.reserved);
    mi_assert_internal(bsize == mi_page_block_size(page));
    const page_area = _mi_page_start(_mi_page_segment(page), page, null);

    const start = mi_page_block_at(page, page_area, bsize, page.capacity);

    // initialize a sequential free list
    const last = mi_page_block_at(page, page_area, bsize, page.capacity + extend - 1);
    var block = start;
    while (@ptrToInt(block) <= @ptrToInt(last)) {
        const next = @intToPtr(*mi_block_t, @ptrToInt(block) + bsize);
        mi_block_set_next(page, block, next);
        block = next;
    }
    // prepend to free list (usually `null`)
    mi_block_set_next(page, last, page.free);
    page.free = start;
}

//-----------------------------------------------------------
//  Page initialize and extend the capacity
//-----------------------------------------------------------

const MI_MAX_EXTEND_SIZE = (4 * 1024); // heuristic, one OS page seems to work well.
const MI_MIN_EXTEND = if (MI_SECURE > 0) (8 * MI_SECURE) // extend at least by this many
else 4;

// Extend the capacity (up to reserved) by initializing a free list
// We do at most `MI_MAX_EXTEND` to avoid touching too much memory
// Note: we also experimented with "bump" allocation on the first
// allocations but this did not speed up any benchmark (due to an
// extra test in malloc? or cache effects?)
fn mi_page_extend_free(heap: *mi_heap_t, page: *mi_page_t, tld: *mi_tld_t) void {
    mi_assert_expensive(mi_page_is_valid_init(page));
    if (MI_SECURE <= 2) {
        mi_assert(page.free == null);
        mi_assert(page.local_free == null);
        if (page.free != null) return;
    }
    if (page.capacity >= page.reserved) return;

    var page_size: usize = undefined;
    _ = _mi_page_start(_mi_page_segment(page), page, &page_size);
    mi_stat_counter_increase(&tld.stats.pages_extended, 1);

    // calculate the extend count
    const bsize = if (page.xblock_size < MI_HUGE_BLOCK_SIZE) page.xblock_size else page_size;
    var extend = page.reserved - page.capacity;
    mi_assert_internal(extend > 0);

    var max_extend = if (bsize >= MI_MAX_EXTEND_SIZE) MI_MIN_EXTEND else MI_MAX_EXTEND_SIZE / bsize;
    if (max_extend < MI_MIN_EXTEND) {
        max_extend = MI_MIN_EXTEND;
    }
    mi_assert_internal(max_extend > 0);

    if (extend > max_extend) {
        // ensure we don't touch memory beyond the page to reduce page commit.
        // the `lean` benchmark tests this. Going from 1 to 8 increases rss by 50%.
        extend = @intCast(u16, max_extend);
    }

    mi_assert_internal(extend > 0 and extend + page.capacity <= page.reserved);
    mi_assert_internal(extend < (1 << 16));

    // and append the extend the free list
    if (extend < MI_MIN_SLICES or MI_SECURE == 0) { // !mi_option_is_enabled(mi_option_secure)) {
        mi_page_free_list_extend(page, bsize, extend, &tld.stats);
    } else {
        mi_page_free_list_extend_secure(heap, page, bsize, extend, &tld.stats);
    }
    // enable the new free list
    page.capacity += extend;
    mi_stat_increase(&tld.stats.page_committed, extend * bsize);

    // extension into zero initialized memory preserves the zero'd free list
    if (!page.b.is_zero_init) {
        page.b.is_zero = false;
    }
    mi_assert_expensive(mi_page_is_valid_init(page));
}

// Initialize a fresh page
fn mi_page_init(heap: *mi_heap_t, page: *mi_page_t, block_size: usize, tld: *mi_tld_t) void {
    const segment = _mi_page_segment(page);
    mi_assert_internal(block_size > 0);
    // set fields
    mi_page_set_heap(page, heap);
    page.xblock_size = if (block_size < MI_HUGE_BLOCK_SIZE) @intCast(u32, block_size) else MI_HUGE_BLOCK_SIZE; // initialize before _mi_segment_page_start
    var page_size: usize = undefined;
    const page_start = _mi_segment_page_start(segment, page, &page_size);
    mi_track_mem_noaccess(page_start, page_size);
    mi_assert_internal(mi_page_block_size(page) <= page_size);
    mi_assert_internal(page_size <= page.slice_count * MI_SEGMENT_SLICE_SIZE);
    mi_assert_internal(page_size / block_size < (1 << 16));
    page.reserved = @intCast(u16, page_size / block_size);
    mi_assert_internal(page.reserved > 0);
    if (MI_ENCODE_FREELIST) {
        page.keys[0] = _mi_heap_random_next(heap);
        page.keys[1] = _mi_heap_random_next(heap);
    }
    if (MI_DEBUG > 0)
        page.b.is_zero = false // ensure in debug mode we initialize with MI_DEBUG_UNINIT, see issue #501
    else
        page.b.is_zero = page.b.is_zero_init;

    mi_assert_internal(page.b.is_committed);
    mi_assert_internal(!page.b.is_reset);
    mi_assert_internal(page.capacity == 0);
    mi_assert_internal(page.free == null);
    mi_assert_internal(page.used == 0);
    mi_assert_internal(page.xthread_free.load(AtomicOrder.Unordered) == 0);
    mi_assert_internal(page.next == null);
    mi_assert_internal(page.prev == null);
    mi_assert_internal(page.b.retire_expire == 0);
    mi_assert_internal(!mi_page_has_aligned(page));
    if (MI_ENCODE_FREELIST) {
        mi_assert_internal(page.keys[0] != 0);
        mi_assert_internal(page.keys[1] != 0);
    }
    mi_assert_expensive(mi_page_is_valid_init(page));

    // initialize an initial free list
    mi_page_extend_free(heap, page, tld);
    mi_assert(mi_page_immediate_available(page));
}

//-----------------------------------------------------------
// Find pages with free blocks
//-----------------------------------------------------------

// Find a page with free blocks of `page.block_size`.
fn mi_page_queue_find_free_ex(heap: *mi_heap_t, pq: *mi_page_queue_t, first_try: bool) ?*mi_page_t {
    // search through the pages in "next fit" order
    var count: usize = 0;
    var page = pq.first;
    while (page != null) {
        const next = page.?.next; // remember next
        count += 1;

        // 0. collect freed blocks by us and other threads
        _mi_page_free_collect(page.?, false);

        // 1. if the page contains free blocks, we are done
        if (mi_page_immediate_available(page.?)) {
            break; // pick this one
        }

        // 2. Try to extend
        if (page.?.capacity < page.?.reserved) {
            mi_page_extend_free(heap, page.?, heap.tld.?);
            mi_assert_internal(mi_page_immediate_available(page.?));
            break;
        }

        // 3. If the page is completely full, move it to the `mi_pages_full`
        // queue so we don't visit long-lived pages too often.
        mi_assert_internal(!mi_page_is_in_full(page.?) and !mi_page_immediate_available(page.?));
        mi_page_to_full(page.?, pq);

        page = next;
    } // for each page

    mi_stat_counter_increase(&heap.tld.?.stats.searches, @intCast(i64, count));

    if (page == null) {
        _mi_heap_collect_retired(heap, false); // perhaps make a page available?
        page = mi_page_fresh(heap, pq);
        if (page == null and first_try) {
            // out-of-memory _or_ an abandoned page with free blocks was reclaimed, try once again
            page = mi_page_queue_find_free_ex(heap, pq, false);
        }
    } else {
        mi_assert(pq.first == page);
        page.?.b.retire_expire = 0;
    }
    mi_assert_internal(page == null or mi_page_immediate_available(page.?));
    return page;
}

// Find a page with free blocks of `size`.
fn mi_find_free_page(heap: *mi_heap_t, size: usize) ?*mi_page_t {
    const pq = mi_page_queue(heap, size);
    const page = pq.first orelse return mi_page_queue_find_free_ex(heap, pq, true);
    if (MI_SECURE >= 3) { // in secure mode, we extend half the time to increase randomness
        if (page.capacity < page.reserved and ((_mi_heap_random_next(heap) & 1) == 1)) {
            mi_page_extend_free(heap, page, heap.tld.?);
            mi_assert_internal(mi_page_immediate_available(page));
        }
    } else {
        _mi_page_free_collect(page, false);
    }

    if (mi_page_immediate_available(page)) {
        page.b.retire_expire = 0;
        return page; // fast path
    }
    return mi_page_queue_find_free_ex(heap, pq, true);
}

//-----------------------------------------------------------
// Users can register a deferred free function called
// when the `free` list is empty. Since the `local_free`
// is separate this is deterministically called after
// a certain number of allocations.
//-----------------------------------------------------------

const DeferredArg = opaque {};
const mi_deferred_free_fun = *const fn (force: bool, heartbeat: u64, arg: ?*DeferredArg) void;

var deferred_free: ?mi_deferred_free_fun = null;
var deferred_arg = Atomic(?*DeferredArg).init(null);

pub fn _mi_deferred_free(heap: *mi_heap_t, force: bool) void {
    heap.tld.?.heartbeat += 1;
    if (deferred_free != null and !heap.tld.?.recurse) {
        heap.tld.?.recurse = true;
        deferred_free.?(force, heap.tld.?.heartbeat, mi_atomic_load_ptr_relaxed(DeferredArg, &deferred_arg));
        heap.tld.?.recurse = false;
    }
}

pub fn mi_register_deferred_free(fun: mi_deferred_free_fun, arg: ?*opaque {}) void {
    deferred_free = fun;
    mi_atomic_store_ptr_release(void, &deferred_arg, arg);
}

//-----------------------------------------------------------
// General allocation
//-----------------------------------------------------------

// Large and huge page allocation.
// Huge pages are allocated directly without being in a queue.
// Because huge pages contain just one block, and the segment contains
// just that page, we always treat them as abandoned and any thread
// that frees the block can free the whole page and segment directly.
fn mi_large_huge_page_alloc(heap: *mi_heap_t, size: usize, page_alignment: usize) ?*mi_page_t {
    const block_size: usize = _mi_os_good_alloc_size(size);
    mi_assert_internal(mi_bin(block_size) == MI_BIN_HUGE or page_alignment > 0);
    const is_huge = (block_size > MI_LARGE_OBJ_SIZE_MAX or page_alignment > 0);
    var pq: ?*mi_page_queue_t = undefined;
    if (MI_HUGE_PAGE_ABANDON) {
        pq = if (is_huge) null else mi_page_queue(heap, block_size);
    } else {
        pq = mi_page_queue(heap, if (is_huge) MI_HUGE_BLOCK_SIZE else block_size); // not block_size as that can be low if the page_alignment > 0
        mi_assert_internal(!is_huge or mi_page_queue_is_huge(pq.?));
    }
    const page = mi_page_fresh_alloc(heap, pq, block_size, page_alignment) orelse return null;
    mi_assert_internal(mi_page_immediate_available(page));

    if (is_huge) {
        mi_assert_internal(_mi_page_segment(page).kind == .MI_SEGMENT_HUGE);
        mi_assert_internal(_mi_page_segment(page).used == 1);
        if (MI_HUGE_PAGE_ABANDON) {
            mi_assert_internal(_mi_page_segment(page).thread_id.load(AtomicOrder.Monotonic) == 0); // abandoned, not in the huge queue
            mi_page_set_heap(page, null);
        }
    } else {
        mi_assert_internal(_mi_page_segment(page).kind != .MI_SEGMENT_HUGE);
    }

    const bsize = mi_page_usable_block_size(page); // note: not `mi_page_block_size` to account for padding
    if (bsize <= MI_LARGE_OBJ_SIZE_MAX) {
        mi_stat_increase(&heap.tld.?.stats.large, bsize);
        mi_stat_counter_increase(&heap.tld.?.stats.large_count, 1);
    } else {
        mi_stat_increase(&heap.tld.?.stats.huge, bsize);
        mi_stat_counter_increase(&heap.tld.?.stats.huge_count, 1);
    }
    return page;
}

// Allocate a page
// Note: in debug mode the size includes MI_PADDING_SIZE and might have overflowed.
fn mi_find_page(heap: *mi_heap_t, size: usize, huge_alignment: usize) ?*mi_page_t {
    // huge allocation?
    const req_size = size - MI_PADDING_SIZE; // correct for padding_size in case of an overflow on `size`
    if (mi_unlikely(req_size > (MI_MEDIUM_OBJ_SIZE_MAX - MI_PADDING_SIZE) or huge_alignment > 0)) {
        if (mi_unlikely(req_size > PTRDIFF_MAX)) { // we don't allocate more than PTRDIFF_MAX (see <https://sourceware.org/ml/libc-announce/2019/msg00001.html>)
            std.log.err("allocation request is too large ({} bytes)", .{req_size});
            return null;
        } else {
            return mi_large_huge_page_alloc(heap, size, huge_alignment);
        }
    } else {
        // otherwise find a page with free blocks in our size segregated queues
        mi_assert_internal(size >= MI_PADDING_SIZE);
        return mi_find_free_page(heap, size);
    }
}

// Generic allocation routine if the fast path (`alloc.c:mi_page_malloc`) does not succeed.
// Note: in debug mode the size includes MI_PADDING_SIZE and might have overflowed.
// The `huge_alignment` is normally 0 but is set to a multiple of MI_SEGMENT_SIZE for
// very large requested alignments in which case we use a huge segment.
pub fn _mi_malloc_generic(heap_in: *mi_heap_t, size: usize, zero: bool, huge_alignment: usize) ?*u8 {
    var heap = heap_in;
    // initialize if necessary
    if (mi_unlikely(!mi_heap_is_initialized(heap))) {
        mi_thread_init(); // calls `_mi_heap_init` in turn
        heap = mi_get_default_heap();
        if (mi_unlikely(!mi_heap_is_initialized(heap))) {
            return null;
        }
    }
    mi_assert_internal(mi_heap_is_initialized(heap));

    // call potential deferred free routines
    _mi_deferred_free(heap, false);

    // free delayed frees from other threads (but skip contended ones)
    _ = _mi_heap_delayed_free_partial(heap);

    // find (or allocate) a page of the right size
    var page = mi_find_page(heap, size, huge_alignment);
    if (mi_unlikely(page == null)) { // first time out of memory, try to collect and retry the allocation once more
        mi_heap_collect(heap, true); // force
        page = mi_find_page(heap, size, huge_alignment);
    }

    if (mi_unlikely(page == null)) { // out of memory
        const req_size = size - MI_PADDING_SIZE; // correct for padding_size in case of an overflow on `size`
        std.log.err("unable to allocate memory ({} bytes)\n", .{req_size});
        return null;
    }

    mi_assert_internal(mi_page_immediate_available(page.?));
    mi_assert_internal(mi_page_block_size(page.?) >= size);

    // and try again, this time succeeding! (i.e. this should never recurse through _mi_page_malloc)
    if (mi_unlikely(zero and page.?.xblock_size == 0)) {
        // note: we cannot call _mi_page_malloc with zeroing for huge blocks; we zero it afterwards in that case.
        const p = _mi_page_malloc(heap, page.?, size, false);
        mi_assert_internal(p != null);
        _mi_memzero_aligned(@ptrCast([*]u8, p), mi_page_usable_block_size(page.?));
        return p;
    } else {
        return _mi_page_malloc(heap, page.?, size, zero);
    }
}
