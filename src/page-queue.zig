//-----------------------------------------------------------------------------
// Copyright (c) 2018-2020, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

//------------------------------------------------------------
//  Definition of page queues for each block size
//------------------------------------------------------------

const std = @import("std");
const assert = std.debug.assert;
const AtomicOrder = std.atomic.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("page.zig");
    usingnamespace @import("page-queue.zig");
    usingnamespace @import("segment.zig");
};

// #defines
const MI_DEBUG = mi.MI_DEBUG;
const MI_BIN_FULL = mi.MI_BIN_FULL;
const MI_BIN_HUGE = mi.MI_BIN_HUGE;

const MI_MEDIUM_OBJ_SIZE_MAX = mi.MI_MEDIUM_OBJ_SIZE_MAX;
const MI_MEDIUM_OBJ_WSIZE_MAX = mi.MI_MEDIUM_OBJ_WSIZE_MAX;
const MI_LARGE_OBJ_SIZE_MAX = mi.MI_LARGE_OBJ_SIZE_MAX;
const MI_SMALL_SIZE_MAX = mi.MI_SMALL_SIZE_MAX;

const mi_bsr = mi.mi_bsr;
const mi_heap_t = mi.mi_heap_t;
const mi_page_t = mi.mi_page_t;
const mi_page_queue_t = mi.mi_page_queue_t;

// Functions

fn noop(cond: bool) void {
    _ = cond;
}

const mi_page_is_in_full = mi.mi_page_is_in_full;
const mi_page_set_in_full = mi.mi_page_set_in_full;
const mi_page_heap = mi.mi_page_heap;
const _mi_page_segment = mi._mi_page_segment;

const mi_assert_internal = if (MI_DEBUG > 1) assert else noop;

//-----------------------------------------------------------
// Minimal alignment in machine words (i.e. `sizeof(void*)`)
//-----------------------------------------------------------

const MI_ALIGN4W = if (mi.MI_MAX_ALIGN_SIZE > 4 * mi.MI_INTPTR_SIZE)
    @compileError("define alignment for more than 4x word size for this platform")
else if (mi.MI_MAX_ALIGN_SIZE > 2 * mi.MI_INTPTR_SIZE) true // 4 machine words minimal alignment
else false;
const MI_ALIGN2W = (!MI_ALIGN4W and mi.MI_MAX_ALIGN_SIZE > mi.MI_INTPTR_SIZE); // 2 machine words minimal alignment

// default alignment is 1 word if both MI_ALIGN4W and MI_ALIGN2W

//-----------------------------------------------------------
//  Queue query
//-----------------------------------------------------------
pub fn mi_page_queue_is_huge(pq: *const mi_page_queue_t) bool {
    return (pq.block_size == (MI_MEDIUM_OBJ_SIZE_MAX + @sizeOf(usize)));
}

pub fn mi_page_queue_is_full(pq: *const mi_page_queue_t) bool {
    return pq.block_size == (MI_MEDIUM_OBJ_SIZE_MAX + 2 * @sizeOf(usize));
}

pub fn mi_page_queue_is_special(pq: *const mi_page_queue_t) bool {
    return pq.block_size > MI_MEDIUM_OBJ_SIZE_MAX;
}

//-------------------------------------------------------------
// Bins
//-------------------------------------------------------------

// Return the bin for a given field size.
// Returns MI_BIN_HUGE if the size is too large.
// We use `wsize` for the size in "machine word sizes",
// i.e. byte size == `wsize*sizeof(void*)`.
pub inline fn mi_bin(size: usize) u8 {
    var wsize = mi._mi_wsize_from_size(size);
    var bin: u8 = undefined;
    if (wsize <= 1) {
        bin = 1;
    } else if (MI_ALIGN4W and wsize <= 4) {
        bin = @intCast(u8, wsize + 1) & ~@intCast(u8, 1); // round to double word sizes
    } else if (MI_ALIGN2W and wsize <= 8) {
        bin = @intCast(u8, wsize + 1) & ~@intCast(u8, 1); // round to double word sizes
    } else if (wsize <= 8) {
        bin = @intCast(u8, wsize);
    } else if (wsize > MI_MEDIUM_OBJ_WSIZE_MAX) {
        bin = MI_BIN_HUGE;
    } else {
        if (MI_ALIGN4W and wsize <= 16)
            wsize = (wsize + 3) & ~@intCast(usize, 3); // round to 4x word sizes
        wsize -= 1;
        // find the highest bit
        const b = @intCast(u8, mi_bsr(wsize)); // note: wsize != 0
        // and use the top 3 bits to determine the bin (~12.5% worst internal fragmentation).
        // - adjust with 3 because we use do not round the first 8 sizes
        //   which each get an exact bin
        bin = ((b << 2) + @intCast(u8, (wsize >> @intCast(u5, b - 2)) & 0x03)) - 3;
        assert(bin < MI_BIN_HUGE);
    }
    assert(bin > 0 and bin <= MI_BIN_HUGE);
    return bin;
}

test "bins" {
    const expect = std.testing.expect;
    var i: usize = 1;
    std.debug.print("mi.MEDIUM_OBJ_WSIZE_MAX: {}\n", .{MI_MEDIUM_OBJ_WSIZE_MAX});
    while (i < std.math.maxInt(usize) and i > 0) : (i <<= 1) {
        std.debug.print("mi_bin({}): {}\n", .{ i, mi_bin(i) });
        std.debug.print("mi_bin({}): {}\n", .{ i + 5, mi_bin(i + 5) });
    }
    // std.debug.print("wsize: {}\n", .{mi._mi_wsize_from_size(400)});
    // std.debug.print("mi_bin(2): {}\n", .{mi_bin(400)});
    try expect(mi_bin(2) == 1);
}

//------------------------------------------------------------
//  Queue of pages with free blocks
//------------------------------------------------------------

pub fn _mi_bin(size: usize) u8 {
    return mi_bin(size);
}

pub fn _mi_bin_size(bin: u8) usize {
    return mi._mi_heap_empty.pages[bin].block_size;
}

pub inline fn mi_page_queue(heap: *const mi_heap_t, size: usize) *mi_page_queue_t {
    // TODO: casting away const!
    return &@intToPtr(*mi_heap_t, @ptrToInt(heap)).pages[_mi_bin(size)];
}

// Good size for allocation
fn mi_good_size(size: usize) usize {
    if (size <= MI_MEDIUM_OBJ_SIZE_MAX) {
        return _mi_bin_size(mi_bin(size));
    } else {
        return mi._mi_align_up(size, mi._mi_os_page_size());
    }
}

pub fn mi_page_queue_contains(queue: *mi_page_queue_t, page: *const mi_page_t) bool {
    if (MI_DEBUG < 1) return true;
    var list = queue.first;
    while (list) |l| : (list = l.next) {
        mi_assert_internal(l.next == null or l.next.?.prev == l);
        mi_assert_internal(l.prev == null or l.prev.?.next == l);
        if (l == page) return true;
    }
    return false;
}

pub fn mi_heap_contains_queue(heap: *const mi_heap_t, pq: *const mi_page_queue_t) bool {
    if (MI_DEBUG > 1) return true;
    const pq_addr = @ptrToInt(pq);
    return pq_addr >= @ptrToInt(&heap.pages[0]) and pq_addr <= @ptrToInt(&heap.pages[MI_BIN_FULL]);
}

pub fn mi_page_queue_of(page: *const mi_page_t) *mi_page_queue_t {
    const bin = if (page.is_in_full()) MI_BIN_FULL else mi_bin(page.xblock_size);
    const heap = mi_page_heap(page).?;
    assert(bin <= MI_BIN_FULL);
    const pq = &heap.pages[bin];
    assert(bin >= MI_BIN_HUGE or page.xblock_size == pq.block_size);
    assert(mi_page_queue_contains(pq, page));
    return pq;
}

pub fn mi_heap_page_queue_of(heap: *mi_heap_t, page: *mi_page_t) *mi_page_queue_t {
    const bin = if (mi_page_is_in_full(page)) MI_BIN_FULL else mi_bin(page.xblock_size);
    assert(bin <= MI_BIN_FULL);
    const pq = &heap.pages[bin];
    assert(mi_page_is_in_full(page) or page.xblock_size == pq.block_size);
    return pq;
}

// The current small page array is for efficiency and for each
// small size (up to 256) it points directly to the page for that
// size without having to compute the bin. This means when the
// current free page queue is updated for a small bin, we need to update a
// range of entries in `_mi_page_small_free`.
fn mi_heap_queue_first_update(heap: *mi_heap_t, pq: *const mi_page_queue_t) void {
    mi_assert_internal(mi_heap_contains_queue(heap, pq));
    const size = pq.block_size;
    if (size > MI_SMALL_SIZE_MAX) return;

    var page = pq.first;
    if (pq.first == null) page = &mi._mi_page_empty;

    // find index in the right direct page array
    var start: usize = undefined;
    var idx = mi._mi_wsize_from_size(size);
    var pages_free = heap.pages_free_direct;

    if (pages_free[idx] == page) return; // already set

    // find start slot
    if (idx <= 1) {
        start = 0;
    } else {
        // find previous size; due to minimal alignment upto 3 previous bins may need to be skipped
        var bin = mi_bin(size);
        var prev = @ptrCast([*]const mi_page_queue_t, pq) - 1;
        while (bin == mi_bin(prev[0].block_size) and @ptrToInt(prev) > @ptrToInt(&heap.pages[0])) {
            prev -= 1;
        }
        start = 1 + mi._mi_wsize_from_size(prev[0].block_size);
        if (start > idx) start = idx;
    }

    // set size range to the right page
    assert(start <= idx);
    var sz = start;
    while (sz <= idx) : (sz += 1) {
        pages_free[sz] = page.?;
    }
}

pub fn mi_page_queue_remove(pq: *mi_page_queue_t, page: *mi_page_t) void {
    mi_assert_internal(mi_page_queue_contains(pq, page));
    mi_assert_internal(page.xblock_size == pq.block_size or (page.xblock_size > MI_MEDIUM_OBJ_SIZE_MAX and mi_page_queue_is_huge(pq)) or (mi_page_is_in_full(page) and mi_page_queue_is_full(pq)));
    const heap = page.heap().?;

    if (page.prev != null) page.prev.?.next = page.next;
    if (page.next != null) page.next.?.prev = page.prev;
    if (page == pq.last) pq.last = page.prev;
    if (page == pq.first) {
        pq.first = page.next;
        // update first
        mi_assert_internal(mi_heap_contains_queue(heap, pq));
        mi_heap_queue_first_update(heap, pq);
    }
    heap.page_count -= 1;
    page.next = null;
    page.prev = null;
    // mi_atomic_store_ptr_release(mi_atomic_cast(void*, &page.heap), NULL);
    page.set_in_full(false);
}

pub fn mi_page_queue_push(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t) void {
    mi_assert_internal(page.heap() == heap);
    mi_assert_internal(!mi_page_queue_contains(pq, page));

    mi_assert_internal(_mi_page_segment(page).kind != .MI_SEGMENT_HUGE);
    mi_assert_internal(page.xblock_size == pq.block_size or
        (page.xblock_size > MI_MEDIUM_OBJ_SIZE_MAX) or
        (mi_page_is_in_full(page) and mi_page_queue_is_full(pq)));

    mi_page_set_in_full(page, mi_page_queue_is_full(pq));
    // mi_atomic_store_ptr_release(mi_atomic_cast(void*, &page.heap), heap);
    page.next = pq.first;
    page.prev = null;
    if (pq.first != null) {
        assert(pq.first.?.prev == null);
        pq.first.?.prev = page;
        pq.first = page;
    } else {
        pq.first = page;
        pq.last = page;
    }

    // update direct
    mi_heap_queue_first_update(heap, pq);
    heap.page_count += 1;
}

pub fn mi_page_queue_enqueue_from(to: *mi_page_queue_t, from: *mi_page_queue_t, page: *mi_page_t) void {
    assert(from.contains(page));
    assert(!to.contains(page));

    assert((page.xblock_size == to.block_size and page.xblock_size == from.block_size) or
        (page.xblock_size == to.block_size and mi_page_queue_is_full(from)) or
        (page.xblock_size == from.block_size and mi_page_queue_is_full(to)) or
        (page.xblock_size > MI_LARGE_OBJ_SIZE_MAX and mi_page_queue_is_huge(from)) or
        (page.xblock_size > MI_LARGE_OBJ_SIZE_MAX and mi_page_queue_is_full(to)));

    const heap = page.heap().?;
    if (page.prev != null) page.prev.?.next = page.next;
    if (page.next != null) page.next.?.prev = page.prev;
    if (page == from.last) from.last = page.prev;
    if (page == from.first) {
        from.first = page.next;
        // update first
        assert(mi_heap_contains_queue(heap, from));
        mi_heap_queue_first_update(heap, from);
    }

    page.prev = to.last;
    page.next = null;
    if (to.last != null) {
        mi_assert_internal(heap == mi_page_heap(to.last.?).?);
        to.last.?.next = page;
        to.last = page;
    } else {
        to.first = page;
        to.last = page;
        mi_heap_queue_first_update(heap, to);
    }

    mi_page_set_in_full(page, mi_page_queue_is_full(to));
}

// Only called from `mi_heap_absorb`.
fn _mi_page_queue_append(heap: *mi_heap_t, pq: *mi_page_queue_t, append: *mi_page_queue_t) usize {
    assert(heap.contains(pq));
    assert(pq.block_size == append.block_size);

    if (append.first == null) return 0;

    // set append pages to new heap and count
    var count: usize = 0;
    var page = append.first;
    while (page != null) : (page = page.next) {
        // inline `mi_page_set_heap` to avoid wrong assertion during absorption;
        // in this case it is ok to be delayed freeing since both "to" and "from"
        // heap are still alive.
        page.xheap.store(heap, AtomicOrder.Release);
        // set the flag to delayed free (not overriding NEVER_DELAYED_FREE) which has as a
        // side effect that it spins until any DELAYED_FREEING is finished. This ensures
        // that after appending only the new heap will be used for delayed free operations.
        mi._mi_page_use_delayed_free(page, mi.USE_DELAYED_FREE, false);
        count += 1;
    }

    if (pq.last == null) {
        // take over afresh
        assert(pq.first == null);
        pq.first = append.first;
        pq.last = append.last;
        mi_heap_queue_first_update(heap, pq);
    } else {
        // append to end
        assert(pq.last != null);
        assert(append.first != null);
        pq.last.next = append.first;
        append.first.prev = pq.last;
        pq.last = append.last;
    }
    return count;
}
