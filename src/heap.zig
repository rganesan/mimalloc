//-----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

const std = @import("std");
const builtin = std.builtin;
const AtomicOrder = builtin.AtomicOrder;
const Thread = std.Thread;
const assert = std.debug.assert;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("page.zig");
    usingnamespace @import("stats.zig");
    usingnamespace @import("segment.zig");
    usingnamespace @import("segment-cache.zig");
    usingnamespace @import("options.zig");

    fn noop(cond: bool) void {
        _ = cond;
    }
};

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

// Types
const mi_heap_t = mi.mi_heap_t;
const mi_page_t = mi.mi_page_t;
const mi_page_queue_t = mi.mi_page_queue_t;

// #defines
const MI_DEBUG = mi.MI_DEBUG;
const MI_BIN_FULL = mi.MI_BIN_FULL;
const MI_PAGES_DIRECT = mi.MI_PAGES_DIRECT;
const MI_SMALL_SIZE_MAX = mi.MI_SMALL_SIZE_MAX;
const MI_PADDING_SIZE = mi.MI_PADDING_SIZE;

// Function aliases
const _mi_heap_set_default_direct = mi._mi_heap_set_default_direct;
const _mi_wsize_from_size = mi._mi_wsize_from_size;
const _mi_page_free_collect = mi._mi_page_free_collect;
const _mi_page_free = mi._mi_page_free;
const _mi_page_abandon = mi._mi_page_abandon;
const _mi_page_queue_append = mi._mi_page_queue_append;
const _mi_page_use_delayed_free = mi._mi_page_use_delayed_free;
const mi_page_all_free = mi.mi_page_all_free;
const _mi_deferred_free = mi._mi_deferred_free;
const _mi_heap_delayed_free_all = mi._mi_heap_delayed_free_all;
const _mi_heap_collect_retired = mi._mi_heap_collect_retired;
const _mi_abandoned_collect = mi._mi_abandoned_collect;
const _mi_segment_thread_collect = mi._mi_segment_thread_collect;
const _mi_segment_cache_collect = mi._mi_segment_cache_collect;
const _mi_segment_page_free = mi._mi_segment_page_free;
const _mi_heap_delayed_free_partial = mi._mi_heap_delayed_free_partial;

const _mi_abandoned_reclaim_all = mi._mi_abandoned_reclaim_all;

const _mi_stat_increase = mi._mi_stats_increase;
const _mi_stat_decrease = mi._mi_stats_decrease;

const NDEBUG = false;
const Arg1 = opaque {};
const Arg2 = opaque {};

// inlines from mimalloc-internal.h
pub inline fn mi_heap_is_initialized(heap: *mi_heap_t) bool {
    return (heap != &mi._mi_heap_empty);
}

pub inline fn _mi_heap_get_free_small_page(heap: *mi_heap_t, size: usize) *mi_page_t {
    mi_assert_internal(size <= (MI_SMALL_SIZE_MAX + MI_PADDING_SIZE));
    const idx = _mi_wsize_from_size(size);
    mi_assert_internal(idx < MI_PAGES_DIRECT);
    return heap.pages_free_direct[idx];
}

//- -----------------------------------------------------------
//  Helpers
//-------------------------------------------------------------

// return `true` if ok, `false` to break
const heap_page_visitor_fun = *const fn (heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, arg1: ?*const Arg1, arg2: ?*const Arg2) bool;

// Visit all pages in a heap; returns `false` if break was called.
fn mi_heap_visit_pages(heap: *mi_heap_t, visit_fn: heap_page_visitor_fun, arg1: ?*const Arg1, arg2: ?*const Arg2) bool {
    if (heap.page_count == 0) return true;

    const total = heap.page_count;
    var count: usize = 0;
    var i: usize = 0;
    // visit all pages
    while (i <= MI_BIN_FULL) : (i += 1) {
        const pq = &heap.pages[i];
        var page = pq.first;
        while (page != null) {
            const next = page.?.next; // save next in case the page gets removed from the queue
            assert(page.?.heap() == heap);
            count += 1;
            if (!visit_fn(heap, pq, page.?, arg1, arg2)) return false;
            page = next; // and continue
        }
    }
    if (MI_DEBUG > 1)
        assert(count == total);
    return true;
}

fn mi_heap_page_is_valid(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, arg1: ?*Arg1, arg2: ?*Arg2) bool {
    _ = pq;
    _ = arg1;
    _ = arg2;
    if (MI_DEBUG < 2) return;
    assert(page.heap() == heap);
    const segment = page.segment();
    assert(segment.thread_id.load(AtomicOrder.Monotonic) == heap.thread_id);
    assert(page.is_valid());
    return true;
}

fn mi_heap_is_valid(heap: *mi_heap_t) bool {
    if (MI_DEBUG < 3) return true;
    mi_heap_visit_pages(heap, mi_heap_page_is_valid, null, null);
    return true;
}

//- -----------------------------------------------------------
// "Collect" pages by migrating `local_free` and `thread_free`
// lists and freeing empty pages. This is done when a thread
// stops (and in that case abandons pages if there are still
// blocks alive)
//----------------------------------------------------------- */

const mi_collect_t = enum { MI_NORMAL, MI_FORCE, MI_ABANDON };

fn mi_heap_page_collect(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, args_collect: ?*const Arg1, arg2: ?*const Arg2) bool {
    _ = arg2;
    assert(mi_heap_page_is_valid(heap, pq, page, null, null));
    const collect = @ptrCast(*const mi_collect_t, args_collect).*;
    _mi_page_free_collect(page, @enumToInt(collect) >= @enumToInt(mi_collect_t.MI_FORCE));
    if (mi_page_all_free(page)) {
        // no more used blocks, free the page.
        // note: this will free retired pages as well.
        _mi_page_free(page, pq, @enumToInt(collect) >= @enumToInt(mi_collect_t.MI_FORCE));
    } else if (collect == .MI_ABANDON) {
        // still used blocks but the thread is done; abandon the page
        _mi_page_abandon(page, pq);
    }
    return true; // don't break
}

fn mi_heap_page_never_delayed_free(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, arg1: ?*const Arg1, arg2: ?*const Arg2) bool {
    _ = heap;
    _ = pq;
    _ = arg1;
    _ = arg2;
    _mi_page_use_delayed_free(page, .MI_NEVER_DELAYED_FREE, false);
    return true; // don't break
}

fn mi_heap_collect_ex(heap: *mi_heap_t, collect: mi_collect_t) void {
    if (!heap.is_initialized()) return;

    const force = @enumToInt(collect) >= @enumToInt(mi_collect_t.MI_FORCE);
    _mi_deferred_free(heap, force);

    // note: never reclaim on collect but leave it to threads that need storage to reclaim
    const force_main = mi._mi_is_main_thread() and heap.is_backing() and !heap.no_reclaim and if (NDEBUG) collect == .MI_FORCE else force;

    if (force_main) {
        // the main thread is abandoned (end-of-program), try to reclaim all abandoned segments.
        // if all memory is freed by now, all segments should be freed.
        _mi_abandoned_reclaim_all(heap, &heap.tld.?.segments);
    }

    // if abandoning, mark all pages to no longer add to delayed_free
    if (collect == .MI_ABANDON) {
        _ = mi_heap_visit_pages(heap, mi_heap_page_never_delayed_free, null, null);
    }

    // free all current thread delayed blocks.
    // (if abandoning, after this there are no more thread-delayed references into the pages.)
    _mi_heap_delayed_free_all(heap);

    // collect retired pages
    _mi_heap_collect_retired(heap, force);

    // collect all pages owned by this thread
    _ = mi_heap_visit_pages(heap, mi_heap_page_collect, @ptrCast(*const Arg1, &collect), null);
    assert(collect != .MI_ABANDON or heap.thread_delayed_free.load(builtin.AtomicOrder.Acquire) == null);

    // collect abandoned segments (in particular, decommit expired parts of segments in the abandoned segment list)
    // note: forced decommit can be quite expensive if many threads are created/destroyed so we do not force on abandonment
    _mi_abandoned_collect(heap, collect == .MI_FORCE, &heap.tld.?.segments);

    // collect segment local caches
    if (force) {
        _mi_segment_thread_collect(&heap.tld.?.segments);
    }

    // decommit in global segment caches
    // note: forced decommit can be quite expensive if many threads are created/destroyed so we do not force on abandonment
    _mi_segment_cache_collect(collect == .MI_FORCE, &heap.tld.?.os);

    // collect regions on program-exit (or shared library unload)
    if (force and mi._mi_is_main_thread() and heap.is_backing()) {
        //  _mi_mem_collect(&heap.tld.os);
    }
}

fn _mi_heap_collect_abandon(heap: *mi_heap_t) void {
    mi_heap_collect_ex(heap, .MI_ABANDON);
}

pub fn mi_heap_collect(heap: *mi_heap_t, force: bool) void {
    mi_heap_collect_ex(heap, if (force) .MI_FORCE else .MI_NORMAL);
}

pub fn mi_collect(force: bool) void {
    mi_heap_collect(mi.mi_get_default_heap(), force);
}

//-------------------------------------------------------------
// Heap new
//-------------------------------------------------------------

pub fn mi_heap_get_default() *mi_heap_t {
    mi.mi_thread_init();
    return mi.mi_get_default_heap();
}

pub fn mi_heap_get_backing() *mi_heap_t {
    const heap = mi_heap_get_default();
    const bheap = heap.tld.heap_backing;
    assert(bheap.thread_id == mi._mi_thread_id());
    return bheap;
}

pub fn mi_heap_new_in_arena(arena_id: mi.arena_id_t) *mi_heap_t {
    const bheap = mi_heap_get_backing();
    const heap = mi.mi_heap_malloc_tp(bheap, mi_heap_t); // todo: OS allocate in secure mode?
    heap.tld = bheap.tld;
    heap.thread_id = Thread.currentId();
    heap.arena_id = arena_id;
    heap.random = std.rand.DefaultPrng.init(std.crypto.random.int(u64));
    const random = heap.random.random();
    heap.cookie = random.int(u64) | 1;
    heap.keys[0] = random.int(u64);
    heap.keys[1] = random.int(u64);
    heap.no_reclaim = true; // don't reclaim abandoned pages or otherwise destroy is unsafe
    // push on the thread local heaps list
    heap.next = heap.tld.heaps;
    heap.tld.heaps = heap;
    return heap;
}

pub fn mi_heap_new() *mi_heap_t {
    return mi_heap_new_in_arena(mi.ARENA_ID_NONE);
}

pub fn _mi_heap_memid_is_suitable(heap: *mi_heap_t, memid: usize) bool {
    return mi._mi_arena_memid_is_suitable(memid, heap.arena_id);
}

pub fn _mi_heap_random_next(heap: *mi_heap_t) usize {
    return heap.random.random().int(usize);
}

// zero out the page queues
fn mi_heap_reset_pages(heap: *mi_heap_t) void {
    assert(heap.is_initialized());
    for (heap.pages_free_direct) |_, i| {
        heap.pages_free_direct[i] = &mi._mi_page_empty;
    }
    heap.pages = mi._mi_heap_empty.pages;
    heap.thread_delayed_free.store(null, AtomicOrder.Monotonic); // TODO: Check order
    heap.page_count = 0;
}

// called from `mi_heap_destroy` and `mi_heap_delete` to free the internal heap resources.
fn mi_heap_free(heap: *mi_heap_t) void {
    assert(heap.is_initialized());
    if (heap.is_backing()) return; // dont free the backing heap

    // reset default
    if (heap.is_default()) {
        _mi_heap_set_default_direct(heap.tld.?.heap_backing.?);
    }

    // remove ourselves from the thread local heaps list
    // linear search but we expect the number of heaps to be relatively small
    var prev: ?*mi_heap_t = null;
    var curr = heap.tld.?.heaps;
    while (curr != heap and curr != null) {
        prev = curr;
        curr = curr.?.next;
    }
    assert(curr == heap);
    if (curr == heap) {
        if (prev != null) {
            prev.?.next = heap.next;
        } else {
            heap.tld.?.heaps = heap.next;
        }
    }
    assert(heap.tld.?.heaps != null);

    // and free the used memory
    mi.mi_free(heap);
}

//-- -----------------------------------------------------------
// Heap destroy
//--------------------------------------------------------------

fn _mi_heap_page_destroy(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, arg1: ?*const Arg1, arg2: ?*const Arg2) bool {
    _ = pq;
    _ = arg1;
    _ = arg2;
    // ensure no more thread_delayed_free will be added
    _mi_page_use_delayed_free(page, mi.NEVER_DELAYED_FREE, false);

    // stats
    const bsize = page.block_size();
    if (bsize > mi.MI_MEDIUM_OBJ_SIZE_MAX) {
        if (bsize <= mi.MI_LARGE_OBJ_SIZE_MAX) {
            mi._mi_stat_decrease(&heap.tld.?.stats.large, bsize);
        } else {
            mi._mi_stat_decrease(&heap.tld.?.stats.huge, bsize);
        }
    }
    if (mi.MI_STAT > 0) {
        _mi_page_free_collect(page, false); // update used count
        const inuse = page.used;
        if (bsize <= mi.MI_LARGE_OBJ_SIZE_MAX) {
            mi._mi_stat_decrease(&heap.tld.?.stats.normal, bsize * inuse);
            if (mi.MI_STAT > 1)
                _mi_stat_decrease(&heap.tld.?.stats.normal_bins[mi._mi_bin(bsize)], inuse);
            _mi_stat_decrease(&heap.tld.?.stats.normal_bins[0], inuse);
        }
        _mi_stat_decrease(&heap.tld.?.stats.malloc, bsize * inuse); // todo: off for aligned blocks...
    }

    // pretend it is all free now
    assert(page.thread_free() == null);
    page.used = 0;

    // and free the page
    // mi_page_free(page,false);
    page.next = null;
    page.prev = null;
    // no force
    _mi_segment_page_free(page, false, &heap.tld.segments);

    return true; // keep going
}

fn _mi_heap_destroy_pages(heap: *mi_heap_t) void {
    _ = mi_heap_visit_pages(heap, _mi_heap_page_destroy, null, null);
    mi_heap_reset_pages(heap);
}

pub fn mi_heap_destroy(heap: *mi_heap_t) void {
    assert(heap.is_initialized());
    assert(heap.no_reclaim);
    assert(mi_heap_is_valid(heap));
    if (!heap.no_reclaim) {
        // don't free in case it may contain reclaimed pages
        mi_heap_delete(heap);
    } else {
        // free all pages
        _mi_heap_destroy_pages(heap);
        mi_heap_free(heap);
    }
}

//--------------------------------------------------------------
// Safe Heap delete
//--------------------------------------------------------------

// Transfer the pages from one heap to the other
fn mi_heap_absorb(heap: *mi_heap_t, from: *mi_heap_t) void {
    if (from.page_count == 0) return;

    // reduce the size of the delayed frees
    _mi_heap_delayed_free_partial(from);

    // transfer all pages by appending the queues; this will set a new heap field
    // so threads may do delayed frees in either heap for a while.
    // note: appending waits for each page to not be in the `MI_DELAYED_FREEING` state
    // so after this only the new heap will get delayed frees

    var i: usize = 0;
    while (i <= MI_BIN_FULL) : (i += 1) {
        const pq = &heap.pages[i];
        const append = &from.pages[i];
        const pcount = _mi_page_queue_append(heap, pq, append);
        heap.page_count += pcount;
        from.page_count -= pcount;
    }
    assert(from.page_count == 0);

    // and do outstanding delayed frees in the `from` heap
    // note: be careful here as the `heap` field in all those pages no longer point to `from`,
    // turns out to be ok as `_mi_heap_delayed_free` only visits the list and calls a
    // the regular `_mi_free_delayed_block` which is safe.
    _mi_heap_delayed_free_all(from);
    assert(from.thread_delayed_free.load(AtomicOrder.Acquire) == null);

    // and reset the `from` heap
    mi_heap_reset_pages(from);
}

// Safe delete a heap without freeing any still allocated blocks in that heap.
fn mi_heap_delete(heap: *mi_heap_t) void {
    assert(heap.is_initialized());
    assert(mi_heap_is_valid(heap));

    if (!heap.is_backing()) {
        // transfer still used pages to the backing heap
        mi_heap_absorb(heap.tld.?.heap_backing.?, heap);
    } else {
        // the backing heap abandons its pages
        _mi_heap_collect_abandon(heap);
    }
    assert(heap.page_count == 0);
    mi_heap_free(heap);
}

fn mi_heap_set_default(heap: *mi_heap_t) *mi_heap_t {
    assert(mi.mi_heap_is_initialized(heap));
    assert(mi_heap_is_valid(heap));
    const old = mi.mi_get_default_heap();
    mi._mi_heap_set_default_direct(heap);
    return old;
}

//-----------------------------------------------------------
// Analysis
//-----------------------------------------------------------

// private since it is not thread safe to access heaps from other threads.
fn mi_heap_of_block(p: *opaque {}) *mi_heap_t {
    const segment = mi._mi_ptr_segment(p);
    const valid = (mi._mi_ptr_cookie(segment) == segment.cookie);
    assert(valid);
    return mi.mi_page_heap(mi._mi_segment_page_of(segment, p));
}

pub fn mi_heap_contains_block(heap: *mi_heap_t, p: *opaque {}) bool {
    if (!mi.mi_heap_is_initialized(heap)) return false;
    return (heap == mi_heap_of_block(p));
}

fn mi_heap_page_check_owned(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, p: *opaque {}, vfound: *opaque {}) bool {
    _ = heap;
    _ = pq;
    const found = @ptrCast(*bool, vfound);
    const segment = mi._mi_page_segment(page);
    const start = mi._mi_page_start(segment, page, null);
    const end = @ptrCast(*u8, start) + (page.capacity * mi.mi_page_block_size(page));
    found.* = (p >= start and p < end);
    return (!found.*); // continue if not found
}

pub fn mi_heap_check_owned(heap: *mi_heap_t, p: *opaque {}) bool {
    if (!mi.mi_heap_is_initialized(heap)) return false;
    if ((@ptrToInt(p) & (mi.INTPTR_SIZE - 1)) != 0) return false; // only aligned pointers
    var found: bool = false;
    mi_heap_visit_pages(heap, mi_heap_page_check_owned, p, &found);
    return found;
}

fn mi_check_owned(p: *opaque {}) bool {
    return mi_heap_check_owned(mi.mi_get_default_heap(), p);
}

//-------------------------------------------------------------
//  Visit all heap blocks and areas
//  Todo: enable visiting abandoned pages, and
//        enable visiting all blocks of all heaps across threads
//-------------------------------------------------------------

// Separate struct to keep `mi_page_t` out of the public interface
const mi_heap_area_ex_t = struct {
    area: mi.mi_heap_area_t,
    page: *mi_page_t,
};

fn mi_heap_area_visit_blocks(xarea: *const mi_heap_area_ex_t, visitor: mi.mi_block_visit_fun, arg: *opaque {}) bool {
    const area = &xarea.area;
    const page = xarea.page;
    if (page == null) return true;

    mi._mi_page_free_collect(page, true);
    assert(page.local_free == null);
    if (page.used == 0) return true;

    const bsize = mi.mi_page_block_size(page);
    const ubsize = mi.mi_page_usable_block_size(page); // without padding
    var psize: usize = undefined;
    const pstart = mi._mi_page_start(mi._mi_page_segment(page), page, &psize);

    if (page.capacity == 1) {
        // optimize page with one block
        assert(page.used == 1 and page.free == null);
        return visitor(mi.mi_page_heap(page), area, pstart, ubsize, arg);
    }

    // create a bitmap of free blocks.
    const MI_MAX_BLOCKS = (mi.SMALL_PAGE_SIZE / @sizeOf(usize));
    var free_map = std.mem.zeroes([mi.MAX_BLOCKS / @sizeOf(usize)]usize);

    var free_count: usize = 0;
    var block = page.free;
    while (block != null) : (block = mi.mi_block_next(page, block)) {
        free_count += 1;
        assert(@intToPtr(*u8, block) >= pstart and (@intToPtr(*u8, block) < (pstart + psize)));
        const offset = @intToPtr(*u8, block) - pstart;
        assert(offset % bsize == 0);
        const blockidx = offset / bsize; // Todo: avoid division?
        assert(blockidx < MI_MAX_BLOCKS);
        const bitidx = (blockidx / @sizeOf(usize));
        const bit = blockidx - (bitidx * @sizeOf(usize));
        free_map[bitidx] |= (@intCast(usize, 1) << bit);
    }
    assert(page.capacity == (free_count + page.used));

    // walk through all blocks skipping the free ones
    var used_count: usize = 0;
    var i: usize = 0;
    while (i < page.capacity) : (i += 1) {
        const bitidx = (i / @sizeOf(usize));
        const bit = i - (bitidx * @sizeOf(usize));
        const m = free_map[bitidx];
        if (bit == 0 and m == std.math.maxInt(usize)) {
            i += (@sizeOf(usize) - 1); // skip a run of free blocks
        } else if ((m & (@intCast(usize, 1) << bit)) == 0) {
            used_count += 1;
            block = pstart + (i * bsize);
            if (!visitor(mi.mi_page_heap(page), area, block, ubsize, arg)) return false;
        }
    }
    assert(page.used == used_count);
    return true;
}

const mi_heap_area_visit_fun = *const fn (heap: *const mi_heap_t, area: *const mi_heap_area_ex_t, arg: ?*opaque {}) bool;

fn mi_heap_visit_areas_page(heap: *mi_heap_t, pq: *mi_page_queue_t, page: *mi_page_t, vfun: *opaque {}, arg: *opaque {}) bool {
    _ = pq;
    const fun = @ptrCast(mi_heap_area_visit_fun, vfun);
    var xarea: mi.mi_heap_area_ex_t = .{};
    const bsize = mi.mi_page_block_size(page);
    const ubsize = mi.mi_page_usable_block_size(page);
    xarea.page = page;
    xarea.area.reserved = page.reserved * bsize;
    xarea.area.committed = page.capacity * bsize;
    xarea.area.blocks = mi._mi_page_start(mi._mi_page_segment(page), page, null);
    xarea.area.used = page.used; // number of blocks in use (#553)
    xarea.area.block_size = ubsize;
    xarea.area.full_block_size = bsize;
    return fun(heap, &xarea, arg);
}

// Visit all heap pages as areas
fn mi_heap_visit_areas(heap: *const mi_heap_t, visitor: mi_heap_area_visit_fun, arg: ?*opaque {}) bool {
    return mi_heap_visit_pages(heap, mi_heap_visit_areas_page, @ptrCast(*opaque {}, visitor), arg); // note: function pointer to void* :-{
}

// Just to pass arguments
const mi_visit_blocks_args_t = struct {
    visit_blocks: bool,
    visitor: mi.mi_block_visit_fun,
    arg: *opaque {},
};

fn mi_heap_area_visitor(heap: *const mi_heap_t, xarea: *const mi_heap_area_ex_t, arg: *opaque {}) bool {
    const args = @ptrCast(*mi_visit_blocks_args_t, arg);
    if (!args.visitor(heap, &xarea.area, null, xarea.area.block_size, args.arg)) return false;
    if (args.visit_blocks) {
        return mi_heap_area_visit_blocks(xarea, args.visitor, args.arg);
    } else {
        return true;
    }
}

// Visit all blocks in a heap
fn mi_heap_visit_blocks(heap: *const mi_heap_t, visit_blocks: bool, visitor: mi.mi_block_visit_fun, arg: ?*opaque {}) bool {
    var args = mi.mi_visit_block_args_t{ .visit_blocks = visit_blocks, .visitor = visitor, .arg = arg };
    return mi_heap_visit_areas(heap, &mi_heap_area_visitor, &args);
}
