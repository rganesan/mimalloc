//-----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

const std = @import("std");
const Thread = std.Thread;
const mi = @import("types.zig");

const assert = std.debug.assert;

//- -----------------------------------------------------------
//  Helpers
//-------------------------------------------------------------

// return `true` if ok, `false` to break
const heap_page_visitor_fun = *const fn (heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, arg1: ?*opaque {}, arg2: ?*opaque {}) bool;

// Visit all pages in a heap; returns `false` if break was called.
fn mi_heap_visit_pages(heap: *mi.heap_t, visit_fn: heap_page_visitor_fun, arg1: ?*opaque {}, arg2: ?*opaque {}) bool {
    if (heap.page_count == 0) return 0;

    const total = heap.page_count;
    var count: usize = 0;
    var i: usize = 0;
    // visit all pages
    while (i <= mi.BIN_FULL) : (i += 1) {
        const pq = &heap.pages[i];
        var page = pq.first;
        while (page != null) {
            const next = page.next; // save next in case the page gets removed from the queue
            assert(mi_page_heap(page) == heap);
            count += 1;
            if (!visit_fn(heap, pq, page, arg1, arg2)) return false;
            page = next; // and continue
        }
    }
    if (mi.DEBUG > 1)
        assert(count == total);
    return true;
}

fn mi_heap_page_is_valid(heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, arg1: ?*opaque {}, arg2: ?*opaque {}) bool {
    if (mi.DEBUG < 2) return;
    assert(mi_page_heap(page) == heap);
    const segment = _mi_page_segment(page);
    assert(segment.thread_id == heap.thread_id);
    assert(_mi_page_is_valid(page));
    return true;
}

fn mi_heap_is_valid(heap: *mi.heap_t) bool {
    if (mi.DEBUG < 3) return;
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

fn mi_heap_page_collect(heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, args_collect: *opaque {}, arg2: ?*opaque {}) bool {
    assert(mi_heap_page_is_valid(heap, pq, page, null, null));
    const collect = @ptrCast(*mi_collect_t, args_collect);
    _mi_page_free_collect(page, collect >= .MI_FORCE);
    if (mi_page_all_free(page)) {
        // no more used blocks, free the page.
        // note: this will free retired pages as well.
        _mi_page_free(page, pq, collect >= .MI_FORCE);
    } else if (collect == .MI_ABANDON) {
        // still used blocks but the thread is done; abandon the page
        _mi_page_abandon(page, pq);
    }
    return true; // don't break
}

fn mi_heap_page_never_delayed_free(heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, arg1: ?*opaque {}, arg2: ?*opaque {}) bool {
    _mi_page_use_delayed_free(page, MI_NEVER_DELAYED_FREE, false);
    return true; // don't break
}

fn mi_heap_collect_ex(heap: *mi.heap_t, collect: mi_collect_t) void {
    if (!mi_heap_is_initialized(heap)) return;

    const force = collect >= .MI_FORCE;
    _mi_deferred_free(heap, force);

    // note: never reclaim on collect but leave it to threads that need storage to reclaim
    const force_main = _mi_is_main_thread() and mi_heap_is_backing(heap) and !heap.no_reclaim and if (NDEBUG) collect == .MI_FORCE else collect >= MI_FORCE;

    if (force_main) {
        // the main thread is abandoned (end-of-program), try to reclaim all abandoned segments.
        // if all memory is freed by now, all segments should be freed.
        _mi_abandoned_reclaim_all(heap, &heap.tld.segments);
    }

    // if abandoning, mark all pages to no longer add to delayed_free
    if (collect == .MI_ABANDON) {
        mi_heap_visit_pages(heap, &mi_heap_page_never_delayed_free, null, null);
    }

    // free all current thread delayed blocks.
    // (if abandoning, after this there are no more thread-delayed references into the pages.)
    _mi_heap_delayed_free_all(heap);

    // collect retired pages
    _mi_heap_collect_retired(heap, force);

    // collect all pages owned by this thread
    mi_heap_visit_pages(heap, &mi_heap_page_collect, &collect, null);
    assert(collect != .MI_ABANDON or @atomicLoad(mi_block_t, &heap.thread_delayed_free, builtin.Acquire) == null);

    // collect abandoned segments (in particular, decommit expired parts of segments in the abandoned segment list)
    // note: forced decommit can be quite expensive if many threads are created/destroyed so we do not force on abandonment
    _mi_abandoned_collect(heap, collect == MI_FORCE, &heap.tld.segments);

    // collect segment local caches
    if (force) {
        _mi_segment_thread_collect(&heap.tld.segments);
    }

    // decommit in global segment caches
    // note: forced decommit can be quite expensive if many threads are created/destroyed so we do not force on abandonment
    _mi_segment_cache_collect(collect == .MI_FORCE, &heap.tld.os);

    // collect regions on program-exit (or shared library unload)
    if (force and _mi_is_main_thread() and mi_heap_is_backing(heap)) {
        //_mi_mem_collect(&heap.tld.os);
    }
}

fn _mi_heap_collect_abandon(heap: *mi.heap_t) void {
    mi_heap_collect_ex(heap, .MI_ABANDON);
}

fn mi_heap_collect(heap: *mi.heap_t, force: bool) void {
    mi_heap_collect_ex(heap, if (force) ?.MI_FORCE else .MI_NORMAL);
}

fn mi_collect(force: bool) void {
    mi_heap_collect(mi_get_default_heap(), force);
}

//-------------------------------------------------------------
// Heap new
//-------------------------------------------------------------

pub fn mi_heap_get_default() *mi.heap_t {
    mi_thread_init();
    return mi_get_default_heap();
}

pub fn mi_heap_get_backing() *mi.heap_t {
    const heap = mi_heap_get_default();
    const bheap = heap.tld.heap_backing;
    assert(bheap.thread_id == Thread.currentId());
    return bheap;
}

pub fn mi_heap_new_in_arena(arena_id: mi.arena_id_t) *mi.heap_t {
    const bheap = mi_heap_get_backing();
    const heap = mi_heap_malloc_tp(bheap, mi.heap_t); // todo: OS allocate in secure mode?
    heap.tld = bheap.tld;
    heap.thread_id = Thread.currentId();
    heap.arena_id = arena_id;
    _mi_random_split(&bheap.random, &heap.random);
    heap.cookie = heap.random.int(u64) | 1;
    heap.keys[0] = heap.random.int(u64);
    heap.keys[1] = heap.random.int(u64);
    heap.no_reclaim = true; // don't reclaim abandoned pages or otherwise destroy is unsafe
    // push on the thread local heaps list
    heap.next = heap.tld.heaps;
    heap.tld.heaps = heap;
    return heap;
}

pub fn mi_heap_new() *mi.heap_t {
    return mi_heap_new_in_arena(mi.ARENA_ID_NONE);
}

fn _mi_heap_memid_is_suitable(heap: *mi.heap_t, memid: usize) bool {
    return _mi_arena_memid_is_suitable(memid, heap.arena_id);
}

// zero out the page queues
fn mi_heap_reset_pages(heap: *mi.heap_t) void {
    assert(mi_heap_is_initialized(heap));
    // TODO: copy full empty heap instead?
    memset(&heap.pages_free_direct, 0, sizeof(heap.pages_free_direct));
    if (MI_MEDIUM_DIRECT)
        memset(&heap.pages_free_medium, 0, sizeof(heap.pages_free_medium));
    _mi_memcpy_aligned(&heap.pages, &_mi_heap_empty.pages, sizeof(heap.pages));
    heap.thread_delayed_free = null;
    heap.page_count = 0;
}

// called from `mi_heap_destroy` and `mi_heap_delete` to free the internal heap resources.
fn mi_heap_free(heap: *mi.heap_t) void {
    assert(mi_heap_is_initialized(heap));
    if (mi_heap_is_backing(heap)) return; // dont free the backing heap

    // reset default
    if (mi_heap_is_default(heap)) {
        _mi_heap_set_default_direct(heap.tld.heap_backing);
    }

    // remove ourselves from the thread local heaps list
    // linear search but we expect the number of heaps to be relatively small
    var prev: ?*mi.heap_t = null;
    var curr = heap.tld.heaps;
    while (curr != heap and curr != null) {
        prev = curr;
        curr = curr.next;
    }
    assert(curr == heap);
    if (curr == heap) {
        if (prev != NULL) {
            prev.next = heap.next;
        } else {
            heap.tld.heaps = heap.next;
        }
    }
    assert(heap.tld.heaps != NULL);

    // and free the used memory
    mi_free(heap);
}

//-- -----------------------------------------------------------
// Heap destroy
//--------------------------------------------------------------

fn _mi_heap_page_destroy(heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, arg1: ?*opaque {}, arg2: ?*opaque {}) bool {
    // ensure no more thread_delayed_free will be added
    _mi_page_use_delayed_free(page, MI_NEVER_DELAYED_FREE, false);

    // stats
    const bsize = mi_page_block_size(page);
    if (bsize > MI_MEDIUM_OBJ_SIZE_MAX) {
        if (bsize <= MI_LARGE_OBJ_SIZE_MAX) {
            mi_heap_stat_decrease(heap, large, bsize);
        } else {
            mi_heap_stat_decrease(heap, huge, bsize);
        }
    }
    if (MI_STAT > 0) {
        _mi_page_free_collect(page, false); // update used count
        const inuse = page.used;
        if (bsize <= MI_LARGE_OBJ_SIZE_MAX) {
            mi_heap_stat_decrease(heap, normal, bsize * inuse);
            if (MI_STAT > 1)
                mi_heap_stat_decrease(heap, normal_bins[_mi_bin(bsize)], inuse);
        }
        mi_heap_stat_decrease(heap, malloc, bsize * inuse); // todo: off for aligned blocks...
    }

    // pretend it is all free now
    assert(mi_page_thread_free(page) == null);
    page.used = 0;

    // and free the page
    // mi_page_free(page,false);
    page.next = null;
    page.prev = null;
    // no force
    _mi_segment_page_free(page, false, &heap.tld.segments);

    return true; // keep going
}

fn _mi_heap_destroy_pages(heap: *mi.heap_t) void {
    mi_heap_visit_pages(heap, &_mi_heap_page_destroy, null, null);
    mi_heap_reset_pages(heap);
}

fn mi_heap_destroy(heap: *mi.heap_t) void {
    assert(mi_heap_is_initialized(heap));
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
fn mi_heap_absorb(heap: *mi.heap_t, from: *mi.heap_t) void {
    if (from.page_count == 0) return;

    // reduce the size of the delayed frees
    _mi_heap_delayed_free_partial(from);

    // transfer all pages by appending the queues; this will set a new heap field
    // so threads may do delayed frees in either heap for a while.
    // note: appending waits for each page to not be in the `MI_DELAYED_FREEING` state
    // so after this only the new heap will get delayed frees

    var i: usize = 0;
    while (i <= mi.BIN_FULL) : (i += 1) {
        const pq = &heap.pages[i];
        const append = &from.pages[i];
        const pcount = _mi_page_queue_append(heap, pq, append);
        heap.page_count += pcount;
        from.page_count -= pcount;
    }
    mi_assert(from.page_count == 0);

    // and do outstanding delayed frees in the `from` heap
    // note: be careful here as the `heap` field in all those pages no longer point to `from`,
    // turns out to be ok as `_mi_heap_delayed_free` only visits the list and calls a
    // the regular `_mi_free_delayed_block` which is safe.
    _mi_heap_delayed_free_all(from);
    assert(@atomicLoad(mi_block_t, &from.thread_delayed_free, builtin.Acquire) == null);

    // and reset the `from` heap
    mi_heap_reset_pages(from);
}

// Safe delete a heap without freeing any still allocated blocks in that heap.
fn mi_heap_delete(heap: *mi.heap_t) void {
    assert(mi_heap_is_initialized(heap));
    assert(mi_heap_is_valid(heap));

    if (!mi_heap_is_backing(heap)) {
        // tranfer still used pages to the backing heap
        mi_heap_absorb(heap.tld.heap_backing, heap);
    } else {
        // the backing heap abandons its pages
        _mi_heap_collect_abandon(heap);
    }
    mi_assert(heap.page_count == 0);
    mi_heap_free(heap);
}

fn mi_heap_set_default(heap: *mi.heap_t) *mi.heap_t {
    assert(mi_heap_is_initialized(heap));
    assert(mi_heap_is_valid(heap));
    old = mi_get_default_heap();
    _mi_heap_set_default_direct(heap);
    return old;
}

//-----------------------------------------------------------
// Analysis
//-----------------------------------------------------------

// private since it is not thread safe to access heaps from other threads.
fn mi_heap_of_block(p: *opaque {}) *mi.heap_t {
    const segment = _mi_ptr_segment(p);
    const valid = (_mi_ptr_cookie(segment) == segment.cookie);
    assert(valid);
    return mi_page_heap(_mi_segment_page_of(segment, p));
}

pub fn mi_heap_contains_block(heap: *mi.heap_t, p: *opaque {}) bool {
    if (!mi_heap_is_initialized(heap)) return false;
    return (heap == mi_heap_of_block(p));
}

fn mi_heap_page_check_owned(heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, p: *opaque {}, vfound: *opaque {}) bool {
    const found = @ptrCast(*bool, vfound);
    const segment = _mi_page_segment(page);
    const start = _mi_page_start(segment, page, null);
    const end = @ptrCast(*u8, start) + (page.capacity * mi_page_block_size(page));
    *found = (p >= start and p < end);
    return (!*found); // continue if not found
}

pub fn mi_heap_check_owned(heap: *mi.heap_t, p: *opaque {}) bool {
    if (!mi_heap_is_initialized(heap)) return false;
    if ((@ptrToInt(usize, p) & (MI_INTPTR_SIZE - 1)) != 0) return false; // only aligned pointers
    var found: bool = false;
    mi_heap_visit_pages(heap, &mi_heap_page_check_owned, p, &found);
    return found;
}

fn mi_check_owned(p: *opaque {}) bool {
    return mi_heap_check_owned(mi_get_default_heap(), p);
}

//-------------------------------------------------------------
//  Visit all heap blocks and areas
//  Todo: enable visiting abandoned pages, and
//        enable visiting all blocks of all heaps across threads
//-------------------------------------------------------------

// Separate struct to keep `mi_page_t` out of the public interface
const mi_heap_area_ex_t = struct {
    area: mi_heap_area_t,
    page: *mi.page_t,
};

fn mi_heap_area_visit_blocks(xarea: *const mi_heap_area_ex_t, visitor: mi_block_visit_fun, arg: *opaque {}) bool {
    const area = &xarea.area;
    const page = xarea.page;
    if (page == null) return true;

    _mi_page_free_collect(page, true);
    assert(page.local_free == null);
    if (page.used == 0) return true;

    const bsize = mi_page_block_size(page);
    const ubsize = mi_page_usable_block_size(page); // without padding
    var psize: usize;
    const pstart = _mi_page_start(_mi_page_segment(page), page, &psize);

    if (page.capacity == 1) {
        // optimize page with one block
        assert(page.used == 1 and page.free == null);
        return visitor(mi_page_heap(page), area, pstart, ubsize, arg);
    }

    // create a bitmap of free blocks.
    const MI_MAX_BLOCKS = (MI_SMALL_PAGE_SIZE / @sizeOf(usize));
    var free_map = std.mem.zeroes([MI_MAX_BLOCKS / @sizeOf(usize)]usize);

    var free_count: usize = 0;
    var block = page.free;
    while (block != null) : (block = mi_block_next(page, block)) {
        free_count += 1;
        assert(@intToPtr(block) >= pstart and (@intToPtr(block) < (pstart + psize)));
        const offset = @intToPtr(block) - pstart;
        assert(offset % bsize == 0);
        const blockidx = offset / bsize; // Todo: avoid division?
        assert(blockidx < MI_MAX_BLOCKS);
        const bitidx = (blockidx / sizeof(uintptr_t));
        const bit = blockidx - (bitidx * sizeof(uintptr_t));
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
        if (bit == 0 and m == UINTPTR_MAX) {
            i += (@sizeOf(usize) - 1); // skip a run of free blocks
        } else if ((m & (@intCast(usize, 1) << bit)) == 0) {
            used_count += 1;
            block = pstart + (i * bsize);
            if (!visitor(mi_page_heap(page), area, block, ubsize, arg)) return false;
        }
    }
    assert(page.used == used_count);
    return true;
}

const mi_heap_area_visit_fun = fn (heap: *const mi.heap_t, area: *const mi_heap_area_ex_t, arg: ?*opaque {}) bool;

fn mi_heap_visit_areas_page(heap: *mi.heap_t, pq: *mi.page_queue_t, page: *mi.page_t, vfun: *opaque {}, arg: *opaque {}) bool {
    const fun = @ptrCast(*mi_heap_area_visit_fun, vfun);
    var xarea: mi_heap_area_ex_t;
    const bsize = mi_page_block_size(page);
    const ubsize = mi_page_usable_block_size(page);
    xarea.page = page;
    xarea.area.reserved = page.reserved * bsize;
    xarea.area.committed = page.capacity * bsize;
    xarea.area.blocks = _mi_page_start(_mi_page_segment(page), page, null);
    xarea.area.used = page.used; // number of blocks in use (#553)
    xarea.area.block_size = ubsize;
    xarea.area.full_block_size = bsize;
    return fun(heap, &xarea, arg);
}

// Visit all heap pages as areas
fn mi_heap_visit_areas(heap: *const mi.heap_t, visitor: *mi_heap_area_visit_fun, arg: ?*opaque {}) bool {
    return mi_heap_visit_pages(heap, &mi_heap_visit_areas_page, @ptrCast(*opaque {}, visitor), arg); // note: function pointer to void* :-{
}

// Just to pass arguments
const mi_visit_blocks_args_t = struct {
    visit_blocks: bool,
    visitor: *mi_block_visit_fun,
    arg: *opaque {},
};

fn mi_heap_area_visitor(heap: *const mi.heap_t, xarea: *const mi_heap_area_ex_t, arg: *opaque {}) bool {
    const args = @ptrCast(*mi_visit_blocks_args_t, arg);
    if (!args.visitor(heap, &xarea.area, null, xarea.area.block_size, args.arg)) return false;
    if (args.visit_blocks) {
        return mi_heap_area_visit_blocks(xarea, args.visitor, args.arg);
    } else {
        return true;
    }
}

// Visit all blocks in a heap
fn mi_heap_visit_blocks(heap: *const mi.heap_t, visit_blocks: bool, visitor: *mi_block_visit_fun, arg: *opaque {}) bool {
    var args = mi_visit_block_args_t{ .visit_blocks = visit_blocks, .visitor = visitor, .arg = arg };
    return mi_heap_visit_areas(heap, &mi_heap_area_visitor, &args);
}
