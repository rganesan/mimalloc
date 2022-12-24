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
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("page-queue.zig");
};

fn noop(cond: bool) void {
    _ = cond;
}
const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else noop;

// alises to avoid clutter

// type aliases
const mi_heap_t = mi.heap_t;
const mi_tld_t = mi.tld_t;
const mi_page_t = mi.page_t;
const mi_page_queue_t = mi.page_queue_t;
const mi_block_t = mi.block_t;
const mi_delayed_t = mi.delayed_t;
const mi_thread_free_t = mi.thread_free_t;
const mi_stats_t = mi.stats_t;

// #defines
const MI_DEBUG = mi.DEBUG;
const MI_SECURE = mi.SECURE;

const PTRDIFF_MAX = mi.PTRDIFF_MAX;

const MI_INTPTR_SIZE = mi.INTPTR_SIZE;

const MI_BIN_FULL = mi.BIN_FULL;
const MI_BIN_HUGE = mi.BIN_HUGE;
const MI_SEGMENT_HUGE = mi.SEGMENT_HUGE;
const MI_PADDING_SIZE = mi.PADDING_SIZE;
const MI_MEDIUM_OBJ_SIZE_MAX = mi.MEDIUM_OBJ_SIZE_MAX;
const MI_SMALL_OBJ_SIZE_MAX = mi.SMALL_OBJ_SIZE_MAX;
const MI_LARGE_OBJ_SIZE_MAX = mi.LARGE_OBJ_SIZE_MAX;
const MI_DELAYED_FREEING = mi.DELAYED_FREEING;
const MI_NEVER_DELAYED_FREE = mi.NEVER_DELAYED_FREE;
const MI_HUGE_BLOCK_SIZE = mi.HUGE_BLOCK_SIZE;
const MI_SEGMENT_SLICE_SIZE = mi.MI_SEGMENT_SLICE_SIZE;

const MI_ENCODE_FREELIST = mi.ENCODE_FREELIST;

const _mi_stats_main = mi._mi_stats_main;

// Function aliases

const mi_thread_init = mi.mi_thread_init;
const _mi_ptr_page = mi._mi_ptr_page;
const _mi_process_is_initialized = mi._mi_process_is_initialized;
const mi_atomic_yield = mi.mi_atomic_yield;
const mi_mem_is_zero = mi.mi_mem_is_zero;

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

const mi_block_next = mi.mi_block_next;
const mi_block_nextx = mi.mi_block_nextx;
const mi_block_set_nextx = mi.mi_block_set_nextx;

const _mi_free_delayed_block = mi._mi_free_delayed_block;

const mi_heap_is_initialized = mi.mi_heap_is_initialized;
const mi_heap_page_queue_of = mi.mi_heap_page_queue_of;
const mi_heap_contains_queue = mi.mi_heap_contains_queue;
const _mi_heap_random_next = mi._mi_heap_random_next;
const mi_get_default_heap = mi.mi_get_default_heap;
const mi_heap_collect = mi.mi_heap_collect;

const _mi_random_shuffle = mi._mi_random_shuffle;

const _mi_os_good_alloc_size = mi._mi_os_good_alloc_size;

const mi_page_queue_of = mi.mi_page_queue_of;
const mi_page_thread_free = mi.mi_page_thread_free;
const mi_page_thread_free_flag = mi.mi_page_thread_free_flag;
const mi_page_block_size = mi.mi_page_block_size;
const mi_page_is_in_full = mi.mi_page_is_in_full;
const mi_page_usable_block_size = mi.mi_page_usable_block_size;
const mi_page_heap = mi.mi_page_heap;
const mi_page_has_aligned = mi.mi_page_has_aligned;
const mi_page_set_has_aligned = mi.mi_page_set_has_aligned;
const mi_page_immediate_available = mi.mi_page_immediate_available;
const _mi_page_segment = mi._mi_page_segment;
const _mi_page_start = mi._mi_page_start;
const _mi_page_malloc = mi._mi_page_malloc;

const _mi_memzero_aligned = mi._mi_memzero_aligned;

const mi_page_queue = mi.mi_page_queue;
const mi_page_queue_push = mi.mi_page_queue_push;
const mi_page_queue_remove = mi.mi_page_queue_remove;
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

const mi_stat_counter_increase = mi.mi_stat_counter_increase;
const mi_stat_increase = mi.mi_stat_increase;

const mi_track_mem_noaccess = mi.mi_track_mem_noaccess;

const mi_deferred_free_fun = mi.mi_deferred_free_fun;

const _mi_error_message = mi._mi_error_message;
const EFAULT = 0;
const ENOMEM = 0;
const EOVERFLOW = 0;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

//-----------------------------------------------------------
//  Page helpers
//-----------------------------------------------------------

// are all blocks in a page freed?
// note: needs up-to-date used count, (as the `xthread_free` list may not be empty). see `_mi_page_collect_free`.
pub inline fn mi_page_all_free(page: *const mi_page_t) bool {
    return (page.used == 0);
}

pub inline fn mi_page_set_heap(page: *mi_page_t, heap: *mi_heap_t) void {
    mi_assert_internal(mi_page_thread_free_flag(page) != MI_DELAYED_FREEING);
    mi_atomic_store_release(&page.xheap, heap);
}

// Index a block in a page
pub fn mi_page_block_at(page: *const mi_page_t, page_start: *const mi_page_t, block_size: usize, i: usize) *mi_block_t {
    mi_assert_internal(i <= page.reserved);
    return @intToPtr(*mi_block_t, @ptrToInt(page_start) + i * block_size);
}

pub inline fn mi_block_set_next(page: *const mi_page_t, block: *mi_block_t, next: *const mi_block_t) void {
    if (MI_ENCODE_FREELIST) {
        mi_block_set_nextx(page, block, next, page.keys);
    } else {
        mi_block_set_nextx(page, block, next, null);
    }
}

fn mi_page_list_count(page: *mi_page_t, head: *mi_block_t) usize {
    if (MI_DEBUG < 3) return 0;
    var count: usize = 0;
    var block: ?*mi_block_t = head;
    while (block != null) : (block = mi_block_next(page, block)) {
        mi_assert_internal(page == _mi_ptr_page(head));
        count += 1;
    }
    return count;
}

fn mi_page_list_is_valid(page: *mi_page_t, block: *mi_block_t) bool {
    if (MI_DEBUG < 3) return true;
    var psize: usize = undefined;
    const page_area = _mi_page_start(_mi_page_segment(page), page, &psize);
    const start = @ptrCast(*mi_block_t, page_area);
    const end = @ptrCast(*mi_block_t, page_area + psize);
    var p: ?*mi_block_t = block;
    while (p != null) : (p = mi_block_next(page, p)) {
        if (p < start or p >= end) return false;
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
    if (MI_SECURE) {
        mi_assert_internal(page.keys[0] != 0);
    }
    if (page.heap() != null) {
        const segment = _mi_page_segment(page);

        mi_assert_internal(!_mi_process_is_initialized or segment.thread_id == 0 or segment.thread_id == mi_page_heap(page).thread_id);
        if (segment.kind != MI_SEGMENT_HUGE) {
            const pq = mi_page_queue_of(page);
            mi_assert_internal(pq.contains(page));
            mi_assert_internal(pq.block_size == mi_page_block_size(page) or mi_page_block_size(page) > MI_MEDIUM_OBJ_SIZE_MAX or mi_page_is_in_full(page));
            mi_assert_internal(mi_heap_contains_queue(mi_page_heap(page), pq));
        }
    }
    return true;
}

fn _mi_page_use_delayed_free(page: *mi_page_t, delay: mi_delayed_t, override_never: bool) void {
    while (!_mi_page_try_use_delayed_free(page, delay, override_never)) {
        mi_atomic_yield();
    }
}

fn _mi_page_try_use_delayed_free(page: *mi_page_t, delay: mi_delayed_t, override_never: bool) bool {
    var tfreex: mi_thread_free_t = undefined;
    var old_delay: mi_delayed_t = MI_DELAYED_FREEING;
    var tfree: mi_thread_free_t = undefined;
    var yield_count: usize = 0;
    while ((old_delay == MI_DELAYED_FREEING) or
        !mi_atomic_cas_weak_release(&page.xthread_free, &tfree, tfreex))
    {
        tfree = mi_atomic_load_acquire(&page.xthread_free); // note: must acquire as we can break/repeat this loop and not do a CAS;
        tfreex = mi_tf_set_delayed(tfree, delay);
        old_delay = mi_tf_delayed(tfree);
        if (mi_unlikely(old_delay == MI_DELAYED_FREEING)) {
            if (yield_count >= 4) return false; // give up after 4 tries
            yield_count += 1;
            mi_atomic_yield(); // delay until outstanding MI_DELAYED_FREEING are done.
            // tfree = mi_tf_set_delayed(tfree, MI_NO_DELAYED_FREE); // will cause CAS to busy fail
        } else if (delay == old_delay) {
            break; // avoid atomic operation if already equal
        } else if (!override_never and old_delay == MI_NEVER_DELAYED_FREE) {
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
    var next: *mi_block_t = mi_block_next(page, tail);
    while (next != null and count <= max_count) : (next = mi_block_next(page, tail)) {
        count += 1;
        tail = next;
    }
    // if `count > max_count` there was a memory corruption (possibly infinite list due to double multi-threaded free)
    if (count > max_count) {
        _mi_error_message(EFAULT, "corrupted thread-free list\n");
        return; // the thread-free items cannot be freed
    }

    // and append the current local free list
    mi_block_set_next(page, tail, page.local_free);
    page.local_free = head;

    // update counts now
    page.used -= count;
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
            page.is_zero = false;
        } else if (force) {
            // append -- only on shutdown (force) as this is a linear operation
            var tail = page.local_free;
            var next = mi_block_next(page, tail);
            while (next != null) : (next = mi_block_next(page, tail)) {
                tail = next;
            }
            mi_block_set_next(page, tail, page.free);
            page.free = page.local_free;
            page.local_free = null;
            page.is_zero = false;
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
    mi_assert_internal(page.heap() == heap);
    mi_assert_internal(mi_page_thread_free_flag(page) != MI_NEVER_DELAYED_FREE);
    mi_assert_internal(_mi_page_segment(page).kind != MI_SEGMENT_HUGE);
    mi_assert_internal(!page.is_reset);
    // TODO: push on full queue immediately if it is full?
    const pq = mi_page_queue(heap, mi_page_block_size(page));
    mi_page_queue_push(heap, pq, page);
    mi_assert_expensive(_mi_page_is_valid(page));
}

// allocate a fresh page from a segment
fn mi_page_fresh_alloc(heap: *mi_heap_t, pq: ?*mi_page_queue_t, block_size: usize) ?*mi_page_t {
    mi_assert_internal(pq == null or mi_heap_contains_queue(heap, pq));
    var page = _mi_segment_page_alloc(heap, block_size, &heap.tld.segments, &heap.tld.os);
    if (page == null) {
        // this may be out-of-memory, or an abandoned page was reclaimed (and in our queue)
        return null;
    }
    mi_assert_internal(pq == null or _mi_page_segment(page).kind != MI_SEGMENT_HUGE);
    mi_page_init(heap, page, block_size, heap.tld);
    mi_stat_increase(heap, heap.tld.stats.page, 1);
    if (pq != null) mi_page_queue_push(heap, pq, page); // huge pages use pq==null
    mi_assert_expensive(_mi_page_is_valid(page));
    return page;
}

// Get a fresh page to use
fn mi_page_fresh(heap: *mi_heap_t, pq: *mi_page_queue_t) ?*mi_page_t {
    mi_assert_internal(heap.contains(pq));
    const page = mi_page_fresh_alloc(heap, pq, pq.block_size);
    if (page == null) return null;
    mi_assert_internal(pq.block_size == page.block_size());
    mi_assert_internal(pq == mi_page_queue(heap, page.block_size()));
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
        var next = mi_block_nextx(heap, block, heap.keys);
        // use internal free instead of regular one to keep stats etc correct
        if (!_mi_free_delayed_block(block)) {
            // we might already start delayed freeing while another thread has not yet
            // reset the delayed_freeing flag; in that case delay it further by reinserting the current block
            // into the delayed free list
            all_freed = false;
            var dfree = mi_atomic_load_ptr_relaxed(mi_block_t, &heap.thread_delayed_free);
            while (true) {
                mi_block_set_nextx(heap, block, dfree, heap.keys);
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
    mi_assert_internal(page.is_in_full());
    if (!page.is_in_full()) return;

    const heap = mi_page_heap(page);
    const pqfull = &heap.pages[MI_BIN_FULL];
    page.set_in_full(false); // to get the right queue
    const pq = mi_heap_page_queue_of(heap, page);
    page.set_in_full(true);
    mi_page_queue_enqueue_from(pq, pqfull, page);
}

fn mi_page_to_full(page: *mi_page_t, pq: *mi_page_queue_t) void {
    mi_assert_internal(pq == mi_page_queue_of(page));
    mi_assert_internal(!mi_page_immediate_available(page));
    mi_assert_internal(!page.is_in_full());

    if (mi_page_is_in_full(page)) return;
    mi_page_queue_enqueue_from(&mi_page_heap(page).pages[MI_BIN_FULL], pq, page);
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

    const pheap = mi_page_heap(page);

    // remove from our page list
    const segments_tld = &pheap.tld.segments;
    mi_page_queue_remove(pq, page);

    // page is no longer associated with our heap
    mi_assert_internal(mi_page_thread_free_flag(page) == MI_NEVER_DELAYED_FREE);
    mi_page_set_heap(page, null);

    if (MI_DEBUG > 1) {
        // check there are no references left..
        var block = pheap.thread_delayed_free;
        while (block != null) : (block = mi_block_nextx(pheap, block, pheap.keys)) {
            mi_assert_internal(_mi_ptr_page(block) != page);
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
    mi_assert_internal(mi_page_thread_free_flag(page) != MI_DELAYED_FREEING);

    // no more aligned blocks in here
    mi_page_set_has_aligned(page, false);

    const heap = mi_page_heap(page);

    // remove from the page list
    // (no need to do _mi_heap_delayed_free first as all blocks are already free)
    const segments_tld = &heap.tld.segments;
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
    if (mi_likely(page.xblock_size <= MI_MAX_RETIRE_SIZE and !mi_page_is_in_full(page))) {
        if (pq.last == page and pq.first == page) { // the only page in the queue?
            mi_stat_counter_increase(_mi_stats_main.page_no_retire, 1);
            page.retire_expire = 1 + if (page.xblock_size <= MI_SMALL_OBJ_SIZE_MAX) MI_RETIRE_CYCLES else MI_RETIRE_CYCLES / 4;
            const heap = mi_page_heap(page);
            mi_assert_internal(pq >= heap.pages);
            const index = pq - heap.pages;
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
    var min = MI_BIN_FULL;
    var max = 0;
    var bin = heap.page_retired_min;
    while (bin <= heap.page_retired_max) : (bin += 1) {
        const pq = &heap.pages[bin];
        var page = pq.first;
        if (page != null and page.retire_expire != 0) {
            if (mi_page_all_free(page)) {
                page.retire_expire -= 1;
                if (force or page.retire_expire == 0) {
                    _mi_page_free(pq.first, pq, force);
                } else {
                    // keep retired, update min/max
                    if (bin < min) min = bin;
                    if (bin > max) max = bin;
                }
            } else {
                page.retire_expire = 0;
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

fn mi_page_free_list_extend_secure(heap: *const mi_heap_t, page: *const mi_page_t, bsize: usize, extend: usize, stats: *const mi_stats_t) void {
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
    var shift: usize = MI_MAX_SLICE_SHIFT;
    while ((extend >> shift) == 0) {
        shift -= 1;
    }
    const slice_count = @intCast(usize, 1) << shift;
    const slice_extend = extend / slice_count;
    mi_assert_internal(slice_extend >= 1);
    var blocks: [MI_MAX_SLICES]mi_block_t = undefined; // current start of the slice
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
        var next = ((rnd >> 8 * round) & (slice_count - 1));
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

fn mi_page_free_list_extend(page: *const mi_page_t, bsize: usize, extend: usize, stats: *const mi_stats_t) void {
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
    while (block <= last) {
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
else 1;

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
    _mi_page_start(_mi_page_segment(page), page, &page_size);
    mi_stat_counter_increase(tld.stats.pages_extended, 1);

    // calculate the extend count
    const bsize = if (page.xblock_size < MI_HUGE_BLOCK_SIZE) page.xblock_size else page_size;
    const extend = page.reserved - page.capacity;
    mi_assert_internal(extend > 0);

    var max_extend = if (bsize >= MI_MAX_EXTEND_SIZE) MI_MIN_EXTEND else MI_MAX_EXTEND_SIZE / bsize;
    if (max_extend < MI_MIN_EXTEND) {
        max_extend = MI_MIN_EXTEND;
    }
    mi_assert_internal(max_extend > 0);

    if (extend > max_extend) {
        // ensure we don't touch memory beyond the page to reduce page commit.
        // the `lean` benchmark tests this. Going from 1 to 8 increases rss by 50%.
        extend = max_extend;
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
    mi_stat_increase(tld.stats.page_committed, extend * bsize);

    // extension into zero initialized memory preserves the zero'd free list
    if (!page.is_zero_init) {
        page.is_zero = false;
    }
    mi_assert_expensive(mi_page_is_valid_init(page));
}

// Initialize a fresh page
fn mi_page_init(heap: *mi_heap_t, page: *mi_page_t, block_size: usize, tld: *mi_tld_t) void {
    const segment = _mi_page_segment(page);
    mi_assert_internal(block_size > 0);
    // set fields
    mi_page_set_heap(page, heap);
    page.xblock_size = if (block_size < MI_HUGE_BLOCK_SIZE) block_size else MI_HUGE_BLOCK_SIZE; // initialize before _mi_segment_page_start
    var page_size: usize = undefined;
    const page_start = _mi_segment_page_start(segment, page, &page_size);
    mi_track_mem_noaccess(page_start, page_size);
    mi_assert_internal(mi_page_block_size(page) <= page_size);
    mi_assert_internal(page_size <= page.slice_count * MI_SEGMENT_SLICE_SIZE);
    mi_assert_internal(page_size / block_size < (1 << 16));
    page.reserved = (page_size / block_size);
    if (MI_ENCODE_FREELIST) {
        page.keys[0] = _mi_heap_random_next(heap);
        page.keys[1] = _mi_heap_random_next(heap);
    }
    if (MI_DEBUG > 0)
        page.is_zero = false // ensure in debug mode we initialize with MI_DEBUG_UNINIT, see issue #501
    else
        page.is_zero = page.is_zero_init;

    mi_assert_internal(page.is_committed);
    mi_assert_internal(!page.is_reset);
    mi_assert_internal(page.capacity == 0);
    mi_assert_internal(page.free == null);
    mi_assert_internal(page.used == 0);
    mi_assert_internal(page.xthread_free == 0);
    mi_assert_internal(page.next == null);
    mi_assert_internal(page.prev == null);
    mi_assert_internal(page.retire_expire == 0);
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
        const next = page.next; // remember next
        count += 1;

        // 0. collect freed blocks by us and other threads
        _mi_page_free_collect(page, false);

        // 1. if the page contains free blocks, we are done
        if (mi_page_immediate_available(page)) {
            break; // pick this one
        }

        // 2. Try to extend
        if (page.capacity < page.reserved) {
            mi_page_extend_free(heap, page, heap.tld);
            mi_assert_internal(mi_page_immediate_available(page));
            break;
        }

        // 3. If the page is completely full, move it to the `mi_pages_full`
        // queue so we don't visit long-lived pages too often.
        mi_assert_internal(!mi_page_is_in_full(page) and !mi_page_immediate_available(page));
        mi_page_to_full(page, pq);

        page = next;
    } // for each page

    mi_stat_counter_increase(heap, heap.tld.stats.searches, count);

    if (page == null) {
        _mi_heap_collect_retired(heap, false); // perhaps make a page available?
        page = mi_page_fresh(heap, pq);
        if (page == null and first_try) {
            // out-of-memory _or_ an abandoned page with free blocks was reclaimed, try once again
            page = mi_page_queue_find_free_ex(heap, pq, false);
        }
    } else {
        mi_assert(pq.first == page);
        page.retire_expire = 0;
    }
    mi_assert_internal(page == null or mi_page_immediate_available(page));
    return page;
}

// Find a page with free blocks of `size`.
fn mi_find_free_page(heap: *mi_heap_t, size: usize) *mi_page_t {
    const pq = mi_page_queue(heap, size);
    const page = pq.first;
    if (page != null) {
        if (MI_SECURE >= 3) { // in secure mode, we extend half the time to increase randomness
            if (page.capacity < page.reserved and ((_mi_heap_random_next(heap) & 1) == 1)) {
                mi_page_extend_free(heap, page, heap.tld);
                mi_assert_internal(mi_page_immediate_available(page));
            }
        } else {
            _mi_page_free_collect(page, false);
        }

        if (mi_page_immediate_available(page)) {
            page.retire_expire = 0;
            return page; // fast path
        }
    }
    return mi_page_queue_find_free_ex(heap, pq, true);
}

//-----------------------------------------------------------
// Users can register a deferred free function called
// when the `free` list is empty. Since the `local_free`
// is separate this is deterministically called after
// a certain number of allocations.
//-----------------------------------------------------------

var deferred_free: ?mi_deferred_free_fun = null;
var deferred_arg = Atomic(?*opaque {}).init(null);

pub fn _mi_deferred_free(heap: *mi_heap_t, force: bool) void {
    heap.tld.heartbeat += 1;
    if (deferred_free != null and !heap.tld.recurse) {
        heap.tld.recurse = true;
        deferred_free(force, heap.tld.heartbeat, mi_atomic_load_ptr_relaxed(void, &deferred_arg));
        heap.tld.recurse = false;
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
fn mi_large_huge_page_alloc(heap: *mi_heap_t, size: usize) *mi_page_t {
    const block_size: usize = _mi_os_good_alloc_size(size);
    mi_assert_internal(mi_bin(block_size) == MI_BIN_HUGE);
    const is_huge = (block_size > MI_LARGE_OBJ_SIZE_MAX);
    const pq = if (is_huge) null else mi_page_queue(heap, block_size);
    const page = mi_page_fresh_alloc(heap, pq, block_size);
    if (page != null) {
        mi_assert_internal(mi_page_immediate_available(page));

        if (pq == null) {
            // huge pages are directly abandoned
            mi_assert_internal(_mi_page_segment(page).kind == MI_SEGMENT_HUGE);
            mi_assert_internal(_mi_page_segment(page).used == 1);
            mi_assert_internal(_mi_page_segment(page).thread_id == 0); // abandoned, not in the huge queue
            mi_page_set_heap(page, null);
        } else {
            mi_assert_internal(_mi_page_segment(page).kind != MI_SEGMENT_HUGE);
        }

        const bsize = mi_page_usable_block_size(page); // note: not `mi_page_block_size` to account for padding
        if (bsize <= MI_LARGE_OBJ_SIZE_MAX) {
            mi_stat_increase(heap, heap.tld.stat.large, bsize);
            mi_stat_counter_increase(heap, heap.tld.stat.large_count, 1);
        } else {
            mi_stat_increase(heap, heap.tld.stat.huge, bsize);
            mi_stat_counter_increase(heap, heap.tld.stat.huge_count, 1);
        }
    }
    return page;
}

// Allocate a page
// Note: in debug mode the size includes MI_PADDING_SIZE and might have overflowed.
fn mi_find_page(heap: *mi_heap_t, size: usize) ?*mi_page_t {
    // huge allocation?
    const req_size = size - MI_PADDING_SIZE; // correct for padding_size in case of an overflow on `size`
    if (mi_unlikely(req_size > (MI_MEDIUM_OBJ_SIZE_MAX - MI_PADDING_SIZE))) {
        if (mi_unlikely(req_size > PTRDIFF_MAX)) { // we don't allocate more than PTRDIFF_MAX (see <https://sourceware.org/ml/libc-announce/2019/msg00001.html>)
            _mi_error_message(EOVERFLOW, "allocation request is too large (%zu bytes)\n", req_size);
            return null;
        } else {
            return mi_large_huge_page_alloc(heap, size);
        }
    } else {
        // otherwise find a page with free blocks in our size segregated queues
        mi_assert_internal(size >= MI_PADDING_SIZE);
        return mi_find_free_page(heap, size);
    }
}

// Generic allocation routine if the fast path (`alloc.c:mi_page_malloc`) does not succeed.
// Note: in debug mode the size includes MI_PADDING_SIZE and might have overflowed.
pub fn _mi_malloc_generic(heap: *mi_heap_t, size: usize, zero: bool) ?*opaque {} {
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
    _mi_heap_delayed_free_partial(heap);

    // find (or allocate) a page of the right size
    const page = mi_find_page(heap, size);
    if (mi_unlikely(page == null)) { // first time out of memory, try to collect and retry the allocation once more
        mi_heap_collect(heap, true); // force
        page = mi_find_page(heap, size);
    }

    if (mi_unlikely(page == null)) { // out of memory
        const req_size = size - MI_PADDING_SIZE; // correct for padding_size in case of an overflow on `size`
        _mi_error_message(ENOMEM, "unable to allocate memory (%zu bytes)\n", req_size);
        return null;
    }

    mi_assert_internal(mi_page_immediate_available(page));
    mi_assert_internal(mi_page_block_size(page) >= size);

    // and try again, this time succeeding! (i.e. this should never recurse through _mi_page_malloc)
    if (mi_unlikely(zero and page.xblock_size == 0)) {
        // note: we cannot call _mi_page_malloc with zeroing for huge blocks; we zero it afterwards in that case.
        const p = _mi_page_malloc(heap, page, size, false);
        mi_assert_internal(p != null);
        _mi_memzero_aligned(p, mi_page_usable_block_size(page));
        return p;
    } else {
        return _mi_page_malloc(heap, page, size, zero);
    }
}
