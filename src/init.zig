// ----------------------------------------------------------------------------
// Copyright (c) 2018-2022, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
// -----------------------------------------------------------------------------*/

const std = @import("std");
const builtin = std.builtin;
const AtomicOrder = builtin.AtomicOrder;
const AtomicRmwOp = builtin.AtomicRmwOp;
const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("heap.zig");
    usingnamespace @import("stats.zig");
};
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const Thread = std.Thread;
const Prng = std.rand.DefaultPrng;
const crandom = std.crypto.random;

// Empty page used to initialize the small free pages array
const _page_empty: mi.page_t = .{};

const SMALL_PAGES_EMPTY = [1]*mi.page_t{&_page_empty} ** if (mi.PADDING > 0 and mi.INTPTR_SIZE >= 8) 130 else if (mi.PADDING > 0) 131 else 129;

// Empty page queues for every bin
fn QNULL(comptime sz: usize) mi.page_queue_t {
    return .{ .block_size = sz * @sizeOf(usize) };
}

const PAGE_QUEUES_EMPTY = [_]mi.page_queue_t{
    QNULL(1),
    QNULL(1), QNULL(2), QNULL(3), QNULL(4), QNULL(5), QNULL(6), QNULL(7), QNULL(8), // 8
    QNULL(10), QNULL(12), QNULL(14), QNULL(16), QNULL(20), QNULL(24), QNULL(28), QNULL(32), // 16
    QNULL(40), QNULL(48), QNULL(56), QNULL(64), QNULL(80), QNULL(96), QNULL(112), QNULL(128), // 24
    QNULL(160), QNULL(192), QNULL(224), QNULL(256), QNULL(320), QNULL(384), QNULL(448), QNULL(512), // 32
    QNULL(640), QNULL(768), QNULL(896), QNULL(1024), QNULL(1280), QNULL(1536), QNULL(1792), QNULL(2048), // 40
    QNULL(2560), QNULL(3072), QNULL(3584), QNULL(4096), QNULL(5120), QNULL(6144), QNULL(7168), QNULL(8192), // 48
    QNULL(10240), QNULL(12288), QNULL(14336), QNULL(16384), QNULL(20480), QNULL(24576), QNULL(28672), QNULL(32768), // 56
    QNULL(40960), QNULL(49152), QNULL(57344), QNULL(65536), QNULL(81920), QNULL(98304), QNULL(114688), QNULL(131072), // 64
    QNULL(163840), QNULL(196608), QNULL(229376), QNULL(262144), QNULL(327680), QNULL(393216), QNULL(458752), QNULL(524288), // 72
    QNULL(mi.MEDIUM_OBJ_WSIZE_MAX + 1), // 655360, Huge queue
    QNULL(mi.MEDIUM_OBJ_WSIZE_MAX + 2), // full queue
};

// Empty slice span queues for every bin
fn SQNULL(comptime sz: usize) mi.span_queue_t {
    return .{ .slice_count = sz };
}

const SEGMENT_SPAN_QUEUES_EMPTY = [_]mi.span_queue_t{
    SQNULL(1),
    SQNULL(1), SQNULL(2), SQNULL(3), SQNULL(4), SQNULL(5), SQNULL(6), SQNULL(7), SQNULL(10), // 8
    SQNULL(12), SQNULL(14), SQNULL(16), SQNULL(20), SQNULL(24), SQNULL(28), SQNULL(32), SQNULL(40), // 16
    SQNULL(48), SQNULL(56), SQNULL(64), SQNULL(80), SQNULL(96), SQNULL(112), SQNULL(128), SQNULL(160), // 24
    SQNULL(192), SQNULL(224), SQNULL(256),  SQNULL(320), SQNULL(384), SQNULL(448), SQNULL(512), SQNULL(640), // 32
    SQNULL(768), SQNULL(896), SQNULL(1024),
}; // 35

// --------------------------------------------------------
// Statically allocate an empty heap as the initial
// thread local value for the default heap,
// and statically allocate the backing heap for the main
// thread so it can function without doing any allocation
// itself (as accessing a thread local for the first time
// may lead to allocation itself on some platforms)
// --------------------------------------------------------

// Should have been const but zig does not allow casting away const
pub var _heap_empty: mi.heap_t = .{ .random = undefined };

// the thread-local default heap for allocation
threadlocal var _heap_default: *mi.heap_t = &_heap_empty;

pub fn get_default_heap() *mi.heap_t {
    return _heap_default;
}

const tld_empty = mi.tld_t{
    .segments = .{ .spans = SEGMENT_SPAN_QUEUES_EMPTY },
};

var tld_main = mi.tld_t{
    .heap_backing = &_heap_main,
    .heaps = &_heap_main,
    //    .segments = .{ .spans = SEGMENT_SPAN_QUEUES_EMPTY, .stats = &tld_main.stats, .os = &tld_main.os }
    .segments = .{ .spans = SEGMENT_SPAN_QUEUES_EMPTY },
    // .os = .{ .stats = &tld_main.stats }, // os
};

var _heap_main = mi.heap_t{
    .pages = PAGE_QUEUES_EMPTY,
    .random = undefined, // TODO
};

var _process_is_initialized = false; // set to `true` in `process_init`.

pub var _stats_main = mi.stats_t{};

fn heap_main_init() void {
    if (_heap_main.cookie == 0) {
        _heap_main.tld = &tld_main;
        _heap_main.thread_id = Thread.getCurrentId();
        var rng = Prng.init(crandom.int(u64));
        _heap_main.random = rng.random();
        _heap_main.cookie = _heap_main.random.int(u64);
        _heap_main.keys[0] = _heap_main.random.int(u64);
        _heap_main.keys[1] = _heap_main.random.int(u64);
    }
}

fn _heap_main_get() *mi.heap_t {
    heap_main_init();
    return &_heap_main;
}

// -----------------------------------------------------------
//  Initialization and freeing of the thread local heaps
//-----------------------------------------------------------

// note: in x64 in release build `sizeof(thread_data_t)` is under 4KiB (= OS page size).
const thread_data_t = struct {
    heap: mi.heap_t, // must come first due to cast in `_heap_done`
    tld: mi.tld_t,
};

// Thread meta-data is allocated directly from the OS. For
// some programs that do not use thread pools and allocate and
// destroy many OS threads, this may causes too much overhead
// per thread so we maintain a small cache of recently freed metadata.

const TD_CACHE_SIZE = 8;
var td_cache = [_]?*thread_data_t{null} ** TD_CACHE_SIZE;

fn thread_data_alloc() *thread_data_t {
    // try to find thread metadata in the cache
    var td: ?*thread_data_t = null;
    var i: usize = 0;
    while (i < TD_CACHE_SIZE) : (i += 1) {
        td = @atomicLoad(?*thread_data_t, &td_cache[i], AtomicOrder.Monotonic);
        if (td != null) {
            td = @atomicRmw(?*thread_data_t, &td_cache[i], AtomicRmwOp.Xchg, null, AtomicOrder.Monotonic);
            if (td != null) {
                return td.?;
            }
        }
    }
    // if that fails, allocate directly from the OS
    const page_allocator = std.heap.page_allocator;
    td = page_allocator.create(thread_data_t) catch null;
    if (td == null) {
        // if this fails, try once more. (issue #257)
        td = page_allocator.create(thread_data_t) catch unreachable;
    }
    return td.?;
}

fn thread_data_free(tdfree: *thread_data_t) void {
    // try to add the thread metadata to the cache
    var i: usize = 0;
    while (i < TD_CACHE_SIZE) : (i += 1) {
        var td: ?*thread_data_t = @atomicLoad(thread_data_t, &td_cache[i], builtin.Monotonic);
        if (td == null) {
            const prev = @atomicRmw(thread_data_t, &td_cache[i], builtin.Xchg, tdfree, builtin.Monotonic);
            if (prev == null) {
                return;
            }
        }
    }
    // if that fails, just free it directly
    const page_allocator = std.heap.page_allocator;
    var allocator = page_allocator.allocator();
    allocator.destory(tdfree);
}

fn thread_data_collect() void {
    // free all thread metadata from the cache
    var i: usize = 0;
    while (i < TD_CACHE_SIZE) : (i += 1) {
        var td: ?*thread_data_t = @atomicLoad(thread_data_t, &td_cache[i], builtin.Monotonic);
        if (td != null) {
            td = @atomicRmw(thread_data_t, &td_cache[i], builtin.Xchg, null, builtin.Monotonic);
            if (td != null) {
                const page_allocator = std.heap.page_allocator;
                var allocator = page_allocator.allocator();
                allocator.destroy(td);
            }
        }
    }
}

// Initialize the thread local default heap, called from `thread_init`
fn _heap_init() bool {
    if (get_default_heap().is_initialized()) return true;
    if (_is_main_thread()) {
        // assert_internal(_heap_main.thread_id != 0);  // can happen on freeBSD where alloc is called before any initialization
        // the main heap is statically allocated
        heap_main_init();
        _heap_set_default_direct(&_heap_main);
        //assert_internal(_heap_default.tld.heap_backing == get_default_heap());
    } else {
        // use `_os_alloc` to allocate directly from the OS
        var td: *thread_data_t = thread_data_alloc();

        // OS allocated so already zero initialized
        var tld = &td.tld;
        var heap = &td.heap;
        heap.thread_id = Thread.getCurrentId();
        var rng = Prng.init(crandom.int(u64));
        heap.random = rng.random();
        heap.cookie = heap.random.int(u64) | 1;
        heap.keys[0] = heap.random.int(u64);
        heap.keys[1] = heap.random.int(u64);
        heap.tld = tld;
        tld.heap_backing = heap;
        tld.heaps = heap;
        tld.segments.stats = &tld.stats;
        tld.segments.os = &tld.os;
        tld.os.stats = &tld.stats;
        _heap_set_default_direct(heap);
    }
    return false;
}

// Free the thread local default heap (called from `thread_done`)
fn _heap_done(heap: *mi.heap_t) bool {
    if (!mi.heap_is_initialized(heap)) return true;

    // reset default heap
    _heap_set_default_direct(if (_is_main_thread()) &_heap_main else &_heap_empty);

    // switch to backing heap
    heap = heap.tld.heap_backing;
    if (!mi.heap_is_initialized(heap)) return false;

    // delete all non-backing heaps in this thread
    var curr = heap.tld.heaps;
    while (curr != null) {
        var next = curr.next; // save `next` as `curr` will be freed
        if (curr != heap) {
            assert(!mi.heap_is_backing(curr));
            mi.mi_heap_delete(curr);
        }
        curr = next;
    }
    assert(heap.tld.heaps == heap and heap.next == null);
    assert(mi.heap_is_backing(heap));

    // collect if not the main thread
    if (heap != &_heap_main) {
        mi.mi_heap_collect_abandon(heap);
    }

    // merge stats
    mi.mi_stats_done(&heap.tld.stats);

    // free if not the main thread
    if (heap != &_heap_main) {
        // the following assertion does not always hold for huge segments as those are always treated
        // as abondened: one may allocate it in one thread, but deallocate in another in which case
        // the count can be too large or negative. todo: perhaps not count huge segments? see issue #363
        // assert_internal(heap.tld.segments.count == 0 || heap.thread_id != Thread.getCurrentId());
        thread_data_free(heap);
    } else {
        thread_data_collect(); // free cached thread metadata
        if (0) {
            // never free the main thread even in debug mode; if a dll is linked statically with mimalloc,
            // there may still be delete/free calls after the fls_done is called. Issue #207
            mi._heap_destroy_pages(heap);
            assert(heap.tld.heap_backing == &_heap_main);
        }
    }
    return false;
}

// --------------------------------------------------------
// Try to run `thread_done()` automatically so any memory
// owned by the thread but not yet released can be abandoned
// and re-owned by another thread.
//
// 1. windows dynamic library:
//     call from DllMain on DLL_THREAD_DETACH
// 2. windows static library:
//     use `FlsAlloc` to call a destructor when the thread is done
// 3. unix, pthreads:
//     use a pthread key to call a destructor when a pthread is done
//
// In the last two cases we also need to call `process_init`
// to set up the thread local keys.
// --------------------------------------------------------

pub fn _is_main_thread() bool {
    return (_heap_main.thread_id == 0 or _heap_main.thread_id == Thread.getCurrentId());
}

var thread_count: Atomic(usize) = Atomic(usize).init(0);

fn _current_thread_count() usize {
    return thread_count.load();
}

// This is called from the `malloc_generic`
pub fn thread_init() void {
    // ensure our process has started already
    process_init();

    // initialize the thread local default heap
    // (this will call `_heap_set_default_direct` and thus set the
    //  fiber/pthread key to a non-zero value, ensuring `_thread_done` is called)
    if (_heap_init()) return; // returns true if already initialized

    mi.mi_stat_increase(&_stats_main.threads, 1);
    _ = thread_count.fetchAdd(1, AtomicOrder.Monotonic);
    //_verbose_message("thread init: 0x%zx\n", Thread.getCurrentId());
}

pub fn thread_done() void {
    _thread_done(mi.get_default_heap());
}

pub fn _thread_done(heap: *mi.heap_t) void {
    thread_count.decrement();
    mi._stat_decrease(&_stats_main.threads, 1);

    // check thread-id as on Windows shutdown with FLS the main (exit) thread may call this on thread-local heaps...
    if (heap.thread_id != Thread.getCurrentId()) return;

    // abandon the thread local heap
    if (_heap_done(heap)) return; // returns true if already ran
}

pub fn _heap_set_default_direct(heap: *mi.heap_t) void {
    _heap_default = heap;
}

// --------------------------------------------------------
// Run functions on process init/done, and thread init/done
// --------------------------------------------------------
var os_preloading: bool = true; // true until this module is initialized
var redirected: bool = false; // true if malloc redirects to malloc

// Returns true if this module has not been initialized; Don't use C runtime routines until it returns false.
fn _preloading() bool {
    return os_preloading;
}

fn is_redirected() bool {
    return redirected;
}

// Called once by the process loader
fn process_load() void {
    heap_main_init();
    os_preloading = false;
    mi._options_init();
    process_init();
    //stats_reset();-
}

// Initialize the process; called by thread_init or the process loader
pub fn process_init() void {
    // ensure we are called once
    if (_process_is_initialized) return;
    std.log.debug("process init: 0x{}", .{Thread.getCurrentId()});
    _process_is_initialized = true;

    // TODO: os.zig: mi._os_init();
    heap_main_init();
    if (mi.DEBUG > 0)
        std.log.debug("debug level: {}", .{mi.DEBUG});
    std.log.debug("secure level: {}", .{mi.SECURE});
    thread_init();

    mi.mi_stats_reset(); // only call stat reset *after* thread init (or the heap tld == NULL)

    // TODO: options.zig: if (mi.option_is_enabled(mi.option_reserve_huge_os_pages)) {
    if (false) {
        const pages = mi.option_get_clamp(mi.option_reserve_huge_os_pages, 0, 128 * 1024);
        const reserve_at = mi.option_get(mi.option_reserve_huge_os_pages_at);
        if (reserve_at != -1) {
            mi.reserve_huge_os_pages_at(pages, reserve_at, pages * 500);
        } else {
            mi.reserve_huge_os_pages_interleave(pages, 0, pages * 500);
        }
    }
    // TODO: options.zig: if (mi.option_is_enabled(mi.option_reserve_os_memory)) {
    if (false) {
        const ksize = mi.option_get(mi.option_reserve_os_memory);
        if (ksize > 0) {
            mi.reserve_os_memory(ksize * mi.KiB, true, true);
        }
    }
}

var is_process_done: bool = false;
const SHARED_LIB = false;

// Called when the process is done (through `at_exit`)
pub fn process_done() void {
    // only shutdown if we were initialized
    if (!_process_is_initialized) return;
    // ensure we are called once
    if (process_done) return;
    process_done = true;

    if (mi.DEBUG != 0 or !SHARED_LIB) {
        // free all memory if possible on process exit. This is not needed for a stand-alone process
        // but should be done if mimalloc is statically linked into another shared library which
        // is repeatedly loaded/unloaded, see issue #281.
        mi.collect(true); // force
    }
    if (mi.option_is_enabled(mi.option_show_stats) || mi.option_is_enabled(mi.option_verbose)) {
        mi.stats_print(null);
    }
    std.log.debug("process done: 0x{:x}", .{_heap_main.thread_id});
    os_preloading = true; // don't call the C runtime anymore
}
