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

const MI_DEBUG = mi.MI_DEBUG;
const MI_SECURE = mi.MI_SECURE;
const MI_PADDING = mi.MI_PADDING;
const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;

const MI_KiB = mi.MI_KiB;

const mi_heap_t = mi.mi_heap_t;
const mi_page_t = mi.mi_page_t;
const mi_tld_t = mi.mi_tld_t;
const mi_stats_t = mi.mi_stats_t;
const mi_page_queue_t = mi.mi_page_queue_t;
const _mi_thread_id = mi._mi_thread_id;
const mi_option_t = mi.mi_option_t;
const mi_span_queue_t = mi.mi_span_queue_t;

const mi_heap_is_initialized = mi.mi_heap_is_initialized;

const _mi_stat_increase = mi._mi_stat_increase;
const _mi_stat_decrease = mi._mi_stat_decrease;

const mi_stats_done = mi.mi_stats_done;
const mi_stats_reset = mi.mi_stats_reset;
const mi_stats_print = mi.mi_stats_print;

const mi_heap_is_backing = mi.mi_heap_is_backing;
const mi_heap_delete = mi.mi_heap_delete;
const mi_heap_collect_abandon = mi.mi_heap_collect_abandon;
const _mi_heap_destroy_pages = mi._mi_heap_destroy_pages;

const mi_collect = mi.mi_collect;

const _mi_options_init = mi._mi_options_init;

// TODO: const mi_option_is_enabled = mi.mi_option_is_enabled;
fn mi_option_is_enabled(option: mi_option_t) bool {
    _ = option;
    return true;
}
// TODO: const mi_option_get = mi.mi_option_get;
fn mi_option_get(option: mi_option_t) isize {
    _ = option;
    return 0;
}

// TODO: const mi_option_get_clamp = mi.mi_option_get_clamp;
fn mi_option_get_clamp(option: mi_option_t, min: isize, max: isize) isize {
    _ = option;
    _ = min;
    _ = max;
    return 0;
}

const mi_option_reserve_os_memory = mi_option_t.mi_option_reserve_os_memory;
const mi_option_reserve_huge_os_pages = mi_option_t.mi_option_reserve_huge_os_pages;
const mi_option_reserve_huge_os_pages_at = mi_option_t.mi_option_reserve_huge_os_pages_at;
const mi_option_show_stats = mi_option_t.mi_option_show_stats;
const mi_option_verbose = mi_option_t.mi_option_verbose;

// TODO: const mi_reserve_os_memory = mi.mi_reserve_os_memory;
fn mi_reserve_os_memory(size: usize, commit: bool, allow_large: bool) i32 {
    _ = size;
    _ = commit;
    _ = allow_large;
    return 0;
}

// TODO: const mi_reserve_huge_os_pages_at = mi.mi_reserve_huge_os_pages_at;
fn mi_reserve_huge_os_pages_at(pages: isize, numa_node: i32, timeout_msecs: usize) i32 {
    _ = pages;
    _ = numa_node;
    _ = timeout_msecs;
    return 0;
}

// TODO: const mi_reserve_huge_os_pages_interleave = mi.mi_reserve_huge_os_pages_interleave;
fn mi_reserve_huge_os_pages_interleave(pages: isize, numa_node: i32, timeout_msecs: usize) i32 {
    _ = pages;
    _ = numa_node;
    _ = timeout_msecs;
    return 0;
}

// Empty page used to initialize the small free pages array
const _mi_page_empty: mi_page_t = .{};

const MI_SMALL_PAGES_EMPTY = [1]*mi_page_t{&_mi_page_empty} ** if (MI_PADDING > 0 and MI_INTPTR_SIZE >= 8) 130 else if (MI_PADDING > 0) 131 else 129;

// Empty page queues for every bin
fn QNULL(comptime sz: usize) mi_page_queue_t {
    return .{ .block_size = sz * @sizeOf(usize) };
}

const MI_PAGE_QUEUES_EMPTY = [_]mi_page_queue_t{
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
    QNULL(mi.MI_MEDIUM_OBJ_WSIZE_MAX + 1), // 655360, Huge queue
    QNULL(mi.MI_MEDIUM_OBJ_WSIZE_MAX + 2), // full queue
};

// Empty slice span queues for every bin
fn SQNULL(comptime sz: usize) mi_span_queue_t {
    return .{ .slice_count = sz };
}

const MI_SEGMENT_SPAN_QUEUES_EMPTY = [_]mi_span_queue_t{
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
pub var _mi_heap_empty: mi_heap_t = .{ .random = undefined };

// the thread-local default heap for allocation
threadlocal var _mi_heap_default: *mi_heap_t = &_mi_heap_empty;

pub fn mi_get_default_heap() *mi_heap_t {
    return _mi_heap_default;
}

const mi_tld_empty = mi_tld_t{
    .segments = .{ .spans = MI_SEGMENT_SPAN_QUEUES_EMPTY },
};

var mi_tld_main = mi_tld_t{
    .heap_backing = &_mi_heap_main,
    .heaps = &_mi_heap_main,
    //    .segments = .{ .spans = MI_SEGMENT_SPAN_QUEUES_EMPTY, .stats = &mi_tld_main.stats, .os = &mi_tld_main.os }
    .segments = .{ .spans = MI_SEGMENT_SPAN_QUEUES_EMPTY },
    // .os = .{ .stats = &mi_tld_main.stats }, // os
};

var _mi_heap_main = mi_heap_t{
    .pages = MI_PAGE_QUEUES_EMPTY,
    .random = undefined, // TODO
};

var _mi_process_is_initialized = false; // set to `true` in `process_init`.

pub var _mi_stats_main = mi_stats_t{};

fn mi_heap_main_init() void {
    if (_mi_heap_main.cookie == 0) {
        _mi_heap_main.tld = &mi_tld_main;
        _mi_heap_main.thread_id = _mi_thread_id();
        var rng = Prng.init(crandom.int(u64));
        _mi_heap_main.random = rng.random();
        _mi_heap_main.cookie = _mi_heap_main.random.int(u64);
        _mi_heap_main.keys[0] = _mi_heap_main.random.int(u64);
        _mi_heap_main.keys[1] = _mi_heap_main.random.int(u64);
    }
}

fn _mi_heap_main_get() *mi_heap_t {
    mi_heap_main_init();
    return &_mi_heap_main;
}

// -----------------------------------------------------------
//  Initialization and freeing of the thread local heaps
//-----------------------------------------------------------

// note: in x64 in release build `sizeof(thread_data_t)` is under 4KiB (= OS page size).
const mi_thread_data_t = struct {
    heap: mi_heap_t, // must come first due to cast in `_heap_done`
    tld: mi_tld_t,
};

// Thread meta-data is allocated directly from the OS. For
// some programs that do not use thread pools and allocate and
// destroy many OS threads, this may causes too much overhead
// per thread so we maintain a small cache of recently freed metadata.

const TD_CACHE_SIZE = 8;
var td_cache = [_]?*mi_thread_data_t{null} ** TD_CACHE_SIZE;

fn mi_thread_data_alloc() *mi_thread_data_t {
    // try to find thread metadata in the cache
    var td: ?*mi_thread_data_t = null;
    var i: usize = 0;
    while (i < TD_CACHE_SIZE) : (i += 1) {
        td = @atomicLoad(?*mi_thread_data_t, &td_cache[i], AtomicOrder.Monotonic);
        if (td != null) {
            td = @atomicRmw(?*mi_thread_data_t, &td_cache[i], AtomicRmwOp.Xchg, null, AtomicOrder.Monotonic);
            if (td != null) {
                return td.?;
            }
        }
    }
    // if that fails, allocate directly from the OS
    const page_allocator = std.heap.page_allocator;
    td = page_allocator.create(mi_thread_data_t) catch null;
    if (td == null) {
        // if this fails, try once more. (issue #257)
        td = page_allocator.create(mi_thread_data_t) catch unreachable;
    }
    return td.?;
}

fn mi_thread_data_free(tdfree: *mi_thread_data_t) void {
    // try to add the thread metadata to the cache
    var i: usize = 0;
    while (i < TD_CACHE_SIZE) : (i += 1) {
        var td: ?*mi_thread_data_t = @atomicLoad(mi_thread_data_t, &td_cache[i], builtin.Monotonic);
        if (td == null) {
            const prev = @atomicRmw(mi_thread_data_t, &td_cache[i], builtin.Xchg, tdfree, builtin.Monotonic);
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

fn mi_thread_data_collect() void {
    // free all thread metadata from the cache
    var i: usize = 0;
    while (i < TD_CACHE_SIZE) : (i += 1) {
        var td: ?*mi_thread_data_t = @atomicLoad(mi_thread_data_t, &td_cache[i], builtin.Monotonic);
        if (td != null) {
            td = @atomicRmw(mi_thread_data_t, &td_cache[i], builtin.Xchg, null, builtin.Monotonic);
            if (td != null) {
                const page_allocator = std.heap.page_allocator;
                var allocator = page_allocator.allocator();
                allocator.destroy(td);
            }
        }
    }
}

// Initialize the thread local default heap, called from `thread_init`
fn _mi_heap_init() bool {
    if (mi_get_default_heap().is_initialized()) return true;
    if (_mi_is_main_thread()) {
        // assert_internal(_heap_main.thread_id != 0);  // can happen on freeBSD where alloc is called before any initialization
        // the main heap is statically allocated
        mi_heap_main_init();
        _mi_heap_set_default_direct(&_mi_heap_main);
        //assert_internal(_mi_heap_default.tld.heap_backing == get_default_heap());
    } else {
        // use `_os_alloc` to allocate directly from the OS
        var td: *mi_thread_data_t = mi_thread_data_alloc();

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
        _mi_heap_set_default_direct(heap);
    }
    return false;
}

// Free the thread local default heap (called from `thread_done`)
fn _mi_heap_done(heap: *mi_heap_t) bool {
    if (!mi_heap_is_initialized(heap)) return true;

    // reset default heap
    _mi_heap_set_default_direct(if (_mi_is_main_thread()) &_mi_heap_main else &_mi_heap_empty);

    // switch to backing heap
    heap = heap.tld.heap_backing;
    if (!mi_heap_is_initialized(heap)) return false;

    // delete all non-backing heaps in this thread
    var curr = heap.tld.heaps;
    while (curr != null) {
        var next = curr.next; // save `next` as `curr` will be freed
        if (curr != heap) {
            assert(!mi_heap_is_backing(curr));
            mi_heap_delete(curr);
        }
        curr = next;
    }
    assert(heap.tld.heaps == heap and heap.next == null);
    assert(mi_heap_is_backing(heap));

    // collect if not the main thread
    if (heap != &_mi_heap_main) {
        mi_heap_collect_abandon(heap);
    }

    // merge stats
    mi_stats_done(&heap.tld.stats);

    // free if not the main thread
    if (heap != &_mi_heap_main) {
        // the following assertion does not always hold for huge segments as those are always treated
        // as abondened: one may allocate it in one thread, but deallocate in another in which case
        // the count can be too large or negative. todo: perhaps not count huge segments? see issue #363
        // assert_internal(heap.tld.segments.count == 0 || heap.thread_id != Thread.getCurrentId());
        mi_thread_data_free(heap);
    } else {
        mi_thread_data_collect(); // free cached thread metadata
        if (0) {
            // never free the main thread even in debug mode; if a dll is linked statically with mimalloc,
            // there may still be delete/free calls after the fls_done is called. Issue #207
            _mi_heap_destroy_pages(heap);
            assert(heap.tld.heap_backing == &_mi_heap_main);
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

pub fn _mi_is_main_thread() bool {
    return (_mi_heap_main.thread_id == 0 or _mi_heap_main.thread_id == _mi_thread_id());
}

var thread_count: Atomic(usize) = Atomic(usize).init(0);

pub fn _mi_current_thread_count() usize {
    return thread_count.load();
}

// This is called from the `malloc_generic`
pub fn mi_thread_init() void {
    // ensure our process has started already
    mi_process_init();

    // initialize the thread local default heap
    // (this will call `_heap_set_default_direct` and thus set the
    //  fiber/pthread key to a non-zero value, ensuring `_thread_done` is called)
    if (_mi_heap_init()) return; // returns true if already initialized

    _mi_stat_increase(&_mi_stats_main.threads, 1);
    _ = thread_count.fetchAdd(1, AtomicOrder.Monotonic);
    //_verbose_message("thread init: 0x%zx\n", Thread.getCurrentId());
}

pub fn mi_thread_done() void {
    _mi_thread_done(mi_get_default_heap());
}

pub fn _mi_thread_done(heap: *mi_heap_t) void {
    thread_count.decrement();
    _mi_stat_decrease(&_mi_stats_main.threads, 1);

    // check thread-id as on Windows shutdown with FLS the main (exit) thread may call this on thread-local heaps...
    if (heap.thread_id != _mi_thread_id()) return;

    // abandon the thread local heap
    if (_mi_heap_done(heap)) return; // returns true if already ran
}

pub fn _mi_heap_set_default_direct(heap: *mi_heap_t) void {
    _mi_heap_default = heap;
}

// --------------------------------------------------------
// Run functions on process init/done, and thread init/done
// --------------------------------------------------------
var os_preloading: bool = true; // true until this module is initialized
var mi_redirected: bool = false; // true if malloc redirects to malloc

// Returns true if this module has not been initialized; Don't use C runtime routines until it returns false.
fn mi_preloading() bool {
    return os_preloading;
}

fn mi_is_redirected() bool {
    return mi_redirected;
}

// Called once by the process loader
fn mi_process_load() void {
    mi_heap_main_init();
    os_preloading = false;
    _mi_options_init();
    mi_process_init();
    //stats_reset();-
}

// Initialize the process; called by thread_init or the process loader
pub fn mi_process_init() void {
    // ensure we are called once
    if (_mi_process_is_initialized) return;
    std.log.debug("process init: 0x{}", .{Thread.getCurrentId()});
    _mi_process_is_initialized = true;

    // TODO: os.zig: mi_os_init();
    mi_heap_main_init();
    if (MI_DEBUG > 0)
        std.log.debug("debug level: {}", .{MI_DEBUG});
    std.log.debug("secure level: {}", .{MI_SECURE});
    mi_thread_init();

    mi_stats_reset(); // only call stat reset *after* thread init (or the heap tld == NULL)

    if (mi_option_is_enabled(mi_option_reserve_huge_os_pages)) {
        const pages = mi_option_get_clamp(mi_option_reserve_huge_os_pages, 0, 128 * 1024);
        const reserve_at = mi_option_get(mi_option_reserve_huge_os_pages_at);
        if (reserve_at != -1) {
            _ = mi_reserve_huge_os_pages_at(pages, @intCast(i32, reserve_at), @intCast(usize, pages) * 500);
        } else {
            _ = mi_reserve_huge_os_pages_interleave(pages, 0, @intCast(usize, pages) * 500);
        }
    }
    if (mi_option_is_enabled(mi_option_reserve_os_memory)) {
        const ksize = mi_option_get(mi_option_reserve_os_memory);
        if (ksize > 0) {
            _ = mi_reserve_os_memory(@intCast(usize, ksize) * MI_KiB, true, true);
        }
    }
}

var mi_is_process_done: bool = false;
const MI_SHARED_LIB = false;

// Called when the process is done (through `at_exit`)
pub fn mi_process_done() void {
    // only shutdown if we were initialized
    if (!_mi_process_is_initialized) return;
    // ensure we are called once
    if (mi_process_done) return;
    mi_process_done = true;

    if (MI_DEBUG != 0 or !MI_SHARED_LIB) {
        // free all memory if possible on process exit. This is not needed for a stand-alone process
        // but should be done if mimalloc is statically linked into another shared library which
        // is repeatedly loaded/unloaded, see issue #281.
        mi_collect(true); // force
    }
    if (mi_option_is_enabled(mi_option_show_stats) || mi_option_is_enabled(mi_option_verbose)) {
        mi_stats_print(null);
    }
    std.log.debug("process done: 0x{:x}", .{_mi_heap_main.thread_id});
    os_preloading = true; // don't call the C runtime anymore
}
