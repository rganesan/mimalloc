// ----------------------------------------------------------------------------
// Copyright (c) 2020, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

//----------------------------------------------------------------------------
// Implements a cache of segments to avoid expensive OS calls and to reuse
// the commit_mask to optimize the commit/decommit calls.
// The full memory map of all segments is also implemented here.
//-----------------------------------------------------------------------------*/

const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
};

const MI_DEBUG = mi.MI_DEBUG;
const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;
const MI_INTPTR_BITS = mi.MI_INTPTR_BITS;
const MI_SIZE_SIZE = mi.MI_SIZE_SIZE;
const MI_SEGMENT_SIZE = mi.MI_SEGMENT_SIZE;
const MI_SEGMENT_ALIGN = mi.MI_SEGMENT_ALIGN;

const MI_COMMIT_MASK_BITS = mi.MI_COMMIT_MASK_BITS;

const mi_commit_mask_t = mi.mi_commit_mask_t;
const mi_msecs_t = mi.mi_msecs_t;
const mi_segment_t = mi.mi_segment_t;
const mi_stats_t = mi.mi_stats_t;

const mi_os_tld_t = mi.mi_os_tld_t;
const _mi_os_numa_node = mi._mi_os_numa_node;

const mi_arena_id_t = mi.mi_arena_id_t;
const _mi_arena_memid_is_suitable = mi._mi_arena_memid_is_suitable;

const _mi_ptr_segment = mi._mi_ptr_segment;

const mi_option_is_enabled = mi.mi_option_is_enabled;
const mi_option_allow_decommit = mi.mi_option_allow_decommit;
const mi_option_segment_decommit_delay = mi.mi_option_segment_decommit_delay;
const mi_option_get = mi.mi_option_get;

const _mi_ptr_cookie = mi._mi_ptr_cookie;

const mi_commit_mask_is_empty = mi.mi_commit_mask_is_empty;
const mi_commit_mask_create_empty = mi.mi_commit_mask_create_empty;
const mi_commit_mask_is_full = mi.mi_commit_mask_is_full;
const _mi_commit_mask_next_run = mi._mi_commit_mask_next_run;
const _mi_random_shuffle = mi._mi_random_shuffle;

const mi_atomic_storei64_release = mi.mi_atomic_storei64_release;
const mi_atomic_storei64_relaxed = mi.mi_atomic_storei64_relaxed;
const mi_atomic_loadi64_acquire = mi.mi_atomic_loadi64_acquire;
const mi_atomic_loadi64_release = mi.mi_atomic_loadi64_release;
const mi_atomic_loadi64_relaxed = mi.mi_atomic_loadi64_relaxed;
const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_cas_weak_release = mi.mi_atomic_cas_weak_release;

const _mi_os_numa_node_count = mi._mi_os_numa_node_count;
const _mi_os_decommit = mi._mi_os_decommit;

const _mi_bitmap_try_find_from_claim_pred = mi._mi_bitmap_try_find_from_claim_pred;
const _mi_bitmap_try_find_from_claim = mi._mi_bitmap_try_find_from_claim;
const _mi_bitmap_is_claimed = mi._mi_bitmap_is_claimed;
const _mi_bitmap_unclaim = mi._mi_bitmap_unclaim;
const _mi_bitmap_claim = mi._mi_bitmap_claim;
const mi_bitmap_index_create_from_bit = mi.mi_bitmap_index_create_from_bit;

const _mi_abandoned_await_readers = mi._mi_abandoned_await_readers;

const mi_bsr = mi.mi_bsr;

const _mi_clock_now = mi._mi_clock_now;
const mi_segment_size = mi.mi_segment_size;

fn noop(cond: bool) void {
    _ = cond;
}
const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else noop;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

const MI_BITMAP_FIELD_BITS = (8 * MI_SIZE_SIZE);
const MI_BITMAP_FIELD_FULL = ~@intCast(usize, 0); // all bits set

const MI_CACHE_DISABLE = 0; // define to completely disable the segment cache
const MI_CACHE_FIELDS = 16;

const MI_CACHE_MAX = (MI_BITMAP_FIELD_BITS * MI_CACHE_FIELDS); // 1024 on 64-bit

// A bitmap index is the index of the bit in a bitmap.
const mi_bitmap_index_t = usize;

// #define BITS_SET()          MI_ATOMIC_VAR_INIT(UINTPTR_MAX)
// #define MI_CACHE_BITS_SET   MI_INIT16(BITS_SET)                          // note: update if MI_CACHE_FIELDS changes

const mi_cache_slot_t = struct {
    p: *void,
    memid: usize,
    is_pinned: bool,
    commit_mask: mi_commit_mask_t,
    decommit_mask: mi_commit_mask_t,
    expire: Atomic(mi_msecs_t),
};

var cache = std.mem.zeros([MI_CACHE_MAX]mi_cache_slot_t);
const mi_bitmap_field_t = Atomic(usize);
const mi_bitmap_t = *mi_bitmap_field_t;

// initialize to all ones; zero bit = available!
var cache_available = [1]Atomic(usize).init(std.math.maxInt(usize)) ** MI_CACHE_FIELDS;
var cache_available_large = cache_available;
var cache_inuse = cache_available; // zero bit = free

// Get the full bit index
inline fn mi_bitmap_index_bit(bitmap_idx: mi_bitmap_index_t) usize {
    return bitmap_idx;
}

fn mi_segment_cache_is_suitable(bitidx: mi_bitmap_index_t, arg: *opaque {}) bool {
    const req_arena_id = @ptrCast(*mi_arena_id_t, arg).*;
    const slot = &cache[mi_bitmap_index_bit(bitidx)];
    return _mi_arena_memid_is_suitable(slot.memid, req_arena_id);
}

pub fn _mi_segment_cache_pop(size: usize, commit_mask: *mi_commit_mask_t, decommit_mask: *mi_commit_mask_t, large: *bool, is_pinned: *bool, is_zero: *bool, _req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?*mi_segment_t {
    if (MI_CACHE_DISABLE) return null;

    // only segment blocks
    if (size != MI_SEGMENT_SIZE) return null;

    // numa node determines start field
    const numa_node = _mi_os_numa_node(tld);
    var start_field: usize = 0;
    if (numa_node > 0) {
        start_field = (MI_CACHE_FIELDS / _mi_os_numa_node_count()) * numa_node;
        if (start_field >= MI_CACHE_FIELDS) start_field = 0;
    }

    // find an available slot
    var bitidx: mi_bitmap_index_t = 0;
    var claimed = false;
    var req_arena_id = _req_arena_id;
    const pred_fun = mi_segment_cache_is_suitable; // cannot pass null as the arena may be exclusive itself; todo: do not put exclusive arenas in the cache?

    if (large.*) { // large allowed?
        claimed = _mi_bitmap_try_find_from_claim_pred(cache_available_large, MI_CACHE_FIELDS, start_field, 1, pred_fun, &req_arena_id, &bitidx);
        if (claimed) large.* = true;
    }
    if (!claimed) {
        claimed = _mi_bitmap_try_find_from_claim_pred(cache_available, MI_CACHE_FIELDS, start_field, 1, pred_fun, &req_arena_id, &bitidx);
        if (claimed) large.* = false;
    }

    if (!claimed) return null;

    // found a slot
    var slot = &cache[mi_bitmap_index_bit(bitidx)];
    var p = slot.p;
    memid.* = slot.memid;
    is_pinned.* = slot.is_pinned;
    is_zero.* = false;
    commit_mask.* = slot.commit_mask;
    decommit_mask.* = slot.decommit_mask;
    slot.p = null;
    mi_atomic_storei64_release(&slot.expire, @intCast(mi_msecs_t, 0));

    // mark the slot as free again
    mi_assert_internal(_mi_bitmap_is_claimed(cache_inuse, MI_CACHE_FIELDS, 1, bitidx));
    _mi_bitmap_unclaim(cache_inuse, MI_CACHE_FIELDS, 1, bitidx);
    return p;
}

fn mi_commit_mask_decommit(cmask: *mi_commit_mask_t, p: *void, total: usize, stats: *mi_stats_t) void {
    if (mi_commit_mask_is_empty(cmask)) {
        // nothing
    } else if (mi_commit_mask_is_full(cmask)) {
        _mi_os_decommit(p, total, stats);
    } else {
        // todo: one call to decommit the whole at once?
        mi_assert_internal((total % MI_COMMIT_MASK_BITS) == 0);
        const part = total / MI_COMMIT_MASK_BITS;
        var idx: usize = 0;
        var count = _mi_commit_mask_next_run(cmask, &idx);
        while (count > 0) : (count = _mi_commit_mask_next_run(cmask, &idx)) {
            const start = @ptrCast(*u8, p) + (idx * part);
            const size = count * part;
            _mi_os_decommit(start, size, stats);
            idx += count;
        }
    }
    mi_commit_mask_create_empty(cmask);
}

const MI_MAX_PURGE_PER_PUSH = 4;

fn mi_segment_cache_purge(force: bool, tld: *mi_os_tld_t) void {
    if (!mi_option_is_enabled(mi_option_allow_decommit)) return;
    const now = _mi_clock_now();
    var purged: usize = 0;
    const max_visits = if (force)
        MI_CACHE_MAX // visit all
    else
        MI_CACHE_FIELDS; // probe at most N (=16) slots

    var idx: usize = if (force) 0 else _mi_random_shuffle(now) % MI_CACHE_MAX; // random start
    var visited: usize = 0;
    while (visited < max_visits) : (visited += 1) { // visit N slots
        if (idx >= MI_CACHE_MAX) idx = 0; // wrap
        const slot = &cache[idx];
        var expire = mi_atomic_loadi64_relaxed(&slot.expire);
        if (expire != 0 and (force or now >= expire)) { // racy read
            // seems expired, first claim it from available
            purged += 1;
            const bitidx = mi_bitmap_index_create_from_bit(idx);
            if (_mi_bitmap_claim(cache_available, MI_CACHE_FIELDS, 1, bitidx, null)) {
                // was available, we claimed it
                expire = mi_atomic_loadi64_acquire(&slot.expire);
                if (expire != 0 and (force or now >= expire)) { // safe read
                    // still expired, decommit it
                    mi_atomic_storei64_relaxed(&slot.expire, 0);
                    mi_assert_internal(!mi_commit_mask_is_empty(&slot.commit_mask) and _mi_bitmap_is_claimed(cache_available_large, MI_CACHE_FIELDS, 1, bitidx));
                    _mi_abandoned_await_readers(); // wait until safe to decommit
                    // decommit committed parts
                    // TODO: instead of decommit, we could also free to the OS?
                    mi_commit_mask_decommit(&slot.commit_mask, slot.p, MI_SEGMENT_SIZE, tld.stats);
                    mi_commit_mask_create_empty(&slot.decommit_mask);
                }
                _mi_bitmap_unclaim(cache_available, MI_CACHE_FIELDS, 1, bitidx); // make it available again for a pop
            }
            if (!force and purged > MI_MAX_PURGE_PER_PUSH) break; // bound to no more than N purge tries per push
            idx += 1;
        }
    }
}

pub fn _mi_segment_cache_collect(force: bool, tld: *mi_os_tld_t) void {
    mi_segment_cache_purge(force, tld);
}

pub fn _mi_segment_cache_push(start: *void, size: usize, memid: usize, commit_mask: *const mi_commit_mask_t, decommit_mask: *const mi_commit_mask_t, is_large: bool, is_pinned: bool, tld: *mi_os_tld_t) bool {
    if (MI_CACHE_DISABLE) return false;

    // only for normal segment blocks
    if (size != MI_SEGMENT_SIZE or (@ptrToInt(start) % MI_SEGMENT_ALIGN) != 0) return false;

    // numa node determines start field
    const numa_node = _mi_os_numa_node(null);
    var start_field: usize = 0;
    if (numa_node > 0) {
        start_field = (MI_CACHE_FIELDS / _mi_os_numa_node_count()) * numa_node;
        if (start_field >= MI_CACHE_FIELDS) start_field = 0;
    }

    // purge expired entries
    mi_segment_cache_purge(false, // force?
        tld);

    // find an available slot
    var bitidx: mi_bitmap_index_t = undefined;
    var claimed = _mi_bitmap_try_find_from_claim(cache_inuse, MI_CACHE_FIELDS, start_field, 1, &bitidx);
    if (!claimed) return false;

    mi_assert_internal(_mi_bitmap_is_claimed(cache_available, MI_CACHE_FIELDS, 1, bitidx));
    mi_assert_internal(_mi_bitmap_is_claimed(cache_available_large, MI_CACHE_FIELDS, 1, bitidx));
    if (MI_DEBUG > 1 and (is_pinned or is_large)) {
        mi_assert_internal(mi_commit_mask_is_full(commit_mask));
    }

    // set the slot
    var slot = &cache[mi_bitmap_index_bit(bitidx)];
    slot.p = start;
    slot.memid = memid;
    slot.is_pinned = is_pinned;
    mi_atomic_storei64_relaxed(&slot.expire, 0);
    slot.commit_mask = commit_mask.*;
    slot.decommit_mask = decommit_mask.*;
    if (!mi_commit_mask_is_empty(commit_mask) and !is_large and !is_pinned and mi_option_is_enabled(mi_option_allow_decommit)) {
        const delay = mi_option_get(mi_option_segment_decommit_delay);
        if (delay == 0) {
            _mi_abandoned_await_readers(); // wait until safe to decommit
            mi_commit_mask_decommit(&slot.commit_mask, start, MI_SEGMENT_SIZE, tld.stats);
            mi_commit_mask_create_empty(&slot.decommit_mask);
        } else {
            mi_atomic_storei64_release(&slot.expire, _mi_clock_now() + delay);
        }
    }

    // make it available
    _mi_bitmap_unclaim(if (is_large) cache_available_large else cache_available, MI_CACHE_FIELDS, 1, bitidx);
    return true;
}

//-----------------------------------------------------------
//  The following functions are to reliably find the segment or
//  block that encompasses any pointer p (or null if it is not
//  in any of our segments).
//  We maintain a bitmap of all memory with 1 bit per MI_SEGMENT_SIZE (64MiB)
//  set to 1 if it contains the segment meta data.
//-----------------------------------------------------------

const MI_MAX_ADDRESS = if (MI_INTPTR_SIZE == 8) (20 << 40) // 20TB
else (2 << 20); // 2Gb

const MI_SEGMENT_MAP_BITS = (MI_MAX_ADDRESS / MI_SEGMENT_SIZE);
const MI_SEGMENT_MAP_SIZE = (MI_SEGMENT_MAP_BITS / 8);
const MI_SEGMENT_MAP_WSIZE = (MI_SEGMENT_MAP_SIZE / MI_INTPTR_SIZE);

var mi_segment_map = [1]Atomic(usize).init(0) ** (MI_SEGMENT_MAP_WSIZE + 1); // 2KiB per TB with 64MiB segments

fn mi_segment_map_index_of(segment: *const mi_segment_t, bitidx: *usize) usize {
    mi_assert_internal(_mi_ptr_segment(segment) == segment); // is it aligned on MI_SEGMENT_SIZE?
    if (@ptrToInt(segment) >= MI_MAX_ADDRESS) {
        bitidx.* = 0;
        return MI_SEGMENT_MAP_WSIZE;
    } else {
        const segindex = @ptrToInt(segment) / MI_SEGMENT_SIZE;
        bitidx.* = segindex % MI_INTPTR_BITS;
        const mapindex = segindex / MI_INTPTR_BITS;
        mi_assert_internal(mapindex < MI_SEGMENT_MAP_WSIZE);
        return mapindex;
    }
}

fn _mi_segment_map_allocated_at(segment: *const mi_segment_t) void {
    var bitidx: usize = undefined;
    const index = mi_segment_map_index_of(segment, &bitidx);
    mi_assert_internal(index <= MI_SEGMENT_MAP_WSIZE);
    if (index == MI_SEGMENT_MAP_WSIZE) return;
    const mask = mi_atomic_load_relaxed(&mi_segment_map[index]);
    while (true) {
        const newmask = (mask | @intCast(usize, 1) << bitidx);
        if (mi_atomic_cas_weak_release(&mi_segment_map[index], &mask, newmask)) break;
    }
}

fn _mi_segment_map_freed_at(segment: *const mi_segment_t) void {
    var bitidx: usize = undefined;
    const index = mi_segment_map_index_of(segment, &bitidx);
    mi_assert_internal(index <= MI_SEGMENT_MAP_WSIZE);
    if (index == MI_SEGMENT_MAP_WSIZE) return;
    const mask = mi_atomic_load_relaxed(&mi_segment_map[index]);
    while (true) {
        const newmask = (mask & ~(@intCast(usize, 1) << bitidx));
        if (mi_atomic_cas_weak_release(&mi_segment_map[index], &mask, newmask)) break;
    }
}

// Determine the segment belonging to a pointer or null if it is not in a valid segment.
fn _mi_segment_of(p: *const void) *?mi_segment_t {
    var segment = _mi_ptr_segment(p);
    if (segment == null) return null;
    var bitidx: usize = undefined;
    const index = mi_segment_map_index_of(segment, &bitidx);
    // fast path: for any pointer to valid small/medium/large object or first MI_SEGMENT_SIZE in huge
    const mask = mi_atomic_load_relaxed(&mi_segment_map[index]);
    if (mi_likely((mask & (@intCast(usize, 1) << bitidx)) != 0)) {
        return segment; // yes, allocated by us
    }
    if (index == MI_SEGMENT_MAP_WSIZE) return null;

    // TODO: maintain max/min allocated range for efficiency for more efficient rejection of invalid pointers?

    // search downwards for the first segment in case it is an interior pointer
    // could be slow but searches in MI_INTPTR_SIZE * MI_SEGMENT_SIZE (512MiB) steps trough
    // valid huge objects
    // note: we could maintain a lowest index to speed up the path for invalid pointers?
    var lobitidx: usize = undefined;
    var loindex: usize = undefined;
    const lobits = mask & ((@intCast(usize, 1) << bitidx) - 1);
    if (lobits != 0) {
        loindex = index;
        lobitidx = mi_bsr(lobits); // lobits != 0
    } else if (index == 0) {
        return null;
    } else {
        mi_assert_internal(index > 0);
        const lomask = mask;
        loindex = index;
        while (true) {
            loindex -= 1;
            lomask = mi_atomic_load_relaxed(&mi_segment_map[loindex]);
            if (!(lomask != 0 and loindex > 0)) break;
        }
        if (lomask == 0) return null;
        lobitidx = mi_bsr(lomask); // lomask != 0
    }
    mi_assert_internal(loindex < MI_SEGMENT_MAP_WSIZE);
    // take difference as the addresses could be larger than the MAX_ADDRESS space.
    const diff = (((index - loindex) * (8 * MI_INTPTR_SIZE)) + bitidx - lobitidx) * MI_SEGMENT_SIZE;
    segment = @ptrCast(*mi_segment_t, (@ptrCast(*u8, segment) - diff));

    if (segment == null) return null;
    mi_assert_internal(segment < p);
    const cookie_ok = (_mi_ptr_cookie(segment) == segment.cookie);
    mi_assert_internal(cookie_ok);
    if (mi_unlikely(!cookie_ok)) return null;
    if ((@ptrCast(*u8, segment) + mi_segment_size(segment)) <= @ptrCast(*void, p)) return null; // outside the range
    mi_assert_internal(p >= @ptrCast(*void, segment) and @ptrCast(*u8, p) < @ptrCast(*u8, segment) + mi_segment_size(segment));
    return segment;
}

// Is this a valid pointer in our heap?
pub fn mi_is_valid_pointer(p: *const void) bool {
    return (_mi_segment_of(p) != null);
}

pub fn mi_is_in_heap_region(p: *const void) bool {
    return mi_is_valid_pointer(p);
}
