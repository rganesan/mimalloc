//----------------------------------------------------------------------------
// Copyright (c) 2018-2020, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//----------------------------------------------------------------------------

const std = @import("std");
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("heap.zig");
    usingnamespace @import("page.zig");
    usingnamespace @import("stats.zig");
    usingnamespace @import("segment-cache.zig");
};

fn noop(cond: bool) void {
    _ = cond;
}
const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else noop;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

const MI_PAGE_HUGE_ALIGN = 256 * 1024;

// alises to avoid clutter

// type aliases
const mi_heap_t = mi.heap_t;
const mi_segment_t = mi.heap_t;
const mi_page_t = mi.page_t;
const mi_block_t = mi.block_t;
const mi_slice_t = mi.slice_t;
const mi_stats_t = mi.stats_t;
const mi_os_tld_t = mi.os_tld_t;
const mi_arena_id_t = mi.arena_id_t;
const mi_commit_mask_t = mi.commit_mask_t;
const mi_segments_tld_t = mi.segments_tld_t;
const mi_span_queue_t = mi.span_queue_t;
const mi_page_kind_t = mi.page_kind_t;
const mi_delayed_t = mi.delayed_t;

// #defines
const MI_DEBUG = mi.DEBUG;
const MI_SECURE = mi.SECURE;
const MI_INTPTR_SIZE = mi.INTPTR_SIZE;
const MI_COMMIT_SIZE = mi.COMMIT_SIZE;
const MI_MINIMAL_COMMIT_SIZE = mi.MINIMAL_COMMIT_SIZE;
const MI_COMMIT_MASK_BITS = mi.COMMIT_MASK_BITS;
const MI_COMMIT_MASK_FIELD_BITS = mi.COMMIT_MASK_FIELD_BITS;
const MI_COMMIT_MASK_FIELD_COUNT = mi.COMMIT_MASK_FIELD_COUNT;

const MI_PAGE_LARGE = mi.MI_PAGE_LARGE;
const MI_PAGE_MEDIUM = mi.MI_PAGE_MEDIUM;
const MI_PAGE_SMALL = mi.MI_PAGE_SMALL;
const MI_MEDIUM_PAGE_SIZE = mi.MI_MEDIUM_PAGE_SIZE;
const MI_LARGE_OBJ_SIZE_MAX = mi.MI_LARGE_OBJ_SIZE_MAX;
const MI_MEDIUM_OBJ_SIZE_MAX = mi.MI_MEDIUM_OBJ_SIZE_MAX;
const MI_SMALL_OBJ_SIZE_MAX = mi.MI_SMALL_OBJ_SIZE_MAX;

const MI_SEGMENT_NORMAL = mi.SEGMENT_NORMAL;
const MI_SEGMENT_MASK = mi.SEGMENT_MASK;
const MI_SEGMENT_HUGE = mi.SEGMENT_HUGE;
const MI_SEGMENT_SIZE = mi.SEGMENT_SIZE;
const MI_SEGMENT_SLICE_SIZE = mi.SEGMENT_SLICE_SIZE;
const MI_SEGMENT_SLICE_SHIFT = mi.SEGMENT_SLICE_SHIFT;
const MI_SEGMENT_BIN_MAX = mi.SEGMENT_BIN_MAX;

const MI_NEVER_DELAYED_FREE = mi.NEVER_DELAYED_FREE;
const MI_HUGE_BLOCK_SIZE = mi.HUGE_BLOCK_SIZE;

const MI_SLICES_PER_SEGMENT = mi.MI_SLICES_PER_SEGMENT;
const MI_MAX_SLICE_OFFSET = mi.MAX_SLICE_OFFSET;
const MI_MAX_ALIGN_GUARANTEE = mi.MAX_ALIGN_GUARANTEE;

const MI_USE_DELAYED_FREE = mi.MI_USE_DELAYED_FREE;

// Function aliases
const mi_bsr = mi.mi_bsr;
const _mi_ptr_cookie = mi._mi_ptr_cookie;
const _mi_thread_id = mi._mi_thread_id;
const _mi_os_page_size = mi._mi_os_page_size;
const _mi_stat_increase = mi._mi_stat_increase;
const _mi_stat_decrease = mi._mi_stat_decrease;
const _mi_align_up = mi._mi_align_up;
const _mi_divide_up = mi._mi_divide_up;
const _mi_align_down = mi._mi_align_down;
const _mi_segment_map_freed_at = mi._mi_segment_map_freed_at;
const mi_option_get = mi.mi_option_get;
const _mi_clock_now = mi._mi_clock_now;

const _mi_page_reclaim = mi._mi_page_reclaim;
const mi_page_all_free = mi.mi_page_all_free;
const _mi_page_free_collect = mi._mi_page_free_collect;
const mi_page_set_heap = mi.mi_page_set_heap;
const _mi_page_use_delayed_free = mi._mi_page_use_delayed_free;
const mi_block_set_next = mi.mi_block_set_next;

const _mi_current_thread_count = mi._mi_current_thread_count;

const _mi_arena_free = mi._mi_arena_free;
const mi_arena_alloc_aligned = mi.mi_arena_alloc_aligned;
const _mi_arena_memid_is_suitable = mi._mi_arena_memid_is_suitable;
const _mi_warning_message = mi._mi_warning_message;

const _mi_heap_memid_is_suitable = mi._mi_heap_memid_is_suitable;

const _mi_os_protect = mi._mi_os_protect;
const _mi_os_unprotect = mi._mi_os_unprotect;
const _mi_os_commit = mi._mi_os_commit;
const _mi_os_decommit = mi._mi_os_decommit;
const _mi_os_reset = mi._mi_os_reset;

const mi_option_decommit_delay = mi.mi_option_decommit_delay;
const mi_option_allow_decommit = mi.mi_option_allow_decommit;
const mi_option_eager_commit = mi.mi_option_eager_commit;
const mi_option_eager_commit_delay = mi.mi_option_eager_commit_delay;
const mi_option_decommit_extend_delay = mi.mi_option_decommit_extend_delay;
const mi_option_page_reset = mi.mi_option_page_reset;
const mi_option_is_enabled = mi.mi_option_is_enabled;
const mi_option_get_clamp = mi.mi_option_get_clamp;
const mi_option_max_segment_reclaim = mi.mi_option_max_segment_reclaim;
const mi_option_abandoned_page_decommit = mi.mi_option_abandoned_page_decommit;

const mi_atomic_load_acquire = mi.mi_atomic_load_acquire;
const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_exchange_acq_rel = mi.mi_atomic_exchange_acq_rel;
const mi_atomic_increment_relaxed = mi.mi_atomic_increment_relaxed;
const mi_atomic_add_relaxed = mi.mi_atomic_add_relaxed;
const mi_atomic_sub_relaxed = mi.mi_atomic_sub_relaxed;
const mi_atomic_decrement_relaxed = mi.mi_atomic_decrement_relaxed;
const mi_atomic_cas_weak_release = mi.mi_atomic_cas_weak_release;
const mi_atomic_cas_weak_acq_rel = mi.mi_atomic_cas_weak_acq_rel;
const mi_atomic_cas_strong_acq_rel = mi.mi_atomic_cas_strong_acq_rel;

const mi_atomic_load_ptr_relaxed = mi.mi_atomic_load_ptr_relaxed;
const mi_atomic_store_ptr_release = mi.mi_atomic_store_ptr_release;
const mi_atomic_cas_ptr_weak_release = mi.mi_atomic_cas_ptr_weak_release;
const mi_atomic_exchange_ptr_acq_rel = mi.mi_atomic_exchange_ptr_acq_rel;
const mi_atomic_yield = mi.mi_atomic_yield;

const mi_heap_get_default = mi.mi_heap_get_default;

const _mi_segment_cache_push = mi._mi_segment_cache_push;
const mi_segment_cache_pop = mi.mi_segment_cache_pop;
const _mi_segment_map_allocated_at = _mi_segment_map_allocated_at;

const mi_track_mem_undefined = mi.mi_track_mem_undefined;

// globals
const _mi_stats_main = mi.stats_main;

// size of a segment
inline fn mi_segment_size(segment: *const mi_segment_t) usize {
    return segment.segment_slices * MI_SEGMENT_SLICE_SIZE;
}

inline fn mi_segment_end(segment: *mi_segment_t) *u8 {
    return @ptrCast(*u8, segment) + mi_segment_size(segment);
}

// Heap access
inline fn mi_page_heap(page: *const mi_page_t) *mi_heap_t {
    return mi_atomic_load_relaxed(&page.xheap);
}

// are there any available blocks?
inline fn mi_page_has_any_available(page: *const mi_page_t) bool {
    mi_assert_internal(page.reserved > 0);
    return (page.used < page.reserved || (mi_page_thread_free(page) != null));
}

inline fn mi_page_to_slice(p: *mi_page_t) *mi_slice_t {
    mi_assert_internal(p.slice_offset == 0 and p.slice_count > 0);
    return @ptrCast(p, *mi_slice_t);
}

inline fn mi_slice_to_page(s: *mi_slice_t) *mi_page_t {
    mi_assert_internal(s.slice_offset == 0 and s.slice_count > 0);
    return @ptrCast(s, *mi_page_t);
}

// Get the block size of a page (special case for huge objects)
inline fn mi_page_block_size(page: *mi_page_t) usize {
    const bsize = page.xblock_size;
    mi_assert_internal(bsize > 0);
    if (mi_likely(bsize < MI_HUGE_BLOCK_SIZE)) {
        return bsize;
    } else {
        var psize: usize = undefined;
        _mi_segment_page_start(_mi_page_segment(page), page, &psize);
        return psize;
    }
}

// Segment that contains the pointer
inline fn _mi_ptr_segment(p: *void) *mi_segment_t {
    // mi_assert_internal(p != NULL);
    return @intToPtr(*mi_segment_t, @ptrToInt(p) & ~MI_SEGMENT_MASK);
}

// Segment belonging to a page
inline fn _mi_page_segment(page: *const mi_page_t) ?*mi_segment_t {
    const segment = _mi_ptr_segment(page);
    mi_assert_internal(segment == null or @ptrCast(*mi_slice_t, page) >= segment.slices and @ptrCast(*mi_slice_t, page) < segment.slices + segment.slice_entries);
    return segment;
}

// Get the page containing the pointer
inline fn _mi_ptr_page(p: *const void) *mi_page_t {
    return _mi_segment_page_of(_mi_ptr_segment(p), p);
}

// Quick page start for initialized pages
inline fn _mi_page_start(segment: *const mi_segment_t, page: *const mi_page_t, page_size: *usize) *u8 {
    return _mi_segment_page_start(segment, page, page_size);
}

// Thread free access
inline fn mi_page_thread_free(page: *const mi_page_t) *mi_block_t {
    return @ptrCast(*mi_block_t, mi_atomic_load_relaxed(page.xthread_free) & ~3);
}

inline fn mi_page_thread_free_flag(page: *const mi_page_t) mi_delayed_t {
    return @intToEnum(mi_delayed_t, mi_atomic_load_relaxed(page.xthread_free) & 3);
}

inline fn mi_slice_first(slice: *const mi_slice_t) *mi_slice_t {
    const start = @ptrCast(*mi_slice_t, @ptrCast(*u8, slice) - slice.slice_offset);
    mi_assert_internal(start >= _mi_ptr_segment(slice).slices);
    mi_assert_internal(start.slice_offset == 0);
    mi_assert_internal(start + start.slice_count > slice);
    return start;
}

inline fn _mi_segment_page_of(segment: *const mi_segment_t, p: *const void) *mi_page_t {
    const diff = @ptrCast(*u8, p) - @ptrCast(*u8, segment);
    mi_assert_internal(diff >= 0 and diff < MI_SEGMENT_SIZE);
    const idx = diff >> MI_SEGMENT_SLICE_SHIFT;
    mi_assert_internal(idx < segment.slice_entries);
    const slice0 = &segment.slices[idx];
    const slice = mi_slice_first(slice0); // adjust to the block that holds the page data
    mi_assert_internal(slice.slice_offset == 0);
    mi_assert_internal(slice >= segment.slices and slice < segment.slices + segment.slice_entries);
    return mi_slice_to_page(slice);
}

// -------------------------------------------------------------------
// commit mask
// -------------------------------------------------------------------

fn mi_commit_mask_create_empty(cm: *mi_commit_mask_t) void {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        cm.mask[i] = 0;
    }
}

fn mi_commit_mask_create_full(cm: *mi_commit_mask_t) void {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        cm.mask[i] = ~@intCast(usize, 0);
    }
}

fn mi_commit_mask_is_empty(cm: *const mi_commit_mask_t) bool {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        if (cm.mask[i] != 0) return false;
    }
    return true;
}

fn mi_commit_mask_is_full(cm: *const mi_commit_mask_t) bool {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        if (cm.mask[i] != ~@intCast(usize, 0)) return false;
    }
    return true;
}

fn mi_commit_mask_all_set(commit: *const mi_commit_mask_t, cm: *const mi_commit_mask_t) bool {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        if ((commit.mask[i] & cm.mask[i]) != cm.mask[i]) return false;
    }
    return true;
}

fn mi_commit_mask_any_set(commit: *const mi_commit_mask_t, cm: *const mi_commit_mask_t) bool {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        if ((commit.mask[i] & cm.mask[i]) != 0) return true;
    }
    return false;
}

fn mi_commit_mask_create_intersect(commit: *const mi_commit_mask_t, cm: *const mi_commit_mask_t, res: *mi_commit_mask_t) void {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        res.mask[i] = (commit.mask[i] & cm.mask[i]);
    }
}

fn mi_commit_mask_clear(res: *mi_commit_mask_t, cm: *const mi_commit_mask_t) void {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        res.mask[i] &= ~(cm.mask[i]);
    }
}

fn mi_commit_mask_set(res: *mi_commit_mask_t, cm: *const mi_commit_mask_t) void {
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        res.mask[i] |= cm.mask[i];
    }
}

fn mi_commit_mask_create(bitidx: usize, bitcount: usize, cm: *mi_commit_mask_t) void {
    mi_assert_internal(bitidx < MI_COMMIT_MASK_BITS);
    mi_assert_internal((bitidx + bitcount) <= MI_COMMIT_MASK_BITS);
    if (bitcount == MI_COMMIT_MASK_BITS) {
        mi_assert_internal(bitidx == 0);
        mi_commit_mask_create_full(cm);
    } else if (bitcount == 0) {
        mi_commit_mask_create_empty(cm);
    } else {
        mi_commit_mask_create_empty(cm);
        const i = bitidx / MI_COMMIT_MASK_FIELD_BITS;
        const ofs = bitidx % MI_COMMIT_MASK_FIELD_BITS;
        var bc = bitcount;
        while (bc > 0) {
            mi_assert_internal(i < MI_COMMIT_MASK_FIELD_COUNT);
            const avail = MI_COMMIT_MASK_FIELD_BITS - ofs;
            const count = if (bitcount > avail) avail else bitcount;
            const mask = if (count >= MI_COMMIT_MASK_FIELD_BITS) ~@intCast(usize, 0) else ((@intCast(usize, 1) << count) - 1) << ofs;
            cm.mask[i] = mask;
            bc -= count;
            ofs = 0;
            i += 1;
        }
    }
}

pub fn _mi_commit_mask_committed_size(cm: *const mi_commit_mask_t, total: usize) usize {
    mi_assert_internal((total % MI_COMMIT_MASK_BITS) == 0);
    var count: usize = 0;
    var i: usize = 0;
    while (i < MI_COMMIT_MASK_FIELD_COUNT) : (i += 1) {
        var mask = cm.mask[i];
        if (~mask == 0) {
            count += MI_COMMIT_MASK_FIELD_BITS;
        } else {
            count += @popCount(mask);
        }
    }
    // we use total since for huge segments each commit bit may represent a larger size
    return ((total / MI_COMMIT_MASK_BITS) * count);
}

pub fn _mi_commit_mask_next_run(cm: *const mi_commit_mask_t, idx: *usize) usize {
    var i = (*idx) / MI_COMMIT_MASK_FIELD_BITS;
    var ofs = (*idx) % MI_COMMIT_MASK_FIELD_BITS;
    var mask: usize = 0;
    // find first ones
    while (i < MI_COMMIT_MASK_FIELD_COUNT) {
        mask = cm.mask[i];
        mask >>= ofs;
        if (mask != 0) {
            while ((mask & 1) == 0) {
                mask >>= 1;
                ofs += 1;
            }
            break;
        }
        i += 1;
        ofs = 0;
    }
    if (i >= MI_COMMIT_MASK_FIELD_COUNT) {
        // not found
        idx.* = MI_COMMIT_MASK_BITS;
        return 0;
    } else {
        // found, count ones
        var count: usize = 0;
        idx.* = (i * MI_COMMIT_MASK_FIELD_BITS) + ofs;
        while (true) {
            mi_assert_internal(ofs < MI_COMMIT_MASK_FIELD_BITS and (mask & 1) == 1);
            while (true) {
                count += 1;
                mask >>= 1;
                if (!(mask & 1) == 1) break;
            }
            if ((((*idx + count) % MI_COMMIT_MASK_FIELD_BITS) == 0)) {
                i += 1;
                if (i >= MI_COMMIT_MASK_FIELD_COUNT) break;
                mask = cm.mask[i];
                ofs = 0;
            }
            if ((mask & 1) != 1) break;
        }
        mi_assert_internal(count > 0);
        return count;
    }
}

//--------------------------------------------------------------------------------
// Segment allocation
//
// If a thread ends, it "abandons" pages with used blocks
// and there is an abandoned segment list whose segments can
// be reclaimed by still running threads, much like work-stealing.
//--------------------------------------------------------------------------------

//-----------------------------------------------------------
//   Slices
//-----------------------------------------------------------

fn mi_segment_slices_end(segment: *const mi_segment_t) *const mi_slice_t {
    return &segment.slices[segment.slice_entries];
}

fn mi_slice_start(slice: *const mi_slice_t) *u8 {
    const segment = _mi_ptr_segment(slice);
    mi_assert_internal(slice >= segment.slices and slice < mi_segment_slices_end(segment));
    return @ptrCast(*u8, segment + ((slice - segment.slices) * MI_SEGMENT_SLICE_SIZE));
}

//-----------------------------------------------------------
//   Bins
//-----------------------------------------------------------
// Use bit scan forward to quickly find the first zero bit if it is available

fn mi_slice_bin8(slice_count: usize) usize {
    if (slice_count <= 1) return slice_count;
    mi_assert_internal(slice_count <= MI_SLICES_PER_SEGMENT);
    slice_count -= 1;
    const s = mi_bsr(slice_count); // slice_count > 1
    if (s <= 2) return slice_count + 1;
    const bin = ((s << 2) | ((slice_count >> (s - 2)) & 0x03)) - 4;
    return bin;
}

fn mi_slice_bin(slice_count: usize) usize {
    mi_assert_internal(slice_count * MI_SEGMENT_SLICE_SIZE <= MI_SEGMENT_SIZE);
    mi_assert_internal(mi_slice_bin8(MI_SLICES_PER_SEGMENT) <= MI_SEGMENT_BIN_MAX);
    const bin = mi_slice_bin8(slice_count);
    mi_assert_internal(bin <= MI_SEGMENT_BIN_MAX);
    return bin;
}

fn mi_slice_index(slice: *const mi_slice_t) usize {
    const segment = _mi_ptr_segment(slice);
    const index = slice - segment.slices;
    mi_assert_internal(index >= 0 and index < segment.slice_entries);
    return index;
}

//-----------------------------------------------------------
//   Slice span queues
//-----------------------------------------------------------

fn mi_span_queue_push(sq: *mi_span_queue_t, slice: *mi_slice_t) void {
    // todo: or push to the end?
    mi_assert_internal(slice.prev == null and slice.next == null);
    slice.prev = null; // paranoia
    slice.next = sq.first;
    sq.first = slice;
    if (slice.next != null)
        slice.next.prev = slice
    else
        sq.last = slice;
    slice.xblock_size = 0; // free
}

fn mi_span_queue_for(slice_count: usize, tld: *mi_segments_tld_t) *mi_span_queue_t {
    const bin = mi_slice_bin(slice_count);
    const sq = &tld.spans[bin];
    mi_assert_internal(sq.slice_count >= slice_count);
    return sq;
}

fn mi_span_queue_delete(sq: *mi_span_queue_t, slice: *mi_slice_t) void {
    mi_assert_internal(slice.xblock_size == 0 and slice.slice_count > 0 and slice.slice_offset == 0);
    // should work too if the queue does not contain slice (which can happen during reclaim)
    if (slice.prev != null) slice.prev.next = slice.next;
    if (slice == sq.first) sq.first = slice.next;
    if (slice.next != null) slice.next.prev = slice.prev;
    if (slice == sq.last) sq.last = slice.prev;
    slice.prev = null;
    slice.next = null;
    slice.xblock_size = 1; // no more free
}

//-----------------------------------------------------------
// Invariant checking
//-----------------------------------------------------------

fn mi_slice_is_used(slice: *const mi_slice_t) bool {
    return (slice.xblock_size > 0);
}

fn mi_span_queue_contains(sq: *const mi_span_queue_t, slice: *const mi_slice_t) bool {
    if (MI_DEBUG < 3) return;
    var s: ?*const mi_span_queue_t = sq.first;
    while (s != null) : (s = s.next) {
        if (s == slice) return true;
    }
    return false;
}

fn mi_segment_is_valid(segment: *mi_segment_t, tld: *mi_segments_tld_t) bool {
    if (MI_DEBUG < 3) return;
    mi_assert_internal(segment != null);
    mi_assert_internal(_mi_ptr_cookie(segment) == segment.cookie);
    mi_assert_internal(segment.abandoned <= segment.used);
    mi_assert_internal(segment.thread_id == 0 or segment.thread_id == _mi_thread_id());
    mi_assert_internal(mi_commit_mask_all_set(&segment.commit_mask, &segment.decommit_mask)); // can only decommit committed blocks
    //mi_assert_internal(segment.segment_info_size % MI_SEGMENT_SLICE_SIZE == 0);
    var slice = &segment.slices[0];
    const end = mi_segment_slices_end(segment);
    var used_count: usize = 0;
    while (slice < end) {
        mi_assert_internal(slice.slice_count > 0);
        mi_assert_internal(slice.slice_offset == 0);
        const index = mi_slice_index(slice);
        const maxindex = (if (index + slice.slice_count >= segment.slice_entries) segment.slice_entries else index + slice.slice_count) - 1;
        if (mi_slice_is_used(slice)) { // a page in use, we need at least MAX_SLICE_OFFSET valid back offsets
            used_count += 1;
            var i: usize = 0;
            while (i <= MI_MAX_SLICE_OFFSET and index + i <= maxindex) : (i += 1) {
                mi_assert_internal(segment.slices[index + i].slice_offset == i * @sizeOf(mi_slice_t));
                mi_assert_internal(i == 0 or segment.slices[index + i].slice_count == 0);
                mi_assert_internal(i == 0 or segment.slices[index + i].xblock_size == 1);
            }
            // and the last entry as well (for coalescing)
            const last = slice + slice.slice_count - 1;
            if (last > slice and last < mi_segment_slices_end(segment)) {
                mi_assert_internal(last.slice_offset == (slice.slice_count - 1) * @sizeOf(mi_slice_t));
                mi_assert_internal(last.slice_count == 0);
                mi_assert_internal(last.xblock_size == 1);
            }
        } else { // free range of slices; only last slice needs a valid back offset
            const last = &segment.slices[maxindex];
            if (segment.kind != MI_SEGMENT_HUGE or slice.slice_count <= (segment.slice_entries - segment.segment_info_slices)) {
                mi_assert_internal(slice == last - last.slice_offset);
            }
            mi_assert_internal(slice == last or last.slice_count == 0);
            mi_assert_internal(last.xblock_size == 0 or (segment.kind == MI_SEGMENT_HUGE and last.xblock_size == 1));
            if (segment.kind != MI_SEGMENT_HUGE and segment.thread_id != 0) { // segment is not huge or abandoned
                const sq = mi_span_queue_for(slice.slice_count, tld);
                mi_assert_internal(mi_span_queue_contains(sq, slice));
            }
        }
        slice = &segment.slices[maxindex + 1];
    }
    mi_assert_internal(slice == end);
    mi_assert_internal(used_count == segment.used + 1);
    return true;
}

//-----------------------------------------------------------
// Segment size calculations
//-----------------------------------------------------------

fn mi_segment_info_size(segment: *mi_segment_t) usize {
    return segment.segment_info_slices * MI_SEGMENT_SLICE_SIZE;
}

fn _mi_segment_page_start_from_slice(segment: *const mi_segment_t, slice: *const mi_slice_t, xblock_size: usize, page_size: ?*usize) *const u8 {
    const idx = slice - segment.slices;
    const psize = slice.slice_count * MI_SEGMENT_SLICE_SIZE;
    // make the start not OS page aligned for smaller blocks to avoid page/cache effects
    const start_offset = if (xblock_size >= MI_INTPTR_SIZE and xblock_size <= 1024) MI_MAX_ALIGN_GUARANTEE else 0;
    if (page_size != null) {
        page_size.* = psize - start_offset;
    }
    return @ptrCast(*const u8, segment) + ((idx * MI_SEGMENT_SLICE_SIZE) + start_offset);
}

// Start of the page available memory; can be used on uninitialized pages
fn _mi_segment_page_start(segment: *const mi_segment_t, page: *const mi_page_t, page_size: ?*usize) *const u8 {
    const slice = mi_page_to_slice(page);
    const p = _mi_segment_page_start_from_slice(segment, slice, page.xblock_size, page_size);
    mi_assert_internal(page.xblock_size > 0 or _mi_ptr_page(p) == page);
    mi_assert_internal(_mi_ptr_segment(p) == segment);
    return p;
}

fn mi_segment_calculate_slices(required: usize, pre_size: ?*usize, info_slices: ?*usize) usize {
    const page_size = _mi_os_page_size();
    var size = _mi_align_up(@sizeOf(mi_segment_t), page_size);
    var guardsize: usize = 0;

    if (MI_SECURE > 0) {
        // in secure mode, we set up a protected page in between the segment info
        // and the page data (and one at the end of the segment)
        guardsize = page_size;
        required = _mi_align_up(required, page_size);
    }

    if (pre_size != null) pre_size.* = size;
    size = _mi_align_up(isize + guardsize, MI_SEGMENT_SLICE_SIZE);
    if (info_slices != null) info_slices.* = size / MI_SEGMENT_SLICE_SIZE;
    const segment_size = if (required == 0) MI_SEGMENT_SIZE else _mi_align_up(required + isize + guardsize, MI_SEGMENT_SLICE_SIZE);
    mi_assert_internal(segment_size % MI_SEGMENT_SLICE_SIZE == 0);
    return (segment_size / MI_SEGMENT_SLICE_SIZE);
}

//-------------------------------------------------------------------------------
// Segment caches
// We keep a small segment cache per thread to increase local
// reuse and avoid setting/clearing guard pages in secure mode.
//-------------------------------------------------------------------------------

fn mi_segments_track_size(segment_size: usize, tld: *mi_segments_tld_t) void {
    if (segment_size >= 0) _mi_stat_increase(&tld.stats.segments, 1) else _mi_stat_decrease(&tld.stats.segments, 1);
    tld.count += if (segment_size >= 0) 1 else -1;
    if (tld.count > tld.peak_count) tld.peak_count = tld.count;
    tld.current_size += segment_size;
    if (tld.current_size > tld.peak_size) tld.peak_size = tld.current_size;
}

fn mi_segment_os_free(segment: *mi_segment_t, tld: *mi_segments_tld_t) void {
    segment.thread_id = 0;
    _mi_segment_map_freed_at(segment);
    mi_segments_track_size(-@intCast(isize, mi_segment_size(segment)), tld);
    if (MI_SECURE > 0) {
        // _mi_os_unprotect(segment, mi_segment_size(segment)); // ensure no more guard pages are set
        // unprotect the guard pages; we cannot just unprotect the whole segment size as part may be decommitted
        const os_pagesize = _mi_os_page_size();
        _mi_os_unprotect(@ptrCast(*u8, segment) + mi_segment_info_size(segment) - os_pagesize, os_pagesize);
        const end = @ptrCast(*const u8, segment) + mi_segment_size(segment) - os_pagesize;
        _mi_os_unprotect(end, os_pagesize);
    }

    // purge delayed decommits now? (no, leave it to the cache)
    // mi_segment_delayed_decommit(segment,true,tld.stats);

    // _mi_os_free(segment, mi_segment_size(segment), /*segment.memid,*/ tld.stats);
    const size = mi_segment_size(segment);
    if (size != MI_SEGMENT_SIZE or !_mi_segment_cache_push(segment, size, segment.memid, &segment.commit_mask, &segment.decommit_mask, segment.mem_is_large, segment.mem_is_pinned, tld.os)) {
        const csize = _mi_commit_mask_committed_size(&segment.commit_mask, size);
        if (csize > 0 and !segment.mem_is_pinned)
            _mi_stat_decrease(&_mi_stats_main.committed, csize);
        _mi_abandoned_await_readers(); // wait until safe to free
        // pretend not committed to not double count decommits
        _mi_arena_free(segment, mi_segment_size(segment), segment.memid, segment.mem_is_pinned, tld.os);
    }
}

// called by threads that are terminating
fn _mi_segment_thread_collect(tld: *mi_segments_tld_t) void {
    _ = tld;
    // nothing to do
}

//-----------------------------------------------------------
//   Span management
//-----------------------------------------------------------

fn mi_segment_commit_mask(segment: *mi_segment_t, conservative: bool, p: *u8, size: usize, start_p: **u8, full_size: *usize, cm: *mi_commit_mask_t) void {
    mi_assert_internal(_mi_ptr_segment(p) == segment);
    mi_assert_internal(segment.kind != MI_SEGMENT_HUGE);
    mi_commit_mask_create_empty(cm);
    if (size == 0 or size > MI_SEGMENT_SIZE or segment.kind == MI_SEGMENT_HUGE) return;
    const segstart = mi_segment_info_size(segment);
    const segsize = mi_segment_size(segment);
    if (p >= @ptrCast(*u8, segment) + segsize) return;

    const pstart = p - @ptrCast(*u8, segment);
    mi_assert_internal(pstart + size <= segsize);

    var start: usize = undefined;
    var end: usize = undefined;
    if (conservative) {
        // decommit conservative
        start = _mi_align_up(pstart, MI_COMMIT_SIZE);
        end = _mi_align_down(pstart + size, MI_COMMIT_SIZE);
        mi_assert_internal(start >= segstart);
        mi_assert_internal(end <= segsize);
    } else {
        // commit liberal
        start = _mi_align_down(pstart, MI_MINIMAL_COMMIT_SIZE);
        end = _mi_align_up(pstart + size, MI_MINIMAL_COMMIT_SIZE);
    }
    if (pstart >= segstart and start < segstart) { // note: the mask is also calculated for an initial commit of the info area
        start = segstart;
    }
    if (end > segsize) {
        end = segsize;
    }

    mi_assert_internal(start <= pstart and (pstart + size) <= end);
    mi_assert_internal(start % MI_COMMIT_SIZE == 0 and end % MI_COMMIT_SIZE == 0);
    start_p.* = @ptrCast(*u8, segment) + start;
    full_size.* = if (end > start) end - start else 0;
    if (*full_size == 0) return;

    const bitidx = start / MI_COMMIT_SIZE;
    mi_assert_internal(bitidx < MI_COMMIT_MASK_BITS);

    const bitcount = *full_size / MI_COMMIT_SIZE; // can be 0
    if (bitidx + bitcount > MI_COMMIT_MASK_BITS) {
        _mi_warning_message("commit mask overflow: idx=%zu count=%zu start=%zx end=%zx p=0x%p size=%zu fullsize=%zu\n", bitidx, bitcount, start, end, p, size, *full_size);
    }
    mi_assert_internal((bitidx + bitcount) <= MI_COMMIT_MASK_BITS);
    mi_commit_mask_create(bitidx, bitcount, cm);
}

fn mi_segment_commitx(segment: *mi_segment_t, commit: bool, p: *u8, size: usize, stats: *mi_stats_t) bool {
    mi_assert_internal(mi_commit_mask_all_set(&segment.commit_mask, &segment.decommit_mask));

    // try to commit in at least MI_MINIMAL_COMMIT_SIZE sizes.
    if (false and commit and size > 0) {
        const csize = _mi_align_up(size, MI_MINIMAL_COMMIT_SIZE);
        if (p + csize <= mi_segment_end(segment)) {
            size = csize;
        }
    }
    // commit liberal, but decommit conservative
    var start = null;
    const full_size: usize = 0;
    var mask: mi_commit_mask_t = undefined;
    // conservative (!commit)
    mi_segment_commit_mask(segment, !commit, p, size, &start, &full_size, &mask);
    if (mi_commit_mask_is_empty(&mask) or full_size == 0) return true;

    if (commit and !mi_commit_mask_all_set(&segment.commit_mask, &mask)) {
        var is_zero: bool = false;
        var cmask: mi_commit_mask_t = undefined;
        mi_commit_mask_create_intersect(&segment.commit_mask, &mask, &cmask);
        _mi_stat_decrease(&_mi_stats_main.committed, _mi_commit_mask_committed_size(&cmask, MI_SEGMENT_SIZE)); // adjust for overlap
        if (!_mi_os_commit(start, full_size, &is_zero, stats)) return false;
        mi_commit_mask_set(&segment.commit_mask, &mask);
    } else if (!commit and mi_commit_mask_any_set(&segment.commit_mask, &mask)) {
        mi_assert_internal(start != segment);
        //mi_assert_internal(mi_commit_mask_all_set(&segment.commit_mask, &mask));

        var cmask: mi_commit_mask_t = undefined;
        mi_commit_mask_create_intersect(&segment.commit_mask, &mask, &cmask);
        _mi_stat_increase(&_mi_stats_main.committed, full_size - _mi_commit_mask_committed_size(&cmask, MI_SEGMENT_SIZE)); // adjust for overlap
        if (segment.allow_decommit) {
            _mi_os_decommit(start, full_size, stats); // ok if this fails
        }
        mi_commit_mask_clear(&segment.commit_mask, &mask);
    }
    // increase expiration of reusing part of the delayed decommit
    if (commit and mi_commit_mask_any_set(&segment.decommit_mask, &mask)) {
        segment.decommit_expire = _mi_clock_now() + mi_option_get(mi_option_decommit_delay);
    }
    // always undo delayed decommits
    mi_commit_mask_clear(&segment.decommit_mask, &mask);
    return true;
}

fn mi_segment_ensure_committed(segment: *mi_segment_t, p: *u8, size: usize, stats: *mi_stats_t) bool {
    mi_assert_internal(mi_commit_mask_all_set(&segment.commit_mask, &segment.decommit_mask));
    // note: assumes commit_mask is always full for huge segments as otherwise the commit mask bits can overflow
    if (mi_commit_mask_is_full(&segment.commit_mask) and mi_commit_mask_is_empty(&segment.decommit_mask)) return true; // fully committed
    return mi_segment_commitx(segment, true, p, size, stats);
}

fn mi_segment_perhaps_decommit(segment: *mi_segment_t, p: *u8, size: usize, stats: *mi_stats_t) void {
    if (!segment.allow_decommit) return;
    if (mi_option_get(mi_option_decommit_delay) == 0) {
        mi_segment_commitx(segment, false, p, size, stats);
    } else {
        // register for future decommit in the decommit mask
        var start: ?*u8 = null;
        var full_size: usize = 0;
        var mask: mi_commit_mask_t = undefined;
        // conservative
        mi_segment_commit_mask(segment, true, p, size, &start, &full_size, &mask);
        if (mi_commit_mask_is_empty(&mask) or full_size == 0) return;

        // update delayed commit
        mi_assert_internal(segment.decommit_expire > 0 or mi_commit_mask_is_empty(&segment.decommit_mask));
        var cmask: mi_commit_mask_t = undefined;
        mi_commit_mask_create_intersect(&segment.commit_mask, &mask, &cmask); // only decommit what is committed; span_free may try to decommit more
        mi_commit_mask_set(&segment.decommit_mask, &cmask);
        const now = _mi_clock_now();
        if (segment.decommit_expire == 0) {
            // no previous decommits, initialize now
            segment.decommit_expire = now + mi_option_get(mi_option_decommit_delay);
        } else if (segment.decommit_expire <= now) {
            // previous decommit mask already expired
            // mi_segment_delayed_decommit(segment, true, stats);
            segment.decommit_expire = now + mi_option_get(mi_option_decommit_extend_delay); // (mi_option_get(mi_option_decommit_delay) / 8); // wait a tiny bit longer in case there is a series of free's
        } else {
            // previous decommit mask is not yet expired, increase the expiration by a bit.
            segment.decommit_expire += mi_option_get(mi_option_decommit_extend_delay);
        }
    }
}

fn mi_segment_delayed_decommit(segment: *mi_segment_t, force: bool, stats: *mi_stats_t) void {
    if (!segment.allow_decommit or mi_commit_mask_is_empty(&segment.decommit_mask)) return;
    const now = _mi_clock_now();
    if (!force and now < segment.decommit_expire) return;

    const mask = segment.decommit_mask;
    segment.decommit_expire = 0;
    mi_commit_mask_create_empty(&segment.decommit_mask);

    var idx: usize = 0;
    var count = _mi_commit_mask_next_run(&mask, &idx);
    while (count > 0) : (count = _mi_commit_mask_next_run(&mask, &idx)) {
        // if found, decommit that sequence
        if (count > 0) {
            const p = @ptrCast(*u8, segment) + (idx * MI_COMMIT_SIZE);
            const size = count * MI_COMMIT_SIZE;
            mi_segment_commitx(segment, false, p, size, stats);
        }
    }
    mi_assert_internal(mi_commit_mask_is_empty(&segment.decommit_mask));
}

fn mi_segment_is_abandoned(segment: *mi_segment_t) bool {
    return (segment.thread_id == 0);
}

// note: can be called on abandoned segments
fn mi_segment_span_free(segment: *mi_segment_t, slice_index: usize, slice_count: usize, tld: *mi_segments_tld_t) void {
    mi_assert_internal(slice_index < segment.slice_entries);
    const sq: ?*mi_span_queue_t = if (segment.kind == MI_SEGMENT_HUGE or mi_segment_is_abandoned(segment))
        null
    else
        mi_span_queue_for(slice_count, tld);
    if (slice_count == 0) slice_count = 1;
    mi_assert_internal(slice_index + slice_count - 1 < segment.slice_entries);

    // set first and last slice (the intermediates can be undetermined)
    const slice = &segment.slices[slice_index];
    slice.slice_count = slice_count;
    mi_assert_internal(slice.slice_count == slice_count); // no overflow?
    slice.slice_offset = 0;
    if (slice_count > 1) {
        const last = &segment.slices[slice_index + slice_count - 1];
        last.slice_count = 0;
        last.slice_offset = @intCast(u32, @sizeOf(mi_page_t) * (slice_count - 1));
        last.xblock_size = 0;
    }

    // perhaps decommit
    mi_segment_perhaps_decommit(segment, mi_slice_start(slice), slice_count * MI_SEGMENT_SLICE_SIZE, tld.stats);

    // and push it on the free page queue (if it was not a huge page)
    if (sq != null) mi_span_queue_push(sq, slice) else slice.xblock_size = 0; // mark huge page as free anyways
}

fn mi_segment_span_remove_from_queue(slice: *mi_slice_t, tld: *mi_segments_tld_t) void {
    mi_assert_internal(slice.slice_count > 0 and slice.slice_offset == 0 and slice.xblock_size == 0);
    mi_assert_internal(_mi_ptr_segment(slice).kind != MI_SEGMENT_HUGE);
    const sq = mi_span_queue_for(slice.slice_count, tld);
    mi_span_queue_delete(sq, slice);
}

// note: can be called on abandoned segments
fn mi_segment_span_free_coalesce(slice: *mi_slice_t, tld: *mi_segments_tld_t) *mi_slice_t {
    mi_assert_internal(slice != null and slice.slice_count > 0 and slice.slice_offset == 0);
    const segment = _mi_ptr_segment(slice);
    const is_abandoned = mi_segment_is_abandoned(segment);

    // for huge pages, just mark as free but don't add to the queues
    if (segment.kind == MI_SEGMENT_HUGE) {
        mi_assert_internal(segment.used == 1); // decreased right after this call in `mi_segment_page_clear`
        slice.xblock_size = 0; // mark as free anyways
        // we should mark the last slice `xblock_size=0` now to maintain invariants but we skip it to
        // avoid a possible cache miss (and the segment is about to be freed)
        return slice;
    }

    // otherwise coalesce the span and add to the free span queues
    const slice_count: usize = slice.slice_count;
    var next = slice + slice.slice_count;
    mi_assert_internal(next <= mi_segment_slices_end(segment));
    if (next < mi_segment_slices_end(segment) and next.xblock_size == 0) {
        // free next block -- remove it from free and merge
        mi_assert_internal(next.slice_count > 0 and next.slice_offset == 0);
        slice_count += next.slice_count; // extend
        if (!is_abandoned) {
            mi_segment_span_remove_from_queue(next, tld);
        }
    }
    if (slice > segment.slices) {
        const prev = mi_slice_first(slice - 1);
        mi_assert_internal(prev >= segment.slices);
        if (prev.xblock_size == 0) {
            // free previous slice -- remove it from free and merge
            mi_assert_internal(prev.slice_count > 0 and prev.slice_offset == 0);
            slice_count += prev.slice_count;
            if (!is_abandoned) {
                mi_segment_span_remove_from_queue(prev, tld);
            }
            slice = prev;
        }
    }

    // and add the new free page
    mi_segment_span_free(segment, mi_slice_index(slice), slice_count, tld);
    return slice;
}

fn mi_segment_slice_split(segment: *mi_segment_t, slice: *mi_slice_t, slice_count: usize, tld: *mi_segments_tld_t) void {
    mi_assert_internal(_mi_ptr_segment(slice) == segment);
    mi_assert_internal(slice.slice_count >= slice_count);
    mi_assert_internal(slice.xblock_size > 0); // no more in free queue
    if (slice.slice_count <= slice_count) return;
    mi_assert_internal(segment.kind != MI_SEGMENT_HUGE);
    const next_index = mi_slice_index(slice) + slice_count;
    const next_count = slice.slice_count - slice_count;
    mi_segment_span_free(segment, next_index, next_count, tld);
    slice.slice_count = slice_count;
}

// Note: may still return null if committing the memory failed
fn mi_segment_span_allocate(segment: *mi_segment_t, slice_index: usize, slice_count: usize, tld: *mi_segments_tld_t) ?*mi_page_t {
    mi_assert_internal(slice_index < segment.slice_entries);
    var slice = &segment.slices[slice_index];
    mi_assert_internal(slice.xblock_size == 0 or slice.xblock_size == 1);

    // commit before changing the slice data
    if (!mi_segment_ensure_committed(segment, _mi_segment_page_start_from_slice(segment, slice, 0, null), slice_count * MI_SEGMENT_SLICE_SIZE, tld.stats)) {
        return null; // commit failed!
    }

    // convert the slices to a page
    slice.slice_offset = 0;
    slice.slice_count = slice_count;
    mi_assert_internal(slice.slice_count == slice_count);
    const bsize = slice_count * MI_SEGMENT_SLICE_SIZE;
    slice.xblock_size = if (bsize >= MI_HUGE_BLOCK_SIZE) MI_HUGE_BLOCK_SIZE else bsize;
    const page = mi_slice_to_page(slice);
    mi_assert_internal(mi_page_block_size(page) == bsize);

    // set slice back pointers for the first MI_MAX_SLICE_OFFSET entries
    const extra = slice_count - 1;
    if (extra > MI_MAX_SLICE_OFFSET) extra = MI_MAX_SLICE_OFFSET;
    if (slice_index + extra >= segment.slice_entries) extra = segment.slice_entries - slice_index - 1; // huge objects may have more slices than avaiable entries in the segment.slices
    slice += 1;

    var i: usize = 1;
    while (i <= extra) : (i += 1) {
        slice.slice_offset = @intCast(u32, @sizeOf(mi_slice_t) * i);
        slice.slice_count = 0;
        slice.xblock_size = 1;
        slice += 1;
    }

    // and also for the last one (if not set already) (the last one is needed for coalescing)
    // note: the cast is needed for ubsan since the index can be larger than MI_SLICES_PER_SEGMENT for huge allocations (see #543)
    const last = &segment.slices[slice_index + slice_count - 1];
    if (last < mi_segment_slices_end(segment) and last >= slice) {
        last.slice_offset = (@sizeOf(mi_slice_t) * (slice_count - 1));
        last.slice_count = 0;
        last.xblock_size = 1;
    }

    // and initialize the page
    page.is_reset = false;
    page.is_committed = true;
    segment.used += 1;
    return page;
}

fn mi_segments_page_find_and_allocate(slice_count_in: usize, req_arena_id: mi_arena_id_t, tld: *mi_segments_tld_t) *mi_page_t {
    var slice_count = slice_count_in;
    mi_assert_internal(slice_count * MI_SEGMENT_SLICE_SIZE <= MI_LARGE_OBJ_SIZE_MAX);
    // search from best fit up
    var sq = mi_span_queue_for(slice_count, tld);
    if (slice_count == 0) slice_count = 1;
    while (sq <= &tld.spans[MI_SEGMENT_BIN_MAX]) {
        var slice = sq.first;
        while (slice != null) : (slice = slice.next) {
            if (slice.slice_count >= slice_count) {
                // found one
                const segment = _mi_ptr_segment(slice);
                if (_mi_arena_memid_is_suitable(segment.memid, req_arena_id)) {
                    // found a suitable page span
                    mi_span_queue_delete(sq, slice);

                    if (slice.slice_count > slice_count) {
                        mi_segment_slice_split(segment, slice, slice_count, tld);
                    }
                    mi_assert_internal(slice != null and slice.slice_count == slice_count and slice.xblock_size > 0);
                    const page = mi_segment_span_allocate(segment, mi_slice_index(slice), slice.slice_count, tld);
                    if (page == null) {
                        // commit failed; return null but first restore the slice
                        mi_segment_span_free_coalesce(slice, tld);
                        return null;
                    }
                    return page;
                }
            }
        }
        sq += 1;
    }
    // could not find a page..
    return null;
}

//-----------------------------------------------------------
//   Segment allocation
//-----------------------------------------------------------

// Allocate a segment from the OS aligned to `MI_SEGMENT_SIZE` .
fn mi_segment_init(segment: ?*mi_segment_t, required: usize, req_arena_id: mi_arena_id_t, tld: *mi_segments_tld_t, os_tld: *mi_os_tld_t, huge_page: ?**mi_page_t) ?*mi_segment_t {
    mi_assert_internal((required == 0 and huge_page == null) or (required > 0 and huge_page != null));
    mi_assert_internal((segment == null) or (segment != null and required == 0));
    // calculate needed sizes first
    var info_slices: usize = undefined;
    var pre_size: usize = undefined;
    const segment_slices = mi_segment_calculate_slices(required, &pre_size, &info_slices);
    const slice_entries = if (segment_slices > MI_SLICES_PER_SEGMENT) MI_SLICES_PER_SEGMENT else segment_slices;
    const segment_size = segment_slices * MI_SEGMENT_SLICE_SIZE;

    // Commit eagerly only if not the first N lazy segments (to reduce impact of many threads that allocate just a little)
    const eager_delay = ( // !_mi_os_has_overcommit() and             // never delay on overcommit systems
        _mi_current_thread_count() > 1 and // do not delay for the first N threads
        tld.count < mi_option_get(mi_option_eager_commit_delay));
    const eager = !eager_delay and mi_option_is_enabled(mi_option_eager_commit);
    const commit = eager or (required > 0);

    // Try to get from our cache first
    const is_zero = false;
    const commit_info_still_good = (segment != null);
    var commit_mask: mi_commit_mask_t = undefined;
    var decommit_mask: mi_commit_mask_t = undefined;
    if (segment != null) {
        commit_mask = segment.commit_mask;
        decommit_mask = segment.decommit_mask;
    } else {
        mi_commit_mask_create_empty(&commit_mask);
        mi_commit_mask_create_empty(&decommit_mask);
    }
    if (segment == null) {
        // Allocate the segment from the OS
        const mem_large = (!eager_delay and (MI_SECURE == 0)); // only allow large OS pages once we are no longer lazy
        var is_pinned = false;
        var memid: usize = 0;
        segment = mi_segment_cache_pop(segment_size, &commit_mask, &decommit_mask, &mem_large, &is_pinned, &is_zero, req_arena_id, &memid, os_tld);
        if (segment == null) {
            segment = mi_arena_alloc_aligned(segment_size, MI_SEGMENT_SIZE, &commit, &mem_large, &is_pinned, &is_zero, req_arena_id, &memid, os_tld);
            if (segment == null) return null; // failed to allocate
            if (commit) {
                mi_commit_mask_create_full(&commit_mask);
            } else {
                mi_commit_mask_create_empty(&commit_mask);
            }
        }
        mi_assert_internal(segment != null and @ptrToInt(segment) % MI_SEGMENT_SIZE == 0);

        const commit_needed = _mi_divide_up(info_slices * MI_SEGMENT_SLICE_SIZE, MI_COMMIT_SIZE);
        mi_assert_internal(commit_needed > 0);
        var commit_needed_mask: mi_commit_mask_t = undefined;
        mi_commit_mask_create(0, commit_needed, &commit_needed_mask);
        if (!mi_commit_mask_all_set(&commit_mask, &commit_needed_mask)) {
            // at least commit the info slices
            mi_assert_internal(commit_needed * MI_COMMIT_SIZE >= info_slices * MI_SEGMENT_SLICE_SIZE);
            const ok = _mi_os_commit(segment, commit_needed * MI_COMMIT_SIZE, &is_zero, tld.stats);
            if (!ok) return null; // failed to commit
            mi_commit_mask_set(&commit_mask, &commit_needed_mask);
        }
        mi_track_mem_undefined(segment, commit_needed);
        segment.memid = memid;
        segment.mem_is_pinned = is_pinned;
        segment.mem_is_large = mem_large;
        segment.mem_is_committed = mi_commit_mask_is_full(&commit_mask);
        mi_segments_track_size(segment_size, tld);
        _mi_segment_map_allocated_at(segment);
    }

    // zero the segment info? -- not always needed as it is zero initialized from the OS
    mi_atomic_store_ptr_release(mi_segment_t, &segment.abandoned_next, null); // tsan
    if (!is_zero) {
        const ofs = @offsetOf(mi_segment_t, "next");
        const prefix = @offsetOf(mi_segment_t, "slices") - ofs;
        @memset(@ptrCast(*u8, segment) + ofs, 0, prefix + @sizeOf(mi_slice_t) * segment_slices);
    }

    if (!commit_info_still_good) {
        segment.commit_mask = commit_mask; // on lazy commit, the initial part is always committed
        segment.allow_decommit = (mi_option_is_enabled(mi_option_allow_decommit) and !segment.mem_is_pinned and !segment.mem_is_large);
        if (segment.allow_decommit) {
            segment.decommit_expire = _mi_clock_now() + mi_option_get(mi_option_decommit_delay);
            segment.decommit_mask = decommit_mask;
            mi_assert_internal(mi_commit_mask_all_set(&segment.commit_mask, &segment.decommit_mask));
            if (MI_DEBUG > 2) {
                const commit_needed = _mi_divide_up(info_slices * MI_SEGMENT_SLICE_SIZE, MI_COMMIT_SIZE);
                var commit_needed_mask: mi_commit_mask_t = undefined;
                mi_commit_mask_create(0, commit_needed, &commit_needed_mask);
                mi_assert_internal(!mi_commit_mask_any_set(&segment.decommit_mask, &commit_needed_mask));
            }
        } else {
            mi_assert_internal(mi_commit_mask_is_empty(&decommit_mask));
            segment.decommit_expire = 0;
            mi_commit_mask_create_empty(&segment.decommit_mask);
            mi_assert_internal(mi_commit_mask_is_empty(&segment.decommit_mask));
        }
    }

    // initialize segment info
    segment.segment_slices = segment_slices;
    segment.segment_info_slices = info_slices;
    segment.thread_id = _mi_thread_id();
    segment.cookie = _mi_ptr_cookie(segment);
    segment.slice_entries = slice_entries;
    segment.kind = if (required == 0) MI_SEGMENT_NORMAL else MI_SEGMENT_HUGE;

    // memset(segment.slices, 0, sizeof(mi_slice_t)*(info_slices+1));
    _mi_stat_increase(&tld.stats.page_committed, mi_segment_info_size(segment));

    // set up guard pages
    var guard_slices: usize = 0;
    if (MI_SECURE > 0) {
        // in secure mode, we set up a protected page in between the segment info
        // and the page data, and at the end of the segment.
        const os_pagesize = _mi_os_page_size();
        mi_assert_internal(mi_segment_info_size(segment) - os_pagesize >= pre_size);
        _mi_os_protect(@ptrCast(*u8, segment) + mi_segment_info_size(segment) - os_pagesize, os_pagesize);
        const end = @ptrCast(*u8, segment) + mi_segment_size(segment) - os_pagesize;
        mi_segment_ensure_committed(segment, end, os_pagesize, tld.stats);
        _mi_os_protect(end, os_pagesize);
        if (slice_entries == segment_slices) segment.slice_entries -= 1; // don't use the last slice :-(
        guard_slices = 1;
    }

    // reserve first slices for segment info
    const page0 = mi_segment_span_allocate(segment, 0, info_slices, tld);
    mi_assert_internal(page0 != null);
    if (page0 == null) return null; // cannot fail as we always commit in advance
    mi_assert_internal(segment.used == 1);
    segment.used = 0; // don't count our internal slices towards usage

    // initialize initial free pages
    if (segment.kind == MI_SEGMENT_NORMAL) { // not a huge page
        mi_assert_internal(huge_page == null);
        mi_segment_span_free(segment, info_slices, segment.slice_entries - info_slices, tld);
    } else {
        mi_assert_internal(huge_page != null);
        mi_assert_internal(mi_commit_mask_is_empty(&segment.decommit_mask));
        mi_assert_internal(mi_commit_mask_is_full(&segment.commit_mask));
        huge_page.?.* = mi_segment_span_allocate(segment, info_slices, segment_slices - info_slices - guard_slices, tld);
        mi_assert_internal(huge_page.?.* != null); // cannot fail as we commit in advance
    }

    mi_assert_expensive(mi_segment_is_valid(segment, tld));
    return segment;
}

// Allocate a segment from the OS aligned to `MI_SEGMENT_SIZE` .
fn mi_segment_alloc(required: usize, req_arena_id: mi_arena_id_t, tld: *mi_segments_tld_t, os_tld: *mi_os_tld_t, huge_page: ?**mi_page_t) ?*mi_segment_t {
    return mi_segment_init(null, required, req_arena_id, tld, os_tld, huge_page);
}

fn mi_segment_free(segment: *mi_segment_t, force: bool, tld: *mi_segments_tld_t) void {
    _ = force;
    mi_assert_internal(segment.next == null);
    mi_assert_internal(segment.used == 0);

    // Remove the free pages
    var slice: ?*mi_slice_t = &segment.slices[0];
    const end = mi_segment_slices_end(segment);
    var page_count: usize = 0;
    while (slice < end) {
        mi_assert_internal(slice.slice_count > 0);
        mi_assert_internal(slice.slice_offset == 0);
        mi_assert_internal(mi_slice_index(slice) == 0 or slice.xblock_size == 0); // no more used pages ..
        if (slice.xblock_size == 0 and segment.kind != MI_SEGMENT_HUGE) {
            mi_segment_span_remove_from_queue(slice, tld);
        }
        page_count += 1;
        slice = slice + slice.slice_count;
    }
    mi_assert_internal(page_count == 2); // first page is allocated by the segment itself

    // stats
    _mi_stat_decrease(&tld.stats.page_committed, mi_segment_info_size(segment));

    // return it to the OS
    mi_segment_os_free(segment, tld);
}

//-----------------------------------------------------------
//   Page Free
//-----------------------------------------------------------

// note: can be called on abandoned pages
fn mi_segment_page_clear(page: *mi_page_t, tld: *mi_segments_tld_t) *mi_slice_t {
    mi_assert_internal(page.xblock_size > 0);
    mi_assert_internal(mi_page_all_free(page));
    const segment = _mi_ptr_segment(page);
    mi_assert_internal(segment.used > 0);

    const inuse = page.capacity * mi_page_block_size(page);
    _mi_stat_decrease(&tld.stats.page_committed, inuse);
    _mi_stat_decrease(&tld.stats.pages, 1);

    // reset the page memory to reduce memory pressure?
    if (!segment.mem_is_pinned and !page.is_reset and mi_option_is_enabled(mi_option_page_reset)) {
        var psize: usize = undefined;
        const start = _mi_page_start(segment, page, &psize);
        page.is_reset = true;
        _mi_os_reset(start, psize, tld.stats);
    }

    // zero the page data, but not the segment fields
    page.is_zero_init = false;
    const ofs = @offsetOf(mi_page_t, "capacity");
    @memset(@ptrCast(*u8, page) + ofs, 0, @sizeOf(mi_page_t) - ofs);
    page.xblock_size = 1;

    // and free it
    const slice = mi_segment_span_free_coalesce(mi_page_to_slice(page), tld);
    segment.used -= 1;
    // cannot assert segment valid as it is called during reclaim
    // mi_assert_expensive(mi_segment_is_valid(segment, tld));
    return slice;
}

fn _mi_segment_page_free(page: *mi_page_t, force: bool, tld: *mi_segments_tld_t) void {
    const segment = _mi_page_segment(page);
    mi_assert_expensive(mi_segment_is_valid(segment, tld));

    // mark it as free now
    mi_segment_page_clear(page, tld);
    mi_assert_expensive(mi_segment_is_valid(segment, tld));

    if (segment.used == 0) {
        // no more used pages; remove from the free list and free the segment
        mi_segment_free(segment, force, tld);
    } else if (segment.used == segment.abandoned) {
        // only abandoned pages; remove from free list and abandon
        mi_segment_abandon(segment, tld);
    }
}

//-----------------------------------------------------------
// Abandonment
//
// When threads terminate, they can leave segments with
// live blocks (reachable through other threads). Such segments
// are "abandoned" and will be reclaimed by other threads to
// reuse their pages and/or free them eventually
//
// We maintain a global list of abandoned segments that are
// reclaimed on demand. Since this is shared among threads
// the implementation needs to avoid the A-B-A problem on
// popping abandoned segments: <https://en.wikipedia.org/wiki/ABA_problem>
// We use tagged pointers to avoid accidentially identifying
// reused segments, much like stamped references in Java.
// Secondly, we maintain a reader counter to avoid resetting
// or decommitting segments that have a pending read operation.
//
// Note: the current implementation is one possible design;
// another way might be to keep track of abandoned segments
// in the arenas/segment_cache's. This would have the advantage of keeping
// all concurrent code in one place and not needing to deal
// with ABA issues. The drawback is that it is unclear how to
// scan abandoned segments efficiently in that case as they
// would be spread among all other segments in the arenas.
// -----------------------------------------------------------

// Use the bottom 20-bits (on 64-bit) of the aligned segment pointers
// to put in a tag that increments on update to avoid the A-B-A problem.
const MI_TAGGED_MASK = MI_SEGMENT_MASK;
const mi_tagged_segment_t = usize;

fn mi_tagged_segment_ptr(ts: mi_tagged_segment_t) *mi_segment_t {
    return @intToPtr(*mi_segment_t, ts & ~MI_TAGGED_MASK);
}

fn mi_tagged_segment(segment: *mi_segment_t, ts: mi_tagged_segment_t) mi_tagged_segment_t {
    mi_assert_internal((@ptrToInt(segment) & MI_TAGGED_MASK) == 0);
    const tag = ((ts & MI_TAGGED_MASK) + 1) & MI_TAGGED_MASK;
    return (@ptrToInt(segment) | tag);
}

// This is a list of visited abandoned pages that were full at the time.
// this list migrates to `abandoned` when that becomes null. The use of
// this list reduces contention and the rate at which segments are visited.
var abandoned_visited = Atomic(?*mi_segment_t).init(null);

// The abandoned page list (tagged as it supports pop)
var abandoned = Atomic(mi_tagged_segment_t).init(0);

// Maintain these for debug purposes (these counts may be a bit off)
var abandoned_count = Atomic(usize).init(0);
var abandoned_visited_count = Atomic(usize).init(0);

// We also maintain a count of current readers of the abandoned list
// in order to prevent resetting/decommitting segment memory if it might
// still be read.
var abandoned_readers = Atomic(usize).init(0);

// Push on the visited list
fn mi_abandoned_visited_push(segment: *mi_segment_t) void {
    mi_assert_internal(segment.thread_id == 0);
    mi_assert_internal(mi_atomic_load_ptr_relaxed(mi_segment_t, &segment.abandoned_next) == null);
    mi_assert_internal(segment.next == null);
    mi_assert_internal(segment.used > 0);
    var anext = mi_atomic_load_ptr_relaxed(mi_segment_t, &abandoned_visited);
    while (true) {
        mi_atomic_store_ptr_release(mi_segment_t, &segment.abandoned_next, anext);
        if (mi_atomic_cas_ptr_weak_release(mi_segment_t, &abandoned_visited, &anext, segment)) break;
    }
    mi_atomic_increment_relaxed(&abandoned_visited_count);
}

// Move the visited list to the abandoned list.
fn mi_abandoned_visited_revisit() bool {
    // quick check if the visited list is empty
    if (mi_atomic_load_ptr_relaxed(mi_segment_t, &abandoned_visited) == null) return false;

    // grab the whole visited list
    const first = mi_atomic_exchange_ptr_acq_rel(mi_segment_t, &abandoned_visited, null);
    if (first == null) return false;

    // first try to swap directly if the abandoned list happens to be null
    var afirst: mi_tagged_segment_t = undefined;
    var ts = mi_atomic_load_relaxed(&abandoned);
    if (mi_tagged_segment_ptr(ts) == null) {
        const count = mi_atomic_load_relaxed(&abandoned_visited_count);
        afirst = mi_tagged_segment(first, ts);
        if (mi_atomic_cas_strong_acq_rel(&abandoned, &ts, afirst)) {
            mi_atomic_add_relaxed(&abandoned_count, count);
            mi_atomic_sub_relaxed(&abandoned_visited_count, count);
            return true;
        }
    }

    // find the last element of the visited list: O(n)
    var last = first;
    var next: ?*mi_segment_t = mi_atomic_load_ptr_relaxed(mi_segment_t, &last.abandoned_next);
    while (next != null) : (next = mi_atomic_load_ptr_relaxed(mi_segment_t, &last.abandoned_next)) {
        last = next;
    }

    // and atomically prepend to the abandoned list
    // (no need to increase the readers as we don't access the abandoned segments)
    var anext = mi_atomic_load_relaxed(&abandoned);
    var count = mi_atomic_load_relaxed(&abandoned_visited_count);
    while (true) : (count = mi_atomic_load_relaxed(&abandoned_visited_count)) {
        mi_atomic_store_ptr_release(mi_segment_t, &last.abandoned_next, mi_tagged_segment_ptr(anext));
        afirst = mi_tagged_segment(first, anext);
        if (mi_atomic_cas_weak_release(&abandoned, &anext, afirst)) break;
    }
    mi_atomic_add_relaxed(&abandoned_count, count);
    mi_atomic_sub_relaxed(&abandoned_visited_count, count);
    return true;
}

// Push on the abandoned list.
fn mi_abandoned_push(segment: *mi_segment_t) void {
    mi_assert_internal(segment.thread_id == 0);
    mi_assert_internal(mi_atomic_load_ptr_relaxed(mi_segment_t, &segment.abandoned_next) == null);
    mi_assert_internal(segment.next == null);
    mi_assert_internal(segment.used > 0);
    var next: mi_tagged_segment_t = undefined;
    var ts = mi_atomic_load_relaxed(&abandoned);
    while (true) {
        mi_atomic_store_ptr_release(mi_segment_t, &segment.abandoned_next, mi_tagged_segment_ptr(ts));
        next = mi_tagged_segment(segment, ts);
        if (mi_atomic_cas_weak_release(&abandoned, &ts, next)) break;
    }
    mi_atomic_increment_relaxed(&abandoned_count);
}

// Wait until there are no more pending reads on segments that used to be in the abandoned list
// called for example from `arena.c` before decommitting
fn _mi_abandoned_await_readers() void {
    var n = mi_atomic_load_acquire(&abandoned_readers);
    while (n != 0) : (n = mi_atomic_load_acquire(&abandoned_readers)) {
        mi_atomic_yield();
    }
}

// Pop from the abandoned list
fn mi_abandoned_pop() ?*mi_segment_t {
    // Check efficiently if it is empty (or if the visited list needs to be moved)
    const ts = mi_atomic_load_relaxed(&abandoned);
    var segment = mi_tagged_segment_ptr(ts);
    if (mi_likely(segment == null)) {
        if (mi_likely(!mi_abandoned_visited_revisit())) { // try to swap in the visited list on null
            return null;
        }
    }

    // Do a pop. We use a reader count to prevent
    // a segment to be decommitted while a read is still pending,
    // and a tagged pointer to prevent A-B-A link corruption.
    // (this is called from `region.c:_mi_mem_free` for example)
    mi_atomic_increment_relaxed(&abandoned_readers); // ensure no segment gets decommitted
    var next: mi_tagged_segment_t = 0;
    ts = mi_atomic_load_acquire(&abandoned);
    while (true) {
        segment = mi_tagged_segment_ptr(ts);
        if (segment != null) {
            const anext = mi_atomic_load_ptr_relaxed(mi_segment_t, &segment.abandoned_next);
            next = mi_tagged_segment(anext, ts); // note: reads the segment's `abandoned_next` field so should not be decommitted
        }
        if (segment == null or mi_atomic_cas_weak_acq_rel(&abandoned, &ts, next)) break;
    }
    mi_atomic_decrement_relaxed(&abandoned_readers); // release reader lock
    if (segment != null) {
        mi_atomic_store_ptr_release(mi_segment_t, &segment.abandoned_next, null);
        mi_atomic_decrement_relaxed(&abandoned_count);
    }
    return segment;
}

//-----------------------------------------------------------
//   Abandon segment/page
//-----------------------------------------------------------

fn mi_segment_abandon(segment: *mi_segment_t, tld: *mi_segments_tld_t) void {
    mi_assert_internal(segment.used == segment.abandoned);
    mi_assert_internal(segment.used > 0);
    mi_assert_internal(mi_atomic_load_ptr_relaxed(mi_segment_t, &segment.abandoned_next) == null);
    mi_assert_internal(segment.abandoned_visits == 0);
    mi_assert_expensive(mi_segment_is_valid(segment, tld));

    // remove the free pages from the free page queues
    var slice: *?mi_slice_t = &segment.slices[0];
    const end = mi_segment_slices_end(segment);
    while (slice < end) {
        mi_assert_internal(slice.slice_count > 0);
        mi_assert_internal(slice.slice_offset == 0);
        if (slice.xblock_size == 0) { // a free page
            mi_segment_span_remove_from_queue(slice, tld);
            slice.xblock_size = 0; // but keep it free
        }
        slice = slice + slice.slice_count;
    }

    // perform delayed decommits (force if option enabled)
    mi_segment_delayed_decommit(segment, mi_option_is_enabled(mi_option_abandoned_page_decommit), tld.stats);

    // all pages in the segment are abandoned; add it to the abandoned list
    _mi_stat_increase(&tld.stats.segments_abandoned, 1);
    mi_segments_track_size(-@intCast(i64, mi_segment_size(segment)), tld);
    segment.thread_id = 0;
    mi_atomic_store_ptr_release(mi_segment_t, &segment.abandoned_next, null);
    segment.abandoned_visits = 1; // from 0 to 1 to signify it is abandoned
    mi_abandoned_push(segment);
}

fn _mi_segment_page_abandon(page: *mi_page_t, tld: *mi_segments_tld_t) void {
    mi_assert(page != null);
    mi_assert_internal(mi_page_thread_free_flag(page) == MI_NEVER_DELAYED_FREE);
    mi_assert_internal(mi_page_heap(page) == null);
    var segment = _mi_page_segment(page);

    mi_assert_expensive(mi_segment_is_valid(segment, tld));
    segment.abandoned += 1;

    _mi_stat_increase(&tld.stats.pages_abandoned, 1);
    mi_assert_internal(segment.abandoned <= segment.used);
    if (segment.used == segment.abandoned) {
        // all pages are abandoned, abandon the entire segment
        mi_segment_abandon(segment, tld);
    }
}

//-----------------------------------------------------------
//  Reclaim abandoned pages
//-----------------------------------------------------------

fn mi_slices_start_iterate(segment: *mi_segment_t, end: **const mi_slice_t) *mi_slice_t {
    var slice = &segment.slices[0];
    end.* = mi_segment_slices_end(segment);
    mi_assert_internal(slice.slice_count > 0 and slice.xblock_size > 0); // segment allocated page
    slice = slice + slice.slice_count; // skip the first segment allocated page
    return slice;
}

// Possibly free pages and check if free space is available
fn mi_segment_check_free(segment: *mi_segment_t, slices_needed: usize, block_size: usize, tld: *mi_segments_tld_t) bool {
    mi_assert_internal(block_size < MI_HUGE_BLOCK_SIZE);
    mi_assert_internal(mi_segment_is_abandoned(segment));
    var has_page = false;

    // for all slices
    var end: *const mi_slice_t = undefined;
    var slice = mi_slices_start_iterate(segment, &end);
    while (slice < end) {
        mi_assert_internal(slice.slice_count > 0);
        mi_assert_internal(slice.slice_offset == 0);
        if (mi_slice_is_used(slice)) { // used page
            // ensure used count is up to date and collect potential concurrent frees
            const page = mi_slice_to_page(slice);
            _mi_page_free_collect(page, false);
            if (mi_page_all_free(page)) {
                // if this page is all free now, free it without adding to any queues (yet)
                mi_assert_internal(page.next == null and page.prev == null);
                _mi_stat_decrease(&tld.stats.pages_abandoned, 1);
                segment.abandoned -= 1;
                slice = mi_segment_page_clear(page, tld); // re-assign slice due to coalesce!
                mi_assert_internal(!mi_slice_is_used(slice));
                if (slice.slice_count >= slices_needed) {
                    has_page = true;
                }
            } else {
                if (page.xblock_size == block_size and mi_page_has_any_available(page)) {
                    // a page has available free blocks of the right size
                    has_page = true;
                }
            }
        } else {
            // empty span
            if (slice.slice_count >= slices_needed) {
                has_page = true;
            }
        }
        slice = slice + slice.slice_count;
    }
    return has_page;
}

// Reclaim an abandoned segment; returns null if the segment was freed
// set `right_page_reclaimed` to `true` if it reclaimed a page of the right `block_size` that was not full.
fn mi_segment_reclaim(segment: *mi_segment_t, heap: *mi_heap_t, requested_block_size: usize, right_page_reclaimed: *bool, tld: *mi_segments_tld_t) ?*mi_segment_t {
    mi_assert_internal(mi_atomic_load_ptr_relaxed(mi_segment_t, &segment.abandoned_next) == null);
    mi_assert_expensive(mi_segment_is_valid(segment, tld));
    if (right_page_reclaimed != null) {
        right_page_reclaimed.* = false;
    }

    segment.thread_id = _mi_thread_id();
    segment.abandoned_visits = 0;
    mi_segments_track_size(mi_segment_size(segment), tld);
    mi_assert_internal(segment.next == null);
    _mi_stat_decrease(&tld.stats.segments_abandoned, 1);

    // for all slices
    var end: *const mi_slice_t = undefined;
    var slice = mi_slices_start_iterate(segment, &end);
    while (slice < end) {
        mi_assert_internal(slice.slice_count > 0);
        mi_assert_internal(slice.slice_offset == 0);
        if (mi_slice_is_used(slice)) {
            // in use: reclaim the page in our heap
            const page = mi_slice_to_page(slice);
            mi_assert_internal(!page.is_reset);
            mi_assert_internal(page.is_committed);
            mi_assert_internal(mi_page_thread_free_flag(page) == MI_NEVER_DELAYED_FREE);
            mi_assert_internal(mi_page_heap(page) == null);
            mi_assert_internal(page.next == null and page.prev == null);
            _mi_stat_decrease(&tld.stats.pages_abandoned, 1);
            segment.abandoned -= 1;
            // set the heap again and allow delayed free again
            mi_page_set_heap(page, heap);
            _mi_page_use_delayed_free(page, MI_USE_DELAYED_FREE, true); // override never (after heap is set)
            _mi_page_free_collect(page, false); // ensure used count is up to date
            if (mi_page_all_free(page)) {
                // if everything free by now, free the page
                slice = mi_segment_page_clear(page, tld); // set slice again due to coalesceing
            } else {
                // otherwise reclaim it into the heap
                _mi_page_reclaim(heap, page);
                if (requested_block_size == page.xblock_size and mi_page_has_any_available(page)) {
                    if (right_page_reclaimed != null) {
                        right_page_reclaimed.* = true;
                    }
                }
            }
        } else {
            // the span is free, add it to our page queues
            slice = mi_segment_span_free_coalesce(slice, tld); // set slice again due to coalesceing
        }
        mi_assert_internal(slice.slice_count > 0 and slice.slice_offset == 0);
        slice = slice + slice.slice_count;
    }

    mi_assert(segment.abandoned == 0);
    if (segment.used == 0) { // due to page_clear
        mi_assert_internal(right_page_reclaimed == null or !(*right_page_reclaimed));
        mi_segment_free(segment, false, tld);
        return null;
    } else {
        return segment;
    }
}

fn _mi_abandoned_reclaim_all(heap: *mi_heap_t, tld: *mi_segments_tld_t) void {
    var segment = mi_abandoned_pop();
    while (segment != null) : (segment = mi_abandoned_pop()) {
        mi_segment_reclaim(segment, heap, 0, null, tld);
    }
}

fn mi_segment_try_reclaim(heap: *mi_heap_t, needed_slices: usize, block_size: usize, reclaimed: *bool, tld: *mi_segments_tld_t) ?*mi_segment_t {
    reclaimed.* = false;
    var max_tries = mi_option_get_clamp(mi_option_max_segment_reclaim, 8, 1024); // limit the work to bound allocation times
    if (max_tries == 0) return null;

    while (max_tries > 0) : (max_tries -= 1) {
        var segment = mi_abandoned_pop();
        if (segment == null) break;
        segment.abandoned_visit -= 1;
        // todo: an arena exclusive heap will potentially visit many abandoned unsuitable segments
        // and push them into the visited list and use many tries. Perhaps we can skip non-suitable ones in a better way?
        const is_suitable = _mi_heap_memid_is_suitable(heap, segment.memid);
        const has_page = mi_segment_check_free(segment, needed_slices, block_size, tld); // try to free up pages (due to concurrent frees)
        if (segment.used == 0) {
            // free the segment (by forced reclaim) to make it available to other threads.
            // note1: we prefer to free a segment as that might lead to reclaiming another
            // segment that is still partially used.
            // note2: we could in principle optimize this by skipping reclaim and directly
            // freeing but that would violate some invariants temporarily)
            mi_segment_reclaim(segment, heap, 0, null, tld);
        } else if (has_page and is_suitable) {
            // found a large enough free span, or a page of the right block_size with free space
            // we return the result of reclaim (which is usually `segment`) as it might free
            // the segment due to concurrent frees (in which case `null` is returned).
            return mi_segment_reclaim(segment, heap, block_size, reclaimed, tld);
        } else if (segment.abandoned_visits > 3 and is_suitable) {
            // always reclaim on 3rd visit to limit the abandoned queue length.
            mi_segment_reclaim(segment, heap, 0, null, tld);
        } else {
            // otherwise, push on the visited list so it gets not looked at too quickly again
            // force delayed decommit
            mi_segment_delayed_decommit(segment, true, tld.stats); // forced decommit if needed as we may not visit soon again
            mi_abandoned_visited_push(segment);
        }
    }
    return null;
}

fn _mi_abandoned_collect(heap: *mi_heap_t, force: bool, tld: *mi_segments_tld_t) void {
    var max_tries = if (force) 16 * 1024 else 1024; // limit latency
    if (force) {
        mi_abandoned_visited_revisit();
    }
    while (max_tries > 0) : (max_tries -= 1) {
        var segment = mi_abandoned_pop();
        if (segment == null) break;
        mi_segment_check_free(segment, 0, 0, tld); // try to free up pages (due to concurrent frees)
        if (segment.used == 0) {
            // free the segment (by forced reclaim) to make it available to other threads.
            // note: we could in principle optimize this by skipping reclaim and directly
            // freeing but that would violate some invariants temporarily)
            mi_segment_reclaim(segment, heap, 0, null, tld);
        } else {
            // otherwise, decommit if needed and push on the visited list
            // note: forced decommit can be expensive if many threads are destroyed/created as in mstress.
            mi_segment_delayed_decommit(segment, force, tld.stats);
            mi_abandoned_visited_push(segment);
        }
    }
}

//-----------------------------------------------------------
//   Reclaim or allocate
//-----------------------------------------------------------

fn mi_segment_reclaim_or_alloc(heap: *mi_heap_t, needed_slices: usize, block_size: usize, tld: *mi_segments_tld_t, os_tld: *mi_os_tld_t) *mi_segment_t {
    mi_assert_internal(block_size < MI_HUGE_BLOCK_SIZE);
    mi_assert_internal(block_size <= MI_LARGE_OBJ_SIZE_MAX);

    // 1. try to reclaim an abandoned segment
    var reclaimed: bool = undefined;
    var segment = mi_segment_try_reclaim(heap, needed_slices, block_size, &reclaimed, tld);
    if (reclaimed) {
        // reclaimed the right page right into the heap
        mi_assert_internal(segment != null);
        return null; // pretend out-of-memory as the page will be in the page queue of the heap with available blocks
    } else if (segment != null) {
        // reclaimed a segment with a large enough empty span in it
        return segment;
    }
    // 2. otherwise allocate a fresh segment
    return mi_segment_alloc(0, heap.arena_id, tld, os_tld, null);
}

//-----------------------------------------------------------
//   Page allocation
//-----------------------------------------------------------

fn mi_segments_page_alloc(heap: *mi_heap_t, page_kind: mi_page_kind_t, required: usize, block_size: usize, tld: *mi_segments_tld_t, os_tld: *mi_os_tld_t) ?*mi_page_t {
    mi_assert_internal(required <= MI_LARGE_OBJ_SIZE_MAX and page_kind <= MI_PAGE_LARGE);

    // find a free page
    const page_size = _mi_align_up(required, if (required > MI_MEDIUM_PAGE_SIZE) MI_MEDIUM_PAGE_SIZE else MI_SEGMENT_SLICE_SIZE);
    const slices_needed = page_size / MI_SEGMENT_SLICE_SIZE;
    mi_assert_internal(slices_needed * MI_SEGMENT_SLICE_SIZE == page_size);
    const page = mi_segments_page_find_and_allocate(slices_needed, heap.arena_id, tld); //(required <= MI_SMALL_SIZE_MAX ? 0 : slices_needed), tld);
    if (page == null) {
        // no free page, allocate a new segment and try again
        if (mi_segment_reclaim_or_alloc(heap, slices_needed, block_size, tld, os_tld) == null) {
            // OOM or reclaimed a good page in the heap
            return null;
        } else {
            // otherwise try again
            return mi_segments_page_alloc(heap, page_kind, required, block_size, tld, os_tld);
        }
    }
    mi_assert_internal(page != null and page.slice_count * MI_SEGMENT_SLICE_SIZE == page_size);
    mi_assert_internal(_mi_ptr_segment(page).thread_id == _mi_thread_id());
    mi_segment_delayed_decommit(_mi_ptr_segment(page), false, tld.stats);
    return page;
}

//-----------------------------------------------------------
//   Huge page allocation
//-----------------------------------------------------------

fn mi_segment_huge_page_alloc(size: usize, req_arena_id: mi_arena_id_t, tld: *mi_segments_tld_t, os_tld: *mi_os_tld_t) ?*mi_page_t {
    var page: ?*mi_page_t = undefined;
    const segment = mi_segment_alloc(size, req_arena_id, tld, os_tld, &page);
    if (segment == null or page == null) return null;
    mi_assert_internal(segment.used == 1);
    mi_assert_internal(mi_page_block_size(page) >= size);
    segment.thread_id = 0; // huge segments are immediately abandoned
    return page;
}

// free huge block from another thread
fn _mi_segment_huge_page_free(segment: *mi_segment_t, page: *mi_page_t, block: *mi_block_t) void {
    // huge page segments are always abandoned and can be freed immediately by any thread
    mi_assert_internal(segment.kind == MI_SEGMENT_HUGE);
    mi_assert_internal(segment == _mi_page_segment(page));
    mi_assert_internal(mi_atomic_load_relaxed(&segment.thread_id) == 0);

    // claim it and free
    const heap = mi_heap_get_default(); // issue #221; don't use the internal get_default_heap as we need to ensure the thread is initialized.
    // paranoia: if this it the last reference, the cas should always succeed
    var expected_tid: usize = 0;
    if (mi_atomic_cas_strong_acq_rel(&segment.thread_id, &expected_tid, heap.thread_id)) {
        mi_block_set_next(page, block, page.free);
        page.free = block;
        page.used -= 1;
        page.is_zero = false;
        mi_assert(page.used == 0);
        const tld = heap.tld;
        _mi_segment_page_free(page, true, &tld.segments);
    } else if (MI_DEBUG != 0) {
        mi_assert_internal(false);
    }
}

//-----------------------------------------------------------
//   Page allocation and free
//----------------------------------------------------------- */
fn _mi_segment_page_alloc(heap: *mi_heap_t, block_size: usize, tld: *mi_segments_tld_t, os_tld: *mi_os_tld_t) *mi_page_t {
    var page: *mi_page_t = undefined;
    if (block_size <= MI_SMALL_OBJ_SIZE_MAX) {
        page = mi_segments_page_alloc(heap, MI_PAGE_SMALL, block_size, block_size, tld, os_tld);
    } else if (block_size <= MI_MEDIUM_OBJ_SIZE_MAX) {
        page = mi_segments_page_alloc(heap, MI_PAGE_MEDIUM, MI_MEDIUM_PAGE_SIZE, block_size, tld, os_tld);
    } else if (block_size <= MI_LARGE_OBJ_SIZE_MAX) {
        page = mi_segments_page_alloc(heap, MI_PAGE_LARGE, block_size, block_size, tld, os_tld);
    } else {
        page = mi_segment_huge_page_alloc(block_size, heap.arena_id, tld, os_tld);
    }
    mi_assert_internal(page == null or _mi_heap_memid_is_suitable(heap, _mi_page_segment(page).memid));
    mi_assert_expensive(page == null or mi_segment_is_valid(_mi_page_segment(page), tld));
    return page;
}
