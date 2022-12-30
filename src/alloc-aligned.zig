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
    usingnamespace @import("init.zig");
    usingnamespace @import("heap.zig");
    usingnamespace @import("page.zig");
    usingnamespace @import("stats.zig");
    usingnamespace @import("alloc.zig");
    usingnamespace @import("segment.zig");

    fn noop(cond: bool) void {
        _ = cond;
    }
};

// alises to avoid clutter

// type aliases
const mi_heap_t = mi.mi_heap_t;
const mi_block_t = mi.mi_block_t;

// #defines
const MI_DEBUG = mi.MI_DEBUG;
const MI_STAT = mi.MI_STAT;
const MI_PADDING = mi.MI_PADDING;
const MI_PADDING_SIZE = mi.MI_PADDING_SIZE;
const PTRDIFF_MAX = std.math.maxInt(isize);
const MI_ALIGNMENT_MAX = mi.MI_ALIGNMENT_MAX;
const MI_SMALL_SIZE_MAX = mi.MI_SMALL_SIZE_MAX;
const MI_MAX_ALIGN_GUARANTEE = mi.MI_MAX_ALIGN_GUARANTEE;
const MI_MAX_ALIGN_SIZE = mi.MI_MAX_ALIGN_SIZE;

const MI_TRACK_ENABLED = false;

// Function aliases
const mi_get_default_heap = mi.mi_get_default_heap;
const mi_count_size_overflow = mi.mi_count_size_overflow;
const _mi_heap_realloc_zero = mi._mi_heap_realloc_zero;
const _mi_heap_malloc_zero = mi._mi_heap_malloc_zero;
const _mi_heap_malloc_zero_ex = mi._mi_heap_malloc_zero_ex;
const _mi_heap_get_free_small_page = mi._mi_heap_get_free_small_page;
const mi_usable_size = mi.mi_usable_size;
const mi_heap_malloc_small = mi.mi_heap_malloc_small;
const mi_page_set_has_aligned = mi.mi_page_set_has_aligned;
const mi_page_usable_block_size = mi.mi_page_usable_block_size;
const _mi_ptr_page = mi._mi_ptr_page;
const _mi_ptr_segment = mi._mi_ptr_segment;
const _mi_page_ptr_unalign = mi._mi_page_ptr_unalign;

const _mi_stat_increase = mi._mi_stat_increase;
const mi_mem_is_zero = mi.mi_mem_is_zero;
const _mi_memcpy_aligned = mi._mi_memcpy_aligned;

const _mi_page_malloc = mi._mi_page_malloc;
const mi_free = mi.mi_free;

const mi_track_malloc = mi.mi_track_malloc;
const mi_track_free = mi.mi_track_free;
const mi_track_free_size = mi.mi_track_free_size;
const mi_track_resize = mi.mi_track_resize;

// local inlines
const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

inline fn _mi_is_power_of_two(x: usize) bool {
    return (x & (x - 1)) == 0;
}

// ------------------------------------------------------
// Aligned Allocation
// ------------------------------------------------------

// Fallback primitive aligned allocation -- split out for better codegen
fn mi_heap_malloc_zero_aligned_at_fallback(heap: *mi_heap_t, size: usize, alignment: usize, offset: usize, zero: bool) ?*u8 {
    mi_assert_internal(size <= PTRDIFF_MAX);
    mi_assert_internal(alignment != 0 and _mi_is_power_of_two(alignment));

    const align_mask = alignment - 1; // for any x, `(x & align_mask) == (x % alignment)`
    const padsize = size + MI_PADDING_SIZE;

    // use regular allocation if it is guaranteed to fit the alignment constraints
    if (offset == 0 and alignment <= padsize and padsize <= MI_MAX_ALIGN_GUARANTEE and (padsize & align_mask) == 0) {
        const p = _mi_heap_malloc_zero(heap, size, zero);
        mi_assert_internal(p == null or (@ptrToInt(p) % alignment) == 0);
        return p;
    }

    var p: *u8 = undefined;
    var oversize: usize = undefined;

    if (mi_unlikely(alignment > MI_ALIGNMENT_MAX)) {
        // use OS allocation for very large alignment and allocate inside a huge page (dedicated segment with 1 page)
        // This can support alignments >= MI_SEGMENT_SIZE by ensuring the object can be aligned at a point in the
        // first (and single) page such that the segment info is `MI_SEGMENT_SIZE` bytes before it (so it can be found by aligning the pointer down)
        if (mi_unlikely(offset != 0)) {
            // todo: cannot support offset alignment for very large alignments yet
            if (MI_DEBUG > 0)
                std.log.warn("aligned allocation with a very large alignment cannot be used with an alignment offset (size {}, alignment {}, offset {})\n", .{ size, alignment, offset });
            return null;
        }
        oversize = if (size <= MI_SMALL_SIZE_MAX) MI_SMALL_SIZE_MAX + 1 // ensure we use generic malloc path
        else size;
        p = _mi_heap_malloc_zero_ex(heap, oversize, false, alignment) orelse return null; // the page block size should be large enough to align in the single huge page block
        // zero afterwards as only the area from the aligned_p may be committed!
    } else {
        // otherwise over-allocate
        oversize = size + alignment - 1;
        p = _mi_heap_malloc_zero(heap, oversize, zero) orelse return null;
    }

    // .. and align within the allocation
    const poffset = ((@ptrToInt(p) + offset) & align_mask);
    const adjust = if (poffset == 0) 0 else alignment - poffset;
    mi_assert_internal(adjust < alignment);
    const aligned_p = @intToPtr(*u8, @ptrToInt(p) + adjust);
    if (aligned_p != p) {
        mi_page_set_has_aligned(_mi_ptr_page(p), true);
    }

    mi_assert_internal(mi_page_usable_block_size(_mi_ptr_page(p)) >= adjust + size);
    mi_assert_internal(p == @ptrCast(*u8, _mi_page_ptr_unalign(_mi_ptr_segment(aligned_p), _mi_ptr_page(aligned_p), aligned_p)));
    mi_assert_internal((@ptrToInt(aligned_p) + offset) % alignment == 0);
    mi_assert_internal(mi_page_usable_block_size(_mi_ptr_page(p)) >= adjust + size);

    // now zero the block if needed
    if (zero and alignment > MI_ALIGNMENT_MAX) {
        const diff = @ptrToInt(aligned_p) - @ptrToInt(p);
        const zsize = mi_page_usable_block_size(_mi_ptr_page(p)) - diff - MI_PADDING_SIZE;
        if (zsize > 0) {
            @memset(@ptrCast([*]u8, aligned_p), 0, zsize);
        }
    }

    if (MI_TRACK_ENABLED) {
        if (p != aligned_p) {
            mi_track_free_size(p, oversize);
            mi_track_malloc(aligned_p, size, zero);
        } else {
            mi_track_resize(aligned_p, oversize, size);
        }
    }
    return aligned_p;
}

// Primitive aligned allocation
fn mi_heap_malloc_zero_aligned_at(heap: *mi_heap_t, size: usize, alignment: usize, offset: usize, zero: bool) ?*u8 {
    // note: we don't require `size > offset`, we just guarantee that the address at offset is aligned regardless of the allocated size.
    mi_assert(alignment > 0);
    if (mi_unlikely(alignment == 0 or !_mi_is_power_of_two(alignment))) { // require power-of-two (see <https://en.cppreference.com/w/c/memory/aligned_alloc>)
        if (MI_DEBUG > 0)
            std.log.err("aligned allocation requires the alignment to be a power-of-two (size {}, alignment {})", .{ size, alignment });
        return null;
    }
    if (mi_unlikely(size > PTRDIFF_MAX)) { // we don't allocate more than PTRDIFF_MAX (see <https://sourceware.org/ml/libc-announce/2019/msg00001.html>)
        if (MI_DEBUG > 0)
            std.log.err("aligned allocation request is too large (size {}, alignment {})\n", .{ size, alignment });

        return null;
    }
    const align_mask = alignment - 1; // for any x, `(x & align_mask) == (x % alignment)`
    const padsize = size + MI_PADDING_SIZE; // note: cannot overflow due to earlier size > PTRDIFF_MAX check

    // try first if there happens to be a small block available with just the right alignment
    if (mi_likely(padsize <= MI_SMALL_SIZE_MAX) and alignment <= padsize) {
        const page = _mi_heap_get_free_small_page(heap, padsize);
        const is_aligned = ((@ptrToInt(page.free) + offset) & align_mask) == 0;
        if (mi_likely(page.free != null and is_aligned)) {
            if (MI_STAT > 1)
                _mi_stat_increase(&heap.tld.?.stats.malloc, size);
            const p = _mi_page_malloc(heap, page, padsize, zero); // TODO: inline _mi_page_malloc
            mi_assert_internal(p != null);
            mi_assert_internal((@ptrToInt(p) + offset) % alignment == 0);
            mi_track_malloc(p, size, zero);
            return p;
        }
    }
    // fallback
    return mi_heap_malloc_zero_aligned_at_fallback(heap, size, alignment, offset, zero);
}

// ------------------------------------------------------
// Optimized mi_heap_malloc_aligned / mi_malloc_aligned
// ------------------------------------------------------

pub fn mi_heap_malloc_aligned_at(heap: *mi_heap_t, size: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_malloc_zero_aligned_at(heap, size, alignment, offset, false);
}

pub fn mi_heap_malloc_aligned(heap: *mi_heap_t, size: usize, alignment: usize) ?*u8 {
    if (MI_PADDING != 0) {
        // without padding, any small sized allocation is naturally aligned (see also `_mi_segment_page_start`)
        if (!_mi_is_power_of_two(alignment)) return null;
        if (mi_likely(_mi_is_power_of_two(size) and size >= alignment and size <= MI_SMALL_SIZE_MAX)) {
            return mi_heap_malloc_small(heap, size);
        } else {
            return mi_heap_malloc_aligned_at(heap, size, alignment, 0);
        }
    } else {
        // with padding, we can only guarantee this for fixed alignments
        if (mi_likely((alignment == @sizeOf(*u8) or (alignment == MI_MAX_ALIGN_SIZE and size > (MI_MAX_ALIGN_SIZE / 2))) and size <= MI_SMALL_SIZE_MAX)) {
            // fast path for common alignment and size_t
            return mi_heap_malloc_small(heap, size);
        } else {
            return mi_heap_malloc_aligned_at(heap, size, alignment, 0);
        }
    }
}

// ------------------------------------------------------
// Aligned Allocation
// ------------------------------------------------------

pub fn mi_heap_zalloc_aligned_at(heap: *mi_heap_t, size: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_malloc_zero_aligned_at(heap, size, alignment, offset, true);
}

pub fn mi_heap_zalloc_aligned(heap: *mi_heap_t, size: usize, alignment: usize) ?*u8 {
    return mi_heap_zalloc_aligned_at(heap, size, alignment, 0);
}

pub fn mi_heap_calloc_aligned_at(heap: *mi_heap_t, count: usize, size: usize, alignment: usize, offset: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(count, size, &total)) return null;
    return mi_heap_zalloc_aligned_at(heap, total, alignment, offset);
}

pub fn mi_heap_calloc_aligned(heap: *mi_heap_t, count: usize, size: usize, alignment: usize) ?*u8 {
    return mi_heap_calloc_aligned_at(heap, count, size, alignment, 0);
}

pub fn mi_malloc_aligned_at(size: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_malloc_aligned_at(mi_get_default_heap(), size, alignment, offset);
}

pub fn mi_malloc_aligned(size: usize, alignment: usize) ?*u8 {
    return mi_heap_malloc_aligned(mi_get_default_heap(), size, alignment);
}

pub fn mi_zalloc_aligned_at(size: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_zalloc_aligned_at(mi_get_default_heap(), size, alignment, offset);
}

pub fn mi_zalloc_aligned(size: usize, alignment: usize) ?*u8 {
    return mi_heap_zalloc_aligned(mi_get_default_heap(), size, alignment);
}

pub fn mi_calloc_aligned_at(count: usize, size: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_calloc_aligned_at(mi_get_default_heap(), count, size, alignment, offset);
}

pub fn mi_calloc_aligned(count: usize, size: usize, alignment: usize) ?*u8 {
    return mi_heap_calloc_aligned(mi_get_default_heap(), count, size, alignment);
}

// ------------------------------------------------------
// Aligned re-allocation
// ------------------------------------------------------

fn mi_heap_realloc_zero_aligned_at(heap: *mi_heap_t, p: *u8, newsize: usize, alignment: usize, offset: usize, zero: bool) ?*u8 {
    mi_assert(alignment > 0);
    if (alignment <= @sizeOf(usize)) return _mi_heap_realloc_zero(heap, p, newsize, zero);
    if (p == null) return mi_heap_malloc_zero_aligned_at(heap, newsize, alignment, offset, zero);
    const size = mi_usable_size(p);
    if (newsize <= size and newsize >= (size - (size / 2)) and ((@ptrToInt(p) + offset) % alignment) == 0) {
        return p; // reallocation still fits, is aligned and not more than 50% waste
    } else {
        const newp = mi_heap_malloc_aligned_at(heap, newsize, alignment, offset);
        if (newp != null) {
            if (zero and newsize > size) {
                const page = _mi_ptr_page(newp);
                if (page.is_zero) {
                    // already zero initialized
                    mi_assert_expensive(mi_mem_is_zero(newp, newsize));
                } else {
                    // also set last word in the previous allocation to zero to ensure any padding is zero-initialized
                    const start = if (size >= @sizeOf(usize)) size - @sizeOf(usize) else 0;
                    std.mem.copy(@ptrCast(*u8, newp) + start, 0, newsize - start);
                }
            }
            _mi_memcpy_aligned(newp, p, if (newsize > size) size else newsize);
            mi_free(p); // only free if successful
        }
        return newp;
    }
}

fn mi_heap_realloc_zero_aligned(heap: *mi_heap_t, p: *u8, newsize: usize, alignment: usize, zero: bool) ?*u8 {
    mi_assert(alignment > 0);
    if (alignment <= @sizeOf(usize)) return _mi_heap_realloc_zero(heap, p, newsize, zero);
    const offset = (@ptrToInt(p) % alignment); // use offset of previous allocation (p can be null)
    return mi_heap_realloc_zero_aligned_at(heap, p, newsize, alignment, offset, zero);
}

pub fn mi_heap_realloc_aligned_at(heap: *mi_heap_t, p: *u8, newsize: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_realloc_zero_aligned_at(heap, p, newsize, alignment, offset, false);
}

pub fn mi_heap_realloc_aligned(heap: *mi_heap_t, p: *u8, newsize: usize, alignment: usize) ?*u8 {
    return mi_heap_realloc_zero_aligned(heap, p, newsize, alignment, false);
}

pub fn mi_heap_rezalloc_aligned_at(heap: *mi_heap_t, p: *u8, newsize: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_realloc_zero_aligned_at(heap, p, newsize, alignment, offset, true);
}

pub fn mi_heap_rezalloc_aligned(heap: *mi_heap_t, p: *u8, newsize: usize, alignment: usize) ?*u8 {
    return mi_heap_realloc_zero_aligned(heap, p, newsize, alignment, true);
}

pub fn mi_heap_recalloc_aligned_at(heap: *mi_heap_t, p: *u8, newcount: usize, size: usize, alignment: usize, offset: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(newcount, size, &total)) return null;
    return mi_heap_rezalloc_aligned_at(heap, p, total, alignment, offset);
}

pub fn mi_heap_recalloc_aligned(heap: *mi_heap_t, p: *u8, newcount: usize, size: usize, alignment: usize) ?*u8 {
    var total: usize = undefined;
    if (mi_count_size_overflow(newcount, size, &total)) return null;
    return mi_heap_rezalloc_aligned(heap, p, total, alignment);
}

pub fn mi_realloc_aligned_at(p: *u8, newsize: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_realloc_aligned_at(mi_get_default_heap(), p, newsize, alignment, offset);
}

pub fn mi_realloc_aligned(p: *u8, newsize: usize, alignment: usize) ?*u8 {
    return mi_heap_realloc_aligned(mi_get_default_heap(), p, newsize, alignment);
}

pub fn mi_rezalloc_aligned_at(p: *u8, newsize: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_rezalloc_aligned_at(mi_get_default_heap(), p, newsize, alignment, offset);
}

pub fn mi_rezalloc_aligned(p: *u8, newsize: usize, alignment: usize) ?*u8 {
    return mi_heap_rezalloc_aligned(mi_get_default_heap(), p, newsize, alignment);
}

pub fn mi_recalloc_aligned_at(p: *u8, newcount: usize, size: usize, alignment: usize, offset: usize) ?*u8 {
    return mi_heap_recalloc_aligned_at(mi_get_default_heap(), p, newcount, size, alignment, offset);
}

pub fn mi_recalloc_aligned(p: *u8, newcount: usize, size: usize, alignment: usize) ?*u8 {
    return mi_heap_recalloc_aligned(mi_get_default_heap(), p, newcount, size, alignment);
}
