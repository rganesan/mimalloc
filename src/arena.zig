//----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//----------------------------------------------------------------------------

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const os = std.os;
const mem = std.mem;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("os.zig");
    fn noop(cond: bool) void {
        _ = cond;
    }
};

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

const mi_arena_id_t = mi.mi_arena_id_t;
const mi_os_tld_t = mi.mi_os_tld_t;

const mi_os_get_aligned_hint = mi.mi_os_get_aligned_hint;
const mi_align_up_ptr = mi.mi_align_up_ptr;
const _mi_align_up = mi._mi_align_up;
const _mi_align_down = mi._mi_align_down;
const _mi_os_page_size = mi._mi_os_page_size;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

// #defines

const MI_DEBUG = mi.MI_DEBUG;
const MI_SECURE = mi.MI_SECURE;
const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;

pub fn _mi_arena_id_none() mi_arena_id_t {
    return 0;
}

fn mi_arena_id_is_suitable(arena_id: mi_arena_id_t, arena_is_exclusive: bool, req_arena_id: mi_arena_id_t) bool {
    return ((!arena_is_exclusive and req_arena_id == _mi_arena_id_none()) or
        (arena_id == req_arena_id));
}

pub fn _mi_arena_memid_is_suitable(arena_memid: usize, request_arena_id: mi_arena_id_t) bool {
    const id = @intCast(mi_arena_id_t, arena_memid & 0x7F);
    const exclusive = ((arena_memid & 0x80) != 0);
    return mi_arena_id_is_suitable(id, exclusive, request_arena_id);
}

// Simplified version, directly get page from OS. Can't use page_allocator because it ignores alignment
pub fn _mi_arena_alloc_aligned(size: usize, alignment: usize, commit: *bool, large: *bool, is_pinned: *bool, is_zero: *bool, req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?*anyopaque {
    _ = large;
    _ = is_pinned;
    _ = is_zero;
    _ = req_arena_id;
    _ = memid;
    _ = tld;

    const hint = mi_os_get_aligned_hint(alignment, size);
    var p = os.mmap(hint, size, os.PROT.WRITE | os.PROT.READ, os.MAP.PRIVATE | os.MAP.ANONYMOUS, -1, alignment) catch return null;

    // if not aligned, free it, overallocate, and unmap around it
    if (@ptrToInt(p.ptr) % alignment != 0) {
        os.munmap(p);
        std.log.warn("unable to allocate aligned OS memory directly, fall back to over-allocation ({} bytes, address: {*}, alignment: {}, commit: {})\n", .{ size, p.ptr, alignment, commit });
        if (size >= (std.math.maxInt(isize) - alignment)) return null; // overflow
        const over_size = size + alignment;
        p = os.mmap(null, over_size, os.PROT.WRITE | os.PROT.READ, os.MAP.PRIVATE | os.MAP.ANONYMOUS, -1, alignment) catch return null;
        // and selectively unmap parts around the over-allocated area. (noop on sbrk)
        const aligned_p = mi_align_up_ptr(p.ptr, alignment);
        const pre_size = @ptrToInt(aligned_p) - @ptrToInt(p.ptr);
        const mid_size = _mi_align_up(size, _mi_os_page_size());
        const post_size = over_size - pre_size - mid_size;
        std.debug.print("pre_size: {}, mid_size: {}, post_size: {}, over_size: {}\n", .{ pre_size, mid_size, post_size, over_size });
        mi_assert_internal(pre_size < over_size and post_size < over_size and mid_size >= size);
        if (pre_size > 0) os.munmap(p[0..pre_size]);
        if (post_size > 0) os.munmap(@alignCast(mem.page_size, p[(pre_size + mid_size)..(pre_size + mid_size + post_size)]));
        // we can return the aligned pointer on `mmap` (and sbrk) systems
        return aligned_p;
    }
    return p.ptr;
}

// Simplified version, directly release memory to page_allocator
pub fn _mi_arena_free(p: *anyopaque, size: usize, alignment: usize, align_offset: usize, memid: usize, all_committed: bool, tld: *mi_os_tld_t) void {
    _ = memid;
    _ = all_committed;
    _ = tld;
    _ = alignment;
    _ = align_offset;
    return os.munmap(@ptrCast([*]u8, @alignCast(mem.page_size, p))[0..size]);
}
