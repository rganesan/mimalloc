//----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//----------------------------------------------------------------------------

const std = @import("std");
const builtin = std.builtin;
const assert = std.debug.assert;
const os = std.os;
const mem = std.mem;
const AtomicOrder = builtin.AtomicOrder;
const Atomic = std.atomic.Atomic;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("os.zig");
    usingnamespace @import("bitmap.zig");
    usingnamespace @import("options.zig");
    fn noop(cond: bool) void {
        _ = cond;
    }
};

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

// #defines

const MI_DEBUG = mi.MI_DEBUG;
const MI_SECURE = mi.MI_SECURE;
const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;

const MI_SEGMENT_SIZE = mi.MI_SEGMENT_SIZE;
const MI_SEGMENT_ALIGN = mi.MI_SEGMENT_ALIGN;

// typedefs
const mi_arena_id_t = mi.mi_arena_id_t;
const mi_os_tld_t = mi.mi_os_tld_t;
const mi_bitmap_field_t = mi.mi_bitmap_field_t;
const mi_bitmap_index_t = mi.mi_bitmap_index_t;
const mi_stats_t = mi.mi_stats_t;

// functions
const mi_os_get_aligned_hint = mi.mi_os_get_aligned_hint;
const mi_align_up_ptr = mi.mi_align_up_ptr;
const _mi_align_up = mi._mi_align_up;
const _mi_align_down = mi._mi_align_down;
const _mi_os_page_size = mi._mi_os_page_size;
const _mi_divide_up = mi._mi_divide_up;
const _mi_os_decommit = mi._mi_os_decommit;

const _mi_bitmap_try_find_from_claim_across = mi._mi_bitmap_try_find_from_claim_across;
const _mi_bitmap_claim_across = mi._mi_bitmap_claim_across;
const _mi_bitmap_unclaim_across = mi._mi_bitmap_unclaim_across;
const _mi_bitmap_is_claimed_across = mi._mi_bitmap_is_claimed_across;
const mi_bitmap_index_field = mi.mi_bitmap_index_field;
const mi_bitmap_index_bit = mi.mi_bitmap_index_bit;

const _mi_os_commit = mi._mi_os_commit;
const _mi_os_numa_node = mi._mi_os_numa_node;

const _mi_os_alloc_aligned_offset = mi._mi_os_alloc_aligned_offset;
const _mi_os_free_aligned = mi._mi_os_free_aligned;

const mi_option_is_enabled = mi.mi_option_is_enabled;

const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_load_ptr_relaxed = mi.mi_atomic_load_ptr_relaxed;
const mi_atomic_store_relaxed = mi.mi_atomic_store_relaxed;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

//----------------------------------------------------------------------------
// "Arenas" are fixed area's of OS memory from which we can allocate
// large blocks (>= MI_ARENA_MIN_BLOCK_SIZE, 4MiB).
// In contrast to the rest of mimalloc, the arenas are shared between
// threads and need to be accessed using atomic operations.
//
// Currently arenas are only used to for huge OS page (1GiB) reservations,
// or direct OS memory reservations -- otherwise it delegates to direct allocation from the OS.
// In the future, we can expose an API to manually add more kinds of arenas
// which is sometimes needed for embedded devices or shared memory for example.
// (We can also employ this with WASI or `sbrk` systems to reserve large arenas
//  on demand and be able to reuse them efficiently).
//
// The arena allocation needs to be thread safe and we use an atomic bitmap to allocate.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------
//  Arena allocation
//-----------------------------------------------------------

// Block info: bit 0 contains the `in_use` bit, the upper bits the
// size in count of arena blocks.
const mi_block_info_t = usize;

const MI_ARENA_BLOCK_SIZE = mi.MI_SEGMENT_SIZE; // 64MiB  (must be at least MI_SEGMENT_ALIGN)
const MI_ARENA_MIN_OBJ_SIZE = MI_ARENA_BLOCK_SIZE / 2; // 32MiB
const MI_MAX_ARENAS = 64; // not more than 126 (since we use 7 bits in the memid and an arena index + 1)

// A memory arena descriptor
const mi_arena_t = struct {
    id: mi_arena_id_t, // arena id; 0 for non-specific
    exclusive: bool, // only allow allocations if specifically for this arena
    start: Atomic([*]u8), // the start of the memory area
    block_count: usize, // size of the area in arena blocks (of `MI_ARENA_BLOCK_SIZE`)
    field_count: usize, // number of bitmap fields (where `field_count * MI_BITMAP_FIELD_BITS >= block_count`)
    numa_node: i32, // associated NUMA node
    is_zero_init: bool, // is the arena zero initialized?
    allow_decommit: bool, // is decommit allowed? if true, is_large should be false and blocks_committed != NULL
    is_large: bool, // large- or huge OS pages (always committed)
    search_idx: Atomic(usize), // optimization to start the search for free blocks
    blocks_dirty: [*]mi_bitmap_field_t, // are the blocks potentially non-zero?
    blocks_committed: ?[*]mi_bitmap_field_t, // are the blocks committed? (can be NULL for memory that cannot be decommitted)
    blocks_inuse: []mi_bitmap_field_t, // in-place bitmap of in-use blocks (of size `field_count`)
};

// The available arenas
var mi_arenas = [_]Atomic(?*align(std.atomic.cache_line) mi_arena_t){Atomic(?*align(std.atomic.cache_line) mi_arena_t).init(null)} ** MI_MAX_ARENAS;
// static mi_decl_cache_align _Atomic(mi_arena_t*) mi_arenas[MI_MAX_ARENAS];
// static mi_decl_cache_align _Atomic(size_t)      mi_arena_count; // = 0
var mi_arena_count = Atomic(usize).init(0);

//-----------------------------------------------------------
//  Arena id's
//  0 is used for non-arena's (like OS memory)
//  id = arena_index + 1
//-----------------------------------------------------------

fn mi_arena_id_index(id: mi_arena_id_t) usize {
    return @intCast(usize, if (id <= 0) MI_MAX_ARENAS else id - 1);
}

fn mi_arena_id_create(arena_index: usize) mi_arena_id_t {
    mi_assert_internal(arena_index < MI_MAX_ARENAS);
    mi_assert_internal(MI_MAX_ARENAS <= 126);
    const id = @intCast(mi_arena_id_t, arena_index + 1);
    mi_assert_internal(id >= 1 and id <= 127);
    return id;
}

pub fn _mi_arena_id_none() mi_arena_id_t {
    return 0;
}

fn mi_arena_id_is_suitable(arena_id: mi_arena_id_t, arena_is_exclusive: bool, req_arena_id: mi_arena_id_t) bool {
    return ((!arena_is_exclusive and req_arena_id == _mi_arena_id_none()) or
        (arena_id == req_arena_id));
}

//-----------------------------------------------------------
//  Arena allocations get a memory id where the lower 8 bits are
//  the arena id, and the upper bits the block index.
//-----------------------------------------------------------

// Use `0` as a special id for direct OS allocated memory.
const MI_MEMID_OS = @intCast(mi_arena_id_t, 0);

fn mi_arena_memid_create(id: mi_arena_id_t, exclusive: bool, bitmap_index: mi_bitmap_index_t) usize {
    mi_assert_internal(((bitmap_index << 8) >> 8) == bitmap_index); // no overflow?
    mi_assert_internal(id >= 0 and id <= 0x7F);
    return ((bitmap_index << 8) | ((@intCast(usize, id) & 0x7F) | if (exclusive) @intCast(usize, 0x80) else 0));
}

fn mi_arena_memid_indices(arena_memid: usize, arena_index: *usize, bitmap_index: *mi_bitmap_index_t) bool {
    bitmap_index.* = (arena_memid >> 8);
    const id = @intCast(mi_arena_id_t, (arena_memid & 0x7F));
    arena_index.* = mi_arena_id_index(id);
    return ((arena_memid & 0x80) != 0);
}

pub fn _mi_arena_memid_is_suitable(arena_memid: usize, request_arena_id: mi_arena_id_t) bool {
    const id = @intCast(mi_arena_id_t, arena_memid & 0x7F);
    const exclusive = ((arena_memid & 0x80) != 0);
    return mi_arena_id_is_suitable(id, exclusive, request_arena_id);
}

fn mi_block_count_of_size(size: usize) usize {
    return _mi_divide_up(size, MI_ARENA_BLOCK_SIZE);
}

//-----------------------------------------------------------
//  Thread safe allocation in an arena
//-----------------------------------------------------------
fn mi_arena_alloc(arena: *mi_arena_t, blocks: usize, bitmap_idx: *mi_bitmap_index_t) bool {
    const idx = mi_atomic_load_relaxed(&arena.search_idx); // start from last search; ok to be relaxed as the exact start does not matter
    if (_mi_bitmap_try_find_from_claim_across(arena.blocks_inuse.ptr, arena.field_count, idx, blocks, bitmap_idx)) {
        mi_atomic_store_relaxed(&arena.search_idx, mi_bitmap_index_field(bitmap_idx.*)); // start search from found location next time around
        return true;
    }
    return false;
}

test "test" {
    try std.testing.expect(mi_arena_count.load(AtomicOrder.Monotonic) == 0);
    try std.testing.expect(mi_arenas.len == MI_MAX_ARENAS);
    try std.testing.expect(mi_arena_memid_create(1, true, 1) == 0x181);
}

//-----------------------------------------------------------
//  Arena Allocation
//-----------------------------------------------------------

fn mi_arena_alloc_from(arena: *mi_arena_t, arena_index: usize, needed_bcount: usize, commit: *bool, large: *bool, is_pinned: *bool, is_zero: *bool, req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?[*]u8 {
    mi_assert_internal(mi_arena_id_index(arena.id) == arena_index);
    if (!mi_arena_id_is_suitable(arena.id, arena.exclusive, req_arena_id)) return null;

    var bitmap_index: mi_bitmap_index_t = undefined;
    if (!mi_arena_alloc(arena, needed_bcount, &bitmap_index)) return null;

    // claimed it! set the dirty bits (todo: no need for an atomic op here?)
    const p = arena.start.load(AtomicOrder.Unordered) + (mi_bitmap_index_bit(bitmap_index) * MI_ARENA_BLOCK_SIZE);
    memid.* = mi_arena_memid_create(arena.id, arena.exclusive, bitmap_index);
    is_zero.* = _mi_bitmap_claim_across(arena.blocks_dirty, arena.field_count, needed_bcount, bitmap_index, null);
    large.* = arena.is_large;
    is_pinned.* = (arena.is_large or !arena.allow_decommit);
    if (arena.blocks_committed == null) {
        // always committed
        commit.* = true;
    } else if (commit.*) {
        // arena not committed as a whole, but commit requested: ensure commit now
        var any_uncommitted: bool = undefined;
        _ = _mi_bitmap_claim_across(arena.blocks_committed.?, arena.field_count, needed_bcount, bitmap_index, &any_uncommitted);
        if (any_uncommitted) {
            var commit_zero: bool = undefined;
            _ = _mi_os_commit(@alignCast(mem.page_size, p), needed_bcount * MI_ARENA_BLOCK_SIZE, &commit_zero, tld.stats.?);
            if (commit_zero) is_zero.* = true;
        }
    } else {
        // no need to commit, but check if already fully committed
        commit.* = _mi_bitmap_is_claimed_across(arena.blocks_committed.?, arena.field_count, needed_bcount, bitmap_index);
    }
    return p;
}

// allocate from an arena with fallback to the OS
fn mi_arena_allocate(numa_node: i32, size: usize, alignment: usize, commit: *bool, large: *bool, is_pinned: *bool, is_zero: *bool, req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?[*]u8 {
    mi_assert_internal(alignment <= MI_SEGMENT_ALIGN);
    const max_arena = mi_atomic_load_relaxed(&mi_arena_count);
    const bcount = mi_block_count_of_size(size);
    if (mi_likely(max_arena == 0)) return null;
    mi_assert_internal(size <= bcount * MI_ARENA_BLOCK_SIZE);

    const arena_index = mi_arena_id_index(req_arena_id);
    if (arena_index < MI_MAX_ARENAS) {
        // try a specific arena if requested
        // const arena = mi_atomic_load_ptr_relaxed(mi_arena_t, &mi_arenas[arena_index]);
        const arena = mi_arenas[arena_index].load(AtomicOrder.Monotonic) orelse return null;
        if ((arena.numa_node < 0 or arena.numa_node == numa_node) and // numa local?
            (large.* or !arena.is_large))
        { // large OS pages allowed, or arena is not large OS pages
            const p = mi_arena_alloc_from(arena, arena_index, bcount, commit, large, is_pinned, is_zero, req_arena_id, memid, tld);
            mi_assert_internal(@ptrToInt(p) % alignment == 0);
            if (p != null) return p;
        }
    } else {
        // try numa affine allocation
        var i: usize = 0;
        while (i < max_arena) : (i += 1) {
            // const arena = mi_atomic_load_ptr_relaxed(mi_arena_t, &mi_arenas[i]);
            const arena = mi_arenas[arena_index].load(AtomicOrder.Monotonic) orelse break; // end reached if null
            if ((arena.numa_node < 0 or arena.numa_node == numa_node) and // numa local?
                (large.* or !arena.is_large))
            { // large OS pages allowed, or arena is not large OS pages
                const p = mi_arena_alloc_from(arena, i, bcount, commit, large, is_pinned, is_zero, req_arena_id, memid, tld);
                mi_assert_internal(@ptrToInt(p) % alignment == 0);
                if (p != null) return p;
            }
        }

        // try from another numa node instead..
        i = 0;
        while (i < max_arena) : (i += 1) {
            // const arena = mi_atomic_load_ptr_relaxed(mi_arena_t, &mi_arenas[i]);
            const arena = mi_arenas[arena_index].load(AtomicOrder.Monotonic) orelse break; // end reached if null
            if ((arena.numa_node >= 0 and arena.numa_node != numa_node) and // not numa local!
                (large.* or !arena.is_large))
            { // large OS pages allowed, or arena is not large OS pages
                const p = mi_arena_alloc_from(arena, i, bcount, commit, large, is_pinned, is_zero, req_arena_id, memid, tld);
                mi_assert_internal(@ptrToInt(p) % alignment == 0);
                if (p != null) return p;
            }
        }
    }
    return null;
}

pub fn _mi_arena_alloc_aligned(size: usize, alignment: usize, align_offset: usize, commit: *bool, large_in: ?*bool, is_pinned: *bool, is_zero: *bool, req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?[*]u8 {
    mi_assert_internal(size > 0);
    memid.* = MI_MEMID_OS;
    is_zero.* = false;
    is_pinned.* = false;

    var default_large = false;
    const large = if (large_in == null) &default_large else large_in.?; // ensure `large != null`
    const numa_node = _mi_os_numa_node(tld); // current numa node

    // try to allocate in an arena if the alignment is small enough and the object is not too small (as for heap meta data)
    if (size >= MI_ARENA_MIN_OBJ_SIZE and alignment <= MI_SEGMENT_ALIGN and align_offset == 0) {
        const p = mi_arena_allocate(numa_node, size, alignment, commit, large, is_pinned, is_zero, req_arena_id, memid, tld);
        if (p != null) return p;
    }

    // finally, fall back to the OS
    if (mi_option_is_enabled(.mi_option_limit_os_alloc) or req_arena_id != _mi_arena_id_none()) {
        // errno = ENOMEM;
        return null;
    }
    is_zero.* = true;
    memid.* = MI_MEMID_OS;
    const p = _mi_os_alloc_aligned_offset(size, alignment, align_offset, commit.*, large, tld.stats.?);
    if (p != null) {
        is_pinned.* = large.*;
    }
    return p;
}

pub fn _mi_arena_alloc(size: usize, commit: *bool, large: *bool, is_pinned: *bool, is_zero: *bool, req_arena_id: mi_arena_id_t, memid: *usize, tld: *mi_os_tld_t) ?[*]u8 {
    return _mi_arena_alloc_aligned(size, MI_ARENA_BLOCK_SIZE, 0, commit, large, is_pinned, is_zero, req_arena_id, memid, tld);
}

pub fn mi_arena_area(arena_id: mi_arena_id_t, size: ?*usize) ?[*]u8 {
    if (size != null) size.?.* = 0;
    const arena_index = mi_arena_id_index(arena_id);
    if (arena_index >= MI_MAX_ARENAS) return null;
    // const arena = mi_atomic_load_ptr_relaxed(arena_t, &mi_arenas[arena_index]);
    const arena = mi_arenas[arena_index].load(AtomicOrder.Monotonic) orelse return null;
    if (size != null) size.?.* = arena.block_count * MI_ARENA_BLOCK_SIZE;
    return arena.start.load(AtomicOrder.Unordered);
}

//-----------------------------------------------------------
//  Arena free
//-----------------------------------------------------------

pub fn _mi_arena_free(p: [*]u8, size: usize, alignment: usize, align_offset: usize, memid: usize, all_committed: bool, stats: *mi_stats_t) void {
    mi_assert_internal(size > 0);
    if (size == 0) return;

    if (memid == MI_MEMID_OS) {
        // was a direct OS allocation, pass through
        _mi_os_free_aligned(@alignCast(mem.page_size, p), size, alignment, align_offset, all_committed, stats);
    } else {
        // allocated in an arena
        mi_assert_internal(align_offset == 0);
        var arena_idx: usize = undefined;
        var bitmap_idx: usize = undefined;
        _ = mi_arena_memid_indices(memid, &arena_idx, &bitmap_idx);
        mi_assert_internal(arena_idx < MI_MAX_ARENAS);
        // const arena = mi_atomic_load_ptr_relaxed(mi_arena_t, &mi_arenas[arena_idx]);
        const arena = mi_arenas[arena_idx].load(AtomicOrder.Monotonic) orelse unreachable;
        const blocks = mi_block_count_of_size(size);
        // checks
        mi_assert_internal(arena.field_count > mi_bitmap_index_field(bitmap_idx));
        if (arena.field_count <= mi_bitmap_index_field(bitmap_idx)) {
            std.log.err("trying to free from non-existent arena block: {*}, size {}, memid: {x}\n", .{ p, size, memid });
            return;
        }
        // potentially decommit
        if (!arena.allow_decommit or arena.blocks_committed == null) {
            mi_assert_internal(all_committed); // note: may be not true as we may "pretend" to be not committed (in segment.c)
        } else {
            mi_assert_internal(arena.blocks_committed != null);
            _ = _mi_os_decommit(@alignCast(mem.page_size, p), blocks * MI_ARENA_BLOCK_SIZE, stats); // ok if this fails
            _ = _mi_bitmap_unclaim_across(arena.blocks_committed.?, arena.field_count, blocks, bitmap_idx);
        }
        // and make it available to others again
        const all_inuse = _mi_bitmap_unclaim_across(arena.blocks_inuse.ptr, arena.field_count, blocks, bitmap_idx);
        if (!all_inuse) {
            std.log.warn("trying to free an already freed block: {*}, size {}\n", .{ p, size });
            return;
        }
    }
}

test "references" {
    std.testing.refAllDeclsRecursive(@This());
}
