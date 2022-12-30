// ----------------------------------------------------------------------------
// Copyright (c) 2020, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// Concurrent bitmap that can set/reset sequences of bits atomically,
// represeted as an array of fields where each field is a machine word (`size_t`)
//
// There are two api's; the standard one cannot have sequences that cross
// between the bitmap fields (and a sequence must be <= MI_BITMAP_FIELD_BITS).
// (this is used in region allocation)
//
// The `_across` postfixed functions do allow sequences that can cross over
// between the fields. (This is used in arena allocation)
//-----------------------------------------------------------------------------

const std = @import("std");
const math = std.math;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");

    fn noop(cond: bool) void {
        _ = cond;
    }
};

const MI_DEBUG = mi.MI_DEBUG;
const MI_SIZE_SIZE = mi.MI_SIZE_SIZE;

const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_cas_weak_acq_rel = mi.mi_atomic_cas_weak_acq_rel;
const mi_atomic_and_acq_rel = mi.mi_atomic_and_acq_rel;
const mi_atomic_or_acq_rel = mi.mi_atomic_or_acq_rel;
const mi_bsr = mi.mi_bsr;

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

//-----------------------------------------------------------
//  Bitmap definition
//-----------------------------------------------------------

pub const MI_BITMAP_FIELD_BITS = (8 * MI_SIZE_SIZE);
pub const MI_BITMAP_FIELD_FULL = ~@intCast(usize, 0); // all bits set

// An atomic bitmap of `size_t` fields
pub const mi_bitmap_field_t = Atomic(usize);
pub const mi_bitmap_t = [*]mi_bitmap_field_t; // this should a slice

// A bitmap index is the index of the bit in a bitmap.
pub const mi_bitmap_index_t = usize;

pub const PredArg = *opaque {};
pub const mi_bitmap_pred_fun_t = *const fn (bitmap_idx: mi_bitmap_index_t, pred_arg: PredArg) bool;

// Create a bit index.
inline fn mi_bitmap_index_create(idx: usize, bitidx: usize) mi_bitmap_index_t {
    mi_assert_internal(bitidx < MI_BITMAP_FIELD_BITS);
    return (idx * MI_BITMAP_FIELD_BITS) + bitidx;
}

// Create a bit index.
pub inline fn mi_bitmap_index_create_from_bit(full_bitidx: usize) mi_bitmap_index_t {
    return mi_bitmap_index_create(full_bitidx / MI_BITMAP_FIELD_BITS, full_bitidx % MI_BITMAP_FIELD_BITS);
}

// Get the field index from a bit index.
inline fn mi_bitmap_index_field(bitmap_idx: mi_bitmap_index_t) usize {
    return (bitmap_idx / MI_BITMAP_FIELD_BITS);
}

// Get the bit index in a bitmap field
inline fn mi_bitmap_index_bit_in_field(bitmap_idx: mi_bitmap_index_t) usize {
    return (bitmap_idx % MI_BITMAP_FIELD_BITS);
}

// Get the full bit index
pub inline fn mi_bitmap_index_bit(bitmap_idx: mi_bitmap_index_t) usize {
    return bitmap_idx;
}

//-----------------------------------------------------------
//  Bitmap definition
//-----------------------------------------------------------

// The bit mask for a given number of blocks at a specified bit index.
inline fn mi_bitmap_mask_(count: usize, bitidx: usize) usize {
    mi_assert_internal(count + bitidx <= MI_BITMAP_FIELD_BITS);
    mi_assert_internal(count > 0);
    if (count >= MI_BITMAP_FIELD_BITS) return MI_BITMAP_FIELD_FULL;
    if (count == 0) return 0;
    return math.shl(usize, math.shl(usize, @intCast(usize, 1), count) - 1, bitidx);
}

//-----------------------------------------------------------
//  Claim a bit sequence atomically
//-----------------------------------------------------------

// Try to atomically claim a sequence of `count` bits in a single
// field at `idx` in `bitmap`. Returns `true` on success.
pub fn _mi_bitmap_try_find_claim_field(bitmap: mi_bitmap_t, idx: usize, count: usize, bitmap_idx: *mi_bitmap_index_t) bool {
    mi_assert_internal(count <= MI_BITMAP_FIELD_BITS);
    mi_assert_internal(count > 0);
    const field = &bitmap[idx];
    var map = mi_atomic_load_relaxed(field);
    if (map == MI_BITMAP_FIELD_FULL) return false; // short cut

    // searches for 0-bit sequence of length count
    const mask: usize = mi_bitmap_mask_(count, 0);
    const bitidx_max = MI_BITMAP_FIELD_BITS - count;

    var bitidx: usize = @ctz(~map); // quickly find the first zero bit if possible
    var m = math.shl(usize, mask, bitidx); // invariant: m == mask shifted by bitidx

    // scan linearly for a free range of zero bits
    while (bitidx <= bitidx_max) {
        const mapm = map & m;
        if (mapm == 0) { // are the mask bits free at bitidx?
            mi_assert_internal(math.shr(usize, m, bitidx) == mask); // no overflow?
            const newmap: usize = map | m;
            mi_assert_internal(math.shr(usize, (newmap ^ map), bitidx) == mask);
            if (!mi_atomic_cas_weak_acq_rel(field, &map, newmap)) { // TODO: use strong cas here?
                // no success, another thread claimed concurrently.. keep going (with updated `map`)
                continue;
            } else {
                // success, we claimed the bits!
                bitmap_idx.* = mi_bitmap_index_create(idx, bitidx);
                return true;
            }
        } else {
            // on to the next bit range
            const shift = if (count == 1) 1 else mi_bsr(mapm) - bitidx + 1;
            mi_assert_internal(shift > 0 and shift <= count);
            bitidx += shift;
            m = math.shl(usize, m, shift);
        }
    }
    // no bits found
    return false;
}

// Find `count` bits of 0 and set them to 1 atomically; returns `true` on success.
// Starts at idx, and wraps around to search in all `bitmap_fields` fields.
// `count` can be at most MI_BITMAP_FIELD_BITS and will never cross fields.
pub fn _mi_bitmap_try_find_from_claim(bitmap: mi_bitmap_t, bitmap_fields: usize, start_field_idx: usize, count: usize, bitmap_idx: *mi_bitmap_index_t) bool {
    var idx = start_field_idx;
    var visited: usize = 0;
    while (visited < bitmap_fields) : (idx += idx + 1) {
        if (idx >= bitmap_fields) idx = 0; // wrap
        if (_mi_bitmap_try_find_claim_field(bitmap, idx, count, bitmap_idx)) {
            return true;
        }
        visited += 1;
    }
    return false;
}

// Like _mi_bitmap_try_find_from_claim but with an extra predicate that must be fullfilled
pub fn _mi_bitmap_try_find_from_claim_pred(bitmap: mi_bitmap_t, bitmap_fields: usize, start_field_idx: usize, count: usize, pred_fun: ?mi_bitmap_pred_fun_t, pred_arg: ?PredArg, bitmap_idx: *mi_bitmap_index_t) bool {
    var idx = start_field_idx;
    var visited: usize = 0;
    while (visited < bitmap_fields) : (idx += idx + 1) {
        if (idx >= bitmap_fields) idx = 0; // wrap
        if (_mi_bitmap_try_find_claim_field(bitmap, idx, count, bitmap_idx)) {
            if (pred_fun != null and pred_fun.?(bitmap_idx.*, pred_arg.?)) {
                return true;
            }
            // predicate returned false, unclaim and look further
            _ = _mi_bitmap_unclaim(bitmap, bitmap_fields, count, bitmap_idx.*);
        }
        visited += 1;
    }
    return false;
}

// Set `count` bits at `bitmap_idx` to 0 atomically
// Returns `true` if all `count` bits were 1 previously.
pub fn _mi_bitmap_unclaim(bitmap: mi_bitmap_t, bitmap_fields: usize, count: usize, bitmap_idx: mi_bitmap_index_t) bool {
    const idx = mi_bitmap_index_field(bitmap_idx);
    const bitidx = mi_bitmap_index_bit_in_field(bitmap_idx);
    const mask = mi_bitmap_mask_(count, bitidx);
    mi_assert_internal(bitmap_fields > idx);
    // mi_assert_internal((bitmap[idx] & mask) == mask);
    const prev = mi_atomic_and_acq_rel(&bitmap[idx], ~mask);
    return ((prev & mask) == mask);
}

// Set `count` bits at `bitmap_idx` to 1 atomically
// Returns `true` if all `count` bits were 0 previously. `any_zero` is `true` if there was at least one zero bit.
pub fn _mi_bitmap_claim(bitmap: mi_bitmap_t, bitmap_fields: usize, count: usize, bitmap_idx: mi_bitmap_index_t, any_zero: ?*bool) bool {
    const idx = mi_bitmap_index_field(bitmap_idx);
    const bitidx = mi_bitmap_index_bit_in_field(bitmap_idx);
    const mask = mi_bitmap_mask_(count, bitidx);
    mi_assert_internal(bitmap_fields > idx);
    //mi_assert_internal(any_zero != NULL || (bitmap[idx] & mask) == 0);
    const prev = mi_atomic_or_acq_rel(&bitmap[idx], mask);
    if (any_zero != null) any_zero.?.* = ((prev & mask) != mask);
    return ((prev & mask) == 0);
}

// Returns `true` if all `count` bits were 1. `any_ones` is `true` if there was at least one bit set to one.
fn mi_bitmap_is_claimedx(bitmap: mi_bitmap_t, bitmap_fields: usize, count: usize, bitmap_idx: mi_bitmap_index_t, any_ones: ?*bool) bool {
    const idx = mi_bitmap_index_field(bitmap_idx);
    const bitidx = mi_bitmap_index_bit_in_field(bitmap_idx);
    const mask = mi_bitmap_mask_(count, bitidx);
    mi_assert_internal(bitmap_fields > idx);
    const field: usize = mi_atomic_load_relaxed(&bitmap[idx]);
    if (any_ones != null) any_ones.?.* = ((field & mask) != 0);
    return ((field & mask) == mask);
}

pub fn _mi_bitmap_is_claimed(bitmap: mi_bitmap_t, bitmap_fields: usize, count: usize, bitmap_idx: mi_bitmap_index_t) bool {
    return mi_bitmap_is_claimedx(bitmap, bitmap_fields, count, bitmap_idx, null);
}

pub fn _mi_bitmap_is_any_claimed(bitmap: mi_bitmap_t, bitmap_fields: usize, count: usize, bitmap_idx: mi_bitmap_index_t) bool {
    var any_ones: bool = undefined;
    mi_bitmap_is_claimedx(bitmap, bitmap_fields, count, bitmap_idx, &any_ones);
    return any_ones;
}

// *_across functions not ported
