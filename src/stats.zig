//-----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

const std = @import("std");
const fmt = std.fmt;
const builtin = std.builtin;
const AtomicOrder = builtin.AtomicOrder;
const AtomicRmwOp = builtin.AtomicRmwOp;
const assert = std.debug.assert;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("heap.zig");
    usingnamespace @import("page-queue.zig");
};

const MI_STAT = mi.MI_STAT;
const MI_BIN_HUGE = mi.MI_BIN_HUGE;

const mi_stats_t = mi.mi_stats_t;
const mi_stat_count_t = mi.mi_stat_count_t;
const mi_stat_counter_t = mi.mi_stat_counter_t;
const mi_msecs_t = mi.mi_msecs_t;

const _mi_bin_size = mi._mi_bin_size;

const mi_heap_get_default = mi.mi_heap_get_default;
const _mi_os_numa_node_count = mi._mi_os_numa_node_count;

const _mi_stats_main = mi._mi_stats_main;

const mi_output_fun = *const fn (msg: []const u8, arg: ?*anyopaque) void;

//-----------------------------------------------------------
//  Statistics operations
//-----------------------------------------------------------

pub fn mi_is_in_main(stat: *const u8) bool {
    return @ptrToInt(stat) >= @ptrToInt(&mi._mi_stats_main) and @ptrToInt(stat) < (@ptrToInt(&mi._mi_stats_main) + @sizeOf(mi_stats_t));
}

inline fn mi_atomic_addi64_relaxed(p: *volatile i64, x: i64) void {
    _ = @atomicRmw(i64, p, AtomicRmwOp.Add, x, AtomicOrder.Monotonic);
}

inline fn mi_atomic_maxi64_relaxed(p: *volatile i64, x: i64) void {
    var current = @atomicLoad(i64, p, AtomicOrder.Monotonic);
    while (current < x) {
        current = @cmpxchgWeak(i64, p, current, x, AtomicOrder.Monotonic, AtomicOrder.Monotonic) orelse current;
    }
}

fn mi_stat_update(stat: *mi_stat_count_t, amount: i64) void {
    if (amount == 0) return;
    if (mi_is_in_main(@ptrCast(*u8, stat))) {
        // add atomically (for abandoned pages)
        const current = @atomicRmw(i64, &stat.current, AtomicRmwOp.Add, amount, AtomicOrder.Monotonic);
        mi_atomic_maxi64_relaxed(&stat.peak, current + amount);
        if (amount > 0) {
            _ = @atomicRmw(i64, &stat.allocated, AtomicRmwOp.Add, amount, AtomicOrder.Monotonic);
        } else {
            _ = @atomicRmw(i64, &stat.freed, AtomicRmwOp.Sub, amount, AtomicOrder.Monotonic);
        }
    } else {
        // add thread local
        stat.current += amount;
        if (stat.current > stat.peak) stat.peak = stat.current;
        if (amount > 0) {
            stat.allocated += amount;
        } else {
            stat.freed += -amount;
        }
    }
}

pub fn _mi_stat_counter_increase(stat: *mi_stat_counter_t, amount: i64) void {
    if (mi_is_in_main(@ptrCast(*u8, stat))) {
        _ = @atomicRmw(i64, &stat.count, AtomicRmwOp.Add, 1, AtomicOrder.Monotonic);
        _ = @atomicRmw(i64, &stat.total, AtomicRmwOp.Add, @intCast(i64, amount), AtomicOrder.Monotonic);
    } else {
        stat.count += 1;
        stat.total += amount;
    }
}

pub fn _mi_stat_increase(stat: *mi_stat_count_t, amount: usize) void {
    mi_stat_update(stat, @intCast(i64, amount));
}

pub fn _mi_stat_decrease(stat: *mi_stat_count_t, amount: usize) void {
    mi_stat_update(stat, -@intCast(i64, amount));
}

// must be thread safe as it is called from stats_merge
fn mi_stat_add(stat: *mi_stat_count_t, src: *const mi_stat_count_t, unit: i64) void {
    if (stat == src) return;
    if (src.allocated == 0 and src.freed == 0) return;
    mi_atomic_addi64_relaxed(&stat.allocated, src.allocated * unit);
    mi_atomic_addi64_relaxed(&stat.current, src.current * unit);
    mi_atomic_addi64_relaxed(&stat.freed, src.freed * unit);
    // peak scores do not work across threads..
    mi_atomic_addi64_relaxed(&stat.peak, src.peak * unit);
}

fn mi_stat_counter_add(stat: *mi_stat_counter_t, src: *const mi_stat_counter_t, unit: i64) void {
    if (stat == src) return;
    mi_atomic_addi64_relaxed(&stat.total, src.total * unit);
    mi_atomic_addi64_relaxed(&stat.count, src.count * unit);
}

// must be thread safe as it is called from stats_merge
fn mi_stats_add(stats: *mi_stats_t, src: *const mi_stats_t) void {
    if (stats == src) return;
    mi_stat_add(&stats.segments, &src.segments, 1);
    mi_stat_add(&stats.pages, &src.pages, 1);
    mi_stat_add(&stats.reserved, &src.reserved, 1);
    mi_stat_add(&stats.committed, &src.committed, 1);
    mi_stat_add(&stats.reset, &src.reset, 1);
    mi_stat_add(&stats.page_committed, &src.page_committed, 1);

    mi_stat_add(&stats.pages_abandoned, &src.pages_abandoned, 1);
    mi_stat_add(&stats.segments_abandoned, &src.segments_abandoned, 1);
    mi_stat_add(&stats.threads, &src.threads, 1);

    mi_stat_add(&stats.malloc, &src.malloc, 1);
    mi_stat_add(&stats.segments_cache, &src.segments_cache, 1);
    mi_stat_add(&stats.normal, &src.normal, 1);
    mi_stat_add(&stats.huge, &src.huge, 1);
    mi_stat_add(&stats.large, &src.large, 1);

    mi_stat_counter_add(&stats.pages_extended, &src.pages_extended, 1);
    mi_stat_counter_add(&stats.mmap_calls, &src.mmap_calls, 1);
    mi_stat_counter_add(&stats.commit_calls, &src.commit_calls, 1);

    mi_stat_counter_add(&stats.page_no_retire, &src.page_no_retire, 1);
    mi_stat_counter_add(&stats.searches, &src.searches, 1);
    mi_stat_counter_add(&stats.normal_count, &src.normal_count, 1);
    mi_stat_counter_add(&stats.huge_count, &src.huge_count, 1);
    mi_stat_counter_add(&stats.large_count, &src.large_count, 1);
    if (MI_STAT > 1) {
        var i: usize = 0;
        while (i <= MI_BIN_HUGE) : (i += 1) {
            if (src.normal_bins[i].allocated > 0 or src.normal_bins[i].freed > 0) {
                mi_stat_add(&stats.normal_bins[i], &src.normal_bins[i], 1);
            }
        }
    }
}

//-----------------------------------------------------------
//  Display statistics
//-----------------------------------------------------------

const mi_print = std.log.info;

// unit > 0 : size in binary bytes
// unit == 0: count as decimal
// unit < 0 : count in binary
fn mi_printf_amount(n_in: i64, unit: i64, out: ?mi_output_fun, arg: ?*anyopaque, format: ?[]const u8) void {
    var n = n_in;
    _ = out;
    _ = arg;
    var buf: [32]u8 = undefined;
    buf[0] = 0;
    const suffix = if (unit <= 0) " " else "B";
    const base: i64 = if (unit == 0) 1000 else 1024;
    if (unit > 0) n *= unit;

    const pos = if (n < 0) -n else n;
    if (pos < base) {
        if (n != 1 or suffix[0] != 'B') { // skip printing 1 B for the unit column
            _ = fmt.bufPrint(&buf, "{} {s:<3}", .{ n, if (n == 0) "" else suffix }) catch return;
        }
    } else {
        var divider = base;
        var magnitude = "K";
        if (pos >= divider * base) {
            divider *= base;
            magnitude = "M";
        }
        if (pos >= divider * base) {
            divider *= base;
            magnitude = "G";
        }
        const tens = @divTrunc(n, @divTrunc(divider, 10));
        const whole = @divTrunc(tens, 10);
        const frac1 = @mod(tens, 10);
        var unitdesc: [8]u8 = undefined;
        _ = fmt.bufPrint(&unitdesc, "{s}{s}{s}", .{ magnitude, if (base == 1024) "i" else "", suffix }) catch return;
        _ = fmt.bufPrint(&buf, "{}.{} {s:<3}", .{ whole, if (frac1 < 0) -frac1 else frac1, unitdesc }) catch return;
    }
    _ = format;
    // TODO: Don't know how to deal with non-comptime format in zig
    mi_print("{s:11}", .{buf});
}

fn mi_print_amount(n: i64, unit: i64, out: ?mi_output_fun, arg: ?*anyopaque) void {
    mi_printf_amount(n, unit, out, arg, null);
}

fn mi_print_count(n: i64, unit: i64, out: ?mi_output_fun, arg: ?*anyopaque) void {
    if (unit == 1) {
        mi_print("{s:11}", .{" "});
    } else {
        mi_print_amount(n, 0, out, arg);
    }
}

pub fn mi_stat_print(stat: *const mi_stat_count_t, msg: []const u8, unit: i64, out: ?mi_output_fun, arg: ?*anyopaque) void {
    mi_print("{s:10}", .{msg});
    if (unit > 0) {
        mi_print_amount(stat.peak, unit, out, arg);
        mi_print_amount(stat.allocated, unit, out, arg);
        mi_print_amount(stat.freed, unit, out, arg);
        mi_print_amount(stat.current, unit, out, arg);
        mi_print_amount(unit, 1, out, arg);
        mi_print_count(stat.allocated, unit, out, arg);
        if (stat.allocated > stat.freed) {
            mi_print("  not all freed!\n", .{});
        } else {
            mi_print("  ok\n", .{});
        }
    } else if (unit < 0) {
        mi_print_amount(stat.peak, -1, out, arg);
        mi_print_amount(stat.allocated, -1, out, arg);
        mi_print_amount(stat.freed, -1, out, arg);
        mi_print_amount(stat.current, -1, out, arg);
        if (unit == -1) {
            mi_print("{s:22}", .{""});
        } else {
            mi_print_amount(-unit, 1, out, arg);
            mi_print_count(@divTrunc(stat.allocated, -unit), 0, out, arg);
        }
        if (stat.allocated > stat.freed) {
            mi_print("  not all freed!\n", .{});
        } else {
            mi_print("  ok\n", .{});
        }
    } else {
        mi_print_amount(stat.peak, 1, out, arg);
        mi_print_amount(stat.allocated, 1, out, arg);
        mi_print("{s:11}", .{" "}); // no freed
        mi_print_amount(stat.current, 1, out, arg);
        mi_print("\n", .{});
    }
}

fn mi_stat_counter_print(stat: *const mi_stat_counter_t, msg: []const u8, out: mi_output_fun, arg: ?*anyopaque) void {
    mi_print("{s:10}", .{msg});
    mi_print_amount(stat.total, -1, out, arg);
    mi_print("\n", .{});
}

fn mi_stat_counter_print_avg(stat: *const mi_stat_counter_t, msg: []const u8, out: mi_output_fun, arg: ?*anyopaque) void {
    _ = out;
    _ = arg;
    const avg_tens = if (stat.count == 0) 0 else @divTrunc(stat.total * 10, stat.count);
    const avg_whole = @divTrunc(avg_tens, 10);
    const avg_frac1 = @mod(avg_tens, 10);
    mi_print("{s:10} {:5}.{} avg\n", .{ msg, avg_whole, avg_frac1 });
}

fn mi_print_header(out: ?mi_output_fun, arg: ?*anyopaque) void {
    _ = out;
    _ = arg;
    mi_print("{s:10} {s:10} {s:10} {s:10} {s:10} {s:10} {s:10}", //
        .{ "heap stats", "peak   ", "total   ", "freed   ", "current   ", "unit   ", "count   " });
}

fn mi_stats_print_bins(bins: [*]const mi_stat_count_t, max: usize, format: []const u8, out: ?mi_output_fun, arg: ?*anyopaque) void {
    if (MI_STAT <= 1) return;

    var found = false;
    var buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i <= max) : (i += 1) {
        if (bins[i].allocated > 0) {
            found = true;
            const unit = _mi_bin_size(@intCast(u8, i));
            _ = fmt.bufPrint(&buf, "{s} {:3}", .{ format, i }) catch return;
            mi_stat_print(&bins[i], &buf, @intCast(i64, unit), out, arg);
        }
    }
    if (found) {
        mi_print("\n", .{});
        mi_print_header(out, arg);
    }
}

//------------------------------------------------------------
// Use an output wrapper for line-buffered output
// (which is nice when using loggers etc.)
//------------------------------------------------------------
const buffered_t = struct {
    out: mi_output_fun, // original output function
    arg: ?*anyopaque, // and state
    buf: []u8, // local buffer of at least size `count+1`
    used: usize, // currently used chars `used <= count`
    count: usize, // total chars available for output
};

fn mi_buffered_flush(buf: *buffered_t) void {
    buf.buf[buf.used] = 0;
    mi_print("{s}", .{buf.buf});
    buf.used = 0;
}

fn mi_buffered_out(msg: []const u8, arg: ?*anyopaque) void {
    var buf = @ptrCast(*buffered_t, @alignCast(@alignOf(buffered_t), arg));
    for (msg) |c| {
        if (buf.used >= buf.count) mi_buffered_flush(buf);
        assert(buf.used < buf.count);
        buf.buf[buf.used] = c;
        buf.used += 1;
        if (c == '\n') mi_buffered_flush(buf);
    }
}

//------------------------------------------------------------
// Print statistics
//------------------------------------------------------------

fn mi_stat_process_info(elapsed: *mi_msecs_t, utime: *mi_msecs_t, stime: *mi_msecs_t, current_rss: *usize, peak_rss: *usize, current_commit: *usize, peak_commit: *usize, page_faults: *usize) void {
    elapsed.* = 0;
    utime.* = 0;
    stime.* = 0;
    current_rss.* = 0;
    peak_rss.* = 0;
    current_commit.* = 0;
    peak_commit.* = 0;
    page_faults.* = 0;
}

fn _mi_stats_print(stats: *mi_stats_t, out0: mi_output_fun, arg0: ?*anyopaque) void {
    // wrap the output function to be line buffered
    var buf: [256]u8 = undefined;
    var buffer = buffered_t{ .out = out0, .arg = arg0, .buf = &buf, .used = 0, .count = 255 };
    const out = &mi_buffered_out;
    const arg = &buffer;

    // and print using that
    mi_print_header(out, arg);
    if (MI_STAT > 1)
        mi_stats_print_bins(&stats.normal_bins, MI_BIN_HUGE, "normal", out, arg);
    if (MI_STAT > 0) {
        mi_stat_print(&stats.normal, "normal", if (stats.normal_count.count == 0) 1 else -@divTrunc(stats.normal.allocated, stats.normal_count.count), out, arg);
        mi_stat_print(&stats.large, "large", if (stats.large_count.count == 0) 1 else -@divTrunc(stats.large.allocated, stats.large_count.count), out, arg);
        mi_stat_print(&stats.huge, "huge", if (stats.huge_count.count == 0) 1 else -@divTrunc(stats.huge.allocated, stats.huge_count.count), out, arg);
        var total: mi_stat_count_t = .{};
        mi_stat_add(&total, &stats.normal, 1);
        mi_stat_add(&total, &stats.large, 1);
        mi_stat_add(&total, &stats.huge, 1);
        mi_stat_print(&total, "total", 1, out, arg);
    }
    if (MI_STAT > 1) {
        mi_stat_print(&stats.malloc, "malloc req", 1, out, arg);
        mi_print("\n", .{});
    }
    mi_stat_print(&stats.reserved, "reserved", 1, out, arg);
    mi_stat_print(&stats.committed, "committed", 1, out, arg);
    mi_stat_print(&stats.reset, "reset", 1, out, arg);
    mi_stat_print(&stats.page_committed, "touched", 1, out, arg);
    mi_stat_print(&stats.segments, "segments", -1, out, arg);
    mi_stat_print(&stats.segments_abandoned, "-abandoned", -1, out, arg);
    mi_stat_print(&stats.segments_cache, "-cached", -1, out, arg);
    mi_stat_print(&stats.pages, "pages", -1, out, arg);
    mi_stat_print(&stats.pages_abandoned, "-abandoned", -1, out, arg);
    mi_stat_counter_print(&stats.pages_extended, "-extended", out, arg);
    mi_stat_counter_print(&stats.page_no_retire, "-noretire", out, arg);
    mi_stat_counter_print(&stats.mmap_calls, "mmaps", out, arg);
    mi_stat_counter_print(&stats.commit_calls, "commits", out, arg);
    mi_stat_print(&stats.threads, "threads", -1, out, arg);
    mi_stat_counter_print_avg(&stats.searches, "searches", out, arg);
    mi_print("{s:10} {:7}\n", .{ "numa nodes", _mi_os_numa_node_count() });

    var elapsed: mi_msecs_t = undefined;
    var user_time: mi_msecs_t = undefined;
    var sys_time: mi_msecs_t = undefined;
    var current_rss: usize = undefined;
    var peak_rss: usize = undefined;
    var current_commit: usize = undefined;
    var peak_commit: usize = undefined;
    var page_faults: usize = undefined;
    mi_stat_process_info(&elapsed, &user_time, &sys_time, &current_rss, &peak_rss, &current_commit, &peak_commit, &page_faults);
    mi_print("{s:10} {:7}.{:03} s\n", .{ "elapsed", @divTrunc(elapsed, 1000), @divTrunc(elapsed, 1000) });
    mi_print("{s:10}: user: {}.{:03} s, system: {}.{:03} s, faults: {}, rss: ", .{ "process", @divTrunc(user_time, 1000), @mod(user_time, 1000), @divTrunc(sys_time, 1000), @mod(sys_time, 1000), page_faults });
    mi_printf_amount(@intCast(i64, peak_rss), 1, out, arg, "%s");
    if (peak_commit > 0) {
        mi_print(", commit: ", .{});
        mi_printf_amount(@intCast(i64, peak_commit), 1, out, arg, "%s");
    }
    mi_print("\n", .{});
}

var mi_process_start: mi_msecs_t = 0;

fn mi_stats_get_default() *mi_stats_t {
    const heap = mi_heap_get_default();
    return &heap.tld.?.stats;
}

fn mi_stats_merge_from(stats: *mi_stats_t) void {
    if (stats != &mi._mi_stats_main) {
        mi_stats_add(&mi._mi_stats_main, stats);
        stats.* = .{};
    }
}

pub fn mi_stats_reset() void {
    var stats = mi_stats_get_default();
    if (stats != &mi._mi_stats_main) {
        stats.* = .{};
    }
    mi._mi_stats_main = .{};
    if (mi_process_start == 0) {
        mi_process_start = _mi_clock_start();
    }
}

fn mi_stats_merge() void {
    mi_stats_merge_from(mi_stats_get_default());
}

fn _mi_stats_done(stats: *mi_stats_t) void { // called from `mi_thread_done`
    mi_stats_merge_from(stats);
}

fn mi_stats_print_out(out: mi_output_fun, arg: ?*anyopaque) void {
    mi_stats_merge_from(mi_stats_get_default());
    _mi_stats_print(&mi._mi_stats_main, out, arg);
}

pub fn mi_stats_print(out: ?*anyopaque) void {
    // for compatibility there is an `out` parameter (which can be `stdout` or `stderr`)
    mi_stats_print_out(@ptrCast(mi_output_fun, out), null);
}

fn mi_thread_stats_print_out(out: mi_output_fun, arg: ?*anyopaque) void {
    _mi_stats_print(mi_stats_get_default(), out, arg);
}

// ----------------------------------------------------------------
// Basic timer for convenience; use milli-seconds to avoid doubles
// ----------------------------------------------------------------
pub fn _mi_clock_now() mi_msecs_t {
    return std.time.milliTimestamp();
}

var mi_clock_diff: mi_msecs_t = 0;

pub fn _mi_clock_start() mi_msecs_t {
    if (mi_clock_diff == 0) {
        const t0 = _mi_clock_now();
        mi_clock_diff = _mi_clock_now() - t0;
    }
    return _mi_clock_now();
}

pub fn _mi_clock_end(start: mi_msecs_t) mi_msecs_t {
    const end = _mi_clock_now();
    return end - start - mi_clock_diff;
}
