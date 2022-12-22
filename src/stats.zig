//-----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-----------------------------------------------------------------------------

const std = @import("std");
const builtin = std.builtin;
const AtomicOrder = builtin.AtomicOrder;
const AtomicRmwOp = builtin.AtomicRmwOp;
const assert = std.debug.assert;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("heap.zig");
};

//-----------------------------------------------------------
//  Statistics operations
//-----------------------------------------------------------

pub fn mi_is_in_main(stat: *const mi.stat_count_t) bool {
    return @ptrToInt(stat) >= @ptrToInt(&mi._stats_main) and @ptrToInt(stat) < (@ptrToInt(&mi._stats_main) + @sizeOf(mi.stats_t));
}

inline fn mi_atomic_addi64_relaxed(p: *volatile i64, x: i64) void {
    @atomicRmw(i64, p, builtin.Add, x, builtin.Monotonic);
}

inline fn mi_atomic_maxi64_relaxed(p: *volatile i64, x: i64) void {
    var current = @atomicLoad(i64, p, AtomicOrder.Monotonic);
    while (current < x) {
        current = @cmpxchgWeak(i64, p, current, x, AtomicOrder.Monotonic, AtomicOrder.Monotonic) orelse current;
    }
}

fn mi_stat_update(stat: *mi.stat_count_t, amount: i64) void {
    if (amount == 0) return;
    if (mi_is_in_main(stat)) {
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

fn _mi_stat_counter_increase(stat: *mi.stat_counter_t, amount: usize) void {
    if (mi_is_in_main(stat)) {
        @atomicRmw(i64, &stat.count, builtin.Add, 1, builtin.Monotonic);
        @atomicRmw(i64, &stat.total, builtin.Add, @intCast(i64, amount), builtin.Monotonic);
    } else {
        stat.count += 1;
        stat.total += amount;
    }
}

pub fn mi_stat_increase(stat: *mi.stat_count_t, amount: usize) void {
    mi_stat_update(stat, @intCast(i64, amount));
}

pub fn mi_stat_decrease(stat: *mi.stat_count_t, amount: usize) void {
    mi_stat_update(stat, -@intCast(i64, amount));
}

// must be thread safe as it is called from stats_merge
fn mi_stat_add(stat: *mi.stat_count_t, src: *const mi.stat_count_t, unit: i64) void {
    if (stat == src) return;
    if (src.allocated == 0 and src.freed == 0) return;
    mi_atomic_addi64_relaxed(&stat.allocated, src.allocated * unit);
    mi_atomic_addi64_relaxed(&stat.current, src.current * unit);
    mi_atomic_addi64_relaxed(&stat.freed, src.freed * unit);
    // peak scores do not work across threads..
    mi_atomic_addi64_relaxed(&stat.peak, src.peak * unit);
}

fn mi_stat_counter_add(stat: *mi.stat_counter_t, src: *const mi.stat_counter_t, unit: i64) void {
    if (stat == src) return;
    mi_atomic_addi64_relaxed(&stat.total, src.total * unit);
    mi_atomic_addi64_relaxed(&stat.count, src.count * unit);
}

// must be thread safe as it is called from stats_merge
fn mi_stats_add(stats: *mi.stats_t, src: *const mi.stats_t) void {
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
    if (mi.STAT > 1) {
        var i: usize = 0;
        while (i <= mi.BIN_HUGE) : (i += 1) {
            if (src.normal_bins[i].allocated > 0 or src.normal_bins[i].freed > 0) {
                mi_stat_add(&stats.normal_bins[i], &src.normal_bins[i], 1);
            }
        }
    }
}

//-----------------------------------------------------------
//  Display statistics
//-----------------------------------------------------------

// unit > 0 : size in binary bytes
// unit == 0: count as decimal
// unit < 0 : count in binary
fn mi_printf_amount(n: i64, unit: i64, out: *mi.output_fun, arg: *opaque {}, fmt: ?[]const u8) void {
    var buf: [32]u8 = undefined;
    buf[0] = 0;
    const suffix = if (unit <= 0) " " else "B";
    const base = if (unit == 0) 1000 else 1024;
    if (unit > 0) n *= unit;

    const pos = if (n < 0) -n else n;
    if (pos < base) {
        if (n != 1 or suffix[0] != 'B') { // skip printing 1 B for the unit column
            std.fmt.bufPrint(buf, "{} {:-3s}", .{ n, if (n == 0) ?"" else suffix });
        }
    } else {
        var divider = base;
        const magnitude = "K";
        if (pos >= divider * base) {
            divider *= base;
            magnitude = "M";
        }
        if (pos >= divider * base) {
            divider *= base;
            magnitude = "G";
        }
        const tens = (n / (divider / 10));
        const whole = (tens / 10);
        const frac1 = (tens % 10);
        var unitdesc: [8]u8 = undefined;
        mi.snprintf(unitdesc, 8, "%s%s%s", magnitude, if (base == 1024) ?"i" else "", suffix);
        mi.snprintf(buf, buf.len, "%ld.%ld %-3s", whole, if (frac1 < 0) -frac1 else frac1, unitdesc);
    }
    mi._mi_fprintf(out, arg, if (fmt == null) "%11s" else fmt.?, buf);
}

fn mi_print_amount(n: i64, unit: i64, out: *mi.output_fun, arg: ?*opaque {}) void {
    mi_printf_amount(n, unit, out, arg, null);
}

fn mi_print_count(n: i64, unit: i64, out: *mi.output_fun, arg: ?*opaque {}) void {
    if (unit == 1) {
        mi._mi_fprintf(out, arg, "%11s", " ");
    } else {
        mi_print_amount(n, 0, out, arg);
    }
}

fn mi_stat_print(stat: *const mi.stat_count_t, msg: []const u8, unit: i64, out: *mi.output_fun, arg: *opaque {}) void {
    mi._mi_fprintf(out, arg, "%10s:", msg);
    if (unit > 0) {
        mi_print_amount(stat.peak, unit, out, arg);
        mi_print_amount(stat.allocated, unit, out, arg);
        mi_print_amount(stat.freed, unit, out, arg);
        mi_print_amount(stat.current, unit, out, arg);
        mi_print_amount(unit, 1, out, arg);
        mi_print_count(stat.allocated, unit, out, arg);
        if (stat.allocated > stat.freed) {
            mi._mi_fprintf(out, arg, "  not all freed!\n");
        } else {
            mi._mi_fprintf(out, arg, "  ok\n");
        }
    } else if (unit < 0) {
        mi_print_amount(stat.peak, -1, out, arg);
        mi_print_amount(stat.allocated, -1, out, arg);
        mi_print_amount(stat.freed, -1, out, arg);
        mi_print_amount(stat.current, -1, out, arg);
        if (unit == -1) {
            mi._mi_fprintf(out, arg, "%22s", "");
        } else {
            mi_print_amount(-unit, 1, out, arg);
            mi_print_count((stat.allocated / -unit), 0, out, arg);
        }
        if (stat.allocated > stat.freed) {
            mi._mi_fprintf(out, arg, "  not all freed!\n");
        } else {
            mi._mi_fprintf(out, arg, "  ok\n");
        }
    } else {
        mi_print_amount(stat.peak, 1, out, arg);
        mi_print_amount(stat.allocated, 1, out, arg);
        mi._mi_fprintf(out, arg, "%11s", " "); // no freed
        mi_print_amount(stat.current, 1, out, arg);
        mi._mi_fprintf(out, arg, "\n");
    }
}

fn mi_stat_counter_print(stat: *const mi.stat_counter_t, msg: []const u8, out: *mi.out_fn, arg: *opaque {}) void {
    mi._mi_fprintf(out, arg, "%10s:", msg);
    mi_print_amount(stat.total, -1, out, arg);
    mi._mi_fprintf(out, arg, "\n");
}

fn mi_stat_counter_print_avg(stat: *const mi.stat_counter_t, msg: []const u8, out: *mi.out_fn, arg: *opaque {}) void {
    const avg_tens = if (stat.count == 0) 0 else (stat.total * 10 / stat.count);
    const avg_whole = (avg_tens / 10);
    const avg_frac1 = (avg_tens % 10);
    mi._mi_fprintf(out, arg, "%10s: %5ld.%ld avg\n", msg, avg_whole, avg_frac1);
}

fn mi_print_header(out: *mi.output_fun, arg: *opaque {}) void {
    mi._mi_fprintf(out, arg, "%10s: %10s %10s %10s %10s %10s %10s\n", "heap stats", "peak   ", "total   ", "freed   ", "current   ", "unit   ", "count   ");
}

fn mi_stats_print_bins(bins: *const mi.stat_count_t, max: usize, fmt: []const u8, out: *mi.output_fun, arg: *opaque {}) void {
    if (mi.STAT <= 1) return;

    var found = false;
    var buf: [64]u8 = undefined;
    var i: usize = 0;
    while (i <= max) : (i += 1) {
        if (bins[i].allocated > 0) {
            found = true;
            const unit = mi.bin_size(i);
            mi.snprintf(buf, 64, "%s %3lu", fmt, i);
            mi_stat_print(&bins[i], buf, unit, out, arg);
        }
    }
    if (found) {
        mi._mi_fprintf(out, arg, "\n");
        mi_print_header(out, arg);
    }
}

//------------------------------------------------------------
// Use an output wrapper for line-buffered output
// (which is nice when using loggers etc.)
//------------------------------------------------------------
const buffered_t = struct {
    out: mi.output_fun, // original output function
    arg: *opaque {}, // and state
    buf: []u8, // local buffer of at least size `count+1`
    used: usize, // currently used chars `used <= count`
    count: usize, // total chars available for output
};

fn mi_buffered_flush(buf: *buffered_t) void {
    buf.buf[buf.used] = 0;
    mi._mi_fputs(buf.out, buf.arg, null, buf.buf);
    buf.used = 0;
}

fn mi_buffered_out(msg: []const u8, arg: *opaque {}) void {
    var buf = @ptrCast(*buffered_t, arg);
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

fn mi_stat_process_info(elapsed: *mi.mi_msecs_t, utime: *mi.mi_msecs_t, stime: *mi.mi_msecs_t, current_rss: *usize, peak_rss: *usize, current_commit: *usize, peak_commit: *usize, page_faults: *usize) void {
    _ = elapsed;
    _ = utime;
    _ = stime;
    _ = current_rss;
    _ = peak_rss;
    _ = current_commit;
    _ = peak_commit;
    _ = page_faults;
}

fn _mi_stats_print(stats: *mi.stats_t, out0: mi.output_fun, arg0: *opaque {}) void {
    // wrap the output function to be line buffered
    var buf: [256]u8 = undefined;
    var buffer = buffered_t{ .out = out0, .arg = arg0, .buf = buf, .used = 0, .count = 255 };
    const out = &mi_buffered_out;
    const arg = &buffer;

    // and print using that
    mi_print_header(out, arg);
    if (mi.STAT > 1)
        mi_stats_print_bins(stats.normal_bins, mi.BIN_HUGE, "normal", out, arg);
    if (mi.STAT > 0) {
        mi_stat_print(&stats.normal, "normal", if (stats.normal_count.count == 0) 1 else -(stats.normal.allocated / stats.normal_count.count), out, arg);
        mi_stat_print(&stats.large, "large", if (stats.large_count.count == 0) 1 else -(stats.large.allocated / stats.large_count.count), out, arg);
        mi_stat_print(&stats.huge, "huge", if (stats.huge_count.count == 0) 1 else -(stats.huge.allocated / stats.huge_count.count), out, arg);
        var total: mi.stats_count_t = .{};
        mi_stat_add(&total, &stats.normal, 1);
        mi_stat_add(&total, &stats.large, 1);
        mi_stat_add(&total, &stats.huge, 1);
        mi_stat_print(&total, "total", 1, out, arg);
    }
    if (mi.STAT > 1) {
        mi_stat_print(&stats.malloc, "malloc req", 1, out, arg);
        mi._mi_fprintf(out, arg, "\n");
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
    mi._mi_fprintf(out, arg, "%10s: %7zu\n", "numa nodes", mi._mi_os_numa_node_count());

    var elapsed: mi.msecs_t = undefined;
    var user_time: mi.msecs_t = undefined;
    var sys_time: mi.msecs_t = undefined;
    var current_rss: usize = undefined;
    var peak_rss: usize = undefined;
    var current_commit: usize = undefined;
    var peak_commit: usize = undefined;
    var page_faults: usize = undefined;
    mi_stat_process_info(&elapsed, &user_time, &sys_time, &current_rss, &peak_rss, &current_commit, &peak_commit, &page_faults);
    mi._mi_fprintf(out, arg, "%10s: %7ld.%03ld s\n", "elapsed", elapsed / 1000, elapsed % 1000);
    mi._mi_fprintf(out, arg, "%10s: user: %ld.%03ld s, system: %ld.%03ld s, faults: %lu, rss: ", "process", user_time / 1000, user_time % 1000, sys_time / 1000, sys_time % 1000, page_faults);
    mi_printf_amount(peak_rss, 1, out, arg, "%s");
    if (peak_commit > 0) {
        mi._mi_fprintf(out, arg, ", commit: ");
        mi_printf_amount(peak_commit, 1, out, arg, "%s");
    }
    mi._mi_fprintf(out, arg, "\n");
}

var mi_process_start: mi.msecs_t = 0;

fn mi_stats_get_default() *mi.stats_t {
    const heap = mi.mi_heap_get_default();
    return &heap.tld.?.stats;
}

fn mi_stats_merge_from(stats: *mi.stats_t) void {
    if (stats != &mi._mi_stats_main) {
        mi_stats_add(&mi._mi_stats_main, stats);
        stats = .{};
        // memset(stats, 0, sizeof(mi_stats_t));
    }
}

pub fn mi_stats_reset() void {
    var stats = mi_stats_get_default();
    if (stats != &mi._stats_main) {
        stats.* = .{};
    }
    mi._stats_main = .{};
    if (mi_process_start == 0) {
        mi_process_start = _mi_clock_start();
    }
}

fn mi_stats_merge() void {
    mi_stats_merge_from(mi_stats_get_default());
}

fn _mi_stats_done(stats: *mi.stats_t) void { // called from `mi_thread_done`
    mi_stats_merge_from(stats);
}

fn mi_stats_print_out(out: mi.output_fun, arg: ?*opaque {}) void {
    mi_stats_merge_from(mi_stats_get_default());
    _mi_stats_print(&mi._mi_stats_main, out, arg);
}

fn mi_stats_print(out: *opaque {}) void {
    // for compatibility there is an `out` parameter (which can be `stdout` or `stderr`)
    mi_stats_print_out(@ptrCast(mi.output_fun, out), null);
}

fn mi_thread_stats_print_out(out: mi.output_fn, arg: *opaque {}) void {
    _mi_stats_print(mi_stats_get_default(), out, arg);
}

// ----------------------------------------------------------------
// Basic timer for convenience; use milli-seconds to avoid doubles
// ----------------------------------------------------------------
fn mi_clock_now() mi.msecs_t {
    return std.time.milliTimestamp();
}

var mi_clock_diff: mi.msecs_t = 0;

pub fn _mi_clock_start() mi.msecs_t {
    if (mi_clock_diff == 0) {
        const t0 = mi_clock_now();
        mi_clock_diff = mi_clock_now() - t0;
    }
    return mi_clock_now();
}

pub fn _mi_clock_end(start: mi.msecs_t) mi.msecs_t {
    const end = mi_clock_now();
    return end - start - mi_clock_diff;
}
