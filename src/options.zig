//-- ----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//-------------------------------------------------------------------------------

var mi_max_error_count: isize = 16; // stop outputting errors after this (use < 0 for no limit)
var mi_max_warning_count: isize = 16; // stop outputting warnings after this (use < 0 for no limit)

const std = @import("std");
const builtin = @import("builtin");
const AtomicOrder = builtin.AtomicOrder;
const AtomicRmwOp = builtin.AtomicRmwOp;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig"); // preloading
    usingnamespace @import("heap.zig");
    usingnamespace @import("stats.zig");
    fn noop(cond: bool) void {
        _ = cond;
    }
};

const MI_DEBUG = mi.MI_DEBUG;
const MI_SHOW_ERRORS = false;

const mi_option_t = mi.mi_option_t;

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

const mi_atomic_load_relaxed = mi.mi_atomic_load_relaxed;
const mi_atomic_load_ptr_acquire = mi.mi_atomic_load_ptr_acquire;
const mi_atomic_add_acq_rel = mi.mi_atomic_add_acq_rel;
const mi_atomic_store_ptr_release = mi.mi_atomic_store_ptr_release;

const _mi_preloading = mi._mi_preloading;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

const MI_MALLOC_VERSION = 209; // major + 2 digits minor

pub fn mi_version() i32 {
    return MI_MALLOC_VERSION;
}

// --------------------------------------------------------
// Options
// These can be accessed by multiple threads and may be
// concurrently initialized, but an initializing data race
// is ok since they resolve to the same value.
// --------------------------------------------------------
const mi_init_t = enum {
    UNINIT, // not yet initialized
    DEFAULTED, // not found in the environment, use default value
    INITIALIZED, // found in environment or set explicitly
};

const mi_option_desc_t = struct {
    value: isize, // the value
    init: mi_init_t = .UNINIT, // is it initialized yet? (from the environment)
    option: mi_option_t, // for debugging: the option index should match the option
    name: []const u8, // option name without `mimalloc_` prefix
};

// #define MI_OPTION(opt)                  mi_option_##opt, #opt, NULL
// #define MI_OPTION_LEGACY(opt,legacy)    mi_option_##opt, #opt, #legacy

var options = [_]mi_option_desc_t{
    // stable options
    .{ .value = if (MI_DEBUG > 0 or MI_SHOW_ERRORS) 1 else 0, .option = .mi_option_show_errors, .name = "show_errors" },
    .{ .value = 0, .option = .mi_option_show_stats, .name = "show_stats" },
    .{ .value = 0, .option = .mi_option_verbose, .name = "verbose" },

    // Some of the following options are experimental and not all combinations are valid. Use with care.
    .{ .value = 1, .option = .mi_option_eager_commit, .name = "eager_commit" }, // commit per segment directly (8MiB)  (but see also `eager_commit_delay`)
    .{ .value = 0, .option = .mi_option_large_os_pages, .name = "large_os_pages" }, // use large OS pages, use only with eager commit to prevent fragmentation of VMA's
    .{ .value = 0, .option = .mi_option_reserve_huge_os_pages, .name = "reserve_huge_os_pages" }, // per 1GiB huge pages
    .{ .value = -1, .option = .mi_option_reserve_huge_os_pages_at, .name = "reserve_huge_os_pages_at" }, // reserve huge pages at node N
    .{ .value = 0, .option = .mi_option_reserve_os_memory, .name = "reserve_os_memory" },
    .{ .value = 0, .option = .mi_option_page_reset, .name = "page_reset" }, // reset page memory on free
    .{ .value = if (builtin.os.tag == .netbsd) 0 else if (builtin.os.tag == .windows) 4 else 1, .option = .mi_option_eager_commit_delay, .name = "eager_commit_delay" }, // the first N segments per thread are not eagerly committed  (but per page in the segment on demand)
    .{ .value = 0, .option = .mi_option_use_numa_nodes, .name = "use_numa_nodes" }, // 0 = use available numa nodes, otherwise use at most N nodes.
    .{ .value = 0, .option = .mi_option_limit_os_alloc, .name = "limit_os_alloc" }, // 1 = do not use OS memory for allocation (but only reserved arenas)
    .{ .value = 100, .option = .mi_option_os_tag, .name = "os_tag" }, // only apple specific for now but might serve more or less related purpose
    .{ .value = 16, .option = .mi_option_max_errors, .name = "max_errors" }, // maximum errors that are output
    .{ .value = 16, .option = .mi_option_max_warnings, .name = "max_warnings" }, // maximum warnings that are output
    .{ .value = 8, .option = .mi_option_max_segment_reclaim, .name = "max_segment_reclaim" }, // max. number of segment reclaims from the abandoned segments per try.
    .{ .value = 1, .option = .mi_option_allow_decommit, .name = "allow_decommit" }, // decommit slices when no longer used (after decommit_delay milli-seconds)
    .{ .value = 500, .option = .mi_option_segment_decommit_delay, .name = "segment_decommit_delay" }, // decommit delay in milli-seconds for freed segments
    .{ .value = 2, .option = .mi_option_decommit_extend_delay, .name = "decommit_extend_delay" },
};

pub fn _mi_options_init() void {
    // called on process load; should not be called before the CRT is initialized!
    // (e.g. do not call this from process_init as that may run before CRT initialization)
    mi_add_stderr_output(); // now it safe to use stderr for output
    for (options) |option, i| {
        const l = mi_option_get(@intToEnum(mi_option_t, i));
        _ = l; // initialize
        if (option.option != .mi_option_verbose) {
            const desc = &options[option];
            std.debug.info("option '{s}': {}\n", desc.name, desc.value);
        }
    }
    mi_max_error_count = mi_option_get(.mi_option_max_errors);
    mi_max_warning_count = mi_option_get(.mi_option_max_warnings);
}

pub fn mi_option_get(option: mi_option_t) isize {
    mi_assert(@enumToInt(option) >= 0 and @enumToInt(option) < @enumToInt(mi_option_t._mi_option_last));
    if (@enumToInt(option) >= 0 and @enumToInt(option) < @enumToInt(mi_option_t._mi_option_last)) return 0;
    const desc = &options[@enumToInt(option)];
    mi_assert(desc.option == option); // index should match the option
    if (mi_unlikely(desc.init == .UNINIT)) {
        mi_option_init(desc);
    }
    return desc.value;
}

pub fn mi_option_get_clamp(option: mi_option_t, min: isize, max: isize) isize {
    const x = mi_option_get(option);
    return if (x < min) min else if (x > max) max else x;
}

pub fn mi_option_set(option: mi_option_t, value: isize) void {
    mi_assert(option >= 0 and option < ._mi_option_last);
    if (option < 0 or option >= ._mi_option_last) return;
    const desc = &options[option];
    mi_assert(desc.option == option); // index should match the option
    desc.value = value;
    desc.init = .INITIALIZED;
}

pub fn mi_option_set_default(option: mi_option_t, value: isize) void {
    mi_assert(option >= 0 and option < ._mi_option_last);
    if (option < 0 and option >= ._mi_option_last) return;
    const desc = &options[option];
    if (desc.init != .INITIALIZED) {
        desc.value = value;
    }
}

pub fn mi_option_is_enabled(option: mi_option_t) bool {
    return (mi_option_get(option) != 0);
}

pub fn mi_option_set_enabled(option: mi_option_t, enable: bool) void {
    mi_option_set(option, if (enable) 1 else 0);
}

pub fn mi_option_set_enabled_default(option: mi_option_t, enable: bool) void {
    mi_option_set_default(option, if (enable) 1 else 0);
}

pub fn mi_option_enable(option: mi_option_t) void {
    mi_option_set_enabled(option, true);
}

pub fn mi_option_disable(option: mi_option_t) void {
    mi_option_set_enabled(option, false);
}

fn mi_out_stderr(msg: []const u8, arg: *void) void {
    _ = arg;
    if (msg == null) return;
    std.debug.print("{}\n", msg);
}

// Since an output function can be registered earliest in the `main`
// function we also buffer output that happens earlier. When
// an output function is registered it is called immediately with
// the output up to that point.
const MI_MAX_DELAY_OUTPUT = (32 * 1024);
var out_buf = [MI_MAX_DELAY_OUTPUT + 1]u8;
var out_len = Atomic(usize);

fn mi_out_buf(msg: []const u8, arg: *void) void {
    _ = arg;
    if (msg == null) return;
    if (mi_atomic_load_relaxed(&out_len) >= MI_MAX_DELAY_OUTPUT) return;
    const n = msg.len;
    if (n == 0) return;
    // claim space
    const start = mi_atomic_add_acq_rel(&out_len, n);
    if (start >= MI_MAX_DELAY_OUTPUT) return;
    // check bound
    if (start + n >= MI_MAX_DELAY_OUTPUT) {
        n = MI_MAX_DELAY_OUTPUT - start - 1;
    }
    @memcpy(&out_buf[start], msg, n);
}

const mi_output_fun = *const fn (msg: []const u8, arg: *void) void;

fn mi_out_buf_flush(out: mi_output_fun, no_more_buf: bool, arg: *void) void {
    if (out == null) return;
    // claim (if `no_more_buf == true`, no more output will be added after this point)
    var count = mi_atomic_add_acq_rel(&out_len, if (no_more_buf) MI_MAX_DELAY_OUTPUT else 1);
    // and output the current contents
    if (count > MI_MAX_DELAY_OUTPUT) count = MI_MAX_DELAY_OUTPUT;
    out_buf[count] = 0;
    out(out_buf, arg);
    if (!no_more_buf) {
        out_buf[count] = '\n'; // if continue with the buffer, insert a newline
    }
}

// Once this module is loaded, switch to this routine
// which outputs to stderr and the delayed output buffer.
fn mi_out_buf_stderr(msg: []const u8, arg: *void) void {
    mi_out_stderr(msg, arg);
    mi_out_buf(msg, arg);
}

// --------------------------------------------------------
// Default output handler
// --------------------------------------------------------

// Should be atomic but gives errors on many platforms as generally we cannot cast a function pointer to a uintptr_t.
// For now, don't register output from multiple threads.
var mi_out_default: mi_output_fun = null;
var mi_out_arg = Atomic(*void).init(null);

fn mi_out_get_default(parg: ?**void) mi_output_fun {
    if (parg != null) {
        parg.* = mi_atomic_load_ptr_acquire(void, &mi_out_arg);
    }
    const out = mi_out_default;
    return if (out == null) &mi_out_buf else out;
}

pub fn mi_register_output(out: mi_output_fun, arg: *void) void {
    mi_out_default = if (out == null) mi_out_stderr else out; // stop using the delayed output buffer
    mi_atomic_store_ptr_release(void, &mi_out_arg, arg);
    if (out != null) mi_out_buf_flush(out, true, arg); // output all the delayed output now
}

// add stderr to the delayed output after the module is loaded
fn mi_add_stderr_output() void {
    mi_assert_internal(mi_out_default == null);
    mi_out_buf_flush(&mi_out_stderr, false, null); // flush current contents to stderr
    mi_out_default = &mi_out_buf_stderr; // and add stderr to the delayed output
}

// --------------------------------------------------------
// Messages, all end up calling `_mi_fputs`.
// --------------------------------------------------------
var error_count = Atomic(usize).init(0); // when >= max_error_count stop emitting errors
var warning_count = Atomic(usize).init(0); // when >= max_warning_count stop emitting warnings

// --------------------------------------------------------
// Initialize options by checking the environment
// --------------------------------------------------------

fn mi_option_init(desc: *mi_option_desc_t) void {
    // Read option value from the environment
    var buf: [64]u8 = undefined;
    const b = std.fmt.bufPrint(&buf, "mimalloc_{s}", .{desc.name}) catch unreachable;
    var envBuf: [128]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&envBuf);
    std.debug.print("{}\n", .{@TypeOf(b)});
    const env = std.process.getEnvVarOwned(fba.allocator(), b) catch null;

    if (env != null) {
        if (!_mi_preloading()) {
            desc.init = .DEFAULTED;
        }
        return;
    }

    for (env.?) |c, i| {
        if (i >= buf.len) break;
        buf[i] = std.ascii.toUpper(c);
    }
    const val = buf[0..env.?.len];

    if (val.len == 0 or std.mem.indexOf(u8, "1;TRUE;YES;ON", val) != null) {
        desc.value = 1;
        desc.init = .INITIALIZED;
    } else if (std.mem.indexOf(u8, "0;FALSE;NO;OFF", val) != null) {
        desc.value = 0;
        desc.init = .INITIALIZED;
    } else {
        const value = std.fmt.parseInt(isize, val, 0) catch -999999;
        if (desc.option == .mi_option_reserve_os_memory) {
            // this option is interpreted in KiB to prevent overflow of `long`
            // TODO
            // if (*end == 'K') { end++; }
            // else if (*end == 'M') { value *= MI_KiB; end++; }
            // else if (*end == 'G') { value *= MI_MiB; end++; }
            // else { value = (value + MI_KiB - 1) / MI_KiB; }
            // if (end[0] == 'I' && end[1] == 'B') { end += 2; }
            // else if (*end == 'B') { end++; }
        }
        if (value != -999999) {
            desc.value = value;
            desc.init = .INITIALIZED;
        } else {
            desc.init = .DEFAULTED;
            std.log.warn("environment option mimalloc_{s} has an invalid value.\n", .{desc.name});
        }
    }
    mi_assert_internal(desc.init != .UNINIT);
}
