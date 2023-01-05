//----------------------------------------------------------------------------
// Copyright (c) 2018-2021, Microsoft Research, Daan Leijen
// This is free software; you can redistribute it and/or modify it under the
// terms of the MIT license. A copy of the license can be found in the file
// "LICENSE" at the root of this distribution.
//----------------------------------------------------------------------------

const std = @import("std");
const builtin = @import("builtin");
const os = std.os;
const windows = std.os.windows;
const kernel32 = windows.kernel32;
const mem = std.mem;
const math = std.math;
const assert = std.debug.assert;
const Atomic = std.atomic.Atomic;
const AtomicOrder = std.builtin.AtomicOrder;

const mi = struct {
    usingnamespace @import("types.zig");
    usingnamespace @import("init.zig");
    usingnamespace @import("stats.zig");
    usingnamespace @import("options.zig");
    fn noop(cond: bool) void {
        _ = cond;
    }

    const MAP_ALIGNED_SUPER = 0;
    const MAP_HUGETLB = 0;
    const MAP_HUGE_1GB = 0;
    const MAP_HUGE_2GB = 0;
    const VM_FLAGS_SUPERPAGE_SIZE_2MB = 0;

    fn VM_MAKE_TAG(v: i32) i32 {
        return v;
    }
};

pub const PageAlignedMem = [*]align(mem.page_size) u8;

const mi_assert = assert;
const mi_assert_internal = if (MI_DEBUG > 1) mi_assert else mi.noop;
const mi_assert_expensive = if (MI_DEBUG > 2) mi_assert else mi.noop;

inline fn mi_likely(cond: bool) bool {
    return cond;
}

inline fn mi_unlikely(cond: bool) bool {
    return cond;
}

const MI_USE_SBRK = (builtin.os.tag == .wasi);

// #defines

const MI_DEBUG = mi.MI_DEBUG;
const MI_SECURE = mi.MI_SECURE;
const MI_INTPTR_SIZE = mi.MI_INTPTR_SIZE;

const MI_KiB = mi.MI_KiB;
const MI_MiB = mi.MI_MiB;
const MI_GiB = mi.MI_MiB;

const MI_SEGMENT_SIZE = mi.MI_SEGMENT_SIZE;

//-----------------------------------------------------------
//  Initialization.
//  On windows initializes support for aligned allocation and
//  large OS pages (if MIMALLOC_LARGE_OS_PAGES is true).
//-----------------------------------------------------------

pub fn mi_align_up_ptr(p: anytype, alignment: usize) @TypeOf(p) {
    return @intToPtr(@TypeOf(p), mi._mi_align_up(@ptrToInt(p), alignment));
}

pub fn mi_align_down_ptr(p: anytype, alignment: usize) @TypeOf(p) {
    return @intToPtr(@TypeOf(p), mi._mi_align_down(@ptrToInt(p), alignment));
}

// page size (initialized properly in `os_init`)
var os_page_size: usize = mem.page_size;

// minimal allocation granularity
var os_alloc_granularity = mem.page_size;

// if non-zero, use large page allocation
var large_os_page_size: usize = 0;

// is memory overcommit allowed?
// set dynamically in _mi_os_init (and if true we use MAP_NORESERVE)
var os_overcommit = true;

pub fn _mi_os_has_overcommit() bool {
    return os_overcommit;
}

// OS (small) page size
pub fn _mi_os_page_size() usize {
    return os_page_size;
}

// if large OS pages are supported (2 or 4MiB), then return the size, otherwise return the small page size (4KiB)
pub fn _mi_os_large_page_size() usize {
    return if (large_os_page_size != 0) large_os_page_size else _mi_os_page_size();
}

// #if !defined(MI_USE_SBRK) && !defined(__wasi__)
fn use_large_os_page(size: usize, alignment: usize) bool {
    // if we have access, check the size and alignment requirements
    if (large_os_page_size == 0 or !mi.mi_option_is_enabled(.mi_option_large_os_pages)) return false;
    return ((size % large_os_page_size) == 0 and (alignment % large_os_page_size) == 0);
}
// #endif

// round to a good OS allocation size (bounded by max 12.5% waste)
pub fn _mi_os_good_alloc_size(size: usize) usize {
    var align_size: usize = if (size < 512 * MI_KiB) std.mem.page_size //
    else if (size < 2 * MI_MiB) 64 * MI_KiB //
    else if (size < 8 * MI_MiB) 256 * MI_KiB //
    else if (size < 32 * MI_MiB) 1 * MI_MiB //
    else 4 * MI_MiB;
    if (mi_unlikely(size >= (std.math.maxInt(isize) - align_size))) return size; // possible overflow?
    return mi._mi_align_up(size, align_size);
}

fn os_detect_overcommit() void {
    switch (builtin.os.tag) {
        .linux => {
            const fh = std.fs.openFileAbsoluteZ("/proc/sys/vm/overcommit_memory", .{}) catch return;
            defer fh.close();
            var buf: [32]u8 = undefined;
            const nread = fh.readAll(&buf) catch return;
            // <https://www.kernel.org/doc/Documentation/vm/overcommit-accounting>
            // 0: heuristic overcommit, 1: always overcommit, 2: never overcommit (ignore NORESERVE)
            if (nread >= 1) {
                os_overcommit = (buf[0] == '0' or buf[0] == '1');
            }
        },
        .freebsd => {
            var val: i32 = 0;
            var olen: usize = @sizeOf(val);
            if (os.sysctlbynameZ("vm.overcommit", &val, &olen, null, 0) == 0) {
                os_overcommit = (val != 0);
            }
        },
        else => {
            // overcommit is true
        },
    }
}

pub fn _mi_os_init() void {
    switch (builtin.os.tag) {
        .wasi => {
            os_overcommit = false;
            os_page_size = 64 * MI_KiB; // WebAssembly has a fixed page size: 64KiB
            os_alloc_granularity = 16;
        },
        .windows => {
            // TODO
        },
        else => { // generic unix
            // TODO: runtime detect page size
            large_os_page_size = 2 * MI_MiB; // TODO: can we query the OS for this?
            os_detect_overcommit();
        },
    }
}

test "os init" {
    _mi_os_init();
    _ = mi_os_get_aligned_hint(1 << 20, 2 << 20);
}

fn mi_madvise(addr: *anyopaque, length: usize, advice: u32) usize {
    return os.system.madvise(@ptrCast(PageAlignedMem, @alignCast(mem.page_size, addr)), length, advice);
}

//--------------------------------------------------------------
// aligned hinting
//--------------------------------------------------------------

// On 64-bit systems, we can do efficient aligned allocation by using
// the 2TiB to 30TiB area to allocate those.
var aligned_base = if (mi.MI_INTPTR_SIZE >= 8) Atomic(usize).init(0) else u0;

// Return a MI_SEGMENT_SIZE aligned address that is probably available.
// If this returns NULL, the OS will determine the address but on some OS's that may not be
// properly aligned which can be more costly as it needs to be adjusted afterwards.
// For a size > 1GiB this always returns NULL in order to guarantee good ASLR randomization;
// (otherwise an initial large allocation of say 2TiB has a 50% chance to include (known) addresses
//  in the middle of the 2TiB - 6TiB address range (see issue #372))

const MI_HINT_BASE = (2 << 40); // 2TiB start
const MI_HINT_AREA = (4 << 40); // upto 6TiB   (since before win8 there is "only" 8TiB available to processes)
const MI_HINT_MAX = (30 << 40); // wrap after 30TiB (area after 32TiB is used for huge OS pages)

pub fn mi_os_get_aligned_hint(try_alignment: usize, size_in: usize) ?PageAlignedMem {
    if (MI_INTPTR_SIZE < 8 or try_alignment > MI_SEGMENT_SIZE) return null;
    var size = mi._mi_align_up(size_in, MI_SEGMENT_SIZE);
    if (size > 1 * MI_GiB) return null; // guarantee the chance of fixed valid address is at most 1/(MI_HINT_AREA / 1<<30) = 1/4096.
    if (MI_SECURE > 0)
        size += MI_SEGMENT_SIZE; // put in `MI_SEGMENT_SIZE` virtual gaps between hinted blocks; this splits VLA's but increases guarded areas.

    var hint = mi.mi_atomic_add_acq_rel(&aligned_base, size);
    if (hint == 0 or hint > MI_HINT_MAX) { // wrap or initialize
        var init: usize = MI_HINT_BASE;
        if (MI_SECURE > 0 or MI_DEBUG == 0) { // security: randomize start of aligned allocations unless in debug mode
            const r = mi._mi_heap_random_next(mi.mi_get_default_heap());
            init = init + ((MI_SEGMENT_SIZE * ((r >> 17) & 0xFFFFF)) % MI_HINT_AREA); // (randomly 20 bits)*4MiB == 0 to 4TiB
        }
        var expected = hint + size;
        _ = mi.mi_atomic_cas_strong_acq_rel(&aligned_base, &expected, init);
        hint = mi.mi_atomic_add_acq_rel(&aligned_base, size); // this may still give 0 or > MI_HINT_MAX but that is ok, it is a hint after all
    }
    if (hint % try_alignment != 0) return null;
    return @intToPtr(PageAlignedMem, hint);
}

//--------------------------------------------------------------
// Free memory
//--------------------------------------------------------------

inline fn mi_ptr_less(lhs: *anyopaque, rhs: *anyopaque) bool {
    return @ptrToInt(lhs) < @ptrToInt(rhs);
}

inline fn mi_ptr_diff(lhs: *anyopaque, rhs: *anyopaque) usize {
    return @ptrToInt(lhs) - @ptrToInt(rhs);
}

pub fn mi_os_mem_free(addr: PageAlignedMem, size: usize, was_committed: bool, stats: *mi.mi_stats_t) bool {
    if (size == 0) return true; // || _mi_os_is_huge_reserved(addr)
    var err = false;
    switch (builtin.os.tag) {
        .windows => {
            var errcode: windows.Win32Error = undefined;
            err = (kernel32.VirtualFree(addr, 0, windows.MEM_RELEASE) == 0);
            if (err) {
                errcode = kernel32.GetLastError();
            }
            if (errcode == .INVALID_ADDRESS) {
                // In mi_os_mem_alloc_aligned the fallback path may have returned a pointer inside
                // the memory region returned by VirtualAlloc; in that case we need to free using
                // the start of the region.
                var info: windows.MEMORY_BASIC_INFORMATION = undefined;
                _ = windows.VirtualQuery(addr, &info, @sizeOf(windows.MEMORY_BASIC_INFORMATION)) catch unreachable;
                if (mi_ptr_less(info.AllocationBase, addr) and mi_ptr_diff(addr, info.AllocationBase) < MI_SEGMENT_SIZE) {
                    errcode = .SUCCESS;
                    err = (kernel32.VirtualFree(info.AllocationBase, 0, windows.MEM_RELEASE) == 0);
                    if (err) {
                        errcode = kernel32.GetLastError();
                    }
                }
            }
            if (errcode != .SUCCESS) {
                std.log.warn("unable to release OS memory: error code {}, addr: {*}, size: {}\n", .{ errcode, addr, size });
            }
        },
        .wasi => err = false, // wasi/sbrk heap cannot be shrunk
        else => if (!MI_USE_SBRK) { // wasi/sbrk heap cannot be shrunk
            // zig os.munmap doesn't return and asserts, so call the low level api
            const errno = os.errno(os.system.munmap(@ptrCast([*]const u8, addr), size));
            if (errno != .SUCCESS) {
                std.log.warn("unable to release OS memory: {}, addr: {*}, size: {}\n", .{ errno, addr, size });
            }
        },
    }
    if (was_committed) {
        mi._mi_stat_decrease(&stats.committed, size);
    }
    mi._mi_stat_decrease(&stats.reserved, size);
    return !err;
}

//--------------------------------------------------------------
//  Raw allocation on Windows (VirtualAlloc)
//--------------------------------------------------------------
fn mi_win_virtual_alloc(addr: ?PageAlignedMem, size: usize, try_alignment: usize, flags: windows.DWORD, large_only: bool, allow_large: bool, is_large: *bool) ?PageAlignedMem {
    _ = addr;
    _ = size;
    _ = try_alignment;
    _ = flags;
    _ = allow_large;
    _ = large_only;
    _ = is_large;
    unreachable;
}

fn mi_win_enable_large_os_pages() bool {
    if (large_os_page_size > 0) return true;
    // TODO
    return false;
}

//--------------------------------------------------------------
//  Raw allocation using `sbrk` or `wasm_memory_grow`
//--------------------------------------------------------------

fn mi_memory_grow(size: usize) ?[*]u8 {
    const p = os.sbrk(size);
    // if (p == (void*)(-1)) return null;
    // #if !defined(__wasi__) // on wasi this is always zero initialized already (?)
    // memset(p,0,size);
    // #endif
    return p;
}

fn mi_heap_grow(size: usize, try_alignment: usize) ?[*]u8 {
    _ = size;
    _ = try_alignment;
}

//--------------------------------------------------------------
//  Raw allocation on Unix's (mmap)
//--------------------------------------------------------------

const MI_OS_USE_MMAP = true;
fn mi_unix_mmapx(addr: ?PageAlignedMem, size: usize, try_alignment: usize, protect_flags: u32, flags: u32, fd: i32) !?PageAlignedMem {
    var align_flags: u32 = 0;
    var p: ?[]align(mem.page_size) u8 = null;
    if (addr == null and try_alignment > 1 and (try_alignment % _mi_os_page_size()) == 0) {
        const n = mi.mi_bsr(try_alignment);
        if (math.shl(usize, @intCast(usize, 1), n) == try_alignment and n >= 12 and n <= 30) { // alignment is a power of 2 and 4096 <= alignment <= 1GiB
            align_flags = switch (builtin.os.tag) {
                .freebsd, .netbsd, .openbsd => 0, // os.MAP_ALIGNED TODO: Zig doesn't define this yet
                .solaris => os.MAP_ALIGN, // os.MAP_ALIGNED TODO: Zig doesn't define this yet
                else => 0,
            };
        }
    }
    if (align_flags != 0) {
        p = try os.mmap(addr, size, protect_flags, flags | align_flags, fd, 0);
        if (p != null) return p.?.ptr;
        // fall back to regular mmap
    }
    if (MI_INTPTR_SIZE >= 8) {
        // on 64-bit systems, use the virtual address area after 2TiB for 4MiB aligned allocations
        if (addr == null) {
            const hint = mi_os_get_aligned_hint(try_alignment, size);
            p = try os.mmap(hint, size, protect_flags, flags, fd, 0);
            if (p != null) return p.?.ptr;
            // fall back to regular mmap
        }
    }
    // regular mmap
    p = try os.mmap(addr, size, protect_flags, flags, fd, 0);
    if (p == null) return null; // failed to allocate
    return p.?.ptr;
}

fn mi_unix_mmap_fd() i32 {
    if (!builtin.os.tag.isDarwin()) return -1;
    // macOS: tracking anonymous page with a specific ID. (All up to 98 are taken officially but LLVM sanitizers had taken 99)
    var os_tag = @intCast(i32, mi.mi_option_get(.mi_option_os_tag));
    if (os_tag < 100 or os_tag > 255) os_tag = 100;
    return mi.VM_MAKE_TAG(os_tag);
}

var large_page_try_ok = Atomic(usize).init(0);
var mi_huge_pages_available = true;

fn mi_unix_mmap(addr: ?PageAlignedMem, size: usize, try_alignment: usize, protect_flags_in: u32, large_only: bool, allow_large: bool, is_large: *bool) ?PageAlignedMem {
    var p: ?PageAlignedMem = null;
    const fd = mi_unix_mmap_fd();
    var flags: u32 = os.MAP.PRIVATE | os.MAP.ANONYMOUS;
    if (_mi_os_has_overcommit()) {
        flags |= os.MAP.NORESERVE;
    }
    var protect_flags = protect_flags_in | switch (builtin.os.tag) {
        .freebsd, .netbsd, .openbsd => mi.PROT.MAX(mi.PROT.READ | mi.PROT.WRITE), // BSD
        else => 0,
    };
    // huge page allocation
    if ((large_only or use_large_os_page(size, try_alignment)) and allow_large) {
        var try_ok = mi.mi_atomic_load_acquire(&large_page_try_ok);
        if (!large_only and try_ok > 0) {
            // If the OS is not configured for large OS pages, or the user does not have
            // enough permission, the `mmap` will always fail (but it might also fail for other reasons).
            // Therefore, once a large page allocation failed, we don't try again for `large_page_try_ok` times
            // to avoid too many failing calls to mmap.
            _ = mi.mi_atomic_cas_strong_acq_rel(&large_page_try_ok, &try_ok, try_ok - 1);
        } else {
            var lflags = flags & ~@intCast(u32, os.MAP.NORESERVE); // using NORESERVE on huge pages seems to fail on Linux
            var lfd = fd;
            if (mi.MAP_ALIGNED_SUPER > 0)
                lflags |= mi.MAP_ALIGNED_SUPER;
            if (mi.MAP_HUGETLB > 0)
                lflags |= mi.MAP_HUGETLB;
            if (mi.MAP_HUGE_1GB > 0 and (size % MI_GiB) == 0 and mi_huge_pages_available)
                lflags |= mi.MAP_HUGE_1GB
            else if (mi.MAP_HUGE_2GB > 0)
                lflags = mi.MAP_HUGE_2MB;
            if (mi.VM_FLAGS_SUPERPAGE_SIZE_2MB > 0)
                lfd |= mi.VM_FLAGS_SUPERPAGE_SIZE_2MB;
            if (large_only or lflags != flags) {
                // try large OS page allocation
                is_large.* = true;
                p = mi_unix_mmapx(addr, size, try_alignment, protect_flags, lflags, lfd) catch null;
                if (mi.MAP_HUGE_1GB > 0 and p == null and (lflags & mi.MAP_HUGE_1GB) != 0) {
                    mi_huge_pages_available = false; // don't try huge 1GiB pages again
                    std.log.warn("unable to allocate huge (1GiB) page, trying large (2MiB) pages instead (error {})\n", .{0}); // TODO: fix error reporting
                    lflags = ((lflags & ~mi.MAP_HUGE_1GB) | mi.MAP_HUGE_2MB);
                    p = mi_unix_mmapx(addr, size, try_alignment, protect_flags, lflags, lfd);
                }
                if (large_only) return p;
                if (p == null) {
                    mi.mi_atomic_store_release(&large_page_try_ok, @intCast(usize, 8)); // on error, don't try again for the next N allocations
                }
            }
        }
    }
    // regular allocation
    if (p == null) {
        is_large.* = false;
        p = mi_unix_mmapx(addr, size, try_alignment, protect_flags, flags, fd) catch null;
        if (p != null and os.MADV.HUGEPAGE > 0) {
            // Many Linux systems don't allow MAP_HUGETLB but they support instead
            // transparent huge pages (THP). Generally, it is not required to call `madvise` with MADV_HUGE
            // though since properly aligned allocations will already use large pages if available
            // in that case -- in particular for our large regions (in `memory.c`).
            // However, some systems only allow THP if called with explicit `madvise`, so
            // when large OS pages are enabled for mimalloc, we call `madvise` anyways.
            if (allow_large and use_large_os_page(size, try_alignment)) {
                if (mi_madvise(p.?, size, os.MADV.HUGEPAGE) == 0) {
                    is_large.* = true; // possibly
                }
            }
        } else if (builtin.os.tag == .solaris) {
            if (allow_large and use_large_os_page(size, try_alignment)) {
                const cmd: mi.memcntl_mha = .{
                    .mha_page_size = large_os_page_size,
                    .mha_cmd = mi.MHA_MAPSIZE_VA,
                };
                if (mi.memcntl(p, size, mi.MC_HAT_ADVISE, &cmd, 0, 0) == 0) {
                    is_large.* = true;
                }
            }
        }
    }
    if (p == null) {
        std.log.warn("unable to allocate OS memory ({} bytes, error code: {}, address: {*}, large only: {}, allow large: {})\n", .{ size, 0, addr, large_only, allow_large }); // TODO: Fix error reporting
    }
    return p;
}

//--------------------------------------------------------------
//   Primitive allocation from the OS.
//--------------------------------------------------------------

// Note: the `try_alignment` is just a hint and the returned pointer is not guaranteed to be aligned.
fn mi_os_mem_alloc(size: usize, try_alignment_in: usize, commit: bool, allow_large_in: bool, is_large: *bool, stats: *mi.mi_stats_t) ?PageAlignedMem {
    var allow_large = allow_large_in;
    var try_alignment = try_alignment_in;

    mi_assert_internal(size > 0 and (size % _mi_os_page_size()) == 0);
    if (size == 0) return null;

    if (!commit) allow_large = false;
    if (try_alignment == 0) try_alignment = 1; // avoid 0 to ensure there will be no divide by zero when aligning

    var p: ?PageAlignedMem = null;

    switch (builtin.os.tag) {
        .windows => {
            var flags = windows.MEM_RESERVE;
            if (commit) {
                flags |= windows.MEM_COMMIT;
            }
            p = mi_win_virtual_alloc(null, size, try_alignment, flags, false, allow_large, is_large);
        },
        .wasi => {
            is_large.* = false;
            p = mi_heap_grow(size, try_alignment);
        },
        else => {
            if (MI_USE_SBRK) {
                is_large.* = false;
                p = mi_heap_grow(size, try_alignment);
            } else {
                const protect_flags: u32 = if (commit) (os.PROT.WRITE | os.PROT.READ) else os.PROT.NONE;
                p = mi_unix_mmap(null, size, try_alignment, protect_flags, false, allow_large, is_large);
            }
        },
    }
    mi._mi_stat_counter_increase(&stats.mmap_calls, 1);
    if (p != null) {
        mi._mi_stat_increase(&stats.reserved, size);
        if (commit) {
            mi._mi_stat_increase(&stats.committed, size);
        }
    }
    return p;
}

// Primitive aligned allocation from the OS.
// This function guarantees the allocated memory is aligned.
fn mi_os_mem_alloc_aligned(size_in: usize, alignment: usize, commit: bool, allow_large_in: bool, is_large: *bool, stats: *mi.mi_stats_t) ?PageAlignedMem {
    var allow_large = allow_large_in;
    var size = size_in;

    mi_assert_internal(alignment >= _mi_os_page_size() and ((alignment & (alignment - 1)) == 0));
    mi_assert_internal(size > 0 and (size % _mi_os_page_size()) == 0);

    if (!commit) allow_large = false;
    if (!(alignment >= _mi_os_page_size() and ((alignment & (alignment - 1)) == 0))) return null;
    size = mi._mi_align_up(size, _mi_os_page_size());

    // try first with a hint (this will be aligned directly on Win 10+ or BSD)
    var p = mi_os_mem_alloc(size, alignment, commit, allow_large, is_large, stats);
    if (p == null) return null;

    // if not aligned, free it, overallocate, and unmap around it
    if ((@ptrToInt(p) % alignment) != 0) {
        _ = mi_os_mem_free(p.?, size, commit, stats);
        std.log.warn("unable to allocate aligned OS memory directly, fall back to over-allocation ({} bytes, address: {*}, alignment: {}, commit: {})\n", .{ size, p, alignment, commit });
        if (size >= (std.math.maxInt(isize) - alignment)) return null; // overflow
        const over_size = size + alignment;

        if (builtin.os.tag == .windows) {
            // over-allocate uncommitted (virtual) memory
            p = mi_os_mem_alloc(over_size, 0, // alignment
                false, // commit?
                false, // allow_large,
                is_large, stats);
            if (p == null) return null;

            // set p to the aligned part in the full region
            // note: this is dangerous on Windows as VirtualFree needs the actual region pointer
            // but in mi_os_mem_free we handle this (hopefully exceptional) situation.
            p = mi_align_up_ptr(p, alignment);

            // explicitly commit only the aligned part
            if (commit) {
                _mi_os_commit(p, size, null, stats);
            }
        } else {
            // overallocate...
            p = mi_os_mem_alloc(over_size, 1, commit, false, is_large, stats);
            if (p == null) return null;
            // and selectively unmap parts around the over-allocated area. (noop on sbrk)
            const aligned_p = mi_align_up_ptr(p, alignment).?;
            const pre_size = @ptrToInt(aligned_p) - @ptrToInt(p);
            const mid_size = mi._mi_align_up(size, _mi_os_page_size());
            const post_size = over_size - pre_size - mid_size;
            mi_assert_internal(pre_size < over_size and post_size < over_size and mid_size >= size);
            if (pre_size > 0) _ = mi_os_mem_free(p.?, pre_size, commit, stats);
            if (post_size > 0) _ = mi_os_mem_free(@alignCast(mem.page_size, aligned_p + mid_size), post_size, commit, stats);
            // we can return the aligned pointer on `mmap` (and sbrk) systems
            return aligned_p;
        }
    }
    mi_assert_internal(p == null or (p != null and (@ptrToInt(p) % alignment) == 0));
    return p;
}

test "os mem_free" {
    // _ = mi_os_mem_free(null, 100, false, &mi._mi_stats_main);
}

test "mmap" {
    var is_large = false;
    _ = mi_unix_mmap(null, 8192, 0x40000, os.PROT.READ | os.PROT.WRITE, false, true, &is_large);
    _ = mi_unix_mmap(@intToPtr([*]align(4096) u8, 0x8000000), 0x200000, 0x40000, os.PROT.READ | os.PROT.WRITE, true, true, &is_large);
}

test "os_mem_alloc_free" {
    var is_large = false;
    var stats = mi.mi_stats_t{};
    var p = mi_os_mem_alloc(8192, 8192, true, true, &is_large, &stats);
    _ = mi_os_mem_free(p.?, 8192, false, &mi._mi_stats_main);
    p = mi_os_mem_alloc_aligned(8192 * 4, 8192 * 8, true, true, &is_large, &stats);
    _ = mi_os_mem_free(p.?, 8192 * 8, false, &mi._mi_stats_main);
}

//-----------------------------------------------------------
//  OS API: alloc, free, alloc_aligned
//-----------------------------------------------------------

pub fn _mi_os_alloc(size: usize, tld_stats: *mi.mi_stats_t) ?PageAlignedMem {
    _ = tld_stats;
    var stats = &mi._mi_stats_main;
    if (size == 0) return null;
    const good_size = _mi_os_good_alloc_size(size);
    var is_large = false;
    return mi_os_mem_alloc(good_size, 0, true, false, &is_large, stats);
}

pub fn _mi_os_free_ex(p: PageAlignedMem, size: usize, was_committed: bool, tld_stats: *mi.mi_stats_t) void {
    _ = tld_stats;
    var stats = &mi._mi_stats_main;
    if (size == 0) return;
    const good_size = _mi_os_good_alloc_size(size);
    _ = mi_os_mem_free(p, good_size, was_committed, stats);
}

pub fn _mi_os_free(p: PageAlignedMem, size: usize, stats: *mi.mi_stats_t) void {
    _mi_os_free_ex(p, size, true, stats);
}

pub fn _mi_os_alloc_aligned(size: usize, alignment: usize, commit: bool, large: ?*bool, tld_stats: *mi.mi_stats_t) ?PageAlignedMem {
    _ = tld_stats;
    if (size == 0) return null;
    const good_size = _mi_os_good_alloc_size(size);
    const os_alignment = mi._mi_align_up(alignment, _mi_os_page_size());
    var allow_large = false;
    if (large != null) {
        allow_large = large.?.*;
        large.?.* = false;
    }
    return mi_os_mem_alloc_aligned(good_size, os_alignment, commit, allow_large, if (large != null) large.? else &allow_large, &mi._mi_stats_main);
}

test "os_alloc_free" {
    var stats = mi.mi_stats_t{};
    var p = _mi_os_alloc(10, &stats);
    try std.testing.expect(p != null);
    _mi_os_free(p.?, 10, &stats);
    var large = false;
    p = _mi_os_alloc_aligned(10, 8 * 4096, true, &large, &stats);
    try std.testing.expect(p != null);
    _mi_os_free(p.?, 10, &stats);
}

//-----------------------------------------------------------
//  OS aligned allocation with an offset. This is used
//  for large alignments > MI_ALIGNMENT_MAX. We use a large mimalloc
//  page where the object can be aligned at an offset from the start of the segment.
//  As we may need to overallocate, we need to free such pointers using `mi_free_aligned`
//  to use the actual start of the memory region.
//-----------------------------------------------------------

pub fn _mi_os_alloc_aligned_offset(size: usize, alignment: usize, offset: usize, commit: bool, large: *bool, tld_stats: *mi.mi_stats_t) ?PageAlignedMem {
    mi_assert(offset <= MI_SEGMENT_SIZE);
    mi_assert(offset <= size);
    mi_assert((alignment % _mi_os_page_size()) == 0);
    if (offset > MI_SEGMENT_SIZE) return null;
    if (offset == 0) {
        // regular aligned allocation
        return _mi_os_alloc_aligned(size, alignment, commit, large, tld_stats);
    } else {
        // overallocate to align at an offset
        const extra = mi._mi_align_up(offset, alignment) - offset;
        const oversize = size + extra;
        var start = _mi_os_alloc_aligned(oversize, alignment, commit, large, tld_stats) orelse return null;
        const p = @alignCast(mem.page_size, start + extra);
        mi_assert(mi._mi_is_aligned(p + offset, alignment));
        // decommit the overallocation at the start
        if (commit and extra > _mi_os_page_size()) {
            _ = _mi_os_decommit(start, extra, tld_stats);
        }
        return p;
    }
}

pub fn _mi_os_free_aligned(p: PageAlignedMem, size: usize, alignment: usize, align_offset: usize, was_committed: bool, tld_stats: *mi.mi_stats_t) void {
    mi_assert(align_offset <= MI_SEGMENT_SIZE);
    const extra = mi._mi_align_up(align_offset, alignment) - align_offset;
    const start = @alignCast(mem.page_size, p - extra);
    _mi_os_free_ex(start, size + extra, was_committed, tld_stats);
}

test "os_alloc_free_aligned_offset" {
    var stats = mi.mi_stats_t{};
    var large = false;
    const size = MI_SEGMENT_SIZE + MI_SEGMENT_SIZE / 2;
    var p = _mi_os_alloc_aligned_offset(size, 8 * 4096, MI_SEGMENT_SIZE, true, &large, &stats);
    try std.testing.expect(p != null);
    _mi_os_free_aligned(p.?, size, 8 * 4096, MI_SEGMENT_SIZE, true, &stats);
    p = _mi_os_alloc_aligned_offset(size, 8 * 4096, MI_SEGMENT_SIZE, false, &large, &stats);
    try std.testing.expect(p != null);
    _mi_os_free_aligned(p.?, size, 8 * 4096, MI_SEGMENT_SIZE, false, &stats);
}

// -----------------------------------------------------------
//  OS memory API: reset, commit, decommit, protect, unprotect.
//------------------------------------------------------------

// OS page align within a given area, either conservative (pages inside the area only),
// or not (straddling pages outside the area is possible)
fn mi_os_page_align_areax(conservative: bool, addr: [*]u8, size: usize, newsize: ?*usize) ?PageAlignedMem {
    mi_assert(size > 0);
    if (newsize != null) newsize.?.* = 0;
    if (size == 0) return null;

    // page align conservatively within the range
    const start = if (conservative) mi_align_up_ptr(addr, _mi_os_page_size()) else mi_align_down_ptr(addr, _mi_os_page_size());
    const end = if (conservative) mi_align_down_ptr(addr + size, _mi_os_page_size()) else mi_align_up_ptr(addr + size, _mi_os_page_size());
    const diff = mi_ptr_diff(end, start);
    if (diff <= 0) return null;

    mi_assert_internal((conservative and diff <= size) or (!conservative and diff >= size));
    if (newsize != null) newsize.?.* = diff;
    return @alignCast(mem.page_size, start);
}

fn mi_os_page_align_area_conservative(addr: PageAlignedMem, size: usize, newsize: *usize) ?PageAlignedMem {
    return mi_os_page_align_areax(true, addr, size, newsize);
}

fn mi_mprotect_hint(err: i32) void {
    if (MI_OS_USE_MMAP and MI_SECURE >= 2) { // guard page around every mimalloc page
        if (err == mi.ENOMEM)
            std.log.warn("the previous warning may have been caused by a low memory map limit.\n" ++
                "  On Linux this is controlled by the vm.max_map_count. For example:\n" ++
                "  > sudo sysctl -w vm.max_map_count=262144\n", .{});
    }
}

// Commit/Decommit memory.
// Usually commit is aligned liberal, while decommit is aligned conservative.
// (but not for the reset version where we want commit to be conservative as well)
fn mi_os_commitx(addr: PageAlignedMem, size: usize, commit: bool, conservative: bool, is_zero: ?*bool, stats: *mi.mi_stats_t) bool {
    // page align in the range, commit liberally, decommit conservative
    if (is_zero != null) {
        is_zero.?.* = false;
    }
    var csize: usize = undefined;
    const start = mi_os_page_align_areax(conservative, addr, size, &csize);
    if (csize == 0) return true; // || _mi_os_is_huge_reserved(addr))
    var err: i32 = 0;
    if (commit) {
        mi._mi_stat_increase(&stats.committed, size); // use size for precise commit vs. decommit
        mi._mi_stat_counter_increase(&stats.commit_calls, 1);
    } else {
        mi._mi_stat_decrease(&stats.committed, size);
    }

    switch (builtin.os.tag) {
        .windows => {
            if (commit) {
                // *is_zero = true;  // note: if the memory was already committed, the call succeeds but the memory is not zero'd
                const p = windows.VirtualAlloc(start, csize, windows.MEM_COMMIT, windows.PAGE_READWRITE);
                err = if (p == start) 0 else kernel32.GetLastError();
            } else {
                const ok = windows.VirtualFree(start, csize, windows.MEM_DECOMMIT);
                err = if (ok) 0 else kernel32.GetLastError();
            }
        },
        .wasi => {
            // WebAssembly guests can't control memory protection
        },
        .other => { // unused for now
            if (false) {
                // Linux: disabled for now as mmap fixed seems much more expensive than MADV_DONTNEED (and splits VMA's?)
                if (commit) {
                    // commit: just change the protection
                    os.mprotect(start[0..csize], (os.PROT.READ | os.PROT.WRITE)) catch return false;
                } else {
                    // decommit: use mmap with MAP_FIXED to discard the existing memory (and reduce rss)
                    const fd = mi_unix_mmap_fd();
                    const p = os.mmap(start, csize, os.PROT.NONE, (os.MAP.FIXED | os.MAP.PRIVATE | os.MAP.ANONYMOUS | os.MAP.NORESERVE), fd, 0);
                    if (p != start) {
                        // TODO: log a message?
                        return false;
                    }
                }
            }
        },
        else => {
            // Linux, macOSX and others.
            if (commit) {
                // commit: ensure we can access the area
                os.mprotect(start.?[0..csize], (os.PROT.READ | os.PROT.WRITE)) catch return false;
            } else {
                if (MI_DEBUG == 0 and MI_SECURE == 0) {
                    // decommit: use MADV_DONTNEED as it decreases rss immediately (unlike MADV_FREE)
                    // (on the other hand, MADV_FREE would be good enough.. it is just not reflected in the stats :-( )
                    os.madvise(start.?[0..csize], os.MADV.DONTNEED) catch return false;
                } else {
                    // decommit: just disable access (also used in debug and secure mode to trap on illegal access)
                    os.mprotect(start.?[0..csize], os.PROT.NONE) catch return false;
                }
            }
        },
    }
    // if (err != 0) {
    //     std.log.warn("{s} error: start: {*}, csize: {}, err: {}\n", .{ if (commit) "commit" else "decommit", start, csize, err });
    //     mi_mprotect_hint(err);
    // }
    mi_assert_internal(err == 0);
    return (err == 0);
}

pub fn _mi_os_commit(addr: PageAlignedMem, size: usize, is_zero: *bool, tld_stats: *mi.mi_stats_t) bool {
    _ = tld_stats;
    var stats = &mi._mi_stats_main;
    return mi_os_commitx(addr, size, true, false, // liberal
        is_zero, stats);
}

pub fn _mi_os_decommit(addr: PageAlignedMem, size: usize, tld_stats: *mi.mi_stats_t) bool {
    _ = tld_stats;
    var stats = &mi._mi_stats_main;
    var is_zero: bool = undefined;
    return mi_os_commitx(addr, size, false, true, // conservative
        &is_zero, stats);
}

var madv_advice = Atomic(u32).init(os.MADV.FREE);

// Signal to the OS that the address range is no longer in use
// but may be used later again. This will release physical memory
// pages and reduce swapping while keeping the memory committed.
// We page align to a conservative area inside the range to reset.
fn mi_os_resetx(addr: PageAlignedMem, size: usize, reset: bool, stats: *mi.mi_stats_t) bool {
    // page align conservatively within the range
    var csize: usize = undefined;
    const start = mi_os_page_align_area_conservative(addr, size, &csize);
    if (csize == 0) return true; // || _mi_os_is_huge_reserved(addr)
    if (reset) mi._mi_stat_increase(&stats.reset, csize) else mi._mi_stat_decrease(&stats.reset, csize);
    if (!reset) return true; // nothing to do on unreset!

    if (MI_DEBUG > 1 and !mi.MI_TRACK_ENABLED) {
        if (MI_SECURE == 0) {
            @memset(start.?, 0, csize); // pretend it is eagerly reset
        }
    }

    var err: usize = 0;
    switch (builtin.os.tag) {
        .windows => {
            // Testing shows that for us (on `malloc-large`) MEM_RESET is 2x faster than DiscardVirtualMemory
            var p = windows.VirtualAlloc(start, csize, windows.MEM_RESET, windows.PAGE_READWRITE);
            mi_assert_internal(p == start);
            if (true) {
                if (p == start and start != null) {
                    kernel32.VirtualUnlock(start, csize); // VirtualUnlock after MEM_RESET removes the memory from the working set
                }
            }
            if (p != start) return false;
        },
        .wasi => {
            // nothing to do
        },
        else => { // platforms that support MADV_FREE
            const oadvice = mi.mi_atomic_load_relaxed(&madv_advice);
            while (true) {
                err = mi_madvise(start.?, csize, oadvice);
                if (err == 0 or os.errno(err) != .AGAIN) break;
            }
            if (err != 0 or os.errno(err) == .INVAL or oadvice == os.MADV.FREE) {
                // if MADV_FREE is not supported, fall back to MADV_DONTNEED from now on
                mi.mi_atomic_store_release(&madv_advice, os.MADV.DONTNEED);
                err = mi_madvise(start.?, csize, os.MADV.DONTNEED);
            }
        },
    }
    if (err != 0) {
        std.log.warn("madvise reset error: start: {*}, csize: {}, errno: {}\n", .{ start, csize, err });
    }
    //mi_assert(err == 0);
    if (err != 0) return false;
    return true;
}

// Signal to the OS that the address range is no longer in use
// but may be used later again. This will release physical memory
// pages and reduce swapping while keeping the memory committed.
// We page align to a conservative area inside the range to reset.
pub fn _mi_os_reset(addr: PageAlignedMem, size: usize, tld_stats: *mi.mi_stats_t) bool {
    _ = tld_stats;
    var stats = &mi._mi_stats_main;
    return mi_os_resetx(addr, size, true, stats);
}

// Protect a region in memory to be not accessible.
fn mi_os_protectx(addr: PageAlignedMem, size: usize, protect: bool) bool {
    // page align conservatively within the range
    var csize: usize = undefined;
    const start = mi_os_page_align_area_conservative(addr, size, &csize);
    if (csize == 0) return false;
    // if (_mi_os_is_huge_reserved(addr)) {
    // _mi_warning_message("cannot mprotect memory allocated in huge OS pages\n");
    //
    var err: usize = 0;
    switch (builtin.os.tag) {
        .windows => {
            var oldprotect: windows.DWORD = 0;
            const ok = kernel32.VirtualProtect(start.?, csize, if (protect) windows.PAGE_NOACCESS else windows.PAGE_READWRITE, &oldprotect);
            err = if (ok) 0 else kernel32.GetLastError();
        },
        .wasi => {
            err = 0;
        },
        else => {
            os.mprotect(start.?[0..csize], if (protect) os.PROT.NONE else (os.PROT.READ | os.PROT.WRITE)) catch |e| {
                std.log.warn("mprotect error: start: {*}, csize: {}, err: {}\n", .{ start, csize, e });
                // mi_mprotect_hint(err);
            };
        },
    }
    return (err == 0);
}

pub fn _mi_os_protect(addr: PageAlignedMem, size: usize) bool {
    return mi_os_protectx(addr, size, true);
}

pub fn _mi_os_unprotect(addr: PageAlignedMem, size: usize) bool {
    return mi_os_protectx(addr, size, false);
}

test "os_protect_unprotect" {
    var stats = mi.mi_stats_t{};
    var large = false;
    const size = MI_SEGMENT_SIZE + MI_SEGMENT_SIZE / 2;
    var p = _mi_os_alloc_aligned_offset(size, 8 * 4096, MI_SEGMENT_SIZE, true, &large, &stats);
    try std.testing.expect(p != null);
    try std.testing.expect(_mi_os_protect(p.?, 8 * 4096) == true);
    try std.testing.expect(_mi_os_unprotect(p.?, 8 * 4096) == true);
    _mi_os_free_aligned(p.?, size, 8 * 4096, MI_SEGMENT_SIZE, true, &stats);
}

pub fn _mi_os_shrink(p: PageAlignedMem, oldsize: usize, newsize: usize, stats: *mi.mi_stats_t) bool {
    // page align conservatively within the range
    mi_assert_internal(oldsize > newsize);
    if (oldsize < newsize) return false;
    if (oldsize == newsize) return true;

    // oldsize and newsize should be page aligned or we cannot shrink precisely
    const addr = @alignCast(mem.page_size, p + newsize);
    var size: usize = undefined;
    const start = mi_os_page_align_area_conservative(addr, oldsize - newsize, &size);
    if (size == 0 or start.? != addr) return false;

    if (builtin.os.tag == .windows) {
        // we cannot shrink on windows, but we can decommit
        return _mi_os_decommit(start.?, size, stats);
    } else {
        return mi_os_mem_free(start.?, size, true, stats);
    }
}

test "os_shrink" {
    var stats = mi.mi_stats_t{};
    var large = false;
    const size = MI_SEGMENT_SIZE + MI_SEGMENT_SIZE / 2;
    var p = _mi_os_alloc_aligned_offset(size, 8 * 4096, MI_SEGMENT_SIZE, true, &large, &stats);
    try std.testing.expect(p != null);
    try std.testing.expect(_mi_os_shrink(p.?, 8 * 4096, 4 * 4096, &stats) == true);
    _mi_os_free_aligned(p.?, size, 4 * 4096, MI_SEGMENT_SIZE, true, &stats);
}

//-----------------------------------------------------------------------------
// Support for allocating huge OS pages (1Gib) that are reserved up-front
// and possibly associated with a specific NUMA node. (use `numa_node>=0`)
//-----------------------------------------------------------------------------
const MI_HUGE_OS_PAGE_SIZE = mi.MI_GiB;

const MI_PROCESSOR_NUMBER = if (builtin.os.tag == .windows) struct { Group: windows.WORD, Number: windows.BYTE, Reserved: windows.BYTE };

fn mi_os_alloc_huge_os_pagesx(addr: PageAlignedMem, size: usize, numa_node: i32) ?PageAlignedMem {
    if (mi.MI_INTPTR_SIZE < 8) return null;
    mi_assert_internal(size % mi.MI_GiB == 0);
    switch (builtin.os.tag) {
        .windows => {
            const flags: windows.DWORD = windows.MEM_LARGE_PAGES | windows.MEM_COMMIT | windows.MEM_RESERVE;

            mi_win_enable_large_os_pages();

            var params = std.mem.zeroes([3]windows.MI_MEM_EXTENDED_PARAMETER);
            // on modern Windows try use NtAllocateVirtualMemoryEx for 1GiB huge pages
            mi_huge_pages_available = true;
            if (windows.pNtAllocateVirtualMemoryEx != null and mi_huge_pages_available) {
                params[0].Type.Type = .MiMemExtendedParameterAttributeFlags;
                params[0].Arg.ULong64 = windows.MI_MEM_EXTENDED_PARAMETER_NONPAGED_HUGE;
                var param_count: windows.ULONG = 1;
                if (numa_node >= 0) {
                    param_count += 1;
                    params[1].Type.Type = .MiMemExtendedParameterNumaNode;
                    params[1].Arg.ULong = numa_node;
                }
                var psize: windows.SIZE_T = size;
                var base = addr;
                const err: windows.NTSTATUS = windows.pNtAllocateVirtualMemoryEx(kernel32.GetCurrentProcess(), &base, &psize, flags, windows.PAGE_READWRITE, params, param_count);
                if (err == 0 and base != null) {
                    return base;
                } else {
                    // fall back to regular large pages
                    mi_huge_pages_available = false; // don't try further huge pages
                    std.log.warn("unable to allocate using huge (1GiB) pages, trying large (2MiB) pages instead (status {})\n", err);
                }
            }
            // on modern Windows try use VirtualAlloc2 for numa aware large OS page allocation
            if (windows.pVirtualAlloc2 != null and numa_node >= 0) {
                params[0].Type.Type = .MiMemExtendedParameterNumaNode;
                params[0].Arg.ULong = numa_node;
                return windows.pVirtualAlloc2(kernel32.GetCurrentProcess(), addr, size, flags, windows.PAGE_READWRITE, params, 1);
            }
            // otherwise use regular virtual alloc on older windows
            return windows.VirtualAlloc(addr, size, flags, windows.PAGE_READWRITE);
        },
        .haiku => return null,
        else => {
            if (!mi.MI_OS_USE_MMAP) return null;
            const MPOL_PREFERRED = 1;
            var is_large = true;
            const p = mi_unix_mmap(addr, size, MI_SEGMENT_SIZE, os.PROT.READ | os.PROT.WRITE, true, true, &is_large);
            if (p == null) return null;
            if (numa_node >= 0 and numa_node < 8 * MI_INTPTR_SIZE) { // at most 64 nodes
                var numa_mask = (@intCast(usize, 1) << numa_node);
                // TODO: does `mbind` work correctly for huge OS pages? should we
                // use `set_mempolicy` before calling mmap instead?
                // see: <https://lkml.org/lkml/2017/2/9/875>
                const err = mi_os_mbind(p, size, MPOL_PREFERRED, &numa_mask, 8 * MI_INTPTR_SIZE, 0);
                if (err != 0) {
                    std.log.warn("failed to bind huge (1GiB) pages to numa node %d: %s\n", numa_node, os.errno(err));
                }
            }
            return p;
        },
    }
}

fn mi_os_mbind(start: PageAlignedMem, len: usize, mode: usize, nmask: *const usize, maxnode: usize, flags: u32) isize {
    return os.syscall7(.mbind, start, len, mode, nmask, maxnode, flags);
}

// To ensure proper alignment, use our own area for huge OS pages
// TODO: align(atomic.cache_line)
var mi_huge_start = Atomic(usize).init(0);

// Claim an aligned address range for huge pages
fn mi_os_claim_huge_pages(pages: usize, total_size: ?*usize) ?PageAlignedMem {
    if (MI_INTPTR_SIZE < 8) return null;
    if (total_size != null) total_size.?.* = 0;
    const size = pages * MI_HUGE_OS_PAGE_SIZE;

    var start: usize = 0;
    var end: usize = 0;
    var huge_start = mi.mi_atomic_load_relaxed(&mi_huge_start);
    while (true) {
        start = huge_start;
        if (start == 0) {
            // Initialize the start address after the 32TiB area
            start = (@intCast(usize, 32) << 40); // 32TiB virtual start address
            if (MI_SECURE > 0 or MI_DEBUG == 0) { // security: randomize start of huge pages unless in debug mode
                const r = mi._mi_heap_random_next(mi.mi_get_default_heap());
                start = start + (MI_HUGE_OS_PAGE_SIZE * ((r >> 17) & 0x0FFF)); // (randomly 12bits)*1GiB == between 0 to 4TiB
            }
        }
        end = start + size;
        mi_assert_internal(end % MI_SEGMENT_SIZE == 0);
        if (mi.mi_atomic_cas_strong_acq_rel(&mi_huge_start, &huge_start, end))
            break;
    }

    if (total_size != null) total_size.?.* = size;
    return start;
}

// Allocate MI_SEGMENT_SIZE aligned huge pages
fn _mi_os_alloc_huge_os_pages(pages: usize, numa_node: i32, max_msecs: mi.mi_msecs_t, pages_reserved: ?*usize, psize: ?*usize) ?PageAlignedMem {
    if (psize != null) psize.?.* = 0;
    if (pages_reserved != null) pages_reserved.?.* = 0;
    var size: usize = 0;
    const start = mi_os_claim_huge_pages(pages, &size);
    if (start == null) return null; // or 32-bit systems

    // Allocate one page at the time but try to place them contiguously
    // We allocate one page at the time to be able to abort if it takes too long
    // or to at least allocate as many as available on the system.
    var start_t = mi._mi_clock_start();
    var page: usize = 0;
    while (page < pages) : (page += 1) {
        // allocate a page
        const addr = start + (page * MI_HUGE_OS_PAGE_SIZE);
        const p = mi_os_alloc_huge_os_pagesx(addr, MI_HUGE_OS_PAGE_SIZE, numa_node);

        // Did we succeed at a contiguous address?
        if (p != addr) {
            // no success, issue a warning and break
            if (p != null) {
                std.log.warn("could not allocate contiguous huge page {} at {*}\n", .{ page, addr });
                mi._mi_os_free(p, MI_HUGE_OS_PAGE_SIZE, &mi._mi_stats_main);
            }
            break;
        }

        // success, record it
        mi._mi_stat_increase(&mi._mi_stats_main.committed, MI_HUGE_OS_PAGE_SIZE);
        mi._mi_stat_increase(&mi._mi_stats_main.reserved, MI_HUGE_OS_PAGE_SIZE);

        // check for timeout
        if (max_msecs > 0) {
            var elapsed = mi._mi_clock_end(start_t);
            if (page >= 1) {
                const estimate = ((elapsed / (page + 1)) * pages);
                if (estimate > 2 * max_msecs) { // seems like we are going to timeout, break
                    elapsed = max_msecs + 1;
                }
            }
            if (elapsed > max_msecs) {
                std.log.warn("huge page allocation timed out\n");
                break;
            }
        }
    }
    mi_assert_internal(page * MI_HUGE_OS_PAGE_SIZE <= size);
    if (pages_reserved != null) {
        pages_reserved.?.* = page;
    }
    if (psize != null) {
        psize.?.* = page * MI_HUGE_OS_PAGE_SIZE;
    }
    return if (page == 0) null else start;
}

// free every huge page in a range individually (as we allocated per page)
// note: needed with VirtualAlloc but could potentially be done in one go on mmap'd systems.
fn _mi_os_free_huge_pages(p: PageAlignedMem, size_in: usize, stats: *mi.mi_stats_t) void {
    if (size_in == 0) return;
    var base = p;
    var size = size_in;
    while (size >= MI_HUGE_OS_PAGE_SIZE) {
        _mi_os_free(base, MI_HUGE_OS_PAGE_SIZE, stats);
        size -= MI_HUGE_OS_PAGE_SIZE;
        base += MI_HUGE_OS_PAGE_SIZE;
    }
}

//-----------------------------------------------------------------------------
// Support NUMA aware allocation
//-----------------------------------------------------------------------------
fn mi_os_numa_nodex() usize {
    switch (builtin.os.tag) {
        .windows => {
            var numa_node: windows.USHORT = 0;
            if (windows.pGetCurrentProcessorNumberEx != null and windows.pGetNumaProcessorNodeEx != null) {
                // Extended API is supported
                var pnum: MI_PROCESSOR_NUMBER = undefined;
                windows.pGetCurrentProcessorNumberEx(&pnum);
                var nnode: windows.USHORT = 0;
                const ok = windows.pGetNumaProcessorNodeEx(&pnum, &nnode);
                if (ok) {
                    numa_node = nnode;
                }
            } else if (windows.pGetNumaProcessorNode != null) {
                // Vista or earlier, use older API that is limited to 64 processors. Issue #277
                var pnum: windows.DWORD = windows.GetCurrentProcessorNumber();
                var nnode: windows.UCHAR = 0;
                const ok = windows.pGetNumaProcessorNode(pnum, &nnode);
                if (ok) {
                    numa_node = nnode;
                }
            }
            return numa_node;
        },
        .linux => {
            var node: usize = 0;
            var ncpu: usize = 0;
            const err = os.linux.syscall2(.getcpu, @ptrToInt(&ncpu), @ptrToInt(&node));
            if (err != 0) return 0;
            return node;
        },
        .freebsd => { // TODO: check __FreeBSD_version >= 1200000
            var dom: os.c.domainset_t = undefined;
            var policy: i32 = undefined;
            if (os.cpuset_getdomain(os.CPU_LEVEL_CPUSET, os.CPU_WHICH_PID, -1, @sizeOf(os.c.domainset_t), &dom, &policy) == -1) return 0;
            var node: usize = 0;
            while (node < os.MAXMEMDOM) : (node += 1) {
                if (os.DOMAINSET_ISSET(node, &dom)) return node;
            }
            return 0;
        },
        .dragonfly => {
            return 0;
        },
        else => return 0,
    }
}

fn mi_os_numa_node_countx() usize {
    switch (builtin.os.tag) {
        .windows => {
            var numa_max: windows.ULONG = 0;
            windows.GetNumaHighestNodeNumber(&numa_max);
            // find the highest node number that has actual processors assigned to it. Issue #282
            while (numa_max > 0) {
                if (windows.pGetNumaNodeProcessorMaskEx != null) {
                    // Extended API is supported
                    var affinity: windows.GROUP_AFFINITY = undefined;
                    if (windows.pGetNumaNodeProcessorMaskEx(numa_max, &affinity)) {
                        if (affinity.Mask != 0) break; // found the maximum non-empty node
                    }
                } else {
                    // Vista or earlier, use older API that is limited to 64 processors.
                    var mask: windows.ULONGLONG = undefined;
                    if (windows.GetNumaNodeProcessorMask(numa_max, &mask)) {
                        if (mask != 0) break; // found the maximum non-empty node
                    }
                }
                // max node was invalid or had no processor assigned, try again
                numa_max -= 1;
            }
            return (numa_max + 1);
        },
        .linux => {
            var buf: [128]u8 = undefined;
            var node: usize = 0;
            while (node < 256) : (node += 1) {
                // enumerate node entries -- todo: it there a more efficient way to do this? (but ensure there is no allocation)
                const b = std.fmt.bufPrint(&buf, "/sys/devices/system/node/node{}", .{node + 1}) catch unreachable;
                os.access(b, os.R_OK) catch break;
            }
            return (node + 1);
        },
        .freebsd => {
            var ndomains: usize = 0;
            var len = @sizeOf(usize);
            if (os.sysctlbyname("vm.ndomains", &ndomains, &len, null, 0) == -1) return 0;
            return ndomains;
        },
        .dragonfly => {
            var ncpus: usize = 0;
            var nvirtcoresperphys: usize = 0;
            var len = @sizeOf(usize);
            if (os.sysctlbyname("hw.ncpu", &ncpus, &len, null, 0) == -1) return 0;
            if (os.sysctlbyname("hw.cpu_topology_ht_ids", &nvirtcoresperphys, &len, null, 0) == -1) return 0;
            return nvirtcoresperphys * ncpus;
        },
        else => return 1,
    }
}
var _mi_numa_node_count = Atomic(usize).init(0); // = 0   // cache the node count

pub fn _mi_os_numa_node_count_get() usize {
    var count = mi.mi_atomic_load_acquire(&_mi_numa_node_count);
    if (count <= 0) {
        var ncount = mi.mi_option_get(.mi_option_use_numa_nodes); // given explicitly?
        if (ncount > 0) {
            count = @intCast(usize, ncount);
        } else {
            count = mi_os_numa_node_countx(); // or detect dynamically
            if (count == 0) count = 1;
        }
        mi.mi_atomic_store_release(&_mi_numa_node_count, count); // save it
        std.log.info("using {} numa regions\n", .{count});
    }
    return count;
}

pub inline fn _mi_os_numa_node(tld: *mi.mi_os_tld_t) i32 {
    if (mi_likely(mi.mi_atomic_load_relaxed(&_mi_numa_node_count) == 1)) {
        return 0;
    } else return _mi_os_numa_node_get(tld);
}

pub inline fn _mi_os_numa_node_count() usize {
    const count = mi.mi_atomic_load_relaxed(&_mi_numa_node_count);
    if (mi_likely(count > 0)) {
        return count;
    } else return _mi_os_numa_node_count_get();
}

pub fn _mi_os_numa_node_get(tld: *mi.mi_os_tld_t) i32 {
    _ = tld;
    var numa_count = _mi_os_numa_node_count();
    if (numa_count <= 1) return 0; // optimize on single numa node systems: always node 0
    // never more than the node count and >= 0
    var numa_node = mi_os_numa_nodex();
    if (numa_node >= numa_count) {
        numa_node = numa_node % numa_count;
    }
    return @intCast(i32, numa_node);
}
