// A wrapper for mimalloc

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mimalloc = struct {
    usingnamespace @import("alloc.zig"); // mi_page_usable_size and mi_free
    usingnamespace @import("alloc-aligned.zig"); // mi_os_mem_alloc_aligned
};

// This is currently unused, still experimenting!
pub const MiMallocOptions = struct {
    // Debug mode
    debug: bool = switch (builtin.mode) {
        .Debug => true,
        else => false,
    },

    // Use full internal heap invariant checking in DEBUG mode (expensive)
    debug_full: bool = false,

    // Show error and warning messages by default (only enabled by default in DEBUG mode)
    show_errors: bool = false,

    // Use full security mitigations (like guard pages, allocation randomization,
    // double-free mitigation, and free-list corruption detection)
    secure: bool = false,

    // Enable padding to detect heap block overflow (used only in DEBUG mode)
    padding: bool = true,
};

pub fn MiMalloc(comptime options: MiMallocOptions) type {
    _ = options;
    return struct {
        pub fn allocator() Allocator {
            // this is a singleton
            return Allocator{
                .ptr = undefined,
                .vtable = &Allocator.VTable{
                    .alloc = alloc,
                    .resize = resize,
                    .free = free,
                },
            };
        }
    };
}

fn alloc(state_ptr: *anyopaque, len: usize, ptr_align: u8, ret_addr: usize) ?[*]u8 {
    _ = state_ptr;
    _ = ret_addr;
    const ptr = mimalloc.mi_malloc_aligned(len, @shlExact(@as(usize, 1), @intCast(u6, ptr_align)));
    return @ptrCast(?[*]u8, ptr);
}

fn resize(state_ptr: *anyopaque, buf: []u8, buf_align: u8, new_len: usize, ret_addr: usize) bool {
    _ = state_ptr;
    _ = buf_align;
    _ = ret_addr;
    const usable_size = mimalloc.mi_usable_size(buf.ptr);
    return (usable_size < new_len);
}

fn free(state_ptr: *anyopaque, buf: []u8, buf_align: u8, ret_addr: usize) void {
    _ = state_ptr;
    _ = buf_align;
    _ = ret_addr;
    mimalloc.mi_free(@ptrCast(*u8, buf.ptr));
}
