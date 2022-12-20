const std = @import("std");
const mimalloc = @import("mimalloc.zig");
const expect = std.testing.expect;

test "basic" {
    const mimalloc_allocator = mimalloc.MiMalloc(.{});
    const allocator = mimalloc_allocator.allocator();

    const foo = try allocator.create(u8);
    const bar = try allocator.alloc(u8, 10);
    try expect(bar.len == 10);
    allocator.destroy(foo);
    allocator.free(bar);
}
