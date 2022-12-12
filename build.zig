const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const target = b.standardTargetOptions(.{});
    const mode = b.standardReleaseOptions();

    const sources = &.{
        "src/stats.c",
        "src/random.c",
        "src/os.c",
        "src/bitmap.c",
        "src/arena.c",
        "src/segment-cache.c",
        "src/segment.c",
        "src/page.c",
        "src/alloc.c",
        "src/alloc-aligned.c",
        "src/alloc-posix.c",
        "src/heap.c",
        "src/options.c",
        "src/init.c",
    };
    const flags = &.{
        "-Wall",
        "-Werror",
    };

    const object = b.addObject("mimalloc", null);
    object.addIncludePath("./include");
    object.addCSourceFiles(sources, flags);
    object.linkLibC();
    object.setTarget(target);
    object.setBuildMode(mode);

    const mimalloc_zig = b.addStaticLibrary("mimalloc-zig", "src/mimalloc.zig");
    mimalloc_zig.setTarget(target);
    mimalloc_zig.addIncludePath("./include");
    mimalloc_zig.setBuildMode(mode);
    mimalloc_zig.addObject(object);
    mimalloc_zig.install();

    const lib = b.addStaticLibrary("mimalloc", null);
    lib.addIncludePath("./include");
    lib.addCSourceFiles(sources, flags);
    lib.linkLibC();
    lib.setBuildMode(mode);
    lib.setTarget(target);
    lib.install();

    const dynlib = b.addSharedLibrary("mimalloc", null, .unversioned);
    dynlib.addIncludePath("./include");
    dynlib.addCSourceFiles(sources, flags);
    dynlib.setBuildMode(mode);
    dynlib.linkLibC();
    dynlib.setTarget(target);
    dynlib.install();

    const ctest_step = b.step("ctest", "Build and Run C Tests");
    const ctests = [_][:0]const u8{ "api", "api-fill", "stress" };
    inline for (ctests) |t| {
        const texe = b.addExecutable(t, "test/test-" ++ t ++ ".c");
        texe.setBuildMode(mode);
        texe.addIncludePath("./include");
        texe.addObject(object);
        ctest_step.dependOn(&texe.run().step);
    }

    const zig_tests = b.addTest("src/test.zig");
    zig_tests.setBuildMode(mode);
    zig_tests.addIncludePath("./include");
    zig_tests.linkLibrary(mimalloc_zig);

    const zigtest_step = b.step("zigtest", "Run library Zig tests");
    zigtest_step.dependOn(&zig_tests.step);

    const test_step = b.step("test", "Run library (C and Zig) tests");
    test_step.dependOn(&zig_tests.step);
    test_step.dependOn(ctest_step);
}
