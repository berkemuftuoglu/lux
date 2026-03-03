const std = @import("std");
const builtin = @import("builtin");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- Main executable ---
    const exe = b.addExecutable(.{
        .name = "lux",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    addPostgres(exe);
    b.installArtifact(exe);

    // --- Run step ---
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run Lux");
    run_step.dependOn(&run_cmd.step);

    // --- Test step ---
    const test_step = b.step("test", "Run unit tests");
    const test_files = [_][]const u8{
        "src/main.zig",
        "src/web.zig",
        "src/postgres.zig",
        "src/utils.zig",
        "src/sql_guard.zig",
        "src/crud.zig",
        "src/schema.zig",
        "src/export.zig",
        "src/sql.zig",
    };
    for (test_files) |source| {
        addTest(b, test_step, source, target, optimize);
    }

    // --- Check step (ZLS build-on-save, compile without linking) ---
    const check_exe = b.addExecutable(.{
        .name = "lux",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    addPostgres(check_exe);
    const check_step = b.step("check", "Check compilation without linking");
    check_step.dependOn(&check_exe.step);
}

/// Add a test compilation for a single source module.
fn addTest(
    b: *std.Build,
    test_step: *std.Build.Step,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const unit_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(source),
            .target = target,
            .optimize = optimize,
        }),
    });
    addPostgres(unit_test);
    const run_test = b.addRunArtifact(unit_test);
    test_step.dependOn(&run_test.step);
}

/// Link PostgreSQL client library (libpq).
/// Uses pkg-config when available, falls back to platform-specific paths.
fn addPostgres(step: *std.Build.Step.Compile) void {
    // Prefer system library detection (uses pkg-config internally)
    step.linkSystemLibrary("pq");
    step.linkLibC();

    // Platform-specific fallback paths for common installations
    switch (builtin.os.tag) {
        .macos => {
            // Homebrew (Apple Silicon)
            step.addIncludePath(.{ .cwd_relative = "/opt/homebrew/opt/libpq/include" });
            step.addLibraryPath(.{ .cwd_relative = "/opt/homebrew/opt/libpq/lib" });
            // Homebrew (Intel)
            step.addIncludePath(.{ .cwd_relative = "/usr/local/opt/libpq/include" });
            step.addLibraryPath(.{ .cwd_relative = "/usr/local/opt/libpq/lib" });
            // Postgres.app
            step.addIncludePath(.{ .cwd_relative = "/Applications/Postgres.app/Contents/Versions/latest/include" });
            step.addLibraryPath(.{ .cwd_relative = "/Applications/Postgres.app/Contents/Versions/latest/lib" });
        },
        .windows => {
            // PostgreSQL installer defaults (v16, v15)
            step.addIncludePath(.{ .cwd_relative = "C:/Program Files/PostgreSQL/16/include" });
            step.addLibraryPath(.{ .cwd_relative = "C:/Program Files/PostgreSQL/16/lib" });
            step.addIncludePath(.{ .cwd_relative = "C:/Program Files/PostgreSQL/15/include" });
            step.addLibraryPath(.{ .cwd_relative = "C:/Program Files/PostgreSQL/15/lib" });
        },
        else => {
            // Linux: common distro paths
            step.addIncludePath(.{ .cwd_relative = "/usr/include/postgresql" });
            step.addIncludePath(.{ .cwd_relative = "/usr/local/include" });
        },
    }
}
