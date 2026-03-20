const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // --- httpz dependency ---
    const httpz_dep = b.dependency("httpz", .{ .target = target, .optimize = optimize });
    const mod_httpz = httpz_dep.module("httpz");

    // --- pg.zig dependency ---
    const pg_dep = b.dependency("pg", .{ .target = target, .optimize = optimize });
    const mod_pg = pg_dep.module("pg");

    // --- Named modules (cross-directory imports) ---
    const mod_utils = b.createModule(.{
        .root_source_file = b.path("src/lib/utils.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mod_sql_guard = b.createModule(.{
        .root_source_file = b.path("src/lib/sql_guard.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod_sql_guard.addImport("utils", mod_utils);
    const mod_postgres = b.createModule(.{
        .root_source_file = b.path("src/server/postgres.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod_postgres.addImport("pg", mod_pg);

    // web.zig depends on utils, postgres, crud, schema, export, sql
    // (handlers depend on web, so web is declared after handlers below via forward ref)
    // We declare web + handlers together, then wire dependencies.

    const mod_crud = b.createModule(.{
        .root_source_file = b.path("src/handlers/crud.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mod_schema = b.createModule(.{
        .root_source_file = b.path("src/handlers/schema.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mod_export = b.createModule(.{
        .root_source_file = b.path("src/handlers/export.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mod_sql = b.createModule(.{
        .root_source_file = b.path("src/handlers/sql.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mod_web = b.createModule(.{
        .root_source_file = b.path("src/web.zig"),
        .target = target,
        .optimize = optimize,
    });
    const mod_main = b.createModule(.{
        .root_source_file = b.path("src/server/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Wire httpz to all modules that need it
    mod_web.addImport("httpz", mod_httpz);
    mod_crud.addImport("httpz", mod_httpz);
    mod_schema.addImport("httpz", mod_httpz);
    mod_export.addImport("httpz", mod_httpz);
    mod_sql.addImport("httpz", mod_httpz);

    // Wire pg to modules that need direct pool/conn access
    mod_web.addImport("pg", mod_pg);
    mod_export.addImport("pg", mod_pg);
    mod_schema.addImport("pg", mod_pg);

    // Wire module dependencies
    mod_crud.addImport("postgres", mod_postgres);
    mod_crud.addImport("utils", mod_utils);
    mod_crud.addImport("web", mod_web);

    mod_schema.addImport("postgres", mod_postgres);
    mod_schema.addImport("utils", mod_utils);
    mod_schema.addImport("web", mod_web);

    mod_export.addImport("postgres", mod_postgres);
    mod_export.addImport("utils", mod_utils);
    mod_export.addImport("web", mod_web);
    mod_export.addImport("crud", mod_crud);
    mod_export.addImport("sql_guard", mod_sql_guard);

    mod_sql.addImport("postgres", mod_postgres);
    mod_sql.addImport("utils", mod_utils);
    mod_sql.addImport("sql_guard", mod_sql_guard);
    mod_sql.addImport("web", mod_web);
    mod_sql.addImport("crud", mod_crud);

    mod_web.addImport("postgres", mod_postgres);
    mod_web.addImport("utils", mod_utils);
    mod_web.addImport("crud", mod_crud);
    mod_web.addImport("schema", mod_schema);
    mod_web.addImport("export", mod_export);
    mod_web.addImport("sql", mod_sql);

    mod_main.addImport("web", mod_web);

    // --- Main executable ---
    const exe = b.addExecutable(.{
        .name = "lux",
        .root_module = mod_main,
    });
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
    addTestMod(b, test_step, mod_main);
    addTestMod(b, test_step, mod_web);
    addTestMod(b, test_step, mod_postgres);
    addTestMod(b, test_step, mod_utils);
    addTestMod(b, test_step, mod_sql_guard);
    addTestMod(b, test_step, mod_crud);
    addTestMod(b, test_step, mod_schema);
    addTestMod(b, test_step, mod_export);
    addTestMod(b, test_step, mod_sql);


    // --- Check step (ZLS build-on-save, compile without linking) ---
    const check_exe = b.addExecutable(.{
        .name = "lux",
        .root_module = mod_main,
    });
    const check_step = b.step("check", "Check compilation without linking");
    check_step.dependOn(&check_exe.step);

    // --- Lint step (zlint + fmt check + JS quality) ---
    // zlint v0.7.9 build.zig uses addStaticLibrary which was removed in Zig 0.15.x.
    // Building from source as a lazy dependency is blocked until zlint publishes a
    // Zig 0.15.x-compatible build. Use the prebuilt binary via addSystemCommand instead.
    const lint_step = b.step("lint", "Run zlint, format check, and Biome");

    const run_zlint = b.addSystemCommand(&.{
        "bash", "-c", "find src/server src/handlers src/lib -name '*.zig' | zlint --stdin",
    });
    run_zlint.stdio = .inherit;
    lint_step.dependOn(&run_zlint.step);

    const fmt_check = b.addFmt(.{
        .paths = &.{"src/"},
        .check = true,
    });
    lint_step.dependOn(&fmt_check.step);

    // Biome JS/CSS lint + format check (replaces grep-based JS checks)
    // Biome is installed as a local npm dev dependency (node_modules/.bin/biome).
    // npx resolves it without requiring a global install or sudo.
    const run_biome = b.addSystemCommand(&.{ "npx", "biome", "check", "src/static/" });
    run_biome.stdio = .inherit;
    lint_step.dependOn(&run_biome.step);
}

/// Add a test compilation for a module.
fn addTestMod(
    b: *std.Build,
    test_step: *std.Build.Step,
    module: *std.Build.Module,
) void {
    const unit_test = b.addTest(.{
        .root_module = module,
    });
    const run_test = b.addRunArtifact(unit_test);
    test_step.dependOn(&run_test.step);
}
