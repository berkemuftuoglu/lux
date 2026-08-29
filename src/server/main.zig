const std = @import("std");
const clap = @import("clap");
const web = @import("web");
const logz = @import("logz");

pub const std_options: std.Options = .{
    .log_level = .info,
};

const log = std.log.scoped(.main);

pub fn main() !void {
    const params = comptime clap.parseParamsComptime(
        \\-h, --help          Show this help and exit.
        \\-p, --port <u16>    Port for web UI (default: 8080).
        \\-b, --bind <str>    Bind address (default: 127.0.0.1).
        \\
    );

    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
        .allocator = std.heap.page_allocator,
    }) catch |err| {
        diag.reportToFile(.stderr(), err) catch |write_err| log.err("failed to report diagnostic: {s}", .{@errorName(write_err)});
        std.process.exit(1);
    };
    defer res.deinit();

    if (res.args.help != 0) {
        clap.helpToFile(.stdout(), clap.Help, &params, .{}) catch |err| log.err("failed to print help: {s}", .{@errorName(err)});
        return;
    }

    const port_is_explicit = res.args.port != null;
    const port: u16 = res.args.port orelse 8080;
    const bind_addr: []const u8 = res.args.bind orelse "127.0.0.1";

    var da = std.heap.DebugAllocator(.{}).init;
    defer {
        if (da.deinit() == .leak) {
            log.err("memory leak detected", .{});
            std.process.exit(1);
        }
    }
    const allocator = da.allocator();

    try logz.setup(allocator, .{
        .level = .Info,
        .pool_size = 32,
        .buffer_size = 4096,
        .encoding = .logfmt,
        .output = .stderr,
    });
    defer logz.deinit();

    var state = try web.ServerState.init(allocator);
    defer state.deinit();

    // A busy port is a user-facing condition, not a crash: the message above already
    // says what to do, so exit cleanly rather than dumping a Zig stack trace.
    web.serveOnPort(&state, port, bind_addr, port_is_explicit) catch |err| switch (err) {
        error.NoAvailablePort => std.process.exit(1),
        else => return err,
    };
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
