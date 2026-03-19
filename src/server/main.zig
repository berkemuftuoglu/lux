const std = @import("std");
const web = @import("web");

pub const std_options: std.Options = .{
    .log_level = .info,
};

const log = std.log.scoped(.main);

pub fn main() !void {
    var port: u16 = 8080;
    var bind_addr: []const u8 = "127.0.0.1";

    var args = std.process.args();
    _ = args.next();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port") or std.mem.eql(u8, arg, "-p")) {
            const port_str = args.next() orelse {
                log.err("--port requires a port number", .{});
                std.process.exit(1);
            };
            port = std.fmt.parseInt(u16, port_str, 10) catch {
                log.err("invalid port '{s}'", .{port_str});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--bind") or std.mem.eql(u8, arg, "-b")) {
            bind_addr = args.next() orelse {
                log.err("--bind requires an address (e.g. 0.0.0.0)", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            const stdout = std.fs.File.stdout().deprecatedWriter();
            try printUsage(stdout);
            return;
        } else {
            log.err("unknown argument '{s}'", .{arg});
            log.err("run with --help for usage information", .{});
            std.process.exit(1);
        }
    }

    var da = std.heap.DebugAllocator(.{}).init;
    defer {
        if (da.deinit() == .leak) {
            log.err("memory leak detected", .{});
            std.process.exit(1);
        }
    }
    const allocator = da.allocator();

    var state = web.ServerState.init(allocator);
    defer state.deinit();

    try web.serve(&state, port, bind_addr);
}

fn printUsage(writer: anytype) !void {
    try writer.print(
        \\Lux — PostgreSQL Web Client
        \\
        \\Usage:
        \\  lux                             Start web UI (connect via browser)
        \\
        \\Options:
        \\  -p, --port <num>        Port for web UI (default: 8080)
        \\  -b, --bind <addr>       Bind address (default: 127.0.0.1)
        \\  -h, --help              Show this help
        \\
        \\Examples:
        \\  lux
        \\  lux -p 3000
        \\
    , .{});
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
