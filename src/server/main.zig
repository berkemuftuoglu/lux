const std = @import("std");
const web = @import("web");
const postgres = @import("postgres");

pub const std_options: std.Options = .{
    .log_level = .info,
};

const log = std.log.scoped(.main);

pub fn main() !void {
    const stdout = std.fs.File.stdout().deprecatedWriter();

    var args = std.process.args();
    _ = args.next();

    var port: u16 = 8080;
    var bind_addr: []const u8 = "127.0.0.1";
    var pg_conninfo: ?[]const u8 = null;

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
        } else if (std.mem.eql(u8, arg, "--pg")) {
            pg_conninfo = args.next() orelse {
                log.err("--pg requires a connection string", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
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

    if (pg_conninfo) |conninfo| {
        log.info("connecting to PostgreSQL", .{});

        const conninfo_z = allocator.dupeZ(u8, conninfo) catch {
            log.err("out of memory", .{});
            std.process.exit(1);
        };
        state.conninfo_z = conninfo_z;

        var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
            log.err("failed to connect to PostgreSQL", .{});
            std.process.exit(1);
        };
        defer pg_conn.deinit();

        log.info("connected to PostgreSQL", .{});

        var schema = pg_conn.fetchSchema(allocator) catch {
            log.err("failed to fetch schema", .{});
            std.process.exit(1);
        };

        const schema_text = schema.format(allocator) catch {
            log.err("failed to format schema", .{});
            std.process.exit(1);
        };
        state.schema_text = schema_text;
        state.schema_tables = schema.tables;
        schema.tables = &.{}; // prevent schema.deinit from freeing tables we now own
        schema.deinit();

        var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;
        if (enhanced) |*es| {
            state.enhanced_schema = es.tables;
            es.tables = &.{};
            es.deinit();
        }

        try stdout.print("{s}", .{schema_text});
    }

    try web.serve(&state, port, bind_addr);
}

fn printUsage(writer: anytype) !void {
    try writer.print(
        \\Lux — PostgreSQL Web Client
        \\
        \\Usage:
        \\  lux                                       Start web UI
        \\  lux --pg <connstr>                        Start with auto-connect
        \\
        \\Options:
        \\  -p, --port <num>        Port for web UI (default: 8080)
        \\  -b, --bind <addr>       Bind address (default: 127.0.0.1)
        \\  --pg <connstr>          PostgreSQL connection string
        \\  -h, --help              Show this help
        \\
        \\Examples:
        \\  lux
        \\  lux --pg "postgresql://user:pass@localhost/mydb"
        \\  lux -p 3000 --pg "postgresql://localhost/mydb"
        \\
    , .{});
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
