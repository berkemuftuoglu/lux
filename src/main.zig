const std = @import("std");
const web = @import("web.zig");
const postgres = @import("postgres.zig");

pub fn main() !void {
    const stderr = std.io.getStdErr().writer();
    const stdout = std.io.getStdOut().writer();

    // --- Parse CLI args ---
    var args = std.process.args();
    _ = args.next(); // skip argv[0]

    var port: u16 = 8080;
    var bind_addr: []const u8 = "127.0.0.1";
    var pg_conninfo: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--port") or std.mem.eql(u8, arg, "-p")) {
            const port_str = args.next() orelse {
                try stderr.print("Error: --port requires a port number\n", .{});
                std.process.exit(1);
            };
            port = std.fmt.parseInt(u16, port_str, 10) catch {
                try stderr.print("Error: invalid port '{s}'\n", .{port_str});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--bind") or std.mem.eql(u8, arg, "-b")) {
            bind_addr = args.next() orelse {
                try stderr.print("Error: --bind requires an address (e.g. 0.0.0.0)\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--pg")) {
            pg_conninfo = args.next() orelse {
                try stderr.print("Error: --pg requires a connection string\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            try printUsage(stdout);
            return;
        } else {
            try stderr.print("Error: unknown argument '{s}'\n", .{arg});
            try stderr.print("Run with --help for usage information.\n", .{});
            std.process.exit(1);
        }
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var state = web.ServerState.init(allocator);

    // Auto-connect to Postgres if --pg provided
    if (pg_conninfo) |conninfo| {
        try stdout.print("Connecting to PostgreSQL...\n", .{});

        const conninfo_z = allocator.allocSentinel(u8, conninfo.len, 0) catch {
            try stderr.print("Error: out of memory\n", .{});
            std.process.exit(1);
        };
        @memcpy(conninfo_z[0..conninfo.len], conninfo);
        state.conninfo_z = conninfo_z;

        // Fetch schema
        var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
            try stderr.print("Error: failed to connect to PostgreSQL\n", .{});
            std.process.exit(1);
        };
        defer pg_conn.deinit();

        try stdout.print("Connected to PostgreSQL.\n", .{});

        var schema = pg_conn.fetchSchema(allocator) catch {
            try stderr.print("Error: failed to fetch schema\n", .{});
            std.process.exit(1);
        };

        const schema_text = schema.format(allocator) catch {
            try stderr.print("Error: failed to format schema\n", .{});
            std.process.exit(1);
        };
        state.schema_text = schema_text;
        state.schema_tables = schema.tables;
        // Prevent schema.deinit from freeing tables we now own
        schema.tables = &.{};
        schema.deinit();

        // Also fetch enhanced schema (PK, FK, ENUM info)
        var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;
        if (enhanced) |*es| {
            state.enhanced_schema = es.tables;
            es.tables = &.{};
            es.deinit();
        }

        try stdout.print("{s}", .{schema_text});
    }

    try web.serve(stderr, stdout, &state, port, bind_addr);
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

// ── Tests ──────────────────────────────────────────────────────────────

test "smoke: binary compiles and links" {
    // If this test runs, the build system is working correctly.
}
