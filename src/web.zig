// HTTP server powered by karlseguin/http.zig
const std = @import("std");
const builtin = @import("builtin");
const httpz = @import("httpz");
const pg = @import("pg");
const postgres = @import("postgres");
const utils = @import("utils");
const crud = @import("crud");
const schema_mod = @import("schema");
const export_mod = @import("export");
const sql_mod = @import("sql");

const log = std.log.scoped(.web);

const index_html = @embedFile("static/index.html");

const StaticResult = struct { data: []const u8, is_heap: bool };

fn readStaticFile(allocator: std.mem.Allocator, disk_path: []const u8, embedded: []const u8) StaticResult {
    if (comptime builtin.mode != .Debug) return .{ .data = embedded, .is_heap = false };
    const data = std.fs.cwd().readFileAlloc(allocator, disk_path, 4 * 1024 * 1024) catch |err| {
        log.warn("dev: could not read '{s}' ({s}), using embedded", .{ disk_path, @errorName(err) });
        return .{ .data = embedded, .is_heap = false };
    };
    return .{ .data = data, .is_heap = true };
}

const static_files = .{
    .{ "/css/variables.css", "text/css", "src/static/css/variables.css", @embedFile("static/css/variables.css") },
    .{ "/css/base.css", "text/css", "src/static/css/base.css", @embedFile("static/css/base.css") },
    .{ "/css/layout.css", "text/css", "src/static/css/layout.css", @embedFile("static/css/layout.css") },
    .{ "/css/sidebar.css", "text/css", "src/static/css/sidebar.css", @embedFile("static/css/sidebar.css") },
    .{ "/css/toolbar.css", "text/css", "src/static/css/toolbar.css", @embedFile("static/css/toolbar.css") },
    .{ "/css/grid.css", "text/css", "src/static/css/grid.css", @embedFile("static/css/grid.css") },
    .{ "/css/pagination.css", "text/css", "src/static/css/pagination.css", @embedFile("static/css/pagination.css") },
    .{ "/css/sql-editor.css", "text/css", "src/static/css/sql-editor.css", @embedFile("static/css/sql-editor.css") },
    .{ "/css/er-diagram.css", "text/css", "src/static/css/er-diagram.css", @embedFile("static/css/er-diagram.css") },
    .{ "/css/journal.css", "text/css", "src/static/css/journal.css", @embedFile("static/css/journal.css") },
    .{ "/css/status-bar.css", "text/css", "src/static/css/status-bar.css", @embedFile("static/css/status-bar.css") },
    .{ "/css/modals.css", "text/css", "src/static/css/modals.css", @embedFile("static/css/modals.css") },
    .{ "/css/toast.css", "text/css", "src/static/css/toast.css", @embedFile("static/css/toast.css") },
    .{ "/css/overlays.css", "text/css", "src/static/css/overlays.css", @embedFile("static/css/overlays.css") },
    .{ "/css/utilities.css", "text/css", "src/static/css/utilities.css", @embedFile("static/css/utilities.css") },
    .{ "/js/state.js", "application/javascript", "src/static/js/state.js", @embedFile("static/js/state.js") },
    .{ "/js/utils.js", "application/javascript", "src/static/js/utils.js", @embedFile("static/js/utils.js") },
    .{ "/js/connection.js", "application/javascript", "src/static/js/connection.js", @embedFile("static/js/connection.js") },
    .{ "/js/sql-tabs.js", "application/javascript", "src/static/js/sql-tabs.js", @embedFile("static/js/sql-tabs.js") },
    .{ "/js/sql.js", "application/javascript", "src/static/js/sql.js", @embedFile("static/js/sql.js") },
    .{ "/js/saved-queries.js", "application/javascript", "src/static/js/saved-queries.js", @embedFile("static/js/saved-queries.js") },
    .{ "/js/cmd-palette.js", "application/javascript", "src/static/js/cmd-palette.js", @embedFile("static/js/cmd-palette.js") },
    .{ "/js/table-create.js", "application/javascript", "src/static/js/table-create.js", @embedFile("static/js/table-create.js") },
    .{ "/js/table-ops.js", "application/javascript", "src/static/js/table-ops.js", @embedFile("static/js/table-ops.js") },
    .{ "/js/app.js", "application/javascript", "src/static/js/app.js", @embedFile("static/js/app.js") },
    .{ "/js/grid.js", "application/javascript", "src/static/js/grid.js", @embedFile("static/js/grid.js") },
    .{ "/js/sidebar.js", "application/javascript", "src/static/js/sidebar.js", @embedFile("static/js/sidebar.js") },
    .{ "/js/crud.js", "application/javascript", "src/static/js/crud.js", @embedFile("static/js/crud.js") },
};

pub fn isValidPath(path: []const u8) bool {
    if (path.len == 0) return false;
    for (path) |byte| {
        if (byte < 0x20 or byte > 0x7E) return false;
    }
    return true;
}

pub const HandlerError = error{
    OutOfMemory,
    ReadOnlyMode,
    NoConnection,
    MissingContentLength,
    PayloadTooLarge,
    MalformedRequest,
    MissingField,
    InvalidData,
    BrokenPipe,
    ConnectionResetByPeer,
} || postgres.PgError;

pub const ChangeEntry = struct {
    id: u64,
    timestamp: i64,
    table_name: []const u8,
    operation: []const u8, // "update", "delete", "insert"
    column_name: []const u8,
    old_value: []const u8,
    new_value: []const u8,
    pk_column: []const u8,
    pk_value: []const u8,
    undone: bool,
};

pub const max_journal_entries = 10000;
pub const max_history_entries = 500;

comptime {
    std.debug.assert(max_journal_entries >= max_history_entries);
    std.debug.assert(max_request_size >= 4096);
    std.debug.assert(max_request_size <= 65536);
    std.debug.assert(max_journal_entries <= 100000);
}

pub const QueryHistoryEntry = struct {
    sql: []const u8,
    timestamp: i64,
    duration_ms: u64,
    row_count: ?usize,
    is_error: bool,
    error_msg: ?[]const u8,
};

pub const ServerState = struct {
    pub const Flags = packed struct {
        read_only: bool = false,
        _padding: u7 = 0,
    };

    allocator: std.mem.Allocator,
    pool: ?*pg.Pool = null,
    conninfo_uri: ?[]const u8 = null,
    schema_tables: ?[]postgres.TableInfo = null,
    schema_text: ?[]u8 = null,
    schema_arena: ?std.heap.ArenaAllocator = null,
    enhanced_schema: ?[]postgres.EnhancedTableInfo = null,
    enhanced_arena: ?std.heap.ArenaAllocator = null,
    flags: Flags = .{},
    change_journal: std.ArrayList(ChangeEntry) = .{},
    next_journal_id: u64 = 1,
    query_history: std.ArrayList(QueryHistoryEntry) = .{},
    last_conninfo: ?[]const u8 = null,
    next_connection_id: u64 = 1,
    /// Stored for CSRF origin checks, not for binding.
    port: u16 = 8080,

    pub fn init(allocator: std.mem.Allocator) ServerState {
        return .{
            .allocator = allocator,
            .change_journal = .{},
            .query_history = .{},
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*ServerState {
        const self = try allocator.create(ServerState);
        self.* = init(allocator);
        return self;
    }

    pub fn hasDbConnection(self: *const ServerState) bool {
        return self.pool != null;
    }

    pub fn deinit(self: *ServerState) void {
        for (self.change_journal.items) |entry| {
            self.allocator.free(entry.table_name);
            self.allocator.free(entry.operation);
            self.allocator.free(entry.column_name);
            self.allocator.free(entry.old_value);
            self.allocator.free(entry.new_value);
            self.allocator.free(entry.pk_column);
            self.allocator.free(entry.pk_value);
        }
        self.change_journal.deinit(self.allocator);
        for (self.query_history.items) |entry| {
            self.allocator.free(entry.sql);
            if (entry.error_msg) |e| self.allocator.free(e);
        }
        self.query_history.deinit(self.allocator);
        if (self.pool) |p| p.deinit();
        if (self.conninfo_uri) |old| self.allocator.free(@constCast(old));
        if (self.schema_text) |old| self.allocator.free(old);
        if (self.schema_arena) |*a| a.deinit();
        if (self.enhanced_arena) |*a| a.deinit();
        if (self.last_conninfo) |old| self.allocator.free(@constCast(old));
        self.* = undefined;
    }

    pub fn destroy(self: *ServerState, allocator: std.mem.Allocator) void {
        self.deinit();
        allocator.destroy(self);
    }
};

// --- httpz Handler ---

pub const Handler = struct {
    state: *ServerState,

    /// CSRF: block cross-origin POST/DELETE
    pub fn dispatch(self: *Handler, action: httpz.Action(*Handler), req: *httpz.Request, res: *httpz.Response) !void {
        const method = req.method;
        if (method == .POST or method == .DELETE) {
            if (!checkOriginHttpz(req, self.state.port)) {
                res.status = 403;
                res.content_type = httpz.ContentType.TEXT;
                res.body = "Forbidden: cross-origin request";
                return;
            }
        }
        try action(self, req, res);
    }

    pub fn notFound(_: *Handler, _: *httpz.Request, res: *httpz.Response) !void {
        res.status = 404;
        res.content_type = httpz.ContentType.TEXT;
        res.body = "Not Found";
    }

    pub fn uncaughtError(_: *Handler, req: *httpz.Request, res: *httpz.Response, err: anyerror) void {
        log.warn("unhandled error at {s}: {s}", .{ req.url.path, @errorName(err) });
        const status_and_body = mapHandlerError(err);
        res.status = status_and_body[0];
        res.content_type = httpz.ContentType.JSON;
        res.body = status_and_body[1];
    }
};

fn mapHandlerError(err: anyerror) struct { u16, []const u8 } {
    return switch (err) {
        error.OutOfMemory => .{ 500, "{\"error\":\"Out of memory\"}" },
        error.ConnectionFailed => .{ 200, "{\"error\":\"Database connection failed\"}" },
        error.QueryFailed => .{ 200, "{\"error\":\"Query execution failed\"}" },
        error.NoResults => .{ 200, "{\"error\":\"No results\"}" },
        error.InvalidData => .{ 400, "{\"error\":\"Invalid data\"}" },
        error.ReadOnlyMode => .{ 403, "{\"error\":\"Read-only mode is enabled\"}" },
        error.NoConnection => .{ 200, "{\"error\":\"No database connected\"}" },
        error.MissingContentLength => .{ 400, "{\"error\":\"Missing Content-Length\"}" },
        error.PayloadTooLarge => .{ 413, "{\"error\":\"Request too large\"}" },
        error.MalformedRequest => .{ 400, "{\"error\":\"Malformed request\"}" },
        error.MissingField => .{ 400, "{\"error\":\"Missing required field\"}" },
        else => .{ 500, "{\"error\":\"Internal server error\"}" },
    };
}

/// Absent Origin is allowed because same-origin requests may omit it.
fn checkOriginHttpz(req: *httpz.Request, port: u16) bool {
    const origin = req.header("origin") orelse return true;
    var buf_127: [64]u8 = undefined;
    var buf_local: [64]u8 = undefined;
    const expected_127 = std.fmt.bufPrint(&buf_127, "http://127.0.0.1:{d}", .{port}) catch return false;
    const expected_local = std.fmt.bufPrint(&buf_local, "http://localhost:{d}", .{port}) catch return false;
    if (std.mem.eql(u8, origin, expected_127)) return true;
    if (std.mem.eql(u8, origin, expected_local)) return true;
    return false;
}

// --- Static file handlers ---

fn serveIndex(_: *Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const r = readStaticFile(res.arena, "src/static/index.html", index_html);
    res.content_type = httpz.ContentType.HTML;
    res.body = r.data;
    res.header("content-security-policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'");
    res.header("x-content-type-options", "nosniff");
    res.header("referrer-policy", "no-referrer");
    res.header("cache-control", "no-store");
}

fn serveFavicon(_: *Handler, _: *httpz.Request, res: *httpz.Response) !void {
    res.status = 204;
    res.content_type = httpz.ContentType.ICO;
}

fn serveStaticFile(_: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const path = req.url.path;
    inline for (static_files) |entry| {
        if (std.mem.eql(u8, path, entry[0])) {
            const r = readStaticFile(res.arena, entry[2], entry[3]);
            res.body = r.data;
            // Map content type string to httpz enum
            if (std.mem.eql(u8, entry[1], "text/css")) {
                res.content_type = httpz.ContentType.CSS;
            } else {
                res.content_type = httpz.ContentType.JS;
            }
            res.header("cache-control", "no-store");
            res.header("x-content-type-options", "nosniff");
            res.header("referrer-policy", "no-referrer");
            return;
        }
    }
    res.status = 404;
    res.content_type = httpz.ContentType.TEXT;
    res.body = "Not Found";
}

// --- Server lifecycle ---

const max_request_size = 8192;

var server_instance: ?*httpz.Server(*Handler) = null;

fn handleSignal(_: c_int) callconv(.c) void {
    if (server_instance) |s| {
        server_instance = null;
        s.stop();
    }
}

pub fn serve(state: *ServerState, port: u16, bind_addr: []const u8) !void {
    state.port = port;

    var handler = Handler{ .state = state };

    const address: httpz.Config.AddressConfig = blk: {
        if (std.mem.eql(u8, bind_addr, "0.0.0.0")) {
            break :blk httpz.Config.AddressConfig.all(port);
        } else if (std.mem.eql(u8, bind_addr, "127.0.0.1")) {
            break :blk httpz.Config.AddressConfig.localhost(port);
        } else {
            break :blk .{ .ip = .{ .host = bind_addr, .port = port } };
        }
    };

    var server = try httpz.Server(*Handler).init(state.allocator, .{
        .address = address,
        .request = .{ .max_body_size = 65536 },
    }, &handler);
    defer server.deinit();

    var sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    server_instance = &server;

    var router = try server.router(.{});

    // Static routes
    router.get("/", serveIndex, .{});
    router.get("/favicon.ico", serveFavicon, .{});
    inline for (static_files) |entry| {
        router.get(entry[0], serveStaticFile, .{});
    }

    // API routes (exact match)
    router.get("/api/schema", schema_mod.handleSchema, .{});
    router.get("/api/settings/read-only", schema_mod.handleReadOnlyGet, .{});
    router.get("/api/connections", schema_mod.handleGetConnections, .{});
    router.get("/api/history", sql_mod.handleHistory, .{});
    router.get("/api/journal", sql_mod.handleJournal, .{});
    router.get("/api/health", schema_mod.handleHealthCheck, .{});
    router.post("/api/connect", schema_mod.handleConnect, .{});
    router.post("/api/sql", sql_mod.handleSql, .{});
    router.post("/api/settings/read-only", schema_mod.handleReadOnlyToggle, .{});
    router.post("/api/connections", schema_mod.handlePostConnection, .{});
    router.post("/api/journal/undo", sql_mod.handleJournalUndo, .{});
    router.post("/api/sql/schema-preview", sql_mod.handleSchemaPreview, .{});
    router.post("/api/sql/preview", sql_mod.handleSqlPreview, .{});
    router.post("/api/sql/export", export_mod.handleSqlExport, .{});
    router.post("/api/update", crud.handleUpdate, .{});
    router.post("/api/delete-row", crud.handleDeleteRow, .{});
    router.post("/api/insert-row", crud.handleInsertRow, .{});
    router.post("/api/reconnect", schema_mod.handleReconnect, .{});

    // Table routes (suffix-based)
    router.get("/api/tables/:table_path/fk-lookup", crud.handleFkLookup, .{});
    router.get("/api/tables/:table_path/ddl", export_mod.handleTableDdl, .{});
    router.get("/api/tables/:table_path/stats", export_mod.handleTableStats, .{});
    router.post("/api/tables/:table_path/bulk-update", crud.handleBulkUpdate, .{});
    router.post("/api/tables/:table_path/import", export_mod.handleCsvImport, .{});
    router.post("/api/tables/:table_path/truncate", crud.handleTruncateTable, .{});
    router.get("/api/tables/:table_path/data", crud.handleTableData, .{});

    router.get("/api/export/:table_name", export_mod.handleExport, .{});
    router.delete("/api/connections/:id", schema_mod.handleDeleteConnection, .{});

    log.info("Lux web UI running at http://{s}:{d}", .{ bind_addr, port });
    log.info("open this URL in your browser, press Ctrl-C to stop", .{});
    if (comptime builtin.mode == .Debug) {
        log.warn("DEV MODE: static assets served from disk (src/static/), not embedded", .{});
        log.warn("edit CSS/JS/HTML and refresh browser - no rebuild needed", .{});
    }

    defer {
        server.stop();
        log.info("shutting down", .{});
    }
    try server.listen();
}

test "ServerState: init defaults" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(state.schema_text == null);
    try std.testing.expect(!state.flags.read_only);
}

test "ServerState.Flags: packed struct has read_only field" {
    var flags = ServerState.Flags{};
    try std.testing.expect(!flags.read_only);
    flags.read_only = true;
    try std.testing.expect(flags.read_only);
}

test "ServerState.Flags: default is all false" {
    const flags = ServerState.Flags{};
    try std.testing.expect(!flags.read_only);
}

test "ServerState: flags.read_only works like old read_only" {
    var state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.flags.read_only);
    state.flags.read_only = true;
    try std.testing.expect(state.flags.read_only);
}

test "readStaticFile: disk read succeeds returns is_heap=true" {
    if (comptime builtin.mode != .Debug) return;

    const allocator = std.testing.allocator;
    const content = "hello from disk";

    const tmp_path = "/tmp/lux_test_static_file.txt";
    {
        const f = try std.fs.createFileAbsolute(tmp_path, .{});
        defer f.close();
        try f.writeAll(content);
    }
    defer std.fs.deleteFileAbsolute(tmp_path) catch {};

    const embedded: []const u8 = "embedded fallback";
    const result = readStaticFile(allocator, tmp_path, embedded);
    defer if (result.is_heap) allocator.free(result.data);

    try std.testing.expect(result.is_heap);
    try std.testing.expectEqualStrings(content, result.data);
}

test "readStaticFile: missing disk file falls back to embedded" {
    if (comptime builtin.mode != .Debug) return;

    const allocator = std.testing.allocator;
    const embedded: []const u8 = "embedded fallback";

    const result = readStaticFile(allocator, "/tmp/lux_nonexistent_file_xyz.txt", embedded);
    defer if (result.is_heap) allocator.free(result.data);

    try std.testing.expect(!result.is_heap);
    try std.testing.expectEqualStrings(embedded, result.data);
}

test "ServerState: create returns heap-allocated state" {
    const state = try ServerState.create(std.testing.allocator);
    defer state.destroy(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(!state.flags.read_only);
}

test "ServerState: deinit cleans up empty state without crash" {
    var state = ServerState.init(std.testing.allocator);
    state.deinit();
}

test "isValidPath: valid API path" {
    try std.testing.expect(isValidPath("/api/tables/users"));
}

test "isValidPath: valid JS path" {
    try std.testing.expect(isValidPath("/js/app.js"));
}

test "isValidPath: empty path is invalid" {
    try std.testing.expect(!isValidPath(""));
}

test "isValidPath: null byte is invalid" {
    try std.testing.expect(!isValidPath("/api/tables/users\x00"));
}

test "isValidPath: control char 0x01 is invalid" {
    try std.testing.expect(!isValidPath("/api/tables/users\x01"));
}

test "isValidPath: DEL 0x7F is invalid" {
    try std.testing.expect(!isValidPath("/api/tables/users\x7F"));
}

test "isValidPath: root path is valid" {
    try std.testing.expect(isValidPath("/"));
}

test "isValidPath: space 0x20 is valid" {
    try std.testing.expect(isValidPath("/api/search?q=hello world"));
}

test "static_files tuple: has 28 JS/CSS entries" {
    try std.testing.expectEqual(@as(usize, 28), static_files.len);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
