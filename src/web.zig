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
const cache_mod = @import("cache");
const logz = @import("logz");

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
    .{ "/css/styles.css", "text/css", "src/static/css/styles.css", @embedFile("static/css/styles.css") },
    .{ "/js/state.js", "application/javascript", "src/static/js/state.js", @embedFile("static/js/state.js") },
    .{ "/js/utils.js", "application/javascript", "src/static/js/utils.js", @embedFile("static/js/utils.js") },
    .{ "/js/connection.js", "application/javascript", "src/static/js/connection.js", @embedFile("static/js/connection.js") },
    .{ "/js/sql-tabs.js", "application/javascript", "src/static/js/sql-tabs.js", @embedFile("static/js/sql-tabs.js") },
    .{ "/js/sql-format.js", "application/javascript", "src/static/js/sql-format.js", @embedFile("static/js/sql-format.js") },
    .{ "/js/sql.js", "application/javascript", "src/static/js/sql.js", @embedFile("static/js/sql.js") },
    .{ "/js/saved-queries.js", "application/javascript", "src/static/js/saved-queries.js", @embedFile("static/js/saved-queries.js") },
    .{ "/js/cmd-palette.js", "application/javascript", "src/static/js/cmd-palette.js", @embedFile("static/js/cmd-palette.js") },
    .{ "/js/table-create.js", "application/javascript", "src/static/js/table-create.js", @embedFile("static/js/table-create.js") },
    .{ "/js/table-ops.js", "application/javascript", "src/static/js/table-ops.js", @embedFile("static/js/table-ops.js") },
    .{ "/js/app.js", "application/javascript", "src/static/js/app.js", @embedFile("static/js/app.js") },
    .{ "/js/grid.js", "application/javascript", "src/static/js/grid.js", @embedFile("static/js/grid.js") },
    .{ "/js/sidebar.js", "application/javascript", "src/static/js/sidebar.js", @embedFile("static/js/sidebar.js") },
    .{ "/js/er-diagram.js", "application/javascript", "src/static/js/er-diagram.js", @embedFile("static/js/er-diagram.js") },
    .{ "/js/crud.js", "application/javascript", "src/static/js/crud.js", @embedFile("static/js/crud.js") },
};

pub fn sendJsonResponse(res: *httpz.Response, body: []const u8) void {
    res.content_type = httpz.ContentType.JSON;
    res.body = body;
}

pub fn sendJsonError(res: *httpz.Response, status: u16, body: []const u8) void {
    res.status = status;
    res.content_type = httpz.ContentType.JSON;
    res.body = body;
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
    schema_cache: cache_mod.Cache([]u8),
    ws_clients: std.ArrayList(*httpz.websocket.Conn),
    ws_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator) !ServerState {
        const sc = try cache_mod.Cache([]u8).init(allocator, .{ .max_size = 10, .segment_count = 1 });
        return .{
            .allocator = allocator,
            .change_journal = .{},
            .query_history = .{},
            .schema_cache = sc,
            .ws_clients = .{},
            .ws_mutex = .{},
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*ServerState {
        const self = try allocator.create(ServerState);
        self.* = try init(allocator);
        return self;
    }

    pub fn hasDbConnection(self: *const ServerState) bool {
        return self.pool != null;
    }

    /// Broadcast a message to all connected WebSocket clients.
    pub fn broadcastAll(self: *ServerState, msg: []const u8) void {
        self.ws_mutex.lock();
        defer self.ws_mutex.unlock();
        for (self.ws_clients.items) |conn| {
            conn.write(msg) catch {};
        }
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
        if (self.schema_arena) |*a| a.deinit();
        if (self.enhanced_arena) |*a| a.deinit();
        if (self.last_conninfo) |old| self.allocator.free(@constCast(old));
        self.schema_cache.deinit();
        self.ws_clients.deinit(self.allocator);
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

    pub const WebsocketHandler = WsClient;

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

pub const WsClient = struct {
    conn: *httpz.websocket.Conn,

    pub fn init(conn: *httpz.websocket.Conn, _: *const void) !WsClient {
        return .{ .conn = conn };
    }

    pub fn afterInit(self: *WsClient) !void {
        try self.conn.write("{\"type\":\"connected\"}");
    }

    pub fn clientMessage(_: *WsClient, _: []const u8) !void {}

    pub fn close(_: *WsClient) void {}
};

fn handleWs(_: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const ctx: void = {};
    if (try httpz.upgradeWebsocket(WsClient, req, res, &ctx) == false) {
        res.status = 400;
        res.body = "invalid websocket upgrade";
    }
    // Do NOT use req or res after this point
}

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

    // WebSocket upgrade endpoint
    router.get("/ws", handleWs, .{});

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
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();
    try std.testing.expect(!state.hasDbConnection());
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
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();
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
    var state = try ServerState.init(std.testing.allocator);
    state.deinit();
}

test "ServerState: schema_cache put and get round-trip" {
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();
    // Use a string literal — no heap allocation needed; cache stores the slice reference
    const val: []u8 = @constCast("hello schema");
    try state.schema_cache.put("schema", val, .{ .ttl = 30 });
    const entry = state.schema_cache.get("schema") orelse return error.CacheMiss;
    defer entry.release();
    try std.testing.expectEqualStrings("hello schema", entry.value);
}

test "ServerState: schema_cache del makes get return null" {
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();
    const val: []u8 = @constCast("data");
    try state.schema_cache.put("schema", val, .{ .ttl = 30 });
    _ = state.schema_cache.del("schema");
    const entry = state.schema_cache.get("schema");
    try std.testing.expect(entry == null);
}

test "static_files tuple: has 17 JS/CSS entries" {
    // 2 CSS (variables.css + styles.css consolidated) + 15 JS files
    try std.testing.expectEqual(@as(usize, 17), static_files.len);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
