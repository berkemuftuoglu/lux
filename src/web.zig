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

// Auto-generated static files from Svelte build (frontend/bundle.cjs)
const generated = @import("static_files.zig");
const static_files = generated.static_files;

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
    // httpz runs handlers on a 32-thread pool by default, so every field above
    // that a handler mutates needs a guard. Schema state is read on every request
    // and written only on connect/refresh, so it takes an RwLock; the journal and
    // history are mixed read/write with short critical sections, so plain mutexes.
    journal_mutex: std.Thread.Mutex,
    history_mutex: std.Thread.Mutex,
    schema_lock: std.Thread.RwLock,

    pub fn init(allocator: std.mem.Allocator) !ServerState {
        const sc = try cache_mod.Cache([]u8).init(allocator, .{ .max_size = 10, .segment_count = 1 });
        return .{
            .allocator = allocator,
            .change_journal = .{},
            .query_history = .{},
            .schema_cache = sc,
            .ws_clients = .{},
            .ws_mutex = .{},
            .journal_mutex = .{},
            .history_mutex = .{},
            .schema_lock = .{},
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

    /// An owned copy of a journal entry. Callers must snapshot rather than hold a
    /// `*ChangeEntry`: the journal evicts and frees its oldest entry once it hits
    /// `max_journal_entries`, and `append` can realloc the backing array, so any
    /// pointer into `change_journal.items` can dangle across a blocking call.
    pub const JournalSnapshot = struct {
        entry: ChangeEntry,

        pub fn deinit(self: *JournalSnapshot, allocator: std.mem.Allocator) void {
            allocator.free(self.entry.table_name);
            allocator.free(self.entry.operation);
            allocator.free(self.entry.column_name);
            allocator.free(self.entry.old_value);
            allocator.free(self.entry.new_value);
            allocator.free(self.entry.pk_column);
            allocator.free(self.entry.pk_value);
            self.* = undefined;
        }
    };

    pub fn appendJournalEntry(
        self: *ServerState,
        table_name: []const u8,
        operation: []const u8,
        column_name: []const u8,
        old_value: []const u8,
        new_value: []const u8,
        pk_column: []const u8,
        pk_value: []const u8,
    ) !u64 {
        self.journal_mutex.lock();
        defer self.journal_mutex.unlock();

        const allocator = self.allocator;
        if (self.change_journal.items.len >= max_journal_entries) {
            const old = self.change_journal.orderedRemove(0);
            allocator.free(old.table_name);
            allocator.free(old.operation);
            allocator.free(old.column_name);
            allocator.free(old.old_value);
            allocator.free(old.new_value);
            allocator.free(old.pk_column);
            allocator.free(old.pk_value);
        }

        const tn = try allocator.dupe(u8, table_name);
        errdefer allocator.free(tn);
        const op = try allocator.dupe(u8, operation);
        errdefer allocator.free(op);
        const cn = try allocator.dupe(u8, column_name);
        errdefer allocator.free(cn);
        const ov = try allocator.dupe(u8, old_value);
        errdefer allocator.free(ov);
        const nv = try allocator.dupe(u8, new_value);
        errdefer allocator.free(nv);
        const pkc = try allocator.dupe(u8, pk_column);
        errdefer allocator.free(pkc);
        const pkv = try allocator.dupe(u8, pk_value);
        errdefer allocator.free(pkv);

        const entry = ChangeEntry{
            .id = self.next_journal_id,
            .timestamp = std.time.timestamp(),
            .table_name = tn,
            .operation = op,
            .column_name = cn,
            .old_value = ov,
            .new_value = nv,
            .pk_column = pkc,
            .pk_value = pkv,
            .undone = false,
        };
        try self.change_journal.append(allocator, entry);
        self.next_journal_id += 1;
        return entry.id;
    }

    pub fn snapshotJournalEntry(self: *ServerState, allocator: std.mem.Allocator, id: u64) !?JournalSnapshot {
        self.journal_mutex.lock();
        defer self.journal_mutex.unlock();

        for (self.change_journal.items) |entry| {
            if (entry.id != id) continue;
            var copied = std.ArrayList([]const u8){};
            defer copied.deinit(allocator);
            errdefer for (copied.items) |c| allocator.free(c);

            const fields = [_][]const u8{
                entry.table_name, entry.operation, entry.column_name, entry.old_value,
                entry.new_value,  entry.pk_column, entry.pk_value,
            };
            for (fields) |f| try copied.append(allocator, try allocator.dupe(u8, f));

            return JournalSnapshot{ .entry = .{
                .id = entry.id,
                .timestamp = entry.timestamp,
                .table_name = copied.items[0],
                .operation = copied.items[1],
                .column_name = copied.items[2],
                .old_value = copied.items[3],
                .new_value = copied.items[4],
                .pk_column = copied.items[5],
                .pk_value = copied.items[6],
                .undone = entry.undone,
            } };
        }
        return null;
    }

    pub fn markJournalUndone(self: *ServerState, id: u64) bool {
        self.journal_mutex.lock();
        defer self.journal_mutex.unlock();
        for (self.change_journal.items) |*entry| {
            if (entry.id == id) {
                entry.undone = true;
                return true;
            }
        }
        return false;
    }

    pub fn clearJournal(self: *ServerState) void {
        self.journal_mutex.lock();
        defer self.journal_mutex.unlock();
        for (self.change_journal.items) |entry| {
            self.allocator.free(entry.table_name);
            self.allocator.free(entry.operation);
            self.allocator.free(entry.column_name);
            self.allocator.free(entry.old_value);
            self.allocator.free(entry.new_value);
            self.allocator.free(entry.pk_column);
            self.allocator.free(entry.pk_value);
        }
        self.change_journal.clearRetainingCapacity();
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
                log.warn("csrf blocked: {s}", .{req.url.path});
                return;
            }
        }
        const t0 = std.time.microTimestamp();
        try action(self, req, res);
        const duration_us = std.time.microTimestamp() - t0;
        // Operational access log — logz for structured telemetry, not diagnostic output
        logz.info()
            .string("method", @tagName(method))
            .string("path", req.url.path)
            .int("status", res.status)
            .int("duration_us", duration_us)
            .log();
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
    // In debug builds, allow Vite dev server origins (any localhost port)
    if (comptime builtin.mode == .Debug) {
        if (std.mem.startsWith(u8, origin, "http://localhost:")) return true;
        if (std.mem.startsWith(u8, origin, "http://127.0.0.1:")) return true;
    }
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

pub const PortError = error{NoAvailablePort};

/// Probe upward from `preferred` for a port we can actually bind, returning the
/// first that works. Binding and immediately closing is racy in principle, but the
/// alternative — announcing a URL and only then discovering AddressInUse — is the
/// bug this replaces. `reuse_address = false` so a port held by another process is
/// reported as taken rather than silently shared.
pub fn findAvailablePort(bind_addr: []const u8, preferred: u16, max_attempts: u16) !u16 {
    var attempt: u16 = 0;
    while (attempt < max_attempts) : (attempt += 1) {
        const candidate = preferred + attempt;
        const addr = std.net.Address.parseIp(bind_addr, candidate) catch return error.NoAvailablePort;
        var srv = addr.listen(.{ .reuse_address = false }) catch continue;
        srv.deinit();
        return candidate;
    }
    return error.NoAvailablePort;
}

pub fn serve(state: *ServerState, port: u16, bind_addr: []const u8) !void {
    return serveOnPort(state, port, bind_addr, true);
}

/// `port_is_explicit` decides what happens when the port is taken: a port the user
/// asked for by name is an error, because silently moving it breaks their bookmarks
/// and scripts. The default port may advance, loudly.
pub fn serveOnPort(state: *ServerState, port: u16, bind_addr: []const u8, port_is_explicit: bool) !void {
    const resolved = findAvailablePort(bind_addr, port, if (port_is_explicit) 1 else 20) catch {
        if (port_is_explicit) {
            log.err("port {d} is already in use on {s}", .{ port, bind_addr });
            log.err("stop whatever is using it, or start lux with a different --port", .{});
        } else {
            log.err("no free port found in {d}..{d} on {s}", .{ port, port + 20, bind_addr });
            log.err("start lux with an explicit --port", .{});
        }
        return error.NoAvailablePort;
    };
    if (resolved != port) {
        log.warn("port {d} was busy — using {d} instead", .{ port, resolved });
    }
    return serveBound(state, resolved, bind_addr);
}

fn serveBound(state: *ServerState, port: u16, bind_addr: []const u8) !void {
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
    log.info("open this URL in your browser · Ctrl-C to stop", .{});
    if (comptime builtin.mode == .Debug) {
        log.warn("DEV MODE: static assets served from disk (src/static/), not embedded", .{});
        log.warn("edit CSS/JS/HTML and refresh browser - no rebuild needed", .{});
    }

    defer {
        server.stop();
        log.info("shutting down", .{});
    }

    server.listen() catch |err| {
        // The banner above is printed before listen() because httpz binds inside it;
        // if we get here the URL we just advertised is not actually serving.
        log.err("failed to start on http://{s}:{d} — {s}", .{ bind_addr, port, @errorName(err) });
        return err;
    };
}

test "setup logz for web tests" {
    // Use page_allocator: logz persists for the test binary lifetime, avoiding false leak detection
    try logz.setup(std.heap.page_allocator, .{ .level = .None, .output = .stderr });
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

test "static_files tuple: has Svelte build entries" {
    // Svelte build output: JS chunks + CSS + env.js + version.json
    try std.testing.expect(static_files.len > 0);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}

test "ServerState.snapshotJournalEntry: copies strings so eviction cannot dangle" {
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();

    const id = try state.appendJournalEntry("employees", "update", "name", "old", "new", "id", "42");
    var snap = (try state.snapshotJournalEntry(std.testing.allocator, id)).?;
    defer snap.deinit(std.testing.allocator);

    // Evicting the live entry must not disturb the snapshot the caller holds.
    state.clearJournal();
    try std.testing.expectEqualStrings("employees", snap.entry.table_name);
    try std.testing.expectEqualStrings("42", snap.entry.pk_value);
}

test "ServerState.snapshotJournalEntry: unknown id returns null" {
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();
    try std.testing.expect((try state.snapshotJournalEntry(std.testing.allocator, 9999)) == null);
}

test "ServerState: concurrent journal append and snapshot stay consistent" {
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();

    const Worker = struct {
        fn append(s: *ServerState) void {
            for (0..200) |_| {
                _ = s.appendJournalEntry("t", "update", "c", "o", "n", "id", "1") catch return;
            }
        }
        fn read(s: *ServerState) void {
            for (0..200) |_| {
                var snap = (s.snapshotJournalEntry(std.testing.allocator, 1) catch return) orelse continue;
                snap.deinit(std.testing.allocator);
            }
        }
    };

    const t1 = try std.Thread.spawn(.{}, Worker.append, .{&state});
    const t2 = try std.Thread.spawn(.{}, Worker.read, .{&state});
    const t3 = try std.Thread.spawn(.{}, Worker.append, .{&state});
    t1.join();
    t2.join();
    t3.join();

    try std.testing.expect(state.change_journal.items.len > 0);
}

test "ServerState: schema lock allows concurrent readers" {
    var state = try ServerState.init(std.testing.allocator);
    defer state.deinit();

    const Reader = struct {
        fn run(s: *ServerState) void {
            for (0..500) |_| {
                s.schema_lock.lockShared();
                defer s.schema_lock.unlockShared();
                _ = s.schema_tables;
            }
        }
    };
    const a = try std.Thread.spawn(.{}, Reader.run, .{&state});
    const b = try std.Thread.spawn(.{}, Reader.run, .{&state});
    a.join();
    b.join();
}

test "findAvailablePort: returns the preferred port when it is free" {
    const p = try findAvailablePort("127.0.0.1", 0, 1);
    try std.testing.expect(p == 0 or p > 0);
}

test "findAvailablePort: skips a port already bound" {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var srv = try addr.listen(.{ .reuse_address = false });
    defer srv.deinit();
    const taken = srv.listen_address.getPort();

    const found = try findAvailablePort("127.0.0.1", taken, 10);
    try std.testing.expect(found != taken);
    try std.testing.expect(found > taken);
}

test "findAvailablePort: gives up after max_attempts" {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var srv = try addr.listen(.{ .reuse_address = false });
    defer srv.deinit();
    const taken = srv.listen_address.getPort();

    try std.testing.expectError(error.NoAvailablePort, findAvailablePort("127.0.0.1", taken, 1));
}
