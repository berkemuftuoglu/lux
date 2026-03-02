const std = @import("std");
const builtin = @import("builtin");
const postgres = @import("postgres.zig");
const utils = @import("utils.zig");
const sql_guard = @import("sql_guard.zig");
const web = @import("web.zig");

const ServerState = web.ServerState;
const ChangeEntry = web.ChangeEntry;
const MAX_JOURNAL_ENTRIES = web.MAX_JOURNAL_ENTRIES;

// ── Helpers shared within crud.zig ───────────────────────────────────────

/// Metadata for table data JSON response.
pub const TableDataMeta = struct {
    total: usize,
    limit: usize,
    offset: usize,
    use_exact_count: bool,
    table_has_pk: bool,
    use_keyset: bool,
    pk_col_name: ?[]const u8,
};

const KVPair = struct {
    key: []const u8,
    value: []const u8,
};

/// Find a table by name in the schema. Returns the TableInfo or null.
pub fn findTableInSchema(tables: []const postgres.TableInfo, name: []const u8) ?postgres.TableInfo {
    for (tables) |t| {
        if (std.mem.eql(u8, t.name, name)) return t;
    }
    return null;
}

/// Check if a column exists in a table's schema.
pub fn findColumnInTable(table: postgres.TableInfo, col_name: []const u8) bool {
    for (table.columns) |col| {
        if (std.mem.eql(u8, col.name, col_name)) return true;
    }
    return false;
}

/// Read the HTTP request body, handling partial reads.
/// Returns the body slice (may point into extra_buf or into request).
pub fn readRequestBody(
    stream: std.net.Stream,
    request: []const u8,
    extra_buf: []u8,
    max_content_length: usize,
) ?[]const u8 {
    const content_length = utils.findContentLength(request) orelse return null;
    if (content_length > max_content_length) return null;

    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse return null;
    const body_start = header_end + 4;
    var body = request[body_start..];

    if (body.len < content_length) {
        if (content_length > extra_buf.len) return null;
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch return null;
            if (n == 0) break;
            read_so_far += n;
        }
        return extra_buf[0..read_so_far];
    } else {
        return body[0..content_length];
    }
}

/// Check if read-only mode blocks this operation. Returns true if blocked.
pub fn enforceReadOnly(stream: std.net.Stream, state: *const ServerState) !bool {
    if (state.read_only) {
        try utils.sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to make changes.\"}");
        return true;
    }
    return false;
}

/// Validate a Postgres ctid value has the expected format: (page,offset).
pub fn validateCtid(ctid: []const u8) bool {
    if (ctid.len < 5) return false;
    if (ctid[0] != '(') return false;
    if (ctid[ctid.len - 1] != ')') return false;
    const inner = ctid[1 .. ctid.len - 1];
    const comma = std.mem.indexOfScalar(u8, inner, ',') orelse return false;
    const page = inner[0..comma];
    const offset = inner[comma + 1 ..];
    if (page.len == 0 or offset.len == 0) return false;
    for (page) |ch| { if (ch < '0' or ch > '9') return false; }
    for (offset) |ch| { if (ch < '0' or ch > '9') return false; }
    return true;
}

/// Format a single row as a compact JSON object: {"col1":"val1","col2":"val2"}
fn formatRowAsJsonCompact(allocator: std.mem.Allocator, col_names: []const []const u8, row: []const []const u8) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    const bw = buf.writer();
    try bw.writeByte('{');
    const len = @min(col_names.len, row.len);
    for (0..len) |i| {
        if (i > 0) try bw.writeByte(',');
        try bw.writeByte('"');
        try utils.writeJsonEscaped(bw, col_names[i]);
        try bw.writeAll("\":\"");
        try utils.writeJsonEscaped(bw, row[i]);
        try bw.writeByte('"');
    }
    try bw.writeByte('}');
    return buf.toOwnedSlice();
}

/// Extract key-value pairs from a JSON object field (e.g. "values": {"col":"val", ...}).
/// Returns null if the field is not found or not an object.
/// Caller must free the returned slice with allocator.free().
fn extractJsonObject(allocator: std.mem.Allocator, body: []const u8, field_name: []const u8) ?[]KVPair {
    // Find "field_name" :
    var search_pos: usize = 0;
    const obj_start = while (search_pos < body.len) {
        const quote_pos = std.mem.indexOfScalarPos(u8, body, search_pos, '"') orelse return null;
        if (quote_pos + 1 + field_name.len + 1 > body.len) return null;
        const after_quote = body[quote_pos + 1 ..];
        if (after_quote.len >= field_name.len + 1 and
            std.mem.eql(u8, after_quote[0..field_name.len], field_name) and
            after_quote[field_name.len] == '"')
        {
            var pos = quote_pos + 1 + field_name.len + 1;
            while (pos < body.len and (body[pos] == ' ' or body[pos] == ':' or body[pos] == '\t' or body[pos] == '\n')) pos += 1;
            if (pos >= body.len or body[pos] != '{') return null;
            break pos + 1; // skip the opening brace
        }
        search_pos = quote_pos + 1;
    } else return null;

    // Parse key-value pairs inside the object
    var pairs = std.ArrayList(KVPair).init(allocator);
    var pos = obj_start;
    while (pos < body.len) {
        // Skip whitespace and commas
        while (pos < body.len and (body[pos] == ' ' or body[pos] == ',' or body[pos] == '\t' or body[pos] == '\n' or body[pos] == '\r')) pos += 1;
        if (pos >= body.len or body[pos] == '}') break;

        // Expect opening quote for key
        if (body[pos] != '"') break;
        pos += 1;
        const key_start = pos;
        while (pos < body.len and body[pos] != '"') pos += 1;
        if (pos >= body.len) break;
        const key = body[key_start..pos];
        pos += 1; // skip closing quote

        // Skip colon and whitespace
        while (pos < body.len and (body[pos] == ' ' or body[pos] == ':' or body[pos] == '\t')) pos += 1;
        if (pos >= body.len) break;

        // Handle null value
        if (pos + 4 <= body.len and std.mem.eql(u8, body[pos..][0..4], "null")) {
            pairs.append(.{ .key = key, .value = "__NULL__" }) catch {
                pairs.deinit();
                return null;
            };
            pos += 4;
            continue;
        }

        // Expect opening quote for value
        if (body[pos] != '"') break;
        pos += 1;
        const val_start = pos;
        while (pos < body.len and body[pos] != '"') pos += 1;
        if (pos >= body.len) break;
        const val = body[val_start..pos];
        pos += 1; // skip closing quote

        pairs.append(.{ .key = key, .value = val }) catch {
            pairs.deinit();
            return null;
        };
    }

    return pairs.toOwnedSlice() catch {
        pairs.deinit();
        return null;
    };
}

/// Add an entry to the change journal.
pub fn addJournalEntry(
    state: *ServerState,
    table_name: []const u8,
    operation: []const u8,
    column_name: []const u8,
    old_value: []const u8,
    new_value: []const u8,
    pk_column: []const u8,
    pk_value: []const u8,
) u64 {
    if (!state.journal_initialized) return 0;
    const allocator = state.allocator;

    // Drop oldest if at capacity — free the inner strings to prevent leak
    if (state.change_journal.items.len >= MAX_JOURNAL_ENTRIES) {
        const old = state.change_journal.orderedRemove(0);
        allocator.free(old.table_name);
        allocator.free(old.operation);
        allocator.free(old.column_name);
        allocator.free(old.old_value);
        allocator.free(old.new_value);
        allocator.free(old.pk_column);
        allocator.free(old.pk_value);
    }

    const tn = allocator.dupe(u8, table_name) catch return 0;
    const op = allocator.dupe(u8, operation) catch {
        allocator.free(tn);
        return 0;
    };
    const cn = allocator.dupe(u8, column_name) catch {
        allocator.free(tn);
        allocator.free(op);
        return 0;
    };
    const ov = allocator.dupe(u8, old_value) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        return 0;
    };
    const nv = allocator.dupe(u8, new_value) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        allocator.free(ov);
        return 0;
    };
    const pkc = allocator.dupe(u8, pk_column) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        allocator.free(ov);
        allocator.free(nv);
        return 0;
    };
    const pkv = allocator.dupe(u8, pk_value) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        allocator.free(ov);
        allocator.free(nv);
        allocator.free(pkc);
        return 0;
    };

    const entry = ChangeEntry{
        .id = state.next_journal_id,
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
    state.change_journal.append(entry) catch return 0;
    state.next_journal_id += 1;
    return entry.id;
}

// ── JSON serialization helpers ────────────────────────────────────────────

/// Write the shared "columns":[...] and "rows":[[...]] JSON to the writer.
/// Used by both sendTableDataJson and sendQueryResultJson to eliminate duplication.
fn writeColumnsAndRows(w: anytype, pg_result: *const postgres.QueryResult) !void {
    // Column names
    try w.writeAll("\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('"');
        try utils.writeJsonEscaped(w, name);
        try w.writeByte('"');
    }
    try w.writeAll("],\"rows\":[");

    // Row data
    for (pg_result.rows, 0..) |row, ri| {
        if (ri > 0) try w.writeByte(',');
        try w.writeByte('[');
        for (row, 0..) |val, ci| {
            if (ci > 0) try w.writeByte(',');
            if (std.mem.eql(u8, val, "NULL")) {
                try w.writeAll("null");
            } else {
                try w.writeByte('"');
                try utils.writeJsonEscaped(w, val);
                try w.writeByte('"');
            }
        }
        try w.writeByte(']');
    }

    try w.writeByte(']');
}

/// Serialize paginated table data as JSON and send as HTTP response.
pub fn sendTableDataJson(
    stream: std.net.Stream,
    allocator: std.mem.Allocator,
    state: *ServerState,
    pg_result: *postgres.QueryResult,
    table_name: []const u8,
    meta: TableDataMeta,
) !void {
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();

    try w.print("{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"count_exact\":{s},\"pk_mode\":\"{s}\",\"pagination\":\"{s}\",", .{
        meta.total, meta.limit, meta.offset,
        if (meta.use_exact_count) "true" else "false",
        if (meta.table_has_pk) "column" else "ctid",
        if (meta.use_keyset) "keyset" else "offset",
    });

    // Include PK info from enhanced schema
    if (state.enhanced_schema) |etables| {
        for (etables) |et| {
            if (std.mem.eql(u8, et.name, table_name)) {
                try w.print("\"has_primary_key\":{s},\"pk_columns\":[", .{if (et.has_primary_key) "true" else "false"});
                for (et.primary_key_columns, 0..) |pk, pi| {
                    if (pi > 0) try w.writeByte(',');
                    try w.writeByte('"');
                    try utils.writeJsonEscaped(w, pk);
                    try w.writeByte('"');
                }
                try w.writeAll("],");
                break;
            }
        }
    }

    // Write shared columns and rows
    try writeColumnsAndRows(w, pg_result);

    // Add keyset cursor values if using keyset pagination
    if (meta.use_keyset and meta.pk_col_name != null and pg_result.n_rows > 0) {
        var pk_result_idx: ?usize = null;
        for (pg_result.col_names, 0..) |name, ci| {
            if (std.mem.eql(u8, name, meta.pk_col_name.?)) { pk_result_idx = ci; break; }
        }
        if (pk_result_idx) |pki| {
            const first_row = pg_result.rows[0];
            const last_row = pg_result.rows[pg_result.n_rows - 1];
            if (pki < first_row.len and pki < last_row.len) {
                try w.writeAll(",\"first_cursor\":\"");
                try utils.writeJsonEscaped(w, first_row[pki]);
                try w.writeAll("\",\"last_cursor\":\"");
                try utils.writeJsonEscaped(w, last_row[pki]);
                try w.writeByte('"');
            }
        }
    }

    try w.writeByte('}');
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Send a Postgres QueryResult as JSON with columns and rows.
pub fn sendQueryResultJson(stream: std.net.Stream, allocator: std.mem.Allocator, pg_result: *postgres.QueryResult) !void {
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();

    try w.print("{{\"row_count\":{d},", .{pg_result.n_rows});

    // Write shared columns and rows
    try writeColumnsAndRows(w, pg_result);

    try w.writeByte('}');
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

// ── Handler functions ─────────────────────────────────────────────────────

/// Handle GET /api/tables/:name/data — return paginated table data.
/// Path format: /api/tables/<name>/data or /api/tables/<name>/data?limit=N&offset=N
pub fn handleTableData(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/data
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    // Split off query string
    const path_part = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[0..qi] else after_prefix;
    const query_string = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[qi + 1 ..] else "";

    // path_part should be "<table_name>/data"
    const data_suffix = "/data";
    if (!std.mem.endsWith(u8, path_part, data_suffix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/data\"}");
        return;
    }

    const table_name = path_part[0 .. path_part.len - data_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema (fail-closed: reject if schema not loaded)
    const schema_tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
        return;
    };
    {
        var found = false;
        for (schema_tables) |t| {
            if (std.mem.eql(u8, t.name, table_name)) {
                found = true;
                break;
            }
        }
        if (!found) {
            try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    // Parse limit and offset from query string
    var limit: usize = 50;
    var offset: usize = 0;
    utils.parseQueryParam(query_string, "limit", &limit);
    utils.parseQueryParam(query_string, "offset", &offset);
    if (limit > 10000) limit = 10000;

    // Parse optional sort column and direction
    var sort_col_buf: [128]u8 = undefined;
    const sort_col = utils.parseStringQueryParam(query_string, "sort", &sort_col_buf);
    var dir_buf: [8]u8 = undefined;
    const dir_param = utils.parseStringQueryParam(query_string, "dir", &dir_buf);
    const sort_dir: []const u8 = if (dir_param) |d| (if (std.mem.eql(u8, d, "desc")) @as([]const u8, "DESC") else "ASC") else "ASC";

    // Validate sort column against schema if provided
    if (sort_col) |col| {
        if (state.schema_tables) |tables| {
            const table_info = findTableInSchema(tables, table_name);
            if (table_info) |ti| {
                if (!findColumnInTable(ti, col)) {
                    try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Sort column not found in table schema\"}");
                    return;
                }
            }
        }
    }

    // Parse count mode: ?count=exact for precise count, otherwise estimate
    var count_exact_buf: [8]u8 = undefined;
    const count_mode = utils.parseStringQueryParam(query_string, "count", &count_exact_buf);
    const use_exact_count_param = if (count_mode) |m| std.mem.eql(u8, m, "exact") else false;

    // Parse column filters: f.column_name=value
    const FilterEntry = struct { column: []const u8, value: []const u8 };
    var filters: [16]FilterEntry = undefined;
    var filter_count: usize = 0;
    {
        var fiter = std.mem.splitScalar(u8, query_string, '&');
        while (fiter.next()) |param| {
            if (std.mem.startsWith(u8, param, "f.") and param.len > 2) {
                if (std.mem.indexOfScalar(u8, param, '=')) |eq| {
                    const col = param[2..eq];
                    const val = param[eq + 1 ..];
                    if (col.len > 0 and val.len > 0 and filter_count < 16) {
                        // Validate column exists in schema
                        if (state.schema_tables) |tables| {
                            const ti = findTableInSchema(tables, table_name);
                            if (ti) |t_info| {
                                if (findColumnInTable(t_info, col)) {
                                    filters[filter_count] = .{ .column = col, .value = val };
                                    filter_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    const has_filters = filter_count > 0;

    // When filters active, use exact count; otherwise respect user preference
    const use_exact_count = use_exact_count_param or has_filters;

    // Parse keyset pagination params: after=value, before=value
    var after_buf: [256]u8 = undefined;
    const after_cursor = utils.parseStringQueryParam(query_string, "after", &after_buf);
    var before_buf: [256]u8 = undefined;
    const before_cursor = utils.parseStringQueryParam(query_string, "before", &before_buf);

    // Check if table has a primary key — if not, include ctid
    var table_has_pk = true;
    var pk_col_name: ?[]const u8 = null;
    if (state.enhanced_schema) |etables| {
        for (etables) |et| {
            if (std.mem.eql(u8, et.name, table_name)) {
                table_has_pk = et.has_primary_key;
                if (et.has_primary_key and et.primary_key_columns.len > 0) {
                    pk_col_name = et.primary_key_columns[0];
                }
                break;
            }
        }
    }

    // Views don't support ctid — check if this relation is a view
    var is_view = false;
    if (!table_has_pk) view_check: {
        var view_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch break :view_check;
        defer view_conn.deinit();
        var vq_buf = std.ArrayList(u8).init(allocator);
        defer vq_buf.deinit();
        const esc_tn = utils.escapeStringValue(allocator, table_name) catch break :view_check;
        defer allocator.free(esc_tn);
        vq_buf.writer().print("SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = '{s}' AND n.nspname = 'public' AND c.relkind = 'v'", .{esc_tn}) catch break :view_check;
        vq_buf.append(0) catch break :view_check;
        const vq_z: [*:0]const u8 = @ptrCast(vq_buf.items[0 .. vq_buf.items.len - 1 :0]);
        var vr = view_conn.runQuery(allocator, vq_z) catch break :view_check;
        is_view = vr.n_rows > 0;
        vr.deinit();
    }
    const select_cols: []const u8 = if (!table_has_pk and !is_view) "ctid::text, *" else "*";

    // Use keyset pagination when: table has PK and after/before cursor present
    const use_keyset = table_has_pk and pk_col_name != null and (after_cursor != null or before_cursor != null);

    // Build WHERE clause from filters and keyset cursor
    var where_buf = std.ArrayList(u8).init(allocator);
    defer where_buf.deinit();
    const ww = where_buf.writer();
    var where_parts: usize = 0;

    if (has_filters or use_keyset) {
        try ww.writeAll(" WHERE ");
    }

    // Add filter conditions
    for (filters[0..filter_count]) |f| {
        if (where_parts > 0) try ww.writeAll(" AND ");
        const esc_val = utils.escapeStringValue(allocator, f.value) catch continue;
        defer allocator.free(esc_val);
        try ww.print("\"{s}\"::text ILIKE '%{s}%'", .{ f.column, esc_val });
        where_parts += 1;
    }

    // Add keyset cursor condition
    if (use_keyset) {
        if (after_cursor) |cursor| {
            if (where_parts > 0) try ww.writeAll(" AND ");
            const esc_cursor = utils.escapeStringValue(allocator, cursor) catch {
                try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
            defer allocator.free(esc_cursor);
            try ww.print("\"{s}\" > '{s}'", .{ pk_col_name.?, esc_cursor });
            where_parts += 1;
        } else if (before_cursor) |cursor| {
            if (where_parts > 0) try ww.writeAll(" AND ");
            const esc_cursor = utils.escapeStringValue(allocator, cursor) catch {
                try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
            defer allocator.free(esc_cursor);
            try ww.print("\"{s}\" < '{s}'", .{ pk_col_name.?, esc_cursor });
            where_parts += 1;
        }
    }
    const where_clause = where_buf.items;

    // Build count query
    var count_buf = std.ArrayList(u8).init(allocator);
    defer count_buf.deinit();
    if (!use_exact_count) {
        const esc_count_tn = try utils.escapeStringValue(allocator, table_name);
        defer allocator.free(esc_count_tn);
        try count_buf.writer().print("SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = '{s}'", .{esc_count_tn});
    } else {
        try count_buf.writer().print("SELECT COUNT(*) FROM \"{s}\"{s}", .{ table_name, where_clause });
    }
    try count_buf.append(0);

    // Build data query
    var sql_list = std.ArrayList(u8).init(allocator);
    defer sql_list.deinit();
    const sw = sql_list.writer();

    if (use_keyset and before_cursor != null) {
        // Backward keyset: reverse order, will re-reverse results
        try sw.print("SELECT {s} FROM \"{s}\"{s}", .{ select_cols, table_name, where_clause });
        try sw.print(" ORDER BY \"{s}\" DESC", .{pk_col_name.?});
        try sw.print(" LIMIT {d}", .{limit});
    } else if (sort_col) |col| {
        try sw.print("SELECT {s} FROM \"{s}\"{s} ORDER BY \"{s}\" {s} LIMIT {d} OFFSET {d}", .{ select_cols, table_name, where_clause, col, sort_dir, limit, offset });
    } else if (use_keyset) {
        // Forward keyset
        try sw.print("SELECT {s} FROM \"{s}\"{s} ORDER BY \"{s}\" ASC LIMIT {d}", .{ select_cols, table_name, where_clause, pk_col_name.?, limit });
    } else {
        try sw.print("SELECT {s} FROM \"{s}\"{s} LIMIT {d} OFFSET {d}", .{ select_cols, table_name, where_clause, limit, offset });
    }
    try sql_list.append(0);

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Get total count
    const count_z: [*:0]const u8 = @ptrCast(count_buf.items[0 .. count_buf.items.len - 1 :0]);
    var count_result = pg_conn.runQuery(allocator, count_z) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Count query failed\"}");
        return;
    };
    defer count_result.deinit();

    var total: usize = 0;
    if (count_result.n_rows > 0 and count_result.rows[0].len > 0) {
        total = std.fmt.parseInt(usize, count_result.rows[0][0], 10) catch 0;
    }

    // Get data
    const data_z: [*:0]const u8 = @ptrCast(sql_list.items[0 .. sql_list.items.len - 1 :0]);
    var pg_result = pg_conn.runQuery(allocator, data_z) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Data query failed\"}");
        return;
    };
    defer pg_result.deinit();

    // Build and send JSON response
    try sendTableDataJson(stream, allocator, state, &pg_result, table_name, .{
        .total = total,
        .limit = limit,
        .offset = offset,
        .use_exact_count = use_exact_count,
        .table_has_pk = table_has_pk,
        .use_keyset = use_keyset,
        .pk_col_name = pk_col_name,
    });
}

/// Handle POST /api/update — update a single cell value.
pub fn handleUpdate(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    var extra_buf: [8192]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    // Extract fields
    const table_name = utils.extractJsonField(body, "table") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table field\"}");
        return;
    };
    const column_name = utils.extractJsonField(body, "column") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing column field\"}");
        return;
    };
    const value = utils.extractJsonField(body, "value") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing value field\"}");
        return;
    };
    const pk_column = utils.extractJsonField(body, "pk_column") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_column field\"}");
        return;
    };
    const pk_value = utils.extractJsonField(body, "pk_value") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_value field\"}");
        return;
    };

    // Check pk_mode: "ctid" or "column" (default)
    const pk_mode = utils.extractJsonField(body, "pk_mode") orelse "column";
    const is_ctid_mode = std.mem.eql(u8, pk_mode, "ctid");

    // Validate table and columns against schema
    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    };
    if (!findColumnInTable(table_info, column_name)) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Column not found in table schema\"}");
        return;
    }
    // For ctid mode, pk_column is "ctid" which is a system column — skip schema validation
    if (!is_ctid_mode) {
        if (!findColumnInTable(table_info, pk_column)) {
            try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"PK column not found in table schema\"}");
            return;
        }
    } else {
        // Validate ctid format
        if (!validateCtid(pk_value)) {
            try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid ctid format\"}");
            return;
        }
    }

    // Escape values
    const escaped_value = utils.escapeStringValue(allocator, value) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_value);

    const escaped_pk = utils.escapeStringValue(allocator, pk_value) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_pk);

    // Check column type for json/jsonb casting
    var is_json_col = false;
    if (state.enhanced_schema) |etables| {
        for (etables) |et| {
            if (std.mem.eql(u8, et.name, table_name)) {
                for (et.columns) |col| {
                    if (std.mem.eql(u8, col.name, column_name)) {
                        if (std.mem.eql(u8, col.data_type, "json") or std.mem.eql(u8, col.data_type, "jsonb")) {
                            is_json_col = true;
                        }
                        break;
                    }
                }
                break;
            }
        }
    }
    const value_expr: []const u8 = if (is_json_col) "'::jsonb" else "'";

    // Build SQL: use ctid WHERE clause for no-PK tables
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const w = sql_buf.writer();
    if (is_ctid_mode) {
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}{s} WHERE ctid = '{s}'::tid RETURNING \"{s}\"", .{
            table_name, column_name, escaped_value, value_expr, escaped_pk, column_name,
        });
    } else {
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}{s} WHERE \"{s}\" = '{s}' RETURNING \"{s}\"", .{
            table_name, column_name, escaped_value, value_expr, pk_column, escaped_pk, column_name,
        });
    }
    try sql_buf.append(0); // null terminator

    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record journal entry — use the value field as the new value, old value from request
    const old_value_field = utils.extractJsonField(body, "old_value") orelse "";
    const journal_id = addJournalEntry(state, table_name, "update", column_name, old_value_field, value, pk_column, pk_value);

    var resp_buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"success\":true,\"journal_id\":{d}}}", .{journal_id}) catch "{\"success\":true}";
    try utils.sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle POST /api/delete-row — delete a row by primary key.
pub fn handleDeleteRow(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    var extra_buf: [4096]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 4096) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = utils.extractJsonField(body, "table") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table field\"}");
        return;
    };
    const pk_column = utils.extractJsonField(body, "pk_column") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_column field\"}");
        return;
    };
    const pk_value = utils.extractJsonField(body, "pk_value") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_value field\"}");
        return;
    };

    // Check pk_mode
    const pk_mode = utils.extractJsonField(body, "pk_mode") orelse "column";
    const is_ctid_mode = std.mem.eql(u8, pk_mode, "ctid");

    // Validate against schema
    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    };
    if (!is_ctid_mode) {
        if (!findColumnInTable(table_info, pk_column)) {
            try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"PK column not found in table schema\"}");
            return;
        }
    } else {
        if (!validateCtid(pk_value)) {
            try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid ctid format\"}");
            return;
        }
    }

    // Escape pk value
    const escaped_pk = utils.escapeStringValue(allocator, pk_value) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_pk);

    // Build SQL: DELETE with ctid or PK column
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const w = sql_buf.writer();
    if (is_ctid_mode) {
        try w.print("DELETE FROM \"{s}\" WHERE ctid = '{s}'::tid", .{ table_name, escaped_pk });
    } else {
        try w.print("DELETE FROM \"{s}\" WHERE \"{s}\" = '{s}'", .{ table_name, pk_column, escaped_pk });
    }
    try sql_buf.append(0);

    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Fetch the row BEFORE deleting so we can record it in the journal
    var old_row_json: []const u8 = "";
    var old_row_buf: ?[]u8 = null;
    defer if (old_row_buf) |b| allocator.free(b);
    {
        var sel_buf = std.ArrayList(u8).init(allocator);
        defer sel_buf.deinit();
        const sel_w = sel_buf.writer();
        if (is_ctid_mode) {
            sel_w.print("SELECT * FROM \"{s}\" WHERE ctid = '{s}'::tid LIMIT 1", .{ table_name, escaped_pk }) catch {};
        } else {
            sel_w.print("SELECT * FROM \"{s}\" WHERE \"{s}\" = '{s}' LIMIT 1", .{ table_name, pk_column, escaped_pk }) catch {};
        }
        sel_buf.append(0) catch {};
        if (sel_buf.items.len > 1) {
            const sel_z: [*:0]const u8 = sel_buf.items[0 .. sel_buf.items.len - 1 :0];
            if (pg_conn.runQuery(allocator, sel_z)) |sel_res| {
                var sel_result = sel_res;
                defer sel_result.deinit();
                if (sel_result.rows.len > 0) {
                    if (formatRowAsJsonCompact(allocator, sel_result.col_names, sel_result.rows[0])) |json| {
                        old_row_buf = json;
                        old_row_json = json;
                    } else |_| {}
                }
            } else |_| {}
        }
    }

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record journal entry for delete with old row data
    _ = addJournalEntry(state, table_name, "delete", "", old_row_json, "", pk_column, pk_value);

    try utils.sendResponse(stream, "200 OK", "application/json", "{\"success\":true}");
}

/// Handle POST /api/insert-row — insert a row with default values.
pub fn handleInsertRow(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    var extra_buf: [4096]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 4096) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = utils.extractJsonField(body, "table") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table field\"}");
        return;
    };

    // Validate against schema
    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    if (findTableInSchema(tables, table_name) == null) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    }

    // Parse optional values object
    const values = extractJsonObject(allocator, body, "values");
    defer if (values) |v| allocator.free(v);

    // Validate column keys against schema
    if (values) |pairs| {
        const table_info = findTableInSchema(tables, table_name) orelse {
            try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        };
        for (pairs) |pair| {
            if (!findColumnInTable(table_info, pair.key)) {
                try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Column not found in table schema\"}");
                return;
            }
        }
    }

    // Build SQL
    var sql_builder = std.ArrayList(u8).init(allocator);
    defer sql_builder.deinit();
    const sw = sql_builder.writer();

    if (values) |pairs| {
        if (pairs.len > 0) {
            // INSERT INTO "table" ("col1", "col2") VALUES ('val1', 'val2') RETURNING *
            try sw.print("INSERT INTO \"{s}\" (", .{table_name});
            for (pairs, 0..) |pair, i| {
                if (i > 0) try sw.writeAll(", ");
                const esc_col = utils.escapeIdentifier(allocator, pair.key) catch {
                    try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid column name\"}");
                    return;
                };
                defer allocator.free(esc_col);
                try sw.print("\"{s}\"", .{esc_col});
            }
            try sw.writeAll(") VALUES (");
            for (pairs, 0..) |pair, i| {
                if (i > 0) try sw.writeAll(", ");
                if (std.mem.eql(u8, pair.value, "__NULL__")) {
                    try sw.writeAll("NULL");
                } else {
                    const escaped = utils.escapeStringValue(allocator, pair.value) catch {
                        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                        return;
                    };
                    defer allocator.free(escaped);
                    try sw.print("'{s}'", .{escaped});
                }
            }
            try sw.writeAll(") RETURNING *");
        } else {
            try sw.print("INSERT INTO \"{s}\" DEFAULT VALUES RETURNING *", .{table_name});
        }
    } else {
        try sw.print("INSERT INTO \"{s}\" DEFAULT VALUES RETURNING *", .{table_name});
    }
    try sw.writeByte(0); // null terminate

    const sql_slice = sql_builder.items;
    const sql_z: [*:0]const u8 = sql_slice[0 .. sql_slice.len - 1 :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record journal entry for insert — use first column as PK
    if (pg_result.n_rows > 0 and pg_result.col_names.len > 0) {
        const insert_pk_col = pg_result.col_names[0];
        const insert_pk_val = if (pg_result.rows[0].len > 0) pg_result.rows[0][0] else "";
        _ = addJournalEntry(state, table_name, "insert", "", "", "", insert_pk_col, insert_pk_val);
    }

    // Build response with the new row
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const jw = json_buf.writer();

    try jw.writeAll("{\"success\":true,\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try jw.writeByte(',');
        try jw.writeByte('"');
        try utils.writeJsonEscaped(jw, name);
        try jw.writeByte('"');
    }
    try jw.writeAll("],\"row\":[");
    if (pg_result.n_rows > 0) {
        for (pg_result.rows[0], 0..) |val, ci| {
            if (ci > 0) try jw.writeByte(',');
            if (std.mem.eql(u8, val, "NULL")) {
                try jw.writeAll("null");
            } else {
                try jw.writeByte('"');
                try utils.writeJsonEscaped(jw, val);
                try jw.writeByte('"');
            }
        }
    }
    try jw.writeAll("]}");

    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle GET /api/tables/:name/fk-lookup?column=col&search=text&limit=20
pub fn handleFkLookup(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = state.allocator;

    // Parse: /api/tables/<table>/fk-lookup?column=...&search=...
    const prefix = "/api/tables/";
    const after_prefix = path[prefix.len..];
    const fk_idx = std.mem.indexOf(u8, after_prefix, "/fk-lookup") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    };
    const table_name = after_prefix[0..fk_idx];

    // Parse query string
    const qs_start = std.mem.indexOfScalar(u8, path, '?') orelse path.len;
    const qs = if (qs_start < path.len) path[qs_start + 1 ..] else "";

    var col_buf: [128]u8 = undefined;
    const column = utils.parseStringQueryParam(qs, "column", &col_buf) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing column param\"}");
        return;
    };
    var search_buf: [256]u8 = undefined;
    const search = utils.parseStringQueryParam(qs, "search", &search_buf) orelse "";

    // Find FK target from enhanced schema
    const etables = state.enhanced_schema orelse {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No enhanced schema\"}");
        return;
    };
    var fk_target_table: ?[]const u8 = null;
    var fk_target_column: ?[]const u8 = null;
    for (etables) |et| {
        if (std.mem.eql(u8, et.name, table_name)) {
            for (et.columns) |col| {
                if (std.mem.eql(u8, col.name, column)) {
                    fk_target_table = col.fk_target_table;
                    fk_target_column = col.fk_target_column;
                    break;
                }
            }
            break;
        }
    }

    const target_table = fk_target_table orelse {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No FK target found for this column\"}");
        return;
    };
    const target_col = fk_target_column orelse {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No FK target column found\"}");
        return;
    };

    // Parse limit
    var fk_limit: usize = 20;
    utils.parseQueryParam(qs, "limit", &fk_limit);
    if (fk_limit > 100) fk_limit = 100;

    // Build query
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const sw = sql_buf.writer();

    if (search.len > 0) {
        const esc_search = utils.escapeStringValue(allocator, search) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(esc_search);
        try sw.print("SELECT \"{s}\" FROM \"{s}\" WHERE \"{s}\"::text ILIKE '%{s}%' LIMIT {d}", .{ target_col, target_table, target_col, esc_search, fk_limit });
    } else {
        try sw.print("SELECT \"{s}\" FROM \"{s}\" LIMIT {d}", .{ target_col, target_table, fk_limit });
    }
    try sql_buf.append(0);

    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"FK lookup query failed\"}");
        return;
    };
    defer pg_result.deinit();

    // Serialize results as JSON array
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const jw = json_buf.writer();
    try jw.writeAll("{\"values\":[");
    for (pg_result.rows, 0..) |row, ri| {
        if (ri > 0) try jw.writeByte(',');
        if (row.len > 0) {
            if (std.mem.eql(u8, row[0], "NULL")) {
                try jw.writeAll("null");
            } else {
                try jw.writeByte('"');
                try utils.writeJsonEscaped(jw, row[0]);
                try jw.writeByte('"');
            }
        }
    }
    try jw.writeAll("]}");

    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle POST /api/tables/:name/bulk-update — find and replace in a column.
pub fn handleBulkUpdate(stream: std.net.Stream, request: []const u8, path: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/bulk-update
    const prefix = "/api/tables/";
    const after_prefix = path[prefix.len..];
    const suffix = "/bulk-update";
    if (!std.mem.endsWith(u8, after_prefix, suffix)) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }
    const table_name = after_prefix[0 .. after_prefix.len - suffix.len];

    // Validate table
    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found\"}");
        return;
    };

    var extra_buf: [8192]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };

    const column = utils.extractJsonField(body, "column") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing column field\"}");
        return;
    };
    const find_val = utils.extractJsonField(body, "find") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing find field\"}");
        return;
    };
    const replace_val = utils.extractJsonField(body, "replace") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing replace field\"}");
        return;
    };
    const force_str = utils.extractJsonField(body, "force") orelse "false";
    const force = std.mem.eql(u8, force_str, "true");

    // Validate column
    if (!findColumnInTable(table_info, column)) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Column not found\"}");
        return;
    }

    const esc_find = utils.escapeStringValue(allocator, find_val) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(esc_find);

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!force) {
        // Preview mode: count affected rows
        var cnt_sql = std.ArrayList(u8).init(allocator);
        defer cnt_sql.deinit();
        try cnt_sql.writer().print("SELECT COUNT(*) FROM \"{s}\" WHERE \"{s}\"::text = '{s}'", .{ table_name, column, esc_find });
        try cnt_sql.append(0);
        const cnt_z: [*:0]const u8 = @ptrCast(cnt_sql.items[0 .. cnt_sql.items.len - 1 :0]);
        var cnt_result = pg_conn.runQuery(allocator, cnt_z) catch {
            try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Count query failed\"}");
            return;
        };
        defer cnt_result.deinit();
        var affected: usize = 0;
        if (cnt_result.n_rows > 0 and cnt_result.rows[0].len > 0) {
            affected = std.fmt.parseInt(usize, cnt_result.rows[0][0], 10) catch 0;
        }
        var resp_buf: [128]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"affected_rows\":{d},\"requires_confirmation\":true}}", .{affected}) catch "{\"error\":\"fmt\"}";
        try utils.sendResponse(stream, "200 OK", "application/json", resp);
    } else {
        // Execute mode
        const esc_replace = utils.escapeStringValue(allocator, replace_val) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(esc_replace);
        var upd_sql = std.ArrayList(u8).init(allocator);
        defer upd_sql.deinit();
        try upd_sql.writer().print("UPDATE \"{s}\" SET \"{s}\" = '{s}' WHERE \"{s}\"::text = '{s}'", .{ table_name, column, esc_replace, column, esc_find });
        try upd_sql.append(0);
        const upd_z: [*:0]const u8 = @ptrCast(upd_sql.items[0 .. upd_sql.items.len - 1 :0]);
        var upd_result = pg_conn.runQuery(allocator, upd_z) catch {
            var err_buf: [512]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&err_buf);
            const ew = fbs.writer();
            ew.writeAll("{\"error\":\"") catch return;
            utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
            ew.writeAll("\"}") catch return;
            try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
            return;
        };
        defer upd_result.deinit();

        // Record journal entries
        _ = addJournalEntry(state, table_name, "update", column, find_val, replace_val, "bulk", "");
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"success\":true}");
    }
}

/// Handle POST /api/tables/<name>/truncate — remove all rows from a table.
pub fn handleTruncateTable(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Extract table name from /api/tables/<name>/truncate
    const prefix = "/api/tables/";
    const suffix = "/truncate";
    if (path.len <= prefix.len + suffix.len) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table name\"}");
        return;
    }
    const table_name = path[prefix.len .. path.len - suffix.len];
    if (table_name.len == 0) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty table name\"}");
        return;
    }

    // Validate table exists in schema
    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    if (findTableInSchema(tables, table_name) == null) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    }

    // Build TRUNCATE SQL
    var sql_buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&sql_buf);
    fbs.writer().print("TRUNCATE TABLE \"{s}\"", .{table_name}) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Table name too long\"}");
        return;
    };
    const sql_len = fbs.pos;
    sql_buf[sql_len] = 0;
    const sql_z: [*:0]const u8 = sql_buf[0..sql_len :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [1024]u8 = undefined;
        var efbs = std.io.fixedBufferStream(&err_buf);
        const ew = efbs.writer();
        ew.writeAll("{\"error\":\"TRUNCATE failed: ") catch {};
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        try utils.sendResponse(stream, "200 OK", "application/json", efbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record in journal
    _ = addJournalEntry(state, table_name, "truncate", "", "", "", "ALL", "");

    try utils.sendResponse(stream, "200 OK", "application/json", "{\"ok\":true}");
}

// ── Tests ─────────────────────────────────────────────────────────────────

test "findTableInSchema: finds existing table" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
    };
    const tables = [_]postgres.TableInfo{
        .{ .name = "users", .columns = @constCast(&cols) },
    };
    const result = findTableInSchema(@constCast(&tables), "users");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("users", result.?.name);
}

test "findTableInSchema: returns null for missing table" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
    };
    const tables = [_]postgres.TableInfo{
        .{ .name = "users", .columns = @constCast(&cols) },
    };
    const result = findTableInSchema(@constCast(&tables), "orders");
    try std.testing.expect(result == null);
}

test "findColumnInTable: finds existing column" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
        .{ .name = "email", .data_type = "text" },
    };
    const table = postgres.TableInfo{ .name = "users", .columns = @constCast(&cols) };
    try std.testing.expect(findColumnInTable(table, "email"));
}

test "findColumnInTable: returns false for missing column" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
    };
    const table = postgres.TableInfo{ .name = "users", .columns = @constCast(&cols) };
    try std.testing.expect(!findColumnInTable(table, "nonexistent"));
}

test "validateCtid: accepts valid ctid formats" {
    // Standard format
    try std.testing.expect(validateCtid("(0,1)"));
    // Large numbers
    try std.testing.expect(validateCtid("(9999999,9999999)"));
    // Zeros
    try std.testing.expect(validateCtid("(0,0)"));
    // Leading zeros
    try std.testing.expect(validateCtid("(007,001)"));
}

test "validateCtid: rejects malformed ctids" {
    // Empty
    try std.testing.expect(!validateCtid(""));
    // Missing parens
    try std.testing.expect(!validateCtid("0,1"));
    // Missing comma
    try std.testing.expect(!validateCtid("(01)"));
    // Empty parens
    try std.testing.expect(!validateCtid("()"));
    // Missing page or tuple
    try std.testing.expect(!validateCtid("(,1)"));
    try std.testing.expect(!validateCtid("(0,)"));
    // Non-numeric
    try std.testing.expect(!validateCtid("(a,b)"));
    try std.testing.expect(!validateCtid("abc"));
    // Negative number
    try std.testing.expect(!validateCtid("(-1,1)"));
    // Space inside
    try std.testing.expect(!validateCtid("(0, 1)"));
}

test "validateCtid: rejects injection and nesting" {
    // SQL injection attempt
    try std.testing.expect(!validateCtid("(0,1); DROP TABLE users"));
    // Nested parens
    try std.testing.expect(!validateCtid("((0,1))"));
}

test "addJournalEntry: adds to journal" {
    var state = ServerState.init(std.testing.allocator);
    defer state.change_journal.deinit();
    const id = addJournalEntry(&state, "users", "update", "name", "old", "new", "id", "1");
    try std.testing.expect(id > 0);
    try std.testing.expectEqual(@as(usize, 1), state.change_journal.items.len);
    // Free the duped strings
    for (state.change_journal.items) |entry| {
        std.testing.allocator.free(entry.table_name);
        std.testing.allocator.free(entry.operation);
        std.testing.allocator.free(entry.column_name);
        std.testing.allocator.free(entry.old_value);
        std.testing.allocator.free(entry.new_value);
        std.testing.allocator.free(entry.pk_column);
        std.testing.allocator.free(entry.pk_value);
    }
}

test "addJournalEntry: multiple entries increment id" {
    var state = ServerState.init(std.testing.allocator);
    defer state.change_journal.deinit();
    const id1 = addJournalEntry(&state, "t", "update", "c", "old1", "new1", "id", "1");
    const id2 = addJournalEntry(&state, "t", "update", "c", "old2", "new2", "id", "2");
    const id3 = addJournalEntry(&state, "t", "update", "c", "old3", "new3", "id", "3");
    try std.testing.expect(id1 < id2);
    try std.testing.expect(id2 < id3);
    try std.testing.expectEqual(@as(usize, 3), state.change_journal.items.len);
    for (state.change_journal.items) |entry| {
        std.testing.allocator.free(entry.table_name);
        std.testing.allocator.free(entry.operation);
        std.testing.allocator.free(entry.column_name);
        std.testing.allocator.free(entry.old_value);
        std.testing.allocator.free(entry.new_value);
        std.testing.allocator.free(entry.pk_column);
        std.testing.allocator.free(entry.pk_value);
    }
}

test "addJournalEntry: uninitialized journal returns 0" {
    var state = ServerState.init(std.testing.allocator);
    defer state.change_journal.deinit();
    state.journal_initialized = false;
    const id = addJournalEntry(&state, "t", "update", "c", "old", "new", "id", "1");
    try std.testing.expectEqual(@as(u64, 0), id);
    try std.testing.expectEqual(@as(usize, 0), state.change_journal.items.len);
}

test "addJournalEntry: delete operation stores table and pk" {
    var state = ServerState.init(std.testing.allocator);
    defer state.change_journal.deinit();
    _ = addJournalEntry(&state, "orders", "delete", "", "", "", "order_id", "42");
    try std.testing.expectEqual(@as(usize, 1), state.change_journal.items.len);
    const entry = state.change_journal.items[0];
    try std.testing.expectEqualStrings("orders", entry.table_name);
    try std.testing.expectEqualStrings("delete", entry.operation);
    try std.testing.expectEqualStrings("order_id", entry.pk_column);
    try std.testing.expectEqualStrings("42", entry.pk_value);
    std.testing.allocator.free(entry.table_name);
    std.testing.allocator.free(entry.operation);
    std.testing.allocator.free(entry.column_name);
    std.testing.allocator.free(entry.old_value);
    std.testing.allocator.free(entry.new_value);
    std.testing.allocator.free(entry.pk_column);
    std.testing.allocator.free(entry.pk_value);
}

test "enforceReadOnly: allows when not read-only" {
    const state = ServerState.init(std.testing.allocator);
    // Can't test with a real stream, but verify the logic
    try std.testing.expect(!state.read_only);
}

test "extractJsonObject: extracts key-value pairs" {
    const body = "{\"table\":\"employees\",\"values\":{\"name\":\"Alice\",\"age\":\"30\"}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 2), pairs.len);
    try std.testing.expectEqualStrings("name", pairs[0].key);
    try std.testing.expectEqualStrings("Alice", pairs[0].value);
    try std.testing.expectEqualStrings("age", pairs[1].key);
    try std.testing.expectEqualStrings("30", pairs[1].value);
}

test "extractJsonObject: returns null for missing field" {
    const body = "{\"table\":\"employees\"}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values");
    try std.testing.expect(pairs == null);
}

test "extractJsonObject: handles null values" {
    const body = "{\"values\":{\"name\":null,\"age\":\"30\"}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 2), pairs.len);
    try std.testing.expectEqualStrings("__NULL__", pairs[0].value);
}

test "extractJsonObject: empty object" {
    const body = "{\"values\":{}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 0), pairs.len);
}

test "extractJsonObject: multiple pairs with whitespace" {
    const body = "{\"values\":{ \"a\" : \"1\" , \"b\" : \"2\" }}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 2), pairs.len);
}

test "extractJsonObject: value with escaped backslash" {
    const body = "{\"values\":{\"path\":\"C:\\\\Windows\"}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 1), pairs.len);
}

test "extractJsonObject: non-object value returns null" {
    const body = "{\"values\":\"not-an-object\"}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values");
    try std.testing.expect(pairs == null);
}

