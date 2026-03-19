const std = @import("std");
const httpz = @import("httpz");
const postgres = @import("postgres");
const utils = @import("utils");
const web = @import("web");

const log = std.log.scoped(.crud);

const ServerState = web.ServerState;
const ChangeEntry = web.ChangeEntry;
const max_journal_entries = web.max_journal_entries;

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

pub fn findTableInSchema(tables: []const postgres.TableInfo, name: []const u8) ?postgres.TableInfo {
    for (tables) |t| {
        if (std.mem.eql(u8, t.name, name)) return t;
    }
    return null;
}

pub fn findColumnInTable(table: postgres.TableInfo, col_name: []const u8) bool {
    for (table.columns) |col| {
        if (std.mem.eql(u8, col.name, col_name)) return true;
    }
    return false;
}

fn sendJsonResponse(res: *httpz.Response, body: []const u8) void {
    res.content_type = httpz.ContentType.JSON;
    res.body = body;
}

fn sendJsonError(res: *httpz.Response, status: u16, body: []const u8) void {
    res.status = status;
    res.content_type = httpz.ContentType.JSON;
    res.body = body;
}

pub fn enforceReadOnly(state: *const ServerState, res: *httpz.Response) bool {
    if (state.flags.read_only) {
        sendJsonError(res, 403, "{\"error\":\"Read-only mode is enabled. Disable it to make changes.\"}");
        return true;
    }
    return false;
}

pub fn validateCtid(ctid: []const u8) bool {
    if (ctid.len < 5) return false;
    if (ctid[0] != '(') return false;
    if (ctid[ctid.len - 1] != ')') return false;
    const inner = ctid[1 .. ctid.len - 1];
    const comma = std.mem.indexOfScalar(u8, inner, ',') orelse return false;
    const page = inner[0..comma];
    const offset = inner[comma + 1 ..];
    if (page.len == 0 or offset.len == 0) return false;
    for (page) |ch| {
        if (ch < '0' or ch > '9') return false;
    }
    for (offset) |ch| {
        if (ch < '0' or ch > '9') return false;
    }
    return true;
}

fn formatRowAsJsonCompact(allocator: std.mem.Allocator, col_names: []const []const u8, row: []const []const u8) ![]u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const bw = buf.writer(allocator);
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
    return buf.toOwnedSlice(allocator);
}

fn extractJsonObject(allocator: std.mem.Allocator, body: []const u8, field_name: []const u8) ?[]KVPair {
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

    var pairs = std.ArrayList(KVPair){};
    var pos = obj_start;
    while (pos < body.len) {
        while (pos < body.len and (body[pos] == ' ' or body[pos] == ',' or body[pos] == '\t' or body[pos] == '\n' or body[pos] == '\r')) pos += 1;
        if (pos >= body.len or body[pos] == '}') break;

        if (body[pos] != '"') break;
        pos += 1;
        const key_start = pos;
        while (pos < body.len and body[pos] != '"') pos += 1;
        if (pos >= body.len) break;
        const key = body[key_start..pos];
        pos += 1; // skip closing quote

        while (pos < body.len and (body[pos] == ' ' or body[pos] == ':' or body[pos] == '\t')) pos += 1;
        if (pos >= body.len) break;

        if (pos + 4 <= body.len and std.mem.eql(u8, body[pos..][0..4], "null")) {
            pairs.append(allocator, .{ .key = key, .value = "__NULL__" }) catch {
                pairs.deinit(allocator);
                return null;
            };
            pos += 4;
            continue;
        }

        if (body[pos] != '"') break;
        pos += 1;
        const val_start = pos;
        while (pos < body.len and body[pos] != '"') pos += 1;
        if (pos >= body.len) break;
        const val = body[val_start..pos];
        pos += 1; // skip closing quote

        pairs.append(allocator, .{ .key = key, .value = val }) catch {
            pairs.deinit(allocator);
            return null;
        };
    }

    return pairs.toOwnedSlice(allocator) catch {
        pairs.deinit(allocator);
        return null;
    };
}

pub fn addJournalEntry(
    state: *ServerState,
    table_name: []const u8,
    operation: []const u8,
    column_name: []const u8,
    old_value: []const u8,
    new_value: []const u8,
    pk_column: []const u8,
    pk_value: []const u8,
) !u64 {
    const allocator = state.allocator;

    // Drop oldest if at capacity -- free the inner strings to prevent leak
    if (state.change_journal.items.len >= max_journal_entries) {
        const old = state.change_journal.orderedRemove(0);
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
    try state.change_journal.append(state.allocator, entry);
    state.next_journal_id += 1;
    return entry.id;
}

fn writeColumnsAndRows(w: anytype, pg_result: *const postgres.QueryResult) !void {
    try w.writeAll("\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('"');
        try utils.writeJsonEscaped(w, name);
        try w.writeByte('"');
    }
    try w.writeAll("],\"rows\":[");

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

pub fn sendTableDataJson(
    allocator: std.mem.Allocator,
    res: *httpz.Response,
    state: *ServerState,
    pg_result: *postgres.QueryResult,
    table_name: []const u8,
    meta: TableDataMeta,
) !void {
    var json_buf = std.ArrayList(u8){};
    // no defer deinit — res.arena owns this memory until the response is sent
    const w = json_buf.writer(allocator);

    try w.print("{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"count_exact\":{s},\"pk_mode\":\"{s}\",\"pagination\":\"{s}\",", .{
        meta.total,                                    meta.limit,                                  meta.offset,
        if (meta.use_exact_count) "true" else "false", if (meta.table_has_pk) "column" else "ctid", if (meta.use_keyset) "keyset" else "offset",
    });

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

    try writeColumnsAndRows(w, pg_result);

    if (meta.use_keyset and meta.pk_col_name != null and pg_result.n_rows > 0) {
        var pk_result_idx: ?usize = null;
        for (pg_result.col_names, 0..) |name, ci| {
            if (std.mem.eql(u8, name, meta.pk_col_name.?)) {
                pk_result_idx = ci;
                break;
            }
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
    sendJsonResponse(res, json_buf.items);
}

pub fn sendQueryResultJson(allocator: std.mem.Allocator, res: *httpz.Response, pg_result: *postgres.QueryResult) !void {
    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(allocator);

    try w.print("{{\"row_count\":{d},", .{pg_result.n_rows});

    try writeColumnsAndRows(w, pg_result);

    try w.writeByte('}');
    sendJsonResponse(res, json_buf.items);
}

/// Extract query string from the raw URL path (everything after '?')
fn getQueryString(raw_path: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, raw_path, '?')) |qi| return raw_path[qi + 1 ..] else return "";
}

pub fn handleTableData(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const path = req.url.path;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 404, "{\"error\":\"Invalid path\"}");
        return;
    };

    if (table_name.len == 0 or table_name.len > 128) {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Fail-closed: reject if schema not loaded
    const schema_tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
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
            sendJsonError(res, 404, "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    const query_string = getQueryString(path);

    var limit: usize = 50;
    var offset: usize = 0;
    utils.parseQueryParam(query_string, "limit", &limit);
    utils.parseQueryParam(query_string, "offset", &offset);
    if (limit > 10000) limit = 10000;

    var sort_col_buf: [128]u8 = undefined;
    const sort_col = utils.parseStringQueryParam(query_string, "sort", &sort_col_buf);
    var dir_buf: [8]u8 = undefined;
    const dir_param = utils.parseStringQueryParam(query_string, "dir", &dir_buf);
    const sort_dir: []const u8 = if (dir_param) |d| (if (std.mem.eql(u8, d, "desc")) @as([]const u8, "DESC") else "ASC") else "ASC";

    if (sort_col) |col| {
        if (state.schema_tables) |tables| {
            const table_info = findTableInSchema(tables, table_name);
            if (table_info) |ti| {
                if (!findColumnInTable(ti, col)) {
                    sendJsonError(res, 400, "{\"error\":\"Sort column not found in table schema\"}");
                    return;
                }
            }
        }
    }

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

    var after_buf: [256]u8 = undefined;
    const after_cursor = utils.parseStringQueryParam(query_string, "after", &after_buf);
    var before_buf: [256]u8 = undefined;
    const before_cursor = utils.parseStringQueryParam(query_string, "before", &before_buf);

    // Tables without PK use ctid as row identifier
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

    // Views don't support ctid -- check if this relation is a view
    var is_view = false;
    if (!table_has_pk) view_check: {
        var view_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch break :view_check;
        defer view_conn.deinit();
        var vq_buf = std.ArrayList(u8){};
        const esc_tn = utils.escapeStringValue(allocator, table_name) catch break :view_check;
        vq_buf.writer(allocator).print("SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = '{s}' AND n.nspname = 'public' AND c.relkind = 'v'", .{esc_tn}) catch break :view_check;
        vq_buf.append(allocator, 0) catch break :view_check;
        const vq_z: [*:0]const u8 = @ptrCast(vq_buf.items[0 .. vq_buf.items.len - 1 :0]);
        var vr = view_conn.runQuery(allocator, vq_z) catch break :view_check;
        is_view = vr.n_rows > 0;
        vr.deinit();
    }
    const select_cols: []const u8 = if (!table_has_pk and !is_view) "ctid::text, *" else "*";

    const use_keyset = table_has_pk and pk_col_name != null and (after_cursor != null or before_cursor != null);

    var where_buf = std.ArrayList(u8){};
    const ww = where_buf.writer(allocator);
    var where_parts: usize = 0;

    if (has_filters or use_keyset) {
        try ww.writeAll(" WHERE ");
    }

    for (filters[0..filter_count]) |f| {
        if (where_parts > 0) try ww.writeAll(" AND ");
        const esc_val = utils.escapeStringValue(allocator, f.value) catch continue;
        try ww.print("\"{s}\"::text ILIKE '%{s}%'", .{ f.column, esc_val });
        where_parts += 1;
    }

    if (use_keyset) {
        if (after_cursor) |cursor| {
            if (where_parts > 0) try ww.writeAll(" AND ");
            const esc_cursor = utils.escapeStringValue(allocator, cursor) catch {
                sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
                return;
            };
            try ww.print("\"{s}\" > '{s}'", .{ pk_col_name.?, esc_cursor });
            where_parts += 1;
        } else if (before_cursor) |cursor| {
            if (where_parts > 0) try ww.writeAll(" AND ");
            const esc_cursor = utils.escapeStringValue(allocator, cursor) catch {
                sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
                return;
            };
            try ww.print("\"{s}\" < '{s}'", .{ pk_col_name.?, esc_cursor });
            where_parts += 1;
        }
    }
    const where_clause = where_buf.items;

    var count_buf = std.ArrayList(u8){};
    if (!use_exact_count) {
        const esc_count_tn = utils.escapeStringValue(allocator, table_name) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.InvalidCharacter => return error.InvalidData,
        };
        try count_buf.writer(allocator).print("SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = '{s}'", .{esc_count_tn});
    } else {
        try count_buf.writer(allocator).print("SELECT COUNT(*) FROM \"{s}\"{s}", .{ table_name, where_clause });
    }
    try count_buf.append(allocator, 0);

    var sql_list = std.ArrayList(u8){};
    const sw = sql_list.writer(allocator);

    if (use_keyset and before_cursor != null) {
        // Backward keyset: fetch in reverse, caller re-reverses
        try sw.print("SELECT {s} FROM \"{s}\"{s}", .{ select_cols, table_name, where_clause });
        try sw.print(" ORDER BY \"{s}\" DESC", .{pk_col_name.?});
        try sw.print(" LIMIT {d}", .{limit});
    } else if (sort_col) |col| {
        try sw.print("SELECT {s} FROM \"{s}\"{s} ORDER BY \"{s}\" {s} LIMIT {d} OFFSET {d}", .{ select_cols, table_name, where_clause, col, sort_dir, limit, offset });
    } else if (use_keyset) {
        try sw.print("SELECT {s} FROM \"{s}\"{s} ORDER BY \"{s}\" ASC LIMIT {d}", .{ select_cols, table_name, where_clause, pk_col_name.?, limit });
    } else {
        try sw.print("SELECT {s} FROM \"{s}\"{s} LIMIT {d} OFFSET {d}", .{ select_cols, table_name, where_clause, limit, offset });
    }
    try sql_list.append(allocator, 0);

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    const count_z: [*:0]const u8 = @ptrCast(count_buf.items[0 .. count_buf.items.len - 1 :0]);
    var count_result = pg_conn.runQuery(allocator, count_z) catch {
        sendJsonResponse(res, "{\"error\":\"Count query failed\"}");
        return;
    };
    defer count_result.deinit();

    var total: usize = 0;
    if (count_result.n_rows > 0 and count_result.rows[0].len > 0) {
        total = std.fmt.parseInt(usize, count_result.rows[0][0], 10) catch 0;
    }

    const data_z: [*:0]const u8 = @ptrCast(sql_list.items[0 .. sql_list.items.len - 1 :0]);
    var pg_result = pg_conn.runQuery(allocator, data_z) catch {
        sendJsonResponse(res, "{\"error\":\"Data query failed\"}");
        return;
    };
    defer pg_result.deinit();

    try sendTableDataJson(allocator, res, state, &pg_result, table_name, .{
        .total = total,
        .limit = limit,
        .offset = offset,
        .use_exact_count = use_exact_count,
        .table_has_pk = table_has_pk,
        .use_keyset = use_keyset,
        .pk_col_name = pk_col_name,
    });
}

pub fn handleUpdate(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (enforceReadOnly(state, res)) return;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = utils.extractJsonField(allocator, body, "table") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing table field\"}");
        return;
    };
    const column_name = utils.extractJsonField(allocator, body, "column") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing column field\"}");
        return;
    };
    const value = utils.extractJsonField(allocator, body, "value") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing value field\"}");
        return;
    };
    const pk_column = utils.extractJsonField(allocator, body, "pk_column") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing pk_column field\"}");
        return;
    };
    const pk_value = utils.extractJsonField(allocator, body, "pk_value") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing pk_value field\"}");
        return;
    };

    const pk_mode = utils.extractJsonField(allocator, body, "pk_mode") orelse "column";
    const is_ctid_mode = std.mem.eql(u8, pk_mode, "ctid");

    const tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        sendJsonError(res, 400, "{\"error\":\"Table not found in schema\"}");
        return;
    };
    if (!findColumnInTable(table_info, column_name)) {
        sendJsonError(res, 400, "{\"error\":\"Column not found in table schema\"}");
        return;
    }
    // ctid is a system column, not in the user schema -- skip validation
    if (!is_ctid_mode) {
        if (!findColumnInTable(table_info, pk_column)) {
            sendJsonError(res, 400, "{\"error\":\"PK column not found in table schema\"}");
            return;
        }
    } else {
        if (!validateCtid(pk_value)) {
            sendJsonError(res, 400, "{\"error\":\"Invalid ctid format\"}");
            return;
        }
    }

    const escaped_value = utils.escapeStringValue(allocator, value) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    const escaped_pk = utils.escapeStringValue(allocator, pk_value) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    // json/jsonb columns need explicit cast
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

    var sql_buf = std.ArrayList(u8){};
    const w = sql_buf.writer(allocator);
    if (is_ctid_mode) {
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}{s} WHERE ctid = '{s}'::tid RETURNING \"{s}\"", .{
            table_name, column_name, escaped_value, value_expr, escaped_pk, column_name,
        });
    } else {
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}{s} WHERE \"{s}\" = '{s}' RETURNING \"{s}\"", .{
            table_name, column_name, escaped_value, value_expr, pk_column, escaped_pk, column_name,
        });
    }
    try sql_buf.append(allocator, 0);

    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
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
        sendJsonResponse(res, fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    const old_value_field = utils.extractJsonField(allocator, body, "old_value") orelse "";
    const journal_id = addJournalEntry(state, table_name, "update", column_name, old_value_field, value, pk_column, pk_value) catch |err| blk: {
        log.warn("journal entry failed: {s}", .{@errorName(err)});
        break :blk @as(u64, 0);
    };

    var resp_buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"success\":true,\"journal_id\":{d}}}", .{journal_id}) catch "{\"success\":true}";
    sendJsonResponse(res, resp);
}

pub fn handleDeleteRow(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (enforceReadOnly(state, res)) return;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = utils.extractJsonField(allocator, body, "table") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing table field\"}");
        return;
    };
    const pk_column = utils.extractJsonField(allocator, body, "pk_column") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing pk_column field\"}");
        return;
    };
    const pk_value = utils.extractJsonField(allocator, body, "pk_value") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing pk_value field\"}");
        return;
    };

    const pk_mode = utils.extractJsonField(allocator, body, "pk_mode") orelse "column";
    const is_ctid_mode = std.mem.eql(u8, pk_mode, "ctid");

    const tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        sendJsonError(res, 400, "{\"error\":\"Table not found in schema\"}");
        return;
    };
    if (!is_ctid_mode) {
        if (!findColumnInTable(table_info, pk_column)) {
            sendJsonError(res, 400, "{\"error\":\"PK column not found in table schema\"}");
            return;
        }
    } else {
        if (!validateCtid(pk_value)) {
            sendJsonError(res, 400, "{\"error\":\"Invalid ctid format\"}");
            return;
        }
    }

    const escaped_pk = utils.escapeStringValue(allocator, pk_value) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var sql_buf = std.ArrayList(u8){};
    const w = sql_buf.writer(allocator);
    if (is_ctid_mode) {
        try w.print("DELETE FROM \"{s}\" WHERE ctid = '{s}'::tid", .{ table_name, escaped_pk });
    } else {
        try w.print("DELETE FROM \"{s}\" WHERE \"{s}\" = '{s}'", .{ table_name, pk_column, escaped_pk });
    }
    try sql_buf.append(allocator, 0);

    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Fetch the row BEFORE deleting so we can record it in the journal
    var old_row_json: []const u8 = "";
    {
        var sel_buf = std.ArrayList(u8){};
        const sel_w = sel_buf.writer(allocator);
        if (is_ctid_mode) {
            sel_w.print("SELECT * FROM \"{s}\" WHERE ctid = '{s}'::tid LIMIT 1", .{ table_name, escaped_pk }) catch |err| {
                log.warn("sel_buf write failed: {s}", .{@errorName(err)});
            };
        } else {
            sel_w.print("SELECT * FROM \"{s}\" WHERE \"{s}\" = '{s}' LIMIT 1", .{ table_name, pk_column, escaped_pk }) catch |err| {
                log.warn("sel_buf write failed: {s}", .{@errorName(err)});
            };
        }
        sel_buf.append(allocator, 0) catch |err| {
            log.warn("sel_buf null-terminator append failed: {s}", .{@errorName(err)});
        };
        if (sel_buf.items.len > 1) {
            const sel_z: [*:0]const u8 = sel_buf.items[0 .. sel_buf.items.len - 1 :0];
            if (pg_conn.runQuery(allocator, sel_z)) |sel_res| {
                var sel_result = sel_res;
                defer sel_result.deinit();
                if (sel_result.rows.len > 0) {
                    if (formatRowAsJsonCompact(allocator, sel_result.col_names, sel_result.rows[0])) |json| {
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
        sendJsonResponse(res, fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    if (addJournalEntry(state, table_name, "delete", "", old_row_json, "", pk_column, pk_value)) |_| {} else |err| {
        log.warn("journal entry failed: {s}", .{@errorName(err)});
    }

    sendJsonResponse(res, "{\"success\":true}");
}

pub fn handleInsertRow(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (enforceReadOnly(state, res)) return;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = utils.extractJsonField(allocator, body, "table") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing table field\"}");
        return;
    };

    const tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"No schema available\"}");
        return;
    };
    if (findTableInSchema(tables, table_name) == null) {
        sendJsonError(res, 400, "{\"error\":\"Table not found in schema\"}");
        return;
    }

    const values = extractJsonObject(allocator, body, "values");

    if (values) |pairs| {
        const table_info = findTableInSchema(tables, table_name) orelse {
            sendJsonError(res, 400, "{\"error\":\"Table not found in schema\"}");
            return;
        };
        for (pairs) |pair| {
            if (!findColumnInTable(table_info, pair.key)) {
                sendJsonError(res, 400, "{\"error\":\"Column not found in table schema\"}");
                return;
            }
        }
    }

    var sql_builder = std.ArrayList(u8){};
    const sw = sql_builder.writer(allocator);

    if (values) |pairs| {
        if (pairs.len > 0) {
            try sw.print("INSERT INTO \"{s}\" (", .{table_name});
            for (pairs, 0..) |pair, i| {
                if (i > 0) try sw.writeAll(", ");
                const esc_col = utils.escapeIdentifier(allocator, pair.key) catch {
                    sendJsonError(res, 400, "{\"error\":\"Invalid column name\"}");
                    return;
                };
                try sw.print("\"{s}\"", .{esc_col});
            }
            try sw.writeAll(") VALUES (");
            for (pairs, 0..) |pair, i| {
                if (i > 0) try sw.writeAll(", ");
                if (std.mem.eql(u8, pair.value, "__NULL__")) {
                    try sw.writeAll("NULL");
                } else {
                    const escaped = utils.escapeStringValue(allocator, pair.value) catch {
                        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
                        return;
                    };
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
    try sw.writeByte(0);

    const sql_slice = sql_builder.items;
    const sql_z: [*:0]const u8 = sql_slice[0 .. sql_slice.len - 1 :0];

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
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
        sendJsonResponse(res, fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // HACK: assumes first returned column is the PK for journal tracking
    if (pg_result.n_rows > 0 and pg_result.col_names.len > 0) {
        const insert_pk_col = pg_result.col_names[0];
        const insert_pk_val = if (pg_result.rows[0].len > 0) pg_result.rows[0][0] else "";
        if (addJournalEntry(state, table_name, "insert", "", "", "", insert_pk_col, insert_pk_val)) |_| {} else |err| {
            log.warn("journal entry failed: {s}", .{@errorName(err)});
        }
    }

    var json_buf = std.ArrayList(u8){};
    const jw = json_buf.writer(allocator);

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

    sendJsonResponse(res, json_buf.items);
}

pub fn handleFkLookup(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = res.arena;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 400, "{\"error\":\"Invalid path\"}");
        return;
    };

    const qs = getQueryString(req.url.path);

    var col_buf: [128]u8 = undefined;
    const column = utils.parseStringQueryParam(qs, "column", &col_buf) orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing column param\"}");
        return;
    };
    var search_buf: [256]u8 = undefined;
    const search = utils.parseStringQueryParam(qs, "search", &search_buf) orelse "";

    const etables = state.enhanced_schema orelse {
        sendJsonResponse(res, "{\"error\":\"No enhanced schema\"}");
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
        sendJsonResponse(res, "{\"error\":\"No FK target found for this column\"}");
        return;
    };
    const target_col = fk_target_column orelse {
        sendJsonResponse(res, "{\"error\":\"No FK target column found\"}");
        return;
    };

    var fk_limit: usize = 20;
    utils.parseQueryParam(qs, "limit", &fk_limit);
    if (fk_limit > 100) fk_limit = 100;

    var sql_buf = std.ArrayList(u8){};
    const sw = sql_buf.writer(allocator);

    if (search.len > 0) {
        const esc_search = utils.escapeStringValue(allocator, search) catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };
        try sw.print("SELECT \"{s}\" FROM \"{s}\" WHERE \"{s}\"::text ILIKE '%{s}%' LIMIT {d}", .{ target_col, target_table, target_col, esc_search, fk_limit });
    } else {
        try sw.print("SELECT \"{s}\" FROM \"{s}\" LIMIT {d}", .{ target_col, target_table, fk_limit });
    }
    try sql_buf.append(allocator, 0);

    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        sendJsonResponse(res, "{\"error\":\"FK lookup query failed\"}");
        return;
    };
    defer pg_result.deinit();

    var json_buf = std.ArrayList(u8){};
    const jw = json_buf.writer(allocator);
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

    sendJsonResponse(res, json_buf.items);
}

pub fn handleBulkUpdate(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (enforceReadOnly(state, res)) return;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = res.arena;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 400, "{\"error\":\"Invalid path\"}");
        return;
    };

    const tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        sendJsonError(res, 400, "{\"error\":\"Table not found\"}");
        return;
    };

    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };

    const column = utils.extractJsonField(allocator, body, "column") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing column field\"}");
        return;
    };
    const find_val = utils.extractJsonField(allocator, body, "find") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing find field\"}");
        return;
    };
    const replace_val = utils.extractJsonField(allocator, body, "replace") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing replace field\"}");
        return;
    };
    const force_str = utils.extractJsonField(allocator, body, "force") orelse "false";
    const force = std.mem.eql(u8, force_str, "true");

    if (!findColumnInTable(table_info, column)) {
        sendJsonError(res, 400, "{\"error\":\"Column not found\"}");
        return;
    }

    const esc_find = utils.escapeStringValue(allocator, find_val) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!force) {
        // Preview mode: count affected rows
        var cnt_sql = std.ArrayList(u8){};
        try cnt_sql.writer(allocator).print("SELECT COUNT(*) FROM \"{s}\" WHERE \"{s}\"::text = '{s}'", .{ table_name, column, esc_find });
        try cnt_sql.append(allocator, 0);
        const cnt_z: [*:0]const u8 = @ptrCast(cnt_sql.items[0 .. cnt_sql.items.len - 1 :0]);
        var cnt_result = pg_conn.runQuery(allocator, cnt_z) catch {
            sendJsonResponse(res, "{\"error\":\"Count query failed\"}");
            return;
        };
        defer cnt_result.deinit();
        var affected: usize = 0;
        if (cnt_result.n_rows > 0 and cnt_result.rows[0].len > 0) {
            affected = std.fmt.parseInt(usize, cnt_result.rows[0][0], 10) catch 0;
        }
        var resp_buf: [128]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"affected_rows\":{d},\"requires_confirmation\":true}}", .{affected}) catch "{\"error\":\"fmt\"}";
        sendJsonResponse(res, resp);
    } else {
        // Execute mode
        const esc_replace = utils.escapeStringValue(allocator, replace_val) catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };
        var upd_sql = std.ArrayList(u8){};
        try upd_sql.writer(allocator).print("UPDATE \"{s}\" SET \"{s}\" = '{s}' WHERE \"{s}\"::text = '{s}'", .{ table_name, column, esc_replace, column, esc_find });
        try upd_sql.append(allocator, 0);
        const upd_z: [*:0]const u8 = @ptrCast(upd_sql.items[0 .. upd_sql.items.len - 1 :0]);
        var upd_result = pg_conn.runQuery(allocator, upd_z) catch {
            var err_buf: [512]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&err_buf);
            const ew = fbs.writer();
            ew.writeAll("{\"error\":\"") catch return;
            utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
            ew.writeAll("\"}") catch return;
            sendJsonResponse(res, fbs.getWritten());
            return;
        };
        defer upd_result.deinit();

        if (addJournalEntry(state, table_name, "update", column, find_val, replace_val, "bulk", "")) |_| {} else |err| {
            log.warn("journal entry failed: {s}", .{@errorName(err)});
        }
        sendJsonResponse(res, "{\"success\":true}");
    }
}

pub fn handleTruncateTable(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (enforceReadOnly(state, res)) return;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing table name\"}");
        return;
    };
    if (table_name.len == 0) {
        sendJsonError(res, 400, "{\"error\":\"Empty table name\"}");
        return;
    }

    const tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"No schema available\"}");
        return;
    };
    if (findTableInSchema(tables, table_name) == null) {
        sendJsonError(res, 400, "{\"error\":\"Table not found in schema\"}");
        return;
    }

    var sql_buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&sql_buf);
    fbs.writer().print("TRUNCATE TABLE \"{s}\"", .{table_name}) catch {
        sendJsonError(res, 500, "{\"error\":\"Table name too long\"}");
        return;
    };
    const sql_len = fbs.pos;
    sql_buf[sql_len] = 0;
    const sql_z: [*:0]const u8 = sql_buf[0..sql_len :0];

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [1024]u8 = undefined;
        var efbs = std.io.fixedBufferStream(&err_buf);
        const ew = efbs.writer();
        ew.writeAll("{\"error\":\"TRUNCATE failed: ") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        sendJsonResponse(res, efbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    if (addJournalEntry(state, table_name, "truncate", "", "", "", "ALL", "")) |_| {} else |err| {
        log.warn("journal entry failed: {s}", .{@errorName(err)});
    }

    sendJsonResponse(res, "{\"ok\":true}");
}

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

test "validateCtid: rejects invalid formats" {
    try std.testing.expect(!validateCtid(""));
    try std.testing.expect(!validateCtid("abc"));
    try std.testing.expect(!validateCtid("(1)"));
    try std.testing.expect(!validateCtid("(,1)"));
    try std.testing.expect(!validateCtid("(1,)"));
    try std.testing.expect(!validateCtid("(a,1)"));
    try std.testing.expect(!validateCtid("1,2"));
}

test "addJournalEntry: adds entry" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.change_journal.items) |entry| {
            std.testing.allocator.free(entry.table_name);
            std.testing.allocator.free(entry.operation);
            std.testing.allocator.free(entry.column_name);
            std.testing.allocator.free(entry.old_value);
            std.testing.allocator.free(entry.new_value);
            std.testing.allocator.free(entry.pk_column);
            std.testing.allocator.free(entry.pk_value);
        }
        state.change_journal.deinit(std.testing.allocator);
    }
    const id = try addJournalEntry(&state, "users", "update", "name", "old", "new", "id", "1");
    try std.testing.expectEqual(@as(u64, 1), id);
    try std.testing.expectEqual(@as(usize, 1), state.change_journal.items.len);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
