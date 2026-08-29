// Response strings passed to sendJsonResponse / sendJsonError / res.body / res.header
// MUST come from res.arena, a string literal, or toOwnedSlice — never a stack array.
// The arena outlives the handler; stack buffers are reclaimed on function return,
// leaving sendJsonResponse with a dangling pointer (manifests as garbled / truncated JSON).
const std = @import("std");
const httpz = @import("httpz");
const pg = @import("pg");
const postgres = @import("postgres");
const utils = @import("utils");
const web = @import("web");
const crud = @import("crud");

const ExportError = error{
    OutOfMemory,
    FormatFailed,
};

const CsvParseError = error{
    EmptyCsv,
    NoDataRows,
    ColumnCountMismatch,
    UnclosedQuote,
    MalformedQuote,
    OutOfMemory,
};

const sendJsonResponse = web.sendJsonResponse;
const sendJsonError = web.sendJsonError;

fn escapeCsvField(writer: anytype, field: []const u8) !void {
    var needs_quoting = false;
    for (field) |ch| {
        if (ch == ',' or ch == '"' or ch == '\n' or ch == '\r') {
            needs_quoting = true;
            break;
        }
    }

    if (!needs_quoting) {
        try writer.writeAll(field);
        return;
    }

    try writer.writeByte('"');
    for (field) |ch| {
        if (ch == '"') {
            try writer.writeAll("\"\"");
        } else {
            try writer.writeByte(ch);
        }
    }
    try writer.writeByte('"');
}

fn formatResultAsCsv(
    allocator: std.mem.Allocator,
    col_names: []const []const u8,
    rows: []const []const []const u8,
) ExportError![]u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    for (col_names, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
        escapeCsvField(w, name) catch return error.OutOfMemory;
    }
    w.writeAll("\r\n") catch return error.OutOfMemory;

    for (rows) |row| {
        for (row, 0..) |val, i| {
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            escapeCsvField(w, val) catch return error.OutOfMemory;
        }
        w.writeAll("\r\n") catch return error.OutOfMemory;
    }

    return buf.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

fn formatResultAsJson(
    allocator: std.mem.Allocator,
    col_names: []const []const u8,
    rows: []const []const []const u8,
) ExportError![]u8 {
    var jw = utils.JsonWriter.init(allocator);
    const s = jw.writer();

    s.beginArray() catch return error.OutOfMemory;
    for (rows) |row| {
        s.beginObject() catch return error.OutOfMemory;
        for (row, 0..) |val, ci| {
            if (ci < col_names.len) {
                s.objectField(col_names[ci]) catch return error.OutOfMemory;
            } else {
                s.objectField("") catch return error.OutOfMemory;
            }
            utils.writeSqlValue(s, val) catch return error.OutOfMemory;
        }
        s.endObject() catch return error.OutOfMemory;
    }
    s.endArray() catch return error.OutOfMemory;

    return jw.toOwnedSlice() catch return error.OutOfMemory;
}

fn parseCsvContent(
    allocator: std.mem.Allocator,
    csv: []const u8,
) CsvParseError!struct { headers: [][]const u8, rows: [][]const []const u8 } {
    if (csv.len == 0) return error.EmptyCsv;

    var all_rows = std.ArrayList([][]const u8){};
    defer all_rows.deinit(allocator);
    errdefer {
        for (all_rows.items) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
    }

    // Strip UTF-8 BOM (Excel/Google Sheets add this)
    var pos: usize = if (csv.len >= 3 and csv[0] == 0xEF and csv[1] == 0xBB and csv[2] == 0xBF) 3 else 0;

    var expected_fields: ?usize = null;

    while (pos < csv.len) {
        var check = pos;
        while (check < csv.len and (csv[check] == '\r' or csv[check] == '\n' or csv[check] == ' ')) check += 1;
        if (check >= csv.len) break;

        var fields = std.ArrayList([]const u8){};
        errdefer {
            for (fields.items) |f| allocator.free(f);
            fields.deinit(allocator);
        }

        while (true) {
            if (pos >= csv.len) break;

            if (csv[pos] == '"') {
                pos += 1;
                var field_buf = std.ArrayList(u8){};
                errdefer field_buf.deinit(allocator);
                var closed = false;
                while (pos < csv.len) {
                    if (csv[pos] == '"') {
                        if (pos + 1 < csv.len and csv[pos + 1] == '"') {
                            field_buf.append(allocator, '"') catch return error.OutOfMemory;
                            pos += 2;
                        } else {
                            pos += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        field_buf.append(allocator, csv[pos]) catch return error.OutOfMemory;
                        pos += 1;
                    }
                }
                if (!closed) return error.UnclosedQuote;
                // Validate: only comma, CR, LF, or EOF may follow a closing quote
                if (pos < csv.len and csv[pos] != ',' and csv[pos] != '\r' and csv[pos] != '\n') {
                    return error.MalformedQuote;
                }
                const owned = field_buf.toOwnedSlice(allocator) catch return error.OutOfMemory;
                fields.append(allocator, owned) catch return error.OutOfMemory;
            } else {
                const start = pos;
                while (pos < csv.len and csv[pos] != ',' and csv[pos] != '\n' and csv[pos] != '\r') {
                    pos += 1;
                }
                const val = allocator.dupe(u8, csv[start..pos]) catch return error.OutOfMemory;
                errdefer allocator.free(val);
                fields.append(allocator, val) catch return error.OutOfMemory;
            }

            if (pos >= csv.len) break;
            if (csv[pos] == ',') {
                pos += 1;
                continue;
            }
            if (csv[pos] == '\r') pos += 1;
            if (pos < csv.len and csv[pos] == '\n') pos += 1;
            break;
        }

        if (fields.items.len > 0) {
            // Validate consistent field count
            if (expected_fields) |ef| {
                if (fields.items.len != ef) return error.ColumnCountMismatch;
            } else {
                expected_fields = fields.items.len;
            }
            const row_slice = fields.toOwnedSlice(allocator) catch return error.OutOfMemory;
            all_rows.append(allocator, row_slice) catch return error.OutOfMemory;
        } else {
            fields.deinit(allocator);
        }
    }

    if (all_rows.items.len == 0) return error.EmptyCsv;

    const headers = all_rows.items[0];

    if (all_rows.items.len < 2) {
        const empty_rows = allocator.alloc([]const []const u8, 0) catch return error.OutOfMemory;
        _ = all_rows.orderedRemove(0);
        return .{ .headers = headers, .rows = empty_rows };
    }

    const data_rows = allocator.alloc([]const []const u8, all_rows.items.len - 1) catch return error.OutOfMemory;
    for (all_rows.items[1..], 0..) |row, i| {
        data_rows[i] = row;
    }
    all_rows.clearRetainingCapacity();
    return .{ .headers = headers, .rows = data_rows };
}

fn buildAndExecuteInsert(
    allocator: std.mem.Allocator,
    conn: *pg.Conn,
    table_name: []const u8,
    col_names: []const []const u8,
    values: []const []const u8,
) bool {
    var sql_buf = std.ArrayList(u8){};
    defer sql_buf.deinit(allocator);
    const w = sql_buf.writer(allocator);

    const esc_tbl = utils.escapeIdentifier(allocator, table_name) catch return false;
    w.print("INSERT INTO \"{s}\" (", .{esc_tbl}) catch return false;
    for (col_names, 0..) |col, i| {
        if (i > 0) w.writeAll(", ") catch return false;
        const esc_col = utils.escapeIdentifier(allocator, col) catch return false;
        w.print("\"{s}\"", .{esc_col}) catch return false;
    }
    w.writeAll(") VALUES (") catch return false;
    for (values, 0..) |val, i| {
        if (i > 0) w.writeAll(", ") catch return false;
        if (std.mem.eql(u8, val, "\\N")) {
            // \N is PostgreSQL COPY convention for NULL
            w.writeAll("NULL") catch return false;
        } else {
            const escaped = utils.escapeStringValue(allocator, val) catch return false;
            defer allocator.free(escaped);
            w.print("'{s}'", .{escaped}) catch return false;
        }
    }
    w.writeAll(")") catch return false;

    var result = postgres.runQueryOnConn(conn, allocator, sql_buf.items) catch return false;
    result.deinit();
    return true;
}

pub fn handleExport(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    // Schema arenas and the pool are torn down by connect/refresh on another
    // worker thread; hold the read side for the life of the handler so the
    // TableInfo strings we borrow cannot be freed mid-request.
    state.schema_lock.lockShared();
    defer state.schema_lock.unlockShared();
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const pool = state.pool.?;

    const table_name = req.param("table_name") orelse {
        sendJsonError(res, 404, "{\"error\":\"Invalid path\"}");
        return;
    };
    if (table_name.len == 0 or table_name.len > 128) {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    }

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

    const qs = try req.query();
    const format_param = qs.get("format") orelse "csv";

    const is_csv = std.mem.eql(u8, format_param, "csv");
    const is_json = std.mem.eql(u8, format_param, "json");
    if (!is_csv and !is_json) {
        sendJsonError(res, 400, "{\"error\":\"Invalid format. Use csv or json.\"}");
        return;
    }

    const esc_table_id = utils.escapeIdentifier(allocator, table_name) catch {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    };
    const sql = std.fmt.allocPrint(allocator, "SELECT * FROM \"{s}\"", .{esc_table_id}) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var result = postgres.runQuery(pool, allocator, sql) catch {
        sendJsonResponse(res, "{\"error\":\"Query failed\"}");
        return;
    };
    defer result.deinit();

    if (is_csv) {
        const csv_data = formatResultAsCsv(allocator, result.col_names, result.rows) catch {
            sendJsonError(res, 500, "{\"error\":\"Failed to format CSV\"}");
            return;
        };

        const filename = std.fmt.allocPrint(allocator, "{s}.csv", .{table_name}) catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };

        res.content_type = httpz.ContentType.CSV;
        res.body = csv_data;
        res.header("content-disposition", try std.fmt.allocPrint(allocator, "attachment; filename=\"{s}\"", .{filename}));
    } else {
        const json_data = formatResultAsJson(allocator, result.col_names, result.rows) catch {
            sendJsonError(res, 500, "{\"error\":\"Failed to format JSON\"}");
            return;
        };

        const filename = std.fmt.allocPrint(allocator, "{s}.json", .{table_name}) catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };

        res.content_type = httpz.ContentType.JSON;
        res.body = json_data;
        res.header("content-disposition", try std.fmt.allocPrint(allocator, "attachment; filename=\"{s}\"", .{filename}));
    }
}

pub fn handleSqlExport(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    // Schema arenas and the pool are torn down by connect/refresh on another
    // worker thread; hold the read side for the life of the handler so the
    // TableInfo strings we borrow cannot be freed mid-request.
    state.schema_lock.lockShared();
    defer state.schema_lock.unlockShared();
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const pool = state.pool.?;
    const obj = (try req.jsonObject()) orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };

    const sql_text = utils.getJsonString(obj, "sql") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing sql field\"}");
        return;
    };
    if (sql_text.len == 0) {
        sendJsonError(res, 400, "{\"error\":\"Empty SQL\"}");
        return;
    }

    const sql_guard_mod = @import("sql_guard");
    if (state.flags.read_only and !sql_guard_mod.isSqlReadSafe(sql_text)) {
        sendJsonError(res, 403, "{\"error\":\"Read-only mode is enabled. Disable it to export write operations.\"}");
        return;
    }

    const format = utils.getJsonString(obj, "format") orelse "csv";
    const is_csv = std.mem.eql(u8, format, "csv");
    const is_json = std.mem.eql(u8, format, "json");
    if (!is_csv and !is_json) {
        sendJsonError(res, 400, "{\"error\":\"Invalid format. Use csv or json.\"}");
        return;
    }

    var pg_result = postgres.runQuery(pool, allocator, sql_text) catch {
        sendJsonResponse(res, "{\"error\":\"Query failed\"}");
        return;
    };
    defer pg_result.deinit();

    if (is_csv) {
        const csv_data = formatResultAsCsv(allocator, pg_result.col_names, pg_result.rows) catch {
            sendJsonError(res, 500, "{\"error\":\"Failed to format CSV\"}");
            return;
        };
        res.content_type = httpz.ContentType.CSV;
        res.body = csv_data;
        res.header("content-disposition", "attachment; filename=\"export.csv\"");
    } else {
        const json_data = formatResultAsJson(allocator, pg_result.col_names, pg_result.rows) catch {
            sendJsonError(res, 500, "{\"error\":\"Failed to format JSON\"}");
            return;
        };
        res.content_type = httpz.ContentType.JSON;
        res.body = json_data;
        res.header("content-disposition", "attachment; filename=\"export.json\"");
    }
}

pub fn handleTableDdl(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    // Schema arenas and the pool are torn down by connect/refresh on another
    // worker thread; hold the read side for the life of the handler so the
    // TableInfo strings we borrow cannot be freed mid-request.
    state.schema_lock.lockShared();
    defer state.schema_lock.unlockShared();
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const pool = state.pool.?;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 404, "{\"error\":\"Invalid path\"}");
        return;
    };
    if (table_name.len == 0 or table_name.len > 128) {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    }

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

    const escaped_val = utils.escapeStringValue(allocator, table_name) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var sql_buf = std.ArrayList(u8){};
    const sw = sql_buf.writer(allocator);

    try sw.writeAll(
        "SELECT CASE WHEN cls.relkind = 'v' THEN " ++
            "'CREATE OR REPLACE VIEW \"' || cls.relname || '\" AS' || chr(10) || pg_get_viewdef(cls.oid, true) " ++
            "ELSE " ++
            "'CREATE TABLE \"' || cls.relname || '\" (' || chr(10) || " ++
            "string_agg('  \"' || c.column_name || '\" ' || c.data_type || " ++
            "CASE WHEN c.character_maximum_length IS NOT NULL THEN '(' || c.character_maximum_length || ')' ELSE '' END || " ++
            "CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END || " ++
            "CASE WHEN c.column_default IS NOT NULL THEN ' DEFAULT ' || c.column_default ELSE '' END" ++
            ", ',' || chr(10) ORDER BY c.ordinal_position) || chr(10) || ');' " ++
            "END " ++
            "FROM pg_class cls " ++
            "JOIN pg_namespace ns ON cls.relnamespace = ns.oid " ++
            "LEFT JOIN information_schema.columns c ON c.table_schema = ns.nspname AND c.table_name = cls.relname " ++
            "WHERE ns.nspname = 'public' AND cls.relname = '",
    );
    try sw.writeAll(escaped_val);
    try sw.writeAll("' AND cls.relkind IN ('r', 'v') GROUP BY cls.relkind, cls.relname, cls.oid");

    var pg_result = postgres.runQuery(pool, allocator, sql_buf.items) catch {
        sendJsonResponse(res, "{\"error\":\"DDL query failed\"}");
        return;
    };
    defer pg_result.deinit();

    if (pg_result.n_rows == 0 or pg_result.rows.len == 0 or pg_result.rows[0].len == 0) {
        sendJsonResponse(res, "{\"error\":\"Could not generate DDL for table\"}");
        return;
    }

    const ddl_text = pg_result.rows[0][0];

    var jw = utils.JsonWriter.init(allocator);
    const s = jw.writer();
    try s.beginObject();
    try s.objectField("ddl");
    try s.write(ddl_text);
    try s.endObject();
    sendJsonResponse(res, try jw.toOwnedSlice());
}

pub fn handleCsvImport(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    // Schema arenas and the pool are torn down by connect/refresh on another
    // worker thread; hold the read side for the life of the handler so the
    // TableInfo strings we borrow cannot be freed mid-request.
    state.schema_lock.lockShared();
    defer state.schema_lock.unlockShared();
    if (crud.enforceReadOnly(state, res)) return;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const pool = state.pool.?;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 404, "{\"error\":\"Invalid path\"}");
        return;
    };
    if (table_name.len == 0 or table_name.len > 128) {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    }

    const tables = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = crud.findTableInSchema(tables, table_name) orelse {
        sendJsonError(res, 404, "{\"error\":\"Table not found in schema\"}");
        return;
    };

    const obj = (try req.jsonObject()) orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const csv_content = utils.getJsonString(obj, "csv") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing csv field\"}");
        return;
    };

    if (csv_content.len == 0) {
        sendJsonError(res, 400, "{\"error\":\"Empty CSV content\"}");
        return;
    }

    const has_header_str = utils.getJsonString(obj, "has_header");
    const has_header = if (has_header_str) |h| !std.mem.eql(u8, h, "false") else true;

    const csv_result = parseCsvContent(allocator, csv_content) catch |err| {
        const msg = switch (err) {
            error.EmptyCsv => "{\"error\":\"CSV content is empty\"}",
            error.NoDataRows => "{\"error\":\"CSV has no data rows\"}",
            error.ColumnCountMismatch => "{\"error\":\"CSV rows have inconsistent column counts\"}",
            error.UnclosedQuote => "{\"error\":\"CSV has an unclosed quoted field\"}",
            error.MalformedQuote => "{\"error\":\"CSV has invalid characters after a closing quote\"}",
            error.OutOfMemory => "{\"error\":\"Out of memory parsing CSV\"}",
        };
        sendJsonError(res, 400, msg);
        return;
    };

    var col_names = std.ArrayList([]const u8){};

    if (has_header) {
        for (csv_result.headers) |header| {
            if (!crud.findColumnInTable(table_info, header)) {
                const err_msg = std.fmt.allocPrint(allocator, "CSV column '{s}' not found in table schema", .{header}) catch {
                    sendJsonError(res, 400, "{\"error\":\"CSV column not found in table schema\"}");
                    return;
                };
                var jw = utils.JsonWriter.init(allocator);
                const s = jw.writer();
                s.beginObject() catch return;
                s.objectField("error") catch return;
                s.write(err_msg) catch return;
                s.endObject() catch return;
                sendJsonError(res, 400, jw.toOwnedSlice() catch return);
                return;
            }
            col_names.append(allocator, header) catch {
                sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
                return;
            };
        }
    } else {
        const field_count = csv_result.headers.len;
        if (field_count > table_info.columns.len) {
            sendJsonError(res, 400, "{\"error\":\"CSV has more columns than table\"}");
            return;
        }
        for (table_info.columns[0..field_count]) |col| {
            col_names.append(allocator, col.name) catch {
                sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
                return;
            };
        }
    }

    // Acquire a dedicated connection for the transaction
    var conn = pool.acquire() catch {
        sendJsonResponse(res, "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer conn.release();

    conn.begin() catch {
        sendJsonResponse(res, "{\"error\":\"Failed to start transaction\"}");
        return;
    };

    var imported: usize = 0;
    var insert_error: bool = false;

    const data_rows = csv_result.rows;
    const num_cols = col_names.items.len;

    if (!has_header) {
        const first_row = csv_result.headers;
        if (first_row.len >= num_cols) {
            if (buildAndExecuteInsert(allocator, conn, table_name, col_names.items, first_row[0..num_cols])) {
                imported += 1;
            } else {
                insert_error = true;
            }
        }
    }

    if (!insert_error) {
        for (data_rows) |row| {
            const field_count = @min(row.len, num_cols);
            if (field_count == 0) continue;
            if (buildAndExecuteInsert(allocator, conn, table_name, col_names.items[0..field_count], row[0..field_count])) {
                imported += 1;
            } else {
                insert_error = true;
                break;
            }
        }
    }

    if (insert_error) {
        conn.rollback() catch {
            sendJsonResponse(res, "{\"error\":\"Insert failed and rollback failed\"}");
            return;
        };
        sendJsonResponse(res, "{\"error\":\"Import failed\"}");
        return;
    }

    conn.commit() catch {
        sendJsonResponse(res, "{\"error\":\"Commit failed\"}");
        return;
    };

    const resp = std.fmt.allocPrint(res.arena, "{{\"imported\":{d}}}", .{imported}) catch {
        sendJsonResponse(res, "{\"imported\":0}");
        return;
    };
    sendJsonResponse(res, resp);
}

pub fn handleTableStats(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    // Schema arenas and the pool are torn down by connect/refresh on another
    // worker thread; hold the read side for the life of the handler so the
    // TableInfo strings we borrow cannot be freed mid-request.
    state.schema_lock.lockShared();
    defer state.schema_lock.unlockShared();
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = res.arena;
    const pool = state.pool.?;

    const table_name = req.param("table_path") orelse {
        sendJsonError(res, 404, "{\"error\":\"Invalid path\"}");
        return;
    };
    if (table_name.len == 0 or table_name.len > 128) {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    }

    const schema_tables_fk = state.schema_tables orelse {
        sendJsonError(res, 400, "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
        return;
    };
    {
        var found = false;
        for (schema_tables_fk) |t| {
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

    const escaped_id = utils.escapeIdentifier(allocator, table_name) catch {
        sendJsonError(res, 400, "{\"error\":\"Invalid table name\"}");
        return;
    };
    const escaped_val = utils.escapeStringValue(allocator, table_name) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var sql_buf = std.ArrayList(u8){};
    const sw = sql_buf.writer(allocator);
    try sw.writeAll(
        "SELECT " ++
            "(SELECT count(*)::text FROM \"",
    );
    try sw.writeAll(escaped_id);
    try sw.writeAll(
        "\") AS row_count, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_relation_size(c.oid)) END AS table_size, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_indexes_size(c.oid)) END AS index_size, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_total_relation_size(c.oid)) END AS total_size " ++
            "FROM pg_class c " ++
            "JOIN pg_namespace n ON c.relnamespace = n.oid " ++
            "WHERE n.nspname = 'public' AND c.relname = '",
    );
    try sw.writeAll(escaped_val);
    try sw.writeAll("' AND c.relkind IN ('r', 'v')");

    var pg_result = postgres.runQuery(pool, allocator, sql_buf.items) catch {
        sendJsonResponse(res, "{\"error\":\"Stats query failed\"}");
        return;
    };
    defer pg_result.deinit();

    if (pg_result.n_rows == 0) {
        sendJsonResponse(res, "{\"error\":\"Could not retrieve table stats\"}");
        return;
    }

    const row = pg_result.rows[0];
    const row_estimate = if (row.len > 0) row[0] else "0";
    const table_size = if (row.len > 1) row[1] else "0 bytes";
    const index_size = if (row.len > 2) row[2] else "0 bytes";
    const total_size = if (row.len > 3) row[3] else "0 bytes";

    // row_estimate is a numeric string from SQL — write it as a raw number
    const row_count_num = std.fmt.parseInt(i64, row_estimate, 10) catch 0;

    var jw = utils.JsonWriter.init(allocator);
    const s = jw.writer();
    try s.beginObject();
    try s.objectField("row_estimate");
    try s.write(row_count_num);
    try s.objectField("table_size");
    try s.write(table_size);
    try s.objectField("index_size");
    try s.write(index_size);
    try s.objectField("total_size");
    try s.write(total_size);
    try s.endObject();

    sendJsonResponse(res, try jw.toOwnedSlice());
}

test "escapeCsvField: plain text passes through" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "hello");
    try std.testing.expectEqualStrings("hello", buf.items);
}

test "escapeCsvField: field with comma is quoted" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "hello,world");
    try std.testing.expectEqualStrings("\"hello,world\"", buf.items);
}

test "escapeCsvField: field with double quote is quoted and escaped" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "say \"hi\"");
    try std.testing.expectEqualStrings("\"say \"\"hi\"\"\"", buf.items);
}

test "escapeCsvField: field with newline is quoted" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "line1\nline2");
    try std.testing.expectEqualStrings("\"line1\nline2\"", buf.items);
}

test "escapeCsvField: field with carriage return is quoted" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "a\rb");
    try std.testing.expectEqualStrings("\"a\rb\"", buf.items);
}

test "escapeCsvField: empty string passes through" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "");
    try std.testing.expectEqualStrings("", buf.items);
}

test "escapeCsvField: field with all special chars" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "a,b\"c\nd");
    try std.testing.expect(buf.items[0] == '"');
}

test "escapeCsvField: NULL literal passes through" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "NULL");
    try std.testing.expectEqualStrings("NULL", buf.items);
}

test "escapeCsvField: field containing only a double quote" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "\"");
    try std.testing.expectEqualStrings("\"\"\"\"", buf.items);
}

test "escapeCsvField: field with comma and quote" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "a,\"b\"");
    try std.testing.expect(buf.items[0] == '"');
}

test "escapeCsvField: field with CRLF" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try escapeCsvField(buf.writer(std.testing.allocator), "a\r\nb");
    try std.testing.expect(buf.items[0] == '"');
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
