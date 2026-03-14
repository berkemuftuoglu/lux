const std = @import("std");
const postgres = @import("postgres.zig");
const utils = @import("utils.zig");
const web = @import("web.zig");
const crud = @import("crud.zig");

const ServerState = web.ServerState;

const ExportError = error{
    OutOfMemory,
    FormatFailed,
};

const CsvParseError = error{
    EmptyCsv,
    NoDataRows,
    ColumnCountMismatch,
    OutOfMemory,
};

/// Write a single CSV field, quoting it if it contains commas, quotes, or newlines.
/// Internal double quotes are escaped by doubling them (RFC 4180).
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

/// Format query result as CSV text (RFC 4180 with CRLF line endings).
/// Caller owns the returned memory.
fn formatResultAsCsv(
    allocator: std.mem.Allocator,
    col_names: []const []const u8,
    rows: []const []const []const u8,
) ExportError![]u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    // Header row
    for (col_names, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
        escapeCsvField(w, name) catch return error.OutOfMemory;
    }
    w.writeAll("\r\n") catch return error.OutOfMemory;

    // Data rows
    for (rows) |row| {
        for (row, 0..) |val, i| {
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            escapeCsvField(w, val) catch return error.OutOfMemory;
        }
        w.writeAll("\r\n") catch return error.OutOfMemory;
    }

    return buf.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

/// Format query result as a JSON array of objects.
/// NULL values (the literal string "NULL" from libpq) are rendered as JSON null.
/// Caller owns the returned memory.
fn formatResultAsJson(
    allocator: std.mem.Allocator,
    col_names: []const []const u8,
    rows: []const []const []const u8,
) ExportError![]u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);

    w.writeByte('[') catch return error.OutOfMemory;

    for (rows, 0..) |row, ri| {
        if (ri > 0) w.writeByte(',') catch return error.OutOfMemory;
        w.writeByte('{') catch return error.OutOfMemory;

        for (row, 0..) |val, ci| {
            if (ci > 0) w.writeByte(',') catch return error.OutOfMemory;

            // Write key
            w.writeByte('"') catch return error.OutOfMemory;
            if (ci < col_names.len) {
                utils.writeJsonEscaped(w, col_names[ci]) catch return error.OutOfMemory;
            }
            w.writeAll("\":") catch return error.OutOfMemory;

            // Write value — NULL becomes JSON null, everything else is a string
            if (std.mem.eql(u8, val, "NULL")) {
                w.writeAll("null") catch return error.OutOfMemory;
            } else {
                w.writeByte('"') catch return error.OutOfMemory;
                utils.writeJsonEscaped(w, val) catch return error.OutOfMemory;
                w.writeByte('"') catch return error.OutOfMemory;
            }
        }

        w.writeByte('}') catch return error.OutOfMemory;
    }

    w.writeByte(']') catch return error.OutOfMemory;

    return buf.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

/// Parse a CSV string into a list of rows, each row being a list of field values.
/// Handles quoted fields (RFC 4180): double quotes inside quoted fields are escaped
/// by doubling them. Supports both \n and \r\n line endings.
/// Returns headers (first row) and data rows as separate allocations.
/// Caller must free each field, each row slice, and the headers/rows slices.
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

    var pos: usize = 0;
    while (pos < csv.len) {
        // Skip trailing whitespace-only content
        var check = pos;
        while (check < csv.len and (csv[check] == '\r' or csv[check] == '\n' or csv[check] == ' ')) check += 1;
        if (check >= csv.len) break;

        var fields = std.ArrayList([]const u8){};
        errdefer {
            for (fields.items) |f| allocator.free(f);
            fields.deinit(allocator);
        }

        // Parse one row
        while (true) {
            if (pos >= csv.len) break;

            if (csv[pos] == '"') {
                // Quoted field
                pos += 1; // skip opening quote
                var field_buf = std.ArrayList(u8){};
                errdefer field_buf.deinit(allocator);
                while (pos < csv.len) {
                    if (csv[pos] == '"') {
                        if (pos + 1 < csv.len and csv[pos + 1] == '"') {
                            // Escaped double quote
                            field_buf.append(allocator, '"') catch return error.OutOfMemory;
                            pos += 2;
                        } else {
                            // End of quoted field
                            pos += 1;
                            break;
                        }
                    } else {
                        field_buf.append(allocator, csv[pos]) catch return error.OutOfMemory;
                        pos += 1;
                    }
                }
                const owned = field_buf.toOwnedSlice(allocator) catch return error.OutOfMemory;
                fields.append(allocator, owned) catch return error.OutOfMemory;
            } else {
                // Unquoted field — read until comma or newline
                const start = pos;
                while (pos < csv.len and csv[pos] != ',' and csv[pos] != '\n' and csv[pos] != '\r') {
                    pos += 1;
                }
                const val = allocator.dupe(u8, csv[start..pos]) catch return error.OutOfMemory;
                fields.append(allocator, val) catch return error.OutOfMemory;
            }

            // After field: expect comma (more fields), newline (end of row), or EOF
            if (pos >= csv.len) break;
            if (csv[pos] == ',') {
                pos += 1; // skip comma, continue to next field
                continue;
            }
            if (csv[pos] == '\r') pos += 1; // skip CR
            if (pos < csv.len and csv[pos] == '\n') pos += 1; // skip LF
            break;
        }

        if (fields.items.len > 0) {
            const row_slice = fields.toOwnedSlice(allocator) catch return error.OutOfMemory;
            all_rows.append(allocator, row_slice) catch return error.OutOfMemory;
        } else {
            fields.deinit(allocator);
        }
    }

    if (all_rows.items.len == 0) return error.EmptyCsv;

    // First row is always returned as headers
    const headers = all_rows.items[0];

    if (all_rows.items.len < 2) {
        // Only headers, no data rows
        const empty_rows = allocator.alloc([]const []const u8, 0) catch return error.OutOfMemory;
        // Remove the header from all_rows so errdefer doesn't free it
        _ = all_rows.orderedRemove(0);
        return .{ .headers = headers, .rows = empty_rows };
    }

    // Copy data rows into a separate allocation
    const data_rows = allocator.alloc([]const []const u8, all_rows.items.len - 1) catch return error.OutOfMemory;
    for (all_rows.items[1..], 0..) |row, i| {
        data_rows[i] = row;
    }
    // Clear all_rows so errdefer doesn't double-free
    all_rows.clearRetainingCapacity();
    return .{ .headers = headers, .rows = data_rows };
}

fn buildAndExecuteInsert(
    allocator: std.mem.Allocator,
    pg_conn: *postgres.PgConnection,
    table_name: []const u8,
    col_names: []const []const u8,
    values: []const []const u8,
) bool {
    var sql_buf = std.ArrayList(u8){};
    defer sql_buf.deinit(allocator);
    const w = sql_buf.writer(allocator);

    w.print("INSERT INTO \"{s}\" (", .{table_name}) catch return false;
    for (col_names, 0..) |col, i| {
        if (i > 0) w.writeAll(", ") catch return false;
        w.print("\"{s}\"", .{col}) catch return false;
    }
    w.writeAll(") VALUES (") catch return false;
    for (values, 0..) |val, i| {
        if (i > 0) w.writeAll(", ") catch return false;
        if (val.len == 0) {
            w.writeAll("NULL") catch return false;
        } else {
            const escaped = utils.escapeStringValue(allocator, val) catch return false;
            defer allocator.free(escaped);
            w.print("'{s}'", .{escaped}) catch return false;
        }
    }
    w.writeAll(")") catch return false;
    sql_buf.append(allocator, 0) catch return false;

    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);
    var result = pg_conn.runQuery(allocator, sql_z) catch return false;
    result.deinit();
    return true;
}

pub fn handleExport(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/export/<name>
    const prefix = "/api/export/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    // Split off query string
    const path_part = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[0..qi] else after_prefix;
    const query_string = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[qi + 1 ..] else "";

    const table_name = path_part;
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

    // Parse format from query string
    var fmt_buf: [8]u8 = undefined;
    const format_param = utils.parseStringQueryParam(query_string, "format", &fmt_buf) orelse "csv";

    const is_csv = std.mem.eql(u8, format_param, "csv");
    const is_json = std.mem.eql(u8, format_param, "json");
    if (!is_csv and !is_json) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid format. Use csv or json.\"}");
        return;
    }

    // Connect to Postgres and run SELECT * FROM "<table>"
    const conninfo_z = state.conninfo_z orelse {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    };

    var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to connect to database\"}");
        return;
    };
    defer pg_conn.deinit();

    // Build query: SELECT * FROM "<table_name>"
    var sql_buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrintZ(&sql_buf, "SELECT * FROM \"{s}\"", .{table_name}) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Table name too long\"}");
        return;
    };

    var result = pg_conn.runQuery(allocator, sql) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Query failed\"}");
        return;
    };
    defer result.deinit();

    // Format the result
    if (is_csv) {
        const csv_data = formatResultAsCsv(allocator, result.col_names, result.rows) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format CSV\"}");
            return;
        };
        defer allocator.free(csv_data);

        // Build filename
        var filename_buf: [256]u8 = undefined;
        const filename = std.fmt.bufPrint(&filename_buf, "{s}.csv", .{table_name}) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Filename too long\"}");
            return;
        };

        try utils.sendResponseWithDownload(stream, "200 OK", "text/csv; charset=utf-8", filename, csv_data);
    } else {
        const json_data = formatResultAsJson(allocator, result.col_names, result.rows) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format JSON\"}");
            return;
        };
        defer allocator.free(json_data);

        // Build filename
        var filename_buf: [256]u8 = undefined;
        const filename = std.fmt.bufPrint(&filename_buf, "{s}.json", .{table_name}) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Filename too long\"}");
            return;
        };

        try utils.sendResponseWithDownload(stream, "200 OK", "application/json", filename, json_data);
    }
}

pub fn handleSqlExport(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse body
    const content_length = utils.findContentLength(request) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };
    if (content_length > 8192) {
        try utils.sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"SQL too large\"}");
        return;
    }
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
        return;
    };
    const body_start = header_end + 4;
    var body = request[body_start..];
    var extra_buf: [8192]u8 = undefined;
    if (body.len < content_length) {
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch {
                try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
                return;
            };
            if (n == 0) break;
            read_so_far += n;
        }
        body = extra_buf[0..read_so_far];
    } else {
        body = body[0..content_length];
    }

    const sql_text = utils.extractJsonField(body, "sql") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };
    if (sql_text.len == 0) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty SQL\"}");
        return;
    }

    // Block write operations in read-only mode
    const sql_guard = @import("sql_guard.zig");
    if (state.read_only and !sql_guard.isSqlReadSafe(sql_text)) {
        try utils.sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to export write operations.\"}");
        return;
    }

    const format = utils.extractJsonField(body, "format") orelse "csv";
    const is_csv = std.mem.eql(u8, format, "csv");
    const is_json = std.mem.eql(u8, format, "json");
    if (!is_csv and !is_json) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid format. Use csv or json.\"}");
        return;
    }

    // Build null-terminated SQL
    const sql_z = allocator.dupeZ(u8, sql_text) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(sql_z);

    // Connect and execute
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

    if (is_csv) {
        const csv_data = formatResultAsCsv(allocator, pg_result.col_names, pg_result.rows) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format CSV\"}");
            return;
        };
        defer allocator.free(csv_data);
        try utils.sendResponseWithDownload(stream, "200 OK", "text/csv; charset=utf-8", "export.csv", csv_data);
    } else {
        const json_data = formatResultAsJson(allocator, pg_result.col_names, pg_result.rows) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format JSON\"}");
            return;
        };
        defer allocator.free(json_data);
        try utils.sendResponseWithDownload(stream, "200 OK", "application/json", "export.json", json_data);
    }
}

pub fn handleTableDdl(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/ddl
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    const ddl_suffix = "/ddl";
    if (!std.mem.endsWith(u8, after_prefix, ddl_suffix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/ddl\"}");
        return;
    }

    const table_name = after_prefix[0 .. after_prefix.len - ddl_suffix.len];
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

    // Check if this is a view or table
    const escaped_name = utils.escapeStringValue(allocator, table_name) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_name);

    var sql_buf = std.ArrayList(u8){};
    defer sql_buf.deinit(allocator);
    const sw = sql_buf.writer(allocator);

    // Use a CASE expression to generate correct DDL for both tables and views
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
    try sw.writeAll(escaped_name);
    try sw.writeAll("' AND cls.relkind IN ('r', 'v') GROUP BY cls.relkind, cls.relname, cls.oid");
    try sql_buf.append(allocator, 0);
    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

    // Connect and execute
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

    // Extract DDL from result
    if (pg_result.n_rows == 0 or pg_result.rows.len == 0 or pg_result.rows[0].len == 0) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Could not generate DDL for table\"}");
        return;
    }

    const ddl_text = pg_result.rows[0][0];

    // Build JSON response
    var json_buf = std.ArrayList(u8){};
    defer json_buf.deinit(allocator);
    const w = json_buf.writer(allocator);
    try w.writeAll("{\"ddl\":\"");
    try utils.writeJsonEscaped(w, ddl_text);
    try w.writeAll("\"}");
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

pub fn handleCsvImport(
    stream: std.net.Stream,
    request: []const u8,
    path: []const u8,
    state: *ServerState,
) !void {
    if (try crud.enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/import
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    const import_suffix = "/import";
    if (!std.mem.endsWith(u8, after_prefix, import_suffix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/import\"}");
        return;
    }

    const table_name = after_prefix[0 .. after_prefix.len - import_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema
    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = crud.findTableInSchema(tables, table_name) orelse {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    };

    // Read request body
    const max_request_size = 8192;
    var extra_buf: [max_request_size]u8 = undefined;
    const body = crud.readRequestBody(stream, request, &extra_buf, max_request_size) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    // Extract CSV content from JSON body
    const csv_content = utils.extractJsonField(body, "csv") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing csv field\"}");
        return;
    };

    if (csv_content.len == 0) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty CSV content\"}");
        return;
    }

    // Check has_header (default true)
    const has_header_str = utils.extractJsonField(body, "has_header");
    const has_header = if (has_header_str) |h| !std.mem.eql(u8, h, "false") else true;

    // Parse CSV
    const csv_result = parseCsvContent(allocator, csv_content) catch |err| {
        const msg = switch (err) {
            error.EmptyCsv => "{\"error\":\"CSV content is empty\"}",
            error.NoDataRows => "{\"error\":\"CSV has no data rows\"}",
            error.ColumnCountMismatch => "{\"error\":\"CSV column count mismatch\"}",
            error.OutOfMemory => "{\"error\":\"Out of memory parsing CSV\"}",
        };
        try utils.sendResponse(stream, "400 Bad Request", "application/json", msg);
        return;
    };
    defer {
        for (csv_result.headers) |h| allocator.free(h);
        allocator.free(csv_result.headers);
        for (csv_result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(csv_result.rows);
    }

    // Determine column names for INSERT
    var col_names = std.ArrayList([]const u8){};
    defer col_names.deinit(allocator);

    if (has_header) {
        // Validate CSV headers match table columns
        for (csv_result.headers) |header| {
            if (!crud.findColumnInTable(table_info, header)) {
                var err_buf: [256]u8 = undefined;
                var fbs = std.io.fixedBufferStream(&err_buf);
                const ew = fbs.writer();
                ew.writeAll("{\"error\":\"CSV column '") catch return;
                utils.writeJsonEscaped(ew, header) catch return;
                ew.writeAll("' not found in table schema\"}") catch return;
                try utils.sendResponse(stream, "400 Bad Request", "application/json", fbs.getWritten());
                return;
            }
            col_names.append(allocator, header) catch {
                try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
        }
    } else {
        // No header row — the "headers" from parseCsv are actually data row 0
        // Use schema column names (first N columns matching field count)
        const field_count = csv_result.headers.len;
        if (field_count > table_info.columns.len) {
            try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"CSV has more columns than table\"}");
            return;
        }
        for (table_info.columns[0..field_count]) |col| {
            col_names.append(allocator, col.name) catch {
                try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
        }
    }

    // Connect to Postgres
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Begin transaction
    var begin_result = pg_conn.runQuery(allocator, "BEGIN") catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to start transaction\"}");
        return;
    };
    begin_result.deinit();

    var imported: usize = 0;
    var insert_error: bool = false;
    var error_msg_buf: [512]u8 = undefined;
    var error_msg_len: usize = 0;

    // Build INSERT for each row
    const data_rows = csv_result.rows;
    const num_cols = col_names.items.len;

    // If !has_header, we also need to insert the "headers" row as data
    if (!has_header) {
        const first_row = csv_result.headers;
        if (first_row.len >= num_cols) {
            if (buildAndExecuteInsert(allocator, &pg_conn, table_name, col_names.items, first_row[0..num_cols])) {
                imported += 1;
            } else {
                insert_error = true;
                const em = pg_conn.errorMessage();
                const copy_len = @min(em.len, error_msg_buf.len);
                @memcpy(error_msg_buf[0..copy_len], em[0..copy_len]);
                error_msg_len = copy_len;
            }
        }
    }

    if (!insert_error) {
        for (data_rows) |row| {
            const field_count = @min(row.len, num_cols);
            if (field_count == 0) continue;
            if (buildAndExecuteInsert(allocator, &pg_conn, table_name, col_names.items[0..field_count], row[0..field_count])) {
                imported += 1;
            } else {
                insert_error = true;
                const em = pg_conn.errorMessage();
                const copy_len = @min(em.len, error_msg_buf.len);
                @memcpy(error_msg_buf[0..copy_len], em[0..copy_len]);
                error_msg_len = copy_len;
                break;
            }
        }
    }

    if (insert_error) {
        // Rollback
        var rb = pg_conn.runQuery(allocator, "ROLLBACK") catch {
            try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Insert failed and rollback failed\"}");
            return;
        };
        rb.deinit();
        var resp_buf = std.ArrayList(u8){};
        defer resp_buf.deinit(allocator);
        const rw = resp_buf.writer(allocator);
        try rw.writeAll("{\"error\":\"Import failed: ");
        try utils.writeJsonEscaped(rw, error_msg_buf[0..error_msg_len]);
        try rw.writeAll("\"}");
        try utils.sendResponse(stream, "200 OK", "application/json", resp_buf.items);
        return;
    }

    // Commit
    var commit_result = pg_conn.runQuery(allocator, "COMMIT") catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Commit failed\"}");
        return;
    };
    commit_result.deinit();

    // Return success
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"imported\":{d}}}", .{imported}) catch "{\"error\":\"fmt\"}";
    try utils.sendResponse(stream, "200 OK", "application/json", resp);
}

pub fn handleTableStats(
    stream: std.net.Stream,
    path: []const u8,
    state: *ServerState,
) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/stats
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    const stats_suffix = "/stats";
    if (!std.mem.endsWith(u8, after_prefix, stats_suffix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/stats\"}");
        return;
    }

    const table_name = after_prefix[0 .. after_prefix.len - stats_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema (fail-closed: reject if schema not loaded)
    const schema_tables_fk = state.schema_tables orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
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
            try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    // Build stats query — works for both tables and views
    const escaped_name = utils.escapeStringValue(allocator, table_name) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_name);

    var sql_buf = std.ArrayList(u8){};
    defer sql_buf.deinit(allocator);
    const sw = sql_buf.writer(allocator);
    try sw.writeAll(
        "SELECT " ++
            "(SELECT count(*) FROM \"",
    );
    try sw.writeAll(escaped_name);
    try sw.writeAll(
        "\") AS row_count, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_relation_size(c.oid)) END AS table_size, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_indexes_size(c.oid)) END AS index_size, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_total_relation_size(c.oid)) END AS total_size " ++
            "FROM pg_class c " ++
            "JOIN pg_namespace n ON c.relnamespace = n.oid " ++
            "WHERE n.nspname = 'public' AND c.relname = '",
    );
    try sw.writeAll(escaped_name);
    try sw.writeAll("' AND c.relkind IN ('r', 'v')");
    try sql_buf.append(allocator, 0);
    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

    // Connect and execute
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

    if (pg_result.n_rows == 0) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Could not retrieve table stats\"}");
        return;
    }

    // Extract values: row_estimate, table_size, index_size, total_size
    const row = pg_result.rows[0];
    const row_estimate = if (row.len > 0) row[0] else "0";
    const table_size = if (row.len > 1) row[1] else "0 bytes";
    const index_size = if (row.len > 2) row[2] else "0 bytes";
    const total_size = if (row.len > 3) row[3] else "0 bytes";

    // Build JSON response
    var json_buf = std.ArrayList(u8){};
    defer json_buf.deinit(allocator);
    const w = json_buf.writer(allocator);
    try w.writeAll("{\"row_estimate\":");
    try w.writeAll(row_estimate);
    try w.writeAll(",\"table_size\":\"");
    try utils.writeJsonEscaped(w, table_size);
    try w.writeAll("\",\"index_size\":\"");
    try utils.writeJsonEscaped(w, index_size);
    try w.writeAll("\",\"total_size\":\"");
    try utils.writeJsonEscaped(w, total_size);
    try w.writeAll("\"}");

    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

// Tests

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

test "formatResultAsCsv: basic table" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "id", "name" };
    const row1 = [_][]const u8{ "1", "Alice" };
    const rows = [_][]const []const u8{&row1};
    const csv = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(csv);
    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, csv, "1,Alice\r\n") != null);
}

test "formatResultAsCsv: escapes special fields" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"note"};
    const row1 = [_][]const u8{"a,b"};
    const rows = [_][]const []const u8{&row1};
    const csv = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(csv);
    try std.testing.expect(std.mem.indexOf(u8, csv, "\"a,b\"") != null);
}

test "formatResultAsCsv: empty rows" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "id", "name" };
    const rows = [_][]const []const u8{};
    const csv = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(csv);
    try std.testing.expect(std.mem.indexOf(u8, csv, "id,name\r\n") != null);
}

test "formatResultAsCsv: single column no rows" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"id"};
    const rows = [_][]const []const u8{};
    const csv = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(csv);
    try std.testing.expectEqualStrings("id\r\n", csv);
}

test "formatResultAsCsv: multiple columns with special chars" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "name", "desc" };
    const row1 = [_][]const u8{ "Alice", "says \"hi\"" };
    const rows = [_][]const []const u8{&row1};
    const csv = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(csv);
    try std.testing.expect(std.mem.indexOf(u8, csv, "\"says \"\"hi\"\"\"") != null);
}

test "formatResultAsJson: basic table" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "id", "name" };
    const row1 = [_][]const u8{ "1", "Alice" };
    const rows = [_][]const []const u8{&row1};
    const json = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":\"1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"Alice\"") != null);
}

test "formatResultAsJson: empty rows" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "id", "name" };
    const rows = [_][]const []const u8{};
    const json = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(json);
    try std.testing.expectEqualStrings("[]", json);
}

test "formatResultAsJson: escapes special chars in values" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"note"};
    const row1 = [_][]const u8{"say \"hi\""};
    const rows = [_][]const []const u8{&row1};
    const json = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\\\"hi\\\"") != null);
}

test "formatResultAsJson: NULL value" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"val"};
    const row1 = [_][]const u8{"NULL"};
    const rows = [_][]const []const u8{&row1};
    const json = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "null") != null);
}

test "formatResultAsJson: empty column names" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{""};
    const row1 = [_][]const u8{"val"};
    const rows = [_][]const []const u8{&row1};
    const json = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"\":\"val\"") != null);
}

test "formatResultAsJson: value with special JSON chars" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"data"};
    const row1 = [_][]const u8{"line1\nline2\ttab"};
    const rows = [_][]const []const u8{&row1};
    const json = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\\n") != null);
}

test "parseCsvContent: simple two-column CSV" {
    const allocator = std.testing.allocator;
    const csv = "name,age\nAlice,30\nBob,25\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 2), result.headers.len);
    try std.testing.expectEqualStrings("name", result.headers[0]);
    try std.testing.expectEqualStrings("age", result.headers[1]);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
}

test "parseCsvContent: quoted fields with commas" {
    const allocator = std.testing.allocator;
    const csv = "a,b\n\"hello,world\",test\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqualStrings("hello,world", result.rows[0][0]);
}

test "parseCsvContent: escaped double quotes in field" {
    const allocator = std.testing.allocator;
    const csv = "a\n\"say \"\"hi\"\"\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqualStrings("say \"hi\"", result.rows[0][0]);
}

test "parseCsvContent: CRLF line endings" {
    const allocator = std.testing.allocator;
    const csv = "a,b\r\n1,2\r\n3,4\r\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
}

test "parseCsvContent: empty input returns error" {
    const allocator = std.testing.allocator;
    const result = parseCsvContent(allocator, "");
    try std.testing.expectError(error.EmptyCsv, result);
}

test "parseCsvContent: headers only no data rows" {
    const allocator = std.testing.allocator;
    const csv = "a,b,c\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 3), result.headers.len);
    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
}

test "parseCsvContent: single column single row" {
    const allocator = std.testing.allocator;
    const csv = "id\n42\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.headers.len);
    try std.testing.expectEqualStrings("id", result.headers[0]);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("42", result.rows[0][0]);
}

test "parseCsvContent: no trailing newline" {
    const allocator = std.testing.allocator;
    const csv = "a,b\n1,2";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("1", result.rows[0][0]);
    try std.testing.expectEqualStrings("2", result.rows[0][1]);
}

test "parseCsvContent: quoted field with newline inside" {
    const allocator = std.testing.allocator;
    const csv = "a\n\"line1\nline2\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("line1\nline2", result.rows[0][0]);
}

test "parseCsvContent: multiple data rows" {
    const allocator = std.testing.allocator;
    const csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqualStrings("Charlie", result.rows[2][1]);
}

test "parseCsvContent: empty field between commas" {
    const allocator = std.testing.allocator;
    const csv = "a,b,c\n1,,3\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("", result.rows[0][1]);
}

test "parseCsvContent: whitespace-only lines at end are skipped" {
    const allocator = std.testing.allocator;
    const csv = "a\n1\n   \n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    // The whitespace-only line is not treated as a data row
    try std.testing.expect(result.rows.len <= 2);
}

test "parseCsvContent: only whitespace returns error" {
    const allocator = std.testing.allocator;
    const result = parseCsvContent(allocator, "   \n\r\n  ");
    try std.testing.expectError(error.EmptyCsv, result);
}

test "parseCsvContent: single column multiple rows" {
    const allocator = std.testing.allocator;
    const csv = "name\nAlice\nBob\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.headers.len);
    try std.testing.expectEqualStrings("name", result.headers[0]);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("Alice", result.rows[0][0]);
    try std.testing.expectEqualStrings("Bob", result.rows[1][0]);
}

test "parseCsvContent: field with only escaped quotes" {
    const allocator = std.testing.allocator;
    const csv = "val\n\"\"\"\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("\"", result.rows[0][0]);
}

test "parseCsvContent: consecutive commas produce empty fields" {
    const allocator = std.testing.allocator;
    const csv = "a,b,c\n,,\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(usize, 3), result.rows[0].len);
    try std.testing.expectEqualStrings("", result.rows[0][0]);
    try std.testing.expectEqualStrings("", result.rows[0][1]);
    try std.testing.expectEqualStrings("", result.rows[0][2]);
}

test "parseCsvContent: very long field value" {
    const allocator = std.testing.allocator;
    const long_val = "x" ** 1000;
    const csv = "data\n" ++ long_val ++ "\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1000), result.rows[0][0].len);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
