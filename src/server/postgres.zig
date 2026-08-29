const std = @import("std");
const pg = @import("pg");
const pg_decode = @import("pg_decode");

const log = std.log.scoped(.postgres);

pub const PgError = error{
    ConnectionFailed,
    QueryFailed,
    OutOfMemory,
    NoResults,
    InvalidData,
};

pub const PgErrorFields = struct {
    severity: ?[]const u8,
    code: ?[]const u8,
    message: ?[]const u8,
    detail: ?[]const u8,
    hint: ?[]const u8,
};

pub const QueryResult = struct {
    col_names: [][]const u8,
    rows: [][][]const u8,
    n_cols: usize,
    n_rows: usize,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *QueryResult) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const ColumnInfo = struct {
    name: []const u8,
    data_type: []const u8,
};

pub const TableInfo = struct {
    name: []const u8,
    columns: []ColumnInfo,
};

pub const EnhancedColumnInfo = struct {
    name: []const u8,
    data_type: []const u8,
    is_primary_key: bool,
    is_nullable: bool,
    column_default: ?[]const u8,
    fk_target_table: ?[]const u8,
    fk_target_column: ?[]const u8,
    enum_values: ?[][]const u8,
};

pub const EnhancedTableInfo = struct {
    name: []const u8,
    columns: []EnhancedColumnInfo,
    primary_key_columns: [][]const u8,
    has_primary_key: bool,
};

pub const EnhancedSchemaInfo = struct {
    tables: []EnhancedTableInfo,
    arena: std.heap.ArenaAllocator,

    pub fn deinit(self: *EnhancedSchemaInfo) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const SchemaInfo = struct {
    tables: []TableInfo,
    arena: std.heap.ArenaAllocator,

    pub fn format(self: *const SchemaInfo, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8){};
        errdefer buf.deinit(allocator);

        try buf.appendSlice(allocator, "DATABASE SCHEMA:\n");
        for (self.tables) |table| {
            try std.fmt.format(buf.writer(allocator), "  {s}(", .{table.name});
            for (table.columns, 0..) |col, i| {
                if (i > 0) try buf.appendSlice(allocator, ", ");
                try std.fmt.format(buf.writer(allocator), "{s} {s}", .{ col.name, col.data_type });
            }
            try buf.appendSlice(allocator, ")\n");
        }

        return buf.toOwnedSlice(allocator);
    }

    pub fn deinit(self: *SchemaInfo) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const ColumnRowData = struct {
    table_name: []const u8,
    col_name: []const u8,
    data_type: []const u8,
    is_nullable: []const u8,
    col_default: []const u8,
    is_pk: []const u8,
};

pub const FkRowData = struct {
    table: []const u8,
    column: []const u8,
    target_table: []const u8,
    target_column: []const u8,
};

pub const EnumRowData = struct {
    table: []const u8,
    column: []const u8,
    label: []const u8,
};

// Arbitrary -- enough for one query + background ops (health check, schema refresh)
// from a single browser tab. No reason to go higher for a local-first tool.
pub const pool_size = 5;

/// Create a connection pool from a PostgreSQL URI string.
pub fn initPool(allocator: std.mem.Allocator, uri_string: []const u8) PgError!*pg.Pool {
    const uri = std.Uri.parse(uri_string) catch return error.ConnectionFailed;
    const pool = pg.Pool.initUri(allocator, uri, .{
        .size = pool_size,
        .timeout = 10 * std.time.ms_per_s,
    }) catch |err| {
        if (err == error.PG) {
            log.err("pool init failed with PostgreSQL error", .{});
        } else {
            log.err("pool init failed: {s}", .{@errorName(err)});
        }
        return error.ConnectionFailed;
    };
    return pool;
}

/// Execute a single SQL statement and collect results into a QueryResult.
/// SQL must have values already interpolated (no parameterized queries).
pub fn runQuery(pool: *pg.Pool, backing: std.mem.Allocator, sql: []const u8) PgError!QueryResult {
    var conn = pool.acquire() catch return error.ConnectionFailed;
    defer conn.release();
    return runQueryOnConn(conn, backing, sql);
}

/// Execute a query on an already-acquired connection (for transaction use).
pub fn runQueryOnConn(conn: *pg.Conn, backing: std.mem.Allocator, sql: []const u8) PgError!QueryResult {
    var result = conn.queryOpts(sql, .{}, .{ .column_names = true }) catch |err| {
        if (err == error.PG) {
            if (conn.err) |pge| {
                log.warn("query failed: {s}", .{pge.message});
            }
        }
        return error.QueryFailed;
    };
    defer result.deinit();
    return extractResult(backing, result);
}

pub fn runQueryParams(pool: *pg.Pool, backing: std.mem.Allocator, sql: []const u8, params: anytype) PgError!QueryResult {
    return runQueryParamsReporting(pool, backing, sql, params, null);
}

/// As `runQueryParams`, but on a server-side failure copies PostgreSQL's own
/// message into `err_out` (allocated from `backing`) so the handler can tell the
/// user *why* the statement was rejected. Without this the message is logged and
/// discarded, and the caller can only report a generic failure.
pub fn runQueryParamsReporting(
    pool: *pg.Pool,
    backing: std.mem.Allocator,
    sql: []const u8,
    params: anytype,
    err_out: ?*?[]const u8,
) PgError!QueryResult {
    var conn = pool.acquire() catch return error.ConnectionFailed;
    defer conn.release();
    return runQueryOnConnParams(conn, backing, sql, params) catch |err| {
        if (err_out) |slot| {
            if (conn.err) |pge| slot.* = backing.dupe(u8, pge.message) catch null;
        }
        return err;
    };
}

pub fn runQueryOnConnParams(conn: *pg.Conn, backing: std.mem.Allocator, sql: []const u8, params: anytype) PgError!QueryResult {
    var result = conn.queryOpts(sql, params, .{ .column_names = true }) catch |err| {
        if (err == error.PG) {
            if (conn.err) |pge| {
                log.warn("query failed: {s}", .{pge.message});
            }
        }
        return error.QueryFailed;
    };
    defer result.deinit();
    return extractResult(backing, result);
}

/// Execute a raw SQL statement without extracting results (for SET commands).
pub fn runRawSql(pool: *pg.Pool, sql: []const u8) PgError!void {
    var conn = pool.acquire() catch return error.ConnectionFailed;
    defer conn.release();
    var result = conn.queryOpts(sql, .{}, .{}) catch |err| {
        if (err == error.PG) {
            if (conn.err) |pge| {
                log.warn("raw sql failed: {s}", .{pge.message});
            }
        }
        return error.QueryFailed;
    };
    result.deinit();
}

/// Get the error message from a connection after a PG error.
pub fn connErrorMessage(conn: *pg.Conn) []const u8 {
    if (conn.err) |pge| return pge.message;
    return "Unknown error";
}

/// Get structured error fields from a connection.
pub fn connErrorFields(conn: *pg.Conn) PgErrorFields {
    if (conn.err) |pge| {
        return .{
            .severity = pge.severity,
            .code = pge.code,
            .message = pge.message,
            .detail = pge.detail,
            .hint = pge.hint,
        };
    }
    return .{ .severity = null, .code = null, .message = null, .detail = null, .hint = null };
}

/// Decode a row's raw column bytes into text using each column's OID.
/// NULL columns retain the legacy "NULL" sentinel string.
/// Returned slices are arena-allocated.
pub fn decodeRow(
    arena: std.mem.Allocator,
    col_oids: []const i32,
    raw_cols: []const ?[]const u8,
) PgError![][]const u8 {
    const out = arena.alloc([]const u8, raw_cols.len) catch return error.OutOfMemory;
    for (raw_cols, 0..) |maybe_raw, i| {
        if (maybe_raw) |raw| {
            // pg.zig stores OIDs as i32; cast to u32 for decodeColumnByOid.
            const col_oid: u32 = if (i < col_oids.len) @intCast(@max(0, col_oids[i])) else 0;
            out[i] = pg_decode.decodeColumnByOid(arena, col_oid, raw) catch return error.OutOfMemory;
        } else {
            out[i] = "NULL";
        }
    }
    return out;
}

fn extractResult(backing: std.mem.Allocator, result: *pg.Result) PgError!QueryResult {
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    const n_cols = result.number_of_columns;

    const col_names = alloc.alloc([]const u8, n_cols) catch return error.OutOfMemory;
    for (result.column_names, 0..) |name, i| {
        col_names[i] = alloc.dupe(u8, name) catch return error.OutOfMemory;
    }

    var rows_list = std.ArrayList([][]const u8){};

    // pg.zig stores column OIDs as _oids: []i32 on the Result.
    const col_oids = result._oids;

    // Scratch buffer for raw column slices — sized once, reused across rows.
    const scratch = alloc.alloc(?[]const u8, n_cols) catch return error.OutOfMemory;

    while (true) {
        const maybe_row = result.next() catch return error.QueryFailed;
        const row = maybe_row orelse break;
        for (0..n_cols) |col_idx| {
            scratch[col_idx] = row.get(?[]const u8, col_idx) catch null;
        }
        const row_data = decodeRow(alloc, col_oids, scratch) catch return error.OutOfMemory;
        rows_list.append(alloc, row_data) catch return error.OutOfMemory;
    }

    const n_rows = rows_list.items.len;
    return QueryResult{
        .col_names = col_names,
        .rows = rows_list.toOwnedSlice(alloc) catch return error.OutOfMemory,
        .n_cols = n_cols,
        .n_rows = n_rows,
        .arena = arena,
    };
}

pub fn fetchSchema(pool: *pg.Pool, backing: std.mem.Allocator) PgError!SchemaInfo {
    const sql =
        "SELECT table_name::text, column_name::text, data_type::text " ++
        "FROM information_schema.columns " ++
        "WHERE table_schema = 'public' " ++
        "ORDER BY table_name, ordinal_position";

    var conn = pool.acquire() catch return error.ConnectionFailed;
    defer conn.release();

    // Cached on the connection: schema_fetch runs on every connect + Refresh.
    var result = conn.queryOpts(sql, .{}, .{ .column_names = true, .cache_name = "schema_fetch" }) catch return error.QueryFailed;
    defer result.deinit();

    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    var tables = std.ArrayList(TableInfo){};
    var current_table: ?[]const u8 = null;
    var current_columns = std.ArrayList(ColumnInfo){};

    while (true) {
        const maybe_row = result.next() catch return error.QueryFailed;
        const row = maybe_row orelse break;

        const tname = alloc.dupe(u8, row.get([]const u8, 0) catch return error.InvalidData) catch return error.OutOfMemory;
        const cname = alloc.dupe(u8, row.get([]const u8, 1) catch return error.InvalidData) catch return error.OutOfMemory;
        const dtype = alloc.dupe(u8, row.get([]const u8, 2) catch return error.InvalidData) catch return error.OutOfMemory;

        if (current_table == null or !std.mem.eql(u8, current_table.?, tname)) {
            if (current_table != null) {
                tables.append(alloc, .{
                    .name = current_table.?,
                    .columns = current_columns.toOwnedSlice(alloc) catch return error.OutOfMemory,
                }) catch return error.OutOfMemory;
                current_columns = std.ArrayList(ColumnInfo){};
            }
            current_table = tname;
        }

        current_columns.append(alloc, .{ .name = cname, .data_type = dtype }) catch return error.OutOfMemory;
    }

    if (current_table != null) {
        tables.append(alloc, .{
            .name = current_table.?,
            .columns = current_columns.toOwnedSlice(alloc) catch return error.OutOfMemory,
        }) catch return error.OutOfMemory;
    }

    return SchemaInfo{
        .tables = tables.toOwnedSlice(alloc) catch return error.OutOfMemory,
        .arena = arena,
    };
}

pub fn fetchEnhancedSchema(pool: *pg.Pool, backing: std.mem.Allocator) PgError!EnhancedSchemaInfo {
    var conn = pool.acquire() catch return error.ConnectionFailed;
    defer conn.release();

    const col_sql =
        "SELECT c.table_name::text, c.column_name::text, c.data_type::text, c.is_nullable::text, COALESCE(c.column_default, '')::text, " ++
        "CASE WHEN pk.column_name IS NOT NULL THEN 'true' ELSE 'false' END AS is_primary_key " ++
        "FROM information_schema.columns c " ++
        "LEFT JOIN (" ++
        "SELECT kcu.table_name, kcu.column_name " ++
        "FROM information_schema.table_constraints tc " ++
        "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " ++
        "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'" ++
        ") pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name " ++
        "WHERE c.table_schema = 'public' ORDER BY c.table_name, c.ordinal_position";

    var col_result = conn.queryOpts(col_sql, .{}, .{ .cache_name = "enhanced_schema_cols" }) catch return error.QueryFailed;
    defer col_result.deinit();

    // Read column data into temp arena
    var tmp = std.heap.ArenaAllocator.init(backing);
    defer tmp.deinit();
    const t = tmp.allocator();

    var col_list = std.ArrayList(ColumnRowData){};
    while (true) {
        const maybe_row = col_result.next() catch return error.QueryFailed;
        const row = maybe_row orelse break;
        col_list.append(t, .{
            .table_name = t.dupe(u8, row.get([]const u8, 0) catch "") catch return error.OutOfMemory,
            .col_name = t.dupe(u8, row.get([]const u8, 1) catch "") catch return error.OutOfMemory,
            .data_type = t.dupe(u8, row.get([]const u8, 2) catch "") catch return error.OutOfMemory,
            .is_nullable = t.dupe(u8, row.get([]const u8, 3) catch "") catch return error.OutOfMemory,
            .col_default = t.dupe(u8, row.get([]const u8, 4) catch "") catch return error.OutOfMemory,
            .is_pk = t.dupe(u8, row.get([]const u8, 5) catch "") catch return error.OutOfMemory,
        }) catch return error.OutOfMemory;
    }

    const fk_sql =
        "SELECT kcu.table_name::text, kcu.column_name::text, ccu.table_name::text AS target_table, ccu.column_name::text AS target_column " ++
        "FROM information_schema.table_constraints tc " ++
        "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " ++
        "JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema " ++
        "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'";

    var fk_list = std.ArrayList(FkRowData){};
    if (conn.queryOpts(fk_sql, .{}, .{ .cache_name = "enhanced_schema_fks" })) |fk_result| {
        defer fk_result.deinit();
        while (true) {
            const maybe_row = fk_result.next() catch break;
            const row = maybe_row orelse break;
            fk_list.append(t, .{
                .table = t.dupe(u8, row.get([]const u8, 0) catch "") catch return error.OutOfMemory,
                .column = t.dupe(u8, row.get([]const u8, 1) catch "") catch return error.OutOfMemory,
                .target_table = t.dupe(u8, row.get([]const u8, 2) catch "") catch return error.OutOfMemory,
                .target_column = t.dupe(u8, row.get([]const u8, 3) catch "") catch return error.OutOfMemory,
            }) catch return error.OutOfMemory;
        }
    } else |_| {}

    const enum_sql =
        "SELECT c.relname::text, a.attname::text, e.enumlabel::text " ++
        "FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid " ++
        "JOIN pg_attribute a ON a.atttypid = t.oid JOIN pg_class cc ON a.attrelid = cc.oid " ++
        "JOIN pg_namespace n ON cc.relnamespace = n.oid " ++
        "WHERE n.nspname = 'public' ORDER BY cc.relname, a.attname, e.enumsortorder";

    var enum_list = std.ArrayList(EnumRowData){};
    if (conn.queryOpts(enum_sql, .{}, .{ .cache_name = "enhanced_schema_enums" })) |enum_result| {
        defer enum_result.deinit();
        while (true) {
            const maybe_row = enum_result.next() catch break;
            const row = maybe_row orelse break;
            enum_list.append(t, .{
                .table = t.dupe(u8, row.get([]const u8, 0) catch "") catch return error.OutOfMemory,
                .column = t.dupe(u8, row.get([]const u8, 1) catch "") catch return error.OutOfMemory,
                .label = t.dupe(u8, row.get([]const u8, 2) catch "") catch return error.OutOfMemory,
            }) catch return error.OutOfMemory;
        }
    } else |_| {}

    return buildEnhancedSchemaFromRows(backing, col_list.items, fk_list.items, enum_list.items);
}

/// Extracted from PgConnection so it can be tested without a live database.
pub fn buildEnhancedSchemaFromRows(
    backing: std.mem.Allocator,
    col_data: []const ColumnRowData,
    fk_data: []const FkRowData,
    enum_data: []const EnumRowData,
) PgError!EnhancedSchemaInfo {
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    const EnumKey = struct { table: []const u8, column: []const u8 };
    const EnumEntry = struct { key: EnumKey, values: std.ArrayList([]const u8) };
    var enum_entries = std.ArrayList(EnumEntry){};
    for (enum_data) |row| {
        var found = false;
        for (enum_entries.items) |*entry| {
            if (std.mem.eql(u8, entry.key.table, row.table) and std.mem.eql(u8, entry.key.column, row.column)) {
                entry.values.append(alloc, alloc.dupe(u8, row.label) catch return error.OutOfMemory) catch return error.OutOfMemory;
                found = true;
                break;
            }
        }
        if (!found) {
            var new_entry = EnumEntry{ .key = .{ .table = row.table, .column = row.column }, .values = std.ArrayList([]const u8){} };
            new_entry.values.append(alloc, alloc.dupe(u8, row.label) catch return error.OutOfMemory) catch return error.OutOfMemory;
            enum_entries.append(alloc, new_entry) catch return error.OutOfMemory;
        }
    }

    var tables = std.ArrayList(EnhancedTableInfo){};
    var current_table: ?[]const u8 = null;
    var current_columns = std.ArrayList(EnhancedColumnInfo){};
    var pk_cols = std.ArrayList([]const u8){};

    for (col_data) |row| {
        const tname = alloc.dupe(u8, row.table_name) catch return error.OutOfMemory;
        const cname = alloc.dupe(u8, row.col_name) catch return error.OutOfMemory;
        const dtype = alloc.dupe(u8, row.data_type) catch return error.OutOfMemory;

        if (current_table == null or !std.mem.eql(u8, current_table.?, tname)) {
            if (current_table != null) {
                const has_pk = pk_cols.items.len > 0;
                tables.append(alloc, .{
                    .name = current_table.?,
                    .columns = current_columns.toOwnedSlice(alloc) catch return error.OutOfMemory,
                    .primary_key_columns = pk_cols.toOwnedSlice(alloc) catch return error.OutOfMemory,
                    .has_primary_key = has_pk,
                }) catch return error.OutOfMemory;
                current_columns = std.ArrayList(EnhancedColumnInfo){};
                pk_cols = std.ArrayList([]const u8){};
            }
            current_table = tname;
        }

        const is_pk = std.mem.eql(u8, row.is_pk, "true");
        const is_nullable = std.mem.eql(u8, row.is_nullable, "YES");
        const col_default: ?[]const u8 = if (row.col_default.len > 0)
            (alloc.dupe(u8, row.col_default) catch return error.OutOfMemory)
        else
            null;

        var fk_table: ?[]const u8 = null;
        var fk_col: ?[]const u8 = null;
        for (fk_data) |fk| {
            if (std.mem.eql(u8, fk.table, current_table.?) and std.mem.eql(u8, fk.column, cname)) {
                fk_table = alloc.dupe(u8, fk.target_table) catch return error.OutOfMemory;
                fk_col = alloc.dupe(u8, fk.target_column) catch return error.OutOfMemory;
                break;
            }
        }

        var enum_vals: ?[][]const u8 = null;
        for (enum_entries.items) |entry| {
            if (std.mem.eql(u8, entry.key.table, current_table.?) and std.mem.eql(u8, entry.key.column, cname)) {
                const vals = alloc.alloc([]const u8, entry.values.items.len) catch return error.OutOfMemory;
                for (entry.values.items, 0..) |v, vi| {
                    vals[vi] = alloc.dupe(u8, v) catch return error.OutOfMemory;
                }
                enum_vals = vals;
                break;
            }
        }

        if (is_pk) {
            pk_cols.append(alloc, alloc.dupe(u8, cname) catch return error.OutOfMemory) catch return error.OutOfMemory;
        }

        current_columns.append(alloc, .{
            .name = cname,
            .data_type = dtype,
            .is_primary_key = is_pk,
            .is_nullable = is_nullable,
            .column_default = col_default,
            .fk_target_table = fk_table,
            .fk_target_column = fk_col,
            .enum_values = enum_vals,
        }) catch return error.OutOfMemory;
    }

    if (current_table != null) {
        const has_pk = pk_cols.items.len > 0;
        tables.append(alloc, .{
            .name = current_table.?,
            .columns = current_columns.toOwnedSlice(alloc) catch return error.OutOfMemory,
            .primary_key_columns = pk_cols.toOwnedSlice(alloc) catch return error.OutOfMemory,
            .has_primary_key = has_pk,
        }) catch return error.OutOfMemory;
    }

    return EnhancedSchemaInfo{
        .tables = tables.toOwnedSlice(alloc) catch return error.OutOfMemory,
        .arena = arena,
    };
}

test "SchemaInfo.format: produces readable output" {
    const allocator = std.testing.allocator;
    var cols = [_]ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
        .{ .name = "name", .data_type = "text" },
    };
    var tables_arr = [_]TableInfo{
        .{ .name = "users", .columns = &cols },
    };
    var schema = SchemaInfo{ .tables = &tables_arr, .arena = std.heap.ArenaAllocator.init(allocator) };
    defer schema.deinit();
    const text = try schema.format(allocator);
    defer allocator.free(text);

    try std.testing.expect(std.mem.indexOf(u8, text, "DATABASE SCHEMA:") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "users(") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "id integer") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "name text") != null);
}

test "buildEnhancedSchemaFromRows: basic smoke test" {
    const allocator = std.testing.allocator;
    const col_data = [_]ColumnRowData{
        .{ .table_name = "users", .col_name = "id", .data_type = "integer", .is_nullable = "NO", .col_default = "", .is_pk = "true" },
        .{ .table_name = "users", .col_name = "name", .data_type = "text", .is_nullable = "YES", .col_default = "", .is_pk = "false" },
    };
    const fk_data = [_]FkRowData{};
    const enum_data = [_]EnumRowData{};
    var result = try buildEnhancedSchemaFromRows(allocator, &col_data, &fk_data, &enum_data);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.tables.len);
    try std.testing.expectEqualStrings("users", result.tables[0].name);
    try std.testing.expectEqual(@as(usize, 2), result.tables[0].columns.len);
    try std.testing.expect(result.tables[0].has_primary_key);
}

fn testBuildEnhancedSchemaFromRows(allocator: std.mem.Allocator) !void {
    const col_data = [_]ColumnRowData{
        .{ .table_name = "users", .col_name = "id", .data_type = "integer", .is_nullable = "NO", .col_default = "", .is_pk = "true" },
        .{ .table_name = "users", .col_name = "name", .data_type = "text", .is_nullable = "YES", .col_default = "", .is_pk = "false" },
    };
    const fk_data = [_]FkRowData{};
    const enum_data = [_]EnumRowData{};
    var result = try buildEnhancedSchemaFromRows(allocator, &col_data, &fk_data, &enum_data);
    defer result.deinit();
}

test "buildEnhancedSchemaFromRows: allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testBuildEnhancedSchemaFromRows, .{});
}

test "buildEnhancedSchemaFromRows: multiple tables with FK and enum" {
    const allocator = std.testing.allocator;
    const col_data = [_]ColumnRowData{
        .{ .table_name = "orders", .col_name = "id", .data_type = "integer", .is_nullable = "NO", .col_default = "", .is_pk = "true" },
        .{ .table_name = "orders", .col_name = "status", .data_type = "order_status", .is_nullable = "NO", .col_default = "'pending'", .is_pk = "false" },
        .{ .table_name = "orders", .col_name = "user_id", .data_type = "integer", .is_nullable = "NO", .col_default = "", .is_pk = "false" },
        .{ .table_name = "users", .col_name = "id", .data_type = "integer", .is_nullable = "NO", .col_default = "", .is_pk = "true" },
        .{ .table_name = "users", .col_name = "name", .data_type = "text", .is_nullable = "YES", .col_default = "", .is_pk = "false" },
    };
    const fk_data = [_]FkRowData{
        .{ .table = "orders", .column = "user_id", .target_table = "users", .target_column = "id" },
    };
    const enum_data = [_]EnumRowData{
        .{ .table = "orders", .column = "status", .label = "pending" },
        .{ .table = "orders", .column = "status", .label = "shipped" },
        .{ .table = "orders", .column = "status", .label = "delivered" },
    };

    var result = try buildEnhancedSchemaFromRows(allocator, &col_data, &fk_data, &enum_data);
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 2), result.tables.len);

    const orders = result.tables[0];
    try std.testing.expectEqualStrings("orders", orders.name);
    try std.testing.expectEqual(@as(usize, 3), orders.columns.len);
    try std.testing.expect(orders.has_primary_key);

    const user_id_col = orders.columns[2];
    try std.testing.expectEqualStrings("user_id", user_id_col.name);
    try std.testing.expectEqualStrings("users", user_id_col.fk_target_table.?);
    try std.testing.expectEqualStrings("id", user_id_col.fk_target_column.?);

    const status_col = orders.columns[1];
    try std.testing.expectEqual(@as(usize, 3), status_col.enum_values.?.len);
    try std.testing.expectEqualStrings("pending", status_col.enum_values.?[0]);
    try std.testing.expectEqualStrings("'pending'", status_col.column_default.?);

    const users = result.tables[1];
    try std.testing.expectEqualStrings("users", users.name);
    try std.testing.expectEqual(@as(usize, 2), users.columns.len);
}

test "PgErrorFields: has required optional fields" {
    const fields = PgErrorFields{
        .severity = "ERROR",
        .code = "42P01",
        .message = "relation does not exist",
        .detail = null,
        .hint = null,
    };
    try std.testing.expectEqualStrings("ERROR", fields.severity.?);
    try std.testing.expectEqualStrings("42P01", fields.code.?);
    try std.testing.expectEqualStrings("relation does not exist", fields.message.?);
    try std.testing.expect(fields.detail == null);
    try std.testing.expect(fields.hint == null);
}

test "decodeRow: int4 column decodes to text" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const oids = [_]i32{23}; // int4
    const cols = [_]?[]const u8{&[_]u8{ 0, 0, 0, 42 }};
    const out = try decodeRow(arena.allocator(), &oids, &cols);
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectEqualStrings("42", out[0]);
}

test "decodeRow: NULL column keeps NULL sentinel" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const oids = [_]i32{23};
    const cols = [_]?[]const u8{null};
    const out = try decodeRow(arena.allocator(), &oids, &cols);
    try std.testing.expectEqualStrings("NULL", out[0]);
}

test "decodeRow: bool column decodes to t/f" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const oids = [_]i32{16};
    const cols = [_]?[]const u8{&[_]u8{0x01}};
    const out = try decodeRow(arena.allocator(), &oids, &cols);
    try std.testing.expectEqualStrings("t", out[0]);
}

test "decodeRow: unknown OID returns placeholder, never panics" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const oids = [_]i32{99999};
    const cols = [_]?[]const u8{&[_]u8{0xFF}};
    const out = try decodeRow(arena.allocator(), &oids, &cols);
    try std.testing.expect(std.mem.startsWith(u8, out[0], "?[OID="));
}

test "fetchSchema cache_name idempotent across repeated calls" {
    const test_url = std.posix.getenv("PG_TEST_URL") orelse return error.SkipZigTest;
    var pool = try initPool(std.testing.allocator, test_url);
    defer pool.deinit();

    var s1 = try fetchSchema(pool, std.testing.allocator);
    defer s1.deinit();
    var s2 = try fetchSchema(pool, std.testing.allocator);
    defer s2.deinit();

    try std.testing.expectEqual(s1.tables.len, s2.tables.len);
    for (s1.tables, s2.tables) |t1, t2| {
        try std.testing.expectEqualStrings(t1.name, t2.name);
        try std.testing.expectEqual(t1.columns.len, t2.columns.len);
        for (t1.columns, t2.columns) |c1, c2| {
            try std.testing.expectEqualStrings(c1.name, c2.name);
            try std.testing.expectEqualStrings(c1.data_type, c2.data_type);
        }
    }
}

test "fetchEnhancedSchema cache_name idempotent across repeated calls" {
    const test_url = std.posix.getenv("PG_TEST_URL") orelse return error.SkipZigTest;
    var pool = try initPool(std.testing.allocator, test_url);
    defer pool.deinit();

    var e1 = try fetchEnhancedSchema(pool, std.testing.allocator);
    defer e1.deinit();
    var e2 = try fetchEnhancedSchema(pool, std.testing.allocator);
    defer e2.deinit();

    try std.testing.expectEqual(e1.tables.len, e2.tables.len);
    for (e1.tables, e2.tables) |t1, t2| {
        try std.testing.expectEqual(t1.columns.len, t2.columns.len);
        try std.testing.expectEqual(t1.primary_key_columns.len, t2.primary_key_columns.len);
    }
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
