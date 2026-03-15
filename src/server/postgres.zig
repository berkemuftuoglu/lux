const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});

const log = std.log.scoped(.postgres);

pub const PgError = error{
    ConnectionFailed,
    QueryFailed,
    OutOfMemory,
    NoResults,
    InvalidData,
};

pub const PgConnection = struct {
    conn: *c.PGconn,

    pub fn connect(conninfo: [*:0]const u8) PgError!PgConnection {
        const conn = c.PQconnectdb(conninfo) orelse return error.ConnectionFailed;
        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            c.PQfinish(conn);
            return error.ConnectionFailed;
        }
        return PgConnection{ .conn = conn };
    }

    /// Connect and return the PGconn even on failure so the caller can read the error.
    /// Caller must call deinit() on the returned connection regardless of success.
    pub fn connectVerbose(conninfo: [*:0]const u8) PgError!PgConnection {
        const conn = c.PQconnectdb(conninfo) orelse return error.ConnectionFailed;
        return PgConnection{ .conn = conn };
    }

    pub fn isOk(self: *const PgConnection) bool {
        return c.PQstatus(self.conn) == c.CONNECTION_OK;
    }

    /// Check connection and transaction state. Returns true only if the
    /// connection is up and idle (not in a failed transaction).
    pub fn isHealthy(self: *const PgConnection) bool {
        if (c.PQstatus(self.conn) != c.CONNECTION_OK) return false;
        const txn = c.PQtransactionStatus(self.conn);
        return txn == c.PQTRANS_IDLE;
    }

    /// If the connection is stuck in a failed transaction, issue ROLLBACK
    /// to return it to an idle state so subsequent queries can proceed.
    pub fn resetIfNeeded(self: *PgConnection) void {
        const txn = c.PQtransactionStatus(self.conn);
        if (txn == c.PQTRANS_INERROR) {
            log.info("connection in failed transaction state, sending ROLLBACK", .{});
            const res = c.PQexec(self.conn, "ROLLBACK");
            if (res) |r| c.PQclear(r);
        }
    }

    pub fn deinit(self: *PgConnection) void {
        c.PQfinish(self.conn);
        self.* = undefined;
    }

    pub fn errorMessage(self: *const PgConnection) []const u8 {
        const msg = c.PQerrorMessage(self.conn);
        if (msg == null) return "";
        return std.mem.sliceTo(msg, 0);
    }

    /// Run a SQL statement and return the result set.
    /// Uses PQexecParams with no parameters for safety (single statement only).
    pub fn runQuery(self: *PgConnection, allocator: std.mem.Allocator, sql: [*:0]const u8) PgError!QueryResult {
        const res = c.PQexecParams(
            self.conn,
            sql,
            0,
            null,
            null,
            null,
            null,
            0,
        ) orelse return error.QueryFailed;
        return extractResult(allocator, res);
    }

    /// Run one or more SQL statements via PQexec (supports multi-statement scripts).
    /// Returns the result of the last statement. Use for the SQL editor endpoint
    /// where users type arbitrary SQL including multi-statement scripts.
    pub fn runQueryMulti(self: *PgConnection, allocator: std.mem.Allocator, sql: [*:0]const u8) PgError!QueryResult {
        const res = c.PQexec(self.conn, sql) orelse return error.QueryFailed;
        return extractResult(allocator, res);
    }

    pub fn fetchSchema(self: *PgConnection, backing: std.mem.Allocator) PgError!SchemaInfo {
        const sql =
            "SELECT table_name, column_name, data_type " ++
            "FROM information_schema.columns " ++
            "WHERE table_schema = 'public' " ++
            "ORDER BY table_name, ordinal_position";

        const res = c.PQexecParams(self.conn, sql, 0, null, null, null, null, 0) orelse return error.QueryFailed;
        defer c.PQclear(res);
        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) return error.QueryFailed;

        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const alloc = arena.allocator();

        const n_rows: usize = @intCast(c.PQntuples(res));
        var tables = std.ArrayList(TableInfo){};
        var current_table: ?[]const u8 = null;
        var current_columns = std.ArrayList(ColumnInfo){};

        for (0..n_rows) |row_idx| {
            const tname = getStringField(alloc, res, row_idx, 0) catch return error.OutOfMemory;
            const cname = getStringField(alloc, res, row_idx, 1) catch return error.OutOfMemory;
            const dtype = getStringField(alloc, res, row_idx, 2) catch return error.OutOfMemory;

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

    pub fn fetchEnhancedSchema(self: *PgConnection, backing: std.mem.Allocator) PgError!EnhancedSchemaInfo {
        const col_sql =
            "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, c.column_default, " ++
            "CASE WHEN pk.column_name IS NOT NULL THEN 'true' ELSE 'false' END AS is_primary_key " ++
            "FROM information_schema.columns c " ++
            "LEFT JOIN (" ++
            "SELECT kcu.table_name, kcu.column_name " ++
            "FROM information_schema.table_constraints tc " ++
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " ++
            "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'" ++
            ") pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name " ++
            "WHERE c.table_schema = 'public' ORDER BY c.table_name, c.ordinal_position";

        const col_res = c.PQexecParams(self.conn, col_sql, 0, null, null, null, null, 0) orelse return error.QueryFailed;
        defer c.PQclear(col_res);
        if (c.PQresultStatus(col_res) != c.PGRES_TUPLES_OK) return error.QueryFailed;

        const fk_sql =
            "SELECT kcu.table_name, kcu.column_name, ccu.table_name AS target_table, ccu.column_name AS target_column " ++
            "FROM information_schema.table_constraints tc " ++
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " ++
            "JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema " ++
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'";

        const fk_res = c.PQexecParams(self.conn, fk_sql, 0, null, null, null, null, 0) orelse return error.QueryFailed;
        defer c.PQclear(fk_res);
        if (c.PQresultStatus(fk_res) != c.PGRES_TUPLES_OK) return error.QueryFailed;

        const enum_sql =
            "SELECT c.relname, a.attname, e.enumlabel " ++
            "FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid " ++
            "JOIN pg_attribute a ON a.atttypid = t.oid JOIN pg_class cc ON a.attrelid = cc.oid " ++
            "JOIN pg_namespace n ON cc.relnamespace = n.oid " ++
            "WHERE n.nspname = 'public' ORDER BY cc.relname, a.attname, e.enumsortorder";

        const enum_res = c.PQexecParams(self.conn, enum_sql, 0, null, null, null, null, 0) orelse return error.QueryFailed;
        defer c.PQclear(enum_res);

        // Extract PGresult rows into typed slices using a temp arena.
        // The slices reference PGresult memory (via getStringFieldNoAlloc),
        // which stays valid until the deferred PQclear calls above.
        var tmp = std.heap.ArenaAllocator.init(backing);
        defer tmp.deinit();
        const t = tmp.allocator();

        const col_n: usize = @intCast(c.PQntuples(col_res));
        const col_data = t.alloc(ColumnRowData, col_n) catch return error.OutOfMemory;
        for (0..col_n) |i| {
            col_data[i] = .{
                .table_name = getStringFieldNoAlloc(col_res, i, 0),
                .col_name = getStringFieldNoAlloc(col_res, i, 1),
                .data_type = getStringFieldNoAlloc(col_res, i, 2),
                .is_nullable = getStringFieldNoAlloc(col_res, i, 3),
                .col_default = getStringFieldNoAlloc(col_res, i, 4),
                .is_pk = getStringFieldNoAlloc(col_res, i, 5),
            };
        }

        const fk_n: usize = if (c.PQresultStatus(fk_res) == c.PGRES_TUPLES_OK)
            @intCast(c.PQntuples(fk_res))
        else
            0;
        const fk_data = t.alloc(FkRowData, fk_n) catch return error.OutOfMemory;
        for (0..fk_n) |i| {
            fk_data[i] = .{
                .table = getStringFieldNoAlloc(fk_res, i, 0),
                .column = getStringFieldNoAlloc(fk_res, i, 1),
                .target_table = getStringFieldNoAlloc(fk_res, i, 2),
                .target_column = getStringFieldNoAlloc(fk_res, i, 3),
            };
        }

        const enum_n: usize = if (c.PQresultStatus(enum_res) == c.PGRES_TUPLES_OK)
            @intCast(c.PQntuples(enum_res))
        else
            0;
        const enum_data = t.alloc(EnumRowData, enum_n) catch return error.OutOfMemory;
        for (0..enum_n) |i| {
            enum_data[i] = .{
                .table = getStringFieldNoAlloc(enum_res, i, 0),
                .column = getStringFieldNoAlloc(enum_res, i, 1),
                .label = getStringFieldNoAlloc(enum_res, i, 2),
            };
        }

        return buildEnhancedSchemaFromRows(backing, col_data, fk_data, enum_data);
    }
};

fn extractResult(backing: std.mem.Allocator, res: *c.PGresult) PgError!QueryResult {
    const status = c.PQresultStatus(res);
    if (status != c.PGRES_TUPLES_OK and status != c.PGRES_COMMAND_OK) {
        c.PQclear(res);
        return error.QueryFailed;
    }

    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    const n_rows: usize = @intCast(c.PQntuples(res));
    const n_cols: usize = @intCast(c.PQnfields(res));

    const col_names = alloc.alloc([]const u8, n_cols) catch return error.OutOfMemory;
    for (0..n_cols) |col_idx| {
        const name_ptr = c.PQfname(res, @intCast(col_idx));
        if (name_ptr == null) {
            col_names[col_idx] = "";
        } else {
            col_names[col_idx] = alloc.dupe(u8, std.mem.sliceTo(name_ptr, 0)) catch return error.OutOfMemory;
        }
    }

    const rows = alloc.alloc([][]const u8, n_rows) catch return error.OutOfMemory;
    for (0..n_rows) |row_idx| {
        const row_data = alloc.alloc([]const u8, n_cols) catch return error.OutOfMemory;
        for (0..n_cols) |col_idx| {
            if (c.PQgetisnull(res, @intCast(row_idx), @intCast(col_idx)) != 0) {
                row_data[col_idx] = "NULL";
            } else {
                const val_ptr = c.PQgetvalue(res, @intCast(row_idx), @intCast(col_idx));
                if (val_ptr == null) {
                    row_data[col_idx] = "";
                } else {
                    row_data[col_idx] = alloc.dupe(u8, std.mem.sliceTo(val_ptr, 0)) catch return error.OutOfMemory;
                }
            }
        }
        rows[row_idx] = row_data;
    }

    c.PQclear(res);

    return QueryResult{
        .col_names = col_names,
        .rows = rows,
        .n_cols = n_cols,
        .n_rows = n_rows,
        .arena = arena,
    };
}

fn getStringFieldNoAlloc(res: *c.PGresult, row: usize, col_idx: usize) []const u8 {
    if (c.PQgetisnull(res, @intCast(row), @intCast(col_idx)) != 0) return "";
    const val_ptr = c.PQgetvalue(res, @intCast(row), @intCast(col_idx));
    if (val_ptr == null) return "";
    return std.mem.sliceTo(val_ptr, 0);
}

fn getStringField(allocator: std.mem.Allocator, res: *c.PGresult, row: usize, col: usize) ![]const u8 {
    const val_ptr = c.PQgetvalue(res, @intCast(row), @intCast(col));
    if (val_ptr == null) return "";
    const val_slice = std.mem.sliceTo(val_ptr, 0);
    return allocator.dupe(u8, val_slice);
}

// ── Result types ──────────────────────────────────────────────────────

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

// ── Raw row types for testable schema building ────────────────────────

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

/// Build an EnhancedSchemaInfo from pre-parsed row slices.
/// All allocations go into a single arena — on error, one errdefer cleans up everything.
/// Extracted so it can be tested without a live PostgreSQL connection.
pub fn buildEnhancedSchemaFromRows(
    backing: std.mem.Allocator,
    col_data: []const ColumnRowData,
    fk_data: []const FkRowData,
    enum_data: []const EnumRowData,
) PgError!EnhancedSchemaInfo {
    var arena = std.heap.ArenaAllocator.init(backing);
    errdefer arena.deinit();
    const alloc = arena.allocator();

    // Build ENUM lookup: group labels by (table, column)
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

    // Build table list from column rows
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

        // FK lookup — search input data directly
        var fk_table: ?[]const u8 = null;
        var fk_col: ?[]const u8 = null;
        for (fk_data) |fk| {
            if (std.mem.eql(u8, fk.table, current_table.?) and std.mem.eql(u8, fk.column, cname)) {
                fk_table = alloc.dupe(u8, fk.target_table) catch return error.OutOfMemory;
                fk_col = alloc.dupe(u8, fk.target_column) catch return error.OutOfMemory;
                break;
            }
        }

        // ENUM lookup
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

// ── Tests ─────────────────────────────────────────────────────────────

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

    // Orders table
    const orders = result.tables[0];
    try std.testing.expectEqualStrings("orders", orders.name);
    try std.testing.expectEqual(@as(usize, 3), orders.columns.len);
    try std.testing.expect(orders.has_primary_key);

    // FK on user_id
    const user_id_col = orders.columns[2];
    try std.testing.expectEqualStrings("user_id", user_id_col.name);
    try std.testing.expectEqualStrings("users", user_id_col.fk_target_table.?);
    try std.testing.expectEqualStrings("id", user_id_col.fk_target_column.?);

    // ENUM on status
    const status_col = orders.columns[1];
    try std.testing.expectEqual(@as(usize, 3), status_col.enum_values.?.len);
    try std.testing.expectEqualStrings("pending", status_col.enum_values.?[0]);
    try std.testing.expectEqualStrings("'pending'", status_col.column_default.?);

    // Users table
    const users = result.tables[1];
    try std.testing.expectEqualStrings("users", users.name);
    try std.testing.expectEqual(@as(usize, 2), users.columns.len);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
