const std = @import("std");
const c = @cImport({
    @cInclude("libpq-fe.h");
});

pub const PgError = error{
    ConnectionFailed,
    QueryFailed,
    OutOfMemory,
    NoResults,
    InvalidData,
};

/// A connection to a PostgreSQL database via libpq.
pub const PgConnection = struct {
    conn: *c.PGconn,

    /// Connect to a PostgreSQL database using a libpq connection string.
    /// Example: "postgresql://user:pass@localhost/mydb"
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
        // Return the connection even if status is bad — caller reads errorMessage() then deinit()s
        return PgConnection{ .conn = conn };
    }

    /// Check if the connection is in a good state.
    pub fn isOk(self: *const PgConnection) bool {
        return c.PQstatus(self.conn) == c.CONNECTION_OK;
    }

    pub fn deinit(self: *PgConnection) void {
        c.PQfinish(self.conn);
        self.* = undefined;
    }

    /// Get the connection error message (if any).
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
            0, // nParams
            null, // paramTypes
            null, // paramValues
            null, // paramLengths
            null, // paramFormats
            0, // resultFormat (text)
        ) orelse return error.QueryFailed;

        const status = c.PQresultStatus(res);
        if (status != c.PGRES_TUPLES_OK and status != c.PGRES_COMMAND_OK) {
            c.PQclear(res);
            return error.QueryFailed;
        }

        const n_rows: usize = @intCast(c.PQntuples(res));
        const n_cols: usize = @intCast(c.PQnfields(res));

        // Collect column names
        var col_names = allocator.alloc([]const u8, n_cols) catch return error.OutOfMemory;
        var col_names_filled: usize = 0;
        errdefer {
            for (col_names[0..col_names_filled]) |name| {
                if (name.len > 0 and !isStaticStr(name)) allocator.free(name);
            }
            allocator.free(col_names);
        }
        for (0..n_cols) |col_idx| {
            const name_ptr = c.PQfname(res, @intCast(col_idx));
            if (name_ptr == null) {
                col_names[col_idx] = "";
            } else {
                const name_slice = std.mem.sliceTo(name_ptr, 0);
                col_names[col_idx] = allocator.dupe(u8, name_slice) catch return error.OutOfMemory;
            }
            col_names_filled = col_idx + 1;
        }

        // Collect rows as string values
        var rows = allocator.alloc([][]const u8, n_rows) catch return error.OutOfMemory;
        var rows_completed: usize = 0;
        errdefer {
            for (rows[0..rows_completed]) |row| {
                for (row) |val| {
                    if (val.len > 0 and !isStaticStr(val)) allocator.free(val);
                }
                allocator.free(row);
            }
            allocator.free(rows);
        }
        for (0..n_rows) |row_idx| {
            var row_data = allocator.alloc([]const u8, n_cols) catch return error.OutOfMemory;
            var cells_filled: usize = 0;
            errdefer {
                for (row_data[0..cells_filled]) |val| {
                    if (val.len > 0 and !isStaticStr(val)) allocator.free(val);
                }
                allocator.free(row_data);
            }
            for (0..n_cols) |col_idx| {
                if (c.PQgetisnull(res, @intCast(row_idx), @intCast(col_idx)) != 0) {
                    row_data[col_idx] = "NULL";
                } else {
                    const val_ptr = c.PQgetvalue(res, @intCast(row_idx), @intCast(col_idx));
                    if (val_ptr == null) {
                        row_data[col_idx] = "";
                    } else {
                        const val_slice = std.mem.sliceTo(val_ptr, 0);
                        row_data[col_idx] = allocator.dupe(u8, val_slice) catch return error.OutOfMemory;
                    }
                }
                cells_filled = col_idx + 1;
            }
            rows[row_idx] = row_data;
            rows_completed = row_idx + 1;
        }

        c.PQclear(res);

        return QueryResult{
            .col_names = col_names,
            .rows = rows,
            .n_cols = n_cols,
            .n_rows = n_rows,
            .allocator = allocator,
        };
    }

    /// Run one or more SQL statements via PQexec (supports multi-statement scripts).
    /// Returns the result of the last statement. Use for the SQL editor endpoint
    /// where users type arbitrary SQL including multi-statement scripts.
    pub fn runQueryMulti(self: *PgConnection, allocator: std.mem.Allocator, sql: [*:0]const u8) PgError!QueryResult {
        const res = c.PQexec(self.conn, sql) orelse return error.QueryFailed;

        const status = c.PQresultStatus(res);
        if (status != c.PGRES_TUPLES_OK and status != c.PGRES_COMMAND_OK) {
            c.PQclear(res);
            return error.QueryFailed;
        }

        const n_rows: usize = @intCast(c.PQntuples(res));
        const n_cols: usize = @intCast(c.PQnfields(res));

        var col_names = allocator.alloc([]const u8, n_cols) catch return error.OutOfMemory;
        var col_names_filled: usize = 0;
        errdefer {
            for (col_names[0..col_names_filled]) |name| {
                if (name.len > 0 and !isStaticStr(name)) allocator.free(name);
            }
            allocator.free(col_names);
        }
        for (0..n_cols) |col_idx| {
            const name_ptr = c.PQfname(res, @intCast(col_idx));
            if (name_ptr == null) {
                col_names[col_idx] = "";
            } else {
                const name_slice = std.mem.sliceTo(name_ptr, 0);
                col_names[col_idx] = allocator.dupe(u8, name_slice) catch return error.OutOfMemory;
            }
            col_names_filled = col_idx + 1;
        }

        var rows = allocator.alloc([][]const u8, n_rows) catch return error.OutOfMemory;
        var rows_completed: usize = 0;
        errdefer {
            for (rows[0..rows_completed]) |row| {
                for (row) |val| {
                    if (val.len > 0 and !isStaticStr(val)) allocator.free(val);
                }
                allocator.free(row);
            }
            allocator.free(rows);
        }
        for (0..n_rows) |row_idx| {
            var row_data = allocator.alloc([]const u8, n_cols) catch return error.OutOfMemory;
            var cells_filled: usize = 0;
            errdefer {
                for (row_data[0..cells_filled]) |val| {
                    if (val.len > 0 and !isStaticStr(val)) allocator.free(val);
                }
                allocator.free(row_data);
            }
            for (0..n_cols) |col_idx| {
                if (c.PQgetisnull(res, @intCast(row_idx), @intCast(col_idx)) != 0) {
                    row_data[col_idx] = "NULL";
                } else {
                    const val_ptr = c.PQgetvalue(res, @intCast(row_idx), @intCast(col_idx));
                    if (val_ptr == null) {
                        row_data[col_idx] = "";
                    } else {
                        const val_slice = std.mem.sliceTo(val_ptr, 0);
                        row_data[col_idx] = allocator.dupe(u8, val_slice) catch return error.OutOfMemory;
                    }
                }
                cells_filled = col_idx + 1;
            }
            rows[row_idx] = row_data;
            rows_completed = row_idx + 1;
        }

        c.PQclear(res);

        return QueryResult{
            .col_names = col_names,
            .rows = rows,
            .n_cols = n_cols,
            .n_rows = n_rows,
            .allocator = allocator,
        };
    }

    /// Fetch the schema: list of tables with their columns and types.
    pub fn fetchSchema(self: *PgConnection, allocator: std.mem.Allocator) PgError!SchemaInfo {
        const sql =
            "SELECT table_name, column_name, data_type " ++
            "FROM information_schema.columns " ++
            "WHERE table_schema = 'public' " ++
            "ORDER BY table_name, ordinal_position";

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
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) return error.QueryFailed;

        const n_rows: usize = @intCast(c.PQntuples(res));

        var tables = std.ArrayList(TableInfo).init(allocator);
        errdefer tables.deinit();

        var current_table: ?[]const u8 = null;
        var current_columns = std.ArrayList(ColumnInfo).init(allocator);
        errdefer current_columns.deinit();

        for (0..n_rows) |row_idx| {
            const tname = getStringField(res, row_idx, 0, allocator) catch return error.OutOfMemory;
            const cname = getStringField(res, row_idx, 1, allocator) catch return error.OutOfMemory;
            const dtype = getStringField(res, row_idx, 2, allocator) catch return error.OutOfMemory;

            if (current_table == null or !std.mem.eql(u8, current_table.?, tname)) {
                // New table — flush previous
                if (current_table != null) {
                    tables.append(.{
                        .name = current_table.?,
                        .columns = current_columns.toOwnedSlice() catch return error.OutOfMemory,
                    }) catch return error.OutOfMemory;
                    current_columns = std.ArrayList(ColumnInfo).init(allocator);
                }
                current_table = tname;
            } else {
                // Same table as current — free the duplicate allocation
                if (tname.len > 0) allocator.free(tname);
            }

            current_columns.append(.{
                .name = cname,
                .data_type = dtype,
            }) catch return error.OutOfMemory;
        }

        // Flush last table
        if (current_table != null) {
            tables.append(.{
                .name = current_table.?,
                .columns = current_columns.toOwnedSlice() catch return error.OutOfMemory,
            }) catch return error.OutOfMemory;
        }

        return SchemaInfo{
            .tables = tables.toOwnedSlice() catch return error.OutOfMemory,
            .allocator = allocator,
        };
    }

    /// Fetch enhanced schema with PK, FK, ENUM, and nullability info.
    pub fn fetchEnhancedSchema(self: *PgConnection, allocator: std.mem.Allocator) PgError!EnhancedSchemaInfo {
        // Q1: Columns + PK info
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

        // Q2: Foreign keys
        const fk_sql =
            "SELECT kcu.table_name, kcu.column_name, ccu.table_name AS target_table, ccu.column_name AS target_column " ++
            "FROM information_schema.table_constraints tc " ++
            "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema " ++
            "JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema " ++
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public'";

        const fk_res = c.PQexecParams(self.conn, fk_sql, 0, null, null, null, null, 0) orelse return error.QueryFailed;
        defer c.PQclear(fk_res);
        if (c.PQresultStatus(fk_res) != c.PGRES_TUPLES_OK) return error.QueryFailed;

        // Q3: ENUM values
        const enum_sql =
            "SELECT c.relname, a.attname, e.enumlabel " ++
            "FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid " ++
            "JOIN pg_attribute a ON a.atttypid = t.oid JOIN pg_class cc ON a.attrelid = cc.oid " ++
            "JOIN pg_namespace n ON cc.relnamespace = n.oid " ++
            "WHERE n.nspname = 'public' ORDER BY cc.relname, a.attname, e.enumsortorder";

        const enum_res = c.PQexecParams(self.conn, enum_sql, 0, null, null, null, null, 0) orelse return error.QueryFailed;
        defer c.PQclear(enum_res);
        // ENUMs may not exist — treat TUPLES_OK only

        // Build FK lookup: table_name+column_name -> (target_table, target_column)
        const FkEntry = struct { table: []const u8, column: []const u8, target_table: []const u8, target_column: []const u8 };
        var fk_entries = std.ArrayList(FkEntry).init(allocator);
        defer fk_entries.deinit();

        if (c.PQresultStatus(fk_res) == c.PGRES_TUPLES_OK) {
            const fk_rows: usize = @intCast(c.PQntuples(fk_res));
            for (0..fk_rows) |ri| {
                fk_entries.append(.{
                    .table = getStringFieldNoAlloc(fk_res, ri, 0),
                    .column = getStringFieldNoAlloc(fk_res, ri, 1),
                    .target_table = getStringFieldNoAlloc(fk_res, ri, 2),
                    .target_column = getStringFieldNoAlloc(fk_res, ri, 3),
                }) catch return error.OutOfMemory;
            }
        }

        // Build ENUM lookup: table_name+column_name -> []enumlabel
        const EnumKey = struct { table: []const u8, column: []const u8 };
        const EnumEntry = struct { key: EnumKey, values: std.ArrayList([]const u8) };
        var enum_entries = std.ArrayList(EnumEntry).init(allocator);
        defer {
            for (enum_entries.items) |*entry| entry.values.deinit();
            enum_entries.deinit();
        }

        if (c.PQresultStatus(enum_res) == c.PGRES_TUPLES_OK) {
            const enum_rows: usize = @intCast(c.PQntuples(enum_res));
            for (0..enum_rows) |ri| {
                const tbl = getStringFieldNoAlloc(enum_res, ri, 0);
                const col = getStringFieldNoAlloc(enum_res, ri, 1);
                const label = getStringFieldNoAlloc(enum_res, ri, 2);

                // Find or create entry
                var found = false;
                for (enum_entries.items) |*entry| {
                    if (std.mem.eql(u8, entry.key.table, tbl) and std.mem.eql(u8, entry.key.column, col)) {
                        const duped_label = allocator.dupe(u8, label) catch return error.OutOfMemory;
                        entry.values.append(duped_label) catch return error.OutOfMemory;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    var new_entry = EnumEntry{
                        .key = .{ .table = tbl, .column = col },
                        .values = std.ArrayList([]const u8).init(allocator),
                    };
                    const duped_label = allocator.dupe(u8, label) catch return error.OutOfMemory;
                    new_entry.values.append(duped_label) catch return error.OutOfMemory;
                    enum_entries.append(new_entry) catch return error.OutOfMemory;
                }
            }
        }

        // Build enhanced table list from Q1 results
        const col_rows: usize = @intCast(c.PQntuples(col_res));
        var tables = std.ArrayList(EnhancedTableInfo).init(allocator);
        errdefer {
            for (tables.items) |*table| {
                @constCast(table).deinit(allocator);
            }
            tables.deinit();
        }

        var current_table: ?[]const u8 = null;
        var current_columns = std.ArrayList(EnhancedColumnInfo).init(allocator);
        errdefer {
            for (current_columns.items) |col| {
                if (col.name.len > 0) allocator.free(col.name);
                if (col.data_type.len > 0) allocator.free(col.data_type);
                if (col.column_default) |d| allocator.free(d);
                if (col.fk_target_table) |t| allocator.free(t);
                if (col.fk_target_column) |tc| allocator.free(tc);
                if (col.enum_values) |vals| {
                    for (vals) |v| allocator.free(v);
                    allocator.free(vals);
                }
            }
            current_columns.deinit();
        }
        var pk_cols = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (pk_cols.items) |pk| allocator.free(pk);
            pk_cols.deinit();
        }

        for (0..col_rows) |ri| {
            const tname = getStringField(col_res, ri, 0, allocator) catch return error.OutOfMemory;
            const cname = getStringField(col_res, ri, 1, allocator) catch return error.OutOfMemory;
            const dtype = getStringField(col_res, ri, 2, allocator) catch return error.OutOfMemory;
            const nullable_str = getStringFieldNoAlloc(col_res, ri, 3);
            const default_str = getStringFieldNoAlloc(col_res, ri, 4);
            const pk_str = getStringFieldNoAlloc(col_res, ri, 5);

            if (current_table == null or !std.mem.eql(u8, current_table.?, tname)) {
                // Flush previous table
                if (current_table != null) {
                    const has_pk = pk_cols.items.len > 0;
                    tables.append(.{
                        .name = current_table.?,
                        .columns = current_columns.toOwnedSlice() catch return error.OutOfMemory,
                        .primary_key_columns = pk_cols.toOwnedSlice() catch return error.OutOfMemory,
                        .has_primary_key = has_pk,
                    }) catch return error.OutOfMemory;
                    current_columns = std.ArrayList(EnhancedColumnInfo).init(allocator);
                    pk_cols = std.ArrayList([]const u8).init(allocator);
                }
                current_table = tname;
            } else {
                // Same table as current — free the duplicate allocation
                if (tname.len > 0) allocator.free(tname);
            }

            const is_pk = std.mem.eql(u8, pk_str, "true");
            const is_nullable = std.mem.eql(u8, nullable_str, "YES");
            const col_default: ?[]const u8 = if (default_str.len > 0) (allocator.dupe(u8, default_str) catch return error.OutOfMemory) else null;

            // FK lookup
            var fk_table: ?[]const u8 = null;
            var fk_col: ?[]const u8 = null;
            for (fk_entries.items) |fk| {
                if (std.mem.eql(u8, fk.table, if (current_table) |ct| ct else "") and std.mem.eql(u8, fk.column, cname)) {
                    fk_table = allocator.dupe(u8, fk.target_table) catch return error.OutOfMemory;
                    fk_col = allocator.dupe(u8, fk.target_column) catch return error.OutOfMemory;
                    break;
                }
            }

            // ENUM lookup
            var enum_vals: ?[][]const u8 = null;
            for (enum_entries.items) |entry| {
                if (std.mem.eql(u8, entry.key.table, if (current_table) |ct| ct else "") and std.mem.eql(u8, entry.key.column, cname)) {
                    // Dupe the values for ownership
                    const vals = allocator.alloc([]const u8, entry.values.items.len) catch return error.OutOfMemory;
                    for (entry.values.items, 0..) |v, vi| {
                        vals[vi] = allocator.dupe(u8, v) catch return error.OutOfMemory;
                    }
                    enum_vals = vals;
                    break;
                }
            }

            if (is_pk) {
                pk_cols.append(allocator.dupe(u8, cname) catch return error.OutOfMemory) catch return error.OutOfMemory;
            }

            current_columns.append(.{
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

        // Flush last table
        if (current_table != null) {
            const has_pk = pk_cols.items.len > 0;
            tables.append(.{
                .name = current_table.?,
                .columns = current_columns.toOwnedSlice() catch return error.OutOfMemory,
                .primary_key_columns = pk_cols.toOwnedSlice() catch return error.OutOfMemory,
                .has_primary_key = has_pk,
            }) catch return error.OutOfMemory;
        }

        return EnhancedSchemaInfo{
            .tables = tables.toOwnedSlice() catch return error.OutOfMemory,
            .allocator = allocator,
        };
    }
};

/// Get a raw string pointer from a PGresult without allocating (valid until PQclear).
fn getStringFieldNoAlloc(res: *c.PGresult, row: usize, col_idx: usize) []const u8 {
    if (c.PQgetisnull(res, @intCast(row), @intCast(col_idx)) != 0) return "";
    const val_ptr = c.PQgetvalue(res, @intCast(row), @intCast(col_idx));
    if (val_ptr == null) return "";
    return std.mem.sliceTo(val_ptr, 0);
}

fn getStringField(res: *c.PGresult, row: usize, col: usize, allocator: std.mem.Allocator) ![]const u8 {
    const val_ptr = c.PQgetvalue(res, @intCast(row), @intCast(col));
    if (val_ptr == null) return "";
    const val_slice = std.mem.sliceTo(val_ptr, 0);
    return allocator.dupe(u8, val_slice);
}

pub const QueryResult = struct {
    col_names: [][]const u8,
    rows: [][][]const u8,
    n_cols: usize,
    n_rows: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *QueryResult) void {
        for (self.col_names) |name| {
            if (name.len > 0 and !isStaticStr(name)) self.allocator.free(name);
        }
        self.allocator.free(self.col_names);
        for (self.rows) |row| {
            for (row) |val| {
                if (val.len > 0 and !isStaticStr(val)) self.allocator.free(val);
            }
            self.allocator.free(row);
        }
        self.allocator.free(self.rows);
        self.* = undefined;
    }
};

fn isStaticStr(s: []const u8) bool {
    return std.mem.eql(u8, s, "NULL") or s.len == 0;
}

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

    pub fn deinit(self: *EnhancedTableInfo, allocator: std.mem.Allocator) void {
        for (self.columns) |col| {
            if (col.name.len > 0) allocator.free(col.name);
            if (col.data_type.len > 0) allocator.free(col.data_type);
            if (col.column_default) |d| allocator.free(d);
            if (col.fk_target_table) |t| allocator.free(t);
            if (col.fk_target_column) |tc| allocator.free(tc);
            if (col.enum_values) |vals| {
                for (vals) |v| allocator.free(v);
                allocator.free(vals);
            }
        }
        allocator.free(self.columns);
        for (self.primary_key_columns) |pk| allocator.free(pk);
        allocator.free(self.primary_key_columns);
        if (self.name.len > 0) allocator.free(self.name);
    }
};

pub const EnhancedSchemaInfo = struct {
    tables: []EnhancedTableInfo,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EnhancedSchemaInfo) void {
        for (self.tables) |*table| {
            @constCast(table).deinit(self.allocator);
        }
        self.allocator.free(self.tables);
        self.* = undefined;
    }
};

pub const SchemaInfo = struct {
    tables: []TableInfo,
    allocator: std.mem.Allocator,

    /// Format the schema as a concise text description.
    pub fn format(self: *const SchemaInfo, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8).init(allocator);
        errdefer buf.deinit();

        try buf.appendSlice("DATABASE SCHEMA:\n");
        for (self.tables) |table| {
            try std.fmt.format(buf.writer(), "  {s}(", .{table.name});
            for (table.columns, 0..) |col, i| {
                if (i > 0) try buf.appendSlice(", ");
                try std.fmt.format(buf.writer(), "{s} {s}", .{ col.name, col.data_type });
            }
            try buf.appendSlice(")\n");
        }

        return buf.toOwnedSlice();
    }

    pub fn deinit(self: *SchemaInfo) void {
        for (self.tables) |table| {
            for (table.columns) |col| {
                if (col.name.len > 0) self.allocator.free(col.name);
                if (col.data_type.len > 0) self.allocator.free(col.data_type);
            }
            self.allocator.free(table.columns);
            if (table.name.len > 0) self.allocator.free(table.name);
        }
        self.allocator.free(self.tables);
        self.* = undefined;
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "isStaticStr: identifies static strings" {
    try std.testing.expect(isStaticStr("NULL"));
    try std.testing.expect(isStaticStr(""));
    try std.testing.expect(!isStaticStr("hello"));
}

test "SchemaInfo.format: produces readable output" {
    const allocator = std.testing.allocator;
    var cols = [_]ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
        .{ .name = "name", .data_type = "text" },
    };
    var tables = [_]TableInfo{
        .{ .name = "users", .columns = &cols },
    };
    var schema = SchemaInfo{ .tables = &tables, .allocator = allocator };
    const text = try schema.format(allocator);
    defer allocator.free(text);

    try std.testing.expect(std.mem.indexOf(u8, text, "DATABASE SCHEMA:") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "users(") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "id integer") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "name text") != null);
}
