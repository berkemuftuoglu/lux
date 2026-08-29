const BASE = '';

export class ApiError extends Error {
	status: number;
	constructor(status: number, message: string) {
		super(message);
		this.status = status;
	}
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
	const opts: RequestInit = {
		method,
		headers: { 'Content-Type': 'application/json' }
	};
	if (body !== undefined) {
		opts.body = JSON.stringify(body);
	}
	const res = await fetch(`${BASE}${path}`, opts);
	if (!res.ok) {
		const text = await res.text();
		let msg = text;
		try {
			const json = JSON.parse(text);
			msg = json.error || json.message || text;
		} catch {
			// plain text error
		}
		throw new ApiError(res.status, msg);
	}
	const contentType = res.headers.get('content-type');
	if (contentType?.includes('application/json')) {
		return res.json();
	}
	return res.text() as unknown as T;
}

export function parseConnectionString(conninfo: string): {
	host: string;
	port: string;
	database: string;
	user: string;
} {
	// postgresql://user:pass@host:port/db
	try {
		const u = new URL(conninfo);
		return {
			host: u.hostname,
			port: u.port || '5432',
			database: u.pathname.replace(/^\//, ''),
			user: decodeURIComponent(u.username)
		};
	} catch {
		return { host: '', port: '5432', database: '', user: '' };
	}
}

export const api = {
	// Connection
	connect(params: {
		host: string;
		port: string;
		database: string;
		user: string;
		password: string;
	}) {
		const conninfo = `postgresql://${encodeURIComponent(params.user)}:${encodeURIComponent(params.password)}@${params.host}:${params.port}/${encodeURIComponent(params.database)}`;
		return request<{ status: string }>('POST', '/api/connect', { conninfo });
	},

	connectRaw(connectionString: string) {
		return request<{ status: string }>('POST', '/api/connect', { conninfo: connectionString });
	},

	reconnect() {
		return request<{ status: string }>('POST', '/api/reconnect');
	},

	// Schema
	getSchema() {
		return request<{
			tables: Array<{
				name: string;
				columns: Array<{
					name: string;
					type: string;
					is_nullable?: boolean;
					is_primary_key?: boolean;
					column_default?: string | null;
					fk_target_table?: string | null;
					fk_target_column?: string | null;
					enum_values?: string[];
				}>;
				has_primary_key?: boolean;
				primary_key_columns?: string[];
			}>;
		}>('GET', '/api/schema');
	},

	// Table data — shape matches src/handlers/crud.zig:sendTableDataJson
	getTableData(
		table: string,
		params?: {
			limit?: number;
			offset?: number;
			sort?: string;
			dir?: string;
			after?: string;
		}
	) {
		const searchParams = new URLSearchParams();
		if (params?.limit) searchParams.set('limit', String(params.limit));
		if (params?.offset !== undefined) searchParams.set('offset', String(params.offset));
		if (params?.sort) searchParams.set('sort', params.sort);
		if (params?.dir) searchParams.set('dir', params.dir);
		if (params?.after) searchParams.set('after', params.after);
		const qs = searchParams.toString();
		const path = `/api/tables/${table}/data${qs ? `?${qs}` : ''}`;
		return request<{
			total: number;
			limit: number;
			offset: number;
			count_exact: boolean;
			pk_mode: 'column' | 'ctid';
			pagination: 'keyset' | 'offset';
			has_primary_key?: boolean;
			pk_columns?: string[];
			columns: string[];
			rows: (string | null)[][];
			first_cursor?: string;
			last_cursor?: string;
		}>('GET', path);
	},

	// SQL
	executeSql(sql: string) {
		return request<{
			row_count?: number;
			columns?: string[];
			rows?: (string | null)[][];
			requires_confirmation?: boolean;
			operation?: string;
			warning?: string;
			error?: string;
		}>('POST', '/api/sql', { sql });
	},

	previewSql(sql: string) {
		return request<{
			preview?: boolean;
			affected_rows?: number;
			columns?: string[];
			rows?: (string | null)[][];
		}>('POST', '/api/sql/preview', { sql });
	},

	// CRUD — shapes match src/handlers/crud.zig
	updateCell(params: {
		table: string;
		column: string;
		value: string | null;
		pk_column: string;
		pk_value: string;
		pk_mode?: 'column' | 'ctid';
		old_value?: string;
	}) {
		return request<{ success: true; journal_id: number }>('POST', '/api/update', {
			...params,
			value: params.value === null ? '__NULL__' : params.value
		});
	},

	insertRow(params: { table: string; values: Record<string, string | null> }) {
		// Convert nulls to backend sentinel — backend parses "__NULL__" as SQL NULL
		const payload: Record<string, string> = {};
		for (const [k, v] of Object.entries(params.values)) {
			payload[k] = v === null ? '__NULL__' : v;
		}
		return request<{ success: true; columns: string[]; row: (string | null)[] }>(
			'POST',
			'/api/insert-row',
			{ table: params.table, values: payload }
		);
	},

	deleteRow(params: {
		table: string;
		pk_column: string;
		pk_value: string;
		pk_mode?: 'column' | 'ctid';
	}) {
		return request<{ success: true }>('POST', '/api/delete-row', params);
	},

	truncateTable(table: string) {
		return request<{ success: true }>('POST', `/api/tables/${table}/truncate`);
	},

	bulkUpdate(
		table: string,
		params: {
			column: string;
			find: string;
			replace: string;
			force?: boolean;
		}
	) {
		return request<{
			affected_rows?: number;
			requires_confirmation?: boolean;
			success?: boolean;
		}>('POST', `/api/tables/${table}/bulk-update`, {
			...params,
			// force is a string "true"/"false" on the wire — backend reads it as a string
			force: params.force === true ? 'true' : 'false'
		});
	},

	// FK lookup — backend infers fk_target_table/fk_target_column from schema by column name
	getFkLookup(table: string, params: { column: string; search?: string; limit?: number }) {
		const sp = new URLSearchParams({ column: params.column });
		if (params.search) sp.set('search', params.search);
		if (params.limit) sp.set('limit', String(params.limit));
		return request<{ values: (string | null)[] }>(
			'GET',
			`/api/tables/${table}/fk-lookup?${sp}`
		);
	},

	// DDL & Stats
	getTableDdl(table: string) {
		return request<{ ddl: string }>('GET', `/api/tables/${table}/ddl`);
	},

	getTableStats(table: string) {
		return request<{
			total_size: string;
			table_size: string;
			index_size: string;
			row_estimate: number;
		}>('GET', `/api/tables/${table}/stats`);
	},

	// Export
	exportTable(table: string, format: 'csv' | 'json' = 'csv') {
		return request<string>('GET', `/api/export/${table}?format=${format}`);
	},

	exportSql(sql: string, format: 'csv' | 'json' = 'csv') {
		return request<string>('POST', '/api/sql/export', { sql, format });
	},

	// CSV Import — response key is `imported`, not `inserted_rows`
	importCsv(table: string, csv: string, has_header: boolean = true) {
		return request<{ imported: number }>('POST', `/api/tables/${table}/import`, {
			csv,
			has_header: has_header ? 'true' : 'false'
		});
	},

	// Settings
	getReadOnly() {
		return request<{ read_only: boolean }>('GET', '/api/settings/read-only');
	},

	// Backend reads {enabled: "true"|"false"} — sending legacy shape to match current handler
	setReadOnly(readOnly: boolean) {
		return request<{ read_only: boolean }>('POST', '/api/settings/read-only', {
			enabled: readOnly ? 'true' : 'false'
		});
	},

	// Connections — backend returns conninfo URI, not separate host/port/database/user
	getConnections() {
		return request<{
			connections: Array<{
				id: number;
				name: string;
				conninfo: string;
				color: string;
			}>;
		}>('GET', '/api/connections');
	},

	saveConnection(conn: {
		name: string;
		color: string;
		host: string;
		port: string;
		database: string;
		user: string;
		password: string;
	}) {
		const conninfo = `postgresql://${encodeURIComponent(conn.user)}:${encodeURIComponent(conn.password)}@${conn.host}:${conn.port}/${encodeURIComponent(conn.database)}`;
		return request<{ id: number }>('POST', '/api/connections', {
			name: conn.name,
			conninfo,
			color: conn.color
		});
	},

	deleteConnection(id: number | string) {
		return request<{ ok: true }>('DELETE', `/api/connections/${id}`);
	},

	// History — timestamp is unix seconds (number), not string
	getHistory() {
		return request<{
			entries: Array<{
				sql: string;
				timestamp: number;
				duration_ms: number;
				row_count: number | null;
				is_error: boolean;
				error?: string;
			}>;
		}>('GET', '/api/history');
	},

	// Journal — entries have stable `id` (u64) and `undone` flag; timestamp is unix seconds
	getJournal() {
		return request<{
			entries: Array<{
				id: number;
				timestamp: number;
				table: string;
				operation: 'update' | 'delete' | 'insert' | 'truncate';
				column: string;
				old_value: string;
				new_value: string;
				pk_column: string;
				pk_value: string;
				undone: boolean;
			}>;
		}>('GET', '/api/journal');
	},

	// Undo by stable entry ID — backend parses id as string via std.fmt.parseInt(u64, id_str, 10)
	// Do NOT pass a visual array index — pass entry.id from the /api/journal response
	undoJournal(id: number) {
		return request<{ success: true }>('POST', '/api/journal/undo', { id: String(id) });
	},

	// Health — backend returns {connected, latency_ms}, NOT {status, latency_ms}
	health() {
		return request<{ connected: boolean; latency_ms?: number }>('GET', '/api/health');
	}
};
