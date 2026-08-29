import { api, ApiError } from '$lib/api/client';
import { toast } from 'svelte-sonner';

// --- Connection state ---
export type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

export interface Column {
	name: string;
	type: string;
	is_nullable?: boolean;
	is_primary_key?: boolean;
	column_default?: string | null;
	fk_target_table?: string | null;
	fk_target_column?: string | null;
}

export interface Table {
	name: string;
	columns: Column[];
	has_primary_key?: boolean;
	primary_key_columns?: string[];
}

// Svelte 5 runes-based reactive state
let connectionStatus = $state<ConnectionStatus>('disconnected');
let connectionColor = $state<string>('#3b82f6');
let tables = $state<Table[]>([]);
let selectedTable = $state<Table | null>(null);
let readOnly = $state(false);
let tableSearch = $state('');

// Table data
let tableColumns = $state<string[]>([]);
let tableRows = $state<(string | null)[][]>([]);
let totalRows = $state(0);
let hasMore = $state(false);
let pageSize = $state(50);
let currentOffset = $state(0);
let sortColumn = $state<string | null>(null);
let sortDirection = $state<'asc' | 'desc'>('asc');
let loading = $state(false);

// Health check
let healthInterval: ReturnType<typeof setInterval> | null = null;
let latencyMs = $state<number | null>(null);

export const appState = {
	get connectionStatus() { return connectionStatus; },
	get connectionColor() { return connectionColor; },
	get tables() { return tables; },
	get selectedTable() { return selectedTable; },
	get readOnly() { return readOnly; },
	get tableSearch() { return tableSearch; },
	set tableSearch(v: string) { tableSearch = v; },

	get tableColumns() { return tableColumns; },
	get tableRows() { return tableRows; },
	get totalRows() { return totalRows; },
	get hasMore() { return hasMore; },
	get pageSize() { return pageSize; },
	get currentOffset() { return currentOffset; },
	get sortColumn() { return sortColumn; },
	get sortDirection() { return sortDirection; },
	get loading() { return loading; },
	get latencyMs() { return latencyMs; },

	get filteredTables(): Table[] {
		if (!tableSearch.trim()) return tables;
		const q = tableSearch.toLowerCase();
		return tables.filter(t => t.name.toLowerCase().includes(q));
	},

	async connect(params: { host: string; port: string; database: string; user: string; password: string }) {
		connectionStatus = 'connecting';
		try {
			await api.connect(params);
			connectionStatus = 'connected';
			await this.loadSchema();
			this.startHealthCheck();
			const ro = await api.getReadOnly();
			readOnly = ro.read_only;
			toast.success('Connected');
		} catch (e) {
			connectionStatus = 'error';
			toast.error(e instanceof Error ? e.message : 'Connection failed');
		}
	},

	async connectRaw(connectionString: string) {
		connectionStatus = 'connecting';
		try {
			await api.connectRaw(connectionString);
			connectionStatus = 'connected';
			await this.loadSchema();
			this.startHealthCheck();
			const ro = await api.getReadOnly();
			readOnly = ro.read_only;
			toast.success('Connected');
		} catch (e) {
			connectionStatus = 'error';
			toast.error(e instanceof Error ? e.message : 'Connection failed');
		}
	},

	async loadSchema() {
		try {
			const schema = await api.getSchema();
			tables = schema.tables;
		} catch (e) {
			toast.error('Failed to load schema');
		}
	},

	async selectTable(table: Table) {
		selectedTable = table;
		currentOffset = 0;
		sortColumn = null;
		sortDirection = 'asc';
		await this.loadTableData();
	},

	async loadTableData() {
		if (!selectedTable) return;
		loading = true;
		try {
			const data = await api.getTableData(selectedTable.name, {
				limit: pageSize,
				offset: currentOffset,
				sort: sortColumn ?? undefined,
				dir: sortDirection
			});
			tableColumns = data.columns;
			tableRows = data.rows;
			totalRows = data.total;
			hasMore = currentOffset + data.rows.length < data.total;
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load data');
		} finally {
			loading = false;
		}
	},

	async sort(column: string) {
		if (sortColumn === column) {
			sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
		} else {
			sortColumn = column;
			sortDirection = 'asc';
		}
		currentOffset = 0;
		await this.loadTableData();
	},

	async nextPage() {
		if (!hasMore) return;
		currentOffset += pageSize;
		await this.loadTableData();
	},

	async prevPage() {
		if (currentOffset === 0) return;
		currentOffset = Math.max(0, currentOffset - pageSize);
		await this.loadTableData();
	},

	async toggleReadOnly() {
		try {
			await api.setReadOnly(!readOnly);
			readOnly = !readOnly;
			toast.success(readOnly ? 'Read-only enabled' : 'Read-only disabled');
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to toggle read-only');
		}
	},

	setConnectionColor(color: string) {
		connectionColor = color;
	},

	startHealthCheck() {
		this.stopHealthCheck();
		healthInterval = setInterval(async () => {
			try {
				const h = await api.health();
				latencyMs = h.latency_ms ?? null;
				if (connectionStatus !== 'connected') {
					connectionStatus = 'connected';
				}
			} catch {
				connectionStatus = 'error';
				latencyMs = null;
			}
		}, 10000);
	},

	stopHealthCheck() {
		if (healthInterval) {
			clearInterval(healthInterval);
			healthInterval = null;
		}
	},

	disconnect() {
		this.stopHealthCheck();
		connectionStatus = 'disconnected';
		tables = [];
		selectedTable = null;
		tableColumns = [];
		tableRows = [];
		totalRows = 0;
		latencyMs = null;
	}
};
