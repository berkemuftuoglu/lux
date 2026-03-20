// state.js -- Shared mutable state (loaded first in index.html)
// All cross-file state is encapsulated in this single object.

const State = {
	schemaData: null,
	currentTable: null,
	currentColumns: [],
	currentRows: [],
	currentPkColumns: [],
	currentPkMode: 'column',
	readOnlyMode: false,
	host: null,
	port: null,
	database: null,
	dbConnected: false,
	pageOffset: 0,
	pageLimit: 100,
	totalRows: 0,
	sortCol: null,
	sortDir: 'asc',
	editingCell: null,
	journalCount: 0,
	erZoom: 1,
	erPanX: 0,
	erPanY: 0,
	focusRow: -1,
	focusCol: -1,
	columnFilters: {},
	columnWidths: {},
	selectedRows: new Set(),
	lastSelectedRow: -1,
	lastSqlQuery: '',
	healthCheckInterval: null,
	pgVersion: null,
	detailRowIdx: -1,
};

// Event names for cross-module communication.
// Modules listen on document for these CustomEvents instead of
// being called directly, keeping coupling one-directional.
const LuxEvents = {
	CONNECTED: 'lux:connected',
	DISCONNECTED: 'lux:disconnected',
	TABLE_SELECTED: 'lux:table-selected',
	SCHEMA_LOADED: 'lux:schema-loaded',
	THEME_CHANGED: 'lux:theme-changed',
	READONLY_CHANGED: 'lux:readonly-changed',
};

function setCurrentTable(name) {
	State.currentTable = name;
	document.dispatchEvent(new CustomEvent(LuxEvents.TABLE_SELECTED, { detail: { table: name } }));
}

function setPageState(offset, limit) {
	State.pageOffset = offset;
	State.pageLimit = limit;
}

function resetTableView() {
	State.pageOffset = 0;
	State.sortCol = null;
	State.sortDir = 'asc';
	State.columnFilters = {};
	State.focusRow = -1;
	State.focusCol = -1;
	State.selectedRows = new Set();
	State.lastSelectedRow = -1;
}

function setConnected(conninfo) {
	State.dbConnected = true;
	if (conninfo) {
		if (conninfo.host) State.host = conninfo.host;
		if (conninfo.port) State.port = conninfo.port;
		if (conninfo.database) State.database = conninfo.database;
	}
	document.dispatchEvent(new CustomEvent(LuxEvents.CONNECTED, { detail: conninfo }));
}

function setDisconnected() {
	State.dbConnected = false;
	State.currentTable = null;
	document.dispatchEvent(new CustomEvent(LuxEvents.DISCONNECTED));
}
