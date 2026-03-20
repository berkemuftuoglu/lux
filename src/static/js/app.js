// app.js — init, event wiring, theme, keyboard shortcuts, resize handlers
// All modal/feature handlers are in dedicated modules:
//   table-ops.js   — insert row, find/replace, DDL, stats, truncate, import CSV
//   table-create.js — create table modal
//   cmd-palette.js — command palette (Ctrl+K)
//   saved-queries.js — saved queries CRUD
//   connection.js  — connect, saved connections, health check
//   sql.js         — SQL editor, autocomplete, run/explain/format
//   sql-tabs.js    — SQL tab management
//   sidebar.js     — schema tree, ER diagram, journal
//   grid.js        — data grid, cell editing, pagination
//   crud.js        — bulk operations, row detail

const HEALTH_CHECK_INTERVAL = 30000;
const AC_LINE_HEIGHT = 23;
const AC_CHAR_WIDTH = 8.4;

function setTheme(theme) {
	document.documentElement.setAttribute('data-theme', theme);
	localStorage.setItem('lux-theme', theme);
	$('theme-icon-sun').style.display = theme === 'dark' ? 'none' : 'block';
	$('theme-icon-moon').style.display = theme === 'dark' ? 'block' : 'none';
	if ($('panel-er').classList.contains('active')) drawER();
}
$('btn-theme').onclick = () => {
	const current = document.documentElement.getAttribute('data-theme');
	setTheme(current === 'dark' ? 'light' : 'dark');
};
const savedTheme = localStorage.getItem('lux-theme');
if (savedTheme) setTheme(savedTheme);
else if (window.matchMedia('(prefers-color-scheme: light)').matches)
	setTheme('light');
else setTheme('dark');

// Breadcrumb: reflects current host, database, and selected table
function updateBreadcrumb() {
	const bcHost = $('bc-host');
	const bcDb = $('bc-db');
	const bcTable = $('bc-table');
	const bcSepTable = $('bc-sep-table');
	const bcSepDb = $('bc-sep-db');

	if (!State.dbConnected) {
		if (bcHost) bcHost.textContent = 'Lux';
		if (bcDb) bcDb.textContent = '';
		if (bcSepDb) bcSepDb.style.display = 'none';
		if (bcSepTable) bcSepTable.style.display = 'none';
		if (bcTable) { bcTable.textContent = ''; bcTable.classList.remove('current'); }
		return;
	}

	if (bcHost) bcHost.textContent = State.host || 'localhost';
	const hasDb = State.database && State.database.length > 0;
	if (bcSepDb) bcSepDb.style.display = hasDb ? '' : 'none';
	if (bcDb) bcDb.textContent = hasDb ? State.database : '';
	if (bcTable && bcSepTable) {
		const table = State.currentTable;
		if (table) {
			bcTable.textContent = table;
			bcSepTable.style.display = '';
			bcTable.classList.add('current');
		} else {
			bcTable.textContent = '';
			bcSepTable.style.display = 'none';
			bcTable.classList.remove('current');
		}
	}
}

// Schema tooltip: floating info about the currently selected table
function updateSchemaTooltip(tableName) {
	const tooltip = $('schema-tooltip');
	const body = $('schema-tooltip-body');
	if (!tooltip || !body) return;

	if (!tableName) {
		tooltip.classList.remove('show');
		return;
	}

	const meta = getTableMeta(tableName);
	if (!meta) {
		tooltip.classList.remove('show');
		return;
	}

	const colCount = meta.columns ? meta.columns.length : 0;
	const constraints = [];
	let hasPk = false;
	let fkCount = 0;
	if (meta.columns) {
		meta.columns.forEach((col) => {
			if (col.is_primary_key) hasPk = true;
			if (col.fk_target_table) fkCount++;
		});
	}
	if (hasPk) constraints.push('primary key');
	if (fkCount > 0) constraints.push(fkCount + ' foreign key' + (fkCount > 1 ? 's' : ''));
	const constraintInfo = constraints.length > 0 ? constraints.join(', ') : 'no constraints';

	body.textContent = 'public.' + tableName + ' \u2014 ' + colCount + ' columns, ' + constraintInfo;
	tooltip.classList.add('show');
}

// Status bar: update execution time display
function updateStatusExecTime(ms) {
	const el = $('status-exec-time');
	if (el) {
		el.textContent = ms != null ? ms + 'ms execution' : '';
	}
}

// Status bar: show editor indicators when in SQL view
function updateStatusIndicators(show) {
	const el = $('status-indicators');
	if (el) {
		el.textContent = show ? 'UTF-8 | LF | SQL' : '';
	}
}

// Panels that don't need the sidebar
const fullWidthPanels = new Set(['conns', 'settings']);

function switchTab(tabName) {
	$$('.tab').forEach((t) => {
		t.classList.remove('active');
		t.setAttribute('aria-selected', 'false');
	});
	$$('.tab-panel').forEach((p) => p.classList.remove('active'));
	const tab = document.querySelector(`.tab[data-tab="${tabName}"]`);
	if (tab) {
		tab.classList.add('active');
		tab.setAttribute('aria-selected', 'true');
	}
	const panel = $(`panel-${tabName}`);
	if (panel) panel.classList.add('active');
	if (tabName === 'journal') {
		loadJournal();
		clearJournalBadge();
	}
	if (tabName === 'er') drawER();
	if (tabName === 'conns') renderConnsDashboard();
	if (tabName === 'settings') updateSettingsPanel();
	updateStatusIndicators(tabName === 'sql');

	// Hide sidebar for full-width panels
	const sidebar = $('sidebar');
	const sidebarResize = $('sidebar-resize');
	if (fullWidthPanels.has(tabName)) {
		if (sidebar) sidebar.style.display = 'none';
		if (sidebarResize) sidebarResize.style.display = 'none';
	} else {
		if (sidebar) sidebar.style.display = '';
		if (sidebarResize) sidebarResize.style.display = '';
	}
}

$$('.tab').forEach((tab) => {
	tab.addEventListener('click', () => {
		switchTab(tab.dataset.tab);
		$$('.rail-btn').forEach((r) => r.classList.remove('active'));
		const railBtn = document.querySelector(`.rail-btn[data-rail="${tab.dataset.tab}"]`);
		if (railBtn) railBtn.classList.add('active');
	});
});

$$('.rail-btn[data-rail]').forEach((btn) => {
	btn.addEventListener('click', () => {
		$$('.rail-btn').forEach((r) => r.classList.remove('active'));
		btn.classList.add('active');
		switchTab(btn.dataset.rail);
	});
});

$('conn-btn').onclick = doConnect;
if ($('conn-btn-raw')) $('conn-btn-raw').onclick = doConnect;
if ($('conn-input')) {
	$('conn-input').addEventListener('keydown', (e) => {
		if (e.key === 'Enter') doConnect();
	});
}
['conn-host', 'conn-port', 'conn-database', 'conn-user', 'conn-pass'].forEach(id => {
	const el = $(id);
	if (el) el.addEventListener('keydown', (e) => { if (e.key === 'Enter') doConnect(); });
});

if ($('conn-toggle-raw')) {
	$('conn-toggle-raw').onclick = () => {
		$('conn-fields').style.display = 'none';
		$('conn-raw').style.display = '';
	};
}
if ($('conn-toggle-fields')) {
	$('conn-toggle-fields').onclick = () => {
		$('conn-raw').style.display = 'none';
		$('conn-fields').style.display = '';
	};
}

// Overflow menu for table actions (DDL, Stats, Truncate, Import, Export)
// Static menu items — no user data in markup, safe to construct as DOM strings
function showOverflowMenu() {
	const menu = $('overflow-menu');
	const btn = $('btn-overflow');
	if (!btn) return;
	const rect = btn.getBoundingClientRect();
	const items = [
		{ action: 'ddl', label: 'Show CREATE TABLE' },
		{ action: 'stats', label: 'Table Statistics' },
		{ action: 'sep' },
		{ action: 'import', label: 'Import CSV' },
		{ action: 'export-csv', label: 'Export as CSV' },
		{ action: 'export-json', label: 'Export as JSON' },
		{ action: 'sep' },
		{ action: 'truncate', label: 'Truncate Table', danger: true },
	];
	menu.textContent = '';
	const actionMap = {
		ddl: () => typeof showDDL === 'function' && showDDL(),
		stats: () => typeof showStats === 'function' && showStats(),
		truncate: () => typeof truncateTable === 'function' && truncateTable(),
		import: () => typeof showImportCSV === 'function' && showImportCSV(),
		'export-csv': () => {
			if (State.currentTable)
				window.location.href = '/api/export/' + encodeURIComponent(State.currentTable) + '?format=csv';
		},
		'export-json': () => {
			if (State.currentTable)
				window.location.href = '/api/export/' + encodeURIComponent(State.currentTable) + '?format=json';
		},
	};
	for (const item of items) {
		if (item.action === 'sep') {
			menu.appendChild(h('div', { cls: 'ctx-menu-sep' }));
		} else {
			menu.appendChild(h('div', {
				cls: 'ctx-menu-item' + (item.danger ? ' danger' : ''),
				text: item.label,
				onclick: () => {
					menu.classList.remove('open');
					const fn = actionMap[item.action];
					if (fn) fn();
				},
			}));
		}
	}
	menu.style.left = rect.left + 'px';
	menu.style.top = (rect.bottom + 4) + 'px';
	menu.classList.add('open');
}
if ($('btn-overflow')) $('btn-overflow').onclick = showOverflowMenu;
document.addEventListener('click', (e) => {
	const om = $('overflow-menu');
	if (om && !om.contains(e.target) && e.target !== $('btn-overflow')) {
		om.classList.remove('open');
	}
});

$('btn-refresh').onclick = async () => {
	if (State.dbConnected) {
		await loadSchema();
		toast('Schema refreshed', 'success');
	}
};
$('btn-tbl-refresh').onclick = () => loadTableData();

$('btn-saved-queries').onclick = () => {
	renderSavedQueries();
	openModal('saved-overlay');
};
$('btn-save-current').onclick = async () => {
	const sql = sqlEditor.value.trim();
	if (!sql) {
		toast('Write a query first', 'error');
		return;
	}
	const name = await promptUser('Query name:');
	if (!name) return;
	const queries = getSavedQueries();
	queries.unshift({ name, sql, saved_at: Date.now() });
	setSavedQueries(queries);
	renderSavedQueries();
	toast('Query saved', 'success');
};
$('saved-overlay').onclick = (e) => {
	if (e.target === $('saved-overlay')) {
		releaseFocus($('saved-overlay'));
		closeModal('saved-overlay');
	}
};

$('sql-tab-add').onclick = addSqlTab;

function isInputFocused() {
	const tag = document.activeElement.tagName;
	return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
}

function isGridNavigable() {
	return $('panel-tables').classList.contains('active') &&
		State.currentRows.length > 0 &&
		!State.editingCell &&
		!isInputFocused();
}

registerKeyAction('focusSql',
	(e) => (e.ctrlKey || e.metaKey) && e.key === 'l',
	() => true,
	() => { switchTab('sql'); sqlEditor.focus(); }
);

registerKeyAction('findReplace',
	(e) => (e.ctrlKey || e.metaKey) && e.key === 'f',
	() => State.currentTable && State.dbConnected && $('panel-tables').classList.contains('active'),
	() => { $('btn-find-replace').click(); }
);

registerKeyAction('escape',
	(e) => e.key === 'Escape',
	() => true,
	() => {
		const modals = [
			'cmd-overlay', 'prompt-overlay', 'confirm-overlay',
			'detail-overlay', 'ddl-overlay', 'shortcuts-overlay',
			'history-overlay', 'saved-overlay', 'save-conn-overlay',
			'create-table-overlay', 'import-overlay', 'fnr-overlay',
			'insert-overlay',
		];
		for (const id of modals) {
			const el = $(id);
			if (el && (el.classList.contains('open') || el.classList.contains('visible'))) {
				if (id === 'prompt-overlay' && promptResolve) promptResolve(null);
				if (id === 'confirm-overlay' && confirmResolve) confirmResolve(false);
				releaseFocus(el);
				closeModal(id);
				el.classList.remove('visible');
				return;
			}
		}
		$('ctx-menu').classList.remove('open');
		const tip = document.querySelector('.stats-tip');
		if (tip) { tip.remove(); return; }
		if (State.editingCell) { cancelEdit(); return; }
		if (State.selectedRows.size > 0) {
			State.selectedRows.clear();
			State.lastSelectedRow = -1;
			updateRowSelection();
		} else {
			State.focusRow = -1;
			State.focusCol = -1;
			updateCellFocus();
		}
	}
);

registerKeyAction('showShortcuts',
	(e) => e.key === '?',
	() => !State.editingCell && !isInputFocused(),
	() => { openModal('shortcuts-overlay'); }
);

registerKeyAction('newSqlTab',
	(e) => (e.ctrlKey || e.metaKey) && e.key === 'n',
	() => true,
	() => { addSqlTab(); switchTab('sql'); }
);

registerKeyAction('saveConnection',
	(e) => (e.ctrlKey || e.metaKey) && e.key === 's',
	() => State.dbConnected,
	() => { $('save-conn-btn').click(); }
);

registerKeyAction('gridArrowDown',
	(e) => e.key === 'ArrowDown',
	isGridNavigable,
	() => {
		const maxRow = State.currentRows.length - 1;
		State.focusRow = Math.min(State.focusRow + 1, maxRow);
		if (State.focusCol < 0) State.focusCol = 0;
		updateCellFocus();
	}
);

registerKeyAction('gridArrowUp',
	(e) => e.key === 'ArrowUp',
	isGridNavigable,
	() => {
		State.focusRow = Math.max(State.focusRow - 1, 0);
		if (State.focusCol < 0) State.focusCol = 0;
		updateCellFocus();
	}
);

registerKeyAction('gridArrowRight',
	(e) => e.key === 'ArrowRight',
	isGridNavigable,
	() => {
		const maxCol = displayCols().length - 1;
		State.focusCol = Math.min(State.focusCol + 1, maxCol);
		if (State.focusRow < 0) State.focusRow = 0;
		updateCellFocus();
	}
);

registerKeyAction('gridArrowLeft',
	(e) => e.key === 'ArrowLeft',
	isGridNavigable,
	() => {
		State.focusCol = Math.max(State.focusCol - 1, 0);
		if (State.focusRow < 0) State.focusRow = 0;
		updateCellFocus();
	}
);

registerKeyAction('gridTab',
	(e) => e.key === 'Tab',
	() => isGridNavigable() && State.focusRow >= 0,
	(e) => {
		const maxCol = displayCols().length - 1;
		if (e.shiftKey) State.focusCol = Math.max(State.focusCol - 1, 0);
		else State.focusCol = Math.min(State.focusCol + 1, maxCol);
		updateCellFocus();
	}
);

registerKeyAction('gridEnter',
	(e) => e.key === 'Enter',
	() => isGridNavigable() && State.focusRow >= 0 && State.focusCol >= 0,
	() => {
		const rows = $('grid-body').querySelectorAll('tr');
		if (State.focusRow < rows.length) {
			const cells = rows[State.focusRow].querySelectorAll('td.editable');
			if (State.focusCol < cells.length) startEdit(cells[State.focusCol]);
		}
	}
);

// Cross-module event listeners -- each module reacts to state changes
// without the source module needing to know who listens.
document.addEventListener(LuxEvents.TABLE_SELECTED, (e) => {
	const name = e.detail?.table;
	updateBreadcrumb();
	updateSchemaTooltip(name);
	loadTableData();
});

document.addEventListener(LuxEvents.CONNECTED, (e) => {
	updateConnUI();
	updateBreadcrumb();
	loadSchema();
	startHealthCheck();
	const detail = e.detail || {};
	if (detail.conninfo) {
		const savedColor = getSavedConnColor(detail.conninfo);
		setConnStripe(savedColor || detectConnColor(detail.conninfo));
	}
	renderSavedConnectionCards();
});

document.addEventListener(LuxEvents.SCHEMA_LOADED, () => {
	renderSidebar();
});

// Deferred until DOMContentLoaded so all modules are fully parsed
async function init() {
	updateDestructiveButtons(false);
	await loadReadOnly();
	try {
		const data = await fetchJson('/api/schema');
		if (data.tables && data.tables.length > 0) {
			State.schemaData = data;
			renderSidebar();
			setConnected({ host: State.host, port: State.port, database: State.database });
		}
	} catch (_e) {
		/* no pre-existing connection -- show welcome screen */
	}
}
document.addEventListener('DOMContentLoaded', () => {
	init();
	loadSavedConnections();
});

window.addEventListener('resize', () => {
	if ($('panel-er').classList.contains('active')) drawER();
});

(() => {
	const handle = $('sidebar-resize');
	const sidebar = $('sidebar');
	let dragging = false;

	handle.addEventListener('mousedown', (e) => {
		e.preventDefault();
		dragging = true;
		handle.classList.add('dragging');
		document.body.style.cursor = 'col-resize';
		document.body.style.userSelect = 'none';
	});

	document.addEventListener('mousemove', (e) => {
		if (!dragging) return;
		const newW = Math.min(500, Math.max(180, e.clientX));
		sidebar.style.width = `${newW}px`;
	});

	document.addEventListener('mouseup', () => {
		if (!dragging) return;
		dragging = false;
		handle.classList.remove('dragging');
		document.body.style.cursor = '';
		document.body.style.userSelect = '';
	});
})();

(() => {
	const handle = $('sql-resize');
	const wrap = document.querySelector('.sql-editor-wrap');
	if (!handle || !wrap) return;
	let dragging = false;
	let startY, startH;

	handle.addEventListener('mousedown', (e) => {
		e.preventDefault();
		dragging = true;
		startY = e.clientY;
		startH = wrap.offsetHeight;
		handle.classList.add('dragging');
		document.body.style.cursor = 'row-resize';
		document.body.style.userSelect = 'none';
	});

	document.addEventListener('mousemove', (e) => {
		if (!dragging) return;
		const newH = Math.min(600, Math.max(80, startH + (e.clientY - startY)));
		wrap.style.height = `${newH}px`;
	});

	document.addEventListener('mouseup', () => {
		if (!dragging) return;
		dragging = false;
		handle.classList.remove('dragging');
		document.body.style.cursor = '';
		document.body.style.userSelect = '';
	});
})();

(() => {
	let resizing = false;
	let resizeTh = null;
	let startX = 0;
	let startW = 0;

	document.addEventListener('mousedown', (e) => {
		const handle = e.target.closest('.col-resize-handle');
		if (!handle) return;
		e.preventDefault();
		resizing = true;
		resizeTh = handle.parentElement;
		startX = e.clientX;
		startW = resizeTh.offsetWidth;
		handle.classList.add('dragging');
		document.body.style.cursor = 'col-resize';
		document.body.style.userSelect = 'none';
	});

	document.addEventListener('mousemove', (e) => {
		if (!resizing || !resizeTh) return;
		const newW = Math.max(50, startW + (e.clientX - startX));
		resizeTh.style.width = `${newW}px`;
		resizeTh.style.minWidth = `${newW}px`;
		resizeTh.style.maxWidth = `${newW}px`;
		const col = resizeTh.dataset.col;
		if (col) State.columnWidths[col] = newW;
	});

	document.addEventListener('mouseup', () => {
		if (!resizing) return;
		resizing = false;
		document
			.querySelectorAll('.col-resize-handle.dragging')
			.forEach((h) => h.classList.remove('dragging'));
		document.body.style.cursor = '';
		document.body.style.userSelect = '';
		resizeTh = null;
	});
})();

document.querySelectorAll('.modal-close-btn').forEach((btn) => {
	btn.addEventListener('click', () => {
		const overlay = btn.closest('.modal-overlay');
		if (overlay) {
			releaseFocus(overlay);
			closeModal(overlay.id);
		}
	});
});

(() => {
	const toggle = $('sidebar-toggle');
	const sidebar = document.querySelector('.sidebar');
	if (toggle && sidebar) {
		toggle.addEventListener('click', () => {
			sidebar.classList.toggle('open');
		});
		document.addEventListener('click', (e) => {
			if (
				sidebar.classList.contains('open') &&
				!sidebar.contains(e.target) &&
				!toggle.contains(e.target)
			) {
				sidebar.classList.remove('open');
			}
		});
	}
})();

// Connections Dashboard
function renderConnsDashboard() {
	const grid = $('conns-grid');
	if (!grid) return;
	grid.textContent = '';
	if (savedConnsMap.size === 0 && !State.dbConnected) {
		const empty = document.createElement('div');
		empty.className = 'conns-empty';
		empty.textContent = 'No saved connections yet. Click "Add New Connection" to get started.';
		grid.appendChild(empty);
		return;
	}
	for (const c of savedConnsMap.values()) {
		grid.appendChild(buildCardElement(c));
	}
}

if ($('btn-conns-add')) {
	$('btn-conns-add').addEventListener('click', () => {
		$('save-conn-name').value = '';
		$('save-conn-host').value = '';
		$('save-conn-port').value = '';
		$('save-conn-database').value = '';
		$('save-conn-username').value = '';
		$('save-conn-password').value = '';
		openModal('save-conn-overlay');
	});
}

// Settings Panel
function updateSettingsPanel() {
	const themeLabel = $('settings-theme-label');
	const roLabel = $('settings-readonly-label');
	const theme = document.documentElement.getAttribute('data-theme');
	if (themeLabel) themeLabel.textContent = theme === 'dark' ? 'Dark' : 'Light';
	if (roLabel) roLabel.textContent = State.readOnlyMode ? 'On' : 'Off';

	const themeToggle = $('settings-theme-toggle');
	const roToggle = $('settings-readonly-toggle');
	if (themeToggle) themeToggle.classList.toggle('active', theme === 'light');
	if (roToggle) roToggle.classList.toggle('active', State.readOnlyMode);

	const hostEl = $('settings-host');
	const dbEl = $('settings-db');
	const pgEl = $('settings-pg-version');
	if (hostEl) hostEl.textContent = State.dbConnected ? (State.host || 'localhost') + ':' + (State.port || '5432') : 'Not connected';
	if (dbEl) dbEl.textContent = State.database || '-';
	if (pgEl) pgEl.textContent = State.pgVersion || '-';
}

if ($('settings-theme-toggle')) {
	$('settings-theme-toggle').addEventListener('click', () => {
		const current = document.documentElement.getAttribute('data-theme');
		setTheme(current === 'dark' ? 'light' : 'dark');
		updateSettingsPanel();
	});
}

if ($('settings-readonly-toggle')) {
	$('settings-readonly-toggle').addEventListener('click', () => {
		$('btn-readonly').click();
		setTimeout(updateSettingsPanel, 100);
	});
}
