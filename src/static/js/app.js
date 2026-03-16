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

$$('.tab').forEach((tab) => {
	tab.addEventListener('click', () => {
		$$('.tab').forEach((t) => {
			t.classList.remove('active');
			t.setAttribute('aria-selected', 'false');
		});
		$$('.tab-panel').forEach((p) => p.classList.remove('active'));
		tab.classList.add('active');
		tab.setAttribute('aria-selected', 'true');
		$(`panel-${tab.dataset.tab}`).classList.add('active');
		if (tab.dataset.tab === 'journal') {
			loadJournal();
			clearJournalBadge();
		}
		if (tab.dataset.tab === 'er') drawER();
	});
});

$('conn-btn').onclick = doConnect;
$('conn-input').addEventListener('keydown', (e) => {
	if (e.key === 'Enter') doConnect();
});

$('btn-export-csv').onclick = () => {
	if (State.currentTable)
		window.location.href = `/api/export/${encodeURIComponent(State.currentTable)}?format=csv`;
};
$('btn-export-json').onclick = () => {
	if (State.currentTable)
		window.location.href = `/api/export/${encodeURIComponent(State.currentTable)}?format=json`;
};

$('btn-refresh').onclick = async () => {
	if (State.dbConnected) {
		await loadSchema();
		toast('Schema refreshed', 'success');
	}
};
$('btn-tbl-refresh').onclick = () => loadTableData();

$('btn-saved-queries').onclick = () => {
	renderSavedQueries();
	$('saved-overlay').classList.add('open');
	trapFocus($('saved-overlay'));
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

document.addEventListener('keydown', (e) => {
	if ((e.ctrlKey || e.metaKey) && e.key === 'l') {
		e.preventDefault();
		$$('.tab').forEach((t) => t.classList.remove('active'));
		$$('.tab-panel').forEach((p) => p.classList.remove('active'));
		document.querySelector('[data-tab="sql"]').classList.add('active');
		$('panel-sql').classList.add('active');
		sqlEditor.focus();
	}
	if (
		(e.ctrlKey || e.metaKey) &&
		e.key === 'f' &&
		State.currentTable &&
		State.dbConnected
	) {
		if ($('panel-tables').classList.contains('active')) {
			e.preventDefault();
			$('btn-find-replace').click();
		}
	}
	// Close modals in priority order (topmost first)
	if (e.key === 'Escape') {
		const modals = [
			'cmd-overlay',
			'prompt-overlay',
			'confirm-overlay',
			'detail-overlay',
			'ddl-overlay',
			'shortcuts-overlay',
			'history-overlay',
			'saved-overlay',
			'save-conn-overlay',
			'create-table-overlay',
			'import-overlay',
			'fnr-overlay',
			'insert-overlay',
		];
		for (const id of modals) {
			const el = $(id);
			if (
				el &&
				(el.classList.contains('open') || el.classList.contains('visible'))
			) {
				if (id === 'prompt-overlay' && promptResolve) {
					promptResolve(null);
				}
				if (id === 'confirm-overlay' && confirmResolve) {
					confirmResolve(false);
				}
				releaseFocus(el);
				closeModal(id);
				el.classList.remove('visible');
				e.preventDefault();
				return; // Only close the topmost one
			}
		}
		$('ctx-menu').classList.remove('open');
		const tip = document.querySelector('.stats-tip');
		if (tip) {
			tip.remove();
			return;
		}
		if (State.editingCell) {
			cancelEdit();
			return;
		}
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
	if (
		e.key === '?' &&
		!State.editingCell &&
		document.activeElement.tagName !== 'INPUT' &&
		document.activeElement.tagName !== 'TEXTAREA' &&
		document.activeElement.tagName !== 'SELECT'
	) {
		e.preventDefault();
		$('shortcuts-overlay').classList.add('open');
	}
	if ((e.ctrlKey || e.metaKey) && e.key === 'n') {
		e.preventDefault();
		addSqlTab();
		$$('.tab').forEach((t) => t.classList.remove('active'));
		$$('.tab-panel').forEach((p) => p.classList.remove('active'));
		document.querySelector('[data-tab="sql"]').classList.add('active');
		$('panel-sql').classList.add('active');
	}
	if ((e.ctrlKey || e.metaKey) && e.key === 's') {
		e.preventDefault();
		if (State.dbConnected) $('save-conn-btn').click();
	}
	if (
		!State.editingCell &&
		$('panel-tables').classList.contains('active') &&
		State.currentRows.length > 0 &&
		document.activeElement.tagName !== 'INPUT' &&
		document.activeElement.tagName !== 'TEXTAREA'
	) {
		const dCols = displayCols();
		const maxRow = State.currentRows.length - 1;
		const maxCol = dCols.length - 1;
		if (e.key === 'ArrowDown') {
			e.preventDefault();
			State.focusRow = Math.min(State.focusRow + 1, maxRow);
			if (State.focusCol < 0) State.focusCol = 0;
			updateCellFocus();
		}
		if (e.key === 'ArrowUp') {
			e.preventDefault();
			State.focusRow = Math.max(State.focusRow - 1, 0);
			if (State.focusCol < 0) State.focusCol = 0;
			updateCellFocus();
		}
		if (e.key === 'ArrowRight') {
			e.preventDefault();
			State.focusCol = Math.min(State.focusCol + 1, maxCol);
			if (State.focusRow < 0) State.focusRow = 0;
			updateCellFocus();
		}
		if (e.key === 'ArrowLeft') {
			e.preventDefault();
			State.focusCol = Math.max(State.focusCol - 1, 0);
			if (State.focusRow < 0) State.focusRow = 0;
			updateCellFocus();
		}
		if (e.key === 'Tab' && State.focusRow >= 0) {
			e.preventDefault();
			if (e.shiftKey) State.focusCol = Math.max(State.focusCol - 1, 0);
			else State.focusCol = Math.min(State.focusCol + 1, maxCol);
			updateCellFocus();
		}
		if (e.key === 'Enter' && State.focusRow >= 0 && State.focusCol >= 0) {
			e.preventDefault();
			const rows = $('grid-body').querySelectorAll('tr');
			if (State.focusRow < rows.length) {
				const cells = rows[State.focusRow].querySelectorAll('td.editable');
				if (State.focusCol < cells.length) startEdit(cells[State.focusCol]);
			}
		}
	}
});

// Deferred until DOMContentLoaded so all modules are fully parsed
async function init() {
	updateDestructiveButtons(false);
	await loadReadOnly();
	try {
		const data = await fetchJson('/api/schema');
		if (data.tables && data.tables.length > 0) {
			State.schemaData = data;
			State.dbConnected = true;
			updateConnUI();
			showConnStatus(true, `${data.tables.length} tables`);
			renderSidebar();
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
