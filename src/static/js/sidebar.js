// sidebar.js — Schema tree, table selection, data loading, type helpers, ER diagram

async function loadSchema() {
	try {
		State.schemaData = await fetchJson('/api/schema');
		document.dispatchEvent(new CustomEvent(LuxEvents.SCHEMA_LOADED));
	} catch (_e) {
		toast('Failed to load schema', 'error');
	}
}

function renderSidebar() {
	const tree = $('schema-tree');
	$('sidebar-empty').style.display = 'none';
	tree
		.querySelectorAll('.tree-table, .tree-columns')
		.forEach((el) => el.remove());

	if (!State.schemaData || !State.schemaData.tables || State.schemaData.tables.length === 0) {
		$('sidebar-empty').style.display = '';
		$('table-search-wrap').style.display = 'none';
		$('table-count').textContent = '';
		return;
	}

	$('table-search-wrap').style.display = '';
	$('table-count').textContent = `(${State.schemaData.tables.length})`;
	const searchTerm = ($('table-search').value || '').toLowerCase();

	function makeTableSvg() {
		const ns = 'http://www.w3.org/2000/svg';
		const svg = document.createElementNS(ns, 'svg');
		svg.setAttribute('viewBox', '0 0 24 24');
		svg.setAttribute('fill', 'none');
		svg.setAttribute('stroke', 'currentColor');
		svg.setAttribute('stroke-width', '1.5');
		const rect = document.createElementNS(ns, 'rect');
		rect.setAttribute('x', '3'); rect.setAttribute('y', '3');
		rect.setAttribute('width', '18'); rect.setAttribute('height', '18');
		rect.setAttribute('rx', '2');
		const line1 = document.createElementNS(ns, 'line');
		line1.setAttribute('x1', '3'); line1.setAttribute('y1', '9');
		line1.setAttribute('x2', '21'); line1.setAttribute('y2', '9');
		const line2 = document.createElementNS(ns, 'line');
		line2.setAttribute('x1', '9'); line2.setAttribute('y1', '3');
		line2.setAttribute('x2', '9'); line2.setAttribute('y2', '21');
		svg.append(rect, line1, line2);
		return svg;
	}

	State.schemaData.tables.forEach((table) => {
		const colCount = table.columns ? table.columns.length : 0;
		const item = h('div', { cls: 'tree-table', 'data-table': table.name },
			h('span', { cls: 'chevron', text: '\u25B6' }),
			makeTableSvg(),
			h('span', { cls: 'name', text: prettyName(table.name) }),
			h('span', { cls: 'count', text: String(colCount) })
		);

		const cols = document.createElement('div');
		cols.className = 'tree-columns';
		(table.columns || []).forEach((col) => {
			const c = document.createElement('div');
			c.className = 'tree-col';

			const nameSpan = document.createElement('span');
			nameSpan.className = 'col-name';
			nameSpan.textContent = prettyName(col.name);
			c.appendChild(nameSpan);

			if (col.is_primary_key) {
				const pk = document.createElement('span');
				pk.className = 'constraint-badge pk';
				pk.textContent = 'PK';
				c.appendChild(pk);
			}
			if (col.fk_target_table) {
				const fk = document.createElement('span');
				fk.className = 'constraint-badge fk';
				fk.textContent = 'FK';
				c.appendChild(fk);
			}
			if (col.is_nullable === false) {
				const nn = document.createElement('span');
				nn.className = 'constraint-badge notnull';
				nn.textContent = 'NOT_NULL';
				c.appendChild(nn);
			}
			if (col.is_indexed) {
				const idx = document.createElement('span');
				idx.className = 'constraint-badge indexed';
				idx.textContent = 'INDEXED';
				c.appendChild(idx);
			}
			if (col.has_check) {
				const chk = document.createElement('span');
				chk.className = 'constraint-badge check-constraint';
				chk.textContent = 'CHECK';
				c.appendChild(chk);
			}
			if (col.has_default) {
				const def = document.createElement('span');
				def.className = 'constraint-badge default-val';
				def.textContent = 'DEFAULT';
				c.appendChild(def);
			}

			const typeSpan = document.createElement('span');
			typeSpan.className = 'col-type-chip';
			typeSpan.textContent = (col.type || '').toUpperCase();
			c.appendChild(typeSpan);

			cols.appendChild(c);
		});

		if (searchTerm && !table.name.toLowerCase().includes(searchTerm)) {
			item.style.display = 'none';
			cols.classList.add('search-hidden');
		}

		item.addEventListener('click', (e) => {
			e.stopPropagation();
			const wasActive = item.classList.contains('active');
			item.classList.toggle('expanded');
			cols.classList.toggle('open');
			if (!wasActive) {
				tree
					.querySelectorAll('.tree-table')
					.forEach((t) => t.classList.remove('active'));
				item.classList.add('active');
				selectTable(table.name);
			}
		});

		tree.appendChild(item);
		tree.appendChild(cols);
	});
}

$('table-search').addEventListener('input', () => {
	const term = $('table-search').value.toLowerCase();
	$('schema-tree')
		.querySelectorAll('.tree-table')
		.forEach((el) => {
			const name = (el.dataset.table || '').toLowerCase();
			const match = !term || name.includes(term);
			el.style.display = match ? '' : 'none';
			if (el.nextElementSibling?.classList.contains('tree-columns')) {
				el.nextElementSibling.classList.toggle('search-hidden', !match);
			}
		});
});

function updateDestructiveButtons(enabled) {
	const ids = [
		'btn-truncate',
		'btn-import-csv',
		'btn-export-csv',
		'btn-export-json',
		'btn-overflow',
	];
	ids.forEach((id) => {
		const el = $(id);
		if (el) el.disabled = !enabled;
	});
}

function selectTable(name) {
	resetTableView();
	$('table-toolbar').style.display = 'flex';
	$('tbl-name').textContent = name;
	updateDestructiveButtons(true);
	setCurrentTable(name);
}

async function loadTableData() {
	if (!State.currentTable) return;
	let url =
		'/api/tables/' +
		encodeURIComponent(State.currentTable) +
		'/data?limit=' +
		State.pageLimit +
		'&offset=' +
		State.pageOffset +
		'&count=exact';
	if (State.sortCol) url += `&sort=${encodeURIComponent(State.sortCol)}&dir=${State.sortDir}`;
	Object.keys(State.columnFilters).forEach((col) => {
		if (State.columnFilters[col])
			url +=
				'&f.' +
				encodeURIComponent(col) +
				'=' +
				encodeURIComponent(State.columnFilters[col]);
	});

	showLoading('grid-wrap');
	try {
		const data = await fetchJsonAbortable(url);
		hideLoading('grid-wrap');
		if (data.error) {
			toast(data.error, 'error');
			return;
		}

		State.currentColumns = data.columns || [];
		State.currentRows = data.rows || [];
		State.totalRows = data.total || 0;
		State.currentPkMode = data.pk_mode || 'column';
		State.currentPkColumns = data.pk_columns || [];

		$('tbl-info').textContent = `${State.totalRows.toLocaleString()} rows`;
		$('status-table').textContent =
			`${State.currentTable} \u00b7 ${State.totalRows.toLocaleString()} rows`;

		renderGrid();
		renderPagination();
		updateBulkBar();
	} catch (e) {
		hideLoading('grid-wrap');
		if (e.name === 'AbortError') return;
		toast(`Failed to load data: ${e.message}`, 'error');
	}
}

function getTableMeta(name) {
	if (!State.schemaData || !State.schemaData.tables) return null;
	return State.schemaData.tables.find((t) => t.name === name) || null;
}

function isNumericType(type) {
	if (!type) return false;
	const t = type.toLowerCase();
	return (
		t.includes('int') ||
		t.includes('numeric') ||
		t.includes('decimal') ||
		t.includes('float') ||
		t.includes('double') ||
		t.includes('real') ||
		t === 'money' ||
		t === 'serial' ||
		t === 'bigserial'
	);
}

function isBoolType(type) {
	return type && type.toLowerCase() === 'boolean';
}
function isDateType(type) {
	if (!type) return false;
	const t = type.toLowerCase();
	return (
		t.includes('date') ||
		t.includes('timestamp') ||
		t.includes('time') ||
		t.includes('interval')
	);
}
function isJsonType(type) {
	if (!type) return false;
	const t = type.toLowerCase();
	return t === 'json' || t === 'jsonb';
}
function isUuidType(type) {
	return type && type.toLowerCase() === 'uuid';
}
function isInetType(type) {
	if (!type) return false;
	const t = type.toLowerCase();
	return t === 'inet' || t === 'cidr' || t === 'macaddr';
}
function isArrayType(type) {
	if (!type) return false;
	return type.includes('[]') || type.toLowerCase().startsWith('array');
}
function isEnumType(type, colMeta) {
	if (!type) return false;
	if (colMeta?.enum_values && colMeta.enum_values.length > 0) return true;
	return type.toLowerCase() === 'user-defined';
}
function enumPillClass(val) {
	const v = val.toLowerCase();
	if (/done|complete|closed|resolved|active|enabled/.test(v))
		return 'pill-success';
	if (/in.progress|in_progress|open|started|running|pending/.test(v))
		return 'pill-accent';
	if (/cancel|blocked|failed|disabled|rejected|archived/.test(v))
		return 'pill-error';
	if (/urgent|critical/.test(v)) return 'pill-error';
	if (/high/.test(v)) return 'pill-warning';
	if (/medium/.test(v)) return 'pill-accent';
	if (/low|none|trivial/.test(v)) return 'pill-muted';
	if (/todo|backlog|draft|review|planned/.test(v)) return 'pill-subtle';
	return 'pill-default';
}

