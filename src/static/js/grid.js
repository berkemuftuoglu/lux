// grid.js — Data grid rendering, cell focus, pagination, context menu, row selection, bulk ops

function displayCols() {
	return State.currentPkMode === 'ctid'
		? State.currentColumns.filter((c) => c !== 'ctid')
		: State.currentColumns;
}

function buildHeaderRow(cols) {
	const tr = h('tr');
	tr.appendChild(
		h(
			'th',
			{ cls: 'row-num-col' },
			h('input', {
				type: 'checkbox',
				cls: 'select-checkbox select-all-checkbox',
				title: 'Select all',
			}),
		),
	);

	for (const col of cols) {
		const sorted = State.sortCol === col;
		const attrs = {
			cls: sorted ? 'sorted' : '',
			'data-col': col,
		};
		const w = State.columnWidths[col];
		if (w) attrs.style = `width:${w}px;min-width:${w}px;max-width:${w}px`;

		const th = h('th', attrs, col);
		if (sorted) {
			const arrow = h('span', { cls: 'sort-arrow active' });
			// Safe: static HTML entity for arrow glyph, no user input
			arrow.innerHTML = State.sortDir === 'asc' ? '&#9650;' : '&#9660;'; // eslint-disable-line no-unsanitized/property
			th.appendChild(document.createTextNode(' '));
			th.appendChild(arrow);
		}
		th.appendChild(h('div', { cls: 'col-resize-handle' }));
		tr.appendChild(th);
	}

	tr.appendChild(h('th', { cls: 'row-actions-col' }));
	return tr;
}

function buildFilterRow(cols) {
	const tr = h('tr', { cls: 'filter-row' });
	tr.appendChild(h('th'));
	for (const col of cols) {
		const val = State.columnFilters[col] || '';
		tr.appendChild(
			h(
				'th',
				null,
				h('input', {
					cls: 'col-filter',
					'data-col': col,
					placeholder: 'Filter...',
					value: val,
				}),
			),
		);
	}
	tr.appendChild(h('th'));
	return tr;
}

function buildBodyRow(ri, row, cols, colMeta, pkSet) {
	const isSelected = State.selectedRows.has(ri);
	const tr = h('tr', { 'data-ridx': ri });
	if (isSelected) tr.className = 'selected';

	const numTd = h('td', { cls: 'row-num', 'data-ridx': ri, title: 'Click for details, Shift+Click to select' });
	const cb = h('input', { type: 'checkbox', cls: 'select-checkbox row-select-checkbox', 'data-ridx': ri });
	if (isSelected) cb.checked = true;
	numTd.appendChild(cb);
	tr.appendChild(numTd);

	for (const col of cols) {
		const dataIdx = State.currentColumns.indexOf(col);
		const val = row[dataIdx];
		const cm = colMeta[col];
		const type = cm ? cm.type || '' : '';
		const isPk = pkSet.has(col) || cm?.is_primary_key;

		let cls = 'editable';
		let content;
		let useInnerHtml = false;

		if (val === null || val === 'NULL' || val === undefined) {
			cls += ' null-val';
			content = 'NULL';
		} else if (isBoolType(type)) {
			cls += ` bool-val ${val === 't' || val === 'true' ? 't' : 'f'}`;
			content = val === 't' || val === 'true' ? 'true' : 'false';
		} else if (isNumericType(type)) {
			cls += ' num-val type-int';
			content = String(val);
		} else if (isDateType(type)) {
			cls += ' type-date';
			content = String(val);
		} else if (isJsonType(type)) {
			cls += ' type-json';
			content = String(val);
		} else if (isUuidType(type)) {
			cls += ' type-uuid';
			content = String(val);
		} else if (isInetType(type)) {
			cls += ' type-inet';
			content = String(val);
		} else if (isArrayType(type)) {
			cls += ' type-array';
			content = String(val);
		} else if (isEnumType(type, cm)) {
			cls += ' type-enum';
			const ev = escHtml(val);
			const enumCls = enumPillClass(ev);
			// Safe: ev is escaped via escHtml, enumCls is from internal enumPillClass
			content = `<span class="enum-pill ${enumCls}">${ev}</span>`;
			useInnerHtml = true;
		} else {
			content = String(val);
		}
		if (isPk) cls += ' pk-val';

		const td = h('td', {
			cls,
			'data-row': ri,
			'data-col': col,
			'data-idx': dataIdx,
			title: val == null ? '' : String(val),
		});
		if (useInnerHtml) {
			// Safe: content built from escHtml(val) and internal enumPillClass
			td.innerHTML = content; // eslint-disable-line no-unsanitized/property
		} else {
			td.textContent = content;
		}
		tr.appendChild(td);
	}

	const actionBtn = h('button', { cls: 'more-btn', 'data-row': ri, title: 'More actions' });
	// Safe: static HTML entity for vertical ellipsis
	actionBtn.innerHTML = '&#8942;'; // eslint-disable-line no-unsanitized/property
	tr.appendChild(h('td', { cls: 'row-actions' }, actionBtn));
	return tr;
}

const debouncedFilter = debounce(() => {
	State.pageOffset = 0;
	loadTableData();
}, 400);

// Event delegation — set up ONCE, not on every render
(() => {
	const body = $('grid-body');
	const head = $('grid-head');

	delegate(body, 'td.editable', 'dblclick', function () {
		startEdit(this);
	});

	delegate(body, 'td.editable', 'click', function (e) {
		if (this.classList.contains('editing')) return;
		const val = this.textContent;
		if (val && val !== 'NULL') {
			copyToClipboard(val)
				.then(() => {
					const tip = document.createElement('div');
					tip.className = 'copy-toast';
					tip.textContent = 'Copied';
					tip.style.left = `${e.clientX}px`;
					tip.style.top = `${e.clientY - 30}px`;
					document.body.appendChild(tip);
					setTimeout(() => tip.remove(), 800);
				})
				.catch(() => {
					/* clipboard not available in insecure context */
				});
		}
		State.focusRow = parseInt(this.dataset.row, 10);
		State.focusCol = Array.from(
			this.parentElement.querySelectorAll('td.editable'),
		).indexOf(this);
		updateCellFocus();
	});

	delegate(body, '.row-select-checkbox', 'change', function () {
		const ri = parseInt(this.dataset.ridx, 10);
		if (this.checked) State.selectedRows.add(ri);
		else State.selectedRows.delete(ri);
		State.lastSelectedRow = ri;
		updateRowSelection();
	});

	delegate(body, '.row-select-checkbox', 'click', (e) => {
		e.stopPropagation();
	});

	delegate(body, '.more-btn', 'click', function (e) {
		const ri = parseInt(this.dataset.row, 10);
		const cols2 = displayCols();
		const firstCol = cols2[0] || '';
		showContextMenu(e.clientX, e.clientY, ri, firstCol, this.closest('tr').querySelector('td.editable'));
	});

	delegate(body, 'td.row-num', 'click', function (e) {
		const ri = parseInt(this.dataset.ridx, 10);
		if (e.shiftKey && State.lastSelectedRow >= 0) {
			const from = Math.min(State.lastSelectedRow, ri);
			const to = Math.max(State.lastSelectedRow, ri);
			for (let i = from; i <= to; i++) State.selectedRows.add(i);
			updateRowSelection();
		} else if (e.ctrlKey || e.metaKey) {
			if (State.selectedRows.has(ri)) State.selectedRows.delete(ri);
			else State.selectedRows.add(ri);
			State.lastSelectedRow = ri;
			updateRowSelection();
		} else {
			showRowDetail(ri);
		}
	});

	body.addEventListener('contextmenu', (e) => {
		const td = e.target.closest('td.editable');
		const tr = e.target.closest('tr');
		if (!td || !tr) return;
		e.preventDefault();
		const ri = parseInt(td.dataset.row, 10);
		const col = td.dataset.col;
		showContextMenu(e.clientX, e.clientY, ri, col, td);
	});

	delegate(head, 'th[data-col]', 'click', function () {
		const col = this.dataset.col;
		if (State.sortCol === col) State.sortDir = State.sortDir === 'asc' ? 'desc' : 'asc';
		else {
			State.sortCol = col;
			State.sortDir = 'asc';
		}
		loadTableData();
	});

	delegate(head, '.col-filter', 'click', (e) => {
		e.stopPropagation();
	});

	delegate(head, '.col-filter', 'input', function () {
		State.columnFilters[this.dataset.col] = this.value;
		debouncedFilter();
	});
})();

function renderGrid() {
	const meta = getTableMeta(State.currentTable);
	const colMeta = {};
	if (meta) meta.columns.forEach((c) => (colMeta[c.name] = c));

	const pkSet = new Set(State.currentPkColumns);
	const cols = displayCols();

	const headEl = $('grid-head');
	headEl.textContent = '';
	headEl.appendChild(buildHeaderRow(cols));
	headEl.appendChild(buildFilterRow(cols));

	// Select-all checkbox
	const selectAllCb = headEl.querySelector('.select-all-checkbox');
	if (selectAllCb) {
		selectAllCb.addEventListener('change', () => {
			if (selectAllCb.checked) {
				for (let i = 0; i < State.currentRows.length; i++) State.selectedRows.add(i);
			} else {
				State.selectedRows.clear();
			}
			updateRowSelection();
		});
	}

	if (State.currentRows.length === 0) {
		$('data-grid').classList.add('hidden');
		const emptyEl = $('table-empty');
		emptyEl.style.display = '';
		// Safe: static SVG + escHtml(State.currentTable)
		emptyEl.innerHTML = // eslint-disable-line no-unsanitized/property
			'<div class="grid-empty" style="display:flex;flex-direction:column;align-items:center"><svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/></svg><div>No rows in ' +
			escHtml(State.currentTable) +
			'</div></div>';
		$('pagination').classList.add('hidden');
		return;
	}

	$('table-empty').style.display = 'none';
	$('data-grid').classList.remove('hidden');

	const bodyEl = $('grid-body');
	bodyEl.textContent = '';
	const frag = document.createDocumentFragment();
	State.currentRows.forEach((row, ri) => {
		frag.appendChild(buildBodyRow(ri, row, cols, colMeta, pkSet));
	});
	bodyEl.appendChild(frag);

	if (State.focusRow >= 0 && State.focusCol >= 0) updateCellFocus();
}

function updateCellFocus() {
	$('grid-body')
		.querySelectorAll('td.cell-focus')
		.forEach((td) => td.classList.remove('cell-focus'));
	const rows = $('grid-body').querySelectorAll('tr');
	if (State.focusRow >= 0 && State.focusRow < rows.length) {
		const cells = rows[State.focusRow].querySelectorAll('td.editable');
		if (State.focusCol >= 0 && State.focusCol < cells.length) {
			cells[State.focusCol].classList.add('cell-focus');
			cells[State.focusCol].scrollIntoView({ block: 'nearest', inline: 'nearest' });
		}
	}
}

function renderPagination() {
	if (State.totalRows === 0) {
		$('pagination').classList.add('hidden');
		return;
	}
	$('pagination').classList.remove('hidden');
	const start = State.pageOffset + 1;
	const end = Math.min(State.pageOffset + State.currentRows.length, State.totalRows);
	const fmtStart = start.toLocaleString();
	const fmtEnd = end.toLocaleString();
	const fmtTotal = State.totalRows.toLocaleString();
	$('page-info').textContent = fmtStart + ' - ' + fmtEnd + ' OF ' + fmtTotal;

	const totalPages = Math.ceil(State.totalRows / State.pageLimit);
	const atFirst = State.pageOffset === 0;
	const atLast = State.pageOffset + State.pageLimit >= State.totalRows;

	$('page-prev').disabled = atFirst;
	$('page-next').disabled = atLast;
	const pageFirst = $('page-first');
	const pageLast = $('page-last');
	if (pageFirst) pageFirst.disabled = atFirst;
	if (pageLast) pageLast.disabled = atLast;

	// Sync rows-per-page dropdown with current limit
	const rppSelect = $('rows-per-page');
	if (rppSelect) rppSelect.value = String(State.pageLimit);
}

function goToPage(page) {
	const totalPages = Math.ceil(State.totalRows / State.pageLimit);
	const clamped = Math.max(1, Math.min(page, totalPages));
	State.pageOffset = (clamped - 1) * State.pageLimit;
	loadTableData();
}

$('page-prev').onclick = () => {
	State.pageOffset = Math.max(0, State.pageOffset - State.pageLimit);
	loadTableData();
};
$('page-next').onclick = () => {
	State.pageOffset += State.pageLimit;
	loadTableData();
};

(() => {
	const pageFirst = $('page-first');
	const pageLast = $('page-last');
	if (pageFirst) pageFirst.addEventListener('click', () => goToPage(1));
	if (pageLast)
		pageLast.addEventListener('click', () => {
			const totalPages = Math.ceil(State.totalRows / State.pageLimit);
			goToPage(totalPages);
		});

	const rppSelect = $('rows-per-page');
	if (rppSelect) {
		rppSelect.addEventListener('change', () => {
			const newSize = parseInt(rppSelect.value, 10);
			if (newSize > 0) {
				State.pageLimit = newSize;
				goToPage(1);
			}
		});
	}
})();

function showContextMenu(x, y, rowIdx, colName, td) {
	const menu = $('ctx-menu');
	const val = td.textContent;
	const isNull = td.classList.contains('null-val');

	menu.textContent = '';
	const frag = document.createDocumentFragment();

	const copyItem = h('div', { cls: 'ctx-menu-item', 'data-action': 'copy' });
	// Safe: static SVG markup for copy icon
	copyItem.innerHTML = // eslint-disable-line no-unsanitized/property
		'<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>';
	copyItem.appendChild(document.createTextNode('Copy Value'));
	copyItem.appendChild(h('span', { cls: 'shortcut', text: 'Click' }));
	frag.appendChild(copyItem);

	frag.appendChild(h('div', { cls: 'ctx-menu-item', 'data-action': 'copy-row', text: 'Copy Row as JSON' }));
	frag.appendChild(h('div', { cls: 'ctx-menu-item', 'data-action': 'copy-insert', text: 'Copy as INSERT' }));
	frag.appendChild(h('div', { cls: 'ctx-menu-sep' }));

	const editItem = h('div', { cls: 'ctx-menu-item', 'data-action': 'edit', text: 'Edit Cell' });
	editItem.appendChild(h('span', { cls: 'shortcut', text: 'DblClick' }));
	frag.appendChild(editItem);

	if (!isNull) {
		frag.appendChild(h('div', { cls: 'ctx-menu-item', 'data-action': 'set-null', text: 'Set to NULL' }));
	}
	frag.appendChild(h('div', { cls: 'ctx-menu-item', 'data-action': 'detail', text: 'View Row Detail' }));
	frag.appendChild(h('div', { cls: 'ctx-menu-sep' }));
	frag.appendChild(h('div', { cls: 'ctx-menu-item', 'data-action': 'filter', text: 'Filter by this value' }));
	frag.appendChild(h('div', { cls: 'ctx-menu-sep' }));
	frag.appendChild(h('div', { cls: 'ctx-menu-item', 'data-action': 'select', text: 'Select Row' }));

	const deleteItem = h('div', { cls: 'ctx-menu-item danger', 'data-action': 'delete', text: 'Delete Row' });
	const deleteShortcut = h('span', { cls: 'shortcut' });
	// Safe: static HTML entity
	deleteShortcut.innerHTML = '&times;'; // eslint-disable-line no-unsanitized/property
	deleteItem.appendChild(deleteShortcut);
	frag.appendChild(deleteItem);

	menu.appendChild(frag);
	menu.style.left = `${Math.min(x, window.innerWidth - 220)}px`;
	menu.style.top = `${Math.min(y, window.innerHeight - 350)}px`;
	menu.classList.add('open');

	menu.querySelectorAll('.ctx-menu-item').forEach((item) => {
		item.addEventListener('click', () => {
			menu.classList.remove('open');
			const action = item.dataset.action;
			if (action === 'copy')
				copyToClipboard(val)
					.then(() => toast('Copied', 'success'))
					.catch(() => {});
			if (action === 'copy-row') copyRowAsJSON(rowIdx);
			if (action === 'copy-insert') copyRowAsInsert(rowIdx);
			if (action === 'edit') startEdit(td);
			if (action === 'set-null') setToNull(rowIdx, colName, td);
			if (action === 'detail') showRowDetail(rowIdx);
			if (action === 'filter') {
				State.columnFilters[colName] = val;
				State.pageOffset = 0;
				loadTableData();
			}
			if (action === 'select') {
				State.selectedRows.add(rowIdx);
				State.lastSelectedRow = rowIdx;
				updateRowSelection();
			}
			if (action === 'delete') deleteRow(rowIdx);
		});
	});
}

document.addEventListener('click', () =>
	$('ctx-menu').classList.remove('open'),
);

function copyRowAsJSON(rowIdx) {
	const cols = displayCols();
	const obj = {};
	cols.forEach((col) => {
		const idx = State.currentColumns.indexOf(col);
		obj[col] = State.currentRows[rowIdx][idx];
	});
	copyToClipboard(JSON.stringify(obj, null, 2))
		.then(() => toast('Row copied as JSON', 'success'))
		.catch(() => {});
}

function copyRowAsInsert(rowIdx) {
	const cols = displayCols();
	const vals = cols.map((col) => {
		const idx = State.currentColumns.indexOf(col);
		const v = State.currentRows[rowIdx][idx];
		if (v === null || v === 'NULL' || v === undefined) return 'NULL';
		return `'${String(v).replace(/'/g, "''")}'`;
	});
	const sql =
		'INSERT INTO "' +
		State.currentTable +
		'" (' +
		cols.map((c) => `"${c.replace(/"/g, '""')}"`).join(', ') +
		') VALUES (' +
		vals.join(', ') +
		');';
	copyToClipboard(sql)
		.then(() => toast('Copied as INSERT', 'success'))
		.catch(() => {});
}

function updateRowSelection() {
	$('grid-body')
		.querySelectorAll('tr')
		.forEach((tr) => {
			const ri = parseInt(tr.dataset.ridx, 10);
			const selected = State.selectedRows.has(ri);
			tr.classList.toggle('selected', selected);
			const cb = tr.querySelector('.row-select-checkbox');
			if (cb) cb.checked = selected;
		});
	updateBulkBar();
}

function updateBulkBar() {
	const bar = $('bulk-bar');
	if (State.selectedRows.size > 0) {
		bar.classList.add('show');
		$('bulk-count').textContent = `${State.selectedRows.size} selected`;
	} else {
		bar.classList.remove('show');
	}
}

$('bulk-deselect').onclick = () => {
	State.selectedRows.clear();
	State.lastSelectedRow = -1;
	updateRowSelection();
};

$('bulk-delete').onclick = async () => {
	if (State.readOnlyMode) {
		toast('Read-only mode', 'error');
		return;
	}
	if (State.selectedRows.size === 0) return;
	const ok = await confirm(
		`Delete ${State.selectedRows.size} rows`,
		'Are you sure you want to delete ' +
			State.selectedRows.size +
			' rows from "' +
			State.currentTable +
			'"?',
	);
	if (!ok) return;

	let deleted = 0;
	for (const ri of [...State.selectedRows].sort((a, b) => b - a)) {
		const pk = getPkForRow(ri);
		try {
			const data = await postJson('/api/delete-row', {
				table: State.currentTable,
				pk_column: pk.col,
				pk_value: pk.val,
				pk_mode: State.currentPkMode,
			});
			if (!data.error) deleted++;
		} catch (_e) {
			/* continue deleting remaining rows */
		}
	}
	State.selectedRows.clear();
	State.lastSelectedRow = -1;
	if (deleted > 0) toast(`Deleted ${deleted} rows`, 'success');
	else toast('Failed to delete rows', 'error');
	bumpJournal();
	loadTableData();
};

$('bulk-copy-json').onclick = () => {
	if (State.selectedRows.size === 0) return;
	const cols = displayCols();
	const objs = [...State.selectedRows]
		.sort((a, b) => a - b)
		.map((ri) => {
			const obj = {};
			cols.forEach((col) => {
				obj[col] = State.currentRows[ri][State.currentColumns.indexOf(col)];
			});
			return obj;
		});
	copyToClipboard(JSON.stringify(objs, null, 2))
		.then(() => toast(`Copied ${objs.length} rows as JSON`, 'success'))
		.catch(() => {});
};

$('bulk-copy-sql').onclick = () => {
	if (State.selectedRows.size === 0) return;
	const cols = displayCols();
	const stmts = [...State.selectedRows]
		.sort((a, b) => a - b)
		.map((ri) => {
			const vals = cols.map((col) => {
				const v = State.currentRows[ri][State.currentColumns.indexOf(col)];
				if (v === null || v === 'NULL' || v === undefined) return 'NULL';
				return `'${String(v).replace(/'/g, "''")}'`;
			});
			return (
				'INSERT INTO "' +
				State.currentTable +
				'" (' +
				cols.map((c) => `"${c.replace(/"/g, '""')}"`).join(', ') +
				') VALUES (' +
				vals.join(', ') +
				');'
			);
		});
	copyToClipboard(stmts.join('\n'))
		.then(() => toast(`Copied ${stmts.length} INSERT statements`, 'success'))
		.catch(() => {});
};

function showRowDetail(rowIdx) {
	State.detailRowIdx = rowIdx;
	const cols = displayCols();
	const meta = getTableMeta(State.currentTable);
	$('detail-title').textContent =
		`${State.currentTable} \u2014 Row ${State.pageOffset + rowIdx + 1}`;

	const grid = $('detail-grid');
	grid.textContent = '';
	const frag = document.createDocumentFragment();
	for (const col of cols) {
		const idx = State.currentColumns.indexOf(col);
		const val = State.currentRows[rowIdx][idx];
		const isNull = val === null || val === 'NULL' || val === undefined;
		const cm = meta ? meta.columns.find((c) => c.name === col) : null;
		const type = cm ? cm.type || '' : '';

		const label = h('div', { cls: 'rd-label' }, col + ' ');
		label.appendChild(
			h('span', {
				style: 'font-weight:400;text-transform:none;font-size:10px;color:var(--text-muted)',
				text: type,
			}),
		);
		frag.appendChild(label);
		frag.appendChild(
			h('div', {
				cls: 'rd-value' + (isNull ? ' null' : ''),
				text: isNull ? 'NULL' : String(val),
			}),
		);
	}
	grid.appendChild(frag);
	$('detail-overlay').classList.add('open');
	trapFocus($('detail-overlay'));
}

$('detail-copy').onclick = () => {
	const cols = displayCols();
	const rowIdx = State.detailRowIdx;
	if (rowIdx < 0 || rowIdx >= State.currentRows.length) return;
	const obj = {};
	cols.forEach((col) => {
		obj[col] = State.currentRows[rowIdx][State.currentColumns.indexOf(col)];
	});
	copyToClipboard(JSON.stringify(obj, null, 2))
		.then(() => toast('Row copied as JSON', 'success'))
		.catch(() => {});
};
$('detail-overlay').onclick = (e) => {
	if (e.target === $('detail-overlay')) {
		releaseFocus($('detail-overlay'));
		$('detail-overlay').classList.remove('open');
	}
};
