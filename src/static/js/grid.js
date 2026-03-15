// grid.js — Data grid rendering, cell focus, pagination, context menu, row selection, bulk ops

function displayCols() {
	return currentPkMode === 'ctid'
		? currentColumns.filter((c) => c !== 'ctid')
		: currentColumns;
}

function renderGrid() {
	const meta = getTableMeta(currentTable);
	const colMeta = {};
	if (meta) meta.columns.forEach((c) => (colMeta[c.name] = c));

	const pkSet = new Set(currentPkColumns);
	const cols = displayCols();

	let headHtml = '<tr><th class="row-num-col">#</th>';
	cols.forEach((col) => {
		const sorted = sortCol === col;
		const arrow = sorted
			? sortDir === 'asc'
				? ' &#9650;'
				: ' &#9660;'
			: ' <span class="sort-arrow">&#9650;</span>';
		const thStyle = columnWidths[col]
			? ' style="width:' +
				columnWidths[col] +
				'px;min-width:' +
				columnWidths[col] +
				'px;max-width:' +
				columnWidths[col] +
				'px"'
			: '';
		headHtml +=
			'<th class="' +
			(sorted ? 'sorted' : '') +
			'" data-col="' +
			escHtml(col) +
			'"' +
			thStyle +
			'>' +
			escHtml(col) +
			arrow +
			'<div class="col-resize-handle"></div></th>';
	});
	headHtml += '<th class="row-actions-col"></th></tr>';

	headHtml += '<tr class="filter-row"><th></th>';
	cols.forEach((col) => {
		const val = columnFilters[col] || '';
		headHtml +=
			'<th><input class="col-filter" data-col="' +
			escHtml(col) +
			'" placeholder="Filter..." value="' +
			escHtml(val) +
			'"></th>';
	});
	headHtml += '<th></th></tr>';
	$('grid-head').innerHTML = headHtml;

	$('grid-head')
		.querySelectorAll('th[data-col]')
		.forEach((th) => {
			th.onclick = () => {
				const col = th.dataset.col;
				if (sortCol === col) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
				else {
					sortCol = col;
					sortDir = 'asc';
				}
				loadTableData();
			};
		});

	let filterTimer = null;
	$('grid-head')
		.querySelectorAll('.col-filter')
		.forEach((input) => {
			input.onclick = (e) => e.stopPropagation();
			input.addEventListener('input', () => {
				columnFilters[input.dataset.col] = input.value;
				clearTimeout(filterTimer);
				filterTimer = setTimeout(() => {
					pageOffset = 0;
					loadTableData();
				}, 400);
			});
		});

	if (currentRows.length === 0) {
		$('data-grid').classList.add('hidden');
		$('table-empty').style.display = '';
		$('table-empty').innerHTML =
			'<div class="grid-empty" style="display:flex;flex-direction:column;align-items:center"><svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/></svg><div>No rows in ' +
			escHtml(currentTable) +
			'</div></div>';
		$('pagination').classList.add('hidden');
		return;
	}

	$('table-empty').style.display = 'none';
	$('data-grid').classList.remove('hidden');

	let bodyHtml = '';
	currentRows.forEach((row, ri) => {
		const isSelected = selectedRows.has(ri);
		bodyHtml +=
			'<tr' +
			(isSelected ? ' class="selected"' : '') +
			' data-ridx="' +
			ri +
			'">';
		bodyHtml +=
			'<td class="row-num" data-ridx="' +
			ri +
			'" title="Click for details, Shift+Click to select">' +
			(pageOffset + ri + 1) +
			'</td>';
		cols.forEach((col, _ci) => {
			const dataIdx = currentColumns.indexOf(col);
			const val = row[dataIdx];
			const cm = colMeta[col];
			const type = cm ? cm.type || '' : '';
			const isPk = pkSet.has(col) || cm?.is_primary_key;

			let cls = 'editable';
			let content;

			if (val === null || val === 'NULL' || val === undefined) {
				cls += ' null-val';
				content = 'NULL';
			} else if (isBoolType(type)) {
				cls += ` bool-val ${val === 't' || val === 'true' ? 't' : 'f'}`;
				content = val === 't' || val === 'true' ? 'true' : 'false';
			} else if (isNumericType(type)) {
				cls += ' num-val type-int';
				content = escHtml(val);
			} else if (isDateType(type)) {
				cls += ' type-date';
				content = escHtml(val);
			} else if (isJsonType(type)) {
				cls += ' type-json';
				content = escHtml(val);
			} else if (isUuidType(type)) {
				cls += ' type-uuid';
				content = escHtml(val);
			} else if (isInetType(type)) {
				cls += ' type-inet';
				content = escHtml(val);
			} else if (isArrayType(type)) {
				cls += ' type-array';
				content = escHtml(val);
			} else if (isEnumType(type, cm)) {
				cls += ' type-enum';
				const ev = escHtml(val);
				const enumCls = enumPillClass(ev);
				content = `<span class="enum-pill ${enumCls}">${ev}</span>`;
			} else {
				content = escHtml(val);
			}
			if (isPk) cls += ' pk-val';

			bodyHtml +=
				'<td class="' +
				cls +
				'" data-row="' +
				ri +
				'" data-col="' +
				escHtml(col) +
				'" data-idx="' +
				dataIdx +
				'" title="' +
				escHtml(val || '') +
				'">' +
				content +
				'</td>';
		});
		bodyHtml +=
			'<td class="row-actions"><button class="del-btn" data-row="' +
			ri +
			'" title="Delete row">&times;</button></td>';
		bodyHtml += '</tr>';
	});
	$('grid-body').innerHTML = bodyHtml;

	$('grid-body')
		.querySelectorAll('td.editable')
		.forEach((td) => {
			td.addEventListener('dblclick', () => startEdit(td));
			td.addEventListener('click', (e) => {
				if (td.classList.contains('editing')) return;
				const val = td.textContent;
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
				focusRow = parseInt(td.dataset.row, 10);
				focusCol = Array.from(
					td.parentElement.querySelectorAll('td.editable')
				).indexOf(td);
				updateCellFocus();
			});
		});

	$('grid-body')
		.querySelectorAll('.del-btn')
		.forEach((btn) => {
			btn.addEventListener('click', () =>
				deleteRow(parseInt(btn.dataset.row, 10))
			);
		});

	$('grid-body')
		.querySelectorAll('td.row-num')
		.forEach((td) => {
			td.addEventListener('click', (e) => {
				const ri = parseInt(td.dataset.ridx, 10);
				if (e.shiftKey && lastSelectedRow >= 0) {
					const from = Math.min(lastSelectedRow, ri);
					const to = Math.max(lastSelectedRow, ri);
					for (let i = from; i <= to; i++) selectedRows.add(i);
					updateRowSelection();
				} else if (e.ctrlKey || e.metaKey) {
					if (selectedRows.has(ri)) selectedRows.delete(ri);
					else selectedRows.add(ri);
					lastSelectedRow = ri;
					updateRowSelection();
				} else {
					showRowDetail(ri);
				}
			});
		});

	$('grid-body').addEventListener('contextmenu', (e) => {
		const td = e.target.closest('td.editable');
		const tr = e.target.closest('tr');
		if (!td || !tr) return;
		e.preventDefault();
		const ri = parseInt(td.dataset.row, 10);
		const col = td.dataset.col;
		showContextMenu(e.clientX, e.clientY, ri, col, td);
	});

	if (focusRow >= 0 && focusCol >= 0) updateCellFocus();
}

function updateCellFocus() {
	$('grid-body')
		.querySelectorAll('td.cell-focus')
		.forEach((td) => td.classList.remove('cell-focus'));
	const rows = $('grid-body').querySelectorAll('tr');
	if (focusRow >= 0 && focusRow < rows.length) {
		const cells = rows[focusRow].querySelectorAll('td.editable');
		if (focusCol >= 0 && focusCol < cells.length) {
			cells[focusCol].classList.add('cell-focus');
			cells[focusCol].scrollIntoView({ block: 'nearest', inline: 'nearest' });
		}
	}
}

function renderPagination() {
	if (totalRows <= pageLimit && pageOffset === 0) {
		$('pagination').classList.add('hidden');
		return;
	}
	$('pagination').classList.remove('hidden');
	const start = pageOffset + 1;
	const end = Math.min(pageOffset + currentRows.length, totalRows);
	$('page-info').textContent = `${start}\u2013${end} of ${totalRows}`;
	$('page-prev').disabled = pageOffset === 0;
	$('page-next').disabled = pageOffset + pageLimit >= totalRows;
}

$('page-prev').onclick = () => {
	pageOffset = Math.max(0, pageOffset - pageLimit);
	loadTableData();
};
$('page-next').onclick = () => {
	pageOffset += pageLimit;
	loadTableData();
};

function showContextMenu(x, y, rowIdx, colName, td) {
	const menu = $('ctx-menu');
	const val = td.textContent;
	const isNull = td.classList.contains('null-val');
	let html = '';
	html +=
		'<div class="ctx-menu-item" data-action="copy"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>Copy Value<span class="shortcut">Click</span></div>';
	html +=
		'<div class="ctx-menu-item" data-action="copy-row">Copy Row as JSON</div>';
	html +=
		'<div class="ctx-menu-item" data-action="copy-insert">Copy as INSERT</div>';
	html += '<div class="ctx-menu-sep"></div>';
	html +=
		'<div class="ctx-menu-item" data-action="edit">Edit Cell<span class="shortcut">DblClick</span></div>';
	if (!isNull) {
		html +=
			'<div class="ctx-menu-item" data-action="set-null">Set to NULL</div>';
	}
	html +=
		'<div class="ctx-menu-item" data-action="detail">View Row Detail</div>';
	html += '<div class="ctx-menu-sep"></div>';
	html +=
		'<div class="ctx-menu-item" data-action="filter">Filter by this value</div>';
	html += '<div class="ctx-menu-sep"></div>';
	html += '<div class="ctx-menu-item" data-action="select">Select Row</div>';
	html +=
		'<div class="ctx-menu-item danger" data-action="delete">Delete Row<span class="shortcut">&times;</span></div>';

	menu.innerHTML = html;
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
				columnFilters[colName] = val;
				pageOffset = 0;
				loadTableData();
			}
			if (action === 'select') {
				selectedRows.add(rowIdx);
				lastSelectedRow = rowIdx;
				updateRowSelection();
			}
			if (action === 'delete') deleteRow(rowIdx);
		});
	});
}

document.addEventListener('click', () =>
	$('ctx-menu').classList.remove('open')
);

function copyRowAsJSON(rowIdx) {
	const cols = displayCols();
	const obj = {};
	cols.forEach((col) => {
		const idx = currentColumns.indexOf(col);
		obj[col] = currentRows[rowIdx][idx];
	});
	copyToClipboard(JSON.stringify(obj, null, 2))
		.then(() => toast('Row copied as JSON', 'success'))
		.catch(() => {});
}

function copyRowAsInsert(rowIdx) {
	const cols = displayCols();
	const vals = cols.map((col) => {
		const idx = currentColumns.indexOf(col);
		const v = currentRows[rowIdx][idx];
		if (v === null || v === 'NULL' || v === undefined) return 'NULL';
		return `'${String(v).replace(/'/g, "''")}'`;
	});
	const sql =
		'INSERT INTO "' +
		currentTable +
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
			tr.classList.toggle('selected', selectedRows.has(ri));
		});
	updateBulkBar();
}

function updateBulkBar() {
	const bar = $('bulk-bar');
	if (selectedRows.size > 0) {
		bar.classList.add('show');
		$('bulk-count').textContent = `${selectedRows.size} selected`;
	} else {
		bar.classList.remove('show');
	}
}

$('bulk-deselect').onclick = () => {
	selectedRows.clear();
	lastSelectedRow = -1;
	updateRowSelection();
};

$('bulk-delete').onclick = async () => {
	if (readOnlyMode) {
		toast('Read-only mode', 'error');
		return;
	}
	if (selectedRows.size === 0) return;
	const ok = await confirm(
		`Delete ${selectedRows.size} rows`,
		'Are you sure you want to delete ' +
			selectedRows.size +
			' rows from "' +
			currentTable +
			'"?'
	);
	if (!ok) return;

	let deleted = 0;
	for (const ri of [...selectedRows].sort((a, b) => b - a)) {
		let pkCol, pkVal;
		if (currentPkMode === 'ctid') {
			pkCol = 'ctid';
			const ctidIdx = currentColumns.indexOf('ctid');
			pkVal = currentRows[ri][ctidIdx];
		} else {
			pkCol = currentPkColumns[0] || currentColumns[0];
			const pkIdx = currentColumns.indexOf(pkCol);
			pkVal = currentRows[ri][pkIdx];
		}
		try {
			const data = await fetchJson('/api/delete-row', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					table: currentTable,
					pk_column: pkCol,
					pk_value: pkVal,
					pk_mode: currentPkMode,
				}),
			});
			if (!data.error) deleted++;
		} catch (_e) {
			/* continue deleting remaining rows */
		}
	}
	selectedRows.clear();
	lastSelectedRow = -1;
	if (deleted > 0) toast(`Deleted ${deleted} rows`, 'success');
	else toast('Failed to delete rows', 'error');
	bumpJournal();
	loadTableData();
};

$('bulk-copy-json').onclick = () => {
	if (selectedRows.size === 0) return;
	const cols = displayCols();
	const objs = [...selectedRows]
		.sort((a, b) => a - b)
		.map((ri) => {
			const obj = {};
			cols.forEach((col) => {
				obj[col] = currentRows[ri][currentColumns.indexOf(col)];
			});
			return obj;
		});
	copyToClipboard(JSON.stringify(objs, null, 2))
		.then(() => toast(`Copied ${objs.length} rows as JSON`, 'success'))
		.catch(() => {});
};

$('bulk-copy-sql').onclick = () => {
	if (selectedRows.size === 0) return;
	const cols = displayCols();
	const stmts = [...selectedRows]
		.sort((a, b) => a - b)
		.map((ri) => {
			const vals = cols.map((col) => {
				const v = currentRows[ri][currentColumns.indexOf(col)];
				if (v === null || v === 'NULL' || v === undefined) return 'NULL';
				return `'${String(v).replace(/'/g, "''")}'`;
			});
			return (
				'INSERT INTO "' +
				currentTable +
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
	const cols = displayCols();
	const meta = getTableMeta(currentTable);
	$('detail-title').textContent =
		`${currentTable} \u2014 Row ${pageOffset + rowIdx + 1}`;

	let html = '';
	cols.forEach((col) => {
		const idx = currentColumns.indexOf(col);
		const val = currentRows[rowIdx][idx];
		const isNull = val === null || val === 'NULL' || val === undefined;
		const cm = meta ? meta.columns.find((c) => c.name === col) : null;
		const type = cm ? cm.type || '' : '';
		html +=
			'<div class="rd-label">' +
			escHtml(col) +
			' <span style="font-weight:400;text-transform:none;font-size:10px;color:var(--text-muted)">' +
			escHtml(type) +
			'</span></div>';
		html +=
			'<div class="rd-value' +
			(isNull ? ' null' : '') +
			'">' +
			(isNull ? 'NULL' : escHtml(String(val))) +
			'</div>';
	});
	$('detail-grid').innerHTML = html;
	$('detail-overlay').classList.add('open');
	trapFocus($('detail-overlay'));
}

$('detail-copy').onclick = () => {
	const cols = displayCols();
	const titleText = $('detail-title').textContent;
	const match = titleText.match(/Row (\d+)/);
	if (!match) return;
	const rowIdx = parseInt(match[1], 10) - 1 - pageOffset;
	if (rowIdx < 0 || rowIdx >= currentRows.length) return;
	const obj = {};
	cols.forEach((col) => {
		obj[col] = currentRows[rowIdx][currentColumns.indexOf(col)];
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
