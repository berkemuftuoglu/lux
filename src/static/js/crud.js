// crud.js — Cell editing, FK autocomplete, row insert/update/delete, read-only toggle, journal

let fkDropdown = null;
let fkItems = [];
let fkIndex = -1;
let fkSelecting = false;

function getColumnMeta(tableName, colName) {
	if (!State.schemaData || !State.schemaData.tables) return null;
	const tbl = State.schemaData.tables.find((t) => t.name === tableName);
	if (!tbl) return null;
	return tbl.columns.find((c) => c.name === colName) || null;
}

function isFkColumn(tableName, colName) {
	const meta = getColumnMeta(tableName, colName);
	return meta?.fk_target_table;
}

async function showFkDropdown(input, tableName, colName) {
	const meta = getColumnMeta(tableName, colName);
	if (!meta || !meta.fk_target_table) return;

	const search = input.value.trim();
	try {
		const url =
			'/api/tables/' +
			encodeURIComponent(tableName) +
			'/fk-lookup?column=' +
			encodeURIComponent(colName) +
			'&search=' +
			encodeURIComponent(search) +
			'&limit=10';
		const data = await fetchJson(url);
		if (!data.values || data.values.length === 0) {
			hideFkDropdown();
			return;
		}

		fkItems = data.values;
		fkIndex = -1;

		if (fkDropdown) fkDropdown.remove();
		fkDropdown = document.createElement('div');
		fkDropdown.className = 'fk-dropdown';

		const rect = input.getBoundingClientRect();
		fkDropdown.style.position = 'fixed';
		fkDropdown.style.top = `${rect.bottom + 2}px`;
		fkDropdown.style.left = `${rect.left}px`;
		fkDropdown.style.minWidth = `${rect.width}px`;

		fkItems.forEach((val, _i) => {
			const item = document.createElement('div');
			item.className = 'fk-item';
			item.textContent = val;
			item.addEventListener('mousedown', (e) => {
				e.preventDefault();
				fkSelecting = true;
				input.value = val;
				hideFkDropdown();
			});
			fkDropdown.appendChild(item);
		});

		document.body.appendChild(fkDropdown);
	} catch (_e) {
		hideFkDropdown();
	}
}

function hideFkDropdown() {
	if (fkDropdown) {
		fkDropdown.remove();
		fkDropdown = null;
	}
	fkItems = [];
	fkIndex = -1;
}

function startEdit(td) {
	if (State.readOnlyMode) {
		toast('Read-only mode', 'error');
		return;
	}
	if (State.editingCell) cancelEdit();

	const row = parseInt(td.dataset.row, 10);
	const col = td.dataset.col;
	const dataIdx = parseInt(td.dataset.idx, 10);
	const val = State.currentRows[row][dataIdx];
	const isNull = val === null || val === 'NULL' || val === undefined;

	State.editingCell = { td, row, col, dataIdx, originalVal: val };
	const hasFk = isFkColumn(State.currentTable, col);

	td.classList.add('editing');
	const input = document.createElement('input');
	input.value = isNull ? '' : val;
	input.placeholder = 'NULL';
	td.innerHTML = '';
	td.appendChild(input);
	input.focus();
	input.select();

	if (hasFk) {
		showFkDropdown(input, State.currentTable, col);
		input.addEventListener('input', () =>
			showFkDropdown(input, State.currentTable, col)
		);
	}

	input.addEventListener('keydown', (e) => {
		if (hasFk && fkDropdown && fkItems.length > 0) {
			if (e.key === 'ArrowDown') {
				e.preventDefault();
				fkIndex = Math.min(fkIndex + 1, fkItems.length - 1);
				updateFkSelection();
				return;
			}
			if (e.key === 'ArrowUp') {
				e.preventDefault();
				fkIndex = Math.max(fkIndex - 1, 0);
				updateFkSelection();
				return;
			}
			if (e.key === 'Tab' && fkIndex >= 0) {
				e.preventDefault();
				input.value = fkItems[fkIndex];
				hideFkDropdown();
				return;
			}
		}
		if (e.key === 'Enter') {
			hideFkDropdown();
			saveEdit();
		}
		if (e.key === 'Escape') {
			hideFkDropdown();
			cancelEdit();
		}
	});
	input.addEventListener('blur', () => {
		setTimeout(() => {
			if (!fkSelecting) {
				hideFkDropdown();
				saveEdit();
			}
			fkSelecting = false;
		}, 200);
	});
}

function updateFkSelection() {
	if (!fkDropdown) return;
	fkDropdown.querySelectorAll('.fk-item').forEach((el, i) => {
		el.classList.toggle('selected', i === fkIndex);
	});
}

function cancelEdit() {
	if (!State.editingCell) return;
	const { td, row, dataIdx } = State.editingCell;
	const val = State.currentRows[row][dataIdx];
	td.classList.remove('editing');
	if (val === null || val === 'NULL' || val === undefined) {
		td.className = 'editable null-val';
		td.textContent = 'NULL';
	} else {
		td.className = 'editable';
		td.textContent = val;
	}
	State.editingCell = null;
}

async function saveEdit() {
	if (!State.editingCell) return;
	const { td, row, col, dataIdx, originalVal } = State.editingCell;
	const input = td.querySelector('input');
	if (!input) {
		cancelEdit();
		return;
	}
	const newVal = input.value;

	if (originalVal !== null && newVal === originalVal) {
		cancelEdit();
		return;
	}

	let pkCol, pkVal;
	if (State.currentPkMode === 'ctid') {
		pkCol = 'ctid';
		const ctidIdx = State.currentColumns.indexOf('ctid');
		pkVal = State.currentRows[row][ctidIdx];
	} else {
		pkCol = State.currentPkColumns[0] || State.currentColumns[0];
		const pkIdx = State.currentColumns.indexOf(pkCol);
		pkVal = State.currentRows[row][pkIdx];
	}

	State.editingCell = null;
	td.classList.remove('editing');

	try {
		const data = await fetchJson('/api/update', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				table: State.currentTable,
				column: col,
				value: newVal,
				pk_column: pkCol,
				pk_value: pkVal,
				pk_mode: State.currentPkMode,
				old_value: originalVal || '',
			}),
		});
		if (data.error) {
			toast(data.error, 'error');
			cancelEdit();
			loadTableData();
			return;
		}

		State.currentRows[row][dataIdx] = newVal;
		td.textContent = newVal;
		td.className = 'editable';
		td.classList.add('saved-flash');
		setTimeout(() => td.classList.remove('saved-flash'), 500);
		toast('Updated', 'success');
		bumpJournal();
	} catch (e) {
		toast(`Update failed: ${e.message}`, 'error');
		loadTableData();
	}
}

async function deleteRow(rowIdx) {
	if (State.readOnlyMode) {
		toast('Read-only mode', 'error');
		return;
	}

	let pkCol, pkVal;
	if (State.currentPkMode === 'ctid') {
		pkCol = 'ctid';
		const ctidIdx = State.currentColumns.indexOf('ctid');
		pkVal = State.currentRows[rowIdx][ctidIdx];
	} else {
		pkCol = State.currentPkColumns[0] || State.currentColumns[0];
		const pkIdx = State.currentColumns.indexOf(pkCol);
		pkVal = State.currentRows[rowIdx][pkIdx];
	}

	const ok = await confirm(
		'Delete Row',
		'DELETE FROM "' +
			State.currentTable +
			'" WHERE "' +
			pkCol +
			'" = \'' +
			pkVal +
			"'"
	);
	if (!ok) return;

	try {
		const data = await fetchJson('/api/delete-row', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				table: State.currentTable,
				pk_column: pkCol,
				pk_value: pkVal,
				pk_mode: State.currentPkMode,
			}),
		});
		if (data.error) toast(data.error, 'error');
		else {
			toast(`Row deleted from ${State.currentTable}`, 'success');
			bumpJournal();
			loadTableData();
		}
	} catch (e) {
		toast(`Delete failed: ${e.message}`, 'error');
	}
}

async function submitInsert() {
	const meta = getTableMeta(State.currentTable);
	if (!meta) return;
	const values = {};
	let hasValues = false;

	meta.columns.forEach((col) => {
		const el = $('insert-fields').querySelector(`[data-column="${col.name}"]`);
		if (!el) return;
		if (el.tagName === 'SELECT') {
			if (el.value !== '__DEFAULT__') {
				values[col.name] = el.value;
				hasValues = true;
			}
			return;
		}
		const val = el.value.trim();
		if (val === '' && el.dataset.autoGen) return;
		if (val === '' && (col.is_nullable || col.column_default)) return;
		if (val === '') return;
		values[col.name] = val;
		hasValues = true;
	});

	releaseFocus($('insert-overlay'));
	$('insert-overlay').classList.remove('open');
	try {
		const payload = { table: State.currentTable };
		if (hasValues) payload.values = values;
		const data = await fetchJson('/api/insert-row', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(payload),
		});
		if (data.error) toast(data.error, 'error');
		else {
			toast(`Row inserted into ${State.currentTable}`, 'success');
			bumpJournal();
			loadTableData();
		}
	} catch (e) {
		toast(`Insert failed: ${e.message}`, 'error');
	}
}

async function setToNull(rowIdx, colName, td) {
	if (State.readOnlyMode) {
		toast('Read-only mode', 'error');
		return;
	}
	let pkCol, pkVal;
	if (State.currentPkMode === 'ctid') {
		pkCol = 'ctid';
		const ctidIdx = State.currentColumns.indexOf('ctid');
		pkVal = State.currentRows[rowIdx][ctidIdx];
	} else {
		pkCol = State.currentPkColumns[0] || State.currentColumns[0];
		const pkIdx = State.currentColumns.indexOf(pkCol);
		pkVal = State.currentRows[rowIdx][pkIdx];
	}
	const dataIdx = State.currentColumns.indexOf(colName);
	const oldVal = State.currentRows[rowIdx][dataIdx];

	try {
		const data = await fetchJson('/api/update', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				table: State.currentTable,
				column: colName,
				value: '__NULL__',
				pk_column: pkCol,
				pk_value: pkVal,
				pk_mode: State.currentPkMode,
				old_value: oldVal || '',
			}),
		});
		if (data.error) toast(data.error, 'error');
		else {
			State.currentRows[rowIdx][dataIdx] = null;
			td.className = 'editable null-val';
			td.textContent = 'NULL';
			td.classList.add('saved-flash');
			setTimeout(() => td.classList.remove('saved-flash'), 500);
			toast('Set to NULL', 'success');
			bumpJournal();
		}
	} catch (e) {
		toast(`Update failed: ${e.message}`, 'error');
	}
}

$('btn-readonly').onclick = async () => {
	try {
		const data = await fetchJson('/api/settings/read-only', { method: 'POST' });
		State.readOnlyMode = data.read_only;
		updateReadOnlyUI();
		toast(State.readOnlyMode ? 'Read-only enabled' : 'Read-only disabled', 'info');
	} catch (_e) {
		toast('Failed to toggle', 'error');
	}
};

async function loadReadOnly() {
	try {
		const data = await fetchJson('/api/settings/read-only');
		State.readOnlyMode = data.read_only;
		updateReadOnlyUI();
	} catch (_e) {
		/* non-critical: read-only status unknown until connected */
	}
}

function updateReadOnlyUI() {
	$('btn-readonly').classList.toggle('active', State.readOnlyMode);
	$('status-ro').classList.toggle('show', State.readOnlyMode);
}

function bumpJournal() {
	State.journalCount++;
	const badge = $('journal-badge');
	badge.textContent = State.journalCount;
	badge.classList.add('show');
}
function clearJournalBadge() {
	State.journalCount = 0;
	$('journal-badge').classList.remove('show');
}

async function loadJournal() {
	try {
		const data = await fetchJson('/api/journal');
		const entries = data.entries || [];
		const list = $('journal-list');

		if (entries.length === 0) {
			$('journal-empty').style.display = '';
			list.querySelectorAll('.journal-entry').forEach((e) => e.remove());
			return;
		}
		$('journal-empty').style.display = 'none';

		let html = '';
		entries.reverse().forEach((entry) => {
			const opClass = entry.operation || 'update';
			html +=
				'<div class="journal-entry' +
				(entry.undone ? ' undone' : '') +
				'" data-id="' +
				entry.id +
				'">';
			html += `<span class="op ${opClass}">${escHtml(opClass)}</span>`;
			html += '<div class="details">';
			html += `<span class="tbl">${escHtml(entry.table || '')}</span>`;
			if (
				entry.operation === 'delete' &&
				entry.old_value &&
				entry.old_value.startsWith('{')
			) {
				try {
					const rowData = JSON.parse(entry.old_value);
					const keys = Object.keys(rowData).slice(0, 4);
					html += '<div class="journal-deleted-row">';
					keys.forEach((k) => {
						const v = rowData[k];
						html +=
							'<span class="journal-kv"><span class="col">' +
							escHtml(k) +
							'</span>=<span class="val old">' +
							escHtml(String(v)) +
							'</span></span> ';
					});
					if (Object.keys(rowData).length > 4)
						html +=
							'<span class="val" style="opacity:0.5">+' +
							(Object.keys(rowData).length - 4) +
							' more</span>';
					html += '</div>';
				} catch (_e) {
					if (entry.pk_column && entry.pk_value) {
						html +=
							' <span class="val">(' +
							escHtml(entry.pk_column) +
							'=' +
							escHtml(entry.pk_value) +
							')</span>';
					}
				}
			} else if (entry.column) {
				html += ` <span class="col">${escHtml(entry.column)}</span>`;
				if (entry.old_value)
					html += ` <span class="val old">${escHtml(entry.old_value)}</span>`;
				if (entry.new_value)
					html +=
						' &rarr; <span class="val new">' +
						escHtml(entry.new_value) +
						'</span>';
			}
			if (
				!(
					entry.operation === 'delete' &&
					entry.old_value &&
					entry.old_value.startsWith('{')
				) &&
				entry.pk_column &&
				entry.pk_value
			) {
				html +=
					' <span class="val">(' +
					escHtml(entry.pk_column) +
					'=' +
					escHtml(entry.pk_value) +
					')</span>';
			}
			if (entry.timestamp) {
				const d = new Date(entry.timestamp * 1000);
				html += `<span class="ts">${d.toLocaleTimeString()}</span>`;
			}
			html += '</div>';
			if (!entry.undone && entry.operation === 'update') {
				html += `<button class="undo-btn" data-id="${entry.id}">Undo</button>`;
			}
			html += '</div>';
		});

		list.querySelectorAll('.journal-entry').forEach((e) => e.remove());
		list.insertAdjacentHTML('beforeend', html);

		list.querySelectorAll('.undo-btn').forEach((btn) => {
			btn.onclick = async () => {
				try {
					const data = await fetchJson('/api/journal/undo', {
						method: 'POST',
						headers: { 'Content-Type': 'application/json' },
						body: JSON.stringify({ id: parseInt(btn.dataset.id, 10) }),
					});
					if (data.error) toast(data.error, 'error');
					else {
						toast('Change undone', 'success');
						loadJournal();
						if (State.currentTable) loadTableData();
					}
				} catch (_e) {
					toast('Undo failed', 'error');
				}
			};
		});

		list.querySelectorAll('.journal-entry').forEach((entry) => {
			const opEl = entry.querySelector('.op.delete');
			if (opEl) {
				const details = entry.querySelector('.details');
				if (details && !details.querySelector('.col')) {
					const valEl = details.querySelector('.val');
					if (valEl) {
						valEl.style.color = 'var(--error)';
						valEl.style.fontWeight = '500';
					}
				}
			}
		});
	} catch (_e) {
		toast('Failed to load journal', 'error');
	}
}

$('btn-journal-refresh').onclick = loadJournal;
