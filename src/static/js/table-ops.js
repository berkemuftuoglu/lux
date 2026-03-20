// table-ops.js — Insert row, find/replace, DDL viewer, table stats, truncate, CSV import

$('btn-add-row').onclick = () => {
	if (!State.currentTable || !State.dbConnected) return;
	const meta = getTableMeta(State.currentTable);
	if (!meta || !meta.columns) {
		toast('No column info', 'error');
		return;
	}

	$('insert-tbl').textContent = State.currentTable;
	const container = $('insert-fields');
	container.innerHTML = '';

	meta.columns.forEach((col) => {
		const autoGen =
			col.column_default &&
			(col.column_default.toLowerCase().includes('nextval') ||
				col.column_default.toLowerCase().includes('gen_random'));
		const required = !col.is_nullable && !col.column_default;

		const field = h('div', { cls: 'modal-field' });

		const label = h('div', { cls: 'modal-field-label' },
			h('span', { text: col.name }),
			h('span', { cls: 'type-hint', text: ' ' + (col.type || '') })
		);
		if (col.is_primary_key) label.appendChild(h('span', { cls: 'badge pk', text: ' PK' }));
		if (col.fk_target_table) label.appendChild(h('span', { cls: 'badge fk', text: ' FK\u2192' + col.fk_target_table }));
		if (required) label.appendChild(h('span', { cls: 'req', text: ' *' }));
		field.appendChild(label);

		if (col.enum_values && col.enum_values.length > 0) {
			const select = document.createElement('select');
			select.dataset.column = col.name;
			if (col.is_nullable || col.column_default) {
				const opt = document.createElement('option');
				opt.value = '__DEFAULT__';
				opt.textContent = col.column_default ? '(default)' : '(null)';
				select.appendChild(opt);
			}
			col.enum_values.forEach((v) => {
				const opt = document.createElement('option');
				opt.value = v;
				opt.textContent = v;
				select.appendChild(opt);
			});
			field.appendChild(select);
		} else {
			const input = document.createElement('input');
			input.type = 'text';
			input.dataset.column = col.name;
			input.dataset.autoGen = autoGen ? '1' : '';
			if (autoGen) {
				input.placeholder = '(auto-generated)';
				input.classList.add('auto-gen');
			} else if (col.column_default)
				input.placeholder = `default: ${col.column_default}`;
			else if (col.is_nullable) input.placeholder = '(null)';
			else input.placeholder = 'required';
			field.appendChild(input);
		}
		container.appendChild(field);
	});

	openModal('insert-overlay');
};

$('insert-ok').onclick = () => submitInsert();
$('insert-cancel').onclick = () => {
	releaseFocus($('insert-overlay'));
	closeModal('insert-overlay');
};
$('insert-overlay').onclick = (e) => {
	if (e.target === $('insert-overlay')) {
		releaseFocus($('insert-overlay'));
		closeModal('insert-overlay');
	}
};
$('insert-fields').addEventListener('keydown', (e) => {
	if (e.key === 'Enter') submitInsert();
});

$('btn-find-replace').onclick = () => {
	if (!State.currentTable || !State.dbConnected) return;
	$('fnr-tbl').textContent = State.currentTable;
	const select = $('fnr-column');
	select.innerHTML = '';
	State.currentColumns.forEach((col) => {
		if (col === 'ctid') return;
		const opt = document.createElement('option');
		opt.value = col;
		opt.textContent = col;
		select.appendChild(opt);
	});
	$('fnr-find').value = '';
	$('fnr-replace').value = '';
	$('fnr-preview').style.display = 'none';
	openModal('fnr-overlay');
};

$('fnr-preview-btn').onclick = async () => {
	const col = $('fnr-column').value;
	const find = $('fnr-find').value;
	const replace = $('fnr-replace').value;
	if (!find) {
		toast('Enter a search term', 'error');
		return;
	}
	try {
		const data = await postJson(
			`/api/tables/${encodeURIComponent(State.currentTable)}/bulk-update`,
			{ column: col, find, replace, force: 'false' }
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}
		$('fnr-preview').style.display = 'block';
		$('fnr-preview').textContent =
			`${data.affected_rows} rows will be affected`;
		$('fnr-preview').style.color =
			data.affected_rows > 0 ? 'var(--warning)' : 'var(--text-muted)';
	} catch (e) {
		toast(`Preview failed: ${e.message}`, 'error');
	}
};

$('fnr-ok').onclick = async () => {
	const col = $('fnr-column').value;
	const find = $('fnr-find').value;
	const replace = $('fnr-replace').value;
	if (!find) {
		toast('Enter a search term', 'error');
		return;
	}
	try {
		const data = await postJson(
			`/api/tables/${encodeURIComponent(State.currentTable)}/bulk-update`,
			{ column: col, find, replace, force: 'true' }
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}
		releaseFocus($('fnr-overlay'));
		closeModal('fnr-overlay');
		toast(`Replaced in ${State.currentTable}`, 'success');
		bumpJournal();
		loadTableData();
	} catch (e) {
		toast(`Replace failed: ${e.message}`, 'error');
	}
};

$('fnr-cancel').onclick = () => {
	releaseFocus($('fnr-overlay'));
	closeModal('fnr-overlay');
};
$('fnr-overlay').onclick = (e) => {
	if (e.target === $('fnr-overlay')) {
		releaseFocus($('fnr-overlay'));
		closeModal('fnr-overlay');
	}
};

async function showDDL() {
	if (!State.currentTable) return;
	$('ddl-title').textContent = `${State.currentTable} \u2014 DDL`;
	$('ddl-content').textContent = 'Loading...';
	openModal('ddl-overlay');
	try {
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(State.currentTable)}/ddl`
		);
		if (data.error) $('ddl-content').textContent = `Error: ${data.error}`;
		else $('ddl-content').textContent = data.ddl || 'No DDL available';
	} catch (_e) {
		$('ddl-content').textContent = 'Failed to load DDL';
	}
}
if ($('btn-ddl')) $('btn-ddl').onclick = showDDL;
$('ddl-copy').onclick = () => {
	copyToClipboard($('ddl-content').textContent)
		.then(() => toast('DDL copied', 'success'))
		.catch(() => {});
};
$('ddl-overlay').onclick = (e) => {
	if (e.target === $('ddl-overlay')) {
		releaseFocus($('ddl-overlay'));
		closeModal('ddl-overlay');
	}
};

async function showStats() {
	if (!State.currentTable) return;
	try {
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(State.currentTable)}/stats`
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}

		let tip = document.querySelector('.stats-tip');
		if (tip) tip.remove();

		tip = document.createElement('div');
		tip.className = 'stats-tip';
		tip.style.top = '100px';
		tip.style.right = '40px';
		const rows = [
			['Rows', String(data.row_count ?? '?')],
			['Table size', data.table_size || '?'],
			['Index size', data.index_size || '?'],
			['Total size', data.total_size || '?'],
		];
		rows.forEach(([label, val]) => {
			const row = document.createElement('div');
			row.className = 'stat-row';
			const lbl = document.createElement('span');
			lbl.className = 'stat-label';
			lbl.textContent = label;
			const v = document.createElement('span');
			v.className = 'stat-val';
			v.textContent = val;
			row.appendChild(lbl);
			row.appendChild(v);
			tip.appendChild(row);
		});
		document.body.appendChild(tip);

		setTimeout(() => {
			const closeTip = (e) => {
				if (!tip.contains(e.target)) {
					tip.remove();
					document.removeEventListener('click', closeTip);
				}
			};
			document.addEventListener('click', closeTip);
		}, 10);
	} catch (_e) {
		toast('Failed to load stats', 'error');
	}
}
if ($('btn-stats')) $('btn-stats').onclick = showStats;

async function truncateTable() {
	if (!State.currentTable || !State.dbConnected) return;
	const ok = await confirm(
		'Truncate Table',
		'TRUNCATE TABLE "' +
			State.currentTable +
			'" \u2014 This will delete ALL rows. This cannot be undone.'
	);
	if (!ok) return;
	try {
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(State.currentTable)}/truncate`,
			{ method: 'POST' }
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}
		toast(`Table ${State.currentTable} truncated`, 'success');
		bumpJournal();
		loadTableData();
	} catch (e) {
		toast(`Truncate failed: ${e.message}`, 'error');
	}
}
if ($('btn-truncate')) $('btn-truncate').onclick = truncateTable;

function showImportCSV() {
	if (!State.currentTable || !State.dbConnected || State.readOnlyMode) {
		if (State.readOnlyMode) toast('Read-only mode', 'error');
		return;
	}
	$('import-tbl').textContent = State.currentTable;
	$('import-csv').value = '';
	$('import-preview').style.display = 'none';
	openModal('import-overlay');
}
if ($('btn-import-csv')) $('btn-import-csv').onclick = showImportCSV;

$('import-ok').onclick = async () => {
	const csv = $('import-csv').value.trim();
	if (!csv) {
		toast('Paste CSV data', 'error');
		return;
	}
	const hasHeader = $('import-header').checked;

	releaseFocus($('import-overlay'));
	closeModal('import-overlay');

	try {
		const data = await postJson(
			`/api/tables/${encodeURIComponent(State.currentTable)}/import`,
			{ csv, has_header: hasHeader }
		);
		if (data.error) toast(data.error, 'error');
		else {
			toast(`Imported ${data.imported || 0} rows`, 'success');
			loadTableData();
		}
	} catch (e) {
		toast(`Import failed: ${e.message}`, 'error');
	}
};

$('import-cancel').onclick = () => {
	releaseFocus($('import-overlay'));
	closeModal('import-overlay');
};
$('import-overlay').onclick = (e) => {
	if (e.target === $('import-overlay')) {
		releaseFocus($('import-overlay'));
		closeModal('import-overlay');
	}
};
