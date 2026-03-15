// Table Operations module
// Handles: Insert Row modal, Find & Replace modal, DDL viewer, Table Stats, Truncate, Import CSV

// Insert Row (Modal)
$('btn-add-row').onclick = () => {
	if (!currentTable || !dbConnected) return;
	const meta = getTableMeta(currentTable);
	if (!meta || !meta.columns) {
		toast('No column info', 'error');
		return;
	}

	$('insert-tbl').textContent = currentTable;
	const container = $('insert-fields');
	container.innerHTML = '';

	meta.columns.forEach((col) => {
		const autoGen =
			col.column_default &&
			(col.column_default.toLowerCase().includes('nextval') ||
				col.column_default.toLowerCase().includes('gen_random'));
		const required = !col.is_nullable && !col.column_default;

		const field = document.createElement('div');
		field.className = 'modal-field';

		let labelHtml = `<span>${escHtml(col.name)}</span>`;
		labelHtml += ` <span class="type-hint">${escHtml(col.type || '')}</span>`;
		if (col.is_primary_key) labelHtml += ' <span class="badge pk">PK</span>';
		if (col.fk_target_table)
			labelHtml +=
				' <span class="badge fk">FK\u2192' +
				escHtml(col.fk_target_table) +
				'</span>';
		if (required) labelHtml += ' <span class="req">*</span>';

		const label = document.createElement('div');
		label.className = 'modal-field-label';
		label.innerHTML = labelHtml;
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

	$('insert-overlay').classList.add('open');
	trapFocus($('insert-overlay'));
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

// Find & Replace
$('btn-find-replace').onclick = () => {
	if (!currentTable || !dbConnected) return;
	$('fnr-tbl').textContent = currentTable;
	const select = $('fnr-column');
	select.innerHTML = '';
	currentColumns.forEach((col) => {
		if (col === 'ctid') return;
		const opt = document.createElement('option');
		opt.value = col;
		opt.textContent = col;
		select.appendChild(opt);
	});
	$('fnr-find').value = '';
	$('fnr-replace').value = '';
	$('fnr-preview').style.display = 'none';
	$('fnr-overlay').classList.add('open');
	trapFocus($('fnr-overlay'));
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
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(currentTable)}/bulk-update`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ column: col, find, replace, force: 'false' }),
			}
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
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(currentTable)}/bulk-update`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ column: col, find, replace, force: 'true' }),
			}
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}
		releaseFocus($('fnr-overlay'));
		closeModal('fnr-overlay');
		toast(`Replaced in ${currentTable}`, 'success');
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

// DDL Viewer
$('btn-ddl').onclick = async () => {
	if (!currentTable) return;
	$('ddl-title').textContent = `${currentTable} \u2014 DDL`;
	$('ddl-content').textContent = 'Loading...';
	$('ddl-overlay').classList.add('open');
	trapFocus($('ddl-overlay'));
	try {
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(currentTable)}/ddl`
		);
		if (data.error) $('ddl-content').textContent = `Error: ${data.error}`;
		else $('ddl-content').textContent = data.ddl || 'No DDL available';
	} catch (_e) {
		$('ddl-content').textContent = 'Failed to load DDL';
	}
};
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

// Table Stats
$('btn-stats').onclick = async () => {
	if (!currentTable) return;
	try {
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(currentTable)}/stats`
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}

		const btn = $('btn-stats');
		const rect = btn.getBoundingClientRect();
		let tip = document.querySelector('.stats-tip');
		if (tip) tip.remove();

		tip = document.createElement('div');
		tip.className = 'stats-tip';
		tip.style.top = `${rect.bottom + 4}px`;
		tip.style.right = `${window.innerWidth - rect.right}px`;
		tip.innerHTML =
			'<div class="stat-row"><span class="stat-label">Rows</span><span class="stat-val">' +
			escHtml(String(data.row_count ?? '?')) +
			'</span></div>' +
			'<div class="stat-row"><span class="stat-label">Table size</span><span class="stat-val">' +
			escHtml(data.table_size || '?') +
			'</span></div>' +
			'<div class="stat-row"><span class="stat-label">Index size</span><span class="stat-val">' +
			escHtml(data.index_size || '?') +
			'</span></div>' +
			'<div class="stat-row"><span class="stat-label">Total size</span><span class="stat-val">' +
			escHtml(data.total_size || '?') +
			'</span></div>';
		document.body.appendChild(tip);

		setTimeout(() => {
			const closeTip = (e) => {
				if (!tip.contains(e.target) && e.target !== btn) {
					tip.remove();
					document.removeEventListener('click', closeTip);
				}
			};
			document.addEventListener('click', closeTip);
		}, 10);
	} catch (_e) {
		toast('Failed to load stats', 'error');
	}
};

// Truncate Table
$('btn-truncate').onclick = async () => {
	if (!currentTable || !dbConnected) return;
	const ok = await confirm(
		'Truncate Table',
		'TRUNCATE TABLE "' +
			currentTable +
			'" \u2014 This will delete ALL rows. This cannot be undone.'
	);
	if (!ok) return;
	try {
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(currentTable)}/truncate`,
			{ method: 'POST' }
		);
		if (data.error) {
			toast(data.error, 'error');
			return;
		}
		toast(`Table ${currentTable} truncated`, 'success');
		bumpJournal();
		loadTableData();
	} catch (e) {
		toast(`Truncate failed: ${e.message}`, 'error');
	}
};

// Import CSV
$('btn-import-csv').onclick = () => {
	if (!currentTable || !dbConnected || readOnlyMode) {
		if (readOnlyMode) toast('Read-only mode', 'error');
		return;
	}
	$('import-tbl').textContent = currentTable;
	$('import-csv').value = '';
	$('import-preview').style.display = 'none';
	$('import-overlay').classList.add('open');
	trapFocus($('import-overlay'));
};

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
		const data = await fetchJson(
			`/api/tables/${encodeURIComponent(currentTable)}/import`,
			{
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ csv, has_header: hasHeader }),
			}
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
