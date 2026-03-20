// table-create.js — Create table modal: column definition UI and DDL generation

let createTableColCount = 0;

function addCreateTableCol() {
	const container = $('create-table-cols');
	const idx = createTableColCount++;

	const typeOptions = [
		'integer', 'bigint', 'serial', 'bigserial',
		'text', 'varchar(255)', 'boolean',
		'numeric', 'real', 'double precision',
		'date', 'timestamp', 'timestamptz',
		'json', 'jsonb', 'uuid',
	];
	const select = h('select', { cls: 'col-type-select', 'data-idx': String(idx) });
	typeOptions.forEach((t) => {
		const opt = h('option', { value: t, text: t });
		if (t === 'text') opt.selected = true;
		select.appendChild(opt);
	});

	const checkLabel = (cls, labelText) =>
		h('label', { style: 'display:flex;align-items:center;gap:3px;font-size:10px;color:var(--text-muted);white-space:nowrap' },
			h('input', { type: 'checkbox', cls, 'data-idx': String(idx), style: 'accent-color:var(--accent)' }),
			labelText
		);

	const row = h('div', { cls: 'create-col-row' },
		h('input', { cls: 'col-name-input', placeholder: 'column_name', 'data-idx': String(idx), spellcheck: 'false' }),
		select,
		checkLabel('col-pk-check', 'PK'),
		checkLabel('col-nn-check', 'NN'),
		h('button', { cls: 'col-del-btn', title: 'Remove', text: '\u00d7', onclick: () => row.remove() })
	);
	container.appendChild(row);
}

function buildCreateTableDDL() {
	const name = $('create-table-name').value.trim();
	if (!name) return null;
	const rows = $('create-table-cols').querySelectorAll('.create-col-row');
	if (rows.length === 0) return null;

	const cols = [];
	const pks = [];
	rows.forEach((row) => {
		const colName = row.querySelector('.col-name-input').value.trim();
		if (!colName) return;
		const colType = row.querySelector('.col-type-select').value;
		const isPk = row.querySelector('.col-pk-check').checked;
		const isNn = row.querySelector('.col-nn-check').checked;
		let def = `"${colName.replace(/"/g, '""')}" ${colType}`;
		if (isNn) def += ' NOT NULL';
		cols.push(def);
		if (isPk) pks.push(`"${colName.replace(/"/g, '""')}"`);
	});

	if (cols.length === 0) return null;
	let ddl =
		'CREATE TABLE "' +
		name.replace(/"/g, '""') +
		'" (\n  ' +
		cols.join(',\n  ');
	if (pks.length > 0) ddl += `,\n  PRIMARY KEY (${pks.join(', ')})`;
	ddl += '\n);';
	return ddl;
}

$('btn-create-table').onclick = () => {
	if (!State.dbConnected) {
		toast('Connect first', 'error');
		return;
	}
	$('create-table-name').value = '';
	$('create-table-cols').innerHTML = '';
	createTableColCount = 0;
	addCreateTableCol();
	addCreateTableCol();
	openModal('create-table-overlay');
};

$('create-add-col').onclick = addCreateTableCol;

$('create-table-preview').onclick = () => {
	const ddl = buildCreateTableDDL();
	if (!ddl) {
		toast('Add table name and columns', 'error');
		return;
	}
	sqlEditor.value = ddl;
	syncHighlight();
	releaseFocus($('create-table-overlay'));
	closeModal('create-table-overlay');
	switchTab('sql');
	toast('DDL preview in SQL editor', 'info');
};

$('create-table-ok').onclick = async () => {
	const ddl = buildCreateTableDDL();
	if (!ddl) {
		toast('Add table name and columns', 'error');
		return;
	}
	const sql = ddl;
	releaseFocus($('create-table-overlay'));
	closeModal('create-table-overlay');
	try {
		const data = await postJson('/api/sql', { sql, force: 'true' });
		if (data.error) toast(data.error, 'error');
		else {
			toast('Table created', 'success');
			await loadSchema();
		}
	} catch (e) {
		toast(`Create failed: ${e.message}`, 'error');
	}
};

$('create-table-cancel').onclick = () => {
	releaseFocus($('create-table-overlay'));
	closeModal('create-table-overlay');
};
$('create-table-overlay').onclick = (e) => {
	if (e.target === $('create-table-overlay')) {
		releaseFocus($('create-table-overlay'));
		closeModal('create-table-overlay');
	}
};
