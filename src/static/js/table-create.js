// table-create.js — Create table modal: column definition UI and DDL generation

let createTableColCount = 0;

function addCreateTableCol() {
	const container = $('create-table-cols');
	const row = document.createElement('div');
	row.className = 'create-col-row';
	const idx = createTableColCount++;
	row.innerHTML =
		'<input class="col-name-input" placeholder="column_name" data-idx="' +
		idx +
		'" spellcheck="false">' +
		'<select class="col-type-select" data-idx="' +
		idx +
		'">' +
		'<option>integer</option><option>bigint</option><option>serial</option><option>bigserial</option>' +
		'<option selected>text</option><option>varchar(255)</option><option>boolean</option>' +
		'<option>numeric</option><option>real</option><option>double precision</option>' +
		'<option>date</option><option>timestamp</option><option>timestamptz</option>' +
		'<option>json</option><option>jsonb</option><option>uuid</option>' +
		'</select>' +
		'<label style="display:flex;align-items:center;gap:3px;font-size:10px;color:var(--text-muted);white-space:nowrap"><input type="checkbox" class="col-pk-check" data-idx="' +
		idx +
		'" style="accent-color:var(--accent)">PK</label>' +
		'<label style="display:flex;align-items:center;gap:3px;font-size:10px;color:var(--text-muted);white-space:nowrap"><input type="checkbox" class="col-nn-check" data-idx="' +
		idx +
		'" style="accent-color:var(--accent)">NN</label>' +
		'<button class="col-del-btn" title="Remove">&times;</button>';
	row.querySelector('.col-del-btn').onclick = () => row.remove();
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
	$('create-table-overlay').classList.add('open');
	trapFocus($('create-table-overlay'));
};

$('create-add-col').onclick = addCreateTableCol;

$('create-table-preview').onclick = () => {
	const ddl = buildCreateTableDDL();
	if (!ddl) {
		toast('Add table name and columns', 'error');
		return;
	}
	sqlEditor.value = ddl.replace(/\\n/g, '\n');
	syncHighlight();
	releaseFocus($('create-table-overlay'));
	closeModal('create-table-overlay');
	$$('.tab').forEach((t) => t.classList.remove('active'));
	$$('.tab-panel').forEach((p) => p.classList.remove('active'));
	document.querySelector('[data-tab="sql"]').classList.add('active');
	$('panel-sql').classList.add('active');
	toast('DDL preview in SQL editor', 'info');
};

$('create-table-ok').onclick = async () => {
	const ddl = buildCreateTableDDL();
	if (!ddl) {
		toast('Add table name and columns', 'error');
		return;
	}
	const sql = ddl.replace(/\\n/g, '\n');
	releaseFocus($('create-table-overlay'));
	closeModal('create-table-overlay');
	try {
		const data = await fetchJson('/api/sql', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ sql, force: 'true' }),
		});
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
