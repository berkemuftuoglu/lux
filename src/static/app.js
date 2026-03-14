// Constants
const TOAST_DURATION = 3500;
const TOAST_HOVER_DURATION = 2000;
const HEALTH_CHECK_INTERVAL = 30000;
const AC_LINE_HEIGHT = 23;
const AC_CHAR_WIDTH = 8.4;

// Helpers
const $ = (id) => document.getElementById(id);
const $$ = (sel) => document.querySelectorAll(sel);
function escHtml(s) {
	const d = document.createElement('div');
	d.textContent = s;
	return d.innerHTML;
}
function prettyName(name) {
	return name.replace(/_/g, ' ').replace(/\bid\b/gi, 'ID');
}
function copyToClipboard(text) {
	if (navigator.clipboard?.writeText) {
		return navigator.clipboard.writeText(text);
	}
	// Fallback for insecure contexts (HTTP)
	const ta = document.createElement('textarea');
	ta.value = text;
	ta.style.position = 'fixed';
	ta.style.opacity = '0';
	document.body.appendChild(ta);
	ta.select();
	try {
		document.execCommand('copy');
	} catch (_e) {
		/* best effort */
	}
	ta.remove();
	return Promise.resolve();
}

async function fetchJson(url, opts) {
	const res = await fetch(url, opts);
	if (!res.ok) {
		let msg = `Server error: ${res.status}`;
		try {
			const body = await res.json();
			if (body.error) msg = body.error;
		} catch (_e) {
			/* no JSON body */
		}
		throw new Error(msg);
	}
	return res.json();
}

// Theme Toggle
function setTheme(theme) {
	document.documentElement.setAttribute('data-theme', theme);
	localStorage.setItem('lux-theme', theme);
	$('theme-icon-sun').style.display = theme === 'dark' ? 'none' : 'block';
	$('theme-icon-moon').style.display = theme === 'dark' ? 'block' : 'none';
	// Redraw ER if visible
	if ($('panel-er').classList.contains('active')) drawER();
}
$('btn-theme').onclick = () => {
	const current = document.documentElement.getAttribute('data-theme');
	setTheme(current === 'dark' ? 'light' : 'dark');
};
// Init theme from localStorage or system preference
const savedTheme = localStorage.getItem('lux-theme');
if (savedTheme) setTheme(savedTheme);
else if (window.matchMedia('(prefers-color-scheme: light)').matches)
	setTheme('light');
else setTheme('dark');

// State
let schemaData = null;
let currentTable = null;
let currentColumns = [];
let currentRows = [];
let currentPkColumns = [];
let currentPkMode = 'column';
let readOnlyMode = false;
let dbConnected = false;
let pageOffset = 0;
let pageLimit = 100;
let totalRows = 0;
let sortCol = null;
let sortDir = 'asc';
let editingCell = null;
let journalCount = 0;
let erZoom = 1;
let erPanX = 0,
	erPanY = 0;
let focusRow = -1,
	focusCol = -1;
let columnFilters = {};
const columnWidths = {};
let selectedRows = new Set();
let lastSelectedRow = -1;
let lastSqlQuery = '';
let healthCheckInterval = null;

// Toast
function toast(msg, type) {
	const el = document.createElement('div');
	el.className = `toast ${type || 'info'}`;
	el.textContent = msg;
	$('toast-container').appendChild(el);
	let timer = setTimeout(() => {
		el.classList.add('hiding');
		setTimeout(() => el.remove(), 200);
	}, TOAST_DURATION);
	el.addEventListener('mouseenter', () => clearTimeout(timer));
	el.addEventListener('mouseleave', () => {
		timer = setTimeout(() => {
			el.classList.add('hiding');
			setTimeout(() => el.remove(), 200);
		}, TOAST_HOVER_DURATION);
	});
}

// Loading Spinner
function showLoading(parentId) {
	const parent = $(parentId);
	if (!parent || parent.querySelector('.loading-overlay')) return;
	const overlay = document.createElement('div');
	overlay.className = 'loading-overlay';
	overlay.innerHTML = '<div class="spinner"></div>';
	parent.style.position = 'relative';
	parent.appendChild(overlay);
}
function hideLoading(parentId) {
	const parent = $(parentId);
	if (!parent) return;
	const overlay = parent.querySelector('.loading-overlay');
	if (overlay) overlay.remove();
}

// Confirm Dialog
let confirmResolve = null;
function confirm(title, body) {
	return new Promise((resolve) => {
		$('confirm-title').textContent = title;
		$('confirm-body').textContent = body;
		$('confirm-overlay').classList.add('open');
		trapFocus($('confirm-overlay'));
		confirmResolve = resolve;
	});
}
$('confirm-ok').onclick = () => {
	$('confirm-overlay').classList.remove('open');
	releaseFocus($('confirm-overlay'));
	if (confirmResolve) confirmResolve(true);
};
$('confirm-cancel').onclick = () => {
	$('confirm-overlay').classList.remove('open');
	releaseFocus($('confirm-overlay'));
	if (confirmResolve) confirmResolve(false);
};

let promptResolve = null;
function promptUser(title, defaultVal) {
	return new Promise((resolve) => {
		$('prompt-title').textContent = title;
		$('prompt-input').value = defaultVal || '';
		$('prompt-overlay').classList.add('open');
		trapFocus($('prompt-overlay'));
		promptResolve = resolve;
		setTimeout(() => $('prompt-input').focus(), 50);
	});
}
$('prompt-ok').onclick = () => {
	$('prompt-overlay').classList.remove('open');
	releaseFocus($('prompt-overlay'));
	if (promptResolve) promptResolve($('prompt-input').value);
};
$('prompt-cancel').onclick = () => {
	$('prompt-overlay').classList.remove('open');
	releaseFocus($('prompt-overlay'));
	if (promptResolve) promptResolve(null);
};
$('prompt-input').addEventListener('keydown', (e) => {
	if (e.key === 'Enter') $('prompt-ok').click();
});
$('prompt-overlay').onclick = (e) => {
	if (e.target === $('prompt-overlay')) $('prompt-cancel').click();
};

// Focus Trap
function trapFocus(modal) {
	const focusable = modal.querySelectorAll(
		'button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
	);
	if (!focusable.length) return;
	const first = focusable[0],
		last = focusable[focusable.length - 1];
	function handler(e) {
		if (e.key !== 'Tab') return;
		if (e.shiftKey && document.activeElement === first) {
			e.preventDefault();
			last.focus();
		} else if (!e.shiftKey && document.activeElement === last) {
			e.preventDefault();
			first.focus();
		}
	}
	modal.addEventListener('keydown', handler);
	modal._focusTrap = handler;
	first.focus();
}
function releaseFocus(modal) {
	if (modal._focusTrap) modal.removeEventListener('keydown', modal._focusTrap);
}

// Tabs
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

// Connection
$('conn-btn').onclick = doConnect;
$('conn-input').addEventListener('keydown', (e) => {
	if (e.key === 'Enter') doConnect();
});

async function doConnect() {
	const conninfo = $('conn-input').value.trim();
	if (!conninfo) return;
	$('conn-btn').disabled = true;
	$('conn-btn').textContent = '...';
	try {
		const data = await fetchJson('/api/connect', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ conninfo }),
		});
		if (data.error) {
			showConnStatus(false, data.error);
			toast(data.error, 'error');
		} else {
			dbConnected = true;
			showConnStatus(true, `${data.tables} tables`);
			updateConnUI();
			await loadSchema();
			toast('Connected', 'success');
			startHealthCheck();
			// Set connection stripe color
			const connStr = $('conn-input').value.trim();
			const savedColor = getSavedConnColor(connStr);
			setConnStripe(savedColor || detectConnColor(connStr));
		}
	} catch (e) {
		showConnStatus(false, e.message);
		toast('Connection failed', 'error');
	}
	$('conn-btn').disabled = false;
	$('conn-btn').textContent = 'Connect';
}

function showConnStatus(ok, msg) {
	$('conn-status').style.display = 'flex';
	$('conn-dot').className = `dot ${ok ? 'ok' : 'err'}`;
	$('conn-msg').textContent = msg;
}
function updateConnUI() {
	$('hdr-dot').classList.toggle('connected', dbConnected);
	$('status-dot').classList.toggle('ok', dbConnected);
	$('status-conn').textContent = dbConnected ? 'Connected' : 'Not connected';
	// Show/hide save connection button
	$('save-conn-btn').style.display = dbConnected ? '' : 'none';
}

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
	$('insert-overlay').classList.remove('open');
};
$('insert-overlay').onclick = (e) => {
	if (e.target === $('insert-overlay')) {
		releaseFocus($('insert-overlay'));
		$('insert-overlay').classList.remove('open');
	}
};
$('insert-fields').addEventListener('keydown', (e) => {
	if (e.key === 'Enter') submitInsert();
});

// Export
$('btn-export-csv').onclick = () => {
	if (currentTable)
		window.location.href = `/api/export/${encodeURIComponent(currentTable)}?format=csv`;
};
$('btn-export-json').onclick = () => {
	if (currentTable)
		window.location.href = `/api/export/${encodeURIComponent(currentTable)}?format=json`;
};

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
		$('fnr-overlay').classList.remove('open');
		toast(`Replaced in ${currentTable}`, 'success');
		bumpJournal();
		loadTableData();
	} catch (e) {
		toast(`Replace failed: ${e.message}`, 'error');
	}
};

$('fnr-cancel').onclick = () => {
	releaseFocus($('fnr-overlay'));
	$('fnr-overlay').classList.remove('open');
};
$('fnr-overlay').onclick = (e) => {
	if (e.target === $('fnr-overlay')) {
		releaseFocus($('fnr-overlay'));
		$('fnr-overlay').classList.remove('open');
	}
};

// Refresh
$('btn-refresh').onclick = async () => {
	if (dbConnected) {
		await loadSchema();
		toast('Schema refreshed', 'success');
	}
};
$('btn-tbl-refresh').onclick = () => loadTableData();

// SQL Editor
const sqlEditor = $('sql-editor');
const sqlHighlight = $('sql-highlight');

const SQL_KW = new Set([
	'SELECT',
	'FROM',
	'WHERE',
	'AND',
	'OR',
	'NOT',
	'IN',
	'IS',
	'NULL',
	'AS',
	'ON',
	'JOIN',
	'LEFT',
	'RIGHT',
	'INNER',
	'OUTER',
	'FULL',
	'CROSS',
	'GROUP',
	'BY',
	'ORDER',
	'HAVING',
	'LIMIT',
	'OFFSET',
	'INSERT',
	'INTO',
	'VALUES',
	'UPDATE',
	'SET',
	'DELETE',
	'CREATE',
	'ALTER',
	'DROP',
	'TABLE',
	'INDEX',
	'VIEW',
	'DISTINCT',
	'BETWEEN',
	'LIKE',
	'ILIKE',
	'EXISTS',
	'CASE',
	'WHEN',
	'THEN',
	'ELSE',
	'END',
	'UNION',
	'ALL',
	'ASC',
	'DESC',
	'CASCADE',
	'RESTRICT',
	'PRIMARY',
	'KEY',
	'FOREIGN',
	'REFERENCES',
	'CONSTRAINT',
	'UNIQUE',
	'DEFAULT',
	'CHECK',
	'RETURNING',
	'WITH',
	'RECURSIVE',
	'GRANT',
	'REVOKE',
	'BEGIN',
	'COMMIT',
	'ROLLBACK',
	'EXPLAIN',
	'ANALYZE',
	'VACUUM',
	'TRUNCATE',
	'SERIAL',
	'BIGSERIAL',
	'TRUE',
	'FALSE',
	'IF',
	'REPLACE',
	'TEMP',
	'TEMPORARY',
	'SCHEMA',
	'DATABASE',
	'TRIGGER',
	'FUNCTION',
	'PROCEDURE',
	'RETURNS',
	'LANGUAGE',
	'VOLATILE',
	'IMMUTABLE',
	'STABLE',
	'SECURITY',
	'DEFINER',
	'INVOKER',
	'EXECUTE',
	'USING',
	'CAST',
	'INTERVAL',
	'AT',
	'TIME',
	'ZONE',
	'CURRENT_TIMESTAMP',
	'CURRENT_DATE',
	'CURRENT_TIME',
	'NOW',
]);

const SQL_FN = new Set([
	'COUNT',
	'SUM',
	'AVG',
	'MIN',
	'MAX',
	'COALESCE',
	'NULLIF',
	'CAST',
	'EXTRACT',
	'DATE_TRUNC',
	'DATE_PART',
	'NOW',
	'CURRENT_DATE',
	'CURRENT_TIMESTAMP',
	'UPPER',
	'LOWER',
	'LENGTH',
	'TRIM',
	'SUBSTRING',
	'REPLACE',
	'CONCAT',
	'STRING_AGG',
	'ARRAY_AGG',
	'JSON_AGG',
	'JSONB_AGG',
	'ROW_NUMBER',
	'RANK',
	'DENSE_RANK',
	'LAG',
	'LEAD',
	'FIRST_VALUE',
	'LAST_VALUE',
	'OVER',
	'PARTITION',
	'GENERATE_SERIES',
	'UNNEST',
	'TO_CHAR',
	'TO_DATE',
	'TO_NUMBER',
	'TO_TIMESTAMP',
	'AGE',
	'ROUND',
	'CEIL',
	'FLOOR',
	'ABS',
	'MOD',
	'POWER',
	'SQRT',
	'RANDOM',
	'PG_SIZE_PRETTY',
	'PG_RELATION_SIZE',
	'PG_TOTAL_RELATION_SIZE',
	'GREATEST',
	'LEAST',
	'ARRAY_LENGTH',
	'JSONB_BUILD_OBJECT',
]);

function highlightSQL(text) {
	let html = '';
	let i = 0;
	while (i < text.length) {
		if (text[i] === '-' && text[i + 1] === '-') {
			const end = text.indexOf('\n', i);
			const comment = end === -1 ? text.slice(i) : text.slice(i, end);
			html += `<span class="sql-comment">${escHtml(comment)}</span>`;
			i += comment.length;
			continue;
		}
		if (text[i] === "'") {
			let j = i + 1;
			while (j < text.length) {
				if (text[j] === "'" && text[j + 1] === "'") j += 2;
				else if (text[j] === "'") {
					j++;
					break;
				} else j++;
			}
			html += `<span class="sql-str">${escHtml(text.slice(i, j))}</span>`;
			i = j;
			continue;
		}
		if (
			/[0-9]/.test(text[i]) &&
			(i === 0 || /[\s,()=<>+\-*/]/.test(text[i - 1]))
		) {
			let j = i;
			while (j < text.length && /[0-9.]/.test(text[j])) j++;
			html += `<span class="sql-num">${escHtml(text.slice(i, j))}</span>`;
			i = j;
			continue;
		}
		if ('=<>!+-*/|&'.includes(text[i])) {
			html += `<span class="sql-op">${escHtml(text[i])}</span>`;
			i++;
			continue;
		}
		if (/[a-zA-Z_]/.test(text[i])) {
			let j = i;
			while (j < text.length && /[a-zA-Z0-9_]/.test(text[j])) j++;
			const word = text.slice(i, j);
			const upper = word.toUpperCase();
			if (SQL_KW.has(upper))
				html += `<span class="sql-kw">${escHtml(word)}</span>`;
			else if (SQL_FN.has(upper))
				html += `<span class="sql-fn">${escHtml(word)}</span>`;
			else html += escHtml(word);
			i = j;
			continue;
		}
		html += escHtml(text[i]);
		i++;
	}
	if (text.endsWith('\n')) html += '\n';
	return html;
}

function syncHighlight() {
	sqlHighlight.innerHTML = highlightSQL(sqlEditor.value);
	sqlHighlight.scrollTop = sqlEditor.scrollTop;
	sqlHighlight.scrollLeft = sqlEditor.scrollLeft;
}
sqlEditor.addEventListener('scroll', () => {
	sqlHighlight.scrollTop = sqlEditor.scrollTop;
	sqlHighlight.scrollLeft = sqlEditor.scrollLeft;
});

// SQL Autocomplete
const acDropdown = $('sql-autocomplete');
let acItems = [];
let acIndex = -1;

function getACWordAt(text, pos) {
	let start = pos;
	while (start > 0 && /[a-zA-Z0-9_.]/.test(text[start - 1])) start--;
	return { word: text.slice(start, pos), start };
}

function buildACList(prefix) {
	if (!prefix || prefix.length < 1) return [];
	const p = prefix.toLowerCase();
	const results = [];
	const seen = new Set();

	if (schemaData?.tables) {
		schemaData.tables.forEach((t) => {
			if (t.name.toLowerCase().startsWith(p) && !seen.has(t.name)) {
				results.push({ text: t.name, type: 'tbl' });
				seen.add(t.name);
			}
			const dotIdx = prefix.indexOf('.');
			if (dotIdx > 0) {
				const tblPart = prefix.slice(0, dotIdx).toLowerCase();
				const colPart = prefix.slice(dotIdx + 1).toLowerCase();
				if (t.name.toLowerCase() === tblPart) {
					t.columns.forEach((c) => {
						const full = `${t.name}.${c.name}`;
						if (c.name.toLowerCase().startsWith(colPart) && !seen.has(full)) {
							results.push({ text: full, type: 'col' });
							seen.add(full);
						}
					});
				}
			} else {
				t.columns.forEach((c) => {
					if (c.name.toLowerCase().startsWith(p) && !seen.has(c.name)) {
						results.push({ text: c.name, type: 'col' });
						seen.add(c.name);
					}
				});
			}
		});
	}

	SQL_KW.forEach((kw) => {
		if (kw.toLowerCase().startsWith(p) && !seen.has(kw)) {
			results.push({ text: kw, type: 'kw' });
			seen.add(kw);
		}
	});

	SQL_FN.forEach((fn) => {
		if (fn.toLowerCase().startsWith(p) && !seen.has(fn)) {
			results.push({ text: fn, type: 'fn' });
			seen.add(fn);
		}
	});

	const order = { tbl: 0, col: 1, kw: 2, fn: 3 };
	results.sort((a, b) => (order[a.type] || 9) - (order[b.type] || 9));
	return results.slice(0, 15);
}

function showAC() {
	const pos = sqlEditor.selectionStart;
	const { word, start: _start } = getACWordAt(sqlEditor.value, pos);

	if (word.length < 2) {
		hideAC();
		return;
	}

	const items = buildACList(word);
	if (items.length === 0) {
		hideAC();
		return;
	}

	acItems = items;
	acIndex = 0;
	renderAC();

	const textBefore = sqlEditor.value.substring(0, pos);
	const lines = textBefore.split('\n');
	const lineNum = lines.length - 1;
	const colNum = lines[lines.length - 1].length;
	const lineH = AC_LINE_HEIGHT;
	const charW = AC_CHAR_WIDTH;
	const top = Math.min(
		(lineNum + 1) * lineH + 16,
		sqlEditor.offsetHeight - 100
	);
	const left = Math.min(colNum * charW + 20, sqlEditor.offsetWidth - 240);
	acDropdown.style.top = `${top}px`;
	acDropdown.style.left = `${left}px`;
	acDropdown.classList.add('open');
}

function renderAC() {
	acDropdown.innerHTML = acItems
		.map(
			(item, i) =>
				'<div class="sql-ac-item' +
				(i === acIndex ? ' selected' : '') +
				'" data-idx="' +
				i +
				'">' +
				'<span class="ac-type ' +
				item.type +
				'">' +
				item.type +
				'</span>' +
				escHtml(item.text) +
				'</div>'
		)
		.join('');

	acDropdown.querySelectorAll('.sql-ac-item').forEach((el) => {
		el.addEventListener('mousedown', (e) => {
			e.preventDefault();
			acIndex = parseInt(el.dataset.idx, 10);
			acceptAC();
		});
	});
}

function acceptAC() {
	if (acIndex < 0 || acIndex >= acItems.length) return;
	const item = acItems[acIndex];
	const pos = sqlEditor.selectionStart;
	const { start } = getACWordAt(sqlEditor.value, pos);
	sqlEditor.value =
		sqlEditor.value.substring(0, start) +
		item.text +
		sqlEditor.value.substring(pos);
	sqlEditor.selectionStart = sqlEditor.selectionEnd = start + item.text.length;
	syncHighlight();
	hideAC();
	sqlEditor.focus();
}

function hideAC() {
	acDropdown.classList.remove('open');
	acItems = [];
	acIndex = -1;
}

let acTimer = null;
sqlEditor.addEventListener('input', () => {
	syncHighlight();
	clearTimeout(acTimer);
	acTimer = setTimeout(showAC, 80);
});

sqlEditor.addEventListener('keydown', (e) => {
	if (acDropdown.classList.contains('open')) {
		if (e.key === 'ArrowDown') {
			e.preventDefault();
			acIndex = Math.min(acIndex + 1, acItems.length - 1);
			renderAC();
			return;
		}
		if (e.key === 'ArrowUp') {
			e.preventDefault();
			acIndex = Math.max(acIndex - 1, 0);
			renderAC();
			return;
		}
		if (e.key === 'Tab' || e.key === 'Enter') {
			if (acItems.length > 0) {
				e.preventDefault();
				acceptAC();
				return;
			}
		}
		if (e.key === 'Escape') {
			e.preventDefault();
			hideAC();
			return;
		}
	}

	if (e.key === 'Tab') {
		e.preventDefault();
		const start = sqlEditor.selectionStart;
		sqlEditor.value =
			sqlEditor.value.substring(0, start) +
			'  ' +
			sqlEditor.value.substring(sqlEditor.selectionEnd);
		sqlEditor.selectionStart = sqlEditor.selectionEnd = start + 2;
		syncHighlight();
	}
});

sqlEditor.addEventListener('blur', () => setTimeout(hideAC, 150));

// Run SQL
$('btn-run-sql').onclick = runSQL;
sqlEditor.addEventListener('keydown', (e) => {
	if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
		e.preventDefault();
		hideAC();
		runSQL();
	}
});

function detectDangerousSQL(sql) {
	const s = sql
		.replace(/--[^\n]*/g, '')
		.replace(/\/\*[\s\S]*?\*\//g, '')
		.trim()
		.toUpperCase();
	const isUpdate = /^\s*UPDATE\b/i.test(s);
	const isDelete = /^\s*DELETE\b/i.test(s);
	if (!isUpdate && !isDelete) return null;
	const hasWhere = /\bWHERE\b/i.test(s);
	if (hasWhere) return null;
	return isDelete ? 'DELETE' : 'UPDATE';
}

async function runSQL() {
	const sql = sqlEditor.value.trim();
	if (!sql) return;

	// Client-side pre-check: warn on UPDATE/DELETE without WHERE, auto-preview first
	const danger = detectDangerousSQL(sql);
	if (danger) {
		// Auto-preview to show affected rows
		let previewMsg = `${danger} without WHERE -- this affects the entire table.`;
		try {
			const pd = await fetchJson('/api/sql/preview', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ sql }),
			});
			if (pd.row_count != null)
				previewMsg += `\n\nRows affected: ${pd.row_count}`;
			if (pd.columns && pd.rows && pd.rows.length > 0) {
				renderSQLResult(pd);
				toast(
					'Preview: ' +
						(pd.row_count || pd.rows.length) +
						' rows would be affected',
					'warning'
				);
			}
		} catch (_e) {
			/* preview failed, still show warning */
		}
		const ok = await confirm(`Dangerous ${danger}`, previewMsg);
		if (!ok) return;
	}

	const t0 = performance.now();
	showLoading('sql-results');
	try {
		const data = await fetchJson('/api/sql', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ sql, force: danger ? 'true' : undefined }),
		});
		const elapsed = Math.round(performance.now() - t0);

		if (data.requires_confirmation) {
			hideLoading('sql-results');
			const ok = await confirm(
				`${data.operation} \u2014 Confirm`,
				data.warning || sql
			);
			if (!ok) return;
			showLoading('sql-results');
			const t1 = performance.now();
			const data2 = await fetchJson('/api/sql', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ sql, force: 'true' }),
			});
			hideLoading('sql-results');
			renderSQLResult(data2, Math.round(performance.now() - t1));
			return;
		}

		hideLoading('sql-results');
		renderSQLResult(data, elapsed);
	} catch (e) {
		hideLoading('sql-results');
		$('sql-results').textContent = e.message;
	}
}

$('btn-explain').onclick = async () => {
	const sql = sqlEditor.value.trim();
	if (!sql) return;
	showLoading('sql-results');
	try {
		const data = await fetchJson('/api/sql', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ sql: `EXPLAIN ANALYZE ${sql}` }),
		});
		hideLoading('sql-results');
		renderSQLResult(data);
	} catch (e) {
		hideLoading('sql-results');
		$('sql-results').textContent = e.message;
	}
};

$('btn-preview-sql').onclick = async () => {
	const sql = sqlEditor.value.trim();
	if (!sql) return;
	showLoading('sql-results');
	try {
		const data = await fetchJson('/api/sql/preview', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ sql }),
		});
		hideLoading('sql-results');
		renderSQLResult(data);
		if (data.preview) toast('Preview only -- not committed', 'info');
	} catch (e) {
		hideLoading('sql-results');
		$('sql-results').textContent = e.message;
	}
};

function renderSQLResult(data, elapsed) {
	const timeStr = elapsed != null ? ` in ${elapsed}ms` : '';
	if (data.error) {
		$('sql-results').innerHTML =
			`<div class="sql-error">${escHtml(data.error)}</div>`;
		return;
	}
	if (!data.columns || data.columns.length === 0) {
		$('sql-results').innerHTML =
			'<div class="sql-message">' +
			(data.row_count != null
				? `${escHtml(String(data.row_count))} rows affected`
				: 'OK') +
			timeStr +
			'</div>';
		if (currentTable) loadTableData();
		return;
	}

	let html =
		'<div class="sql-results-info">' +
		escHtml(String(data.row_count || 0)) +
		' rows' +
		timeStr +
		'</div>';
	html +=
		'<div class="grid-wrap" style="max-height:100%"><table class="grid"><thead><tr>';
	data.columns.forEach((c) => {
		html += `<th>${escHtml(c)}</th>`;
	});
	html += '</tr></thead><tbody>';
	(data.rows || []).forEach((row) => {
		html += '<tr>';
		row.forEach((val) => {
			if (val === null || val === 'NULL')
				html += '<td class="null-val">NULL</td>';
			else html += `<td>${escHtml(val)}</td>`;
		});
		html += '</tr>';
	});
	html += '</tbody></table></div>';
	$('sql-results').innerHTML = html;

	// Add export buttons if there are results
	if (
		data.columns &&
		data.columns.length > 0 &&
		data.rows &&
		data.rows.length > 0
	) {
		const resultsInfo = $('sql-results').querySelector('.sql-results-info');
		if (resultsInfo) {
			const csvBtn = document.createElement('button');
			csvBtn.className = 'btn-export-inline';
			csvBtn.textContent = 'Export CSV';
			csvBtn.addEventListener('click', () => {
				exportSqlResults('csv');
			});
			const jsonBtn = document.createElement('button');
			jsonBtn.className = 'btn-export-inline';
			jsonBtn.textContent = 'Export JSON';
			jsonBtn.addEventListener('click', () => {
				exportSqlResults('json');
			});
			resultsInfo.appendChild(csvBtn);
			resultsInfo.appendChild(jsonBtn);
		}
	}
	// Store last SQL for export
	lastSqlQuery = sqlEditor.value.trim();
}

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
		$('ddl-overlay').classList.remove('open');
	}
};

// Query History
$('btn-sql-history').onclick = async () => {
	$('history-overlay').classList.add('open');
	trapFocus($('history-overlay'));
	try {
		const data = await fetchJson('/api/history');
		const entries = data.entries || [];
		const list = $('history-list');
		if (entries.length === 0) {
			list.innerHTML =
				'<div style="padding:32px;text-align:center;color:var(--text-muted)">No queries yet</div>';
			return;
		}
		let html = '';
		entries.forEach((entry) => {
			const time = new Date(entry.timestamp * 1000);
			const timeStr = time.toLocaleTimeString();
			html += `<div class="history-entry${entry.is_error ? ' error' : ''}">`;
			html +=
				'<div class="sql-preview">' +
				escHtml(entry.sql.substring(0, 200)) +
				'</div>';
			html += '<div class="history-meta">';
			html += `<span>${timeStr}</span>`;
			html += `<span class="dur">${entry.duration_ms}ms</span>`;
			if (entry.row_count != null)
				html += `<span>${escHtml(String(entry.row_count))} rows</span>`;
			if (entry.error)
				html +=
					'<span style="color:var(--error)">' +
					escHtml(entry.error.substring(0, 80)) +
					'</span>';
			html += '</div></div>';
		});
		list.innerHTML = html;
		// Click to load into editor
		list.querySelectorAll('.history-entry').forEach((el, i) => {
			el.addEventListener('click', () => {
				sqlEditor.value = entries[i].sql;
				syncHighlight();
				releaseFocus($('history-overlay'));
				$('history-overlay').classList.remove('open');
				$$('.tab').forEach((t) => t.classList.remove('active'));
				$$('.tab-panel').forEach((p) => p.classList.remove('active'));
				document.querySelector('[data-tab="sql"]').classList.add('active');
				$('panel-sql').classList.add('active');
				sqlEditor.focus();
			});
		});
	} catch (_e) {
		$('history-list').innerHTML =
			'<div style="padding:16px;color:var(--error)">Failed to load history</div>';
	}
};
$('history-overlay').onclick = (e) => {
	if (e.target === $('history-overlay')) {
		releaseFocus($('history-overlay'));
		$('history-overlay').classList.remove('open');
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

// SQL Formatter
$('btn-format-sql').onclick = () => {
	const sql = sqlEditor.value.trim();
	if (!sql) return;
	sqlEditor.value = formatSQL(sql);
	syncHighlight();
	toast('SQL formatted', 'success');
};

function formatSQL(sql) {
	// Simple SQL formatter: uppercase keywords, add newlines before major clauses
	const majorClauses = [
		'SELECT',
		'FROM',
		'WHERE',
		'AND',
		'OR',
		'JOIN',
		'LEFT JOIN',
		'RIGHT JOIN',
		'INNER JOIN',
		'OUTER JOIN',
		'FULL JOIN',
		'CROSS JOIN',
		'ON',
		'GROUP BY',
		'ORDER BY',
		'HAVING',
		'LIMIT',
		'OFFSET',
		'UNION',
		'UNION ALL',
		'INSERT INTO',
		'VALUES',
		'UPDATE',
		'SET',
		'DELETE FROM',
		'CREATE TABLE',
		'ALTER TABLE',
		'DROP TABLE',
		'WITH',
		'RETURNING',
		'CASE',
		'WHEN',
		'THEN',
		'ELSE',
		'END',
	];

	// Tokenize preserving strings and comments
	let i = 0;
	const tokens = [];
	while (i < sql.length) {
		if (sql[i] === "'") {
			let j = i + 1;
			while (j < sql.length) {
				if (sql[j] === "'" && sql[j + 1] === "'") j += 2;
				else if (sql[j] === "'") {
					j++;
					break;
				} else j++;
			}
			tokens.push({ type: 'string', text: sql.slice(i, j) });
			i = j;
		} else if (sql[i] === '-' && sql[i + 1] === '-') {
			const end = sql.indexOf('\n', i);
			tokens.push({
				type: 'comment',
				text: end === -1 ? sql.slice(i) : sql.slice(i, end),
			});
			i = end === -1 ? sql.length : end;
		} else if (/\s/.test(sql[i])) {
			let j = i;
			while (j < sql.length && /\s/.test(sql[j])) j++;
			tokens.push({ type: 'ws', text: ' ' });
			i = j;
		} else if (/[a-zA-Z_]/.test(sql[i])) {
			let j = i;
			while (j < sql.length && /[a-zA-Z0-9_]/.test(sql[j])) j++;
			tokens.push({ type: 'word', text: sql.slice(i, j) });
			i = j;
		} else {
			tokens.push({ type: 'sym', text: sql[i] });
			i++;
		}
	}

	// Reconstruct with formatting
	let formatted = '';
	for (let ti = 0; ti < tokens.length; ti++) {
		const t = tokens[ti];
		if (t.type === 'word') {
			const upper = t.text.toUpperCase();
			// Check for two-word clauses
			let twoWord = '';
			if (
				ti + 2 < tokens.length &&
				tokens[ti + 1].type === 'ws' &&
				tokens[ti + 2].type === 'word'
			) {
				twoWord = `${upper} ${tokens[ti + 2].text.toUpperCase()}`;
			}
			if (majorClauses.includes(twoWord)) {
				if (
					[
						'SELECT',
						'INSERT INTO',
						'UPDATE',
						'DELETE FROM',
						'CREATE TABLE',
						'WITH',
					].includes(twoWord)
				) {
					if (formatted.trim()) formatted += '\n';
				} else {
					formatted += '\n';
				}
				formatted += twoWord;
				ti += 2; // skip ws + second word
				continue;
			}
			if (majorClauses.includes(upper)) {
				const isSubClause = [
					'AND',
					'OR',
					'ON',
					'WHEN',
					'THEN',
					'ELSE',
				].includes(upper);
				if (
					['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'WITH'].includes(
						upper
					)
				) {
					if (formatted.trim()) formatted += '\n';
				} else {
					formatted += `\n${'  '.repeat(isSubClause ? 1 : 0)}`;
				}
				if (SQL_KW.has(upper)) formatted += upper;
				else formatted += t.text;
			} else {
				if (SQL_KW.has(upper) || SQL_FN.has(upper)) formatted += upper;
				else formatted += t.text;
			}
		} else if (t.type === 'string' || t.type === 'comment') {
			formatted += t.text;
		} else if (t.type === 'ws') {
			if (
				formatted.length > 0 &&
				!formatted.endsWith(' ') &&
				!formatted.endsWith('\n')
			) {
				formatted += ' ';
			}
		} else {
			formatted += t.text;
		}
	}
	return formatted.trim();
}

// Saved Queries
function getSavedQueries() {
	try {
		return JSON.parse(localStorage.getItem('lux-saved-queries') || '[]');
	} catch (_e) {
		return [];
	}
}
function setSavedQueries(queries) {
	localStorage.setItem('lux-saved-queries', JSON.stringify(queries));
}

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

function renderSavedQueries() {
	const queries = getSavedQueries();
	const list = $('saved-list');
	if (queries.length === 0) {
		list.innerHTML =
			'<div style="padding:32px;text-align:center;color:var(--text-muted)">No saved queries yet.<br>Write a query and click "Save Current".</div>';
		return;
	}
	let html = '';
	queries.forEach((q, i) => {
		const time = new Date(q.saved_at);
		html += `<div class="history-entry" data-idx="${i}">`;
		html +=
			'<div class="sql-preview" style="font-weight:500;color:var(--text-bright)">' +
			escHtml(q.name) +
			'</div>';
		html +=
			'<div class="sql-preview" style="font-size:11px;margin-top:2px">' +
			escHtml(q.sql.substring(0, 120)) +
			'</div>';
		html += '<div class="history-meta">';
		html += `<span>${time.toLocaleDateString()}</span>`;
		html +=
			'<span style="cursor:pointer;color:var(--error)" data-del="' +
			i +
			'">Delete</span>';
		html += '</div></div>';
	});
	list.innerHTML = html;

	list.querySelectorAll('.history-entry').forEach((el) => {
		el.addEventListener('click', (e) => {
			if (e.target.dataset.del != null) {
				const idx = parseInt(e.target.dataset.del, 10);
				const qs = getSavedQueries();
				qs.splice(idx, 1);
				setSavedQueries(qs);
				renderSavedQueries();
				toast('Query deleted', 'info');
				return;
			}
			const idx = parseInt(el.dataset.idx, 10);
			const qs = getSavedQueries();
			sqlEditor.value = qs[idx].sql;
			syncHighlight();
			releaseFocus($('saved-overlay'));
			$('saved-overlay').classList.remove('open');
			$$('.tab').forEach((t) => t.classList.remove('active'));
			$$('.tab-panel').forEach((p) => p.classList.remove('active'));
			document.querySelector('[data-tab="sql"]').classList.add('active');
			$('panel-sql').classList.add('active');
			sqlEditor.focus();
		});
	});
}

$('saved-overlay').onclick = (e) => {
	if (e.target === $('saved-overlay')) {
		releaseFocus($('saved-overlay'));
		$('saved-overlay').classList.remove('open');
	}
};

// Sidebar Resize
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

// SQL Editor Resize
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

// Keyboard Shortcuts
document.addEventListener('keydown', (e) => {
	// Ctrl+L: focus SQL editor
	if ((e.ctrlKey || e.metaKey) && e.key === 'l') {
		e.preventDefault();
		$$('.tab').forEach((t) => t.classList.remove('active'));
		$$('.tab-panel').forEach((p) => p.classList.remove('active'));
		document.querySelector('[data-tab="sql"]').classList.add('active');
		$('panel-sql').classList.add('active');
		sqlEditor.focus();
	}
	// Ctrl+F: find & replace
	if (
		(e.ctrlKey || e.metaKey) &&
		e.key === 'f' &&
		currentTable &&
		dbConnected
	) {
		if ($('panel-tables').classList.contains('active')) {
			e.preventDefault();
			$('btn-find-replace').click();
		}
	}
	// Escape: close modals in priority order (topmost first)
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
				el.classList.remove('open');
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
		if (editingCell) {
			cancelEdit();
			return;
		}
		if (selectedRows.size > 0) {
			selectedRows.clear();
			lastSelectedRow = -1;
			updateRowSelection();
		} else {
			focusRow = -1;
			focusCol = -1;
			updateCellFocus();
		}
	}
	// ? key: show shortcuts (when not typing)
	if (
		e.key === '?' &&
		!editingCell &&
		document.activeElement.tagName !== 'INPUT' &&
		document.activeElement.tagName !== 'TEXTAREA' &&
		document.activeElement.tagName !== 'SELECT'
	) {
		e.preventDefault();
		$('shortcuts-overlay').classList.add('open');
	}
	// Ctrl+N: new SQL tab
	if ((e.ctrlKey || e.metaKey) && e.key === 'n') {
		e.preventDefault();
		addSqlTab();
		$$('.tab').forEach((t) => t.classList.remove('active'));
		$$('.tab-panel').forEach((p) => p.classList.remove('active'));
		document.querySelector('[data-tab="sql"]').classList.add('active');
		$('panel-sql').classList.add('active');
	}
	// Ctrl+S: save connection
	if ((e.ctrlKey || e.metaKey) && e.key === 's') {
		e.preventDefault();
		if (dbConnected) $('save-conn-btn').click();
	}
	// Grid arrow key navigation
	if (
		!editingCell &&
		$('panel-tables').classList.contains('active') &&
		currentRows.length > 0 &&
		document.activeElement.tagName !== 'INPUT' &&
		document.activeElement.tagName !== 'TEXTAREA'
	) {
		const dCols = displayCols();
		const maxRow = currentRows.length - 1;
		const maxCol = dCols.length - 1;
		if (e.key === 'ArrowDown') {
			e.preventDefault();
			focusRow = Math.min(focusRow + 1, maxRow);
			if (focusCol < 0) focusCol = 0;
			updateCellFocus();
		}
		if (e.key === 'ArrowUp') {
			e.preventDefault();
			focusRow = Math.max(focusRow - 1, 0);
			if (focusCol < 0) focusCol = 0;
			updateCellFocus();
		}
		if (e.key === 'ArrowRight') {
			e.preventDefault();
			focusCol = Math.min(focusCol + 1, maxCol);
			if (focusRow < 0) focusRow = 0;
			updateCellFocus();
		}
		if (e.key === 'ArrowLeft') {
			e.preventDefault();
			focusCol = Math.max(focusCol - 1, 0);
			if (focusRow < 0) focusRow = 0;
			updateCellFocus();
		}
		if (e.key === 'Tab' && focusRow >= 0) {
			e.preventDefault();
			if (e.shiftKey) focusCol = Math.max(focusCol - 1, 0);
			else focusCol = Math.min(focusCol + 1, maxCol);
			updateCellFocus();
		}
		if (e.key === 'Enter' && focusRow >= 0 && focusCol >= 0) {
			e.preventDefault();
			const rows = $('grid-body').querySelectorAll('tr');
			if (focusRow < rows.length) {
				const cells = rows[focusRow].querySelectorAll('td.editable');
				if (focusCol < cells.length) startEdit(cells[focusCol]);
			}
		}
	}
});

// Init
// Deferred until DOMContentLoaded so all script modules (sidebar.js, grid.js, crud.js)
// are fully parsed and their functions are available.
async function init() {
	updateDestructiveButtons(false);
	await loadReadOnly();
	try {
		const data = await fetchJson('/api/schema');
		if (data.tables && data.tables.length > 0) {
			schemaData = data;
			dbConnected = true;
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

// Connection Manager
let savedConnColor = 'blue';
let savedConnsMap = new Map();

async function loadSavedConnections() {
	try {
		const conns = await fetchJson('/api/connections');
		const container = $('saved-conns');
		if (!Array.isArray(conns) || conns.length === 0) {
			container.classList.remove('show');
			savedConnsMap.clear();
			return;
		}
		container.classList.add('show');
		savedConnsMap = new Map();
		const colorMap = {
			green: '#6ee7a0',
			yellow: '#fbbf4e',
			red: '#f87171',
			blue: '#7aa2f7',
			purple: '#a78bfa',
		};
		let html = '';
		conns.forEach((c) => {
			savedConnsMap.set(c.id, c);
			html += `<div class="saved-conn" data-id="${escHtml(String(c.id))}">`;
			html +=
				'<span class="conn-color" style="background:' +
				(colorMap[c.color] || colorMap.blue) +
				'"></span>';
			html += `<span class="conn-name">${escHtml(c.name)}</span>`;
			html +=
				'<span class="conn-del" data-id="' +
				escHtml(String(c.id)) +
				'" title="Delete">&times;</span>';
			html += '</div>';
		});
		container.innerHTML = html;

		container.querySelectorAll('.saved-conn').forEach((el) => {
			el.addEventListener('click', (e) => {
				if (e.target.classList.contains('conn-del')) {
					e.stopPropagation();
					deleteSavedConnection(parseInt(e.target.dataset.id, 10));
					return;
				}
				const connId = parseInt(el.dataset.id, 10);
				const conn = savedConnsMap.get(connId);
				if (conn) {
					$('conn-input').value = conn.conninfo;
					doConnect();
				}
			});
		});
	} catch (_e) {
		/* saved connections unavailable -- non-critical */
	}
}

async function deleteSavedConnection(id) {
	try {
		await fetchJson(`/api/connections/${id}`, { method: 'DELETE' });
		toast('Connection removed', 'info');
		loadSavedConnections();
	} catch (e) {
		toast(`Failed to delete connection: ${e.message}`, 'error');
	}
}

$('save-conn-btn').onclick = () => {
	const conninfo = $('conn-input').value.trim();
	if (!conninfo) {
		toast('Connect first', 'error');
		return;
	}
	$('save-conn-name').value = '';
	savedConnColor = 'blue';
	$('save-conn-colors')
		.querySelectorAll('span')
		.forEach((s) => {
			s.style.borderColor =
				s.dataset.color === 'blue' ? 'var(--text-primary)' : 'transparent';
		});
	$('save-conn-overlay').classList.add('open');
	trapFocus($('save-conn-overlay'));
};

$('save-conn-colors')
	.querySelectorAll('span')
	.forEach((s) => {
		s.addEventListener('click', () => {
			savedConnColor = s.dataset.color;
			$('save-conn-colors')
				.querySelectorAll('span')
				.forEach((x) => (x.style.borderColor = 'transparent'));
			s.style.borderColor = 'var(--text-primary)';
		});
	});

$('save-conn-ok').onclick = async () => {
	const name = $('save-conn-name').value.trim();
	if (!name) {
		toast('Enter a name', 'error');
		return;
	}
	const conninfo = $('conn-input').value.trim();
	try {
		await fetchJson('/api/connections', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ name, conninfo, color: savedConnColor }),
		});
		releaseFocus($('save-conn-overlay'));
		$('save-conn-overlay').classList.remove('open');
		toast('Connection saved', 'success');
		loadSavedConnections();
	} catch (e) {
		toast(`Failed to save: ${e.message}`, 'error');
	}
};

$('save-conn-cancel').onclick = () => {
	releaseFocus($('save-conn-overlay'));
	$('save-conn-overlay').classList.remove('open');
};
$('save-conn-overlay').onclick = (e) => {
	if (e.target === $('save-conn-overlay')) {
		releaseFocus($('save-conn-overlay'));
		$('save-conn-overlay').classList.remove('open');
	}
};

// SQL Tabs
const sqlTabs = [{ id: 0, name: 'Query 1', sql: '', results: '' }];
let activeSqlTab = 0;
let nextSqlTabId = 1;

function renderSqlTabs() {
	const bar = $('sql-tab-bar');
	bar.querySelectorAll('.sql-tab-item').forEach((el) => el.remove());
	const addBtn = $('sql-tab-add');

	sqlTabs.forEach((tab) => {
		const btn = document.createElement('button');
		btn.className = `sql-tab-item${tab.id === activeSqlTab ? ' active' : ''}`;
		btn.dataset.sqlTab = tab.id;
		btn.innerHTML = escHtml(tab.name);
		if (sqlTabs.length > 1) {
			btn.innerHTML += `<span class="close-tab" data-close="${tab.id}">&times;</span>`;
		}
		btn.addEventListener('click', (e) => {
			if (e.target.classList.contains('close-tab')) {
				e.stopPropagation();
				closeSqlTab(parseInt(e.target.dataset.close, 10));
				return;
			}
			switchSqlTab(tab.id);
		});
		bar.insertBefore(btn, addBtn);
	});
}

function switchSqlTab(tabId) {
	// Save current tab state
	const current = sqlTabs.find((t) => t.id === activeSqlTab);
	if (current) {
		current.sql = sqlEditor.value;
		current.results = $('sql-results').innerHTML;
	}
	// Load new tab
	activeSqlTab = tabId;
	const tab = sqlTabs.find((t) => t.id === tabId);
	if (tab) {
		sqlEditor.value = tab.sql;
		syncHighlight();
		$('sql-results').innerHTML = tab.results;
	}
	renderSqlTabs();
}

function addSqlTab() {
	const current = sqlTabs.find((t) => t.id === activeSqlTab);
	if (current) {
		current.sql = sqlEditor.value;
		current.results = $('sql-results').innerHTML;
	}
	const id = nextSqlTabId++;
	sqlTabs.push({
		id,
		name: `Query ${sqlTabs.length + 1}`,
		sql: '',
		results: '',
	});
	activeSqlTab = id;
	sqlEditor.value = '';
	syncHighlight();
	$('sql-results').innerHTML =
		'<div class="sql-results-info" style="padding:48px;text-align:center;color:var(--text-muted)">Write a query and press Run or Ctrl+Enter</div>';
	renderSqlTabs();
	sqlEditor.focus();
}

async function closeSqlTab(tabId) {
	if (sqlTabs.length <= 1) return;
	const idx = sqlTabs.findIndex((t) => t.id === tabId);
	if (idx < 0) return;
	const tab = sqlTabs[idx];
	// Get the live content if this is the active tab
	const tabContent = tabId === activeSqlTab ? sqlEditor.value : tab.sql || '';
	// Warn if tab has unsaved content
	if (tabContent && tabContent.trim().length > 0) {
		const ok = await confirm(
			'Close Tab',
			'Close this tab? Unsaved query will be lost.'
		);
		if (!ok) return;
	}
	sqlTabs.splice(idx, 1);
	if (activeSqlTab === tabId) {
		activeSqlTab = sqlTabs[Math.min(idx, sqlTabs.length - 1)].id;
		const tab = sqlTabs.find((t) => t.id === activeSqlTab);
		if (tab) {
			sqlEditor.value = tab.sql;
			syncHighlight();
			$('sql-results').innerHTML = tab.results;
		}
	}
	renderSqlTabs();
}

$('sql-tab-add').onclick = addSqlTab;

// Column Resizing
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
		if (col) columnWidths[col] = newW;
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

// Table Creation
let createTableColCount = 0;

$('btn-create-table').onclick = () => {
	if (!dbConnected) {
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

$('create-add-col').onclick = addCreateTableCol;

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

$('create-table-preview').onclick = () => {
	const ddl = buildCreateTableDDL();
	if (!ddl) {
		toast('Add table name and columns', 'error');
		return;
	}
	// Show in SQL editor
	sqlEditor.value = ddl.replace(/\\n/g, '\n');
	syncHighlight();
	releaseFocus($('create-table-overlay'));
	$('create-table-overlay').classList.remove('open');
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
	$('create-table-overlay').classList.remove('open');
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
	$('create-table-overlay').classList.remove('open');
};
$('create-table-overlay').onclick = (e) => {
	if (e.target === $('create-table-overlay')) {
		releaseFocus($('create-table-overlay'));
		$('create-table-overlay').classList.remove('open');
	}
};

// Health Check & Reconnect

function startHealthCheck() {
	if (healthCheckInterval) clearInterval(healthCheckInterval);
	healthCheckInterval = setInterval(async () => {
		if (!dbConnected) return;
		try {
			const data = await fetchJson('/api/health');
			if (data.status === 'error' || data.status === 'disconnected') {
				$('reconnect-banner').classList.add('show');
				$('status-dot').classList.remove('ok');
				$('hdr-dot').classList.remove('connected');
			} else {
				$('reconnect-banner').classList.remove('show');
			}
		} catch (_e) {
			$('reconnect-banner').classList.add('show');
		}
	}, HEALTH_CHECK_INTERVAL);
}

$('reconnect-btn').onclick = async () => {
	$('reconnect-btn').textContent = '...';
	$('reconnect-btn').disabled = true;
	try {
		const data = await fetchJson('/api/reconnect', { method: 'POST' });
		if (data.error) {
			toast(data.error, 'error');
		} else {
			$('reconnect-banner').classList.remove('show');
			dbConnected = true;
			updateConnUI();
			await loadSchema();
			toast('Reconnected', 'success');
		}
	} catch (e) {
		toast(`Reconnect failed: ${e.message}`, 'error');
	}
	$('reconnect-btn').textContent = 'Reconnect';
	$('reconnect-btn').disabled = false;
};

// SQL Results Export
async function exportSqlResults(format) {
	const sql = lastSqlQuery;
	if (!sql) {
		toast('No query to export', 'error');
		return;
	}
	try {
		const res = await fetch('/api/sql/export', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ query: sql, format }),
		});
		if (!res.ok) throw new Error(`Export failed: ${res.status}`);
		const blob = await res.blob();
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `export.${format}`;
		document.body.appendChild(a);
		a.click();
		a.remove();
		URL.revokeObjectURL(url);
		toast(`Exported as ${format.toUpperCase()}`, 'success');
	} catch (e) {
		toast(`Export failed: ${e.message}`, 'error');
	}
}

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
	$('import-overlay').classList.remove('open');

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
	$('import-overlay').classList.remove('open');
};
$('import-overlay').onclick = (e) => {
	if (e.target === $('import-overlay')) {
		releaseFocus($('import-overlay'));
		$('import-overlay').classList.remove('open');
	}
};

// Connection Color Stripe
function setConnStripe(color) {
	const stripe = $('conn-stripe');
	stripe.className = 'conn-stripe';
	if (color) {
		stripe.classList.add(color);
	}
}
// Auto-detect color from conninfo
function detectConnColor(conninfo) {
	const ci = (conninfo || '').toLowerCase();
	if (ci.includes('prod') || ci.includes('production')) return 'red';
	if (ci.includes('stag') || ci.includes('staging') || ci.includes('preprod'))
		return 'yellow';
	if (
		ci.includes('local') ||
		ci.includes('127.0.0.1') ||
		ci.includes('localhost') ||
		ci.includes('dev')
	)
		return 'green';
	return 'blue';
}

function getSavedConnColor(conninfo) {
	for (const conn of savedConnsMap.values()) {
		if (conn.conninfo === conninfo) {
			return conn.color || 'blue';
		}
	}
	return null;
}

// Command Palette (Ctrl+K)
let cmdIndex = 0;
let cmdItems = [];

function openCmdPalette() {
	$('cmd-overlay').classList.add('open');
	$('cmd-input').value = '';
	$('cmd-input').focus();
	buildCmdResults('');
}

function closeCmdPalette() {
	$('cmd-overlay').classList.remove('open');
}

function buildCmdResults(query) {
	const q = query.toLowerCase().trim();
	const results = [];
	const tblIcon =
		'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="3" x2="9" y2="21"/></svg>';
	const actionIcon =
		'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>';

	// Tables
	if (schemaData?.tables) {
		schemaData.tables.forEach((t) => {
			if (!q || t.name.toLowerCase().includes(q)) {
				results.push({
					icon: tblIcon,
					label: t.name,
					hint: `${t.columns ? t.columns.length : 0} cols`,
					action: () => {
						selectTableFromCmd(t.name);
						closeCmdPalette();
					},
				});
			}
			// Also search columns
			if (q && t.columns) {
				t.columns.forEach((c) => {
					if (c.name.toLowerCase().includes(q)) {
						results.push({
							icon: tblIcon,
							label: `${t.name}.${c.name}`,
							hint: c.type || '',
							action: () => {
								selectTableFromCmd(t.name);
								closeCmdPalette();
							},
						});
					}
				});
			}
		});
	}

	// Actions
	const actions = [
		{
			label: 'New SQL Tab',
			hint: 'Ctrl+N',
			action: () => {
				addSqlTab();
				switchToSqlTab();
				closeCmdPalette();
			},
		},
		{
			label: 'Toggle Read-Only',
			hint: '',
			action: () => {
				$('btn-readonly').click();
				closeCmdPalette();
			},
		},
		{
			label: 'Toggle Theme',
			hint: '',
			action: () => {
				$('btn-theme').click();
				closeCmdPalette();
			},
		},
		{
			label: 'Refresh Schema',
			hint: '',
			action: () => {
				$('btn-refresh').click();
				closeCmdPalette();
			},
		},
		{
			label: 'Query History',
			hint: '',
			action: () => {
				$('btn-sql-history').click();
				closeCmdPalette();
			},
		},
		{
			label: 'Keyboard Shortcuts',
			hint: '?',
			action: () => {
				$('shortcuts-overlay').classList.add('open');
				closeCmdPalette();
			},
		},
		{
			label: 'ER Diagram',
			hint: '',
			action: () => {
				switchToTab('er');
				closeCmdPalette();
			},
		},
		{
			label: 'Change Journal',
			hint: '',
			action: () => {
				switchToTab('journal');
				closeCmdPalette();
			},
		},
	];
	actions.forEach((a) => {
		if (!q || a.label.toLowerCase().includes(q)) {
			results.push({
				icon: actionIcon,
				label: a.label,
				hint: a.hint,
				action: a.action,
			});
		}
	});

	cmdItems = results;
	cmdIndex = 0;
	renderCmdResults();
}

function selectTableFromCmd(name) {
	// Switch to tables tab and select the table
	switchToTab('tables');
	const tree = $('schema-tree');
	tree.querySelectorAll('.tree-table').forEach((el) => {
		el.classList.remove('active');
		if (el.dataset.table === name) el.classList.add('active');
	});
	selectTable(name);
}

function switchToTab(tabName) {
	$$('.tab').forEach((t) => t.classList.remove('active'));
	$$('.tab-panel').forEach((p) => p.classList.remove('active'));
	const tab = document.querySelector(`[data-tab="${tabName}"]`);
	if (tab) tab.classList.add('active');
	$(`panel-${tabName}`).classList.add('active');
	if (tabName === 'journal') {
		loadJournal();
		clearJournalBadge();
	}
	if (tabName === 'er') drawER();
}

function switchToSqlTab() {
	switchToTab('sql');
	sqlEditor.focus();
}

function renderCmdResults() {
	const container = $('cmd-results');
	if (cmdItems.length === 0) {
		container.innerHTML = '<div class="cmd-empty">No results found</div>';
		return;
	}
	container.innerHTML = cmdItems
		.slice(0, 20)
		.map(
			(item, i) =>
				'<div class="cmd-item' +
				(i === cmdIndex ? ' selected' : '') +
				'" data-idx="' +
				i +
				'">' +
				// item.icon is always a hardcoded SVG string, never user data
				'<div class="cmd-icon">' +
				item.icon +
				'</div>' +
				'<span class="cmd-label">' +
				escHtml(item.label) +
				'</span>' +
				'<span class="cmd-hint">' +
				escHtml(item.hint) +
				'</span>' +
				'</div>'
		)
		.join('');

	container.querySelectorAll('.cmd-item').forEach((el) => {
		el.addEventListener('click', () => {
			const idx = parseInt(el.dataset.idx, 10);
			if (cmdItems[idx]) cmdItems[idx].action();
		});
		el.addEventListener('mousemove', () => {
			cmdIndex = parseInt(el.dataset.idx, 10);
			container.querySelectorAll('.cmd-item').forEach((item, i) => {
				item.classList.toggle('selected', i === cmdIndex);
			});
		});
	});

	// Scroll selected into view
	const selected = container.querySelector('.cmd-item.selected');
	if (selected) selected.scrollIntoView({ block: 'nearest' });
}

$('cmd-input').addEventListener('input', () =>
	buildCmdResults($('cmd-input').value)
);
$('cmd-input').addEventListener('keydown', (e) => {
	if (e.key === 'ArrowDown') {
		e.preventDefault();
		cmdIndex = Math.min(cmdIndex + 1, Math.min(cmdItems.length - 1, 19));
		renderCmdResults();
	}
	if (e.key === 'ArrowUp') {
		e.preventDefault();
		cmdIndex = Math.max(cmdIndex - 1, 0);
		renderCmdResults();
	}
	if (e.key === 'Enter' && cmdItems.length > 0) {
		e.preventDefault();
		cmdItems[cmdIndex].action();
	}
	if (e.key === 'Escape') {
		e.preventDefault();
		closeCmdPalette();
	}
});
$('cmd-overlay').addEventListener('click', (e) => {
	if (e.target === $('cmd-overlay')) closeCmdPalette();
});
$('btn-cmd-palette').onclick = openCmdPalette;

// Ctrl+K global shortcut
document.addEventListener('keydown', (e) => {
	if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
		e.preventDefault();
		if ($('cmd-overlay').classList.contains('open')) closeCmdPalette();
		else openCmdPalette();
	}
});

// Wire up modal close buttons (delegated from HTML class="modal-close-btn")
document.querySelectorAll('.modal-close-btn').forEach((btn) => {
	btn.addEventListener('click', () => {
		const overlay = btn.closest('.modal-overlay');
		if (overlay) {
			releaseFocus(overlay);
			overlay.classList.remove('open');
		}
	});
});

// Mobile sidebar toggle
(() => {
	const toggle = $('sidebar-toggle');
	const sidebar = document.querySelector('.sidebar');
	if (toggle && sidebar) {
		toggle.addEventListener('click', () => {
			sidebar.classList.toggle('open');
		});
		// Close sidebar when clicking outside on mobile
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
