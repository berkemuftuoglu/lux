// sql.js — SQL editor, syntax highlighting, autocomplete, run/render, format, export
// Loaded after state.js, utils.js, and sql-tabs.js, before app.js.

const sqlEditor = $('sql-editor');
const sqlHighlight = $('sql-highlight');

const acDropdown = $('sql-autocomplete');
let acItems = [];
let acIndex = -1;
let acTimer = null;

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

	const danger = detectDangerousSQL(sql);
	if (danger) {
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
	lastSqlQuery = sqlEditor.value.trim();
}

function formatSQL(sql) {
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

	let formatted = '';
	for (let ti = 0; ti < tokens.length; ti++) {
		const t = tokens[ti];
		if (t.type === 'word') {
			const upper = t.text.toUpperCase();
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

$('btn-run-sql').onclick = runSQL;
sqlEditor.addEventListener('keydown', (e) => {
	if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
		e.preventDefault();
		hideAC();
		runSQL();
	}
});

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

$('btn-format-sql').onclick = () => {
	const sql = sqlEditor.value.trim();
	if (!sql) return;
	sqlEditor.value = formatSQL(sql);
	syncHighlight();
	toast('SQL formatted', 'success');
};

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
		list.querySelectorAll('.history-entry').forEach((el, i) => {
			el.addEventListener('click', () => {
				sqlEditor.value = entries[i].sql;
				syncHighlight();
				releaseFocus($('history-overlay'));
				closeModal('history-overlay');
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
		closeModal('history-overlay');
	}
};
