// sql-format.js — SQL formatter (tokenize + reformat)
// Loaded before sql.js. Uses SQL_KW and SQL_FN globals defined in sql.js
// (safe because formatSQL is only called at runtime, not at load time).

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
