// cmd-palette.js — Command palette (Ctrl+K): fuzzy search for tables, columns, and actions

let cmdIndex = 0;
let cmdItems = [];

function openCmdPalette() {
	openModal('cmd-overlay');
	$('cmd-input').value = '';
	$('cmd-input').focus();
	buildCmdResults('');
}

function closeCmdPalette() {
	closeModal('cmd-overlay');
}

function buildCmdResults(query) {
	const q = query.toLowerCase().trim();
	const results = [];
	const tblIcon =
		'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="3" x2="9" y2="21"/></svg>';
	const actionIcon =
		'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>';

	if (State.schemaData?.tables) {
		State.schemaData.tables.forEach((t) => {
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
				openModal('shortcuts-overlay');
				closeCmdPalette();
			},
		},
		{
			label: 'ER Diagram',
			hint: '',
			action: () => {
				switchTab('er');
				closeCmdPalette();
			},
		},
		{
			label: 'Change Journal',
			hint: '',
			action: () => {
				switchTab('journal');
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
	switchTab('tables');
	const tree = $('schema-tree');
	tree.querySelectorAll('.tree-table').forEach((el) => {
		el.classList.remove('active');
		if (el.dataset.table === name) el.classList.add('active');
	});
	selectTable(name);
}

function switchToSqlTab() {
	switchTab('sql');
	sqlEditor.focus();
}

function makeSvgIcon(svgString) {
	const wrap = h('div');
	wrap.innerHTML = svgString;
	return wrap.firstElementChild;
}

function renderCmdResults() {
	const container = $('cmd-results');
	if (cmdItems.length === 0) {
		container.textContent = '';
		container.appendChild(h('div', { cls: 'cmd-empty', text: 'No results found' }));
		return;
	}
	container.textContent = '';
	cmdItems.slice(0, 20).forEach((item, i) => {
		const iconDiv = h('div', { cls: 'cmd-icon' }, makeSvgIcon(item.icon));
		container.appendChild(
			h('div', {
				cls: 'cmd-item' + (i === cmdIndex ? ' selected' : ''),
				'data-idx': String(i),
			},
				iconDiv,
				h('span', { cls: 'cmd-label', text: item.label }),
				h('span', { cls: 'cmd-hint', text: item.hint })
			)
		);
	});

	const selected = container.querySelector('.cmd-item.selected');
	if (selected) selected.scrollIntoView({ block: 'nearest' });
}

delegate($('cmd-results'), '.cmd-item', 'click', function () {
	const idx = parseInt(this.dataset.idx, 10);
	if (cmdItems[idx]) cmdItems[idx].action();
});
delegate($('cmd-results'), '.cmd-item', 'mousemove', function () {
	const newIdx = parseInt(this.dataset.idx, 10);
	if (newIdx === cmdIndex) return;
	cmdIndex = newIdx;
	$('cmd-results').querySelectorAll('.cmd-item').forEach((item, i) => {
		item.classList.toggle('selected', i === cmdIndex);
	});
});

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

document.addEventListener('keydown', (e) => {
	if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
		e.preventDefault();
		if ($('cmd-overlay').classList.contains('open')) closeCmdPalette();
		else openCmdPalette();
	}
});
