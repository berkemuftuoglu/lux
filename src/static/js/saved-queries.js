// saved-queries.js — Saved query CRUD (localStorage-backed)
// Loaded after state.js and utils.js, before app.js.

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

function renderSavedQueries() {
	const queries = getSavedQueries();
	const list = $('saved-list');
	if (queries.length === 0) {
		list.textContent = '';
		list.appendChild(h('div', {
			style: 'padding:32px;text-align:center;color:var(--text-muted)',
			text: 'No saved queries yet.\nWrite a query and click "Save Current".'
		}));
		return;
	}
	list.textContent = '';
	queries.forEach((q, i) => {
		const time = new Date(q.saved_at);
		list.appendChild(
			h('div', { cls: 'history-entry', 'data-idx': String(i) },
				h('div', { cls: 'sql-preview', style: 'font-weight:500;color:var(--text-bright)', text: q.name }),
				h('div', { cls: 'sql-preview', style: 'font-size:11px;margin-top:2px', text: q.sql.substring(0, 120) }),
				h('div', { cls: 'history-meta' },
					h('span', { text: time.toLocaleDateString() }),
					h('span', { style: 'cursor:pointer;color:var(--error)', 'data-del': String(i), text: 'Delete' })
				)
			)
		);
	});
}

delegate($('saved-list'), '.history-entry', 'click', function (e) {
	if (e.target.dataset.del != null) {
		const idx = parseInt(e.target.dataset.del, 10);
		const queries = getSavedQueries();
		queries.splice(idx, 1);
		setSavedQueries(queries);
		renderSavedQueries();
		toast('Query deleted', 'info');
		return;
	}
	const idx = parseInt(this.dataset.idx, 10);
	const queries = getSavedQueries();
	sqlEditor.value = queries[idx].sql;
	syncHighlight();
	releaseFocus($('saved-overlay'));
	closeModal('saved-overlay');
	switchTab('sql');
	sqlEditor.focus();
});
