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
			closeModal('saved-overlay');
			$$('.tab').forEach((t) => t.classList.remove('active'));
			$$('.tab-panel').forEach((p) => p.classList.remove('active'));
			document.querySelector('[data-tab="sql"]').classList.add('active');
			$('panel-sql').classList.add('active');
			sqlEditor.focus();
		});
	});
}
