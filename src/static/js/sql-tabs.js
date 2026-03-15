// sql-tabs.js — SQL tab management: create, switch, close tabs
// Loaded after state.js and utils.js, before sql.js (sql.js references tab state).

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
	const current = sqlTabs.find((t) => t.id === activeSqlTab);
	if (current) {
		current.sql = sqlEditor.value;
		current.results = $('sql-results').innerHTML;
	}
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
	const tabContent = tabId === activeSqlTab ? sqlEditor.value : tab.sql || '';
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
		const nextTab = sqlTabs.find((t) => t.id === activeSqlTab);
		if (nextTab) {
			sqlEditor.value = nextTab.sql;
			syncHighlight();
			$('sql-results').innerHTML = nextTab.results;
		}
	}
	renderSqlTabs();
}
