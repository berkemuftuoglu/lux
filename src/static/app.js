/* ═══════════════════════════════════════════════════════════════
   LUX — Client Logic
   ═══════════════════════════════════════════════════════════════ */
(function() {
'use strict';

// ── Constants ──
const TOAST_DURATION = 3500;
const TOAST_HOVER_DURATION = 2000;
const HEALTH_CHECK_INTERVAL = 30000;
const AC_LINE_HEIGHT = 23;
const AC_CHAR_WIDTH = 8.4;

// ── Helpers ──
const $ = id => document.getElementById(id);
const $$ = sel => document.querySelectorAll(sel);
function escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function prettyName(name) { return name.replace(/_/g, ' ').replace(/\bid\b/gi, 'ID'); }
function copyToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    return navigator.clipboard.writeText(text);
  }
  // Fallback for insecure contexts (HTTP)
  const ta = document.createElement('textarea');
  ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
  document.body.appendChild(ta); ta.select();
  try { document.execCommand('copy'); } catch(e) { /* best effort */ }
  ta.remove();
  return Promise.resolve();
}

async function fetchJson(url, opts) {
  const res = await fetch(url, opts);
  if (!res.ok) {
    let msg = 'Server error: ' + res.status;
    try { const body = await res.json(); if (body.error) msg = body.error; } catch(e) { /* no JSON body */ }
    throw new Error(msg);
  }
  return res.json();
}

// ── Theme Toggle ──
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
else if (window.matchMedia('(prefers-color-scheme: light)').matches) setTheme('light');
else setTheme('dark');

// ── State ──
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
let erPanX = 0, erPanY = 0;
let focusRow = -1, focusCol = -1;
let columnFilters = {};
let columnWidths = {};
let selectedRows = new Set();
let lastSelectedRow = -1;
let lastSqlQuery = '';

// ── Toast ──
function toast(msg, type) {
  const el = document.createElement('div');
  el.className = 'toast ' + (type || 'info');
  el.textContent = msg;
  $('toast-container').appendChild(el);
  let timer = setTimeout(() => { el.classList.add('hiding'); setTimeout(() => el.remove(), 200); }, TOAST_DURATION);
  el.addEventListener('mouseenter', () => clearTimeout(timer));
  el.addEventListener('mouseleave', () => { timer = setTimeout(() => { el.classList.add('hiding'); setTimeout(() => el.remove(), 200); }, TOAST_HOVER_DURATION); });
}

// ── Loading Spinner ──
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

// ── Confirm Dialog ──
let confirmResolve = null;
function confirm(title, body) {
  return new Promise(resolve => {
    $('confirm-title').textContent = title;
    $('confirm-body').textContent = body;
    $('confirm-overlay').classList.add('open');
    trapFocus($('confirm-overlay'));
    confirmResolve = resolve;
  });
}
$('confirm-ok').onclick = () => { $('confirm-overlay').classList.remove('open'); releaseFocus($('confirm-overlay')); if (confirmResolve) confirmResolve(true); };
$('confirm-cancel').onclick = () => { $('confirm-overlay').classList.remove('open'); releaseFocus($('confirm-overlay')); if (confirmResolve) confirmResolve(false); };

let promptResolve = null;
function promptUser(title, defaultVal) {
  return new Promise(resolve => {
    $('prompt-title').textContent = title;
    $('prompt-input').value = defaultVal || '';
    $('prompt-overlay').classList.add('open');
    trapFocus($('prompt-overlay'));
    promptResolve = resolve;
    setTimeout(() => $('prompt-input').focus(), 50);
  });
}
$('prompt-ok').onclick = () => { $('prompt-overlay').classList.remove('open'); releaseFocus($('prompt-overlay')); if (promptResolve) promptResolve($('prompt-input').value); };
$('prompt-cancel').onclick = () => { $('prompt-overlay').classList.remove('open'); releaseFocus($('prompt-overlay')); if (promptResolve) promptResolve(null); };
$('prompt-input').addEventListener('keydown', e => { if (e.key === 'Enter') $('prompt-ok').click(); });
$('prompt-overlay').onclick = e => { if (e.target === $('prompt-overlay')) $('prompt-cancel').click(); };

// ── Focus Trap ──
function trapFocus(modal) {
  const focusable = modal.querySelectorAll('button, input, textarea, select, [tabindex]:not([tabindex="-1"])');
  if (!focusable.length) return;
  const first = focusable[0], last = focusable[focusable.length - 1];
  function handler(e) {
    if (e.key !== 'Tab') return;
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  }
  modal.addEventListener('keydown', handler);
  modal._focusTrap = handler;
  first.focus();
}
function releaseFocus(modal) {
  if (modal._focusTrap) modal.removeEventListener('keydown', modal._focusTrap);
}

// ── Tabs ──
$$('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    $$('.tab').forEach(t => { t.classList.remove('active'); t.setAttribute('aria-selected', 'false'); });
    $$('.tab-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    tab.setAttribute('aria-selected', 'true');
    $('panel-' + tab.dataset.tab).classList.add('active');
    if (tab.dataset.tab === 'journal') { loadJournal(); clearJournalBadge(); }
    if (tab.dataset.tab === 'er') drawER();
  });
});

// ── Connection ──
$('conn-btn').onclick = doConnect;
$('conn-input').addEventListener('keydown', e => { if (e.key === 'Enter') doConnect(); });

async function doConnect() {
  const conninfo = $('conn-input').value.trim();
  if (!conninfo) return;
  $('conn-btn').disabled = true;
  $('conn-btn').textContent = '...';
  try {
    const data = await fetchJson('/api/connect', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({conninfo})
    });
    if (data.error) {
      showConnStatus(false, data.error);
      toast(data.error, 'error');
    } else {
      dbConnected = true;
      showConnStatus(true, data.tables + ' tables');
      updateConnUI();
      await loadSchema();
      toast('Connected', 'success');
      startHealthCheck();
      // Set connection stripe color
      const connStr = $('conn-input').value.trim();
      const savedColor = getSavedConnColor(connStr);
      setConnStripe(savedColor || detectConnColor(connStr));
    }
  } catch(e) { showConnStatus(false, e.message); toast('Connection failed', 'error'); }
  $('conn-btn').disabled = false;
  $('conn-btn').textContent = 'Connect';
}

function showConnStatus(ok, msg) {
  $('conn-status').style.display = 'flex';
  $('conn-dot').className = 'dot ' + (ok ? 'ok' : 'err');
  $('conn-msg').textContent = msg;
}
function updateConnUI() {
  $('hdr-dot').classList.toggle('connected', dbConnected);
  $('status-dot').classList.toggle('ok', dbConnected);
  $('status-conn').textContent = dbConnected ? 'Connected' : 'Not connected';
  // Show/hide save connection button
  $('save-conn-btn').style.display = dbConnected ? '' : 'none';
}

// ── Schema ──
async function loadSchema() {
  try {
    schemaData = await fetchJson('/api/schema');
    renderSidebar();
  } catch(e) { toast('Failed to load schema', 'error'); }
}

function renderSidebar() {
  const tree = $('schema-tree');
  $('sidebar-empty').style.display = 'none';
  tree.querySelectorAll('.tree-table, .tree-columns').forEach(el => el.remove());

  if (!schemaData || !schemaData.tables || schemaData.tables.length === 0) {
    $('sidebar-empty').style.display = '';
    $('table-search-wrap').style.display = 'none';
    $('table-count').textContent = '';
    return;
  }

  $('table-search-wrap').style.display = '';
  $('table-count').textContent = '(' + schemaData.tables.length + ')';
  const searchTerm = ($('table-search').value || '').toLowerCase();

  schemaData.tables.forEach(table => {
    const item = document.createElement('div');
    item.className = 'tree-table';
    item.dataset.table = table.name;
    const colCount = table.columns ? table.columns.length : 0;
    item.innerHTML =
      '<span class="chevron">&#9654;</span>' +
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="3" x2="9" y2="21"/></svg>' +
      '<span class="name">' + escHtml(prettyName(table.name)) + '</span>' +
      '<span class="count">' + colCount + '</span>';

    const cols = document.createElement('div');
    cols.className = 'tree-columns';
    (table.columns || []).forEach(col => {
      const c = document.createElement('div');
      c.className = 'tree-col';
      let badges = '';
      if (col.is_primary_key) badges += '<span class="badge pk">PK</span>';
      if (col.fk_target_table) badges += '<span class="badge fk">FK</span>';
      c.innerHTML = '<span class="col-name">' + escHtml(prettyName(col.name)) + '</span>' +
                    badges +
                    '<span class="col-type">' + escHtml(col.type || '') + '</span>';
      cols.appendChild(c);
    });

    // Hide if doesn't match search
    if (searchTerm && !table.name.toLowerCase().includes(searchTerm)) {
      item.style.display = 'none';
      cols.style.display = 'none';
    }

    item.addEventListener('click', (e) => {
      e.stopPropagation();
      const wasActive = item.classList.contains('active');
      item.classList.toggle('expanded');
      cols.classList.toggle('open');
      if (!wasActive) {
        tree.querySelectorAll('.tree-table').forEach(t => t.classList.remove('active'));
        item.classList.add('active');
        selectTable(table.name);
      }
    });

    tree.appendChild(item);
    tree.appendChild(cols);
  });
}

// Table search handler
$('table-search').addEventListener('input', () => {
  const term = $('table-search').value.toLowerCase();
  $('schema-tree').querySelectorAll('.tree-table').forEach(el => {
    const name = (el.dataset.table || '').toLowerCase();
    const match = !term || name.includes(term);
    el.style.display = match ? '' : 'none';
    // Also hide the columns div right after
    if (el.nextElementSibling && el.nextElementSibling.classList.contains('tree-columns')) {
      el.nextElementSibling.style.display = match ? '' : 'none';
    }
  });
});

// ── Table Selection & Data ──
function updateDestructiveButtons(enabled) {
  const ids = ['btn-truncate', 'btn-import-csv', 'btn-export-csv', 'btn-export-json'];
  ids.forEach(id => {
    const el = $(id);
    if (el) el.disabled = !enabled;
  });
}

function selectTable(name) {
  currentTable = name;
  pageOffset = 0;
  sortCol = null;
  sortDir = 'asc';
  columnFilters = {};
  focusRow = -1; focusCol = -1;
  selectedRows = new Set();
  lastSelectedRow = -1;
  $('table-toolbar').style.display = 'flex';
  $('tbl-name').textContent = name;
  updateDestructiveButtons(true);
  loadTableData();
}

async function loadTableData() {
  if (!currentTable) return;
  let url = '/api/tables/' + encodeURIComponent(currentTable) + '/data?limit=' + pageLimit + '&offset=' + pageOffset + '&count=exact';
  if (sortCol) url += '&sort=' + encodeURIComponent(sortCol) + '&dir=' + sortDir;
  Object.keys(columnFilters).forEach(col => {
    if (columnFilters[col]) url += '&f.' + encodeURIComponent(col) + '=' + encodeURIComponent(columnFilters[col]);
  });

  showLoading('grid-wrap');
  try {
    const data = await fetchJson(url);
    hideLoading('grid-wrap');
    if (data.error) { toast(data.error, 'error'); return; }

    currentColumns = data.columns || [];
    currentRows = data.rows || [];
    totalRows = data.total || 0;
    currentPkMode = data.pk_mode || 'column';
    currentPkColumns = data.pk_columns || [];

    $('tbl-info').textContent = totalRows.toLocaleString() + ' rows';
    $('status-table').textContent = currentTable + ' · ' + totalRows.toLocaleString() + ' rows';

    renderGrid();
    renderPagination();
    updateBulkBar();
  } catch(e) { hideLoading('grid-wrap'); toast('Failed to load data', 'error'); }
}

function getTableMeta(name) {
  if (!schemaData || !schemaData.tables) return null;
  return schemaData.tables.find(t => t.name === name) || null;
}

function isNumericType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return t.includes('int') || t.includes('numeric') || t.includes('decimal') ||
         t.includes('float') || t.includes('double') || t.includes('real') ||
         t === 'money' || t === 'serial' || t === 'bigserial';
}

function isBoolType(type) {
  return type && type.toLowerCase() === 'boolean';
}
function isDateType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return t.includes('date') || t.includes('timestamp') || t.includes('time') || t.includes('interval');
}
function isJsonType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return t === 'json' || t === 'jsonb';
}
function isUuidType(type) {
  return type && type.toLowerCase() === 'uuid';
}
function isInetType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return t === 'inet' || t === 'cidr' || t === 'macaddr';
}
function isArrayType(type) {
  if (!type) return false;
  return type.includes('[]') || type.toLowerCase().startsWith('array');
}
function isEnumType(type, colMeta) {
  if (!type) return false;
  if (colMeta && colMeta.enum_values && colMeta.enum_values.length > 0) return true;
  return type.toLowerCase() === 'user-defined';
}
// Map enum values to semantic color classes for pill rendering
function enumPillClass(val) {
  const v = val.toLowerCase();
  // Status-like: done/complete/closed/resolved → success
  if (/done|complete|closed|resolved|active|enabled/.test(v)) return 'pill-success';
  // In-progress/open → accent
  if (/in.progress|in_progress|open|started|running|pending/.test(v)) return 'pill-accent';
  // Cancelled/blocked/failed/disabled → error
  if (/cancel|blocked|failed|disabled|rejected|archived/.test(v)) return 'pill-error';
  // Priority: urgent/critical → error, high → warning, medium → accent, low/none → muted
  if (/urgent|critical/.test(v)) return 'pill-error';
  if (/high/.test(v)) return 'pill-warning';
  if (/medium/.test(v)) return 'pill-accent';
  if (/low|none|trivial/.test(v)) return 'pill-muted';
  // Todo/backlog → subtle
  if (/todo|backlog|draft|review|planned/.test(v)) return 'pill-subtle';
  return 'pill-default';
}

function displayCols() { return currentPkMode === 'ctid' ? currentColumns.filter(c => c !== 'ctid') : currentColumns; }

function renderGrid() {
  const meta = getTableMeta(currentTable);
  const colMeta = {};
  if (meta) meta.columns.forEach(c => colMeta[c.name] = c);

  const pkSet = new Set(currentPkColumns);
  const cols = displayCols();

  let headHtml = '<tr><th class="row-num-col">#</th>';
  cols.forEach(col => {
    const sorted = sortCol === col;
    const arrow = sorted ? (sortDir === 'asc' ? ' &#9650;' : ' &#9660;') : ' <span class="sort-arrow">&#9650;</span>';
    const thStyle = columnWidths[col] ? ' style="width:' + columnWidths[col] + 'px;min-width:' + columnWidths[col] + 'px;max-width:' + columnWidths[col] + 'px"' : '';
    headHtml += '<th class="' + (sorted ? 'sorted' : '') + '" data-col="' + escHtml(col) + '"' + thStyle + '>' +
                escHtml(col) + arrow + '<div class="col-resize-handle"></div></th>';
  });
  headHtml += '<th class="row-actions-col"></th></tr>';

  // Filter row
  headHtml += '<tr class="filter-row"><th></th>';
  cols.forEach(col => {
    const val = columnFilters[col] || '';
    headHtml += '<th><input class="col-filter" data-col="' + escHtml(col) + '" placeholder="Filter..." value="' + escHtml(val) + '"></th>';
  });
  headHtml += '<th></th></tr>';
  $('grid-head').innerHTML = headHtml;

  $('grid-head').querySelectorAll('th[data-col]').forEach(th => {
    th.onclick = () => {
      const col = th.dataset.col;
      if (sortCol === col) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
      else { sortCol = col; sortDir = 'asc'; }
      loadTableData();
    };
  });

  // Column filter handlers
  let filterTimer = null;
  $('grid-head').querySelectorAll('.col-filter').forEach(input => {
    input.onclick = e => e.stopPropagation();
    input.addEventListener('input', () => {
      columnFilters[input.dataset.col] = input.value;
      clearTimeout(filterTimer);
      filterTimer = setTimeout(() => { pageOffset = 0; loadTableData(); }, 400);
    });
  });

  if (currentRows.length === 0) {
    $('data-grid').classList.add('hidden');
    $('table-empty').style.display = '';
    $('table-empty').innerHTML = '<div class="grid-empty" style="display:flex;flex-direction:column;align-items:center"><svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/></svg><div>No rows in ' + escHtml(currentTable) + '</div></div>';
    $('pagination').classList.add('hidden');
    return;
  }

  $('table-empty').style.display = 'none';
  $('data-grid').classList.remove('hidden');

  let bodyHtml = '';
  currentRows.forEach((row, ri) => {
    const isSelected = selectedRows.has(ri);
    bodyHtml += '<tr' + (isSelected ? ' class="selected"' : '') + ' data-ridx="' + ri + '">';
    bodyHtml += '<td class="row-num" data-ridx="' + ri + '" title="Click for details, Shift+Click to select">' + (pageOffset + ri + 1) + '</td>';
    cols.forEach((col, ci) => {
      const dataIdx = currentColumns.indexOf(col);
      const val = row[dataIdx];
      const cm = colMeta[col];
      const type = cm ? (cm.type || '') : '';
      const isPk = pkSet.has(col) || (cm && cm.is_primary_key);

      let cls = 'editable';
      let content;

      if (val === null || val === 'NULL' || val === undefined) {
        cls += ' null-val';
        content = 'NULL';
      } else if (isBoolType(type)) {
        cls += ' bool-val ' + (val === 't' || val === 'true' ? 't' : 'f');
        content = val === 't' || val === 'true' ? 'true' : 'false';
      } else if (isNumericType(type)) {
        cls += ' num-val type-int';
        content = escHtml(val);
      } else if (isDateType(type)) {
        cls += ' type-date';
        content = escHtml(val);
      } else if (isJsonType(type)) {
        cls += ' type-json';
        content = escHtml(val);
      } else if (isUuidType(type)) {
        cls += ' type-uuid';
        content = escHtml(val);
      } else if (isInetType(type)) {
        cls += ' type-inet';
        content = escHtml(val);
      } else if (isArrayType(type)) {
        cls += ' type-array';
        content = escHtml(val);
      } else if (isEnumType(type, cm)) {
        cls += ' type-enum';
        const ev = escHtml(val);
        const enumCls = enumPillClass(ev);
        content = '<span class="enum-pill ' + enumCls + '">' + ev + '</span>';
      } else {
        content = escHtml(val);
      }
      if (isPk) cls += ' pk-val';

      bodyHtml += '<td class="' + cls + '" data-row="' + ri + '" data-col="' + escHtml(col) + '" data-idx="' + dataIdx + '" title="' + escHtml(val || '') + '">' + content + '</td>';
    });
    bodyHtml += '<td class="row-actions"><button class="del-btn" data-row="' + ri + '" title="Delete row">&times;</button></td>';
    bodyHtml += '</tr>';
  });
  $('grid-body').innerHTML = bodyHtml;

  $('grid-body').querySelectorAll('td.editable').forEach(td => {
    td.addEventListener('dblclick', () => startEdit(td));
    // Single click: copy to clipboard and focus cell
    td.addEventListener('click', (e) => {
      if (td.classList.contains('editing')) return;
      const val = td.textContent;
      if (val && val !== 'NULL') {
        copyToClipboard(val).then(() => {
          const tip = document.createElement('div');
          tip.className = 'copy-toast';
          tip.textContent = 'Copied';
          tip.style.left = e.clientX + 'px';
          tip.style.top = (e.clientY - 30) + 'px';
          document.body.appendChild(tip);
          setTimeout(() => tip.remove(), 800);
        }).catch(() => { /* clipboard not available in insecure context */ });
      }
      // Set focus
      focusRow = parseInt(td.dataset.row);
      focusCol = Array.from(td.parentElement.querySelectorAll('td.editable')).indexOf(td);
      updateCellFocus();
    });
  });

  $('grid-body').querySelectorAll('.del-btn').forEach(btn => {
    btn.addEventListener('click', () => deleteRow(parseInt(btn.dataset.row)));
  });

  // Row number click: detail view or multi-select
  $('grid-body').querySelectorAll('td.row-num').forEach(td => {
    td.addEventListener('click', (e) => {
      const ri = parseInt(td.dataset.ridx);
      if (e.shiftKey && lastSelectedRow >= 0) {
        const from = Math.min(lastSelectedRow, ri);
        const to = Math.max(lastSelectedRow, ri);
        for (let i = from; i <= to; i++) selectedRows.add(i);
        updateRowSelection();
      } else if (e.ctrlKey || e.metaKey) {
        if (selectedRows.has(ri)) selectedRows.delete(ri);
        else selectedRows.add(ri);
        lastSelectedRow = ri;
        updateRowSelection();
      } else {
        showRowDetail(ri);
      }
    });
  });

  // Context menu on cells
  $('grid-body').addEventListener('contextmenu', (e) => {
    const td = e.target.closest('td.editable');
    const tr = e.target.closest('tr');
    if (!td || !tr) return;
    e.preventDefault();
    const ri = parseInt(td.dataset.row);
    const col = td.dataset.col;
    showContextMenu(e.clientX, e.clientY, ri, col, td);
  });

  // Restore focus if any
  if (focusRow >= 0 && focusCol >= 0) updateCellFocus();
}

function updateCellFocus() {
  $('grid-body').querySelectorAll('td.cell-focus').forEach(td => td.classList.remove('cell-focus'));
  const rows = $('grid-body').querySelectorAll('tr');
  if (focusRow >= 0 && focusRow < rows.length) {
    const cells = rows[focusRow].querySelectorAll('td.editable');
    if (focusCol >= 0 && focusCol < cells.length) {
      cells[focusCol].classList.add('cell-focus');
      cells[focusCol].scrollIntoView({ block: 'nearest', inline: 'nearest' });
    }
  }
}

function renderPagination() {
  if (totalRows <= pageLimit && pageOffset === 0) {
    $('pagination').classList.add('hidden');
    return;
  }
  $('pagination').classList.remove('hidden');
  const start = pageOffset + 1;
  const end = Math.min(pageOffset + currentRows.length, totalRows);
  $('page-info').textContent = start + '–' + end + ' of ' + totalRows;
  $('page-prev').disabled = pageOffset === 0;
  $('page-next').disabled = pageOffset + pageLimit >= totalRows;
}

$('page-prev').onclick = () => { pageOffset = Math.max(0, pageOffset - pageLimit); loadTableData(); };
$('page-next').onclick = () => { pageOffset += pageLimit; loadTableData(); };

// ── Inline Cell Editing ──
function startEdit(td) {
  if (readOnlyMode) { toast('Read-only mode', 'error'); return; }
  if (editingCell) cancelEdit();

  const row = parseInt(td.dataset.row);
  const col = td.dataset.col;
  const dataIdx = parseInt(td.dataset.idx);
  const val = currentRows[row][dataIdx];
  const isNull = val === null || val === 'NULL' || val === undefined;

  editingCell = { td, row, col, dataIdx, originalVal: val };
  const hasFk = isFkColumn(currentTable, col);

  td.classList.add('editing');
  const input = document.createElement('input');
  input.value = isNull ? '' : val;
  input.placeholder = 'NULL';
  td.innerHTML = '';
  td.appendChild(input);
  input.focus();
  input.select();

  if (hasFk) {
    showFkDropdown(input, currentTable, col);
    input.addEventListener('input', () => showFkDropdown(input, currentTable, col));
  }

  input.addEventListener('keydown', e => {
    if (hasFk && fkDropdown && fkItems.length > 0) {
      if (e.key === 'ArrowDown') { e.preventDefault(); fkIndex = Math.min(fkIndex + 1, fkItems.length - 1); updateFkSelection(); return; }
      if (e.key === 'ArrowUp') { e.preventDefault(); fkIndex = Math.max(fkIndex - 1, 0); updateFkSelection(); return; }
      if (e.key === 'Tab' && fkIndex >= 0) { e.preventDefault(); input.value = fkItems[fkIndex]; hideFkDropdown(); return; }
    }
    if (e.key === 'Enter') { hideFkDropdown(); saveEdit(); }
    if (e.key === 'Escape') { hideFkDropdown(); cancelEdit(); }
  });
  input.addEventListener('blur', () => {
    setTimeout(() => {
      if (!fkSelecting) { hideFkDropdown(); saveEdit(); }
      fkSelecting = false;
    }, 200);
  });
}

function updateFkSelection() {
  if (!fkDropdown) return;
  fkDropdown.querySelectorAll('.fk-item').forEach((el, i) => {
    el.classList.toggle('selected', i === fkIndex);
  });
}

function cancelEdit() {
  if (!editingCell) return;
  const { td, row, dataIdx } = editingCell;
  const val = currentRows[row][dataIdx];
  td.classList.remove('editing');
  if (val === null || val === 'NULL' || val === undefined) {
    td.className = 'editable null-val';
    td.textContent = 'NULL';
  } else {
    td.className = 'editable';
    td.textContent = val;
  }
  editingCell = null;
}

async function saveEdit() {
  if (!editingCell) return;
  const { td, row, col, dataIdx, originalVal } = editingCell;
  const input = td.querySelector('input');
  if (!input) { cancelEdit(); return; }
  const newVal = input.value;

  if (newVal === (originalVal || '')) { cancelEdit(); return; }

  let pkCol, pkVal;
  if (currentPkMode === 'ctid') {
    pkCol = 'ctid';
    const ctidIdx = currentColumns.indexOf('ctid');
    pkVal = currentRows[row][ctidIdx];
  } else {
    pkCol = currentPkColumns[0] || currentColumns[0];
    const pkIdx = currentColumns.indexOf(pkCol);
    pkVal = currentRows[row][pkIdx];
  }

  editingCell = null;
  td.classList.remove('editing');

  try {
    const data = await fetchJson('/api/update', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        table: currentTable, column: col,
        value: newVal, pk_column: pkCol, pk_value: pkVal,
        pk_mode: currentPkMode, old_value: originalVal || ''
      })
    });
    if (data.error) { toast(data.error, 'error'); cancelEdit(); loadTableData(); return; }

    currentRows[row][dataIdx] = newVal;
    td.textContent = newVal;
    td.className = 'editable';
    td.classList.add('saved-flash');
    setTimeout(() => td.classList.remove('saved-flash'), 500);
    toast('Updated', 'success');
    bumpJournal();
  } catch(e) { toast('Update failed', 'error'); loadTableData(); }
}

// ── Delete Row ──
async function deleteRow(rowIdx) {
  if (readOnlyMode) { toast('Read-only mode', 'error'); return; }

  let pkCol, pkVal;
  if (currentPkMode === 'ctid') {
    pkCol = 'ctid';
    const ctidIdx = currentColumns.indexOf('ctid');
    pkVal = currentRows[rowIdx][ctidIdx];
  } else {
    pkCol = currentPkColumns[0] || currentColumns[0];
    const pkIdx = currentColumns.indexOf(pkCol);
    pkVal = currentRows[rowIdx][pkIdx];
  }

  const ok = await confirm('Delete Row', 'DELETE FROM "' + currentTable + '" WHERE "' + pkCol + '" = \'' + pkVal + '\'');
  if (!ok) return;

  try {
    const data = await fetchJson('/api/delete-row', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        table: currentTable, pk_column: pkCol, pk_value: pkVal,
        pk_mode: currentPkMode
      })
    });
    if (data.error) toast(data.error, 'error');
    else { toast('Row deleted from ' + currentTable, 'success'); bumpJournal(); loadTableData(); }
  } catch(e) { toast('Delete failed', 'error'); }
}

// ── Insert Row (Modal) ──
$('btn-add-row').onclick = () => {
  if (!currentTable || !dbConnected) return;
  const meta = getTableMeta(currentTable);
  if (!meta || !meta.columns) { toast('No column info', 'error'); return; }

  $('insert-tbl').textContent = currentTable;
  const container = $('insert-fields');
  container.innerHTML = '';

  meta.columns.forEach(col => {
    const autoGen = col.column_default && (col.column_default.toLowerCase().includes('nextval') || col.column_default.toLowerCase().includes('gen_random'));
    const required = !col.is_nullable && !col.column_default;

    const field = document.createElement('div');
    field.className = 'modal-field';

    let labelHtml = '<span>' + escHtml(col.name) + '</span>';
    labelHtml += ' <span class="type-hint">' + escHtml(col.type || '') + '</span>';
    if (col.is_primary_key) labelHtml += ' <span class="badge pk">PK</span>';
    if (col.fk_target_table) labelHtml += ' <span class="badge fk">FK→' + escHtml(col.fk_target_table) + '</span>';
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
      col.enum_values.forEach(v => {
        const opt = document.createElement('option');
        opt.value = v; opt.textContent = v;
        select.appendChild(opt);
      });
      field.appendChild(select);
    } else {
      const input = document.createElement('input');
      input.type = 'text';
      input.dataset.column = col.name;
      input.dataset.autoGen = autoGen ? '1' : '';
      if (autoGen) { input.placeholder = '(auto-generated)'; input.classList.add('auto-gen'); }
      else if (col.column_default) input.placeholder = 'default: ' + col.column_default;
      else if (col.is_nullable) input.placeholder = '(null)';
      else input.placeholder = 'required';
      field.appendChild(input);
    }
    container.appendChild(field);
  });

  $('insert-overlay').classList.add('open');
  trapFocus($('insert-overlay'));
};

async function submitInsert() {
  const meta = getTableMeta(currentTable);
  if (!meta) return;
  const values = {};
  let hasValues = false;

  meta.columns.forEach(col => {
    const el = $('insert-fields').querySelector('[data-column="' + col.name + '"]');
    if (!el) return;
    if (el.tagName === 'SELECT') {
      if (el.value !== '__DEFAULT__') { values[col.name] = el.value; hasValues = true; }
      return;
    }
    const val = el.value.trim();
    if (val === '' && el.dataset.autoGen) return;
    if (val === '' && (col.is_nullable || col.column_default)) return;
    if (val === '') return;
    values[col.name] = val;
    hasValues = true;
  });

  releaseFocus($('insert-overlay'));
  $('insert-overlay').classList.remove('open');
  try {
    const payload = { table: currentTable };
    if (hasValues) payload.values = values;
    const data = await fetchJson('/api/insert-row', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    if (data.error) toast(data.error, 'error');
    else { toast('Row inserted into ' + currentTable, 'success'); bumpJournal(); loadTableData(); }
  } catch(e) { toast('Insert failed', 'error'); }
}

$('insert-ok').onclick = submitInsert;
$('insert-cancel').onclick = () => { releaseFocus($('insert-overlay')); $('insert-overlay').classList.remove('open'); };
$('insert-overlay').onclick = e => { if (e.target === $('insert-overlay')) { releaseFocus($('insert-overlay')); $('insert-overlay').classList.remove('open'); } };
$('insert-fields').addEventListener('keydown', e => { if (e.key === 'Enter') submitInsert(); });

// ── Export ──
$('btn-export-csv').onclick = () => {
  if (currentTable) window.location.href = '/api/export/' + encodeURIComponent(currentTable) + '?format=csv';
};
$('btn-export-json').onclick = () => {
  if (currentTable) window.location.href = '/api/export/' + encodeURIComponent(currentTable) + '?format=json';
};

// ── Find & Replace ──
$('btn-find-replace').onclick = () => {
  if (!currentTable || !dbConnected) return;
  $('fnr-tbl').textContent = currentTable;
  const select = $('fnr-column');
  select.innerHTML = '';
  currentColumns.forEach(col => {
    if (col === 'ctid') return;
    const opt = document.createElement('option');
    opt.value = col; opt.textContent = col;
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
  if (!find) { toast('Enter a search term', 'error'); return; }
  try {
    const data = await fetchJson('/api/tables/' + encodeURIComponent(currentTable) + '/bulk-update', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ column: col, find, replace, force: 'false' })
    });
    if (data.error) { toast(data.error, 'error'); return; }
    $('fnr-preview').style.display = 'block';
    $('fnr-preview').textContent = data.affected_rows + ' rows will be affected';
    $('fnr-preview').style.color = data.affected_rows > 0 ? 'var(--warning)' : 'var(--text-muted)';
  } catch(e) { toast('Preview failed', 'error'); }
};

$('fnr-ok').onclick = async () => {
  const col = $('fnr-column').value;
  const find = $('fnr-find').value;
  const replace = $('fnr-replace').value;
  if (!find) { toast('Enter a search term', 'error'); return; }
  try {
    const data = await fetchJson('/api/tables/' + encodeURIComponent(currentTable) + '/bulk-update', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ column: col, find, replace, force: 'true' })
    });
    if (data.error) { toast(data.error, 'error'); return; }
    releaseFocus($('fnr-overlay'));
    $('fnr-overlay').classList.remove('open');
    toast('Replaced in ' + currentTable, 'success');
    bumpJournal();
    loadTableData();
  } catch(e) { toast('Replace failed', 'error'); }
};

$('fnr-cancel').onclick = () => { releaseFocus($('fnr-overlay')); $('fnr-overlay').classList.remove('open'); };
$('fnr-overlay').onclick = e => { if (e.target === $('fnr-overlay')) { releaseFocus($('fnr-overlay')); $('fnr-overlay').classList.remove('open'); } };

// ── FK Autocomplete (in-grid editing) ──
let fkDropdown = null;
let fkItems = [];
let fkIndex = -1;
let fkSelecting = false;

function getColumnMeta(tableName, colName) {
  if (!schemaData || !schemaData.tables) return null;
  const tbl = schemaData.tables.find(t => t.name === tableName);
  if (!tbl) return null;
  return tbl.columns.find(c => c.name === colName) || null;
}

function isFkColumn(tableName, colName) {
  const meta = getColumnMeta(tableName, colName);
  return meta && meta.fk_target_table;
}

async function showFkDropdown(input, tableName, colName) {
  const meta = getColumnMeta(tableName, colName);
  if (!meta || !meta.fk_target_table) return;

  const search = input.value.trim();
  try {
    const url = '/api/tables/' + encodeURIComponent(tableName) + '/fk-lookup?column=' +
                encodeURIComponent(colName) + '&search=' + encodeURIComponent(search) + '&limit=10';
    const data = await fetchJson(url);
    if (!data.values || data.values.length === 0) { hideFkDropdown(); return; }

    fkItems = data.values;
    fkIndex = -1;

    if (fkDropdown) fkDropdown.remove();
    fkDropdown = document.createElement('div');
    fkDropdown.className = 'fk-dropdown';

    const rect = input.getBoundingClientRect();
    fkDropdown.style.position = 'fixed';
    fkDropdown.style.top = (rect.bottom + 2) + 'px';
    fkDropdown.style.left = rect.left + 'px';
    fkDropdown.style.minWidth = rect.width + 'px';

    fkItems.forEach((val, i) => {
      const item = document.createElement('div');
      item.className = 'fk-item';
      item.textContent = val;
      item.addEventListener('mousedown', e => {
        e.preventDefault();
        fkSelecting = true;
        input.value = val;
        hideFkDropdown();
      });
      fkDropdown.appendChild(item);
    });

    document.body.appendChild(fkDropdown);
  } catch(e) { hideFkDropdown(); }
}

function hideFkDropdown() {
  if (fkDropdown) { fkDropdown.remove(); fkDropdown = null; }
  fkItems = [];
  fkIndex = -1;
}

// ── Read-Only Toggle ──
$('btn-readonly').onclick = async () => {
  try {
    const data = await fetchJson('/api/settings/read-only', { method: 'POST' });
    readOnlyMode = data.read_only;
    updateReadOnlyUI();
    toast(readOnlyMode ? 'Read-only enabled' : 'Read-only disabled', 'info');
  } catch(e) { toast('Failed to toggle', 'error'); }
};

async function loadReadOnly() {
  try {
    const data = await fetchJson('/api/settings/read-only');
    readOnlyMode = data.read_only;
    updateReadOnlyUI();
  } catch(e) { /* non-critical: read-only status unknown until connected */ }
}

function updateReadOnlyUI() {
  $('btn-readonly').classList.toggle('active', readOnlyMode);
  $('status-ro').classList.toggle('show', readOnlyMode);
}

// ── Refresh ──
$('btn-refresh').onclick = async () => { if (dbConnected) { await loadSchema(); toast('Schema refreshed', 'success'); } };
$('btn-tbl-refresh').onclick = () => loadTableData();

// ── SQL Editor ──
const sqlEditor = $('sql-editor');
const sqlHighlight = $('sql-highlight');

const SQL_KW = new Set(['SELECT','FROM','WHERE','AND','OR','NOT','IN','IS','NULL','AS','ON','JOIN',
  'LEFT','RIGHT','INNER','OUTER','FULL','CROSS','GROUP','BY','ORDER','HAVING','LIMIT','OFFSET',
  'INSERT','INTO','VALUES','UPDATE','SET','DELETE','CREATE','ALTER','DROP','TABLE','INDEX','VIEW',
  'DISTINCT','BETWEEN','LIKE','ILIKE','EXISTS','CASE','WHEN','THEN','ELSE','END','UNION','ALL',
  'ASC','DESC','CASCADE','RESTRICT','PRIMARY','KEY','FOREIGN','REFERENCES','CONSTRAINT','UNIQUE',
  'DEFAULT','CHECK','RETURNING','WITH','RECURSIVE','GRANT','REVOKE','BEGIN','COMMIT','ROLLBACK',
  'EXPLAIN','ANALYZE','VACUUM','TRUNCATE','SERIAL','BIGSERIAL','TRUE','FALSE','IF','REPLACE',
  'TEMP','TEMPORARY','SCHEMA','DATABASE','TRIGGER','FUNCTION','PROCEDURE','RETURNS','LANGUAGE',
  'VOLATILE','IMMUTABLE','STABLE','SECURITY','DEFINER','INVOKER','EXECUTE','USING','CAST',
  'INTERVAL','AT','TIME','ZONE','CURRENT_TIMESTAMP','CURRENT_DATE','CURRENT_TIME','NOW']);

const SQL_FN = new Set(['COUNT','SUM','AVG','MIN','MAX','COALESCE','NULLIF','CAST','EXTRACT',
  'DATE_TRUNC','DATE_PART','NOW','CURRENT_DATE','CURRENT_TIMESTAMP','UPPER','LOWER','LENGTH',
  'TRIM','SUBSTRING','REPLACE','CONCAT','STRING_AGG','ARRAY_AGG','JSON_AGG','JSONB_AGG',
  'ROW_NUMBER','RANK','DENSE_RANK','LAG','LEAD','FIRST_VALUE','LAST_VALUE','OVER','PARTITION',
  'GENERATE_SERIES','UNNEST','TO_CHAR','TO_DATE','TO_NUMBER','TO_TIMESTAMP','AGE','ROUND',
  'CEIL','FLOOR','ABS','MOD','POWER','SQRT','RANDOM','PG_SIZE_PRETTY','PG_RELATION_SIZE',
  'PG_TOTAL_RELATION_SIZE','GREATEST','LEAST','ARRAY_LENGTH','JSONB_BUILD_OBJECT']);

function highlightSQL(text) {
  let html = '';
  let i = 0;
  while (i < text.length) {
    if (text[i] === '-' && text[i+1] === '-') {
      const end = text.indexOf('\n', i);
      const comment = end === -1 ? text.slice(i) : text.slice(i, end);
      html += '<span class="sql-comment">' + escHtml(comment) + '</span>';
      i += comment.length;
      continue;
    }
    if (text[i] === "'") {
      let j = i + 1;
      while (j < text.length) {
        if (text[j] === "'" && text[j+1] === "'") j += 2;
        else if (text[j] === "'") { j++; break; }
        else j++;
      }
      html += '<span class="sql-str">' + escHtml(text.slice(i, j)) + '</span>';
      i = j;
      continue;
    }
    if (/[0-9]/.test(text[i]) && (i === 0 || /[\s,()=<>+\-*/]/.test(text[i-1]))) {
      let j = i;
      while (j < text.length && /[0-9.]/.test(text[j])) j++;
      html += '<span class="sql-num">' + escHtml(text.slice(i, j)) + '</span>';
      i = j;
      continue;
    }
    if ('=<>!+-*/|&'.includes(text[i])) {
      html += '<span class="sql-op">' + escHtml(text[i]) + '</span>';
      i++;
      continue;
    }
    if (/[a-zA-Z_]/.test(text[i])) {
      let j = i;
      while (j < text.length && /[a-zA-Z0-9_]/.test(text[j])) j++;
      const word = text.slice(i, j);
      const upper = word.toUpperCase();
      if (SQL_KW.has(upper)) html += '<span class="sql-kw">' + escHtml(word) + '</span>';
      else if (SQL_FN.has(upper)) html += '<span class="sql-fn">' + escHtml(word) + '</span>';
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

// ── SQL Autocomplete ──
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

  if (schemaData && schemaData.tables) {
    schemaData.tables.forEach(t => {
      if (t.name.toLowerCase().startsWith(p) && !seen.has(t.name)) {
        results.push({ text: t.name, type: 'tbl' });
        seen.add(t.name);
      }
      const dotIdx = prefix.indexOf('.');
      if (dotIdx > 0) {
        const tblPart = prefix.slice(0, dotIdx).toLowerCase();
        const colPart = prefix.slice(dotIdx + 1).toLowerCase();
        if (t.name.toLowerCase() === tblPart) {
          t.columns.forEach(c => {
            const full = t.name + '.' + c.name;
            if (c.name.toLowerCase().startsWith(colPart) && !seen.has(full)) {
              results.push({ text: full, type: 'col' });
              seen.add(full);
            }
          });
        }
      } else {
        t.columns.forEach(c => {
          if (c.name.toLowerCase().startsWith(p) && !seen.has(c.name)) {
            results.push({ text: c.name, type: 'col' });
            seen.add(c.name);
          }
        });
      }
    });
  }

  SQL_KW.forEach(kw => {
    if (kw.toLowerCase().startsWith(p) && !seen.has(kw)) {
      results.push({ text: kw, type: 'kw' });
      seen.add(kw);
    }
  });

  SQL_FN.forEach(fn => {
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
  const { word, start } = getACWordAt(sqlEditor.value, pos);

  if (word.length < 2) { hideAC(); return; }

  const items = buildACList(word);
  if (items.length === 0) { hideAC(); return; }

  acItems = items;
  acIndex = 0;
  renderAC();

  const textBefore = sqlEditor.value.substring(0, pos);
  const lines = textBefore.split('\n');
  const lineNum = lines.length - 1;
  const colNum = lines[lines.length - 1].length;
  const lineH = AC_LINE_HEIGHT;
  const charW = AC_CHAR_WIDTH;
  const top = Math.min((lineNum + 1) * lineH + 16, sqlEditor.offsetHeight - 100);
  const left = Math.min(colNum * charW + 20, sqlEditor.offsetWidth - 240);
  acDropdown.style.top = top + 'px';
  acDropdown.style.left = left + 'px';
  acDropdown.classList.add('open');
}

function renderAC() {
  acDropdown.innerHTML = acItems.map((item, i) =>
    '<div class="sql-ac-item' + (i === acIndex ? ' selected' : '') + '" data-idx="' + i + '">' +
    '<span class="ac-type ' + item.type + '">' + item.type + '</span>' +
    escHtml(item.text) + '</div>'
  ).join('');

  acDropdown.querySelectorAll('.sql-ac-item').forEach(el => {
    el.addEventListener('mousedown', e => {
      e.preventDefault();
      acIndex = parseInt(el.dataset.idx);
      acceptAC();
    });
  });
}

function acceptAC() {
  if (acIndex < 0 || acIndex >= acItems.length) return;
  const item = acItems[acIndex];
  const pos = sqlEditor.selectionStart;
  const { start } = getACWordAt(sqlEditor.value, pos);
  sqlEditor.value = sqlEditor.value.substring(0, start) + item.text + sqlEditor.value.substring(pos);
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
sqlEditor.addEventListener('input', function() {
  syncHighlight();
  clearTimeout(acTimer);
  acTimer = setTimeout(showAC, 80);
});

sqlEditor.addEventListener('keydown', e => {
  if (acDropdown.classList.contains('open')) {
    if (e.key === 'ArrowDown') { e.preventDefault(); acIndex = Math.min(acIndex + 1, acItems.length - 1); renderAC(); return; }
    if (e.key === 'ArrowUp') { e.preventDefault(); acIndex = Math.max(acIndex - 1, 0); renderAC(); return; }
    if (e.key === 'Tab' || e.key === 'Enter') {
      if (acItems.length > 0) { e.preventDefault(); acceptAC(); return; }
    }
    if (e.key === 'Escape') { e.preventDefault(); hideAC(); return; }
  }

  if (e.key === 'Tab') {
    e.preventDefault();
    const start = sqlEditor.selectionStart;
    sqlEditor.value = sqlEditor.value.substring(0, start) + '  ' + sqlEditor.value.substring(sqlEditor.selectionEnd);
    sqlEditor.selectionStart = sqlEditor.selectionEnd = start + 2;
    syncHighlight();
  }
});

sqlEditor.addEventListener('blur', () => setTimeout(hideAC, 150));

// ── Run SQL ──
$('btn-run-sql').onclick = runSQL;
sqlEditor.addEventListener('keydown', e => {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); hideAC(); runSQL(); }
});

function detectDangerousSQL(sql) {
  const s = sql.replace(/--[^\n]*/g, '').replace(/\/\*[\s\S]*?\*\//g, '').trim().toUpperCase();
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
    let previewMsg = danger + ' without WHERE — this affects the entire table.';
    try {
      const pd = await fetchJson('/api/sql/preview', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ sql })
      });
      if (pd.row_count != null) previewMsg += '\n\nRows affected: ' + pd.row_count;
      if (pd.columns && pd.rows && pd.rows.length > 0) {
        renderSQLResult(pd);
        toast('Preview: ' + (pd.row_count || pd.rows.length) + ' rows would be affected', 'warning');
      }
    } catch(e) { /* preview failed, still show warning */ }
    const ok = await confirm('Dangerous ' + danger, previewMsg);
    if (!ok) return;
  }

  const t0 = performance.now();
  showLoading('sql-results');
  try {
    const data = await fetchJson('/api/sql', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ sql, force: danger ? 'true' : undefined })
    });
    const elapsed = Math.round(performance.now() - t0);

    if (data.requires_confirmation) {
      hideLoading('sql-results');
      const ok = await confirm(data.operation + ' — Confirm', data.warning || sql);
      if (!ok) return;
      showLoading('sql-results');
      const t1 = performance.now();
      const data2 = await fetchJson('/api/sql', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ sql, force: 'true' })
      });
      hideLoading('sql-results');
      renderSQLResult(data2, Math.round(performance.now() - t1));
      return;
    }

    hideLoading('sql-results');
    renderSQLResult(data, elapsed);
  } catch(e) { hideLoading('sql-results'); $('sql-results').textContent = e.message; }
}

$('btn-explain').onclick = async () => {
  const sql = sqlEditor.value.trim();
  if (!sql) return;
  showLoading('sql-results');
  try {
    const data = await fetchJson('/api/sql', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ sql: 'EXPLAIN ANALYZE ' + sql })
    });
    hideLoading('sql-results');
    renderSQLResult(data);
  } catch(e) { hideLoading('sql-results'); $('sql-results').textContent = e.message; }
};

$('btn-preview-sql').onclick = async () => {
  const sql = sqlEditor.value.trim();
  if (!sql) return;
  showLoading('sql-results');
  try {
    const data = await fetchJson('/api/sql/preview', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ sql })
    });
    hideLoading('sql-results');
    renderSQLResult(data);
    if (data.preview) toast('Preview only — not committed', 'info');
  } catch(e) { hideLoading('sql-results'); $('sql-results').textContent = e.message; }
};

function renderSQLResult(data, elapsed) {
  const timeStr = elapsed != null ? ' in ' + elapsed + 'ms' : '';
  if (data.error) {
    $('sql-results').innerHTML = '<div class="sql-error">' + escHtml(data.error) + '</div>';
    return;
  }
  if (!data.columns || data.columns.length === 0) {
    $('sql-results').innerHTML = '<div class="sql-message">' + (data.row_count != null ? escHtml(String(data.row_count)) + ' rows affected' : 'OK') + timeStr + '</div>';
    if (currentTable) loadTableData();
    return;
  }

  let html = '<div class="sql-results-info">' + escHtml(String(data.row_count || 0)) + ' rows' + timeStr + '</div>';
  html += '<div class="grid-wrap" style="max-height:100%"><table class="grid"><thead><tr>';
  data.columns.forEach(c => { html += '<th>' + escHtml(c) + '</th>'; });
  html += '</tr></thead><tbody>';
  (data.rows || []).forEach(row => {
    html += '<tr>';
    row.forEach(val => {
      if (val === null || val === 'NULL') html += '<td class="null-val">NULL</td>';
      else html += '<td>' + escHtml(val) + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table></div>';
  $('sql-results').innerHTML = html;

  // Add export buttons if there are results
  if (data.columns && data.columns.length > 0 && data.rows && data.rows.length > 0) {
    const resultsInfo = $('sql-results').querySelector('.sql-results-info');
    if (resultsInfo) {
      const csvBtn = document.createElement('button');
      csvBtn.className = 'btn-export-inline';
      csvBtn.textContent = 'Export CSV';
      csvBtn.addEventListener('click', function() { exportSqlResults('csv'); });
      const jsonBtn = document.createElement('button');
      jsonBtn.className = 'btn-export-inline';
      jsonBtn.textContent = 'Export JSON';
      jsonBtn.addEventListener('click', function() { exportSqlResults('json'); });
      resultsInfo.appendChild(csvBtn);
      resultsInfo.appendChild(jsonBtn);
    }
  }
  // Store last SQL for export
  lastSqlQuery = sqlEditor.value.trim();
}

// ── DDL Viewer ──
$('btn-ddl').onclick = async () => {
  if (!currentTable) return;
  $('ddl-title').textContent = currentTable + ' — DDL';
  $('ddl-content').textContent = 'Loading...';
  $('ddl-overlay').classList.add('open');
  trapFocus($('ddl-overlay'));
  try {
    const data = await fetchJson('/api/tables/' + encodeURIComponent(currentTable) + '/ddl');
    if (data.error) $('ddl-content').textContent = 'Error: ' + data.error;
    else $('ddl-content').textContent = data.ddl || 'No DDL available';
  } catch(e) { $('ddl-content').textContent = 'Failed to load DDL'; }
};
$('ddl-copy').onclick = () => {
  copyToClipboard($('ddl-content').textContent).then(() => toast('DDL copied', 'success')).catch(() => {});
};
$('ddl-overlay').onclick = e => { if (e.target === $('ddl-overlay')) { releaseFocus($('ddl-overlay')); $('ddl-overlay').classList.remove('open'); } };

// ── Query History ──
$('btn-sql-history').onclick = async () => {
  $('history-overlay').classList.add('open');
  trapFocus($('history-overlay'));
  try {
    const data = await fetchJson('/api/history');
    const entries = data.entries || [];
    const list = $('history-list');
    if (entries.length === 0) {
      list.innerHTML = '<div style="padding:32px;text-align:center;color:var(--text-muted)">No queries yet</div>';
      return;
    }
    let html = '';
    entries.forEach(entry => {
      const time = new Date(entry.timestamp * 1000);
      const timeStr = time.toLocaleTimeString();
      html += '<div class="history-entry' + (entry.is_error ? ' error' : '') + '">';
      html += '<div class="sql-preview">' + escHtml(entry.sql.substring(0, 200)) + '</div>';
      html += '<div class="history-meta">';
      html += '<span>' + timeStr + '</span>';
      html += '<span class="dur">' + entry.duration_ms + 'ms</span>';
      if (entry.row_count != null) html += '<span>' + escHtml(String(entry.row_count)) + ' rows</span>';
      if (entry.error) html += '<span style="color:var(--error)">' + escHtml(entry.error.substring(0, 80)) + '</span>';
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
        $$('.tab').forEach(t => t.classList.remove('active'));
        $$('.tab-panel').forEach(p => p.classList.remove('active'));
        document.querySelector('[data-tab="sql"]').classList.add('active');
        $('panel-sql').classList.add('active');
        sqlEditor.focus();
      });
    });
  } catch(e) { $('history-list').innerHTML = '<div style="padding:16px;color:var(--error)">Failed to load history</div>'; }
};
$('history-overlay').onclick = e => { if (e.target === $('history-overlay')) { releaseFocus($('history-overlay')); $('history-overlay').classList.remove('open'); } };

// ── ER Diagram ──
let erPositions = []; // stored for hover detection
let erTablePos = {};
let erHoveredTable = null;
let erLayoutCache = null; // cached force-directed layout

// Force-directed graph layout — computes non-overlapping positions
// where FK-connected tables are placed near each other.
function computeERLayout(tables, fkLinks, boxSizeMap, W, H) {
  const cacheKey = tables.map(t => t.name).sort().join(',');
  if (erLayoutCache && erLayoutCache.key === cacheKey) return erLayoutCache.nodes;

  const PAD = 28; // minimum gap between boxes

  // Sort: most-connected tables first
  const sorted = [...tables].sort((a, b) => {
    const al = fkLinks[a.name] ? fkLinks[a.name].size : 0;
    const bl = fkLinks[b.name] ? fkLinks[b.name].size : 0;
    return bl - al;
  });

  // Build unique edge list for spring forces
  const edgeSet = new Set();
  const edges = [];
  tables.forEach(t => {
    t.columns.forEach(col => {
      if (col.fk_target_table) {
        const key = [t.name, col.fk_target_table].sort().join('|');
        if (!edgeSet.has(key)) { edgeSet.add(key); edges.push({ from: t.name, to: col.fk_target_table }); }
      }
    });
  });

  // Initialize positions — circular for connected tables, row below for isolated
  const cx = Math.max(W, 1200) / 2, cy = Math.max(H, 900) / 2;
  const connected = sorted.filter(t => fkLinks[t.name] && fkLinks[t.name].size > 0);
  const isolated = sorted.filter(t => !fkLinks[t.name] || fkLinks[t.name].size === 0);
  const radius = Math.max(350, connected.length * 50);

  const nodes = [];
  const nodeMap = {};
  connected.forEach((t, i) => {
    const angle = (2 * Math.PI * i) / connected.length - Math.PI / 2;
    const sz = boxSizeMap[t.name];
    const n = { name: t.name, table: t, x: cx + radius * Math.cos(angle) - sz.w / 2,
      y: cy + radius * Math.sin(angle) - sz.h / 2, w: sz.w, h: sz.h, vx: 0, vy: 0 };
    nodes.push(n); nodeMap[n.name] = n;
  });
  let isoX = 40, isoY = cy + radius + 200;
  isolated.forEach(t => {
    const sz = boxSizeMap[t.name];
    if (isoX + sz.w > W - 40) { isoX = 40; isoY += 280; }
    const n = { name: t.name, table: t, x: isoX, y: isoY, w: sz.w, h: sz.h, vx: 0, vy: 0 };
    nodes.push(n); nodeMap[n.name] = n;
    isoX += sz.w + PAD + 20;
  });

  // Run force simulation (only on connected nodes for performance)
  const simNodes = nodes.filter(n => fkLinks[n.name] && fkLinks[n.name].size > 0);
  const ITERS = 300;

  for (let iter = 0; iter < ITERS; iter++) {
    const alpha = 1 - iter / ITERS; // cooling
    const t = alpha * alpha; // quadratic decay for smooth settling

    // Reset forces
    simNodes.forEach(n => { n.fx = 0; n.fy = 0; });

    // Repulsion — push all pairs apart (rectangle-aware)
    for (let i = 0; i < simNodes.length; i++) {
      for (let j = i + 1; j < simNodes.length; j++) {
        const a = simNodes[i], b = simNodes[j];
        const acx = a.x + a.w / 2, acy = a.y + a.h / 2;
        const bcx = b.x + b.w / 2, bcy = b.y + b.h / 2;
        let dx = bcx - acx, dy = bcy - acy;
        if (dx === 0 && dy === 0) { dx = (Math.random() - 0.5) * 10; dy = (Math.random() - 0.5) * 10; }
        const dist = Math.sqrt(dx * dx + dy * dy);
        // Stronger repulsion when close, weaker when far
        const strength = 120000 / (dist * dist + 200);
        const fx = (dx / dist) * strength * t;
        const fy = (dy / dist) * strength * t;
        a.fx -= fx; a.fy -= fy;
        b.fx += fx; b.fy += fy;
      }
    }

    // Attraction — FK-connected tables pull toward each other
    edges.forEach(e => {
      const a = nodeMap[e.from], b = nodeMap[e.to];
      if (!a || !b) return;
      const acx = a.x + a.w / 2, acy = a.y + a.h / 2;
      const bcx = b.x + b.w / 2, bcy = b.y + b.h / 2;
      const dx = bcx - acx, dy = bcy - acy;
      const dist = Math.sqrt(dx * dx + dy * dy) || 1;
      const idealDist = (a.w + b.w) / 2 + PAD + 40;
      const force = (dist - idealDist) * 0.04 * t;
      const fx = (dx / dist) * force, fy = (dy / dist) * force;
      a.fx += fx; a.fy += fy;
      b.fx -= fx; b.fy -= fy;
    });

    // Centering gravity — gentle pull toward center
    simNodes.forEach(n => {
      n.fx += (cx - n.x - n.w / 2) * 0.005 * t;
      n.fy += (cy - n.y - n.h / 2) * 0.005 * t;
    });

    // Apply forces with velocity damping
    let maxV = 0;
    simNodes.forEach(n => {
      n.vx = (n.vx + n.fx) * 0.6;
      n.vy = (n.vy + n.fy) * 0.6;
      n.x += n.vx;
      n.y += n.vy;
      const v = Math.abs(n.vx) + Math.abs(n.vy);
      if (v > maxV) maxV = v;
    });
    // Early termination when all nodes have settled
    if (iter > 50 && maxV < 0.5) break;

    // Hard overlap resolution — push overlapping boxes apart
    for (let pass = 0; pass < 4; pass++) {
      for (let i = 0; i < simNodes.length; i++) {
        for (let j = i + 1; j < simNodes.length; j++) {
          const a = simNodes[i], b = simNodes[j];
          const ox = (a.w / 2 + b.w / 2 + PAD) - Math.abs((b.x + b.w / 2) - (a.x + a.w / 2));
          const oy = (a.h / 2 + b.h / 2 + PAD) - Math.abs((b.y + b.h / 2) - (a.y + a.h / 2));
          if (ox > 0 && oy > 0) {
            // Push apart along the axis of least overlap
            if (ox < oy) {
              const push = ox / 2 + 1;
              if (a.x + a.w / 2 < b.x + b.w / 2) { a.x -= push; b.x += push; }
              else { a.x += push; b.x -= push; }
            } else {
              const push = oy / 2 + 1;
              if (a.y + a.h / 2 < b.y + b.h / 2) { a.y -= push; b.y += push; }
              else { a.y += push; b.y -= push; }
            }
          }
        }
      }
    }
  }

  // Normalize: shift so all positions start at margin
  let minX = Infinity, minY = Infinity;
  nodes.forEach(n => { minX = Math.min(minX, n.x); minY = Math.min(minY, n.y); });
  const marginX = 30, marginY = 24;
  nodes.forEach(n => { n.x += marginX - minX; n.y += marginY - minY; });

  erLayoutCache = { key: cacheKey, nodes };
  return nodes;
}

function drawER() {
  if (!schemaData || !schemaData.tables || schemaData.tables.length === 0) {
    $('er-empty').style.display = 'flex';
    return;
  }
  $('er-empty').style.display = 'none';

  const canvas = $('er-canvas');
  const container = $('er-container');
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const style = getComputedStyle(document.documentElement);

  canvas.width = container.clientWidth * dpr;
  canvas.height = container.clientHeight * dpr;
  canvas.style.width = container.clientWidth + 'px';
  canvas.style.height = container.clientHeight + 'px';
  ctx.scale(dpr, dpr);

  const tables = schemaData.tables;
  const W = container.clientWidth;
  const H = container.clientHeight;

  const rowH = 20;
  const headerH = 30;

  // Build FK index: which tables are related to which
  const fkLinks = {};
  tables.forEach(t => {
    if (!fkLinks[t.name]) fkLinks[t.name] = new Set();
    t.columns.forEach(col => {
      if (col.fk_target_table) {
        fkLinks[t.name].add(col.fk_target_table);
        if (!fkLinks[col.fk_target_table]) fkLinks[col.fk_target_table] = new Set();
        fkLinks[col.fk_target_table].add(t.name);
      }
    });
  });

  // Measure box sizes
  ctx.font = '12px ui-monospace, monospace';
  const boxSizeMap = {};
  tables.forEach(t => {
    let maxW = ctx.measureText(t.name).width + 40;
    t.columns.forEach(col => {
      const nameW = ctx.measureText(col.name).width;
      ctx.font = '10px ui-monospace, monospace';
      const typeW = ctx.measureText(col.type || '').width;
      ctx.font = '12px ui-monospace, monospace';
      maxW = Math.max(maxW, nameW + typeW + 36);
    });
    const w = Math.max(170, Math.min(300, maxW));
    const h = headerH + t.columns.length * rowH + 10;
    boxSizeMap[t.name] = { w, h };
  });

  // Get force-directed layout (cached)
  const layoutNodes = computeERLayout(tables, fkLinks, boxSizeMap, W, H);

  // Build positions with pan offset
  const positions = layoutNodes.map(n => ({
    x: n.x + erPanX, y: n.y + erPanY,
    w: n.w, h: n.h, table: n.table
  }));

  // Store globally for hover detection
  erPositions = positions;
  erTablePos = {};
  positions.forEach(p => erTablePos[p.table.name] = p);

  // Canvas sizing — fit all content
  let maxX = 0, maxY = 0;
  positions.forEach(p => { maxX = Math.max(maxX, p.x + p.w + 40); maxY = Math.max(maxY, p.y + p.h + 40); });
  const canvasW = Math.max(W, maxX);
  const canvasH = Math.max(H, maxY);
  canvas.width = canvasW * dpr;
  canvas.height = canvasH * dpr;
  canvas.style.width = canvasW + 'px';
  canvas.style.height = canvasH + 'px';
  ctx.scale(dpr, dpr);

  ctx.save();
  ctx.scale(erZoom, erZoom);
  ctx.clearRect(0, 0, canvasW / erZoom, canvasH / erZoom);

  // Determine which tables are "active" (hovered or related to hovered)
  const hovered = erHoveredTable;
  const relatedSet = new Set();
  if (hovered) {
    relatedSet.add(hovered);
    if (fkLinks[hovered]) fkLinks[hovered].forEach(t => relatedSet.add(t));
  }
  const hasHover = hovered !== null;

  const accentColor = style.getPropertyValue('--er-fk').trim() || '#7c8df4';
  const dimLineColor = 'rgba(100,110,140,0.15)';
  const brightLineColor = accentColor;

  // FK line drawing — bezier curves with smart exit side
  function drawFkLine(fromTable, col, ci, bright) {
    const from = erTablePos[fromTable.name];
    const to = erTablePos[col.fk_target_table];
    if (!from || !to) return;

    const fromCy = from.y + headerH + ci * rowH + rowH / 2;
    let toCi = 0;
    const targetCols = to.table.columns;
    for (let j = 0; j < targetCols.length; j++) {
      if (targetCols[j].is_primary_key || targetCols[j].name === col.fk_target_column) { toCi = j; break; }
    }
    const toCy = to.y + headerH + toCi * rowH + rowH / 2;

    const fromCx = from.x + from.w / 2;
    const toCx = to.x + to.w / 2;
    let x1, x2, cp1x, cp2x;
    if (toCx > fromCx) {
      x1 = from.x + from.w; x2 = to.x;
      const dx = Math.abs(x2 - x1);
      cp1x = x1 + Math.max(40, dx * 0.4);
      cp2x = x2 - Math.max(40, dx * 0.4);
    } else {
      x1 = from.x; x2 = to.x + to.w;
      const dx = Math.abs(x1 - x2);
      cp1x = x1 - Math.max(40, dx * 0.4);
      cp2x = x2 + Math.max(40, dx * 0.4);
    }

    if (bright) {
      ctx.strokeStyle = brightLineColor;
      ctx.lineWidth = 2.5;
      ctx.shadowColor = accentColor;
      ctx.shadowBlur = 6;
    } else {
      ctx.strokeStyle = hasHover ? dimLineColor : (style.getPropertyValue('--er-line').trim() || 'rgba(124,141,244,0.35)');
      ctx.lineWidth = 1.5;
      ctx.shadowBlur = 0;
    }

    ctx.beginPath();
    ctx.moveTo(x1, fromCy);
    ctx.bezierCurveTo(cp1x, fromCy, cp2x, toCy, x2, toCy);
    ctx.stroke();
    ctx.shadowBlur = 0;

    if (bright) {
      ctx.fillStyle = accentColor;
      ctx.beginPath(); ctx.arc(x1, fromCy, 4, 0, Math.PI * 2); ctx.fill();
      ctx.beginPath(); ctx.arc(x2, toCy, 4, 0, Math.PI * 2); ctx.fill();

      const mx = (x1 + cp1x + cp2x + x2) / 4;
      const my = (fromCy + fromCy + toCy + toCy) / 4;
      const label = col.name;
      ctx.font = '600 9px -apple-system, sans-serif';
      const tw = ctx.measureText(label).width;
      ctx.fillStyle = style.getPropertyValue('--er-box-bg').trim();
      ctx.fillRect(mx - tw / 2 - 4, my - 7, tw + 8, 14);
      ctx.strokeStyle = accentColor;
      ctx.lineWidth = 1;
      ctx.strokeRect(mx - tw / 2 - 4, my - 7, tw + 8, 14);
      ctx.fillStyle = accentColor;
      ctx.fillText(label, mx - tw / 2, my + 3);
    }
  }

  // Pass 1: dim FK lines
  tables.forEach(t => {
    t.columns.forEach((col, ci) => {
      if (!col.fk_target_table) return;
      const isBright = hasHover && (relatedSet.has(t.name) && relatedSet.has(col.fk_target_table));
      if (!isBright) drawFkLine(t, col, ci, false);
    });
  });
  // Pass 2: bright FK lines (on top)
  if (hasHover) {
    tables.forEach(t => {
      t.columns.forEach((col, ci) => {
        if (!col.fk_target_table) return;
        const isBright = relatedSet.has(t.name) && relatedSet.has(col.fk_target_table);
        if (isBright) drawFkLine(t, col, ci, true);
      });
    });
  }

  // Table boxes
  positions.forEach(p => {
    const t = p.table;
    const isActive = !hasHover || relatedSet.has(t.name);
    const boxAlpha = isActive ? 1 : 0.3;

    ctx.globalAlpha = boxAlpha;

    // Box shadow
    ctx.fillStyle = 'rgba(0,0,0,0.15)';
    ctx.beginPath(); ctx.roundRect(p.x + 2, p.y + 2, p.w, p.h, 8); ctx.fill();

    // Box body
    ctx.fillStyle = style.getPropertyValue('--er-box-bg').trim();
    ctx.strokeStyle = isActive && hasHover && t.name === hovered ? accentColor : style.getPropertyValue('--er-box-border').trim();
    ctx.lineWidth = isActive && hasHover && t.name === hovered ? 2 : 1;
    ctx.beginPath(); ctx.roundRect(p.x, p.y, p.w, p.h, 8); ctx.fill(); ctx.stroke();

    // Header
    ctx.fillStyle = style.getPropertyValue('--er-header-bg').trim();
    ctx.beginPath(); ctx.roundRect(p.x, p.y, p.w, headerH, [8, 8, 0, 0]); ctx.fill();
    ctx.strokeStyle = style.getPropertyValue('--er-box-border').trim();
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(p.x, p.y + headerH); ctx.lineTo(p.x + p.w, p.y + headerH); ctx.stroke();

    // Table name
    ctx.fillStyle = style.getPropertyValue('--er-text').trim();
    ctx.font = '600 12px -apple-system, sans-serif';
    ctx.fillText(t.name, p.x + 12, p.y + 19);

    // Column count + link badge
    const incomingCount = fkLinks[t.name] ? fkLinks[t.name].size : 0;
    ctx.fillStyle = style.getPropertyValue('--er-text-dim').trim();
    ctx.font = '10px -apple-system, sans-serif';
    const badge = t.columns.length + ' cols' + (incomingCount > 0 ? ' · ' + incomingCount + ' links' : '');
    const bw = ctx.measureText(badge).width;
    ctx.fillText(badge, p.x + p.w - bw - 10, p.y + 19);

    // Columns
    t.columns.forEach((col, ci) => {
      const y = p.y + headerH + ci * rowH + 15;
      if (ci % 2 === 1) {
        ctx.fillStyle = 'rgba(255,255,255,0.02)';
        ctx.fillRect(p.x + 1, p.y + headerH + ci * rowH, p.w - 2, rowH);
      }
      ctx.font = '12px ui-monospace, monospace';
      if (col.is_primary_key) ctx.fillStyle = style.getPropertyValue('--er-pk').trim();
      else if (col.fk_target_table) ctx.fillStyle = style.getPropertyValue('--er-fk').trim();
      else ctx.fillStyle = style.getPropertyValue('--er-col').trim();
      ctx.fillText(col.name, p.x + 12, y);

      ctx.fillStyle = style.getPropertyValue('--er-text-dim').trim();
      ctx.font = '10px ui-monospace, monospace';
      const typeW = ctx.measureText(col.type || '').width;
      ctx.fillText(col.type || '', p.x + p.w - typeW - 12, y);
    });

    ctx.globalAlpha = 1;
  });

  ctx.restore();
}

$('btn-er-zoom-in').onclick = () => { erZoom = Math.min(3, erZoom + 0.2); drawER(); };
$('btn-er-zoom-out').onclick = () => { erZoom = Math.max(0.3, erZoom - 0.2); drawER(); };
$('btn-er-fit').onclick = () => { erZoom = 1; erPanX = 0; erPanY = 0; drawER(); };

let erDragging = false, erLastX, erLastY;
$('er-canvas').addEventListener('mousedown', e => { erDragging = true; erLastX = e.clientX; erLastY = e.clientY; });
window.addEventListener('mousemove', e => {
  if (erDragging) {
    erPanX += e.clientX - erLastX;
    erPanY += e.clientY - erLastY;
    erLastX = e.clientX;
    erLastY = e.clientY;
    drawER();
    return;
  }
  // Hover detection on ER canvas
  const canvas = $('er-canvas');
  if (!canvas || !$('panel-er').classList.contains('active')) return;
  const rect = canvas.getBoundingClientRect();
  const mx = (e.clientX - rect.left) / erZoom;
  const my = (e.clientY - rect.top) / erZoom;
  let found = null;
  for (const p of erPositions) {
    if (mx >= p.x && mx <= p.x + p.w && my >= p.y && my <= p.y + p.h) {
      found = p.table.name;
      break;
    }
  }
  if (found !== erHoveredTable) {
    erHoveredTable = found;
    canvas.style.cursor = found ? 'pointer' : 'grab';
    drawER();
  }
});
window.addEventListener('mouseup', () => { erDragging = false; });

// ── Journal ──
function bumpJournal() {
  journalCount++;
  const badge = $('journal-badge');
  badge.textContent = journalCount;
  badge.classList.add('show');
}
function clearJournalBadge() {
  journalCount = 0;
  $('journal-badge').classList.remove('show');
}

async function loadJournal() {
  try {
    const data = await fetchJson('/api/journal');
    const entries = data.entries || [];
    const list = $('journal-list');

    if (entries.length === 0) {
      $('journal-empty').style.display = '';
      list.querySelectorAll('.journal-entry').forEach(e => e.remove());
      return;
    }
    $('journal-empty').style.display = 'none';

    let html = '';
    entries.reverse().forEach(entry => {
      const opClass = entry.operation || 'update';
      html += '<div class="journal-entry' + (entry.undone ? ' undone' : '') + '" data-id="' + entry.id + '">';
      html += '<span class="op ' + opClass + '">' + escHtml(opClass) + '</span>';
      html += '<div class="details">';
      html += '<span class="tbl">' + escHtml(entry.table || '') + '</span>';
      if (entry.operation === 'delete' && entry.old_value && entry.old_value.startsWith('{')) {
        // Show deleted row data
        try {
          const rowData = JSON.parse(entry.old_value);
          const keys = Object.keys(rowData).slice(0, 4);
          html += '<div class="journal-deleted-row">';
          keys.forEach(k => {
            const v = rowData[k];
            html += '<span class="journal-kv"><span class="col">' + escHtml(k) + '</span>=<span class="val old">' + escHtml(String(v)) + '</span></span> ';
          });
          if (Object.keys(rowData).length > 4) html += '<span class="val" style="opacity:0.5">+' + (Object.keys(rowData).length - 4) + ' more</span>';
          html += '</div>';
        } catch(e) {
          if (entry.pk_column && entry.pk_value) {
            html += ' <span class="val">(' + escHtml(entry.pk_column) + '=' + escHtml(entry.pk_value) + ')</span>';
          }
        }
      } else if (entry.column) {
        html += ' <span class="col">' + escHtml(entry.column) + '</span>';
        if (entry.old_value) html += ' <span class="val old">' + escHtml(entry.old_value) + '</span>';
        if (entry.new_value) html += ' &rarr; <span class="val new">' + escHtml(entry.new_value) + '</span>';
      }
      if (!(entry.operation === 'delete' && entry.old_value && entry.old_value.startsWith('{')) && entry.pk_column && entry.pk_value) {
        html += ' <span class="val">(' + escHtml(entry.pk_column) + '=' + escHtml(entry.pk_value) + ')</span>';
      }
      if (entry.timestamp) {
        const d = new Date(entry.timestamp * 1000);
        html += '<span class="ts">' + d.toLocaleTimeString() + '</span>';
      }
      html += '</div>';
      if (!entry.undone && entry.operation === 'update') {
        html += '<button class="undo-btn" data-id="' + entry.id + '">Undo</button>';
      }
      html += '</div>';
    });

    list.querySelectorAll('.journal-entry').forEach(e => e.remove());
    list.insertAdjacentHTML('beforeend', html);

    list.querySelectorAll('.undo-btn').forEach(btn => {
      btn.onclick = async () => {
        try {
          const data = await fetchJson('/api/journal/undo', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ id: parseInt(btn.dataset.id) })
          });
          if (data.error) toast(data.error, 'error');
          else { toast('Change undone', 'success'); loadJournal(); if (currentTable) loadTableData(); }
        } catch(e) { toast('Undo failed', 'error'); }
      };
    });

    // Enhance delete entries with more prominent PK info
    list.querySelectorAll('.journal-entry').forEach(entry => {
      const opEl = entry.querySelector('.op.delete');
      if (opEl) {
        const details = entry.querySelector('.details');
        if (details && !details.querySelector('.col')) {
          const valEl = details.querySelector('.val');
          if (valEl) {
            valEl.style.color = 'var(--error)';
            valEl.style.fontWeight = '500';
          }
        }
      }
    });
  } catch(e) { toast('Failed to load journal', 'error'); }
}

$('btn-journal-refresh').onclick = loadJournal;

// ── Context Menu ──
function showContextMenu(x, y, rowIdx, colName, td) {
  const menu = $('ctx-menu');
  const val = td.textContent;
  const isNull = td.classList.contains('null-val');
  let html = '';
  html += '<div class="ctx-menu-item" data-action="copy"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>Copy Value<span class="shortcut">Click</span></div>';
  html += '<div class="ctx-menu-item" data-action="copy-row">Copy Row as JSON</div>';
  html += '<div class="ctx-menu-item" data-action="copy-insert">Copy as INSERT</div>';
  html += '<div class="ctx-menu-sep"></div>';
  html += '<div class="ctx-menu-item" data-action="edit">Edit Cell<span class="shortcut">DblClick</span></div>';
  if (!isNull) {
    html += '<div class="ctx-menu-item" data-action="set-null">Set to NULL</div>';
  }
  html += '<div class="ctx-menu-item" data-action="detail">View Row Detail</div>';
  html += '<div class="ctx-menu-sep"></div>';
  html += '<div class="ctx-menu-item" data-action="filter">Filter by this value</div>';
  html += '<div class="ctx-menu-sep"></div>';
  html += '<div class="ctx-menu-item" data-action="select">Select Row</div>';
  html += '<div class="ctx-menu-item danger" data-action="delete">Delete Row<span class="shortcut">&times;</span></div>';

  menu.innerHTML = html;
  menu.style.left = Math.min(x, window.innerWidth - 220) + 'px';
  menu.style.top = Math.min(y, window.innerHeight - 350) + 'px';
  menu.classList.add('open');

  menu.querySelectorAll('.ctx-menu-item').forEach(item => {
    item.addEventListener('click', () => {
      menu.classList.remove('open');
      const action = item.dataset.action;
      if (action === 'copy') copyToClipboard(val).then(() => toast('Copied', 'success')).catch(() => {});
      if (action === 'copy-row') copyRowAsJSON(rowIdx);
      if (action === 'copy-insert') copyRowAsInsert(rowIdx);
      if (action === 'edit') startEdit(td);
      if (action === 'set-null') setToNull(rowIdx, colName, td);
      if (action === 'detail') showRowDetail(rowIdx);
      if (action === 'filter') { columnFilters[colName] = val; pageOffset = 0; loadTableData(); }
      if (action === 'select') { selectedRows.add(rowIdx); lastSelectedRow = rowIdx; updateRowSelection(); }
      if (action === 'delete') deleteRow(rowIdx);
    });
  });
}

document.addEventListener('click', () => $('ctx-menu').classList.remove('open'));

function copyRowAsJSON(rowIdx) {
  const cols = displayCols();
  const obj = {};
  cols.forEach(col => {
    const idx = currentColumns.indexOf(col);
    obj[col] = currentRows[rowIdx][idx];
  });
  copyToClipboard(JSON.stringify(obj, null, 2)).then(() => toast('Row copied as JSON', 'success')).catch(() => {});
}

function copyRowAsInsert(rowIdx) {
  const cols = displayCols();
  const vals = cols.map(col => {
    const idx = currentColumns.indexOf(col);
    const v = currentRows[rowIdx][idx];
    if (v === null || v === 'NULL' || v === undefined) return 'NULL';
    return "'" + String(v).replace(/'/g, "''") + "'";
  });
  const sql = 'INSERT INTO "' + currentTable + '" (' + cols.map(c => '"' + c.replace(/"/g, '""') + '"').join(', ') + ') VALUES (' + vals.join(', ') + ');';
  copyToClipboard(sql).then(() => toast('Copied as INSERT', 'success')).catch(() => {});
}

async function setToNull(rowIdx, colName, td) {
  if (readOnlyMode) { toast('Read-only mode', 'error'); return; }
  let pkCol, pkVal;
  if (currentPkMode === 'ctid') {
    pkCol = 'ctid';
    const ctidIdx = currentColumns.indexOf('ctid');
    pkVal = currentRows[rowIdx][ctidIdx];
  } else {
    pkCol = currentPkColumns[0] || currentColumns[0];
    const pkIdx = currentColumns.indexOf(pkCol);
    pkVal = currentRows[rowIdx][pkIdx];
  }
  const dataIdx = currentColumns.indexOf(colName);
  const oldVal = currentRows[rowIdx][dataIdx];

  try {
    const data = await fetchJson('/api/update', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        table: currentTable, column: colName,
        value: '__NULL__', pk_column: pkCol, pk_value: pkVal,
        pk_mode: currentPkMode, old_value: oldVal || ''
      })
    });
    if (data.error) toast(data.error, 'error');
    else {
      currentRows[rowIdx][dataIdx] = null;
      td.className = 'editable null-val';
      td.textContent = 'NULL';
      td.classList.add('saved-flash');
      setTimeout(() => td.classList.remove('saved-flash'), 500);
      toast('Set to NULL', 'success');
      bumpJournal();
    }
  } catch(e) { toast('Update failed', 'error'); }
}

// ── Multi-Row Selection ──
function updateRowSelection() {
  $('grid-body').querySelectorAll('tr').forEach(tr => {
    const ri = parseInt(tr.dataset.ridx);
    tr.classList.toggle('selected', selectedRows.has(ri));
  });
  updateBulkBar();
}

function updateBulkBar() {
  const bar = $('bulk-bar');
  if (selectedRows.size > 0) {
    bar.classList.add('show');
    $('bulk-count').textContent = selectedRows.size + ' selected';
  } else {
    bar.classList.remove('show');
  }
}

$('bulk-deselect').onclick = () => {
  selectedRows.clear();
  lastSelectedRow = -1;
  updateRowSelection();
};

$('bulk-delete').onclick = async () => {
  if (readOnlyMode) { toast('Read-only mode', 'error'); return; }
  if (selectedRows.size === 0) return;
  const ok = await confirm('Delete ' + selectedRows.size + ' rows', 'Are you sure you want to delete ' + selectedRows.size + ' rows from "' + currentTable + '"?');
  if (!ok) return;

  let deleted = 0;
  for (const ri of [...selectedRows].sort((a, b) => b - a)) {
    let pkCol, pkVal;
    if (currentPkMode === 'ctid') {
      pkCol = 'ctid';
      const ctidIdx = currentColumns.indexOf('ctid');
      pkVal = currentRows[ri][ctidIdx];
    } else {
      pkCol = currentPkColumns[0] || currentColumns[0];
      const pkIdx = currentColumns.indexOf(pkCol);
      pkVal = currentRows[ri][pkIdx];
    }
    try {
      const data = await fetchJson('/api/delete-row', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ table: currentTable, pk_column: pkCol, pk_value: pkVal, pk_mode: currentPkMode })
      });
      if (!data.error) deleted++;
    } catch(e) { /* continue deleting remaining rows */ }
  }
  selectedRows.clear();
  lastSelectedRow = -1;
  if (deleted > 0) toast('Deleted ' + deleted + ' rows', 'success');
  else toast('Failed to delete rows', 'error');
  bumpJournal();
  loadTableData();
};

$('bulk-copy-json').onclick = () => {
  if (selectedRows.size === 0) return;
  const cols = displayCols();
  const objs = [...selectedRows].sort((a, b) => a - b).map(ri => {
    const obj = {};
    cols.forEach(col => { obj[col] = currentRows[ri][currentColumns.indexOf(col)]; });
    return obj;
  });
  copyToClipboard(JSON.stringify(objs, null, 2)).then(() => toast('Copied ' + objs.length + ' rows as JSON', 'success')).catch(() => {});
};

$('bulk-copy-sql').onclick = () => {
  if (selectedRows.size === 0) return;
  const cols = displayCols();
  const stmts = [...selectedRows].sort((a, b) => a - b).map(ri => {
    const vals = cols.map(col => {
      const v = currentRows[ri][currentColumns.indexOf(col)];
      if (v === null || v === 'NULL' || v === undefined) return 'NULL';
      return "'" + String(v).replace(/'/g, "''") + "'";
    });
    return 'INSERT INTO "' + currentTable + '" (' + cols.map(c => '"' + c.replace(/"/g, '""') + '"').join(', ') + ') VALUES (' + vals.join(', ') + ');';
  });
  copyToClipboard(stmts.join('\n')).then(() => toast('Copied ' + stmts.length + ' INSERT statements', 'success')).catch(() => {});
};

// ── Row Detail Modal ──
function showRowDetail(rowIdx) {
  const cols = displayCols();
  const meta = getTableMeta(currentTable);
  $('detail-title').textContent = currentTable + ' — Row ' + (pageOffset + rowIdx + 1);

  let html = '';
  cols.forEach(col => {
    const idx = currentColumns.indexOf(col);
    const val = currentRows[rowIdx][idx];
    const isNull = val === null || val === 'NULL' || val === undefined;
    const cm = meta ? meta.columns.find(c => c.name === col) : null;
    const type = cm ? (cm.type || '') : '';
    html += '<div class="rd-label">' + escHtml(col) + ' <span style="font-weight:400;text-transform:none;font-size:10px;color:var(--text-muted)">' + escHtml(type) + '</span></div>';
    html += '<div class="rd-value' + (isNull ? ' null' : '') + '">' + (isNull ? 'NULL' : escHtml(String(val))) + '</div>';
  });
  $('detail-grid').innerHTML = html;
  $('detail-overlay').classList.add('open');
  trapFocus($('detail-overlay'));
}

$('detail-copy').onclick = () => {
  const cols = displayCols();
  // Get row index from the title
  const titleText = $('detail-title').textContent;
  const match = titleText.match(/Row (\d+)/);
  if (!match) return;
  const rowIdx = parseInt(match[1]) - 1 - pageOffset;
  if (rowIdx < 0 || rowIdx >= currentRows.length) return;
  const obj = {};
  cols.forEach(col => { obj[col] = currentRows[rowIdx][currentColumns.indexOf(col)]; });
  copyToClipboard(JSON.stringify(obj, null, 2)).then(() => toast('Row copied as JSON', 'success')).catch(() => {});
};
$('detail-overlay').onclick = e => { if (e.target === $('detail-overlay')) { releaseFocus($('detail-overlay')); $('detail-overlay').classList.remove('open'); } };

// ── Import CSV ──
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
  if (!csv) { toast('Paste CSV data', 'error'); return; }
  const hasHeader = $('import-header').checked;

  releaseFocus($('import-overlay'));
  $('import-overlay').classList.remove('open');

  try {
    const data = await fetchJson('/api/tables/' + encodeURIComponent(currentTable) + '/import', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ csv, has_header: hasHeader })
    });
    if (data.error) toast(data.error, 'error');
    else { toast('Imported ' + (data.imported || 0) + ' rows', 'success'); loadTableData(); }
  } catch(e) { toast('Import failed', 'error'); }
};

$('import-cancel').onclick = () => { releaseFocus($('import-overlay')); $('import-overlay').classList.remove('open'); };
$('import-overlay').onclick = e => { if (e.target === $('import-overlay')) { releaseFocus($('import-overlay')); $('import-overlay').classList.remove('open'); } };

// ── Table Stats ──
$('btn-stats').onclick = async () => {
  if (!currentTable) return;
  try {
    const data = await fetchJson('/api/tables/' + encodeURIComponent(currentTable) + '/stats');
    if (data.error) { toast(data.error, 'error'); return; }

    const btn = $('btn-stats');
    const rect = btn.getBoundingClientRect();
    let tip = document.querySelector('.stats-tip');
    if (tip) tip.remove();

    tip = document.createElement('div');
    tip.className = 'stats-tip';
    tip.style.top = (rect.bottom + 4) + 'px';
    tip.style.right = (window.innerWidth - rect.right) + 'px';
    tip.innerHTML =
      '<div class="stat-row"><span class="stat-label">Rows (est.)</span><span class="stat-val">' + escHtml(String(data.row_estimate || '?')) + '</span></div>' +
      '<div class="stat-row"><span class="stat-label">Table size</span><span class="stat-val">' + escHtml(data.table_size || '?') + '</span></div>' +
      '<div class="stat-row"><span class="stat-label">Index size</span><span class="stat-val">' + escHtml(data.index_size || '?') + '</span></div>' +
      '<div class="stat-row"><span class="stat-label">Total size</span><span class="stat-val">' + escHtml(data.total_size || '?') + '</span></div>';
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
  } catch(e) { toast('Failed to load stats', 'error'); }
};

// ── Truncate Table ──
$('btn-truncate').onclick = async () => {
  if (!currentTable || !dbConnected) return;
  const ok = await confirm('Truncate Table', 'TRUNCATE TABLE "' + currentTable + '" — This will delete ALL rows. This cannot be undone.');
  if (!ok) return;
  try {
    const data = await fetchJson('/api/tables/' + encodeURIComponent(currentTable) + '/truncate', { method: 'POST' });
    if (data.error) { toast(data.error, 'error'); return; }
    toast('Table ' + currentTable + ' truncated', 'success');
    bumpJournal();
    loadTableData();
  } catch(e) { toast('Truncate failed', 'error'); }
};

// ── SQL Formatter ──
$('btn-format-sql').onclick = () => {
  const sql = sqlEditor.value.trim();
  if (!sql) return;
  sqlEditor.value = formatSQL(sql);
  syncHighlight();
  toast('SQL formatted', 'success');
};

function formatSQL(sql) {
  // Simple SQL formatter: uppercase keywords, add newlines before major clauses
  const majorClauses = ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN',
    'INNER JOIN', 'OUTER JOIN', 'FULL JOIN', 'CROSS JOIN', 'ON', 'GROUP BY', 'ORDER BY',
    'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'UNION ALL', 'INSERT INTO', 'VALUES',
    'UPDATE', 'SET', 'DELETE FROM', 'CREATE TABLE', 'ALTER TABLE', 'DROP TABLE',
    'WITH', 'RETURNING', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'];

  // Tokenize preserving strings and comments
  let i = 0;
  const tokens = [];
  while (i < sql.length) {
    if (sql[i] === "'" ) {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === "'" && sql[j+1] === "'") j += 2;
        else if (sql[j] === "'") { j++; break; }
        else j++;
      }
      tokens.push({ type: 'string', text: sql.slice(i, j) });
      i = j;
    } else if (sql[i] === '-' && sql[i+1] === '-') {
      const end = sql.indexOf('\n', i);
      tokens.push({ type: 'comment', text: end === -1 ? sql.slice(i) : sql.slice(i, end) });
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
      if (ti + 2 < tokens.length && tokens[ti + 1].type === 'ws' && tokens[ti + 2].type === 'word') {
        twoWord = upper + ' ' + tokens[ti + 2].text.toUpperCase();
      }
      if (majorClauses.includes(twoWord)) {
        if (['SELECT', 'INSERT INTO', 'UPDATE', 'DELETE FROM', 'CREATE TABLE', 'WITH'].includes(twoWord)) {
          if (formatted.trim()) formatted += '\n';
        } else {
          formatted += '\n';
        }
        formatted += twoWord;
        ti += 2; // skip ws + second word
        continue;
      }
      if (majorClauses.includes(upper)) {
        const isSubClause = ['AND', 'OR', 'ON', 'WHEN', 'THEN', 'ELSE'].includes(upper);
        if (['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'WITH'].includes(upper)) {
          if (formatted.trim()) formatted += '\n';
        } else {
          formatted += '\n' + '  '.repeat(isSubClause ? 1 : 0);
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
      if (formatted.length > 0 && !formatted.endsWith(' ') && !formatted.endsWith('\n')) {
        formatted += ' ';
      }
    } else {
      formatted += t.text;
    }
  }
  return formatted.trim();
}

// ── Saved Queries ──
function getSavedQueries() {
  try { return JSON.parse(localStorage.getItem('lux-saved-queries') || '[]'); }
  catch(e) { return []; }
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
  if (!sql) { toast('Write a query first', 'error'); return; }
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
    list.innerHTML = '<div style="padding:32px;text-align:center;color:var(--text-muted)">No saved queries yet.<br>Write a query and click "Save Current".</div>';
    return;
  }
  let html = '';
  queries.forEach((q, i) => {
    const time = new Date(q.saved_at);
    html += '<div class="history-entry" data-idx="' + i + '">';
    html += '<div class="sql-preview" style="font-weight:500;color:var(--text-bright)">' + escHtml(q.name) + '</div>';
    html += '<div class="sql-preview" style="font-size:11px;margin-top:2px">' + escHtml(q.sql.substring(0, 120)) + '</div>';
    html += '<div class="history-meta">';
    html += '<span>' + time.toLocaleDateString() + '</span>';
    html += '<span style="cursor:pointer;color:var(--error)" data-del="' + i + '">Delete</span>';
    html += '</div></div>';
  });
  list.innerHTML = html;

  list.querySelectorAll('.history-entry').forEach(el => {
    el.addEventListener('click', (e) => {
      if (e.target.dataset.del != null) {
        const idx = parseInt(e.target.dataset.del);
        const qs = getSavedQueries();
        qs.splice(idx, 1);
        setSavedQueries(qs);
        renderSavedQueries();
        toast('Query deleted', 'info');
        return;
      }
      const idx = parseInt(el.dataset.idx);
      const qs = getSavedQueries();
      sqlEditor.value = qs[idx].sql;
      syncHighlight();
      releaseFocus($('saved-overlay'));
      $('saved-overlay').classList.remove('open');
      $$('.tab').forEach(t => t.classList.remove('active'));
      $$('.tab-panel').forEach(p => p.classList.remove('active'));
      document.querySelector('[data-tab="sql"]').classList.add('active');
      $('panel-sql').classList.add('active');
      sqlEditor.focus();
    });
  });
}

$('saved-overlay').onclick = e => { if (e.target === $('saved-overlay')) { releaseFocus($('saved-overlay')); $('saved-overlay').classList.remove('open'); } };

// ── Sidebar Resize ──
(function() {
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
    sidebar.style.width = newW + 'px';
  });

  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    handle.classList.remove('dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  });
})();

// ── SQL Editor Resize ──
(function() {
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
    wrap.style.height = newH + 'px';
  });

  document.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    handle.classList.remove('dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
  });
})();

// ── Keyboard Shortcuts ──
document.addEventListener('keydown', e => {
  // Ctrl+L: focus SQL editor
  if ((e.ctrlKey || e.metaKey) && e.key === 'l') {
    e.preventDefault();
    $$('.tab').forEach(t => t.classList.remove('active'));
    $$('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelector('[data-tab="sql"]').classList.add('active');
    $('panel-sql').classList.add('active');
    sqlEditor.focus();
  }
  // Ctrl+F: find & replace
  if ((e.ctrlKey || e.metaKey) && e.key === 'f' && currentTable && dbConnected) {
    if ($('panel-tables').classList.contains('active')) {
      e.preventDefault();
      $('btn-find-replace').click();
    }
  }
  // Escape: close modals in priority order (topmost first)
  if (e.key === 'Escape') {
    const modals = ['cmd-overlay', 'prompt-overlay', 'confirm-overlay', 'detail-overlay', 'ddl-overlay', 'shortcuts-overlay',
                    'history-overlay', 'saved-overlay', 'save-conn-overlay', 'create-table-overlay',
                    'import-overlay', 'fnr-overlay', 'insert-overlay'];
    for (const id of modals) {
      const el = $(id);
      if (el && (el.classList.contains('open') || el.classList.contains('visible'))) {
        if (id === 'prompt-overlay' && promptResolve) { promptResolve(null); }
        if (id === 'confirm-overlay' && confirmResolve) { confirmResolve(false); }
        releaseFocus(el);
        el.classList.remove('open');
        el.classList.remove('visible');
        e.preventDefault();
        return; // Only close the topmost one
      }
    }
    $('ctx-menu').classList.remove('open');
    const tip = document.querySelector('.stats-tip');
    if (tip) { tip.remove(); return; }
    if (editingCell) { cancelEdit(); return; }
    if (selectedRows.size > 0) { selectedRows.clear(); lastSelectedRow = -1; updateRowSelection(); }
    else { focusRow = -1; focusCol = -1; updateCellFocus(); }
  }
  // ? key: show shortcuts (when not typing)
  if (e.key === '?' && !editingCell && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA' && document.activeElement.tagName !== 'SELECT') {
    e.preventDefault();
    $('shortcuts-overlay').classList.add('open');
  }
  // Ctrl+N: new SQL tab
  if ((e.ctrlKey || e.metaKey) && e.key === 'n') {
    e.preventDefault();
    addSqlTab();
    $$('.tab').forEach(t => t.classList.remove('active'));
    $$('.tab-panel').forEach(p => p.classList.remove('active'));
    document.querySelector('[data-tab="sql"]').classList.add('active');
    $('panel-sql').classList.add('active');
  }
  // Ctrl+S: save connection
  if ((e.ctrlKey || e.metaKey) && e.key === 's') {
    e.preventDefault();
    if (dbConnected) $('save-conn-btn').click();
  }
  // Grid arrow key navigation
  if (!editingCell && $('panel-tables').classList.contains('active') && currentRows.length > 0 &&
      document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
    const dCols = displayCols();
    const maxRow = currentRows.length - 1;
    const maxCol = dCols.length - 1;
    if (e.key === 'ArrowDown') { e.preventDefault(); focusRow = Math.min(focusRow + 1, maxRow); if (focusCol < 0) focusCol = 0; updateCellFocus(); }
    if (e.key === 'ArrowUp') { e.preventDefault(); focusRow = Math.max(focusRow - 1, 0); if (focusCol < 0) focusCol = 0; updateCellFocus(); }
    if (e.key === 'ArrowRight') { e.preventDefault(); focusCol = Math.min(focusCol + 1, maxCol); if (focusRow < 0) focusRow = 0; updateCellFocus(); }
    if (e.key === 'ArrowLeft') { e.preventDefault(); focusCol = Math.max(focusCol - 1, 0); if (focusRow < 0) focusRow = 0; updateCellFocus(); }
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

// ── Init ──
updateDestructiveButtons(false);
async function init() {
  await loadReadOnly();
  try {
    const data = await fetchJson('/api/schema');
    if (data.tables && data.tables.length > 0) {
      schemaData = data;
      dbConnected = true;
      updateConnUI();
      showConnStatus(true, data.tables.length + ' tables');
      renderSidebar();
    }
  } catch(e) { /* no pre-existing connection — show welcome screen */ }
}
init();

window.addEventListener('resize', () => {
  if ($('panel-er').classList.contains('active')) drawER();
});

// ── Connection Manager ──
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
    const colorMap = { green: '#6ee7a0', yellow: '#fbbf4e', red: '#f87171', blue: '#7aa2f7', purple: '#a78bfa' };
    let html = '';
    conns.forEach(c => {
      savedConnsMap.set(c.id, c);
      html += '<div class="saved-conn" data-id="' + escHtml(String(c.id)) + '">';
      html += '<span class="conn-color" style="background:' + (colorMap[c.color] || colorMap.blue) + '"></span>';
      html += '<span class="conn-name">' + escHtml(c.name) + '</span>';
      html += '<span class="conn-del" data-id="' + escHtml(String(c.id)) + '" title="Delete">&times;</span>';
      html += '</div>';
    });
    container.innerHTML = html;

    container.querySelectorAll('.saved-conn').forEach(el => {
      el.addEventListener('click', (e) => {
        if (e.target.classList.contains('conn-del')) {
          e.stopPropagation();
          deleteSavedConnection(parseInt(e.target.dataset.id));
          return;
        }
        const connId = parseInt(el.dataset.id);
        const conn = savedConnsMap.get(connId);
        if (conn) {
          $('conn-input').value = conn.conninfo;
          doConnect();
        }
      });
    });
  } catch(e) { /* saved connections unavailable — non-critical */ }
}

async function deleteSavedConnection(id) {
  try {
    await fetchJson('/api/connections/' + id, { method: 'DELETE' });
    toast('Connection removed', 'info');
    loadSavedConnections();
  } catch(e) { toast('Failed to delete connection', 'error'); }
}

$('save-conn-btn').onclick = () => {
  const conninfo = $('conn-input').value.trim();
  if (!conninfo) { toast('Connect first', 'error'); return; }
  $('save-conn-name').value = '';
  savedConnColor = 'blue';
  $('save-conn-colors').querySelectorAll('span').forEach(s => {
    s.style.borderColor = s.dataset.color === 'blue' ? 'var(--text-primary)' : 'transparent';
  });
  $('save-conn-overlay').classList.add('open');
  trapFocus($('save-conn-overlay'));
};

$('save-conn-colors').querySelectorAll('span').forEach(s => {
  s.addEventListener('click', () => {
    savedConnColor = s.dataset.color;
    $('save-conn-colors').querySelectorAll('span').forEach(x => x.style.borderColor = 'transparent');
    s.style.borderColor = 'var(--text-primary)';
  });
});

$('save-conn-ok').onclick = async () => {
  const name = $('save-conn-name').value.trim();
  if (!name) { toast('Enter a name', 'error'); return; }
  const conninfo = $('conn-input').value.trim();
  try {
    await fetchJson('/api/connections', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ name, conninfo, color: savedConnColor })
    });
    releaseFocus($('save-conn-overlay'));
    $('save-conn-overlay').classList.remove('open');
    toast('Connection saved', 'success');
    loadSavedConnections();
  } catch(e) { toast('Failed to save', 'error'); }
};

$('save-conn-cancel').onclick = () => { releaseFocus($('save-conn-overlay')); $('save-conn-overlay').classList.remove('open'); };
$('save-conn-overlay').onclick = e => { if (e.target === $('save-conn-overlay')) { releaseFocus($('save-conn-overlay')); $('save-conn-overlay').classList.remove('open'); } };

// Load saved connections on init
loadSavedConnections();

// ── SQL Tabs ──
let sqlTabs = [{ id: 0, name: 'Query 1', sql: '', results: '' }];
let activeSqlTab = 0;
let nextSqlTabId = 1;

function renderSqlTabs() {
  const bar = $('sql-tab-bar');
  bar.querySelectorAll('.sql-tab-item').forEach(el => el.remove());
  const addBtn = $('sql-tab-add');

  sqlTabs.forEach(tab => {
    const btn = document.createElement('button');
    btn.className = 'sql-tab-item' + (tab.id === activeSqlTab ? ' active' : '');
    btn.dataset.sqlTab = tab.id;
    btn.innerHTML = escHtml(tab.name);
    if (sqlTabs.length > 1) {
      btn.innerHTML += '<span class="close-tab" data-close="' + tab.id + '">&times;</span>';
    }
    btn.addEventListener('click', (e) => {
      if (e.target.classList.contains('close-tab')) {
        e.stopPropagation();
        closeSqlTab(parseInt(e.target.dataset.close));
        return;
      }
      switchSqlTab(tab.id);
    });
    bar.insertBefore(btn, addBtn);
  });
}

function switchSqlTab(tabId) {
  // Save current tab state
  const current = sqlTabs.find(t => t.id === activeSqlTab);
  if (current) {
    current.sql = sqlEditor.value;
    current.results = $('sql-results').innerHTML;
  }
  // Load new tab
  activeSqlTab = tabId;
  const tab = sqlTabs.find(t => t.id === tabId);
  if (tab) {
    sqlEditor.value = tab.sql;
    syncHighlight();
    $('sql-results').innerHTML = tab.results;
  }
  renderSqlTabs();
}

function addSqlTab() {
  const current = sqlTabs.find(t => t.id === activeSqlTab);
  if (current) {
    current.sql = sqlEditor.value;
    current.results = $('sql-results').innerHTML;
  }
  const id = nextSqlTabId++;
  sqlTabs.push({ id, name: 'Query ' + (sqlTabs.length + 1), sql: '', results: '' });
  activeSqlTab = id;
  sqlEditor.value = '';
  syncHighlight();
  $('sql-results').innerHTML = '<div class="sql-results-info" style="padding:48px;text-align:center;color:var(--text-muted)">Write a query and press Run or Ctrl+Enter</div>';
  renderSqlTabs();
  sqlEditor.focus();
}

async function closeSqlTab(tabId) {
  if (sqlTabs.length <= 1) return;
  const idx = sqlTabs.findIndex(t => t.id === tabId);
  if (idx < 0) return;
  const tab = sqlTabs[idx];
  // Get the live content if this is the active tab
  const tabContent = (tabId === activeSqlTab) ? sqlEditor.value : (tab.sql || '');
  // Warn if tab has unsaved content
  if (tabContent && tabContent.trim().length > 0) {
    const ok = await confirm('Close Tab', 'Close this tab? Unsaved query will be lost.');
    if (!ok) return;
  }
  sqlTabs.splice(idx, 1);
  if (activeSqlTab === tabId) {
    activeSqlTab = sqlTabs[Math.min(idx, sqlTabs.length - 1)].id;
    const tab = sqlTabs.find(t => t.id === activeSqlTab);
    if (tab) {
      sqlEditor.value = tab.sql;
      syncHighlight();
      $('sql-results').innerHTML = tab.results;
    }
  }
  renderSqlTabs();
}

$('sql-tab-add').onclick = addSqlTab;

// ── Column Resizing ──
(function() {
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
    resizeTh.style.width = newW + 'px';
    resizeTh.style.minWidth = newW + 'px';
    resizeTh.style.maxWidth = newW + 'px';
    const col = resizeTh.dataset.col;
    if (col) columnWidths[col] = newW;
  });

  document.addEventListener('mouseup', () => {
    if (!resizing) return;
    resizing = false;
    document.querySelectorAll('.col-resize-handle.dragging').forEach(h => h.classList.remove('dragging'));
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    resizeTh = null;
  });
})();

// ── Table Creation ──
let createTableColCount = 0;

$('btn-create-table').onclick = () => {
  if (!dbConnected) { toast('Connect first', 'error'); return; }
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
    '<input class="col-name-input" placeholder="column_name" data-idx="' + idx + '" spellcheck="false">' +
    '<select class="col-type-select" data-idx="' + idx + '">' +
    '<option>integer</option><option>bigint</option><option>serial</option><option>bigserial</option>' +
    '<option selected>text</option><option>varchar(255)</option><option>boolean</option>' +
    '<option>numeric</option><option>real</option><option>double precision</option>' +
    '<option>date</option><option>timestamp</option><option>timestamptz</option>' +
    '<option>json</option><option>jsonb</option><option>uuid</option>' +
    '</select>' +
    '<label style="display:flex;align-items:center;gap:3px;font-size:10px;color:var(--text-muted);white-space:nowrap"><input type="checkbox" class="col-pk-check" data-idx="' + idx + '" style="accent-color:var(--accent)">PK</label>' +
    '<label style="display:flex;align-items:center;gap:3px;font-size:10px;color:var(--text-muted);white-space:nowrap"><input type="checkbox" class="col-nn-check" data-idx="' + idx + '" style="accent-color:var(--accent)">NN</label>' +
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
  rows.forEach(row => {
    const colName = row.querySelector('.col-name-input').value.trim();
    if (!colName) return;
    const colType = row.querySelector('.col-type-select').value;
    const isPk = row.querySelector('.col-pk-check').checked;
    const isNn = row.querySelector('.col-nn-check').checked;
    let def = '"' + colName.replace(/"/g, '""') + '" ' + colType;
    if (isNn) def += ' NOT NULL';
    cols.push(def);
    if (isPk) pks.push('"' + colName.replace(/"/g, '""') + '"');
  });

  if (cols.length === 0) return null;
  let ddl = 'CREATE TABLE "' + name.replace(/"/g, '""') + '" (\n  ' + cols.join(',\n  ');
  if (pks.length > 0) ddl += ',\n  PRIMARY KEY (' + pks.join(', ') + ')';
  ddl += '\n);';
  return ddl;
}

$('create-table-preview').onclick = () => {
  const ddl = buildCreateTableDDL();
  if (!ddl) { toast('Add table name and columns', 'error'); return; }
  // Show in SQL editor
  sqlEditor.value = ddl.replace(/\\n/g, '\n');
  syncHighlight();
  releaseFocus($('create-table-overlay'));
  $('create-table-overlay').classList.remove('open');
  $$('.tab').forEach(t => t.classList.remove('active'));
  $$('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelector('[data-tab="sql"]').classList.add('active');
  $('panel-sql').classList.add('active');
  toast('DDL preview in SQL editor', 'info');
};

$('create-table-ok').onclick = async () => {
  const ddl = buildCreateTableDDL();
  if (!ddl) { toast('Add table name and columns', 'error'); return; }
  const sql = ddl.replace(/\\n/g, '\n');
  releaseFocus($('create-table-overlay'));
  $('create-table-overlay').classList.remove('open');
  try {
    const data = await fetchJson('/api/sql', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ sql, force: 'true' })
    });
    if (data.error) toast(data.error, 'error');
    else {
      toast('Table created', 'success');
      await loadSchema();
    }
  } catch(e) { toast('Create failed', 'error'); }
};

$('create-table-cancel').onclick = () => { releaseFocus($('create-table-overlay')); $('create-table-overlay').classList.remove('open'); };
$('create-table-overlay').onclick = e => { if (e.target === $('create-table-overlay')) { releaseFocus($('create-table-overlay')); $('create-table-overlay').classList.remove('open'); } };

// ── Health Check & Reconnect ──
let healthCheckInterval = null;

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
    } catch(e) {
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
  } catch(e) { toast('Reconnect failed', 'error'); }
  $('reconnect-btn').textContent = 'Reconnect';
  $('reconnect-btn').disabled = false;
};

// ── SQL Results Export ──
async function exportSqlResults(format) {
  const sql = lastSqlQuery;
  if (!sql) { toast('No query to export', 'error'); return; }
  try {
    const res = await fetch('/api/sql/export', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ query: sql, format })
    });
    if (!res.ok) throw new Error('Export failed: ' + res.status);
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'export.' + format;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    toast('Exported as ' + format.toUpperCase(), 'success');
  } catch(e) { toast('Export failed', 'error'); }
}

// Ctrl+N and Ctrl+S are handled in the main keydown handler above

// ── Connection Color Stripe ──
function setConnStripe(color) {
  const stripe = $('conn-stripe');
  stripe.className = 'conn-stripe';
  if (color) { stripe.classList.add(color); }
}
// Auto-detect color from conninfo
function detectConnColor(conninfo) {
  const ci = (conninfo || '').toLowerCase();
  if (ci.includes('prod') || ci.includes('production')) return 'red';
  if (ci.includes('stag') || ci.includes('staging') || ci.includes('preprod')) return 'yellow';
  if (ci.includes('local') || ci.includes('127.0.0.1') || ci.includes('localhost') || ci.includes('dev')) return 'green';
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

// ── Command Palette (Ctrl+K) ──
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
  const tblIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="9" y1="3" x2="9" y2="21"/></svg>';
  const actionIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>';
  const tabIcon = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>';

  // Tables
  if (schemaData && schemaData.tables) {
    schemaData.tables.forEach(t => {
      if (!q || t.name.toLowerCase().includes(q)) {
        results.push({
          icon: tblIcon, label: t.name,
          hint: (t.columns ? t.columns.length : 0) + ' cols',
          action: () => { selectTableFromCmd(t.name); closeCmdPalette(); }
        });
      }
      // Also search columns
      if (q && t.columns) {
        t.columns.forEach(c => {
          if (c.name.toLowerCase().includes(q)) {
            results.push({
              icon: tblIcon, label: t.name + '.' + c.name,
              hint: c.type || '',
              action: () => { selectTableFromCmd(t.name); closeCmdPalette(); }
            });
          }
        });
      }
    });
  }

  // Actions
  const actions = [
    { label: 'New SQL Tab', hint: 'Ctrl+N', action: () => { addSqlTab(); switchToSqlTab(); closeCmdPalette(); } },
    { label: 'Toggle Read-Only', hint: '', action: () => { $('btn-readonly').click(); closeCmdPalette(); } },
    { label: 'Toggle Theme', hint: '', action: () => { $('btn-theme').click(); closeCmdPalette(); } },
    { label: 'Refresh Schema', hint: '', action: () => { $('btn-refresh').click(); closeCmdPalette(); } },
    { label: 'Query History', hint: '', action: () => { $('btn-sql-history').click(); closeCmdPalette(); } },
    { label: 'Keyboard Shortcuts', hint: '?', action: () => { $('shortcuts-overlay').classList.add('open'); closeCmdPalette(); } },
    { label: 'ER Diagram', hint: '', action: () => { switchToTab('er'); closeCmdPalette(); } },
    { label: 'Change Journal', hint: '', action: () => { switchToTab('journal'); closeCmdPalette(); } },
  ];
  actions.forEach(a => {
    if (!q || a.label.toLowerCase().includes(q)) {
      results.push({ icon: actionIcon, label: a.label, hint: a.hint, action: a.action });
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
  tree.querySelectorAll('.tree-table').forEach(el => {
    el.classList.remove('active');
    if (el.dataset.table === name) el.classList.add('active');
  });
  selectTable(name);
}

function switchToTab(tabName) {
  $$('.tab').forEach(t => t.classList.remove('active'));
  $$('.tab-panel').forEach(p => p.classList.remove('active'));
  const tab = document.querySelector('[data-tab="' + tabName + '"]');
  if (tab) tab.classList.add('active');
  $('panel-' + tabName).classList.add('active');
  if (tabName === 'journal') { loadJournal(); clearJournalBadge(); }
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
  container.innerHTML = cmdItems.slice(0, 20).map((item, i) =>
    '<div class="cmd-item' + (i === cmdIndex ? ' selected' : '') + '" data-idx="' + i + '">' +
    // item.icon is always a hardcoded SVG string, never user data
    '<div class="cmd-icon">' + item.icon + '</div>' +
    '<span class="cmd-label">' + escHtml(item.label) + '</span>' +
    '<span class="cmd-hint">' + escHtml(item.hint) + '</span>' +
    '</div>'
  ).join('');

  container.querySelectorAll('.cmd-item').forEach(el => {
    el.addEventListener('click', () => {
      const idx = parseInt(el.dataset.idx);
      if (cmdItems[idx]) cmdItems[idx].action();
    });
    el.addEventListener('mousemove', () => {
      cmdIndex = parseInt(el.dataset.idx);
      container.querySelectorAll('.cmd-item').forEach((item, i) => {
        item.classList.toggle('selected', i === cmdIndex);
      });
    });
  });

  // Scroll selected into view
  const selected = container.querySelector('.cmd-item.selected');
  if (selected) selected.scrollIntoView({ block: 'nearest' });
}

$('cmd-input').addEventListener('input', () => buildCmdResults($('cmd-input').value));
$('cmd-input').addEventListener('keydown', e => {
  if (e.key === 'ArrowDown') { e.preventDefault(); cmdIndex = Math.min(cmdIndex + 1, Math.min(cmdItems.length - 1, 19)); renderCmdResults(); }
  if (e.key === 'ArrowUp') { e.preventDefault(); cmdIndex = Math.max(cmdIndex - 1, 0); renderCmdResults(); }
  if (e.key === 'Enter' && cmdItems.length > 0) { e.preventDefault(); cmdItems[cmdIndex].action(); }
  if (e.key === 'Escape') { e.preventDefault(); closeCmdPalette(); }
});
$('cmd-overlay').addEventListener('click', e => { if (e.target === $('cmd-overlay')) closeCmdPalette(); });
$('btn-cmd-palette').onclick = openCmdPalette;

// Ctrl+K global shortcut
document.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
    e.preventDefault();
    if ($('cmd-overlay').classList.contains('open')) closeCmdPalette();
    else openCmdPalette();
  }
});

// Enhanced journal display for deletes is integrated directly into loadJournal above

// Wire up modal close buttons (delegated from HTML class="modal-close-btn")
document.querySelectorAll('.modal-close-btn').forEach(function(btn) {
  btn.addEventListener('click', function() {
    const overlay = btn.closest('.modal-overlay');
    if (overlay) { releaseFocus(overlay); overlay.classList.remove('open'); }
  });
});

// Mobile sidebar toggle
(function() {
  const toggle = $('sidebar-toggle');
  const sidebar = document.querySelector('.sidebar');
  if (toggle && sidebar) {
    toggle.addEventListener('click', function() {
      sidebar.classList.toggle('open');
    });
    // Close sidebar when clicking outside on mobile
    document.addEventListener('click', function(e) {
      if (sidebar.classList.contains('open') && !sidebar.contains(e.target) && !toggle.contains(e.target)) {
        sidebar.classList.remove('open');
      }
    });
  }
})();

})();
