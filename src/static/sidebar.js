'use strict';

// Schema
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

// Table Selection & Data
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
    $('status-table').textContent = currentTable + ' \u00b7 ' + totalRows.toLocaleString() + ' rows';

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
  // Status-like: done/complete/closed/resolved -> success
  if (/done|complete|closed|resolved|active|enabled/.test(v)) return 'pill-success';
  // In-progress/open -> accent
  if (/in.progress|in_progress|open|started|running|pending/.test(v)) return 'pill-accent';
  // Cancelled/blocked/failed/disabled -> error
  if (/cancel|blocked|failed|disabled|rejected|archived/.test(v)) return 'pill-error';
  // Priority: urgent/critical -> error, high -> warning, medium -> accent, low/none -> muted
  if (/urgent|critical/.test(v)) return 'pill-error';
  if (/high/.test(v)) return 'pill-warning';
  if (/medium/.test(v)) return 'pill-accent';
  if (/low|none|trivial/.test(v)) return 'pill-muted';
  // Todo/backlog -> subtle
  if (/todo|backlog|draft|review|planned/.test(v)) return 'pill-subtle';
  return 'pill-default';
}

// ER Diagram
let erPositions = []; // stored for hover detection
let erTablePos = {};
let erHoveredTable = null;
let erLayoutCache = null; // cached force-directed layout

// Force-directed graph layout -- computes non-overlapping positions
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

  // Initialize positions -- circular for connected tables, row below for isolated
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

    // Repulsion -- push all pairs apart (rectangle-aware)
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

    // Attraction -- FK-connected tables pull toward each other
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

    // Centering gravity -- gentle pull toward center
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

    // Hard overlap resolution -- push overlapping boxes apart
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

  // Canvas sizing -- fit all content
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

  // FK line drawing -- bezier curves with smart exit side
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
    const badge = t.columns.length + ' cols' + (incomingCount > 0 ? ' \u00b7 ' + incomingCount + ' links' : '');
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
