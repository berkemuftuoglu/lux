// state.js — Shared mutable state (loaded first in index.html)
// All variables here are cross-file globals. They must be `let` so
// any JS file can reassign them. Do not change to `const`.

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
let erPanX = 0;
let erPanY = 0;
let focusRow = -1;
let focusCol = -1;
let columnFilters = {};
const columnWidths = {};
let selectedRows = new Set();
let lastSelectedRow = -1;
let lastSqlQuery = '';
let healthCheckInterval = null;
