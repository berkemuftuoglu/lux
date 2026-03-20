// connection.js — Connection management: connect, status, saved connections, health check, stripe
// Loaded after state.js and utils.js, before app.js.

let savedConnColor = 'blue';
let savedConnsMap = new Map();

function buildConnectionString(host, port, db, user, pass) {
	let url = 'postgresql://';
	if (user) {
		url += encodeURIComponent(user);
		if (pass) url += ':' + encodeURIComponent(pass);
		url += '@';
	}
	url += host || 'localhost';
	if (port && port !== '5432') url += ':' + port;
	url += '/' + (db || 'postgres');
	return url;
}

function parseConnectionString(url) {
	try {
		const u = new URL(url);
		return {
			host: u.hostname || 'localhost',
			port: u.port || '5432',
			database: u.pathname.slice(1) || 'postgres',
			username: decodeURIComponent(u.username || ''),
			password: decodeURIComponent(u.password || ''),
		};
	} catch (_e) {
		return { host: 'localhost', port: '5432', database: 'postgres', username: '', password: '' };
	}
}

function getConninfo() {
	const rawEl = $('conn-input');
	const rawMode = $('conn-raw') && $('conn-raw').style.display !== 'none';
	if (rawMode && rawEl && rawEl.value.trim()) {
		return rawEl.value.trim();
	}
	const host = ($('conn-host') && $('conn-host').value.trim()) || 'localhost';
	const port = ($('conn-port') && $('conn-port').value.trim()) || '5432';
	const db = ($('conn-database') && $('conn-database').value.trim()) || 'postgres';
	const user = $('conn-user') ? $('conn-user').value.trim() : '';
	const pass = $('conn-pass') ? $('conn-pass').value : '';
	if (!host && !user && !db) return rawEl ? rawEl.value.trim() : '';
	return buildConnectionString(host, port, db, user, pass);
}

async function doConnect() {
	const conninfo = getConninfo();
	if (!conninfo) return;
	const connectBtns = [
		$('conn-btn'), $('conn-btn-raw')
	].filter(Boolean);
	connectBtns.forEach(b => { b.disabled = true; b.textContent = '...'; });
	try {
		const data = await postJson('/api/connect', { conninfo });
		if (data.error) {
			showConnStatus(false, data.error);
			toast(data.error, 'error');
		} else {
			const parsed = parseConnectionString(conninfo);
			// Keep conn-input in sync for saved connections lookup
			if ($('conn-input')) $('conn-input').value = conninfo;
			showConnStatus(true, `${data.tables} tables`);
			setConnected({ host: parsed.host, port: parsed.port, database: parsed.database, conninfo });
			toast('Connected', 'success');
		}
	} catch (e) {
		showConnStatus(false, e.message);
		toast('Connection failed', 'error');
	}
	connectBtns.forEach(b => { b.disabled = false; b.textContent = 'Connect'; });
}

function showConnStatus(ok, msg) {
	$('conn-status').style.display = 'flex';
	$('conn-dot').className = `dot ${ok ? 'ok' : 'err'}`;
	$('conn-msg').textContent = msg;
	pulseDot($('conn-dot'));
	const statusDot = document.querySelector('.status-bar .dot');
	if (statusDot) pulseDot(statusDot);
}

function updateConnUI() {
	$('hdr-dot').classList.toggle('connected', State.dbConnected);
	$('status-dot').classList.toggle('ok', State.dbConnected);
	if (State.dbConnected && State.host) {
		$('status-conn').textContent = State.host + ':' + (State.port || '5432');
	} else {
		$('status-conn').textContent = State.dbConnected ? 'Connected' : 'Not connected';
	}
	$('save-conn-btn').style.display = State.dbConnected ? '' : 'none';
}

function startHealthCheck() {
	if (State.healthCheckInterval) clearInterval(State.healthCheckInterval);
	State.healthCheckInterval = setInterval(async () => {
		if (!State.dbConnected) return;
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

function getConnStatus(conn) {
	if (!State.dbConnected) return 'offline';
	const currentConninfo = $('conn-input').value.trim();
	if (conn.conninfo === currentConninfo) return 'connected';
	return 'idle';
}

const dbIconSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4.03 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"/></svg>';
const settingsIconSvg = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>';

function buildCardElement(c) {
	const status = getConnStatus(c);
	const isConnected = status === 'connected';
	const card = document.createElement('div');
	card.className = 'conn-card' + (status === 'offline' ? ' offline' : '');
	card.dataset.id = String(c.id);

	// Derive hostname from fields or conninfo
	let hostname = 'localhost';
	if (c.host) {
		hostname = c.host;
		if (c.port && c.port !== '5432') hostname += ':' + c.port;
	} else if (c.conninfo) {
		const parsed = parseConnectionString(c.conninfo);
		hostname = parsed.host;
		if (parsed.port !== '5432') hostname += ':' + parsed.port;
	}
	const desc = c.database || (c.conninfo ? parseConnectionString(c.conninfo).database : 'postgres');

	const iconDiv = h('div', { cls: 'card-icon' });
	iconDiv.innerHTML = dbIconSvg;
	const header = h('div', { cls: 'card-header' },
		iconDiv,
		h('span', { cls: 'card-status ' + status, text: status.toUpperCase() })
	);

	const hostnameEl = h('div', { cls: 'card-hostname', text: hostname });
	const descEl = h('div', { cls: 'card-desc', text: c.name + ' / ' + desc });

	const connectBtn = h('button', {
		cls: 'card-btn card-connect-btn',
		text: isConnected ? 'DISCONNECT' : 'CONNECT',
		onclick: (e) => {
		e.stopPropagation();
		if ($('conn-input')) $('conn-input').value = c.conninfo;
		const parsed = parseConnectionString(c.conninfo);
		if ($('conn-host')) $('conn-host').value = parsed.host;
		if ($('conn-port')) $('conn-port').value = parsed.port;
		if ($('conn-database')) $('conn-database').value = parsed.database;
		if ($('conn-user')) $('conn-user').value = parsed.username;
		if ($('conn-pass')) $('conn-pass').value = parsed.password;
		doConnect();
	}});

	const settingsBtn = h('button', {
		cls: 'card-btn-settings',
		title: 'Delete connection',
		onclick: (e) => {
			e.stopPropagation();
			deleteSavedConnection(c.id);
		}
	});
	settingsBtn.innerHTML = settingsIconSvg;

	const actions = h('div', { cls: 'card-actions' }, connectBtn, settingsBtn);

	card.appendChild(header);
	card.appendChild(hostnameEl);
	card.appendChild(descEl);
	card.appendChild(actions);

	return card;
}

function renderSavedConnectionCards() {
	const container = $('saved-conns');
	if (savedConnsMap.size === 0) {
		container.classList.remove('show');
		container.classList.remove('conn-cards-grid');
		return;
	}
	container.classList.add('show');
	container.classList.add('conn-cards-grid');
	container.textContent = '';

	for (const c of savedConnsMap.values()) {
		container.appendChild(buildCardElement(c));
	}
}

async function loadSavedConnections() {
	try {
		const resp = await fetchJson('/api/connections');
		const conns = resp.connections || [];
		const container = $('saved-conns');
		if (!Array.isArray(conns) || conns.length === 0) {
			container.classList.remove('show');
			container.classList.remove('conn-cards-grid');
			savedConnsMap.clear();
			return;
		}
		savedConnsMap = new Map();
		conns.forEach((c) => {
			savedConnsMap.set(c.id, c);
		});
		renderSavedConnectionCards();
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

function setConnStripe(color) {
	const stripe = $('conn-stripe');
	stripe.className = 'conn-stripe';
	if (color) {
		stripe.classList.add(color);
	}
}

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

function ensureSaveConnFields() {
	// Fields now live in HTML — nothing to create dynamically
}

$('save-conn-btn').onclick = () => {
	const conninfo = getConninfo();
	if (!conninfo) {
		toast('Connect first', 'error');
		return;
	}

	const parsed = parseConnectionString(conninfo);
	$('save-conn-name').value = '';
	$('save-conn-host').value = parsed.host;
	$('save-conn-port').value = parsed.port;
	$('save-conn-database').value = parsed.database;
	$('save-conn-username').value = parsed.username;
	$('save-conn-password').value = parsed.password;

	savedConnColor = 'blue';
	$('save-conn-colors')
		.querySelectorAll('span')
		.forEach((s) => {
			s.style.borderColor =
				s.dataset.color === 'blue' ? 'var(--text-primary)' : 'transparent';
		});
	openModal('save-conn-overlay');
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

	const host = $('save-conn-host').value.trim() || 'localhost';
	const port = $('save-conn-port').value.trim() || '5432';
	const database = $('save-conn-database').value.trim() || 'postgres';
	const username = $('save-conn-username').value.trim() || '';
	const password = $('save-conn-password').value || '';
	const conninfo = buildConnectionString(host, port, database, username, password);

	try {
		await postJson('/api/connections', {
			name,
			conninfo,
			color: savedConnColor,
			host,
			port,
			database,
			username,
			password,
		});
		releaseFocus($('save-conn-overlay'));
		closeModal('save-conn-overlay');
		toast('Connection saved', 'success');
		loadSavedConnections();
	} catch (e) {
		toast(`Failed to save: ${e.message}`, 'error');
	}
};

$('save-conn-cancel').onclick = () => {
	releaseFocus($('save-conn-overlay'));
	closeModal('save-conn-overlay');
};
$('save-conn-overlay').onclick = (e) => {
	if (e.target === $('save-conn-overlay')) {
		releaseFocus($('save-conn-overlay'));
		closeModal('save-conn-overlay');
	}
};

$('reconnect-btn').onclick = async () => {
	$('reconnect-btn').textContent = '...';
	$('reconnect-btn').disabled = true;
	try {
		const data = await fetchJson('/api/reconnect', { method: 'POST' });
		if (data.error) {
			toast(data.error, 'error');
		} else {
			$('reconnect-banner').classList.remove('show');
			setConnected({ host: State.host, port: State.port, database: State.database });
			toast('Reconnected', 'success');
		}
	} catch (e) {
		toast(`Reconnect failed: ${e.message}`, 'error');
	}
	$('reconnect-btn').textContent = 'Reconnect';
	$('reconnect-btn').disabled = false;
};
