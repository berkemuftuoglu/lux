// connection.js — Connection management: connect, status, saved connections, health check, stripe
// Loaded after state.js and utils.js, before app.js.

let savedConnColor = 'blue';
let savedConnsMap = new Map();

async function doConnect() {
	const conninfo = $('conn-input').value.trim();
	if (!conninfo) return;
	$('conn-btn').disabled = true;
	$('conn-btn').textContent = '...';
	try {
		const data = await fetchJson('/api/connect', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ conninfo }),
		});
		if (data.error) {
			showConnStatus(false, data.error);
			toast(data.error, 'error');
		} else {
			dbConnected = true;
			showConnStatus(true, `${data.tables} tables`);
			updateConnUI();
			await loadSchema();
			toast('Connected', 'success');
			startHealthCheck();
			const connStr = $('conn-input').value.trim();
			const savedColor = getSavedConnColor(connStr);
			setConnStripe(savedColor || detectConnColor(connStr));
		}
	} catch (e) {
		showConnStatus(false, e.message);
		toast('Connection failed', 'error');
	}
	$('conn-btn').disabled = false;
	$('conn-btn').textContent = 'Connect';
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
	$('hdr-dot').classList.toggle('connected', dbConnected);
	$('status-dot').classList.toggle('ok', dbConnected);
	$('status-conn').textContent = dbConnected ? 'Connected' : 'Not connected';
	$('save-conn-btn').style.display = dbConnected ? '' : 'none';
}

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
		} catch (_e) {
			$('reconnect-banner').classList.add('show');
		}
	}, HEALTH_CHECK_INTERVAL);
}

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
		const colorMap = {
			green: '#6ee7a0',
			yellow: '#fbbf4e',
			red: '#f87171',
			blue: '#7aa2f7',
			purple: '#a78bfa',
		};
		let html = '';
		conns.forEach((c) => {
			savedConnsMap.set(c.id, c);
			html += `<div class="saved-conn" data-id="${escHtml(String(c.id))}">`;
			html +=
				'<span class="conn-color" style="background:' +
				(colorMap[c.color] || colorMap.blue) +
				'"></span>';
			html += `<span class="conn-name">${escHtml(c.name)}</span>`;
			html +=
				'<span class="conn-del" data-id="' +
				escHtml(String(c.id)) +
				'" title="Delete">&times;</span>';
			html += '</div>';
		});
		container.innerHTML = html;

		container.querySelectorAll('.saved-conn').forEach((el) => {
			el.addEventListener('click', (e) => {
				if (e.target.classList.contains('conn-del')) {
					e.stopPropagation();
					deleteSavedConnection(parseInt(e.target.dataset.id, 10));
					return;
				}
				const connId = parseInt(el.dataset.id, 10);
				const conn = savedConnsMap.get(connId);
				if (conn) {
					$('conn-input').value = conn.conninfo;
					doConnect();
				}
			});
		});
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

$('save-conn-btn').onclick = () => {
	const conninfo = $('conn-input').value.trim();
	if (!conninfo) {
		toast('Connect first', 'error');
		return;
	}
	$('save-conn-name').value = '';
	savedConnColor = 'blue';
	$('save-conn-colors')
		.querySelectorAll('span')
		.forEach((s) => {
			s.style.borderColor =
				s.dataset.color === 'blue' ? 'var(--text-primary)' : 'transparent';
		});
	$('save-conn-overlay').classList.add('open');
	trapFocus($('save-conn-overlay'));
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
	const conninfo = $('conn-input').value.trim();
	try {
		await fetchJson('/api/connections', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ name, conninfo, color: savedConnColor }),
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
			dbConnected = true;
			updateConnUI();
			await loadSchema();
			toast('Reconnected', 'success');
		}
	} catch (e) {
		toast(`Reconnect failed: ${e.message}`, 'error');
	}
	$('reconnect-btn').textContent = 'Reconnect';
	$('reconnect-btn').disabled = false;
};
