// utils.js -- Pure utility functions (loaded second in index.html, after state.js)
// These functions have no feature-specific state dependencies.

// Constants
const TOAST_DURATION = 3500;
const TOAST_HOVER_DURATION = 2000;

// DOM query helpers
const $ = (id) => document.getElementById(id);
const $$ = (sel) => document.querySelectorAll(sel);

// HTML escaping -- safe, uses textContent not innerHTML
function escHtml(s) {
	const d = document.createElement('div');
	d.textContent = s;
	return d.innerHTML;
}

// snake_case to Title Case
function prettyName(name) {
	return name.replace(/_/g, ' ').replace(/\bid\b/gi, 'ID');
}

// Modal close with exit animation
function closeModal(overlayId) {
	const overlay = $(overlayId);
	if (!overlay || !overlay.classList.contains('open')) return;
	overlay.classList.add('closing');
	const modal = overlay.querySelector('.modal');
	if (modal) {
		modal.addEventListener(
			'animationend',
			() => {
				overlay.classList.remove('open', 'closing');
			},
			{ once: true },
		);
		// Fallback timeout in case animationend doesn't fire (e.g., reduced motion)
		setTimeout(() => {
			overlay.classList.remove('open', 'closing');
		}, 200);
	} else {
		overlay.classList.remove('open');
	}
}

// Re-trigger CSS animation on connection dot
function pulseDot(dotEl) {
	dotEl.classList.remove('pulse');
	dotEl.offsetHeight; // force reflow to re-trigger animation
	dotEl.classList.add('pulse');
	dotEl.addEventListener('animationend', () => dotEl.classList.remove('pulse'), { once: true });
}

// Clipboard copy with fallback for HTTP (insecure) contexts
function copyToClipboard(text) {
	if (navigator.clipboard?.writeText) {
		return navigator.clipboard.writeText(text);
	}
	const ta = document.createElement('textarea');
	ta.value = text;
	ta.style.position = 'fixed';
	ta.style.opacity = '0';
	document.body.appendChild(ta);
	ta.select();
	try {
		document.execCommand('copy');
	} catch (_e) {
		/* best effort */
	}
	ta.remove();
	return Promise.resolve();
}

// Fetch wrapper -- throws on non-2xx, extracts server error messages
async function fetchJson(url, opts) {
	const res = await fetch(url, opts);
	if (!res.ok) {
		let msg = 'Server error: ' + res.status;
		try {
			const body = await res.json();
			if (body.error) msg = body.error;
		} catch (_e) {
			/* no JSON body */
		}
		throw new Error(msg);
	}
	return res.json();
}

// Toast notification
function toast(msg, type) {
	const el = document.createElement('div');
	el.className = 'toast ' + (type || 'info');
	el.textContent = msg;
	$('toast-container').appendChild(el);
	let timer = setTimeout(() => {
		el.classList.add('hiding');
		setTimeout(() => el.remove(), 200);
	}, TOAST_DURATION);
	el.addEventListener('mouseenter', () => clearTimeout(timer));
	el.addEventListener('mouseleave', () => {
		timer = setTimeout(() => {
			el.classList.add('hiding');
			setTimeout(() => el.remove(), 200);
		}, TOAST_HOVER_DURATION);
	});
}

// Loading spinner overlay
function showLoading(parentId) {
	const parent = $(parentId);
	if (!parent || parent.querySelector('.loading-overlay')) return;
	const overlay = document.createElement('div');
	overlay.className = 'loading-overlay';
	const spinner = document.createElement('div');
	spinner.className = 'spinner';
	overlay.appendChild(spinner);
	parent.style.position = 'relative';
	parent.appendChild(overlay);
}
function hideLoading(parentId) {
	const parent = $(parentId);
	if (!parent) return;
	const overlay = parent.querySelector('.loading-overlay');
	if (overlay) overlay.remove();
}

// Async confirmation dialog
let confirmResolve = null;
function confirm(title, body) {
	return new Promise((resolve) => {
		$('confirm-title').textContent = title;
		$('confirm-body').textContent = body;
		$('confirm-overlay').classList.add('open');
		trapFocus($('confirm-overlay'));
		confirmResolve = resolve;
	});
}
$('confirm-ok').onclick = () => {
	closeModal('confirm-overlay');
	releaseFocus($('confirm-overlay'));
	if (confirmResolve) confirmResolve(true);
};
$('confirm-cancel').onclick = () => {
	closeModal('confirm-overlay');
	releaseFocus($('confirm-overlay'));
	if (confirmResolve) confirmResolve(false);
};

// Async prompt dialog
let promptResolve = null;
function promptUser(title, defaultVal) {
	return new Promise((resolve) => {
		$('prompt-title').textContent = title;
		$('prompt-input').value = defaultVal || '';
		$('prompt-overlay').classList.add('open');
		trapFocus($('prompt-overlay'));
		promptResolve = resolve;
		setTimeout(() => $('prompt-input').focus(), 50);
	});
}
$('prompt-ok').onclick = () => {
	closeModal('prompt-overlay');
	releaseFocus($('prompt-overlay'));
	if (promptResolve) promptResolve($('prompt-input').value);
};
$('prompt-cancel').onclick = () => {
	closeModal('prompt-overlay');
	releaseFocus($('prompt-overlay'));
	if (promptResolve) promptResolve(null);
};
$('prompt-input').addEventListener('keydown', (e) => {
	if (e.key === 'Enter') $('prompt-ok').click();
});
$('prompt-overlay').onclick = (e) => {
	if (e.target === $('prompt-overlay')) $('prompt-cancel').click();
};

// Keyboard focus trap for modal accessibility
function trapFocus(modal) {
	const focusable = modal.querySelectorAll(
		'button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
	);
	if (!focusable.length) return;
	const first = focusable[0];
	const last = focusable[focusable.length - 1];
	function handler(e) {
		if (e.key !== 'Tab') return;
		if (e.shiftKey && document.activeElement === first) {
			e.preventDefault();
			last.focus();
		} else if (!e.shiftKey && document.activeElement === last) {
			e.preventDefault();
			first.focus();
		}
	}
	modal.addEventListener('keydown', handler);
	modal._focusTrap = handler;
	first.focus();
}
function releaseFocus(modal) {
	if (modal._focusTrap) modal.removeEventListener('keydown', modal._focusTrap);
}
