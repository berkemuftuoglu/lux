// utils.js -- Pure utility functions (loaded second in index.html, after state.js)
// These functions have no feature-specific state dependencies.

const TOAST_DURATION = 3500;
const TOAST_HOVER_DURATION = 2000;

// --- Query helpers ---

const qs = (sel, ctx) => (ctx || document).querySelector(sel);
const qsa = (sel, ctx) => (ctx || document).querySelectorAll(sel);
const byId = (id) => document.getElementById(id);

const $ = byId;
const $$ = qsa;

// --- Element creator (VS Code-style) ---

function h(tag, attrs, ...children) {
	const el = document.createElement(tag);
	if (attrs) {
		for (const [k, v] of Object.entries(attrs)) {
			if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
			else if (k === "text") el.textContent = v;
			else if (k === "cls") el.className = v;
			else el.setAttribute(k, v);
		}
	}
	for (const c of children) {
		if (typeof c === "string") el.appendChild(document.createTextNode(c));
		else if (c) el.appendChild(c);
	}
	return el;
}

// --- Event delegation ---

function delegate(parent, selector, type, handler) {
	const useCapture = type === "blur" || type === "focus";
	parent.addEventListener(
		type,
		(e) => {
			const target = e.target.closest(selector);
			if (target && parent.contains(target)) handler.call(target, e);
		},
		useCapture,
	);
}

// --- Fetch helpers ---

let currentAbort = null;

async function fetchJson(url, opts) {
	const res = await fetch(url, opts);
	if (!res.ok) {
		let msg = `Server error: ${res.status}`;
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

async function fetchJsonAbortable(url, opts) {
	if (currentAbort) currentAbort.abort();
	currentAbort = new AbortController();
	try {
		return await fetchJson(url, { ...opts, signal: currentAbort.signal });
	} finally {
		currentAbort = null;
	}
}

async function postJson(url, data) {
	return fetchJson(url, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(data),
	});
}

// --- Debounce ---

function debounce(fn, ms) {
	let timer;
	return (...args) => {
		clearTimeout(timer);
		timer = setTimeout(() => fn(...args), ms);
	};
}

// --- Action registry for keyboard shortcuts ---

const keyActions = [];

function registerKeyAction(name, keyTest, predicate, perform) {
	keyActions.push({ name, keyTest, predicate, perform });
}

document.addEventListener("keydown", (e) => {
	for (const action of keyActions) {
		if (action.keyTest(e) && action.predicate()) {
			e.preventDefault();
			action.perform(e);
			return;
		}
	}
});

// --- HTML escaping ---

function escHtml(s) {
	const d = document.createElement("div");
	d.textContent = s;
	return d.innerHTML;
}

function prettyName(name) {
	return name.replace(/_/g, " ").replace(/\bid\b/gi, "ID");
}

// --- Modals ---

function openModal(overlayId) {
	const overlay = $(overlayId);
	if (overlay) {
		overlay.classList.add("open");
		trapFocus(overlay);
	}
}

function closeModal(overlayId) {
	const overlay = $(overlayId);
	if (!overlay || !overlay.classList.contains("open")) return;
	overlay.classList.add("closing");
	const modal = overlay.querySelector(".modal");
	if (modal) {
		modal.addEventListener(
			"animationend",
			() => {
				overlay.classList.remove("open", "closing");
			},
			{ once: true },
		);
		setTimeout(() => {
			overlay.classList.remove("open", "closing");
		}, 200);
	} else {
		overlay.classList.remove("open");
	}
}

// Focus trapping for modal accessibility
function trapFocus(modal) {
	const focusable = modal.querySelectorAll(
		'button, input, textarea, select, [tabindex]:not([tabindex="-1"])',
	);
	if (!focusable.length) return;
	const first = focusable[0];
	const last = focusable[focusable.length - 1];
	function handler(e) {
		if (e.key !== "Tab") return;
		if (e.shiftKey && document.activeElement === first) {
			e.preventDefault();
			last.focus();
		} else if (!e.shiftKey && document.activeElement === last) {
			e.preventDefault();
			first.focus();
		}
	}
	modal.addEventListener("keydown", handler);
	modal._focusTrap = handler;
	first.focus();
}
function releaseFocus(modal) {
	if (modal._focusTrap) modal.removeEventListener("keydown", modal._focusTrap);
}

// --- Visual helpers ---

function pulseDot(dotEl) {
	dotEl.classList.remove("pulse");
	dotEl.offsetHeight;
	dotEl.classList.add("pulse");
	dotEl.addEventListener("animationend", () => dotEl.classList.remove("pulse"), {
		once: true,
	});
}

function copyToClipboard(text) {
	if (navigator.clipboard?.writeText) {
		return navigator.clipboard.writeText(text);
	}
	const ta = document.createElement("textarea");
	ta.value = text;
	ta.style.position = "fixed";
	ta.style.opacity = "0";
	document.body.appendChild(ta);
	ta.select();
	try {
		document.execCommand("copy");
	} catch (_e) {
		/* best effort */
	}
	ta.remove();
	return Promise.resolve();
}

function toast(msg, type) {
	const el = document.createElement("div");
	el.className = `toast ${type || "info"}`;
	el.textContent = msg;
	$("toast-container").appendChild(el);
	let timer = setTimeout(() => {
		el.classList.add("hiding");
		setTimeout(() => el.remove(), 200);
	}, TOAST_DURATION);
	el.addEventListener("mouseenter", () => clearTimeout(timer));
	el.addEventListener("mouseleave", () => {
		timer = setTimeout(() => {
			el.classList.add("hiding");
			setTimeout(() => el.remove(), 200);
		}, TOAST_HOVER_DURATION);
	});
}

function showLoading(parentId) {
	const parent = $(parentId);
	if (!parent || parent.querySelector(".loading-overlay")) return;
	const overlay = document.createElement("div");
	overlay.className = "loading-overlay";
	const spinner = document.createElement("div");
	spinner.className = "spinner";
	overlay.appendChild(spinner);
	parent.style.position = "relative";
	parent.appendChild(overlay);
}
function hideLoading(parentId) {
	const parent = $(parentId);
	if (!parent) return;
	const overlay = parent.querySelector(".loading-overlay");
	if (overlay) overlay.remove();
}

// --- Confirm / Prompt dialogs ---

let confirmResolve = null;
function confirm(title, body) {
	return new Promise((resolve) => {
		$("confirm-title").textContent = title;
		$("confirm-body").textContent = body;
		openModal("confirm-overlay");
		confirmResolve = resolve;
	});
}
$("confirm-ok").onclick = () => {
	closeModal("confirm-overlay");
	releaseFocus($("confirm-overlay"));
	if (confirmResolve) confirmResolve(true);
};
$("confirm-cancel").onclick = () => {
	closeModal("confirm-overlay");
	releaseFocus($("confirm-overlay"));
	if (confirmResolve) confirmResolve(false);
};

let promptResolve = null;
function promptUser(title, defaultVal) {
	return new Promise((resolve) => {
		$("prompt-title").textContent = title;
		$("prompt-input").value = defaultVal || "";
		openModal("prompt-overlay");
		promptResolve = resolve;
		setTimeout(() => $("prompt-input").focus(), 50);
	});
}
$("prompt-ok").onclick = () => {
	closeModal("prompt-overlay");
	releaseFocus($("prompt-overlay"));
	if (promptResolve) promptResolve($("prompt-input").value);
};
$("prompt-cancel").onclick = () => {
	closeModal("prompt-overlay");
	releaseFocus($("prompt-overlay"));
	if (promptResolve) promptResolve(null);
};
$("prompt-input").addEventListener("keydown", (e) => {
	if (e.key === "Enter") $("prompt-ok").click();
});
$("prompt-overlay").onclick = (e) => {
	if (e.target === $("prompt-overlay")) $("prompt-cancel").click();
};
