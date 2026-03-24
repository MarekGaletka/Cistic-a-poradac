/* GOD MODE Media Library — Command Palette (Cmd+K) */

import { api } from "./api.js";

let _overlay = null;
let _selectedIndex = 0;
let _results = [];

const PAGES = [
  { type: "nav", label: "Přehled", icon: "🏠", action: () => location.hash = "#dashboard" },
  { type: "nav", label: "Soubory", icon: "📷", action: () => location.hash = "#files" },
  { type: "nav", label: "Duplicity", icon: "📋", action: () => location.hash = "#duplicates" },
  { type: "nav", label: "Podobné", icon: "🎨", action: () => location.hash = "#similar" },
  { type: "nav", label: "Časová osa", icon: "📅", action: () => location.hash = "#timeline" },
  { type: "nav", label: "Mapa", icon: "🌎", action: () => location.hash = "#map" },
  { type: "nav", label: "Galerie", icon: "🌄", action: () => location.hash = "#gallery" },
  { type: "nav", label: "Osoby", icon: "👤", action: () => location.hash = "#people" },
  { type: "nav", label: "Cloud", icon: "☁", action: () => location.hash = "#cloud" },
  { type: "nav", label: "Záloha", icon: "📡", action: () => location.hash = "#backup" },
  { type: "nav", label: "Recovery", icon: "🛡", action: () => location.hash = "#recovery" },
  { type: "nav", label: "Scénáře", icon: "🎬", action: () => location.hash = "#scenarios" },
  { type: "nav", label: "Reorganizace", icon: "📦", action: () => location.hash = "#reorganize" },
];

const ACTIONS = [
  { type: "action", label: "Naskenovat složku", icon: "📂", action: () => { location.hash = "#dashboard"; /* trigger scan dialog */ } },
  { type: "action", label: "Vytvořit plán zálohy", icon: "📋", action: async () => { location.hash = "#backup"; } },
  { type: "action", label: "Kontrola zdraví záloh", icon: "🩺", action: async () => { const { apiPost } = await import("./api.js"); apiPost("/backup/monitor/check"); } },
  { type: "action", label: "Bit rot sken", icon: "🔬", action: async () => { const { apiPost } = await import("./api.js"); apiPost("/bitrot/scan?limit=500"); } },
  { type: "action", label: "Generovat report", icon: "📊", action: async () => { const { apiPost } = await import("./api.js"); apiPost("/report/generate"); } },
];

export function init() {
  document.addEventListener("keydown", (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "k") {
      e.preventDefault();
      toggle();
    }
  });
}

function toggle() {
  if (_overlay) {
    close();
  } else {
    open();
  }
}

function open() {
  _selectedIndex = 0;
  _results = [...PAGES, ...ACTIONS];

  _overlay = document.createElement("div");
  _overlay.className = "cmd-palette-overlay";
  _overlay.innerHTML = `
    <div class="cmd-palette">
      <div class="cmd-palette-input-wrap">
        <span class="cmd-palette-icon">🔍</span>
        <input type="text" class="cmd-palette-input" placeholder="Hledat stránky, akce, soubory..." autofocus />
        <kbd class="cmd-palette-kbd">ESC</kbd>
      </div>
      <div class="cmd-palette-results"></div>
      <div class="cmd-palette-footer">
        <span><kbd>↑↓</kbd> navigace</span>
        <span><kbd>↵</kbd> vybrat</span>
        <span><kbd>esc</kbd> zavřít</span>
      </div>
    </div>`;

  document.body.appendChild(_overlay);

  const input = _overlay.querySelector(".cmd-palette-input");
  let _debounce = null;

  input.addEventListener("input", () => {
    clearTimeout(_debounce);
    const q = input.value.trim().toLowerCase();
    _debounce = setTimeout(() => search(q), q.length > 2 ? 200 : 50);
  });

  input.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown") { e.preventDefault(); _selectedIndex = Math.min(_selectedIndex + 1, _results.length - 1); renderResults(); }
    if (e.key === "ArrowUp") { e.preventDefault(); _selectedIndex = Math.max(_selectedIndex - 1, 0); renderResults(); }
    if (e.key === "Enter" && _results[_selectedIndex]) { e.preventDefault(); selectResult(_results[_selectedIndex]); }
    if (e.key === "Escape") { close(); }
  });

  _overlay.addEventListener("click", (e) => {
    if (e.target === _overlay) close();
  });

  renderResults();
  input.focus();
}

function close() {
  if (_overlay) {
    _overlay.remove();
    _overlay = null;
  }
}

async function search(query) {
  if (!query) {
    _results = [...PAGES, ...ACTIONS];
    _selectedIndex = 0;
    renderResults();
    return;
  }

  // Filter pages and actions
  const filtered = [...PAGES, ...ACTIONS].filter(item =>
    item.label.toLowerCase().includes(query)
  );

  // Search files if query is long enough
  let fileResults = [];
  if (query.length > 2) {
    try {
      const data = await api(`/files?search=${encodeURIComponent(query)}&limit=8`);
      fileResults = (data.files || []).map(f => ({
        type: "file",
        label: f.path.split("/").pop(),
        sublabel: f.path,
        icon: _fileIcon(f.ext),
        action: () => { location.hash = `#files?search=${encodeURIComponent(query)}`; },
      }));
    } catch { /* ignore */ }
  }

  _results = [...filtered, ...fileResults];
  _selectedIndex = 0;
  renderResults();
}

function _fileIcon(ext) {
  const e = (ext || "").toLowerCase();
  if (["jpg","jpeg","png","heic","webp","tiff"].includes(e)) return "🖼";
  if (["mp4","mov","avi","mkv"].includes(e)) return "🎬";
  if (["mp3","m4a","wav","flac"].includes(e)) return "🎵";
  return "📄";
}

function renderResults() {
  const el = _overlay?.querySelector(".cmd-palette-results");
  if (!el) return;

  if (_results.length === 0) {
    el.innerHTML = '<div class="cmd-palette-empty">Žádné výsledky</div>';
    return;
  }

  // Group by type
  let lastType = "";
  let html = "";

  _results.forEach((item, i) => {
    if (item.type !== lastType) {
      const label = item.type === "nav" ? "Navigace" : item.type === "action" ? "Akce" : "Soubory";
      html += `<div class="cmd-palette-group">${label}</div>`;
      lastType = item.type;
    }
    const selected = i === _selectedIndex ? "cmd-palette-selected" : "";
    const sub = item.sublabel ? `<span class="cmd-palette-sublabel">${item.sublabel}</span>` : "";
    html += `<div class="cmd-palette-item ${selected}" data-index="${i}">
      <span class="cmd-palette-item-icon">${item.icon}</span>
      <div class="cmd-palette-item-text">
        <span>${item.label}</span>
        ${sub}
      </div>
    </div>`;
  });

  el.innerHTML = html;

  // Bind clicks
  el.querySelectorAll(".cmd-palette-item").forEach(item => {
    item.addEventListener("click", () => {
      const idx = parseInt(item.dataset.index, 10);
      selectResult(_results[idx]);
    });
    item.addEventListener("mouseenter", () => {
      _selectedIndex = parseInt(item.dataset.index, 10);
      renderResults();
    });
  });

  // Scroll selected into view
  const sel = el.querySelector(".cmd-palette-selected");
  if (sel) sel.scrollIntoView({ block: "nearest" });
}

function selectResult(item) {
  close();
  if (item?.action) item.action();
}
