/* GOD MODE Media Library — Command Palette (Cmd+K) */

import { api } from "./api.js";
import { t } from "./i18n.js";

function _esc(s) { const d = document.createElement("div"); d.textContent = s; return d.innerHTML; }

let _overlay = null;
let _selectedIndex = 0;
let _results = [];

const PAGES = [
  { type: "nav", label: () => t("cmd.page_dashboard"), icon: "🏠", action: () => location.hash = "#dashboard" },
  { type: "nav", label: () => t("cmd.page_files"), icon: "📷", action: () => location.hash = "#files" },
  { type: "nav", label: () => t("cmd.page_duplicates"), icon: "📋", action: () => location.hash = "#duplicates" },
  { type: "nav", label: () => t("cmd.page_similar"), icon: "🎨", action: () => location.hash = "#similar" },
  { type: "nav", label: () => t("cmd.page_timeline"), icon: "📅", action: () => location.hash = "#timeline" },
  { type: "nav", label: () => t("cmd.page_map"), icon: "🌎", action: () => location.hash = "#map" },
  { type: "nav", label: () => t("cmd.page_gallery"), icon: "🌄", action: () => location.hash = "#gallery" },
  { type: "nav", label: () => t("cmd.page_people"), icon: "👤", action: () => location.hash = "#people" },
  { type: "nav", label: () => t("cmd.page_cloud"), icon: "☁", action: () => location.hash = "#cloud" },
  { type: "nav", label: () => t("cmd.page_backup"), icon: "📡", action: () => location.hash = "#backup" },
  { type: "nav", label: () => t("cmd.page_recovery"), icon: "🛡", action: () => location.hash = "#recovery" },
  { type: "nav", label: () => t("cmd.page_scenarios"), icon: "🎬", action: () => location.hash = "#scenarios" },
  { type: "nav", label: () => t("cmd.page_reorganize"), icon: "📦", action: () => location.hash = "#reorganize" },
];

const ACTIONS = [
  { type: "action", label: () => t("cmd.action_scan"), icon: "📂", action: () => { location.hash = "#dashboard"; /* trigger scan dialog */ } },
  { type: "action", label: () => t("cmd.action_backup_plan"), icon: "📋", action: async () => { location.hash = "#backup"; } },
  { type: "action", label: () => t("cmd.action_health_check"), icon: "🩺", action: async () => { const { apiPost } = await import("./api.js"); apiPost("/backup/monitor/check"); } },
  { type: "action", label: () => t("cmd.action_bitrot"), icon: "🔬", action: async () => { const { apiPost } = await import("./api.js"); apiPost("/bitrot/scan?limit=500"); } },
  { type: "action", label: () => t("cmd.action_report"), icon: "📊", action: async () => { const { apiPost } = await import("./api.js"); apiPost("/report/generate"); } },
];

function _getLabel(item) {
  return typeof item.label === "function" ? item.label() : item.label;
}

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
        <input type="text" class="cmd-palette-input" placeholder="${t("cmd.search_placeholder")}" autofocus />
        <kbd class="cmd-palette-kbd">ESC</kbd>
      </div>
      <div class="cmd-palette-results"></div>
      <div class="cmd-palette-footer">
        <span><kbd>↑↓</kbd> ${t("cmd.navigate_hint")}</span>
        <span><kbd>↵</kbd> ${t("cmd.select_hint")}</span>
        <span><kbd>esc</kbd> ${t("cmd.close_hint")}</span>
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
    _getLabel(item).toLowerCase().includes(query)
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
    el.innerHTML = `<div class="cmd-palette-empty">${t("cmd.no_results")}</div>`;
    return;
  }

  // Group by type
  let lastType = "";
  let html = "";

  _results.forEach((item, i) => {
    if (item.type !== lastType) {
      const label = item.type === "nav" ? t("cmd.nav") : item.type === "action" ? t("cmd.actions") : t("cmd.files_group");
      html += `<div class="cmd-palette-group">${label}</div>`;
      lastType = item.type;
    }
    const selected = i === _selectedIndex ? "cmd-palette-selected" : "";
    const sub = item.sublabel ? `<span class="cmd-palette-sublabel">${_esc(item.sublabel)}</span>` : "";
    html += `<div class="cmd-palette-item ${selected}" data-index="${i}">
      <span class="cmd-palette-item-icon">${item.icon}</span>
      <div class="cmd-palette-item-text">
        <span>${_esc(_getLabel(item))}</span>
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
