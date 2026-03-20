/* GOD MODE Media Library — Folder picker component */

import { api } from "./api.js";
import { $, escapeHtml } from "./utils.js";
import { t } from "./i18n.js";

let _currentPath = null;
let _selectedPaths = [];
let _onSelectCallback = null;
let _bookmarks = [];

/**
 * Open the folder picker modal.
 * @param {Function} onSelect - Callback receiving array of selected paths when confirmed.
 * @param {string[]} [initialSelected] - Paths already selected.
 */
export function openFolderPicker(onSelect, initialSelected = []) {
  _onSelectCallback = onSelect;
  _selectedPaths = [...initialSelected];
  _currentPath = null;

  const overlay = $("#folder-picker-overlay");
  if (overlay) {
    overlay.classList.remove("hidden");
    overlay.setAttribute("aria-hidden", "false");
  }
  _browseTo(null);
}

export function closeFolderPicker() {
  const overlay = $("#folder-picker-overlay");
  if (overlay) {
    overlay.classList.add("hidden");
    overlay.setAttribute("aria-hidden", "true");
  }
}

async function _browseTo(path) {
  const main = $("#folder-picker-main");
  if (main) {
    main.innerHTML = '<div class="folder-picker-loading"><div class="spinner"></div></div>';
  }

  try {
    const query = path ? `?path=${encodeURIComponent(path)}` : "";
    const data = await api(`/browse${query}`);
    _currentPath = data.current;
    _bookmarks = data.bookmarks || [];
    _renderHeader(data);
    _renderSidebar(data);
    _renderMain(data);
    _renderFooter();
  } catch (e) {
    if (main) {
      main.innerHTML = `<div class="folder-picker-error">${escapeHtml(e.message)}</div>`;
    }
  }
}

function _renderHeader(data) {
  const header = $("#folder-picker-header");
  if (!header) return;

  const parts = data.current.split("/").filter(Boolean);
  let breadcrumbs = `<button class="breadcrumb-item breadcrumb-home" data-path="/">\u{1F3E0}</button>`;
  let cumPath = "";
  for (const part of parts) {
    cumPath += "/" + part;
    const p = cumPath;
    breadcrumbs += `<span class="breadcrumb-sep">&rsaquo;</span><button class="breadcrumb-item" data-path="${escapeHtml(p)}">${escapeHtml(part)}</button>`;
  }

  header.innerHTML = `
    <div class="folder-picker-breadcrumbs">${breadcrumbs}</div>
    <button class="folder-picker-close" aria-label="${t("general.close")}">&times;</button>
  `;

  header.querySelectorAll(".breadcrumb-item").forEach(btn => {
    btn.addEventListener("click", () => _browseTo(btn.dataset.path));
  });
  header.querySelector(".folder-picker-close").addEventListener("click", () => {
    closeFolderPicker();
  });
}

function _renderSidebar(data) {
  const sidebar = $("#folder-picker-sidebar");
  if (!sidebar) return;

  let html = '<div class="folder-picker-bookmarks">';
  for (const bm of _bookmarks) {
    const isActive = _currentPath === bm.path;
    html += `<button class="bookmark-item${isActive ? " active" : ""}" data-path="${escapeHtml(bm.path)}" title="${escapeHtml(bm.path)}">
      <span class="bookmark-icon">${bm.icon}</span>
      <span class="bookmark-name">${escapeHtml(bm.name)}</span>
    </button>`;
  }
  html += "</div>";
  sidebar.innerHTML = html;

  sidebar.querySelectorAll(".bookmark-item").forEach(btn => {
    btn.addEventListener("click", () => _browseTo(btn.dataset.path));
  });
}

function _renderMain(data) {
  const main = $("#folder-picker-main");
  if (!main) return;

  if (!data.entries || data.entries.length === 0) {
    main.innerHTML = `<div class="folder-picker-empty">${t("folder.empty_folder")}</div>`;
    return;
  }

  let html = '<div class="folder-picker-list">';
  for (const entry of data.entries) {
    const isSelected = _selectedPaths.includes(entry.path);
    html += `<button class="folder-item${isSelected ? " selected" : ""}" data-path="${escapeHtml(entry.path)}">
      <span class="folder-item-icon">\u{1F4C1}</span>
      <span class="folder-item-name">${escapeHtml(entry.name)}</span>
      <span class="folder-item-count">${entry.item_count > 0 ? t("folder.items", { count: entry.item_count }) : ""}</span>
      <span class="folder-item-arrow">&rsaquo;</span>
    </button>`;
  }
  html += "</div>";
  main.innerHTML = html;

  main.querySelectorAll(".folder-item").forEach(btn => {
    btn.addEventListener("click", () => _browseTo(btn.dataset.path));
  });
}

function _renderFooter() {
  const footer = $("#folder-picker-footer");
  if (!footer) return;

  const isAlreadySelected = _selectedPaths.includes(_currentPath);

  let chipsHtml = "";
  if (_selectedPaths.length > 0) {
    chipsHtml = `<div class="folder-chips">`;
    for (const p of _selectedPaths) {
      const name = p.split("/").pop() || p;
      chipsHtml += `<span class="folder-chip" data-path="${escapeHtml(p)}">\u{1F4C1} ${escapeHtml(name)}<button class="folder-chip-remove" data-path="${escapeHtml(p)}" aria-label="${t("folder.remove")}">&times;</button></span>`;
    }
    chipsHtml += "</div>";
  }

  footer.innerHTML = `
    ${chipsHtml}
    <div class="folder-picker-actions">
      <button class="folder-picker-select-btn${isAlreadySelected ? " already-selected" : ""}" id="btn-fp-select" ${isAlreadySelected ? "disabled" : ""}>
        ${isAlreadySelected ? "\u2713 " : "\u{1F4C1} "}${t("folder.select_this")}
      </button>
      ${_selectedPaths.length > 0 ? `<button class="folder-picker-confirm-btn" id="btn-fp-confirm">\u2713 ${t("general.confirm")} (${_selectedPaths.length})</button>` : ""}
    </div>
  `;

  const selectBtn = footer.querySelector("#btn-fp-select");
  if (selectBtn && !isAlreadySelected) {
    selectBtn.addEventListener("click", () => {
      if (_currentPath && !_selectedPaths.includes(_currentPath)) {
        _selectedPaths.push(_currentPath);
        // Flash animation
        selectBtn.classList.add("flash-green");
        setTimeout(() => {
          selectBtn.classList.remove("flash-green");
          // Re-browse to update selected state in folder list
          _browseTo(_currentPath);
        }, 400);
        _renderFooter();
      }
    });
  }

  const confirmBtn = footer.querySelector("#btn-fp-confirm");
  if (confirmBtn) {
    confirmBtn.addEventListener("click", () => {
      confirmSelection();
    });
  }

  footer.querySelectorAll(".folder-chip-remove").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const pathToRemove = btn.dataset.path;
      _selectedPaths = _selectedPaths.filter(p => p !== pathToRemove);
      _renderFooter();
      // Re-browse to update selected indicators
      _browseTo(_currentPath);
    });
  });
}

/**
 * Get currently selected paths from the picker.
 */
export function getSelectedPaths() {
  return [..._selectedPaths];
}

/**
 * Confirm selection and call the callback.
 */
export function confirmSelection() {
  if (_onSelectCallback) {
    _onSelectCallback([..._selectedPaths]);
  }
  closeFolderPicker();
}


// Initialize event listeners when DOM is ready
document.addEventListener("DOMContentLoaded", () => {
  const overlay = $("#folder-picker-overlay");
  if (overlay) {
    overlay.addEventListener("click", (e) => {
      if (e.target === overlay) closeFolderPicker();
    });
  }

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      const overlay = $("#folder-picker-overlay");
      if (overlay && !overlay.classList.contains("hidden")) {
        closeFolderPicker();
      }
    }
  });
});
