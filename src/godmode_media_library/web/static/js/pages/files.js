/* GOD MODE Media Library — Files page (redesigned as photo gallery) */

import { api } from "../api.js";
import { $, content, formatBytes, escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";
import { toggleSelect, selectAll, deselectAll, isSelected, getSelectedPaths, getSelectedCount } from "../selection.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

const FILES_PER_PAGE = 50;
let _filesOffset = 0;
let _currentFiles = [];
let _viewMode = "grid"; // Default to grid (photo gallery)
let _selectionMode = false;
let _filtersVisible = false;
let _sortField = "date"; // name, date, size, ext
let _sortDir = "desc";   // asc, desc

export async function render(container) {
  _filesOffset = 0;
  _selectionMode = false;

  let html = `
    <div class="page-header">
      <h2>${t("files.title")}</h2>
      <div class="files-header-actions">
        <button id="btn-toggle-filters" class="btn-icon" title="${t("files.filters_toggle")}">
          &#128270; ${t("files.filters_toggle")}<span id="filter-badge-count" class="filter-badge" style="display:none"></span>
        </button>
        <div class="view-toggle">
          <button id="btn-view-grid" class="${_viewMode === 'grid' ? 'active' : ''}" aria-label="Grid view" title="Galerie">&#9783;</button>
          <button id="btn-view-table" class="${_viewMode === 'table' ? 'active' : ''}" aria-label="Table view" title="Tabulka">&#9776;</button>
        </div>
        <button id="btn-selection-mode" class="btn-icon" title="${t("files.selection_mode")}">
          &#9744;
        </button>
      </div>
    </div>
    <div class="filters-panel ${_filtersVisible ? '' : 'hidden'}" id="filters-panel">
      <div class="filters" role="search" aria-label="${t("files.title")}">
        <input type="text" id="f-ext" placeholder="${t("files.ext_placeholder")}" size="10" aria-label="${t("files.ext_placeholder")}">
        <input type="text" id="f-camera" placeholder="${t("files.camera_placeholder")}" size="15" aria-label="${t("files.camera_placeholder")}">
        <input type="text" id="f-path" placeholder="${t("files.path_placeholder")}" size="20" aria-label="${t("files.path_placeholder")}">
        <button id="btn-files-search" class="primary" aria-label="${t("files.search")}">${t("files.search")}</button>
      </div>
      <div class="filters filters-advanced">
        <div class="filter-group"><label for="f-date-from">${t("files.date_from")}</label><input type="date" id="f-date-from"></div>
        <div class="filter-group"><label for="f-date-to">${t("files.date_to")}</label><input type="date" id="f-date-to"></div>
        <div class="filter-group"><label for="f-min-size">${t("files.min_size")}</label><input type="number" id="f-min-size" min="0" style="width:80px"></div>
        <div class="filter-group"><label for="f-max-size">${t("files.max_size")}</label><input type="number" id="f-max-size" min="0" style="width:80px"></div>
        <label class="filter-checkbox"><input type="checkbox" id="f-has-gps"> ${t("files.has_gps")}</label>
        <label class="filter-checkbox"><input type="checkbox" id="f-has-phash"> ${t("files.has_phash")}</label>
      </div>
    </div>
    <div id="selection-bar" class="selection-bar hidden">
      <span id="selection-count" class="selection-count"></span>
      <button id="btn-select-all" class="small">${t("action.select_all")}</button>
      <button id="btn-deselect-all" class="small">${t("action.deselect_all")}</button>
      <button id="btn-exit-selection" class="small">${t("files.exit_selection")}</button>
    </div>
    <div class="sort-controls" id="sort-controls">
      <label for="sort-field">${t("files.sort_by")}:</label>
      <select id="sort-field">
        <option value="date" ${_sortField === "date" ? "selected" : ""}>${t("files.sort_date")}</option>
        <option value="name" ${_sortField === "name" ? "selected" : ""}>${t("files.sort_name")}</option>
        <option value="size" ${_sortField === "size" ? "selected" : ""}>${t("files.sort_size")}</option>
        <option value="ext" ${_sortField === "ext" ? "selected" : ""}>${t("files.sort_ext")}</option>
      </select>
      <button id="btn-sort-dir" title="${_sortDir === "asc" ? t("files.sort_asc") : t("files.sort_desc")}">
        ${_sortDir === "asc" ? "\u2191" : "\u2193"} ${_sortDir === "asc" ? t("files.sort_asc") : t("files.sort_desc")}
      </button>
    </div>
    <div id="files-table" aria-live="polite"></div>`;
  container.innerHTML = html;

  // Bind sort events
  container.querySelector("#sort-field").addEventListener("change", (e) => {
    _sortField = e.target.value;
    sortAndRender();
  });
  container.querySelector("#btn-sort-dir").addEventListener("click", () => {
    _sortDir = _sortDir === "asc" ? "desc" : "asc";
    const btn = container.querySelector("#btn-sort-dir");
    if (btn) {
      btn.title = _sortDir === "asc" ? t("files.sort_asc") : t("files.sort_desc");
      btn.innerHTML = `${_sortDir === "asc" ? "\u2191" : "\u2193"} ${_sortDir === "asc" ? t("files.sort_asc") : t("files.sort_desc")}`;
    }
    sortAndRender();
  });

  // Bind events
  container.querySelector("#btn-files-search").addEventListener("click", () => { _filesOffset = 0; loadFiles(); });
  container.querySelector("#btn-view-table").addEventListener("click", () => { _viewMode = "table"; renderCurrentFiles(); updateViewToggle(container); });
  container.querySelector("#btn-view-grid").addEventListener("click", () => { _viewMode = "grid"; renderCurrentFiles(); updateViewToggle(container); });

  container.querySelector("#btn-toggle-filters").addEventListener("click", () => {
    _filtersVisible = !_filtersVisible;
    const panel = container.querySelector("#filters-panel");
    if (panel) panel.classList.toggle("hidden", !_filtersVisible);
  });

  container.querySelector("#btn-selection-mode").addEventListener("click", () => {
    _selectionMode = !_selectionMode;
    const selBar = container.querySelector("#selection-bar");
    if (selBar) selBar.classList.toggle("hidden", !_selectionMode);
    if (!_selectionMode) deselectAll();
    renderCurrentFiles();
  });

  container.querySelector("#btn-select-all").addEventListener("click", () => { selectAll(_currentFiles.map(f => f.path)); renderCurrentFiles(); updateSelectionCount(container); });
  container.querySelector("#btn-deselect-all").addEventListener("click", () => { deselectAll(); renderCurrentFiles(); updateSelectionCount(container); });
  container.querySelector("#btn-exit-selection").addEventListener("click", () => {
    _selectionMode = false;
    deselectAll();
    container.querySelector("#selection-bar")?.classList.add("hidden");
    renderCurrentFiles();
  });

  // Enter key triggers search
  container.querySelectorAll(".filters input").forEach(input => {
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") { _filesOffset = 0; loadFiles(); }
    });
  });

  loadFiles();
}

function sortAndRender() {
  if (!_currentFiles.length) return;
  _currentFiles.sort((a, b) => {
    let cmp = 0;
    if (_sortField === "name") {
      const nameA = (a.path || "").split("/").pop().toLowerCase();
      const nameB = (b.path || "").split("/").pop().toLowerCase();
      cmp = nameA.localeCompare(nameB);
    } else if (_sortField === "date") {
      cmp = (a.date_original || "").localeCompare(b.date_original || "");
    } else if (_sortField === "size") {
      cmp = (a.size || 0) - (b.size || 0);
    } else if (_sortField === "ext") {
      cmp = (a.ext || "").localeCompare(b.ext || "");
    }
    return _sortDir === "asc" ? cmp : -cmp;
  });
  renderCurrentFiles();
}

function updateFilterBadge() {
  const badge = document.querySelector("#filter-badge-count");
  if (!badge) return;
  let count = 0;
  if ($("#f-ext")?.value) count++;
  if ($("#f-camera")?.value) count++;
  if ($("#f-path")?.value) count++;
  if ($("#f-date-from")?.value) count++;
  if ($("#f-date-to")?.value) count++;
  if ($("#f-min-size")?.value) count++;
  if ($("#f-max-size")?.value) count++;
  if ($("#f-has-gps")?.checked) count++;
  if ($("#f-has-phash")?.checked) count++;
  if (count > 0) {
    badge.textContent = String(count);
    badge.style.display = "";
  } else {
    badge.style.display = "none";
  }
}

function updateViewToggle(container) {
  const tbl = container.querySelector("#btn-view-table");
  const grd = container.querySelector("#btn-view-grid");
  if (tbl) tbl.classList.toggle("active", _viewMode === "table");
  if (grd) grd.classList.toggle("active", _viewMode === "grid");
}

function updateSelectionCount(container) {
  const el = container.querySelector("#selection-count");
  if (el) el.textContent = t("general.selected", { count: getSelectedCount() });
}

let _lastData = null;

async function loadFiles() {
  const ext = $("#f-ext")?.value || "";
  const camera = $("#f-camera")?.value || "";
  const pathC = $("#f-path")?.value || "";
  const dateFrom = $("#f-date-from")?.value || "";
  const dateTo = $("#f-date-to")?.value || "";
  const minSize = $("#f-min-size")?.value || "";
  const maxSize = $("#f-max-size")?.value || "";
  const hasGps = $("#f-has-gps")?.checked;
  const hasPhash = $("#f-has-phash")?.checked;
  let q = `/files?limit=${FILES_PER_PAGE}`;
  if (ext) q += `&ext=${encodeURIComponent(ext)}`;
  if (camera) q += `&camera=${encodeURIComponent(camera)}`;
  if (pathC) q += `&path_contains=${encodeURIComponent(pathC)}`;
  if (dateFrom) q += `&date_from=${encodeURIComponent(dateFrom)}`;
  if (dateTo) q += `&date_to=${encodeURIComponent(dateTo)}`;
  if (minSize) q += `&min_size=${encodeURIComponent(minSize)}`;
  if (maxSize) q += `&max_size=${encodeURIComponent(maxSize)}`;
  if (hasGps) q += "&has_gps=true";
  if (hasPhash) q += "&has_phash=true";
  q += `&offset=${_filesOffset}`;
  try {
    const data = await api(q);
    _lastData = data;
    _currentFiles = data.files;
    updateFilterBadge();
    if (!data.files.length) {
      $("#files-table").innerHTML = `<div class="empty-state-hero" style="padding:40px 0">
        <div class="empty-state-icon" style="font-size:48px">&#128269;</div>
        <h3 class="empty-state-title">${t("files.empty_title")}</h3>
        <p class="empty-state-subtitle">${t("files.empty_hint")}</p>
      </div>`;
      return;
    }
    renderCurrentFiles();
  } catch (e) {
    $("#files-table").innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

function renderCurrentFiles() {
  if (!_lastData) return;
  const data = _lastData;
  const filesEl = $("#files-table");
  if (!filesEl) return;

  if (_viewMode === "grid") {
    renderGrid(filesEl, data);
  } else {
    renderTable(filesEl, data);
  }
}

function renderTable(el, data) {
  let html = `<table><tr>`;
  if (_selectionMode) html += `<th class="select-cell"></th>`;
  html += `<th>${t("files.name")}</th><th>${t("files.ext")}</th><th>${t("files.size")}</th><th>${t("files.camera")}</th><th>${t("files.date")}</th><th>${t("files.gps")}</th><th>${t("files.resolution")}</th></tr>`;
  for (const f of data.files) {
    const gps = f.gps_latitude ? `${f.gps_latitude.toFixed(4)}, ${f.gps_longitude.toFixed(4)}` : "";
    const res = f.width && f.height ? `${f.width}x${f.height}` : "";
    const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
    const checked = isSelected(f.path) ? "checked" : "";
    html += `<tr class="file-row" tabindex="0" role="button" aria-label="${escapeHtml(fileName(f.path))}" data-file-path="${escapeHtml(f.path)}">`;
    if (_selectionMode) {
      html += `<td class="select-cell"><input type="checkbox" data-select-path="${escapeHtml(f.path)}" ${checked} aria-label="${t("action.select_all")}"></td>`;
    }
    html += `<td class="path" title="${escapeHtml(f.path)}">${escapeHtml(fileName(f.path))}</td>
      <td>${escapeHtml(f.ext)}</td>
      <td>${formatBytes(f.size)}</td>
      <td>${escapeHtml(cam)}</td>
      <td>${escapeHtml(f.date_original ?? "")}</td>
      <td>${gps}</td>
      <td>${res}</td>
    </tr>`;
  }
  html += "</table>";
  html += renderPagination(data);
  el.innerHTML = html;
  bindFileEvents(el);
}

function renderGrid(el, data) {
  let html = '<div class="files-grid files-grid-large">';
  for (const f of data.files) {
    const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
    const checked = isSelected(f.path) ? "checked" : "";
    const thumb = isImage
      ? `<img src="/api/thumbnail${encodeURI(f.path)}?size=300" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}" loading="lazy">`
      : `<div class="grid-icon">${escapeHtml(f.ext)}</div>`;

    const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
    const dateStr = f.date_original ? f.date_original.split(" ")[0] : "";

    html += `<div class="file-grid-item file-grid-item-large" data-file-path="${escapeHtml(f.path)}" tabindex="0" role="button">`;

    // Only show checkbox in selection mode
    if (_selectionMode) {
      html += `<div class="grid-select"><input type="checkbox" data-select-path="${escapeHtml(f.path)}" ${checked}></div>`;
    }

    html += `<div class="grid-thumb grid-thumb-large">${thumb}</div>`;

    // Hover overlay with key info
    html += `<div class="grid-hover-overlay">
      <div class="grid-hover-name">${escapeHtml(fileName(f.path))}</div>
      <div class="grid-hover-meta">${formatBytes(f.size)}${dateStr ? ' &middot; ' + escapeHtml(dateStr) : ''}${cam ? ' &middot; ' + escapeHtml(cam) : ''}</div>
    </div>`;

    html += `</div>`;
  }
  html += '</div>';
  html += renderPagination(data);
  el.innerHTML = html;
  bindFileEvents(el);
}

function renderPagination(data) {
  const pageNum = Math.floor(_filesOffset / FILES_PER_PAGE) + 1;
  const from = _filesOffset + 1;
  const to = _filesOffset + data.count;
  return `<div class="pagination" role="navigation" aria-label="Pagination">
    <button ${_filesOffset === 0 ? "disabled" : ""} id="btn-page-prev" aria-label="${t("files.previous")}">&#8592; ${t("files.previous")}</button>
    <span class="page-info" aria-live="polite">${t("files.showing", { from, to, page: pageNum })}</span>
    <button ${!data.has_more ? "disabled" : ""} id="btn-page-next" aria-label="${t("files.next")}">${t("files.next")} &#8594;</button>
  </div>`;
}

function bindFileEvents(el) {
  // Checkbox clicks
  if (_selectionMode) {
    el.querySelectorAll("[data-select-path]").forEach(cb => {
      cb.addEventListener("click", e => {
        e.stopPropagation();
        toggleSelect(cb.dataset.selectPath);
      });
    });
  }

  // Row/card clicks (excluding checkboxes) — open lightbox for images/videos, modal for others
  const allPaths = _currentFiles.map(f => f.path);
  const lightboxPaths = _currentFiles
    .filter(f => {
      const ext = (f.ext || "").toLowerCase();
      return IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
    })
    .map(f => f.path);

  el.querySelectorAll("[data-file-path]").forEach(row => {
    const handler = (e) => {
      if (e.type === "keydown" && e.key !== "Enter") return;
      if (e.target.matches("input[type=checkbox]")) return;
      const filePath = row.dataset.filePath;
      const lbIndex = lightboxPaths.indexOf(filePath);
      if (lbIndex >= 0) {
        openLightbox(lightboxPaths, lbIndex);
      } else {
        showFileDetail(filePath);
      }
    };
    row.addEventListener("click", handler);
    row.addEventListener("keydown", handler);
  });

  // Pagination
  const prev = el.querySelector("#btn-page-prev");
  const next = el.querySelector("#btn-page-next");
  if (prev) prev.addEventListener("click", () => { _filesOffset = Math.max(0, _filesOffset - FILES_PER_PAGE); loadFiles(); });
  if (next) next.addEventListener("click", () => { _filesOffset += FILES_PER_PAGE; loadFiles(); });
}
