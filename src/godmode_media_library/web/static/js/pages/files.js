/* GOD MODE Media Library — Files page */

import { api } from "../api.js";
import { $, content, formatBytes, escapeHtml, fileName } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { toggleSelect, selectAll, deselectAll, isSelected, getSelectedPaths } from "../selection.js";

const FILES_PER_PAGE = 50;
let _filesOffset = 0;
let _currentFiles = [];
let _viewMode = "table"; // "table" or "grid"

export async function render(container) {
  _filesOffset = 0;
  let html = `<h2>${t("files.title")}</h2>
    <div class="filters" role="search" aria-label="${t("files.title")}">
      <input type="text" id="f-ext" placeholder="${t("files.ext_placeholder")}" size="10" aria-label="${t("files.ext_placeholder")}">
      <input type="text" id="f-camera" placeholder="${t("files.camera_placeholder")}" size="15" aria-label="${t("files.camera_placeholder")}">
      <input type="text" id="f-path" placeholder="${t("files.path_placeholder")}" size="20" aria-label="${t("files.path_placeholder")}">
      <button id="btn-files-search" aria-label="${t("files.search")}">${t("files.search")}</button>
    </div>
    <div class="filters filters-advanced">
      <div class="filter-group"><label for="f-date-from">${t("files.date_from")}</label><input type="date" id="f-date-from"></div>
      <div class="filter-group"><label for="f-date-to">${t("files.date_to")}</label><input type="date" id="f-date-to"></div>
      <div class="filter-group"><label for="f-min-size">${t("files.min_size")}</label><input type="number" id="f-min-size" min="0" style="width:80px"></div>
      <div class="filter-group"><label for="f-max-size">${t("files.max_size")}</label><input type="number" id="f-max-size" min="0" style="width:80px"></div>
      <label class="filter-checkbox"><input type="checkbox" id="f-has-gps"> ${t("files.has_gps")}</label>
      <label class="filter-checkbox"><input type="checkbox" id="f-has-phash"> ${t("files.has_phash")}</label>
    </div>
    <div class="files-toolbar">
      <div class="view-toggle">
        <button id="btn-view-table" class="${_viewMode === 'table' ? 'active' : ''}" aria-label="Table view" title="Tabulka">&#9776;</button>
        <button id="btn-view-grid" class="${_viewMode === 'grid' ? 'active' : ''}" aria-label="Grid view" title="Mřížka">&#9783;</button>
      </div>
      <div class="files-select-actions">
        <button id="btn-select-all" class="small">${t("action.select_all")}</button>
        <button id="btn-deselect-all" class="small">${t("action.deselect_all")}</button>
      </div>
    </div>
    <div id="files-table" aria-live="polite"></div>`;
  container.innerHTML = html;

  // Bind events
  container.querySelector("#btn-files-search").addEventListener("click", () => { _filesOffset = 0; loadFiles(); });
  container.querySelector("#btn-view-table").addEventListener("click", () => { _viewMode = "table"; renderCurrentFiles(); updateViewToggle(container); });
  container.querySelector("#btn-view-grid").addEventListener("click", () => { _viewMode = "grid"; renderCurrentFiles(); updateViewToggle(container); });
  container.querySelector("#btn-select-all").addEventListener("click", () => { selectAll(_currentFiles.map(f => f.path)); renderCurrentFiles(); });
  container.querySelector("#btn-deselect-all").addEventListener("click", () => { deselectAll(); renderCurrentFiles(); });

  // Enter key triggers search
  container.querySelectorAll(".filters input").forEach(input => {
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") { _filesOffset = 0; loadFiles(); }
    });
  });

  loadFiles();
}

function updateViewToggle(container) {
  const tbl = container.querySelector("#btn-view-table");
  const grd = container.querySelector("#btn-view-grid");
  if (tbl) tbl.classList.toggle("active", _viewMode === "table");
  if (grd) grd.classList.toggle("active", _viewMode === "grid");
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
    if (!data.files.length) {
      $("#files-table").innerHTML = `<div class="empty"><div class="empty-icon">&#128269;</div><div class="empty-text">${t("files.empty_title")}</div><div class="empty-hint">${t("files.empty_hint")}</div></div>`;
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
  let html = `<table><tr><th></th><th>${t("files.name")}</th><th>${t("files.ext")}</th><th>${t("files.size")}</th><th>${t("files.camera")}</th><th>${t("files.date")}</th><th>${t("files.gps")}</th><th>${t("files.resolution")}</th></tr>`;
  for (const f of data.files) {
    const gps = f.gps_latitude ? `${f.gps_latitude.toFixed(4)}, ${f.gps_longitude.toFixed(4)}` : "";
    const res = f.width && f.height ? `${f.width}x${f.height}` : "";
    const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
    const checked = isSelected(f.path) ? "checked" : "";
    html += `<tr class="file-row" tabindex="0" role="button" aria-label="${escapeHtml(fileName(f.path))}" data-file-path="${escapeHtml(f.path)}">
      <td class="select-cell"><input type="checkbox" data-select-path="${escapeHtml(f.path)}" ${checked} aria-label="${t("action.select_all")}"></td>
      <td class="path" title="${escapeHtml(f.path)}">${escapeHtml(fileName(f.path))}</td>
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
  const IMAGE_EXTS = new Set(["jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp", "heic", "heif"]);
  let html = '<div class="files-grid">';
  for (const f of data.files) {
    const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
    const checked = isSelected(f.path) ? "checked" : "";
    const thumb = isImage
      ? `<img src="/api/thumbnail${encodeURI(f.path)}?size=200" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}">`
      : `<div class="grid-icon">${escapeHtml(f.ext)}</div>`;
    html += `<div class="file-grid-item" data-file-path="${escapeHtml(f.path)}" tabindex="0" role="button">
      <div class="grid-select"><input type="checkbox" data-select-path="${escapeHtml(f.path)}" ${checked}></div>
      <div class="grid-thumb">${thumb}</div>
      <div class="grid-name" title="${escapeHtml(f.path)}">${escapeHtml(fileName(f.path))}</div>
      <div class="grid-meta">${formatBytes(f.size)}</div>
    </div>`;
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
  el.querySelectorAll("[data-select-path]").forEach(cb => {
    cb.addEventListener("click", e => {
      e.stopPropagation();
      toggleSelect(cb.dataset.selectPath);
    });
  });

  // Row/card clicks (excluding checkboxes)
  el.querySelectorAll("[data-file-path]").forEach(row => {
    row.addEventListener("click", e => {
      if (e.target.matches("input[type=checkbox]")) return;
      showFileDetail(row.dataset.filePath);
    });
    row.addEventListener("keydown", e => {
      if (e.key === "Enter") showFileDetail(row.dataset.filePath);
    });
  });

  // Pagination
  const prev = el.querySelector("#btn-page-prev");
  const next = el.querySelector("#btn-page-next");
  if (prev) prev.addEventListener("click", () => { _filesOffset = Math.max(0, _filesOffset - FILES_PER_PAGE); loadFiles(); });
  if (next) next.addEventListener("click", () => { _filesOffset += FILES_PER_PAGE; loadFiles(); });
}
