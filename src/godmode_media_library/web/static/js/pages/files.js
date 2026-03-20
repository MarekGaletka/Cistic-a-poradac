/* GOD MODE Media Library — Files page (infinite scroll + grid density) */

import { api, apiPost } from "../api.js";
import { $, content, formatBytes, escapeHtml, fileName, IMAGE_EXTS, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";
import { openQuickLook, isQuickLookOpen } from "../quicklook.js";
import { toggleSelect, selectAll, deselectAll, isSelected, getSelectedPaths, getSelectedCount } from "../selection.js";
import { renderTagDots, loadTags, getAllTags, openTagPicker } from "../tags.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

const FILES_PER_PAGE = 500;
let _offset = 0;
let _hasMore = true;
let _loading = false;
let _currentFiles = [];
let _viewMode = "grid";
let _selectionMode = false;
let _filtersVisible = false;
let _sortField = "date";
let _sortDir = "desc";
let _totalLoaded = 0;
let _observer = null;
let _container = null;
let _thumbSize = parseInt(localStorage.getItem("godmode_thumb_size") || "200", 10);
let _favoritesOnly = false;
let _tagFilter = null;
const _videoHoverTimers = new Map();
let _focusedPath = null;
let _spacebarHandler = null;

// ── Drag-to-select state ────────────────────────────
let _dragSelect = null;

export async function render(container) {
  _offset = 0;
  _hasMore = true;
  _loading = false;
  _currentFiles = [];
  _totalLoaded = 0;
  _selectionMode = false;
  _container = container;

  if (_observer) { _observer.disconnect(); _observer = null; }

  let html = `
    <div class="page-header">
      <h2>${t("files.title")} <span class="header-count" id="files-total-count"></span></h2>
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
        <label class="filter-checkbox"><input type="checkbox" id="f-favorites-only"> ${t("files.favorites")}</label>
        <div class="filter-group"><label for="f-tag">${t("tags.filter_by")}</label><select id="f-tag"><option value="">${t("tags.title")}</option></select></div>
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
      <div class="grid-density-control" id="grid-density-control">
        <span class="grid-density-label">${t("files.grid_small")}</span>
        <input type="range" id="grid-density-slider" min="100" max="400" value="${_thumbSize}" step="10" aria-label="${t("files.grid_size")}" title="${t("files.grid_size")}">
        <span class="grid-density-label">${t("files.grid_large")}</span>
      </div>
    </div>
    <div id="files-table" aria-live="polite"></div>`;
  container.innerHTML = html;

  // Grid density slider
  const slider = container.querySelector("#grid-density-slider");
  if (slider) {
    slider.addEventListener("input", (e) => {
      _thumbSize = parseInt(e.target.value, 10);
      const grid = container.querySelector(".file-grid");
      if (grid) grid.style.setProperty("--thumb-size", _thumbSize + "px");
      localStorage.setItem("godmode_thumb_size", String(_thumbSize));
    });
  }

  // Bind sort events
  container.querySelector("#sort-field").addEventListener("change", (e) => {
    _sortField = e.target.value;
    resetAndReload();
  });
  container.querySelector("#btn-sort-dir").addEventListener("click", () => {
    _sortDir = _sortDir === "asc" ? "desc" : "asc";
    const btn = container.querySelector("#btn-sort-dir");
    if (btn) {
      btn.title = _sortDir === "asc" ? t("files.sort_asc") : t("files.sort_desc");
      btn.innerHTML = `${_sortDir === "asc" ? "\u2191" : "\u2193"} ${_sortDir === "asc" ? t("files.sort_asc") : t("files.sort_desc")}`;
    }
    resetAndReload();
  });

  // Bind events
  container.querySelector("#btn-files-search").addEventListener("click", () => { resetAndReload(); });
  container.querySelector("#btn-view-table").addEventListener("click", () => { _viewMode = "table"; resetAndReload(); updateViewToggle(container); });
  container.querySelector("#btn-view-grid").addEventListener("click", () => { _viewMode = "grid"; resetAndReload(); updateViewToggle(container); });

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
    resetAndReload();
  });

  container.querySelector("#btn-select-all").addEventListener("click", () => { selectAll(_currentFiles.map(f => f.path)); resetAndReload(); updateSelectionCount(container); });
  container.querySelector("#btn-deselect-all").addEventListener("click", () => { deselectAll(); resetAndReload(); updateSelectionCount(container); });
  container.querySelector("#btn-exit-selection").addEventListener("click", () => {
    _selectionMode = false;
    deselectAll();
    container.querySelector("#selection-bar")?.classList.add("hidden");
    resetAndReload();
  });

  // Enter key triggers search
  container.querySelectorAll(".filters input").forEach(input => {
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") { resetAndReload(); }
    });
  });

  // Show/hide density control based on view mode
  updateDensityVisibility(container);

  // Setup drag-to-select
  setupDragSelect(container);

  // Load tags for filter dropdown
  loadTags().then(tags => {
    const sel = container.querySelector("#f-tag");
    if (sel && tags.length) {
      for (const tag of tags) {
        const opt = document.createElement("option");
        opt.value = String(tag.id);
        opt.textContent = tag.name;
        if (_tagFilter !== null && _tagFilter === tag.id) opt.selected = true;
        sel.appendChild(opt);
      }
    }
  });

  // Tag filter change
  container.querySelector("#f-tag")?.addEventListener("change", (e) => {
    _tagFilter = e.target.value ? parseInt(e.target.value, 10) : null;
    resetAndReload();
  });

  // Spacebar → Quick Look
  if (_spacebarHandler) document.removeEventListener("keydown", _spacebarHandler);
  _spacebarHandler = (e) => {
    if (e.key !== " " || e.target.matches("input, textarea, select")) return;
    if (isQuickLookOpen()) return; // Quick Look handles its own spacebar
    if (!_focusedPath) return;
    e.preventDefault();
    const allPaths = _currentFiles.map(f => f.path);
    openQuickLook(_focusedPath, allPaths);
  };
  document.addEventListener("keydown", _spacebarHandler);

  loadFiles();
}

function resetAndReload() {
  _offset = 0;
  _hasMore = true;
  _currentFiles = [];
  _totalLoaded = 0;
  if (_observer) { _observer.disconnect(); _observer = null; }
  updateDensityVisibility(_container);
  loadFiles();
}

function updateDensityVisibility(container) {
  const ctrl = container?.querySelector("#grid-density-control");
  if (ctrl) ctrl.style.display = _viewMode === "grid" ? "" : "none";
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
  if ($("#f-favorites-only")?.checked) count++;
  if ($("#f-tag")?.value) count++;
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

function _buildQuery() {
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
  const favsOnly = $("#f-favorites-only")?.checked;
  if (favsOnly) q += "&favorites_only=true";
  if (_tagFilter !== null) q += `&tag_id=${_tagFilter}`;
  if (_sortField) q += `&sort=${_sortField}`;
  if (_sortDir) q += `&order=${_sortDir}`;
  return q;
}

async function loadFiles() {
  if (_loading) return;
  _loading = true;

  const isFirstLoad = _offset === 0;
  const filesEl = $("#files-table");
  if (!filesEl) { _loading = false; return; }

  // Show spinner for first load or sentinel for subsequent
  if (isFirstLoad) {
    filesEl.innerHTML = `<div class="loading"><div class="spinner" role="status" aria-label="${t("general.loading")}"></div>${t("general.loading")}</div>`;
  }

  const q = _buildQuery() + `&offset=${_offset}`;

  try {
    const data = await api(q);
    updateFilterBadge();

    const newFiles = data.files;
    _hasMore = data.has_more;

    if (isFirstLoad && !newFiles.length) {
      filesEl.innerHTML = `<div class="empty-state-hero" style="padding:40px 0">
        <div class="empty-state-icon" style="font-size:48px">&#128269;</div>
        <h3 class="empty-state-title">${t("files.empty_title")}</h3>
        <p class="empty-state-subtitle">${t("files.empty_hint")}</p>
      </div>`;
      _loading = false;
      updateTotalCount();
      return;
    }

    _currentFiles = _currentFiles.concat(newFiles);
    _totalLoaded = _currentFiles.length;
    _offset += newFiles.length;

    if (isFirstLoad) {
      renderInitial(filesEl);
    } else {
      appendItems(filesEl, newFiles);
    }

    updateTotalCount();
    updateSentinel(filesEl);
    setupIntersectionObserver(filesEl);
  } catch (e) {
    if (isFirstLoad) {
      filesEl.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
    }
  }

  _loading = false;
}

function updateTotalCount() {
  const el = document.querySelector("#files-total-count");
  if (el) {
    const countStr = _totalLoaded.toLocaleString("cs-CZ");
    el.textContent = _hasMore
      ? t("files.total_count", { count: countStr + "+" })
      : t("files.total_count", { count: countStr });
  }
}

function renderInitial(filesEl) {
  if (_viewMode === "grid") {
    renderGridInitial(filesEl);
  } else {
    renderTableInitial(filesEl);
  }
}

function renderGridInitial(filesEl) {
  let html = `<div class="file-grid files-grid files-grid-large" style="--thumb-size: ${_thumbSize}px">`;
  for (const f of _currentFiles) {
    html += renderGridItem(f);
  }
  html += '</div>';
  html += renderSentinel();
  filesEl.innerHTML = html;
  bindFileEvents(filesEl);
}

function renderTableInitial(filesEl) {
  let html = `<table id="files-data-table"><thead><tr>`;
  if (_selectionMode) html += `<th class="select-cell"></th>`;
  html += `<th>${t("files.name")}</th><th>${t("files.ext")}</th><th>${t("files.size")}</th><th>${t("files.camera")}</th><th>${t("files.date")}</th><th>${t("files.gps")}</th><th>${t("files.resolution")}</th></tr></thead><tbody>`;
  for (const f of _currentFiles) {
    html += renderTableRow(f);
  }
  html += '</tbody></table>';
  html += renderSentinel();
  filesEl.innerHTML = html;
  bindFileEvents(filesEl);
}

function appendItems(filesEl, newFiles) {
  if (_viewMode === "grid") {
    const grid = filesEl.querySelector(".file-grid");
    if (!grid) return;
    const fragment = document.createDocumentFragment();
    const temp = document.createElement("div");
    for (const f of newFiles) {
      temp.innerHTML = renderGridItem(f);
      const item = temp.firstElementChild;
      fragment.appendChild(item);
    }
    grid.appendChild(fragment);
    bindNewItems(filesEl, newFiles);
  } else {
    const tbody = filesEl.querySelector("tbody");
    if (!tbody) return;
    const fragment = document.createDocumentFragment();
    const temp = document.createElement("tbody");
    for (const f of newFiles) {
      temp.innerHTML = renderTableRow(f);
      const row = temp.firstElementChild;
      fragment.appendChild(row);
    }
    tbody.appendChild(fragment);
    bindNewItems(filesEl, newFiles);
  }
}

function renderGridItem(f) {
  const ext = (f.ext || "").toLowerCase();
  const isImage = IMAGE_EXTS.has(ext);
  const isVideo = VIDEO_EXTS.has(ext);
  const checked = isSelected(f.path) ? "checked" : "";
  const thumb = isImage
    ? `<img src="/api/thumbnail${encodeURI(f.path)}?size=300" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}" loading="lazy">`
    : `<div class="grid-icon">${escapeHtml(f.ext)}</div>`;

  const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
  const dateStr = f.date_original ? f.date_original.split(" ")[0] : "";

  let html = `<div class="file-grid-item file-grid-item-large" data-file-path="${escapeHtml(f.path)}" tabindex="0" role="button">`;

  if (_selectionMode) {
    html += `<div class="grid-select"><input type="checkbox" data-select-path="${escapeHtml(f.path)}" ${checked}></div>`;
  }

  html += `<div class="grid-thumb grid-thumb-large">${thumb}`;

  // Thumbnail badges
  if (isVideo) html += `<span class="thumb-badge thumb-badge-video">\uD83C\uDFAC</span>`;
  if (f.gps_latitude) html += `<span class="thumb-badge thumb-badge-gps">\uD83D\uDCCD</span>`;
  if (f.duplicate_group_id) html += `<span class="thumb-badge thumb-badge-dup">\uD83D\uDCCB</span>`;
  if (f.is_favorite) html += `<span class="thumb-badge thumb-badge-fav">\u2B50</span>`;

  // Video play overlay
  if (isVideo) html += `<span class="video-play-overlay">\u25B6</span>`;

  // Tag dots
  html += renderTagDots(f.tags);

  html += `</div>`;

  html += `<div class="grid-hover-overlay">
    <div class="grid-hover-name">${escapeHtml(fileName(f.path))}</div>
    <div class="grid-hover-meta">${formatBytes(f.size)}${dateStr ? ' &middot; ' + escapeHtml(dateStr) : ''}${cam ? ' &middot; ' + escapeHtml(cam) : ''}</div>
  </div>`;
  html += `</div>`;
  return html;
}

function renderTableRow(f) {
  const gps = f.gps_latitude ? `${f.gps_latitude.toFixed(4)}, ${f.gps_longitude.toFixed(4)}` : "";
  const res = f.width && f.height ? `${f.width}x${f.height}` : "";
  const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
  const checked = isSelected(f.path) ? "checked" : "";
  let html = `<tr class="file-row" tabindex="0" role="button" aria-label="${escapeHtml(fileName(f.path))}" data-file-path="${escapeHtml(f.path)}">`;
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
  return html;
}

function renderSentinel() {
  if (!_hasMore) {
    return `<div class="infinite-scroll-sentinel" id="scroll-sentinel">
      <span class="scroll-end-msg">${t("files.all_loaded")}</span>
    </div>`;
  }
  return `<div class="infinite-scroll-sentinel" id="scroll-sentinel">
    <div class="scroll-spinner"><div class="spinner"></div> ${t("files.loading_more")}</div>
  </div>`;
}

function updateSentinel(filesEl) {
  const sentinel = filesEl.querySelector("#scroll-sentinel");
  if (sentinel) {
    if (!_hasMore) {
      sentinel.innerHTML = `<span class="scroll-end-msg">${t("files.all_loaded")}</span>`;
    } else {
      sentinel.innerHTML = `<div class="scroll-spinner"><div class="spinner"></div> ${t("files.loading_more")}</div>`;
    }
  }
}

function setupIntersectionObserver(filesEl) {
  if (_observer) _observer.disconnect();
  if (!_hasMore) return;

  const sentinel = filesEl.querySelector("#scroll-sentinel");
  if (!sentinel) return;

  _observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (entry.isIntersecting && _hasMore && !_loading) {
        loadFiles();
      }
    }
  }, { rootMargin: "400px" });

  _observer.observe(sentinel);
}

function _bindVideoHover(row) {
  const filePath = row.dataset.filePath;
  const fileObj = _currentFiles.find(f => f.path === filePath);
  if (!fileObj) return;
  const ext = (fileObj.ext || "").toLowerCase();
  if (!VIDEO_EXTS.has(ext)) return;

  row.addEventListener("mouseenter", () => {
    const timer = setTimeout(() => {
      const thumb = row.querySelector(".grid-thumb");
      if (!thumb || thumb.querySelector(".video-hover-preview")) return;
      const video = document.createElement("video");
      video.className = "video-hover-preview";
      video.src = `/api/stream${encodeURI(filePath)}`;
      video.muted = true;
      video.autoplay = true;
      video.loop = true;
      video.playsInline = true;
      thumb.appendChild(video);
    }, 500);
    _videoHoverTimers.set(filePath, timer);
  });

  row.addEventListener("mouseleave", () => {
    const timer = _videoHoverTimers.get(filePath);
    if (timer) { clearTimeout(timer); _videoHoverTimers.delete(filePath); }
    const video = row.querySelector(".video-hover-preview");
    if (video) { video.pause(); video.remove(); }
  });
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

  // Row/card clicks — open lightbox for images/videos, modal for others
  const lightboxPaths = _currentFiles
    .filter(f => {
      const ext = (f.ext || "").toLowerCase();
      return IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
    })
    .map(f => f.path);

  el.querySelectorAll("[data-file-path]").forEach(row => {
    if (row._boundClick) return; // avoid double binding
    row._boundClick = true;
    const handler = (e) => {
      if (e.type === "keydown" && e.key !== "Enter") return;
      if (e.target.matches("input[type=checkbox]")) return;
      const filePath = row.dataset.filePath;
      _setFocusedPath(filePath, el);
      const lbIndex = lightboxPaths.indexOf(filePath);
      if (lbIndex >= 0) {
        openLightbox(lightboxPaths, lbIndex);
      } else {
        const allPaths = _currentFiles.map(f => f.path);
        openQuickLook(filePath, allPaths);
      }
    };
    row.addEventListener("click", handler);
    row.addEventListener("keydown", handler);

    // Right-click / context focus (set focus without opening)
    row.addEventListener("mousedown", () => {
      _setFocusedPath(row.dataset.filePath, el);
    });

    // Video hover preview (grid only)
    if (_viewMode === "grid") _bindVideoHover(row);
  });
}

function bindNewItems(el, newFiles) {
  // Only bind events on newly added items
  const lightboxPaths = _currentFiles
    .filter(f => {
      const ext = (f.ext || "").toLowerCase();
      return IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
    })
    .map(f => f.path);

  const newPaths = new Set(newFiles.map(f => f.path));

  el.querySelectorAll("[data-file-path]").forEach(row => {
    if (row._boundClick) return;
    const filePath = row.dataset.filePath;
    if (!newPaths.has(filePath)) return;
    row._boundClick = true;

    if (_selectionMode) {
      const cb = row.querySelector("[data-select-path]");
      if (cb) {
        cb.addEventListener("click", e => {
          e.stopPropagation();
          toggleSelect(cb.dataset.selectPath);
        });
      }
    }

    const handler = (e) => {
      if (e.type === "keydown" && e.key !== "Enter") return;
      if (e.target.matches("input[type=checkbox]")) return;
      _setFocusedPath(filePath, el);
      const lbIndex = lightboxPaths.indexOf(filePath);
      if (lbIndex >= 0) {
        openLightbox(lightboxPaths, lbIndex);
      } else {
        showFileDetail(filePath);
      }
    };
    row.addEventListener("click", handler);
    row.addEventListener("keydown", handler);

    // Right-click / context focus (set focus without opening)
    row.addEventListener("mousedown", () => {
      _setFocusedPath(filePath, el);
    });

    // Video hover preview (grid only)
    if (_viewMode === "grid") _bindVideoHover(row);
  });
}

// ── Focus tracking (for Quick Look) ─────────────────

function _setFocusedPath(path, parentEl) {
  _focusedPath = path;
  // Update visual focus indicator
  const root = parentEl || _container;
  if (!root) return;
  root.querySelectorAll("[data-file-path]").forEach(item => {
    item.classList.toggle("file-focused", item.dataset.filePath === path);
  });
}

// ── Drag-to-select ──────────────────────────────────

function setupDragSelect(container) {
  // Clean up previous handlers
  if (_dragSelect) {
    document.removeEventListener("mousemove", _dragSelect.moveHandler);
    document.removeEventListener("mouseup", _dragSelect.upHandler);
    _dragSelect = null;
  }

  const filesTable = container.querySelector("#files-table");
  if (!filesTable) return;

  filesTable.addEventListener("mousedown", (e) => {
    // Only activate in grid view
    if (_viewMode !== "grid") return;
    // Only on left click
    if (e.button !== 0) return;
    // Skip if clicking on a file item, button, input, or link
    if (e.target.closest(".file-grid-item, button, input, a, .lightbox-overlay")) return;
    // Skip touch events
    if (e.sourceCapabilities && e.sourceCapabilities.firesTouchEvents) return;

    e.preventDefault();

    // Auto-enter selection mode
    if (!_selectionMode) {
      _selectionMode = true;
      const selBar = container.querySelector("#selection-bar");
      if (selBar) selBar.classList.remove("hidden");
    }

    const startX = e.clientX;
    const startY = e.clientY;
    let rect = null;

    const moveHandler = (me) => {
      me.preventDefault();
      if (!rect) {
        // Only start after a minimum drag distance (5px)
        const dx = me.clientX - startX;
        const dy = me.clientY - startY;
        if (Math.abs(dx) < 5 && Math.abs(dy) < 5) return;
        rect = document.createElement("div");
        rect.className = "selection-rect";
        document.body.appendChild(rect);
        document.body.style.userSelect = "none";
      }

      const x = Math.min(startX, me.clientX);
      const y = Math.min(startY, me.clientY);
      const w = Math.abs(me.clientX - startX);
      const h = Math.abs(me.clientY - startY);

      rect.style.left = x + "px";
      rect.style.top = y + "px";
      rect.style.width = w + "px";
      rect.style.height = h + "px";

      // Check intersections with file grid items
      const selRect = { left: x, top: y, right: x + w, bottom: y + h };
      filesTable.querySelectorAll(".file-grid-item").forEach(item => {
        const ir = item.getBoundingClientRect();
        const intersects = !(ir.right < selRect.left || ir.left > selRect.right ||
                             ir.bottom < selRect.top || ir.top > selRect.bottom);
        item.classList.toggle("drag-selected", intersects);
      });
    };

    const upHandler = () => {
      document.removeEventListener("mousemove", moveHandler);
      document.removeEventListener("mouseup", upHandler);
      document.body.style.userSelect = "";

      if (rect) {
        // Finalize selection
        filesTable.querySelectorAll(".file-grid-item.drag-selected").forEach(item => {
          const path = item.dataset.filePath;
          if (path && !isSelected(path)) {
            toggleSelect(path);
          }
          item.classList.remove("drag-selected");
        });
        rect.remove();
        updateSelectionCount(container);
      }
    };

    document.addEventListener("mousemove", moveHandler);
    document.addEventListener("mouseup", upHandler);

    _dragSelect = { moveHandler, upHandler };
  });
}
