/* GOD MODE Media Library — Gallery & Slideshow page */

import { api } from "../api.js";
import { $, content, formatBytes, escapeHtml, fileName, IMAGE_EXTS, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { openLightbox } from "../lightbox.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

let _collections = {};
let _activeCollection = "best_of";
let _slideshowActive = false;
let _slideshowTimer = null;
let _slideshowIndex = 0;
let _slideshowFiles = [];
let _slideshowInterval = 5000;
let _slideshowOverlay = null;
let _slideshowPaused = false;
let _slideshowTransitioning = false;
let _slideshowLoop = true;

// ── Helpers ─────────────────────────────────────────────────────────

function _thumbUrl(path, size = 400) {
  return `/api/thumbnail${encodeURI(path)}?size=${size}`;
}

function _streamUrl(path) {
  return `/api/stream${encodeURI(path)}`;
}

// ── Collection metadata ─────────────────────────────────────────────

const COLLECTION_META = {
  best_of:      { icon: "\u2B50", color: "#f59e0b" },
  masterpieces: { icon: "\uD83C\uDFC6", color: "#eab308" },
  top_rated:    { icon: "\u2764\uFE0F", color: "#ef4444" },
  travel:       { icon: "\uD83C\uDF0D", color: "#3b82f6" },
  pro_shots:    { icon: "\uD83D\uDCF7", color: "#8b5cf6" },
  recent:       { icon: "\u2728", color: "#10b981" },
  hidden_gems:  { icon: "\uD83D\uDC8E", color: "#ec4899" },
};

const TIER_COLORS = {
  masterpiece: "#f59e0b",
  excellent:   "#10b981",
  good:        "#3b82f6",
  average:     "#6b7280",
  poor:        "#9ca3af",
};

// ── Init ────────────────────────────────────────────────────────────

export async function render(container) {
  container.innerHTML = `
    <div class="gallery-page">
      <div class="gallery-header">
        <h1>${t("gallery.title")}</h1>
        <p class="gallery-subtitle">${t("gallery.subtitle")}</p>
      </div>

      <div class="gallery-collections" id="gallery-collections">
        <div class="loading"><div class="spinner"></div>${t("general.loading")}</div>
      </div>

      <div class="gallery-toolbar" id="gallery-toolbar" style="display:none">
        <div class="gallery-toolbar-left">
          <h2 id="gallery-collection-title"></h2>
          <span id="gallery-collection-count" class="gallery-count-badge"></span>
        </div>
        <div class="gallery-toolbar-right">
          <label class="gallery-loop-toggle" title="${t("gallery.loop")}">
            <input type="checkbox" id="gallery-loop-check" checked>
            \uD83D\uDD01 ${t("gallery.loop")}
          </label>
          <select id="gallery-slideshow-speed" class="gallery-speed-select" title="${t("gallery.speed")}">
            <option value="3000">3s</option>
            <option value="5000" selected>5s</option>
            <option value="8000">8s</option>
            <option value="12000">12s</option>
            <option value="20000">20s</option>
          </select>
          <button id="gallery-slideshow-btn" class="btn btn-accent" title="${t("gallery.start_slideshow")}">
            \u25B6 ${t("gallery.slideshow")}
          </button>
        </div>
      </div>

      <div class="gallery-grid" id="gallery-grid"></div>
    </div>
  `;

  $("#gallery-slideshow-btn")?.addEventListener("click", _startSlideshow);
  $("#gallery-slideshow-speed")?.addEventListener("change", (e) => {
    _slideshowInterval = parseInt(e.target.value, 10);
  });
  $("#gallery-loop-check")?.addEventListener("change", (e) => {
    _slideshowLoop = e.target.checked;
  });

  await _loadCollections();
}

export function destroy() {
  _stopSlideshow();
}

// ── Data loading ────────────────────────────────────────────────────

async function _loadCollections() {
  try {
    const data = await api("/gallery/collections");
    _collections = data.collections || {};
    _renderCollections();
  } catch (e) {
    console.error("Failed to load collections:", e);
    $("#gallery-collections").innerHTML = `
      <div class="gallery-empty">${t("gallery.load_error")}</div>
    `;
  }
}

async function _loadCollection(name) {
  _activeCollection = name;
  const toolbar = $("#gallery-toolbar");
  const grid = $("#gallery-grid");
  if (!toolbar || !grid) return;

  toolbar.style.display = "flex";

  const meta = COLLECTION_META[name] || { icon: "\uD83D\uDCCE", color: "#6b7280" };
  $("#gallery-collection-title").innerHTML = `${meta.icon} ${t("gallery.col_" + name)}`;

  const files = _collections[name] || [];
  $("#gallery-collection-count").textContent = `${files.length} ${t("gallery.items")}`;

  document.querySelectorAll(".gallery-col-card").forEach((c) => {
    c.classList.toggle("active", c.dataset.collection === name);
  });

  _renderGrid(files);
}

// ── Render collections ──────────────────────────────────────────────

function _renderCollections() {
  const el = $("#gallery-collections");
  if (!el) return;

  const names = Object.keys(_collections).filter((k) => (_collections[k]?.length || 0) > 0);

  if (names.length === 0) {
    el.innerHTML = `
      <div class="gallery-empty">
        <div class="gallery-empty-icon">\uD83C\uDFA8</div>
        <h3>${t("gallery.no_media")}</h3>
        <p>${t("gallery.scan_hint")}</p>
      </div>
    `;
    return;
  }

  el.innerHTML = names
    .map((name) => {
      const meta = COLLECTION_META[name] || { icon: "\uD83D\uDCCE", color: "#6b7280" };
      const files = _collections[name];
      const preview = files.slice(0, 4);

      return `
        <div class="gallery-col-card" data-collection="${name}">
          <div class="gallery-col-preview">
            ${preview
              .map(
                (f) => `
              <div class="gallery-col-thumb">
                <img src="${_thumbUrl(f.path, 300)}" loading="lazy" alt="" />
              </div>
            `,
              )
              .join("")}
            ${preview.length < 4
              ? Array(4 - preview.length)
                  .fill('<div class="gallery-col-thumb gallery-col-thumb-empty"></div>')
                  .join("")
              : ""}
          </div>
          <div class="gallery-col-info">
            <span class="gallery-col-icon" style="color:${meta.color}">${meta.icon}</span>
            <span class="gallery-col-name">${t("gallery.col_" + name)}</span>
            <span class="gallery-col-count">${files.length}</span>
          </div>
        </div>
      `;
    })
    .join("");

  el.querySelectorAll(".gallery-col-card").forEach((card) => {
    card.addEventListener("click", () => _loadCollection(card.dataset.collection));
  });

  // Auto-load best_of
  if (_collections.best_of?.length) {
    _loadCollection("best_of");
  } else if (names.length) {
    _loadCollection(names[0]);
  }
}

// ── Render grid ─────────────────────────────────────────────────────

function _renderGrid(files) {
  const grid = $("#gallery-grid");
  if (!grid) return;

  if (!files.length) {
    grid.innerHTML = `<div class="gallery-empty">${t("gallery.empty_collection")}</div>`;
    return;
  }

  grid.innerHTML = files
    .map((f, idx) => {
      const ext = (f.path.split(".").pop() || "").toLowerCase();
      const isVideo = VIDEO_EXTS.has(ext);
      const isImage = IMAGE_EXTS.has(ext);
      const name = fileName(f.path);
      const tierColor = TIER_COLORS[f.tier] || "#6b7280";
      const scoreDisplay = Math.round(f.total);

      return `
        <div class="gallery-item" data-index="${idx}" data-path="${escapeHtml(f.path)}">
          <div class="gallery-thumb">
            ${
              isImage || isVideo
                ? `<img src="${_thumbUrl(f.path, 400)}"
                       loading="lazy" alt="${escapeHtml(name)}" />`
                : `<div class="gallery-thumb-icon">${isVideo ? "\uD83C\uDFAC" : "\uD83D\uDCC4"}</div>`
            }
            ${isVideo ? '<div class="gallery-video-badge">\u25B6</div>' : ""}
            <div class="gallery-score-badge" style="background:${tierColor}">
              ${scoreDisplay}
            </div>
            <div class="gallery-tier-label">${t("gallery.tier_" + f.tier)}</div>
          </div>
          <div class="gallery-item-info">
            <div class="gallery-item-name" title="${escapeHtml(name)}">${escapeHtml(name)}</div>
          </div>
        </div>
      `;
    })
    .join("");

  grid.querySelectorAll(".gallery-item").forEach((item) => {
    item.addEventListener("click", () => {
      const idx = parseInt(item.dataset.index, 10);
      const paths = files.map((f) => f.path);
      openLightbox(paths, idx, item.querySelector("img"));
    });
  });
}

// ── Slideshow ───────────────────────────────────────────────────────

function _startSlideshow() {
  const files = _collections[_activeCollection];
  if (!files?.length) {
    showToast(t("gallery.no_files_slideshow"), "warning");
    return;
  }

  _slideshowFiles = files;
  _slideshowIndex = 0;
  _slideshowActive = true;
  _slideshowPaused = false;
  _slideshowTransitioning = false;

  _slideshowOverlay = document.createElement("div");
  _slideshowOverlay.className = "slideshow-overlay";
  _slideshowOverlay.innerHTML = `
    <div class="slideshow-media-wrap" id="slideshow-media"></div>

    <div class="slideshow-controls">
      <button class="slideshow-btn" id="ss-prev" title="${t("gallery.prev")}">\u276E</button>
      <button class="slideshow-btn" id="ss-play-pause" title="${t("gallery.pause")}">\u23F8</button>
      <button class="slideshow-btn" id="ss-next" title="${t("gallery.next")}">\u276F</button>
      <div class="slideshow-separator"></div>
      <button class="slideshow-btn ${_slideshowLoop ? 'ss-active' : ''}" id="ss-loop" title="${t("gallery.loop")}">\uD83D\uDD01</button>
      <button class="slideshow-btn" id="ss-fullscreen" title="${t("gallery.fullscreen")}">\u26F6</button>
    </div>

    <div class="slideshow-top-bar">
      <div class="slideshow-counter" id="ss-counter"></div>
      <div class="slideshow-score" id="ss-score"></div>
      <button class="slideshow-close" id="ss-close">\u2715</button>
    </div>

    <div class="slideshow-progress-track">
      <div class="slideshow-progress-bar" id="ss-progress"></div>
    </div>

    <div class="slideshow-info" id="ss-info"></div>
  `;
  document.body.appendChild(_slideshowOverlay);
  document.body.style.overflow = "hidden";

  // Bind controls
  _slideshowOverlay.querySelector("#ss-close").addEventListener("click", _stopSlideshow);
  _slideshowOverlay.querySelector("#ss-prev").addEventListener("click", () => _slideshowNav(-1));
  _slideshowOverlay.querySelector("#ss-next").addEventListener("click", () => _slideshowNav(1));
  _slideshowOverlay.querySelector("#ss-play-pause").addEventListener("click", _toggleSlideshowPause);
  _slideshowOverlay.querySelector("#ss-loop").addEventListener("click", _toggleLoop);
  _slideshowOverlay.querySelector("#ss-fullscreen").addEventListener("click", _toggleFullscreen);

  // Click on media area pauses/resumes
  _slideshowOverlay.querySelector("#slideshow-media").addEventListener("click", (e) => {
    if (e.target.closest(".slideshow-controls, .slideshow-top-bar")) return;
    _toggleSlideshowPause();
  });

  // Keyboard
  _slideshowOverlay._keyHandler = (e) => {
    if (e.key === "Escape") _stopSlideshow();
    else if (e.key === "ArrowLeft") _slideshowNav(-1);
    else if (e.key === "ArrowRight") _slideshowNav(1);
    else if (e.key === " ") { e.preventDefault(); _toggleSlideshowPause(); }
    else if (e.key === "f" || e.key === "F") _toggleFullscreen();
    else if (e.key === "l" || e.key === "L") _toggleLoop();
  };
  document.addEventListener("keydown", _slideshowOverlay._keyHandler);

  // Fullscreen change listener
  _slideshowOverlay._fsHandler = () => {
    const btn = _slideshowOverlay?.querySelector("#ss-fullscreen");
    if (btn) {
      btn.textContent = document.fullscreenElement ? "\u2716" : "\u26F6";
    }
  };
  document.addEventListener("fullscreenchange", _slideshowOverlay._fsHandler);

  _showSlideshowSlide();
  _startSlideshowTimer();
}

function _stopSlideshow() {
  _slideshowActive = false;
  if (_slideshowTimer) clearInterval(_slideshowTimer);
  _slideshowTimer = null;

  // Exit fullscreen if active
  if (document.fullscreenElement) {
    document.exitFullscreen().catch(() => {});
  }

  if (_slideshowOverlay) {
    if (_slideshowOverlay._keyHandler) {
      document.removeEventListener("keydown", _slideshowOverlay._keyHandler);
    }
    if (_slideshowOverlay._fsHandler) {
      document.removeEventListener("fullscreenchange", _slideshowOverlay._fsHandler);
    }
    _slideshowOverlay.remove();
    _slideshowOverlay = null;
  }
  document.body.style.overflow = "";
}

function _toggleSlideshowPause() {
  _slideshowPaused = !_slideshowPaused;
  const btn = _slideshowOverlay?.querySelector("#ss-play-pause");
  if (btn) {
    btn.textContent = _slideshowPaused ? "\u25B6" : "\u23F8";
    btn.title = _slideshowPaused ? t("gallery.play") : t("gallery.pause");
  }

  // Show a brief pause/play indicator
  _showPauseIndicator(_slideshowPaused);

  if (_slideshowPaused) {
    if (_slideshowTimer) clearInterval(_slideshowTimer);
    _slideshowTimer = null;
  } else {
    _startSlideshowTimer();
  }
}

function _showPauseIndicator(paused) {
  let ind = _slideshowOverlay?.querySelector(".slideshow-pause-indicator");
  if (!ind) {
    ind = document.createElement("div");
    ind.className = "slideshow-pause-indicator";
    _slideshowOverlay?.querySelector("#slideshow-media")?.appendChild(ind);
  }
  ind.textContent = paused ? "\u23F8" : "\u25B6";
  ind.classList.add("show");
  setTimeout(() => ind.classList.remove("show"), 600);
}

function _toggleLoop() {
  _slideshowLoop = !_slideshowLoop;
  const btn = _slideshowOverlay?.querySelector("#ss-loop");
  if (btn) {
    btn.classList.toggle("ss-active", _slideshowLoop);
    btn.title = _slideshowLoop ? t("gallery.loop_on") : t("gallery.loop_off");
  }
  // Also sync the toolbar checkbox
  const check = $("#gallery-loop-check");
  if (check) check.checked = _slideshowLoop;
}

function _toggleFullscreen() {
  if (!_slideshowOverlay) return;
  if (document.fullscreenElement) {
    document.exitFullscreen().catch(() => {});
  } else {
    _slideshowOverlay.requestFullscreen().catch(() => {});
  }
}

function _slideshowNav(dir) {
  if (_slideshowTransitioning) return;

  const newIndex = _slideshowIndex + dir;

  // Check loop boundary
  if (!_slideshowLoop) {
    if (newIndex < 0 || newIndex >= _slideshowFiles.length) {
      // At the end without loop — stop
      if (newIndex >= _slideshowFiles.length && !_slideshowPaused) {
        _slideshowPaused = true;
        const btn = _slideshowOverlay?.querySelector("#ss-play-pause");
        if (btn) { btn.textContent = "\u25B6"; btn.title = t("gallery.play"); }
        if (_slideshowTimer) clearInterval(_slideshowTimer);
        _slideshowTimer = null;
        showToast(t("gallery.slideshow_ended"), "info");
      }
      return;
    }
    _slideshowIndex = newIndex;
  } else {
    _slideshowIndex = newIndex < 0
      ? _slideshowFiles.length - 1
      : newIndex % _slideshowFiles.length;
  }

  _showSlideshowSlide();
  if (!_slideshowPaused) {
    if (_slideshowTimer) clearInterval(_slideshowTimer);
    _startSlideshowTimer();
  }
}

function _startSlideshowTimer() {
  if (_slideshowTimer) clearInterval(_slideshowTimer);
  _slideshowTimer = setInterval(() => {
    if (!_slideshowPaused && _slideshowActive) {
      const nextIndex = _slideshowIndex + 1;
      if (!_slideshowLoop && nextIndex >= _slideshowFiles.length) {
        // End of slideshow without loop
        _slideshowPaused = true;
        const btn = _slideshowOverlay?.querySelector("#ss-play-pause");
        if (btn) { btn.textContent = "\u25B6"; btn.title = t("gallery.play"); }
        clearInterval(_slideshowTimer);
        _slideshowTimer = null;
        showToast(t("gallery.slideshow_ended"), "info");
        return;
      }
      _slideshowIndex = _slideshowLoop
        ? nextIndex % _slideshowFiles.length
        : nextIndex;
      _showSlideshowSlide();
    }
  }, _slideshowInterval);
}

function _showSlideshowSlide() {
  const wrap = _slideshowOverlay?.querySelector("#slideshow-media");
  const counter = _slideshowOverlay?.querySelector("#ss-counter");
  const scoreEl = _slideshowOverlay?.querySelector("#ss-score");
  const infoEl = _slideshowOverlay?.querySelector("#ss-info");
  const progressBar = _slideshowOverlay?.querySelector("#ss-progress");
  if (!wrap) return;

  const f = _slideshowFiles[_slideshowIndex];
  if (!f) return;

  const ext = (f.path.split(".").pop() || "").toLowerCase();
  const isVideo = VIDEO_EXTS.has(ext);
  const name = fileName(f.path);
  const tierColor = TIER_COLORS[f.tier] || "#6b7280";

  // Fade out
  _slideshowTransitioning = true;
  wrap.classList.add("ss-fade-out");

  setTimeout(() => {
    if (isVideo) {
      wrap.innerHTML = `
        <video class="slideshow-video" src="${_streamUrl(f.path)}"
               autoplay muted loop playsinline></video>
      `;
    } else {
      // Use stream endpoint for full-resolution images
      wrap.innerHTML = `
        <img class="slideshow-image"
             src="${_streamUrl(f.path)}"
             alt="${escapeHtml(name)}"
             onerror="this.src='${_thumbUrl(f.path, 800)}'" />
      `;
    }

    wrap.classList.remove("ss-fade-out");
    wrap.classList.add("ss-fade-in");
    _slideshowTransitioning = false;

    setTimeout(() => wrap.classList.remove("ss-fade-in"), 500);
  }, 300);

  // Update UI
  if (counter) {
    counter.textContent = `${_slideshowIndex + 1} / ${_slideshowFiles.length}`;
  }
  if (scoreEl) {
    scoreEl.innerHTML = `
      <span class="ss-score-value" style="color:${tierColor}">${Math.round(f.total)}</span>
      <span class="ss-score-label">${t("gallery.tier_" + f.tier)}</span>
    `;
  }
  if (infoEl) {
    infoEl.innerHTML = `<span class="ss-file-name">${escapeHtml(name)}</span>`;
  }
  if (progressBar) {
    progressBar.style.transition = "none";
    progressBar.style.width = "0%";
    void progressBar.offsetWidth;
    progressBar.style.transition = `width ${_slideshowInterval}ms linear`;
    progressBar.style.width = "100%";
  }

  // Preload next
  const nextIdx = _slideshowLoop
    ? (_slideshowIndex + 1) % _slideshowFiles.length
    : Math.min(_slideshowIndex + 1, _slideshowFiles.length - 1);
  const nextFile = _slideshowFiles[nextIdx];
  if (nextFile) {
    const nextExt = (nextFile.path.split(".").pop() || "").toLowerCase();
    if (!VIDEO_EXTS.has(nextExt)) {
      const preload = new Image();
      preload.src = _streamUrl(nextFile.path);
    }
  }
}
