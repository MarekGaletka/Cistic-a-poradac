/* GOD MODE Media Library — Fullscreen Lightbox (Google Photos / Apple Photos style) */

import { api, apiPost, apiPut, apiDelete } from "./api.js";
import { escapeHtml, fileName, formatBytes, IMAGE_EXTS, showToast } from "./utils.js";
import { t } from "./i18n.js";
import { renderFileTagsWithRemove, openTagPicker } from "./tags.js";
import { openShareModal } from "./share.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

let _paths = [];
let _index = 0;
let _overlay = null;
let _infoOpen = false;
let _zoom = 1;
let _rotation = 0;
let _zoomOriginX = 50;
let _zoomOriginY = 50;
let _preloaded = {};
let _keyHandler = null;
let _wheelHandler = null;
let _fileDetailsCache = {};
let _favoritesSet = new Set();
let _sourceThumbEl = null;

// ── Public API ───────────────────────────────────────

export function openLightbox(paths, startIndex = 0, sourceEl = null) {
  if (!paths || !paths.length) return;
  _paths = paths;
  _index = Math.max(0, Math.min(startIndex, paths.length - 1));
  _zoom = 1;
  _rotation = 0;
  _infoOpen = false;
  _preloaded = {};
  _fileDetailsCache = {};
  _sourceThumbEl = sourceEl || _findSourceThumb(paths[startIndex]);

  _overlay = document.getElementById("lightbox-overlay");
  if (!_overlay) return;

  _overlay.classList.remove("hidden");
  _overlay.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";

  // Load favorites list
  api("/files/favorites").then(data => {
    _favoritesSet = new Set(data.favorites || []);
    _updateFavButton();
  }).catch(() => {});

  renderCurrent();
  _animateFlyIn();
  preloadAdjacent();
  bindEvents();
}

function _findSourceThumb(path) {
  // Try to find the thumbnail element in the DOM for the given path
  if (!path) return null;
  const item = document.querySelector(`[data-file-path="${CSS.escape(path)}"]`);
  if (!item) return null;
  const img = item.querySelector("img");
  return img || item;
}

function _animateFlyIn() {
  if (!_sourceThumbEl || !_overlay) return;
  const sourceRect = _sourceThumbEl.getBoundingClientRect();
  if (!sourceRect || sourceRect.width === 0) return;

  const mediaWrap = document.getElementById("lightbox-media-wrap");
  if (!mediaWrap) return;

  // Create a clone of the thumbnail for fly animation
  const clone = document.createElement("img");
  clone.className = "lightbox-fly-clone";
  clone.src = _sourceThumbEl.src || `/api/thumbnail${encodeURI(_paths[_index])}?size=300`;
  clone.style.left = sourceRect.left + "px";
  clone.style.top = sourceRect.top + "px";
  clone.style.width = sourceRect.width + "px";
  clone.style.height = sourceRect.height + "px";

  // Hide the real media until animation completes
  mediaWrap.classList.add("fly-entering");
  document.body.appendChild(clone);

  // Animate to center of viewport
  requestAnimationFrame(() => {
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const targetSize = Math.min(vw * 0.8, vh * 0.8);
    const targetLeft = (vw - targetSize) / 2;
    const targetTop = (vh - targetSize) / 2;

    clone.style.left = targetLeft + "px";
    clone.style.top = targetTop + "px";
    clone.style.width = targetSize + "px";
    clone.style.height = targetSize + "px";
    clone.style.borderRadius = "4px";

    clone.addEventListener("transitionend", () => {
      mediaWrap.classList.remove("fly-entering");
      clone.remove();
    }, { once: true });

    // Fallback: remove after timeout in case transitionend doesn't fire
    setTimeout(() => {
      mediaWrap.classList.remove("fly-entering");
      if (clone.parentNode) clone.remove();
    }, 400);
  });
}

export function closeLightbox() {
  if (!_overlay) return;
  _overlay.classList.add("hidden");
  _overlay.setAttribute("aria-hidden", "true");
  document.body.style.overflow = "";

  // Stop any playing video
  const video = _overlay.querySelector("video");
  if (video) video.pause();

  // Clean up content
  const contentEl = document.getElementById("lightbox-content");
  if (contentEl) contentEl.innerHTML = "";

  unbindEvents();
  _overlay = null;
  _paths = [];
  _fileDetailsCache = {};
  _preloaded = {};
}

// ── Navigation ───────────────────────────────────────

function goNext() {
  if (_index < _paths.length - 1) {
    _index++;
    _zoom = 1;
    _rotation = 0;
    renderCurrent();
    preloadAdjacent();
  }
}

function goPrev() {
  if (_index > 0) {
    _index--;
    _zoom = 1;
    _rotation = 0;
    renderCurrent();
    preloadAdjacent();
  }
}

// ── Rendering ────────────────────────────────────────

function renderCurrent() {
  const contentEl = document.getElementById("lightbox-content");
  if (!contentEl) return;

  const path = _paths[_index];
  const ext = (path.split(".").pop() || "").toLowerCase();
  const isImage = IMAGE_EXTS.has(ext);
  const isVideo = VIDEO_EXTS.has(ext);

  let mediaHtml;
  if (isImage) {
    const src = `/api/thumbnail${encodeURI(path)}?size=800`;
    mediaHtml = `<img
      id="lightbox-media"
      class="lightbox-image"
      src="${src}"
      alt="${escapeHtml(fileName(path))}"
      draggable="false"
      onerror="this.outerHTML='<div class=\\'lightbox-no-preview\\'>${t("lightbox.no_preview")}</div>'"
    >`;
  } else if (isVideo) {
    const src = `/api/stream${encodeURI(path)}`;
    mediaHtml = `<video
      id="lightbox-media"
      class="lightbox-video"
      controls
      autoplay
      src="${src}"
    >
      <source src="${src}">
      ${t("lightbox.no_preview")}
    </video>`;
  } else {
    mediaHtml = `<div class="lightbox-no-preview">
      <div style="font-size:64px;margin-bottom:16px">&#128196;</div>
      ${t("lightbox.no_preview")}
    </div>`;
  }

  const counterText = t("lightbox.counter", { current: _index + 1, total: _paths.length });

  contentEl.innerHTML = `
    <button class="lightbox-close" aria-label="${t("lightbox.close")}" id="lightbox-btn-close">&times;</button>
    <div class="lightbox-counter" id="lightbox-counter">${escapeHtml(counterText)}</div>
    ${_paths.length > 1 ? `
      <button class="lightbox-nav lightbox-nav-prev" aria-label="${t("general.previous")}" id="lightbox-btn-prev" ${_index === 0 ? "disabled" : ""}>&lsaquo;</button>
      <button class="lightbox-nav lightbox-nav-next" aria-label="${t("general.next")}" id="lightbox-btn-next" ${_index === _paths.length - 1 ? "disabled" : ""}>&rsaquo;</button>
    ` : ""}
    <div class="lightbox-media-wrap" id="lightbox-media-wrap">
      ${mediaHtml}
    </div>
    <div class="lightbox-info ${_infoOpen ? "open" : ""}" id="lightbox-info">
      <div class="lightbox-info-header">
        <h3>${t("lightbox.info")}</h3>
        <button class="lightbox-info-close" id="lightbox-info-close" aria-label="${t("lightbox.close")}">&times;</button>
      </div>
      <div class="lightbox-info-body" id="lightbox-info-body">
        <div class="loading-inline"><div class="spinner-small"></div></div>
      </div>
    </div>
    <button class="lightbox-info-toggle" id="lightbox-info-toggle" aria-label="${t("lightbox.info")}" title="${t("lightbox.info")}">&#9432;</button>
    <div class="lightbox-toolbar">
      <button class="lightbox-toolbar-btn" id="lightbox-rotate-left" aria-label="${t("lightbox.rotate_left")}" title="${t("lightbox.rotate_left")}">&#x21BA;</button>
      <button class="lightbox-toolbar-btn" id="lightbox-rotate-right" aria-label="${t("lightbox.rotate_right")}" title="${t("lightbox.rotate_right")}">&#x21BB;</button>
      <span class="lightbox-toolbar-sep"></span>
      <button class="lightbox-fav-btn ${_favoritesSet.has(path) ? "is-favorite" : ""}" id="lightbox-fav-btn" aria-label="${_favoritesSet.has(path) ? t("files.unfavorite") : t("files.favorite")}" title="${_favoritesSet.has(path) ? t("files.unfavorite") : t("files.favorite")}">
        ${_favoritesSet.has(path) ? "\u2605" : "\u2606"} ${_favoritesSet.has(path) ? t("files.unfavorite") : t("files.favorite")}
      </button>
      <button class="lightbox-share-btn" id="lightbox-share-btn" aria-label="${t("share.title")}" title="${t("share.title")}">
        &#128279; ${t("share.title")}
      </button>
      ${navigator.share ? `<button class="lightbox-native-share-btn" id="lightbox-native-share-btn" aria-label="${t("share.native")}" title="${t("share.native")}">
        &#x1F4E4; ${t("share.native")}
      </button>` : ""}
    </div>
  `;

  // Bind internal events
  contentEl.querySelector("#lightbox-btn-close").addEventListener("click", closeLightbox);

  const prevBtn = contentEl.querySelector("#lightbox-btn-prev");
  const nextBtn = contentEl.querySelector("#lightbox-btn-next");
  if (prevBtn) prevBtn.addEventListener("click", goPrev);
  if (nextBtn) nextBtn.addEventListener("click", goNext);

  contentEl.querySelector("#lightbox-info-toggle").addEventListener("click", toggleInfo);
  contentEl.querySelector("#lightbox-info-close").addEventListener("click", toggleInfo);

  // Favorite button
  const favBtn = contentEl.querySelector("#lightbox-fav-btn");
  if (favBtn) {
    favBtn.addEventListener("click", async () => {
      const currentPath = _paths[_index];
      try {
        const result = await apiPost("/files/favorite", { path: currentPath });
        if (result.is_favorite) {
          _favoritesSet.add(currentPath);
          showToast(t("files.favorited"), "success");
        } else {
          _favoritesSet.delete(currentPath);
          showToast(t("files.unfavorited"), "info");
        }
        _updateFavButton();
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  }

  // Rotate buttons
  const rotLeftBtn = contentEl.querySelector("#lightbox-rotate-left");
  const rotRightBtn = contentEl.querySelector("#lightbox-rotate-right");
  if (rotLeftBtn) {
    rotLeftBtn.addEventListener("click", () => {
      _rotation = (_rotation - 90) % 360;
      const mw = document.getElementById("lightbox-media-wrap");
      if (mw) applyTransform(mw);
    });
  }
  if (rotRightBtn) {
    rotRightBtn.addEventListener("click", () => {
      _rotation = (_rotation + 90) % 360;
      const mw = document.getElementById("lightbox-media-wrap");
      if (mw) applyTransform(mw);
    });
  }

  // Share button (link-based)
  const shareBtn = contentEl.querySelector("#lightbox-share-btn");
  if (shareBtn) {
    shareBtn.addEventListener("click", () => {
      openShareModal(_paths[_index]);
    });
  }

  // Native share (Web Share API — mail, messengers, social networks)
  const nativeShareBtn = contentEl.querySelector("#lightbox-native-share-btn");
  if (nativeShareBtn) {
    nativeShareBtn.addEventListener("click", async () => {
      const currentPath = _paths[_index];
      const name = fileName(currentPath);
      try {
        // Fetch the file as blob for native sharing
        const response = await fetch(`/api/thumbnail${encodeURI(currentPath)}?size=800`);
        const blob = await response.blob();
        const file = new File([blob], name, { type: blob.type });
        await navigator.share({
          title: name,
          files: [file],
        });
        showToast(t("share.shared"), "success");
      } catch (e) {
        if (e.name !== "AbortError") {
          // Fallback: share without file (just text)
          try {
            await navigator.share({
              title: name,
              text: `${name} — GOD MODE Media Library`,
            });
          } catch {
            showToast(t("share.share_failed"), "error");
          }
        }
      }
    });
  }

  // Double-click to zoom
  const mediaWrap = contentEl.querySelector("#lightbox-media-wrap");
  if (mediaWrap) {
    mediaWrap.addEventListener("dblclick", (e) => {
      if (_zoom === 1) {
        _zoom = 2.5;
        updateZoomOrigin(e, mediaWrap);
      } else {
        _zoom = 1;
      }
      applyZoom(mediaWrap);
    });
  }

  // If info panel was open, load info
  if (_infoOpen) {
    loadFileInfo(path);
  }
}

function applyZoom(mediaWrap) {
  applyTransform(mediaWrap);
}

function applyTransform(mediaWrap) {
  const media = mediaWrap?.querySelector("#lightbox-media");
  if (!media) return;
  const parts = [];
  if (_zoom !== 1) parts.push(`scale(${_zoom})`);
  if (_rotation !== 0) parts.push(`rotate(${_rotation}deg)`);
  media.style.transform = parts.length ? parts.join(" ") : "";
  media.style.transformOrigin = _zoom !== 1 ? `${_zoomOriginX}% ${_zoomOriginY}%` : "";
  media.style.cursor = _zoom > 1 ? "zoom-out" : "zoom-in";
}

function updateZoomOrigin(e, mediaWrap) {
  const rect = mediaWrap.getBoundingClientRect();
  _zoomOriginX = ((e.clientX - rect.left) / rect.width) * 100;
  _zoomOriginY = ((e.clientY - rect.top) / rect.height) * 100;
}

// ── Info panel ───────────────────────────────────────

function toggleInfo() {
  _infoOpen = !_infoOpen;
  const infoEl = document.getElementById("lightbox-info");
  if (infoEl) {
    infoEl.classList.toggle("open", _infoOpen);
  }
  if (_infoOpen) {
    loadFileInfo(_paths[_index]);
  }
}

async function loadFileInfo(filePath) {
  const bodyEl = document.getElementById("lightbox-info-body");
  if (!bodyEl) return;

  // Check cache
  if (_fileDetailsCache[filePath]) {
    renderFileInfo(bodyEl, _fileDetailsCache[filePath]);
    return;
  }

  bodyEl.innerHTML = `<div class="loading-inline"><div class="spinner-small"></div></div>`;

  try {
    const data = await api(`/files${filePath}`);
    _fileDetailsCache[filePath] = data;
    renderFileInfo(bodyEl, data);
  } catch (e) {
    bodyEl.innerHTML = `<div class="lightbox-info-error">${t("general.error", { message: e.message })}</div>`;
  }
}

function renderFileInfo(bodyEl, data) {
  const f = data.file;
  const meta = data.metadata || {};
  const richness = data.richness;

  let html = "";

  // Path and favorite
  const isFav = _favoritesSet.has(f.path);
  html += `<div class="lightbox-info-section">
    <div class="lightbox-info-path">${escapeHtml(f.path)}</div>
    <div style="margin-top:6px">${isFav ? '\u2605 ' + t("files.favorites") : ""}</div>
  </div>`;

  // Rating
  const currentRating = data.rating || 0;
  html += `<div class="lightbox-info-section">
    <div class="lightbox-info-section-title">${t("rating.title")}</div>
    <div class="lightbox-star-rating" id="lightbox-star-rating" data-path="${escapeHtml(f.path)}">`;
  for (let i = 1; i <= 5; i++) {
    const filled = i <= currentRating;
    html += `<span class="lightbox-star ${filled ? "filled" : ""}" data-star="${i}" title="${i}">${filled ? "\u2605" : "\u2606"}</span>`;
  }
  if (currentRating > 0) {
    html += ` <button class="lightbox-rating-clear" id="lightbox-rating-clear" title="${t("rating.clear")}">&times;</button>`;
  }
  html += `</div></div>`;

  // Notes
  const currentNote = data.note || "";
  html += `<div class="lightbox-info-section">
    <div class="lightbox-info-section-title">${t("notes.title")}</div>
    <textarea class="lightbox-note-textarea" id="lightbox-note-textarea" data-path="${escapeHtml(f.path)}" placeholder="${t("notes.placeholder")}" rows="3">${escapeHtml(currentNote)}</textarea>
  </div>`;

  // Tags
  const fileTags = data.tags || [];
  html += `<div class="lightbox-info-section">
    <div class="lightbox-info-section-title">${t("tags.title")}</div>
    <div class="lightbox-tags-container" id="lightbox-tags-container">${renderFileTagsWithRemove(fileTags, f.path)}</div>
    <button class="lightbox-add-tag-btn" id="lightbox-add-tag-btn" style="margin-top:6px;font-size:12px">+ ${t("tags.add_to_file")}</button>
  </div>`;

  // Basic info
  html += `<div class="lightbox-info-section">`;
  html += infoRow(t("detail.size"), formatBytes(f.size));
  if (f.ext) html += infoRow(t("detail.extension"), f.ext);
  if (f.width && f.height) html += infoRow(t("detail.resolution"), `${f.width} x ${f.height}`);
  if (f.date_original) html += infoRow(t("detail.date"), f.date_original);
  if (f.duration_seconds) html += infoRow(t("detail.duration"), `${f.duration_seconds.toFixed(1)}s`);
  html += `</div>`;

  // Camera
  const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
  if (cam) {
    html += `<div class="lightbox-info-section">
      <div class="lightbox-info-section-title">${t("detail.camera")}</div>
      ${infoRow(t("detail.camera"), cam)}
    </div>`;
  }

  // GPS
  if (f.gps_latitude && f.gps_longitude) {
    const lat = f.gps_latitude.toFixed(6);
    const lng = f.gps_longitude.toFixed(6);
    html += `<div class="lightbox-info-section">
      <div class="lightbox-info-section-title">GPS</div>
      <a class="lightbox-gps-link" href="https://maps.google.com/?q=${lat},${lng}" target="_blank" rel="noopener noreferrer">
        ${lat}, ${lng} &#x2197;
      </a>
    </div>`;
  }

  // Richness / quality score
  if (richness != null) {
    const level = richness >= 30 ? "high" : richness >= 15 ? "medium" : "low";
    html += `<div class="lightbox-info-section">
      <div class="lightbox-info-section-title">${t("detail.quality_score")}</div>
      <span class="richness-badge ${level}">${Number(richness).toFixed(1)} pts</span>
    </div>`;
  }

  // Hashes
  if (f.sha256 || f.phash) {
    html += `<div class="lightbox-info-section">`;
    if (f.sha256) html += infoRow(t("detail.sha256"), f.sha256.slice(0, 16) + "\u2026");
    if (f.phash) html += infoRow(t("detail.phash"), f.phash.slice(0, 16) + "\u2026");
    html += `</div>`;
  }

  // EXIF / metadata tags (collapsible)
  const metaKeys = Object.keys(meta);
  if (metaKeys.length) {
    html += `<div class="lightbox-info-section">
      <details class="lightbox-exif-details">
        <summary>${t("detail.metadata_tags", { count: metaKeys.length })}</summary>
        <table class="lightbox-meta-table">`;
    for (const key of metaKeys.sort()) {
      const val = typeof meta[key] === "object" ? JSON.stringify(meta[key]) : String(meta[key]);
      html += `<tr><td>${escapeHtml(key)}</td><td>${escapeHtml(val)}</td></tr>`;
    }
    html += `</table></details></div>`;
  }

  // Download link
  html += `<div class="lightbox-info-section">
    <a class="lightbox-download-link" href="/api/thumbnail${encodeURI(f.path)}?size=800" download="${escapeHtml(fileName(f.path))}">
      &#11015; ${t("lightbox.download")}
    </a>
  </div>`;

  bodyEl.innerHTML = html;

  // Bind tag remove buttons
  bodyEl.querySelectorAll(".tag-pill-remove").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const tagId = parseInt(btn.dataset.tagId, 10);
      const filePath = btn.dataset.filePath;
      try {
        await apiDelete("/files/tag", { paths: [filePath], tag_id: tagId });
        showToast(t("tags.untagged"), "info");
        // Refresh info
        delete _fileDetailsCache[filePath];
        loadFileInfo(filePath);
      } catch (err) {
        showToast(t("general.error", { message: err.message }), "error");
      }
    });
  });

  // Bind add tag button
  const addTagBtn = bodyEl.querySelector("#lightbox-add-tag-btn");
  if (addTagBtn) {
    addTagBtn.addEventListener("click", () => {
      const currentPath = _paths[_index];
      openTagPicker(addTagBtn, [currentPath], () => {
        delete _fileDetailsCache[currentPath];
        loadFileInfo(currentPath);
      });
    });
  }

  // Bind star rating clicks
  const starContainer = bodyEl.querySelector("#lightbox-star-rating");
  if (starContainer) {
    starContainer.querySelectorAll(".lightbox-star").forEach(star => {
      star.addEventListener("click", async () => {
        const starVal = parseInt(star.dataset.star, 10);
        const filePath = starContainer.dataset.path;
        const currentR = data.rating || 0;
        if (starVal === currentR) {
          // Click same star = clear
          try {
            await apiDelete(`/files${filePath}/rating`);
            showToast(t("rating.clear"), "info");
            delete _fileDetailsCache[filePath];
            loadFileInfo(filePath);
          } catch (err) {
            showToast(t("general.error", { message: err.message }), "error");
          }
        } else {
          try {
            await apiPut(`/files${filePath}/rating`, { rating: starVal });
            showToast(t("rating.set"), "success");
            delete _fileDetailsCache[filePath];
            loadFileInfo(filePath);
          } catch (err) {
            showToast(t("general.error", { message: err.message }), "error");
          }
        }
      });
    });
    const clearBtn = starContainer.querySelector("#lightbox-rating-clear");
    if (clearBtn) {
      clearBtn.addEventListener("click", async () => {
        const filePath = starContainer.dataset.path;
        try {
          await apiDelete(`/files${filePath}/rating`);
          showToast(t("rating.clear"), "info");
          delete _fileDetailsCache[filePath];
          loadFileInfo(filePath);
        } catch (err) {
          showToast(t("general.error", { message: err.message }), "error");
        }
      });
    }
  }

  // Bind notes textarea (auto-save on blur with debounce)
  const noteArea = bodyEl.querySelector("#lightbox-note-textarea");
  if (noteArea) {
    let _noteTimer = null;
    const saveNote = async () => {
      const filePath = noteArea.dataset.path;
      const noteText = noteArea.value.trim();
      try {
        if (noteText) {
          await apiPut(`/files${filePath}/note`, { note: noteText });
          showToast(t("notes.saved"), "success");
        } else {
          await apiDelete(`/files${filePath}/note`);
          showToast(t("notes.deleted"), "info");
        }
        delete _fileDetailsCache[filePath];
      } catch (err) {
        showToast(t("general.error", { message: err.message }), "error");
      }
    };
    noteArea.addEventListener("blur", () => {
      if (_noteTimer) clearTimeout(_noteTimer);
      _noteTimer = setTimeout(saveNote, 500);
    });
    noteArea.addEventListener("keydown", (e) => {
      // Prevent lightbox keyboard shortcuts while typing in textarea
      e.stopPropagation();
    });
  }
}

function infoRow(label, value) {
  return `<div class="lightbox-info-row">
    <span class="lightbox-info-label">${escapeHtml(label)}</span>
    <span class="lightbox-info-value">${escapeHtml(value)}</span>
  </div>`;
}

function _updateFavButton() {
  const favBtn = document.getElementById("lightbox-fav-btn");
  if (!favBtn) return;
  const path = _paths[_index];
  const isFav = _favoritesSet.has(path);
  favBtn.classList.toggle("is-favorite", isFav);
  favBtn.title = isFav ? t("files.unfavorite") : t("files.favorite");
  favBtn.setAttribute("aria-label", isFav ? t("files.unfavorite") : t("files.favorite"));
  favBtn.innerHTML = `${isFav ? "\u2605" : "\u2606"} ${isFav ? t("files.unfavorite") : t("files.favorite")}`;
}

// ── Preloading ───────────────────────────────────────

function preloadAdjacent() {
  for (const offset of [-1, 1]) {
    const idx = _index + offset;
    if (idx < 0 || idx >= _paths.length) continue;
    const path = _paths[idx];
    if (_preloaded[path]) continue;
    const ext = (path.split(".").pop() || "").toLowerCase();
    if (IMAGE_EXTS.has(ext)) {
      const img = new Image();
      img.src = `/api/thumbnail${encodeURI(path)}?size=800`;
      _preloaded[path] = img;
    }
  }
}

// ── Rating keyboard shortcut ─────────────────────────

async function _setRatingFromKey(rating) {
  const path = _paths[_index];
  if (!path) return;
  try {
    const cached = _fileDetailsCache[path];
    const currentR = cached?.rating || 0;
    if (rating === currentR) {
      await apiDelete(`/files${path}/rating`);
      showToast(t("rating.clear"), "info");
    } else {
      await apiPut(`/files${path}/rating`, { rating });
      showToast(t("rating.set"), "success");
    }
    delete _fileDetailsCache[path];
    if (_infoOpen) loadFileInfo(path);
  } catch (err) {
    showToast(t("general.error", { message: err.message }), "error");
  }
}

// ── Event binding ────────────────────────────────────

function bindEvents() {
  _keyHandler = (e) => {
    // Don't capture keys when lightbox is closed
    if (!_overlay || _overlay.classList.contains("hidden")) return;

    switch (e.key) {
      case "Escape":
        e.preventDefault();
        e.stopPropagation();
        closeLightbox();
        break;
      case "ArrowLeft":
        e.preventDefault();
        goPrev();
        break;
      case "ArrowRight":
        e.preventDefault();
        goNext();
        break;
      case "i":
      case "I":
        if (!e.target.matches("input, textarea, select")) {
          e.preventDefault();
          toggleInfo();
        }
        break;
      case "f":
      case "F":
        if (!e.target.matches("input, textarea, select")) {
          e.preventDefault();
          const fb = document.getElementById("lightbox-fav-btn");
          if (fb) fb.click();
        }
        break;
      case "r":
        if (!e.target.matches("input, textarea, select")) {
          e.preventDefault();
          _rotation = (_rotation + 90) % 360;
          const mwr = document.getElementById("lightbox-media-wrap");
          if (mwr) applyTransform(mwr);
        }
        break;
      case "R":
        if (!e.target.matches("input, textarea, select")) {
          e.preventDefault();
          _rotation = (_rotation - 90) % 360;
          const mwl = document.getElementById("lightbox-media-wrap");
          if (mwl) applyTransform(mwl);
        }
        break;
      case " ":
        if (!e.target.matches("input, textarea, select")) {
          e.preventDefault();
          // Toggle play/pause for video, or toggle fullscreen for images
          const video = document.querySelector("#lightbox-media.lightbox-video");
          if (video) {
            if (video.paused) video.play();
            else video.pause();
          } else {
            // Toggle fullscreen for image viewing
            if (document.fullscreenElement) {
              document.exitFullscreen().catch(() => {});
            } else if (_overlay) {
              _overlay.requestFullscreen().catch(() => {});
            }
          }
        }
        break;
      case "1": case "2": case "3": case "4": case "5":
        if (!e.target.matches("input, textarea, select")) {
          e.preventDefault();
          e.stopPropagation();
          _setRatingFromKey(parseInt(e.key, 10));
        }
        break;
    }
  };

  _wheelHandler = (e) => {
    if (!_overlay || _overlay.classList.contains("hidden")) return;
    const mediaWrap = document.getElementById("lightbox-media-wrap");
    if (!mediaWrap || !mediaWrap.contains(e.target)) return;

    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.25 : 0.25;
    _zoom = Math.max(0.5, Math.min(5, _zoom + delta));

    if (Math.abs(_zoom - 1) < 0.1) _zoom = 1;
    updateZoomOrigin(e, mediaWrap);
    applyZoom(mediaWrap);
  };

  // Use capture phase so we intercept Escape before main.js
  document.addEventListener("keydown", _keyHandler, true);
  document.addEventListener("wheel", _wheelHandler, { passive: false });

  // Click outside media to close
  _overlay.addEventListener("click", onOverlayClick);
}

function onOverlayClick(e) {
  // Close only if clicking the overlay background or media wrap (not nav/info/buttons)
  if (e.target === _overlay ||
      e.target.id === "lightbox-media-wrap" ||
      e.target.classList.contains("lightbox-content")) {
    closeLightbox();
  }
}

function unbindEvents() {
  if (_keyHandler) {
    document.removeEventListener("keydown", _keyHandler, true);
    _keyHandler = null;
  }
  if (_wheelHandler) {
    document.removeEventListener("wheel", _wheelHandler, { passive: false });
    _wheelHandler = null;
  }
  if (_overlay) {
    _overlay.removeEventListener("click", onOverlayClick);
  }
}
