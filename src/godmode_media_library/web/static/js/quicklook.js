/* GOD MODE Media Library — Quick Look (spacebar preview) */

import { api } from "./api.js";
import { $, escapeHtml, formatBytes, IMAGE_EXTS } from "./utils.js";
import { t } from "./i18n.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

let _currentPath = null;
let _isOpen = false;
let _allPaths = [];
let _currentIndex = -1;
let _keyHandler = null;

// ── Public API ───────────────────────────────────────

export function openQuickLook(path, allPaths = []) {
  _currentPath = path;
  _allPaths = allPaths;
  _currentIndex = allPaths.indexOf(path);
  _isOpen = true;

  const overlay = $("#quicklook-overlay");
  if (!overlay) return;

  overlay.classList.remove("hidden");
  overlay.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";

  _renderQuickLook(path);
  _bindEvents();
}

export function closeQuickLook() {
  _isOpen = false;
  _currentPath = null;

  const overlay = $("#quicklook-overlay");
  if (!overlay) return;

  overlay.classList.add("hidden");
  overlay.setAttribute("aria-hidden", "true");
  document.body.style.overflow = "";

  // Stop any playing media
  const audio = overlay.querySelector("audio");
  if (audio) audio.pause();
  const video = overlay.querySelector("video");
  if (video) video.pause();

  // Clean up container
  const container = $("#quicklook-container");
  if (container) container.innerHTML = "";

  _unbindEvents();
}

export function isQuickLookOpen() {
  return _isOpen;
}

// ── Navigation ───────────────────────────────────────

function _goNext() {
  if (_currentIndex < _allPaths.length - 1) {
    _currentIndex++;
    _currentPath = _allPaths[_currentIndex];
    _renderQuickLook(_currentPath);
  }
}

function _goPrev() {
  if (_currentIndex > 0) {
    _currentIndex--;
    _currentPath = _allPaths[_currentIndex];
    _renderQuickLook(_currentPath);
  }
}

// ── Rendering ────────────────────────────────────────

async function _renderQuickLook(path) {
  const container = $("#quicklook-container");
  if (!container) return;

  const ext = (path.split(".").pop() || "").toLowerCase();
  const isImage = IMAGE_EXTS.has(ext);
  const isVideo = VIDEO_EXTS.has(ext);
  const name = path.split("/").pop();

  // Show loading state
  container.innerHTML = `
    <div class="quicklook-preview">
      <div class="loading"><div class="spinner"></div></div>
    </div>
    <div class="quicklook-footer">
      <span>${escapeHtml(name)}</span>
    </div>`;

  if (isImage) {
    _renderImagePreview(container, path, name);
    return;
  }
  if (isVideo) {
    _renderVideoPreview(container, path, name);
    return;
  }

  // For other types, fetch preview data from API
  try {
    const encodedPath = encodeURI(path);
    const data = await api(`/preview${encodedPath}`);
    _renderPreviewByType(container, data, path);
  } catch {
    _renderUnknownPreview(container, path, name);
  }
}

function _renderImagePreview(container, path, name) {
  const src = `/api/thumbnail${encodeURI(path)}?size=800`;
  container.innerHTML = `
    <div class="quicklook-preview">
      <img src="${src}" alt="${escapeHtml(name)}" class="ql-image"
           onerror="this.outerHTML='<div class=\\'ql-unknown\\'><div class=\\'ql-unknown-icon\\'>&#128196;</div><div>${t("quicklook.no_preview")}</div></div>'">
    </div>
    <div class="quicklook-footer">
      <span class="ql-footer-name">${escapeHtml(name)}</span>
      <span class="ql-footer-nav">${_navInfo()}</span>
    </div>`;
}

function _renderVideoPreview(container, path, name) {
  const src = `/api/stream${encodeURI(path)}`;
  container.innerHTML = `
    <div class="quicklook-preview">
      <video controls autoplay class="ql-video" src="${src}">
        ${t("quicklook.no_preview")}
      </video>
    </div>
    <div class="quicklook-footer">
      <span class="ql-footer-name">${escapeHtml(name)}</span>
      <span class="ql-footer-nav">${_navInfo()}</span>
    </div>`;
}

function _renderPreviewByType(container, data, path) {
  const name = data.name || path.split("/").pop();
  let previewHtml = "";

  switch (data.type) {
    case "text":
      previewHtml = _buildTextPreview(data);
      break;
    case "pdf":
      previewHtml = _buildPdfPreview(data);
      break;
    case "archive":
      previewHtml = _buildArchivePreview(data);
      break;
    case "audio":
      previewHtml = _buildAudioPreview(data);
      break;
    default:
      previewHtml = _buildUnknownPreview(name);
      break;
  }

  const sizeStr = data.size ? formatBytes(data.size) : "";
  const typeLabel = _typeLabel(data.type);

  container.innerHTML = `
    <div class="quicklook-preview">${previewHtml}</div>
    <div class="quicklook-footer">
      <span class="ql-footer-name">${escapeHtml(name)}</span>
      <span class="ql-footer-meta">${escapeHtml(typeLabel)}${sizeStr ? " &middot; " + escapeHtml(sizeStr) : ""}</span>
      <span class="ql-footer-nav">${_navInfo()}</span>
    </div>`;
}

function _renderUnknownPreview(container, path, name) {
  const ext = (path.split(".").pop() || "").toUpperCase();
  container.innerHTML = `
    <div class="quicklook-preview">
      ${_buildUnknownPreview(name, ext)}
    </div>
    <div class="quicklook-footer">
      <span class="ql-footer-name">${escapeHtml(name)}</span>
      <span class="ql-footer-nav">${_navInfo()}</span>
    </div>`;
}

// ── Preview builders ─────────────────────────────────

function _buildTextPreview(data) {
  const lines = data.content.split("\n");
  let html = '<div class="ql-code">';
  for (let i = 0; i < lines.length; i++) {
    html += `<span class="line-num">${i + 1}</span>${escapeHtml(lines[i])}\n`;
  }
  html += "</div>";
  const linesInfo = t("quicklook.lines", { count: data.lines });
  html += `<div class="ql-text-info">${escapeHtml(linesInfo)}</div>`;
  return html;
}

function _buildPdfPreview(data) {
  return `<iframe src="${data.url}" class="ql-pdf" title="${t("quicklook.pdf")}"></iframe>`;
}

function _buildArchivePreview(data) {
  let html = '<div class="ql-archive"><table><thead><tr>';
  html += `<th>${t("files.name")}</th><th>${t("files.size")}</th>`;
  html += "</tr></thead><tbody>";
  for (const entry of data.entries) {
    const cls = entry.is_dir ? ' class="entry-dir"' : "";
    const icon = entry.is_dir ? "&#128193; " : "";
    html += `<tr><td${cls}>${icon}${escapeHtml(entry.name)}</td>`;
    html += `<td>${entry.is_dir ? "" : formatBytes(entry.size)}</td></tr>`;
  }
  html += "</tbody></table>";
  html += `<div class="ql-archive-info">${t("quicklook.entries", { count: data.total_entries })}</div>`;
  html += "</div>";
  return html;
}

function _buildAudioPreview(data) {
  return `<div class="ql-audio">
    <div class="ql-audio-icon">&#127925;</div>
    <div class="ql-audio-name">${escapeHtml(data.name)}</div>
    <audio controls src="${data.url}" autoplay></audio>
  </div>`;
}

function _buildUnknownPreview(name, ext) {
  const displayExt = ext || (name.split(".").pop() || "").toUpperCase();
  return `<div class="ql-unknown">
    <div class="ql-unknown-icon">&#128196;</div>
    <div class="ql-unknown-ext">${escapeHtml(displayExt)}</div>
    <div class="ql-unknown-msg">${t("quicklook.no_preview")}</div>
  </div>`;
}

// ── Helpers ──────────────────────────────────────────

function _navInfo() {
  if (_allPaths.length <= 1) return "";
  return `${_currentIndex + 1} / ${_allPaths.length}`;
}

function _typeLabel(type) {
  const labels = {
    text: t("quicklook.text"),
    pdf: t("quicklook.pdf"),
    archive: t("quicklook.archive"),
    audio: t("quicklook.audio"),
  };
  return labels[type] || "";
}

// ── Events ───────────────────────────────────────────

function _bindEvents() {
  _keyHandler = (e) => {
    if (!_isOpen) return;

    switch (e.key) {
      case "Escape":
      case " ":
        e.preventDefault();
        e.stopPropagation();
        closeQuickLook();
        break;
      case "ArrowLeft":
        e.preventDefault();
        _goPrev();
        break;
      case "ArrowRight":
        e.preventDefault();
        _goNext();
        break;
    }
  };

  document.addEventListener("keydown", _keyHandler, true);

  // Click overlay to close
  const overlay = $("#quicklook-overlay");
  if (overlay) {
    overlay.addEventListener("click", _onOverlayClick);
  }
}

function _onOverlayClick(e) {
  if (e.target.id === "quicklook-overlay") {
    closeQuickLook();
  }
}

function _unbindEvents() {
  if (_keyHandler) {
    document.removeEventListener("keydown", _keyHandler, true);
    _keyHandler = null;
  }
  const overlay = $("#quicklook-overlay");
  if (overlay) {
    overlay.removeEventListener("click", _onOverlayClick);
  }
}
