/* GOD MODE Media Library — Smart Albums page */

import { api, apiPost, apiPut, apiDelete } from "../api.js";
import { $, escapeHtml, formatBytes, showToast } from "../utils.js";

const IMAGE_EXTS = new Set(["jpg","jpeg","png","gif","bmp","tiff","tif","webp","heic","heif","svg","raw","cr2","nef","arw","dng"]);
const VIDEO_EXTS = new Set(["mp4","mov","avi","mkv","wmv","flv","webm","m4v","3gp"]);

let _albums = [];
let _viewingAlbum = null;
let _albumFiles = [];
let _albumOffset = 0;
let _albumTotal = 0;
let _container = null;

// ── Helpers ──────────────────────────────────────────

function _thumbUrl(path, size = 300) {
  return `/api/thumbnail${encodeURI(path)}?size=${size}`;
}

function _fileExt(path) {
  return (path || "").split(".").pop().toLowerCase();
}

function _isImage(path) { return IMAGE_EXTS.has(_fileExt(path)); }
function _isVideo(path) { return VIDEO_EXTS.has(_fileExt(path)); }

// ── Filter description ──────────────────────────────

function _describeFilters(filters) {
  if (!filters || Object.keys(filters).length === 0) return "Bez filtru";
  const parts = [];
  if (filters.date_from || filters.date_to) {
    const from = filters.date_from || "...";
    const to = filters.date_to || "...";
    parts.push(`Datum: ${from} - ${to}`);
  }
  if (filters.camera_make) parts.push(`Znacka: ${filters.camera_make}`);
  if (filters.camera_model) parts.push(`Model: ${filters.camera_model}`);
  if (filters.file_type) parts.push(`Typ: ${filters.file_type}`);
  if (filters.has_gps) parts.push("S GPS");
  if (filters.has_faces) parts.push("S obliceji");
  if (filters.min_rating) parts.push(`Hodnoceni >= ${filters.min_rating}`);
  if (filters.tags && filters.tags.length) parts.push(`Stitky: ${filters.tags.join(", ")}`);
  if (filters.path_contains) parts.push(`Cesta: *${filters.path_contains}*`);
  if (filters.ext) parts.push(`Pripona: ${filters.ext}`);
  if (filters.quality_category) parts.push(`Kvalita: ${filters.quality_category}`);
  if (filters.min_size) parts.push(`Min: ${formatBytes(filters.min_size)}`);
  if (filters.max_size) parts.push(`Max: ${formatBytes(filters.max_size)}`);
  return parts.join(" | ") || "Bez filtru";
}

// ── Render: Album list ──────────────────────────────

async function _loadAlbums() {
  try {
    const data = await api("/albums");
    _albums = data.albums || [];
  } catch (err) {
    showToast("Chyba pri nacitani alb: " + err.message, "error");
    _albums = [];
  }
}

function _renderAlbumList() {
  if (!_container) return;

  const header = `
    <div class="albums-header" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
      <h2 style="margin:0;font-size:22px;font-weight:600">Smart Alba</h2>
      <button id="btn-create-album" class="btn-primary" style="display:flex;align-items:center;gap:6px;padding:8px 16px;border-radius:8px;border:none;background:var(--accent,#3b82f6);color:#fff;font-size:14px;cursor:pointer">
        + Nove album
      </button>
    </div>
  `;

  let grid = "";
  if (_albums.length === 0) {
    grid = `
      <div style="text-align:center;padding:60px 20px;color:var(--text-muted,#888)">
        <div style="font-size:48px;margin-bottom:16px">&#128218;</div>
        <h3 style="margin:0 0 8px;font-weight:500">Zadna alba</h3>
        <p style="margin:0;font-size:14px">Vytvorte sve prvni smart album s dynamickymi filtry.</p>
      </div>
    `;
  } else {
    grid = `<div class="albums-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px">`;
    for (const album of _albums) {
      const coverStyle = album.cover_path
        ? `background-image:url('${_thumbUrl(album.cover_path)}');background-size:cover;background-position:center`
        : `background:linear-gradient(135deg,var(--bg-secondary,#1e293b),var(--bg-tertiary,#334155));display:flex;align-items:center;justify-content:center;font-size:48px`;
      const coverContent = album.cover_path ? "" : (escapeHtml(album.icon) || "&#128218;");
      const desc = _describeFilters(album.filters);

      grid += `
        <div class="album-card" data-album-id="${album.id}" style="border-radius:12px;overflow:hidden;background:var(--bg-card,#1e293b);cursor:pointer;transition:transform 0.15s,box-shadow 0.15s;border:1px solid var(--border,#334155)">
          <div class="album-cover" style="height:180px;${coverStyle}">${coverContent}</div>
          <div style="padding:12px 14px">
            <div style="display:flex;justify-content:space-between;align-items:center">
              <h3 style="margin:0;font-size:15px;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(album.icon)} ${escapeHtml(album.name)}</h3>
              <span style="font-size:12px;color:var(--text-muted,#888);white-space:nowrap;margin-left:8px">${album.file_count} souboru</span>
            </div>
            <p style="margin:6px 0 0;font-size:12px;color:var(--text-muted,#888);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(desc)}</p>
          </div>
        </div>
      `;
    }
    grid += `</div>`;
  }

  _container.innerHTML = header + grid;

  // Bind create button
  const createBtn = _container.querySelector("#btn-create-album");
  if (createBtn) createBtn.addEventListener("click", () => _showAlbumForm());

  // Bind album cards
  _container.querySelectorAll(".album-card").forEach(card => {
    card.addEventListener("click", () => {
      const id = parseInt(card.dataset.albumId, 10);
      _openAlbum(id);
    });
    card.addEventListener("mouseenter", () => { card.style.transform = "translateY(-2px)"; card.style.boxShadow = "0 8px 24px rgba(0,0,0,0.25)"; });
    card.addEventListener("mouseleave", () => { card.style.transform = ""; card.style.boxShadow = ""; });
  });
}

// ── Album form (create / edit) ──────────────────────

function _showAlbumForm(existingAlbum = null) {
  const isEdit = !!existingAlbum;
  const f = existingAlbum?.filters || {};
  const name = existingAlbum?.name || "";
  const icon = existingAlbum?.icon || "";

  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.style.cssText = "position:fixed;inset:0;z-index:1000;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;padding:20px";

  overlay.innerHTML = `
    <div class="album-form-modal" style="background:var(--bg-card,#1e293b);border-radius:14px;width:100%;max-width:520px;max-height:90vh;overflow-y:auto;padding:24px;border:1px solid var(--border,#334155)">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
        <h3 style="margin:0;font-size:18px">${isEdit ? "Upravit album" : "Nove smart album"}</h3>
        <button class="album-form-close" style="background:none;border:none;font-size:22px;cursor:pointer;color:var(--text-muted,#888)">&times;</button>
      </div>
      <div style="display:flex;flex-direction:column;gap:14px">
        <div>
          <label style="font-size:13px;font-weight:500;margin-bottom:4px;display:block">Nazev</label>
          <input id="af-name" type="text" value="${escapeHtml(name)}" placeholder="Letni dovolena 2025" style="width:100%;padding:8px 12px;border-radius:8px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:14px;box-sizing:border-box">
        </div>
        <div>
          <label style="font-size:13px;font-weight:500;margin-bottom:4px;display:block">Ikona (emoji)</label>
          <input id="af-icon" type="text" value="${escapeHtml(icon)}" placeholder="&#127796;" maxlength="4" style="width:60px;padding:8px 12px;border-radius:8px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:18px;text-align:center">
        </div>
        <hr style="border:none;border-top:1px solid var(--border,#334155);margin:4px 0">
        <h4 style="margin:0;font-size:14px;font-weight:500;color:var(--text-muted,#888)">Filtry</h4>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Datum od</label>
            <input id="af-date-from" type="date" value="${f.date_from || ""}" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
          </div>
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Datum do</label>
            <input id="af-date-to" type="date" value="${f.date_to || ""}" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Znacka fotoaparatu</label>
            <input id="af-camera-make" type="text" value="${escapeHtml(f.camera_make || "")}" placeholder="Apple, Canon..." style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
          </div>
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Model</label>
            <input id="af-camera-model" type="text" value="${escapeHtml(f.camera_model || "")}" placeholder="iPhone 15 Pro..." style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Typ souboru</label>
            <select id="af-file-type" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px">
              <option value="">Vse</option>
              <option value="image" ${f.file_type === "image" ? "selected" : ""}>Obrazky</option>
              <option value="video" ${f.file_type === "video" ? "selected" : ""}>Videa</option>
            </select>
          </div>
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Min. hodnoceni</label>
            <select id="af-min-rating" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px">
              <option value="">Vse</option>
              <option value="1" ${f.min_rating == 1 ? "selected" : ""}>1+</option>
              <option value="2" ${f.min_rating == 2 ? "selected" : ""}>2+</option>
              <option value="3" ${f.min_rating == 3 ? "selected" : ""}>3+</option>
              <option value="4" ${f.min_rating == 4 ? "selected" : ""}>4+</option>
              <option value="5" ${f.min_rating == 5 ? "selected" : ""}>5</option>
            </select>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          <label style="font-size:12px;display:flex;align-items:center;gap:6px;cursor:pointer">
            <input id="af-has-gps" type="checkbox" ${f.has_gps ? "checked" : ""}> S GPS souradnicemi
          </label>
          <label style="font-size:12px;display:flex;align-items:center;gap:6px;cursor:pointer">
            <input id="af-has-faces" type="checkbox" ${f.has_faces ? "checked" : ""}> S obliceji
          </label>
        </div>
        <div>
          <label style="font-size:12px;display:block;margin-bottom:2px">Stitky (carkou)</label>
          <input id="af-tags" type="text" value="${escapeHtml((f.tags || []).join(", "))}" placeholder="dovolena, rodina" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
        </div>
        <div>
          <label style="font-size:12px;display:block;margin-bottom:2px">Cesta obsahuje</label>
          <input id="af-path" type="text" value="${escapeHtml(f.path_contains || "")}" placeholder="/Photos/2025/" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Pripona</label>
            <input id="af-ext" type="text" value="${escapeHtml(f.ext || "")}" placeholder="jpg, png" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px;box-sizing:border-box">
          </div>
          <div>
            <label style="font-size:12px;display:block;margin-bottom:2px">Kvalita</label>
            <select id="af-quality" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--border,#334155);background:var(--bg-secondary,#0f172a);color:inherit;font-size:13px">
              <option value="">Vse</option>
              <option value="excellent" ${f.quality_category === "excellent" ? "selected" : ""}>Excellent</option>
              <option value="good" ${f.quality_category === "good" ? "selected" : ""}>Good</option>
              <option value="average" ${f.quality_category === "average" ? "selected" : ""}>Average</option>
              <option value="poor" ${f.quality_category === "poor" ? "selected" : ""}>Poor</option>
            </select>
          </div>
        </div>
        <div style="display:flex;justify-content:flex-end;gap:10px;margin-top:8px">
          ${isEdit ? `<button class="album-form-delete" style="margin-right:auto;padding:8px 14px;border-radius:8px;border:1px solid #ef4444;background:none;color:#ef4444;font-size:13px;cursor:pointer">Smazat album</button>` : ""}
          <button class="album-form-cancel" style="padding:8px 14px;border-radius:8px;border:1px solid var(--border,#334155);background:none;color:inherit;font-size:13px;cursor:pointer">Zrusit</button>
          <button class="album-form-save" style="padding:8px 18px;border-radius:8px;border:none;background:var(--accent,#3b82f6);color:#fff;font-size:13px;cursor:pointer;font-weight:500">${isEdit ? "Ulozit" : "Vytvorit"}</button>
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);

  // Close handlers
  overlay.querySelector(".album-form-close").addEventListener("click", () => overlay.remove());
  overlay.querySelector(".album-form-cancel").addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", e => { if (e.target === overlay) overlay.remove(); });

  // Delete handler
  const deleteBtn = overlay.querySelector(".album-form-delete");
  if (deleteBtn && existingAlbum) {
    deleteBtn.addEventListener("click", async () => {
      if (!confirm("Opravdu smazat album '" + existingAlbum.name + "'?")) return;
      try {
        await apiDelete(`/albums/${existingAlbum.id}`);
        showToast("Album smazano", "success");
        overlay.remove();
        _viewingAlbum = null;
        await _loadAlbums();
        _renderAlbumList();
      } catch (err) {
        showToast("Chyba: " + err.message, "error");
      }
    });
  }

  // Save handler
  overlay.querySelector(".album-form-save").addEventListener("click", async () => {
    const albumName = overlay.querySelector("#af-name").value.trim();
    if (!albumName) { showToast("Zadejte nazev alba", "error"); return; }

    const filters = {};
    const v = (id) => (overlay.querySelector(id)?.value || "").trim();

    if (v("#af-date-from")) filters.date_from = v("#af-date-from");
    if (v("#af-date-to")) filters.date_to = v("#af-date-to");
    if (v("#af-camera-make")) filters.camera_make = v("#af-camera-make");
    if (v("#af-camera-model")) filters.camera_model = v("#af-camera-model");
    if (v("#af-file-type")) filters.file_type = v("#af-file-type");
    if (v("#af-min-rating")) filters.min_rating = parseInt(v("#af-min-rating"), 10);
    if (overlay.querySelector("#af-has-gps")?.checked) filters.has_gps = true;
    if (overlay.querySelector("#af-has-faces")?.checked) filters.has_faces = true;
    const tagsStr = v("#af-tags");
    if (tagsStr) filters.tags = tagsStr.split(",").map(t => t.trim()).filter(Boolean);
    if (v("#af-path")) filters.path_contains = v("#af-path");
    if (v("#af-ext")) filters.ext = v("#af-ext");
    if (v("#af-quality")) filters.quality_category = v("#af-quality");

    const payload = {
      name: albumName,
      icon: v("#af-icon"),
      filters,
    };

    try {
      if (isEdit) {
        await apiPut(`/albums/${existingAlbum.id}`, payload);
        showToast("Album aktualizovano", "success");
      } else {
        await apiPost("/albums", payload);
        showToast("Album vytvoreno", "success");
      }
      overlay.remove();
      _viewingAlbum = null;
      await _loadAlbums();
      _renderAlbumList();
    } catch (err) {
      showToast("Chyba: " + err.message, "error");
    }
  });

  // Focus name input
  setTimeout(() => overlay.querySelector("#af-name")?.focus(), 50);
}

// ── Album detail view ───────────────────────────────

async function _openAlbum(albumId) {
  if (!_container) return;
  _container.innerHTML = `<div class="loading" style="padding:40px;text-align:center"><div class="spinner"></div> Nacitani alba...</div>`;

  try {
    const data = await api(`/albums/${albumId}`);
    _viewingAlbum = data;
    _albumFiles = data.files || [];
    _albumTotal = data.file_count || 0;
    _albumOffset = 0;
    _renderAlbumDetail();
  } catch (err) {
    showToast("Chyba: " + err.message, "error");
    _renderAlbumList();
  }
}

function _renderAlbumDetail() {
  if (!_container || !_viewingAlbum) return;
  const a = _viewingAlbum;
  const desc = _describeFilters(a.filters);

  let html = `
    <div style="margin-bottom:16px">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
        <button id="btn-back-albums" style="background:none;border:none;font-size:18px;cursor:pointer;color:var(--text-muted,#888);padding:4px">&larr;</button>
        <h2 style="margin:0;font-size:22px;font-weight:600">${escapeHtml(a.icon)} ${escapeHtml(a.name)}</h2>
        <span style="font-size:13px;color:var(--text-muted,#888)">${_albumTotal} souboru</span>
        <button id="btn-edit-album" style="margin-left:auto;background:none;border:1px solid var(--border,#334155);border-radius:8px;padding:6px 12px;font-size:13px;cursor:pointer;color:inherit">Upravit</button>
      </div>
      <p style="margin:0;font-size:13px;color:var(--text-muted,#888)">${escapeHtml(desc)}</p>
    </div>
  `;

  if (_albumFiles.length === 0) {
    html += `<div style="text-align:center;padding:40px;color:var(--text-muted,#888)">
      <p>Zadne soubory neodpovidaji filtrum tohoto alba.</p>
    </div>`;
  } else {
    html += `<div class="album-file-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:8px">`;
    for (const file of _albumFiles) {
      const ext = _fileExt(file.path);
      const isVid = VIDEO_EXTS.has(ext);
      html += `
        <div class="album-file-thumb" style="position:relative;border-radius:8px;overflow:hidden;aspect-ratio:1;background:var(--bg-secondary,#0f172a);cursor:pointer" title="${escapeHtml(file.path)}">
          <img src="${_thumbUrl(file.path, 300)}" alt="" loading="lazy" style="width:100%;height:100%;object-fit:cover" onerror="this.style.display='none'">
          ${isVid ? `<div style="position:absolute;top:6px;right:6px;background:rgba(0,0,0,0.6);color:#fff;font-size:10px;padding:2px 6px;border-radius:4px">VIDEO</div>` : ""}
        </div>
      `;
    }
    html += `</div>`;

    if (_albumFiles.length < _albumTotal) {
      html += `<div style="text-align:center;margin-top:16px">
        <button id="btn-load-more" style="padding:8px 20px;border-radius:8px;border:1px solid var(--border,#334155);background:none;color:inherit;font-size:13px;cursor:pointer">Nacist dalsi</button>
      </div>`;
    }
  }

  _container.innerHTML = html;

  // Bind back
  _container.querySelector("#btn-back-albums")?.addEventListener("click", () => {
    _viewingAlbum = null;
    _renderAlbumList();
  });

  // Bind edit
  _container.querySelector("#btn-edit-album")?.addEventListener("click", () => {
    const albumForEdit = _albums.find(a2 => a2.id === _viewingAlbum.id) || _viewingAlbum;
    _showAlbumForm(albumForEdit);
  });

  // Bind load more
  _container.querySelector("#btn-load-more")?.addEventListener("click", async () => {
    _albumOffset += 100;
    try {
      const data = await api(`/albums/${_viewingAlbum.id}/files?limit=100&offset=${_albumOffset}`);
      _albumFiles = _albumFiles.concat(data.files || []);
      _albumTotal = data.total || _albumTotal;
      _renderAlbumDetail();
    } catch (err) {
      showToast("Chyba: " + err.message, "error");
    }
  });

  // Bind file clicks to open lightbox if available
  _container.querySelectorAll(".album-file-thumb").forEach((thumb, idx) => {
    thumb.addEventListener("click", async () => {
      try {
        const { openLightbox } = await import("../lightbox.js");
        openLightbox(_albumFiles, idx);
      } catch {
        // Lightbox not available
      }
    });
  });
}

// ── Public API ───────────────────────────────────────

export async function render(container) {
  _container = container;
  _viewingAlbum = null;
  await _loadAlbums();
  _renderAlbumList();
}

export function cleanup() {
  _container = null;
  _viewingAlbum = null;
  _albumFiles = [];
}
