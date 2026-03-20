/* GOD MODE Media Library — Modal system */

import { api } from "./api.js";
import { $, escapeHtml, fileName, formatBytes, IMAGE_EXTS } from "./utils.js";
import { t } from "./i18n.js";

// ── Generic modal ───────────────────────────────────

export function openModal(contentHtml) {
  closeAllModals();
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.setAttribute("role", "dialog");
  overlay.innerHTML = `<div class="modal"><button class="modal-close" aria-label="${t("general.close")}">&times;</button>${contentHtml}</div>`;
  overlay.querySelector(".modal-close").addEventListener("click", closeAllModals);
  overlay.addEventListener("click", e => { if (e.target === overlay) closeAllModals(); });
  document.body.appendChild(overlay);
  return overlay;
}

export function closeModal() {
  const overlay = $(".modal-overlay");
  if (overlay) overlay.remove();
}

export function closeAllModals() {
  closeModal();
  const vdOverlay = $(".visual-diff-overlay");
  if (vdOverlay) vdOverlay.remove();
}

// ── File detail modal ───────────────────────────────

export async function showFileDetail(filePath) {
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.setAttribute("role", "dialog");
  overlay.setAttribute("aria-label", t("detail.title"));
  overlay.innerHTML = `<div class="modal"><button class="modal-close" aria-label="${t("general.close")}">&times;</button><div class="loading"><div class="spinner" role="status" aria-label="${t("general.loading")}"></div>${t("general.loading")}</div></div>`;
  overlay.querySelector(".modal-close").addEventListener("click", closeAllModals);
  overlay.addEventListener("click", e => { if (e.target === overlay) closeAllModals(); });
  document.body.appendChild(overlay);

  try {
    const data = await api(`/files${filePath}`);
    const f = data.file;
    const meta = data.metadata || {};
    const richness = data.richness;
    const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());

    let thumbHtml;
    if (isImage) {
      const thumbSrc = `/api/thumbnail${encodeURI(f.path)}?size=400`;
      thumbHtml = `<img class="modal-thumb" src="${thumbSrc}" onerror="this.outerHTML='<div class=\\'modal-thumb-placeholder\\'>&#128444;</div>'" alt="${escapeHtml(fileName(f.path))}">`;
    } else {
      const icon = (f.ext || "").match(/^(mp4|mov|avi|mkv|wmv|flv|webm)$/i) ? "&#127910;" : "&#128196;";
      thumbHtml = `<div class="modal-thumb-placeholder">${icon}</div>`;
    }

    let richnessHtml = "";
    if (richness != null) {
      const level = richness >= 30 ? "high" : richness >= 15 ? "medium" : "low";
      richnessHtml = `<span class="richness-badge ${level}">${Number(richness).toFixed(1)} pts</span>`;
    }

    let gpsHtml = "";
    if (f.gps_latitude && f.gps_longitude) {
      gpsHtml = `<div class="meta-row"><span class="meta-label">GPS</span><a class="gps-link" href="https://maps.google.com/?q=${f.gps_latitude},${f.gps_longitude}" target="_blank" rel="noopener noreferrer">${f.gps_latitude.toFixed(6)}, ${f.gps_longitude.toFixed(6)} &#x2197;</a></div>`;
    }

    const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
    const res = f.width && f.height ? `${f.width} x ${f.height}` : "";
    const infoRows = [
      [t("detail.size"), formatBytes(f.size)],
      [t("detail.extension"), f.ext],
      [t("detail.date"), f.date_original || "\u2014"],
      cam ? [t("detail.camera"), cam] : null,
      res ? [t("detail.resolution"), res] : null,
      f.duration_seconds ? [t("detail.duration"), `${f.duration_seconds.toFixed(1)}s`] : null,
      f.video_codec ? [t("detail.video"), f.video_codec] : null,
      f.audio_codec ? [t("detail.audio"), f.audio_codec] : null,
      f.sha256 ? [t("detail.sha256"), f.sha256.slice(0, 16) + "\u2026"] : null,
      f.phash ? [t("detail.phash"), f.phash.slice(0, 16) + "\u2026"] : null,
    ].filter(Boolean);

    let metaHtml = "";
    const metaKeys = Object.keys(meta);
    if (metaKeys.length) {
      metaHtml = `<div class="modal-section"><h4>${t("detail.metadata_tags", { count: metaKeys.length })}</h4><table class="meta-table">`;
      for (const key of metaKeys.sort()) {
        const val = typeof meta[key] === "object" ? JSON.stringify(meta[key]) : String(meta[key]);
        metaHtml += `<tr><td>${escapeHtml(key)}</td><td>${escapeHtml(val)}</td></tr>`;
      }
      metaHtml += "</table></div>";
    }

    const modalEl = overlay.querySelector(".modal");
    modalEl.innerHTML = `
      <button class="modal-close" aria-label="${t("general.close")}">&times;</button>
      <div class="modal-header">
        ${thumbHtml}
        <div class="modal-info">
          <h3>${escapeHtml(fileName(f.path))}</h3>
          <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;word-break:break-all">${escapeHtml(f.path)}</div>
          ${richnessHtml ? `<div style="margin-bottom:12px">${richnessHtml}</div>` : ""}
          ${infoRows.map(([l, v]) => `<div class="meta-row"><span class="meta-label">${escapeHtml(l)}</span><span>${escapeHtml(v)}</span></div>`).join("")}
          ${gpsHtml}
        </div>
      </div>
      ${metaHtml}
    `;
    modalEl.querySelector(".modal-close").addEventListener("click", closeAllModals);
  } catch (e) {
    const modalEl = overlay.querySelector(".modal");
    modalEl.innerHTML = `<button class="modal-close" aria-label="${t("general.close")}">&times;</button><div class="empty">${t("detail.error", { message: e.message })}</div>`;
    modalEl.querySelector(".modal-close").addEventListener("click", closeAllModals);
  }
}

// ── Visual diff ─────────────────────────────────────

export function showVisualDiff(pathA, pathB, scoreA, scoreB) {
  const overlay = document.createElement("div");
  overlay.className = "visual-diff-overlay";
  overlay.setAttribute("role", "dialog");
  overlay.setAttribute("aria-label", t("action.visual_compare"));

  const nameA = fileName(pathA);
  const nameB = fileName(pathB);
  const thumbA = `/api/thumbnail${encodeURI(pathA)}?size=800`;
  const thumbB = `/api/thumbnail${encodeURI(pathB)}?size=800`;
  const winA = scoreA > scoreB;
  const winB = scoreB > scoreA;

  let mode = "side";

  function setMode(m) {
    mode = m;
    renderView();
  }

  function renderView() {
    let viewHtml = "";
    if (mode === "side") {
      viewHtml = `<div class="visual-diff-side">
        <div class="vd-pane"><img src="${thumbA}" alt="${escapeHtml(nameA)}"><div class="vd-label">${escapeHtml(nameA)}</div></div>
        <div class="vd-pane"><img src="${thumbB}" alt="${escapeHtml(nameB)}"><div class="vd-label">${escapeHtml(nameB)}</div></div>
      </div>`;
    } else if (mode === "slider") {
      viewHtml = `<div class="visual-diff-slider" id="vd-slider">
        <img src="${thumbB}" alt="${escapeHtml(nameB)}">
        <div class="vd-clip" id="vd-clip"><img src="${thumbA}" alt="${escapeHtml(nameA)}"></div>
        <div class="vd-divider" id="vd-divider"></div>
      </div>`;
    } else {
      viewHtml = `<div class="visual-diff-overlay-mode">
        <img src="${thumbB}" alt="${escapeHtml(nameB)}">
        <img class="vd-top" src="${thumbA}" alt="${escapeHtml(nameA)}">
      </div>`;
    }

    overlay.innerHTML = `
      <button class="visual-diff-close" aria-label="${t("general.close")}">&times;</button>
      <div class="visual-diff-controls">
        <button class="${mode === 'side' ? 'active' : ''}" data-vd-mode="side">${t("vdiff.side_by_side")}</button>
        <button class="${mode === 'slider' ? 'active' : ''}" data-vd-mode="slider">${t("vdiff.slider")}</button>
        <button class="${mode === 'overlay' ? 'active' : ''}" data-vd-mode="overlay">${t("vdiff.overlay")}</button>
      </div>
      ${viewHtml}
      <div class="visual-diff-info">
        <div class="vd-file ${winA ? 'vd-winner' : ''}">${escapeHtml(nameA)} ${scoreA != null ? `(${Number(scoreA).toFixed(1)} pts${winA ? ' \u2605' : ''})` : ''}</div>
        <div class="vd-file ${winB ? 'vd-winner' : ''}">${escapeHtml(nameB)} ${scoreB != null ? `(${Number(scoreB).toFixed(1)} pts${winB ? ' \u2605' : ''})` : ''}</div>
      </div>
    `;

    // Bind mode buttons
    overlay.querySelectorAll("[data-vd-mode]").forEach(btn => {
      btn.addEventListener("click", () => setMode(btn.dataset.vdMode));
    });

    // Bind close button
    overlay.querySelector(".visual-diff-close").addEventListener("click", () => overlay.remove());

    if (mode === "slider") {
      requestAnimationFrame(() => initSlider());
    }
  }

  overlay.addEventListener("click", e => { if (e.target === overlay) overlay.remove(); });
  overlay.addEventListener("keydown", e => { if (e.key === "Escape") overlay.remove(); });
  document.body.appendChild(overlay);
  renderView();
}

function initSlider() {
  const slider = document.getElementById("vd-slider");
  const clip = document.getElementById("vd-clip");
  const divider = document.getElementById("vd-divider");
  if (!slider || !clip || !divider) return;

  let dragging = false;
  const setPos = (x) => {
    const rect = slider.getBoundingClientRect();
    const pct = Math.max(0, Math.min(1, (x - rect.left) / rect.width));
    clip.style.width = `${pct * 100}%`;
    divider.style.left = `${pct * 100}%`;
  };
  requestAnimationFrame(() => {
    const rect = slider.getBoundingClientRect();
    setPos(rect.left + rect.width / 2);
  });

  slider.addEventListener("mousedown", (e) => { dragging = true; setPos(e.clientX); });
  document.addEventListener("mousemove", (e) => { if (dragging) setPos(e.clientX); });
  document.addEventListener("mouseup", () => { dragging = false; });
  slider.addEventListener("touchstart", (e) => { dragging = true; setPos(e.touches[0].clientX); }, { passive: true });
  document.addEventListener("touchmove", (e) => { if (dragging) setPos(e.touches[0].clientX); }, { passive: true });
  document.addEventListener("touchend", () => { dragging = false; });
}
