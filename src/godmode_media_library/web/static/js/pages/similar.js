/* GOD MODE Media Library — Similar page (upgraded) */

import { api, apiPost } from "../api.js";
import { escapeHtml, fileName, formatBytes, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showVisualDiff } from "../modal.js";
import { openLightbox } from "../lightbox.js";

let _currentThreshold = 10;

function _distanceBadge(distance) {
  if (distance <= 3) {
    return `<span class="similar-badge similar-badge-identical">${t("similar.almost_identical")}</span>`;
  } else if (distance <= 6) {
    return `<span class="similar-badge similar-badge-very">${t("similar.very_similar")}</span>`;
  }
  return `<span class="similar-badge similar-badge-somewhat">${t("similar.somewhat_similar")}</span>`;
}

async function _loadPairs(container, threshold) {
  _currentThreshold = threshold;
  try {
    const data = await api(`/similar?threshold=${threshold}&limit=100`);
    _renderContent(container, data);
  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("similar.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

function _renderContent(container, data) {
  let html = `
    <div class="page-header">
      <h2>${t("similar.title")} <span class="header-count">${t("similar.total_pairs", { count: data.total_pairs })}</span></h2>
    </div>
    <div class="similar-controls" style="display:flex;align-items:center;gap:16px;margin-bottom:20px;padding:12px 16px;background:var(--surface);border:1px solid var(--border);border-radius:8px;flex-wrap:wrap">
      <label style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:600">
        ${t("similar.threshold")}
        <input type="range" min="0" max="20" value="${_currentThreshold}" id="similar-threshold-slider" style="width:140px;accent-color:var(--accent)">
        <span id="similar-threshold-value" style="min-width:24px;text-align:center;font-weight:700">${_currentThreshold}</span>
      </label>
      <span style="font-size:11px;color:var(--text-muted)">${t("similar.threshold_hint")}</span>
      ${data.pairs.length > 0 ? `<button class="btn-resolve-all" id="btn-resolve-all-pairs" style="margin-left:auto;padding:6px 14px;background:var(--red);color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600">${t("similar.resolve_all_pairs")}</button>` : ""}
    </div>`;

  if (!data.pairs.length) {
    html += `
      <div class="empty-state-hero" style="padding:40px 0">
        <div class="empty-state-icon" style="font-size:48px">&#127912;</div>
        <h3 class="empty-state-title">${t("similar.empty_title")}</h3>
        <p class="empty-state-subtitle">${t("similar.empty_hint")}</p>
      </div>`;
    container.innerHTML = html;
    _bindSlider(container);
    return;
  }

  html += '<div class="similar-grid">';
  for (const p of data.pairs) {
    const srcA = `/api/thumbnail${encodeURI(p.path_a)}?size=250`;
    const srcB = `/api/thumbnail${encodeURI(p.path_b)}?size=250`;
    const distColor = p.distance <= 3 ? "var(--red)" : p.distance <= 6 ? "var(--yellow)" : "var(--text-muted)";
    html += `<div class="similar-pair" data-pair-a="${escapeHtml(p.path_a)}" data-pair-b="${escapeHtml(p.path_b)}">
      <div class="similar-pair-header" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <div style="display:flex;align-items:center;gap:8px">
          <span class="distance" style="color:${distColor};font-weight:700;font-size:13px">${t("similar.distance", { value: p.distance })}</span>
          ${_distanceBadge(p.distance)}
        </div>
      </div>
      <div class="thumbs" style="display:flex;gap:8px;align-items:center;justify-content:center">
        <img src="${srcA}" alt="${escapeHtml(fileName(p.path_a))}" onerror="this.style.display='none'" loading="lazy" style="max-width:250px;max-height:200px;border-radius:6px;object-fit:cover">
        <span style="font-size:24px;color:var(--text-muted)">&#8596;</span>
        <img src="${srcB}" alt="${escapeHtml(fileName(p.path_b))}" onerror="this.style.display='none'" loading="lazy" style="max-width:250px;max-height:200px;border-radius:6px;object-fit:cover">
      </div>
      <div class="similar-names" style="display:flex;justify-content:space-between;margin-top:6px;font-size:12px;color:var(--text-muted)">
        <span title="${escapeHtml(p.path_a)}">${escapeHtml(fileName(p.path_a))}</span>
        <span title="${escapeHtml(p.path_b)}">${escapeHtml(fileName(p.path_b))}</span>
      </div>
      <div style="display:flex;gap:6px;margin-top:8px">
        <button class="btn-compare primary" data-pa="${escapeHtml(p.path_a)}" data-pb="${escapeHtml(p.path_b)}" style="flex:1">${t("similar.compare")}</button>
        <button class="btn-resolve-pair" data-pa="${escapeHtml(p.path_a)}" data-pb="${escapeHtml(p.path_b)}" style="flex:0 0 auto;padding:6px 12px;background:var(--yellow);color:#000;border:none;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600">${t("similar.resolve_pair")}</button>
      </div>
    </div>`;
  }
  html += "</div>";
  container.innerHTML = html;

  // Bind thumbnail clicks to open lightbox
  container.querySelectorAll(".similar-pair .thumbs img").forEach(img => {
    img.style.cursor = "pointer";
    img.addEventListener("click", (e) => {
      e.stopPropagation();
      const pair = img.closest(".similar-pair");
      if (!pair) return;
      const pathA = pair.dataset.pairA;
      const pathB = pair.dataset.pairB;
      const paths = [pathA, pathB].filter(Boolean);
      // Determine which image was clicked (first or second)
      const imgs = pair.querySelectorAll(".thumbs img");
      const clickedIndex = Array.from(imgs).indexOf(img);
      openLightbox(paths, Math.max(0, clickedIndex));
    });
  });

  // Bind compare buttons
  container.querySelectorAll(".btn-compare").forEach(btn => {
    btn.addEventListener("click", () => {
      showVisualDiff(btn.dataset.pa, btn.dataset.pb, null, null);
    });
  });

  // Bind resolve pair buttons (quarantine the second file)
  container.querySelectorAll(".btn-resolve-pair").forEach(btn => {
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      btn.textContent = "...";
      try {
        await apiPost("/files/quarantine", { paths: [btn.dataset.pb] });
        const pairEl = btn.closest(".similar-pair");
        if (pairEl) {
          pairEl.style.opacity = "0.4";
          pairEl.style.pointerEvents = "none";
        }
        showToast(t("duplicates.resolved"), "info");
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
        btn.disabled = false;
        btn.textContent = t("similar.resolve_pair");
      }
    });
  });

  // Bind resolve all button
  const resolveAllBtn = container.querySelector("#btn-resolve-all-pairs");
  if (resolveAllBtn) {
    resolveAllBtn.addEventListener("click", async () => {
      if (!confirm(t("confirm.quarantine", { count: data.pairs.length }))) return;
      resolveAllBtn.disabled = true;
      resolveAllBtn.textContent = "...";
      const pathsToQuarantine = data.pairs.map(p => p.path_b);
      try {
        await apiPost("/files/quarantine", { paths: pathsToQuarantine });
        showToast(t("duplicates.resolved"), "info");
        await _loadPairs(container, _currentThreshold);
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
        resolveAllBtn.disabled = false;
        resolveAllBtn.textContent = t("similar.resolve_all_pairs");
      }
    });
  }

  _bindSlider(container);
}

function _bindSlider(container) {
  const slider = container.querySelector("#similar-threshold-slider");
  const valueDisplay = container.querySelector("#similar-threshold-value");
  if (!slider) return;

  let debounceTimer = null;
  slider.addEventListener("input", () => {
    valueDisplay.textContent = slider.value;
  });
  slider.addEventListener("change", () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      _loadPairs(container, parseInt(slider.value, 10));
    }, 300);
  });
}

export async function render(container) {
  container.innerHTML = `<div class="page-header"><h2>${t("similar.title")}</h2></div><div class="loading"><div class="spinner"></div>${t("general.loading")}</div>`;
  await _loadPairs(container, _currentThreshold);
}
