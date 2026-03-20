/* GOD MODE Media Library — Dashboard page (redesigned) */

import { api, apiPost } from "../api.js";
import { $, content, formatBytes, escapeHtml, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showGlobalProgress } from "../tasks.js";

export async function render(container) {
  try {
    const stats = await api("/stats");

    // Check if catalog is empty
    if (!stats.total_files || stats.total_files === 0) {
      renderEmptyState(container);
      return;
    }

    renderDashboard(container, stats);
  } catch (e) {
    // API error likely means no catalog — show empty state
    renderEmptyState(container);
  }
}

function renderEmptyState(container) {
  container.innerHTML = `
    <div class="empty-state-hero">
      <div class="empty-state-icon">&#128247;</div>
      <h2 class="empty-state-title">${t("dashboard.empty_title")}</h2>
      <p class="empty-state-subtitle">${t("dashboard.empty_hint")}</p>
      <div class="scan-card">
        <div class="scan-card-inner">
          <label class="form-label" for="scan-path-input">${t("pipeline.roots")}</label>
          <input type="text" id="scan-path-input" class="scan-input" placeholder="${t("dashboard.scan_path_placeholder")}" aria-label="${t("pipeline.roots")}">
          <button class="primary scan-btn" id="btn-empty-scan">
            &#128269; ${t("dashboard.empty_scan_btn")}
          </button>
        </div>
      </div>
    </div>`;

  const btn = container.querySelector("#btn-empty-scan");
  if (btn) {
    btn.addEventListener("click", async () => {
      const input = container.querySelector("#scan-path-input");
      const path = input?.value?.trim();
      if (!path) {
        input?.focus();
        return;
      }
      btn.disabled = true;
      btn.textContent = t("general.loading");
      try {
        const data = await apiPost("/pipeline", { roots: [path], workers: 1, extract_exiftool: true });
        showToast(t("pipeline.started"), "info");
        showGlobalProgress(data.task_id);
      } catch (e) {
        showToast(t("pipeline.start_failed", { message: e.message }), "error");
        btn.disabled = false;
        btn.textContent = `${t("dashboard.empty_scan_btn")}`;
      }
    });
  }
}

function renderDashboard(container, stats) {
  const statCards = [
    { icon: "&#128247;", label: t("dashboard.total_files"), value: stats.total_files?.toLocaleString() ?? "0", color: "accent" },
    { icon: "&#128190;", label: t("dashboard.total_size"), value: formatBytes(stats.total_size_bytes), color: "accent" },
    { icon: "&#128203;", label: t("dashboard.duplicate_groups"), value: String(stats.duplicate_groups ?? 0), color: stats.duplicate_groups > 0 ? "yellow" : "green", link: stats.duplicate_groups > 0 ? "#duplicates" : null },
    { icon: "&#127758;", label: t("dashboard.gps_files"), value: String(stats.gps_files ?? 0), color: "accent" },
    { icon: "&#128274;", label: t("dashboard.hashed"), value: stats.hashed_files?.toLocaleString() ?? "0", color: "green" },
    { icon: "&#127910;", label: t("dashboard.media_probed"), value: String(stats.media_probed ?? 0), color: "accent" },
  ];

  let html = `
    <div class="dashboard-header">
      <h2>${t("dashboard.title")}</h2>
      <button class="btn-refresh" id="btn-dashboard-refresh" title="${t("dashboard.refresh")}">&#8635; ${t("dashboard.refresh")}</button>
    </div>
    <div class="stats-grid-v2">`;

  for (const card of statCards) {
    const linkStart = card.link ? `<a href="${card.link}" class="stat-card-v2-link">` : "";
    const linkEnd = card.link ? "</a>" : "";
    html += `${linkStart}<div class="stat-card-v2 stat-color-${card.color}">
      <div class="stat-icon">${card.icon}</div>
      <div class="stat-content">
        <div class="stat-value">${card.value}</div>
        <div class="stat-label">${card.label}</div>
      </div>
    </div>${linkEnd}`;
  }

  html += `</div>`;

  // Quick actions
  html += `
    <div class="quick-actions">
      <h3>${t("dashboard.quick_actions")}</h3>
      <div class="quick-actions-grid">
        <a href="#duplicates" class="quick-action-card" data-page="duplicates">
          <span class="qa-icon">&#128203;</span>
          <span class="qa-label">${t("dashboard.view_duplicates")}</span>
          ${stats.duplicate_groups > 0 ? `<span class="qa-badge">${stats.duplicate_groups}</span>` : ""}
        </a>
        <a href="#files" class="quick-action-card" data-page="files">
          <span class="qa-icon">&#128247;</span>
          <span class="qa-label">${t("dashboard.view_files")}</span>
        </a>
        <button class="quick-action-card" id="btn-dashboard-scan">
          <span class="qa-icon">&#128269;</span>
          <span class="qa-label">${t("dashboard.scan_folder")}</span>
        </button>
      </div>
    </div>`;

  // Top extensions as visual bars
  const exts = stats.top_extensions;
  if (exts && exts.length) {
    const maxCount = exts[0][1];
    html += `<div class="dashboard-section">
      <h3>${t("dashboard.top_extensions")}</h3>
      <div class="bar-chart">`;
    for (const [ext, count] of exts.slice(0, 8)) {
      const pct = Math.round((count / maxCount) * 100);
      html += `<div class="bar-row">
        <span class="bar-label">.${escapeHtml(ext)}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        <span class="bar-value">${count.toLocaleString()}</span>
      </div>`;
    }
    html += `</div></div>`;
  }

  // Top cameras
  const cams = stats.top_cameras;
  if (cams && cams.length) {
    const maxCam = cams[0][1];
    html += `<div class="dashboard-section">
      <h3>${t("dashboard.top_cameras")}</h3>
      <div class="bar-chart">`;
    for (const [cam, count] of cams.slice(0, 6)) {
      const pct = Math.round((count / maxCam) * 100);
      html += `<div class="bar-row">
        <span class="bar-label">${escapeHtml(cam)}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        <span class="bar-value">${count.toLocaleString()}</span>
      </div>`;
    }
    html += `</div></div>`;
  }

  container.innerHTML = html;

  // Bind refresh
  const refreshBtn = container.querySelector("#btn-dashboard-refresh");
  if (refreshBtn) {
    refreshBtn.addEventListener("click", () => render(container));
  }

  // Bind scan
  const scanBtn = container.querySelector("#btn-dashboard-scan");
  if (scanBtn) {
    scanBtn.addEventListener("click", () => {
      // Open settings panel (which has the pipeline form)
      const settingsBtn = $("#btn-settings");
      if (settingsBtn) settingsBtn.click();
    });
  }
}
