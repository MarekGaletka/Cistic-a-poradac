/* GOD MODE Media Library — Dashboard page (redesigned) */

import { api, apiPost, apiDelete } from "../api.js";
import { $, content, formatBytes, escapeHtml, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showGlobalProgress } from "../tasks.js";
import { openFolderPicker } from "../folder-picker.js";
import { openLightbox } from "../lightbox.js";
import { loadTags } from "../tags.js";

let _selectedRoots = [];

export async function render(container) {
  try {
    const stats = await api("/stats");

    // Check if catalog is empty
    if (!stats.total_files || stats.total_files === 0) {
      await renderEmptyState(container);
      return;
    }

    await renderDashboard(container, stats);
  } catch (e) {
    // API error likely means no catalog — show empty state
    await renderEmptyState(container);
  }
}

async function renderEmptyState(container) {
  // Load saved roots
  try {
    const data = await api("/roots");
    _selectedRoots = data.roots || [];
  } catch {
    _selectedRoots = [];
  }

  // Load bookmarks for quick-add
  let bookmarks = [];
  try {
    const data = await api("/browse");
    bookmarks = (data.bookmarks || []).slice(0, 4); // First 4: Desktop, Pictures, Documents, Downloads
  } catch {
    // silent
  }

  _renderEmptyContent(container, bookmarks);
}

function _renderEmptyContent(container, bookmarks) {
  let quickAddHtml = "";
  if (bookmarks.length > 0) {
    quickAddHtml = `
      <p class="empty-state-or">${t("dashboard.or_quick_add")}</p>
      <div class="quick-add-grid">`;
    for (const bm of bookmarks) {
      const isAdded = _selectedRoots.includes(bm.path);
      quickAddHtml += `<button class="quick-add-btn${isAdded ? " added" : ""}" data-path="${escapeHtml(bm.path)}" ${isAdded ? "disabled" : ""}>
        <span class="quick-add-icon">${bm.icon}</span>
        <span class="quick-add-label">${escapeHtml(bm.name)}</span>
      </button>`;
    }
    quickAddHtml += "</div>";
  }

  let chipsHtml = "";
  if (_selectedRoots.length > 0) {
    chipsHtml = `
      <div class="empty-state-selected">
        <div class="empty-state-selected-title">${t("folder.selected_folders")}</div>
        <div class="folder-chips">`;
    for (const root of _selectedRoots) {
      const name = root.split("/").pop() || root;
      chipsHtml += `<span class="folder-chip"><span class="folder-chip-icon">\u{1F4C1}</span> ${escapeHtml(name)}<span class="folder-chip-path">${escapeHtml(root)}</span><button class="folder-chip-remove" data-path="${escapeHtml(root)}" aria-label="${t("folder.remove")}">&times;</button></span>`;
    }
    chipsHtml += "</div></div>";
  }

  container.innerHTML = `
    <div class="empty-state-hero">
      <div class="empty-state-icon">&#128247;</div>
      <h2 class="empty-state-title">${t("dashboard.empty_title")}</h2>
      <p class="empty-state-subtitle">${t("dashboard.empty_hint_v2")}</p>
      <div class="empty-state-actions">
        <button class="folder-add-btn" id="btn-open-folder-picker">
          <span class="folder-add-icon">\u{1F4C1}</span>
          <span class="folder-add-text">
            <strong>${t("folder.add_folders")}</strong>
            <small>${t("folder.browse")}</small>
          </span>
        </button>
      </div>
      ${quickAddHtml}
      ${chipsHtml}
      ${_selectedRoots.length > 0 ? `
        <button class="primary scan-start-btn" id="btn-start-scanning">
          \u25B6 ${t("folder.start_scan")}
        </button>
      ` : ""}
    </div>`;

  // Bind folder picker button
  const pickerBtn = container.querySelector("#btn-open-folder-picker");
  if (pickerBtn) {
    pickerBtn.addEventListener("click", () => {
      openFolderPicker(async (paths) => {
        // Merge with existing
        const merged = [...new Set([..._selectedRoots, ...paths])];
        _selectedRoots = merged;
        try {
          await apiPost("/roots", { roots: _selectedRoots });
        } catch { /* silent */ }
        _renderEmptyContent(container, bookmarks);
      }, _selectedRoots);
    });
  }

  // Bind quick-add buttons
  container.querySelectorAll(".quick-add-btn:not([disabled])").forEach(btn => {
    btn.addEventListener("click", async () => {
      const path = btn.dataset.path;
      if (!_selectedRoots.includes(path)) {
        _selectedRoots.push(path);
        try {
          await apiPost("/roots", { roots: _selectedRoots });
        } catch { /* silent */ }
        btn.classList.add("added");
        btn.disabled = true;
        _renderEmptyContent(container, bookmarks);
      }
    });
  });

  // Bind chip remove buttons
  container.querySelectorAll(".folder-chip-remove").forEach(btn => {
    btn.addEventListener("click", async () => {
      const path = btn.dataset.path;
      _selectedRoots = _selectedRoots.filter(r => r !== path);
      try {
        await apiDelete("/roots", { path });
      } catch { /* silent */ }
      _renderEmptyContent(container, bookmarks);
    });
  });

  // Bind start scan button
  const startBtn = container.querySelector("#btn-start-scanning");
  if (startBtn) {
    startBtn.addEventListener("click", async () => {
      startBtn.disabled = true;
      startBtn.textContent = t("general.loading");
      try {
        const data = await apiPost("/pipeline", { roots: _selectedRoots, workers: 1, extract_exiftool: true });
        showToast(t("pipeline.started"), "info");
        showGlobalProgress(data.task_id);
      } catch (e) {
        showToast(t("pipeline.start_failed", { message: e.message }), "error");
        startBtn.disabled = false;
        startBtn.textContent = `\u25B6 ${t("folder.start_scan")}`;
      }
    });
  }
}

async function renderDashboard(container, stats) {
  // Load roots, system info, memories, and favorites in parallel
  let roots = [];
  let sysInfo = null;
  let memoriesData = null;
  let favoritesCount = 0;
  let tagsData = [];
  try {
    const [rootsData, sysData, memData, favsData, tagsResult] = await Promise.all([
      api("/roots").catch(() => ({ roots: [] })),
      api("/system-info").catch(() => null),
      api("/memories").catch(() => null),
      api("/files/favorites").catch(() => ({ count: 0 })),
      loadTags().catch(() => []),
    ]);
    roots = rootsData.roots || [];
    sysInfo = sysData;
    memoriesData = memData;
    favoritesCount = favsData?.count ?? 0;
    tagsData = tagsResult || [];
  } catch {
    // silent
  }

  const statCards = [
    { icon: "&#128247;", label: t("dashboard.total_files"), value: stats.total_files?.toLocaleString() ?? "0", color: "accent" },
    { icon: "&#128190;", label: t("dashboard.total_size"), value: formatBytes(stats.total_size_bytes), color: "accent" },
    { icon: "&#128203;", label: t("dashboard.duplicate_groups"), value: String(stats.duplicate_groups ?? 0), color: stats.duplicate_groups > 0 ? "yellow" : "green", link: stats.duplicate_groups > 0 ? "#duplicates" : null },
    { icon: "&#127758;", label: t("dashboard.gps_files"), value: String(stats.gps_files ?? 0), color: "accent" },
    { icon: "&#128274;", label: t("dashboard.hashed"), value: stats.hashed_files?.toLocaleString() ?? "0", color: "green" },
    { icon: "&#127910;", label: t("dashboard.media_probed"), value: String(stats.media_probed ?? 0), color: "accent" },
  ];
  if (favoritesCount > 0) {
    statCards.push({ icon: "&#11088;", label: t("dashboard.favorites"), value: String(favoritesCount), color: "yellow" });
  }

  let html = `
    <div class="dashboard-header">
      <h2>${t("dashboard.title")}</h2>
      <button class="btn-refresh" id="btn-dashboard-refresh" title="${t("dashboard.refresh")}">&#8635; ${t("dashboard.refresh")}</button>
    </div>`;

  // Last scan info
  const lastScanRoot = stats.last_scan_root || (sysInfo && sysInfo.last_scan_root) || "";
  if (lastScanRoot) {
    const scanDate = stats.last_scan_date || "";
    const displayDate = scanDate || "\u2014";
    html += `<div style="margin-bottom:12px;padding:8px 14px;background:var(--surface);border:1px solid var(--border);border-radius:8px;font-size:12px;color:var(--text-muted)">
      ${t("dashboard.last_scan_info", { date: displayDate })}
    </div>`;
  }

  html += `<div class="stats-grid-v2">`;

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

  // Memories (On This Day) section
  if (memoriesData && memoriesData.memories && memoriesData.memories.length > 0) {
    const monthNames = ["ledna", "února", "března", "dubna", "května", "června",
      "července", "srpna", "září", "října", "listopadu", "prosince"];
    html += `<div class="memories-section">
      <div class="memories-section-header">
        <span class="memories-icon">&#128248;</span>
        <h3>${t("dashboard.memories")}</h3>
      </div>`;
    for (const mem of memoriesData.memories) {
      const yearsAgoLabel = mem.years_ago === 1
        ? t("dashboard.one_year_ago")
        : t("dashboard.years_ago", { count: mem.years_ago });
      const memDate = new Date(memoriesData.date);
      const dateLabel = `${memDate.getDate()}. ${monthNames[memDate.getMonth()]} ${mem.year}`;
      html += `<div class="memories-year">
        <div class="memories-header">
          <span class="years-ago">${escapeHtml(yearsAgoLabel)}</span>
          <span style="color:var(--text-muted);font-weight:400;font-size:13px">&mdash; ${escapeHtml(dateLabel)}</span>
        </div>
        <div class="memories-scroll">`;
      for (const f of mem.files) {
        const thumbUrl = `/api/thumbnail${encodeURI(f.path)}?size=250`;
        html += `<img class="memories-thumb" src="${thumbUrl}" alt="${escapeHtml(f.path.split("/").pop())}" data-memory-path="${escapeHtml(f.path)}" onerror="this.style.display='none'" loading="lazy">`;
      }
      html += `</div></div>`;
    }
    html += `</div>`;
  }

  // Managed folders section
  html += `<div class="dashboard-section" style="margin-top:16px">
    <h3>${t("dashboard.managed_folders")}</h3>`;
  if (roots.length > 0) {
    html += `<div style="display:flex;flex-wrap:wrap;gap:6px">`;
    for (const root of roots) {
      const name = root.split("/").pop() || root;
      html += `<span class="folder-chip" style="cursor:default"><span class="folder-chip-icon">\u{1F4C1}</span> ${escapeHtml(name)}<span class="folder-chip-path" style="font-size:10px;color:var(--text-muted);margin-left:4px">${escapeHtml(root)}</span></span>`;
    }
    html += `</div>`;
  } else {
    html += `<div style="font-size:12px;color:var(--text-muted);padding:8px 0">${t("dashboard.no_folders")} <a href="#" id="btn-dashboard-open-settings" style="color:var(--accent)">${t("dashboard.open_settings")}</a></div>`;
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

  // Top tags section
  if (tagsData.length > 0) {
    const topTags = tagsData.filter(t => t.file_count > 0).slice(0, 5);
    if (topTags.length > 0) {
      html += `<div class="dashboard-section">
        <h3>${t("tags.top_tags")}</h3>
        <div style="display:flex;flex-wrap:wrap;gap:6px">`;
      for (const tag of topTags) {
        html += `<a href="#files" class="tag-pill tag-pill-dashboard" style="background:${tag.color}22;color:${tag.color};border:1px solid ${tag.color}44;text-decoration:none" data-tag-id="${tag.id}">${escapeHtml(tag.name)} <span style="opacity:0.7;font-size:11px">(${tag.file_count})</span></a>`;
      }
      html += `</div></div>`;
    }
  }

  // Storage breakdown (donut chart using CSS conic-gradient)
  const topExts = stats.top_extensions || [];
  if (topExts.length > 0) {
    const imageExts = new Set(["jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp", "heic", "heif", "raw", "cr2", "nef", "arw", "dng"]);
    const videoExts = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v", "mts"]);
    let imageCount = 0, videoCount = 0, otherCount = 0;
    for (const [ext, count] of topExts) {
      const lext = ext.toLowerCase();
      if (imageExts.has(lext)) imageCount += count;
      else if (videoExts.has(lext)) videoCount += count;
      else otherCount += count;
    }
    const total = imageCount + videoCount + otherCount;
    if (total > 0) {
      const imagePct = Math.round((imageCount / total) * 100);
      const videoPct = Math.round((videoCount / total) * 100);
      const otherPct = 100 - imagePct - videoPct;
      const imageEnd = imagePct;
      const videoEnd = imagePct + videoPct;

      html += `<div class="dashboard-section">
        <h3>${t("dashboard.storage_breakdown")}</h3>
        <div style="display:flex;align-items:center;gap:24px">
          <div style="width:80px;height:80px;border-radius:50%;background:conic-gradient(#3b82f6 0% ${imageEnd}%, #eab308 ${imageEnd}% ${videoEnd}%, #6b7280 ${videoEnd}% 100%);flex-shrink:0"></div>
          <div style="display:flex;flex-direction:column;gap:6px">
            <div style="display:flex;align-items:center;gap:6px;font-size:12px"><span style="width:10px;height:10px;border-radius:2px;background:#3b82f6;display:inline-block"></span> ${t("dashboard.images")} — ${imageCount.toLocaleString()} (${imagePct}%)</div>
            <div style="display:flex;align-items:center;gap:6px;font-size:12px"><span style="width:10px;height:10px;border-radius:2px;background:#eab308;display:inline-block"></span> ${t("dashboard.videos")} — ${videoCount.toLocaleString()} (${videoPct}%)</div>
            <div style="display:flex;align-items:center;gap:6px;font-size:12px"><span style="width:10px;height:10px;border-radius:2px;background:#6b7280;display:inline-block"></span> ${t("dashboard.other")} — ${otherCount.toLocaleString()} (${otherPct}%)</div>
          </div>
        </div>
      </div>`;
    }
  }

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

  // Bind open settings link (for empty managed folders)
  const openSettingsLink = container.querySelector("#btn-dashboard-open-settings");
  if (openSettingsLink) {
    openSettingsLink.addEventListener("click", (e) => {
      e.preventDefault();
      const settingsBtn = $("#btn-settings");
      if (settingsBtn) settingsBtn.click();
    });
  }

  // Bind memories thumbnail clicks to open lightbox
  const memThumbs = container.querySelectorAll("[data-memory-path]");
  if (memThumbs.length > 0) {
    const memPaths = Array.from(memThumbs).map(el => el.dataset.memoryPath);
    memThumbs.forEach((thumb, idx) => {
      thumb.addEventListener("click", () => {
        openLightbox(memPaths, idx);
      });
    });
  }
}
