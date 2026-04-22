/* GOD MODE Media Library — Dashboard page (Apple-quality redesign) */

import { api, apiPost, apiDelete } from "../api.js";
import { $, content, formatBytes, escapeHtml, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showGlobalProgress } from "../tasks.js";
import { openFolderPicker } from "../folder-picker.js";
import { openLightbox } from "../lightbox.js";
import { loadTags } from "../tags.js";
import { applySmartFilter } from "./files.js";
import { renderActivityFeed } from "../activity-feed.js";

let _selectedRoots = [];

export async function render(container) {
  try {
    const stats = await api("/stats");
    if (!stats.total_files || stats.total_files === 0) {
      await renderEmptyState(container);
      return;
    }
    await renderDashboard(container, stats);
  } catch (e) {
    await renderEmptyState(container);
  }
}

// ── Empty state (first-run) ─────────────────────────────────────

async function renderEmptyState(container) {
  try {
    const data = await api("/roots");
    _selectedRoots = data.roots || [];
  } catch {
    _selectedRoots = [];
  }

  let bookmarks = [];
  try {
    const data = await api("/browse");
    bookmarks = (data.bookmarks || []).slice(0, 4);
  } catch { /* silent */ }

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
        const merged = [...new Set([..._selectedRoots, ...paths])];
        _selectedRoots = merged;
        try { await apiPost("/roots", { roots: _selectedRoots }); } catch { /* silent */ }
        _renderEmptyContent(container, bookmarks);
      }, _selectedRoots);
    });
  }

  container.querySelectorAll(".quick-add-btn:not([disabled])").forEach(btn => {
    btn.addEventListener("click", async () => {
      const path = btn.dataset.path;
      if (!_selectedRoots.includes(path)) {
        _selectedRoots.push(path);
        try { await apiPost("/roots", { roots: _selectedRoots }); } catch { /* silent */ }
        btn.classList.add("added");
        btn.disabled = true;
        _renderEmptyContent(container, bookmarks);
      }
    });
  });

  container.querySelectorAll(".folder-chip-remove").forEach(btn => {
    btn.addEventListener("click", async () => {
      const path = btn.dataset.path;
      _selectedRoots = _selectedRoots.filter(r => r !== path);
      try { await apiDelete("/roots", { path }); } catch { /* silent */ }
      _renderEmptyContent(container, bookmarks);
    });
  });

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

// ── Main dashboard ──────────────────────────────────────────────

async function renderDashboard(container, stats) {
  let roots = [];
  let sysInfo = null;
  let memoriesData = null;
  let favoritesCount = 0;
  let tagsData = [];
  let sourcesData = null;
  try {
    const [rootsData, sysData, memData, favsData, tagsResult, srcData] = await Promise.all([
      api("/roots").catch(() => ({ roots: [] })),
      api("/system-info").catch(() => null),
      api("/memories").catch(() => null),
      api("/files/favorites").catch(() => ({ count: 0 })),
      loadTags().catch(() => []),
      api("/sources").catch(() => null),
    ]);
    roots = rootsData.roots || [];
    sysInfo = sysData;
    memoriesData = memData;
    favoritesCount = favsData?.count ?? 0;
    tagsData = tagsResult || [];
    sourcesData = srcData;
  } catch { /* silent */ }

  // Compute derived values
  const totalFiles = stats.total_files || 0;
  const hashedPct = totalFiles > 0 ? Math.round((stats.hashed_files / totalFiles) * 100) : 0;
  const gpsPct = totalFiles > 0 ? Math.round((stats.gps_files / totalFiles) * 100) : 0;
  const datePct = totalFiles > 0 ? Math.round(((stats.date_original_count || 0) / totalFiles) * 100) : 0;

  // ── Greeting based on time of day
  const hour = new Date().getHours();
  const greeting = hour < 12 ? t("dashboard.greeting_morning") : hour < 18 ? t("dashboard.greeting_afternoon") : t("dashboard.greeting_evening");

  let html = `<div class="dash">`;

  // ── Hero header
  html += `
    <header class="dash-hero">
      <div class="dash-hero-text">
        <h1 class="dash-greeting">${greeting}</h1>
        <p class="dash-subtitle">${t("dashboard.subtitle", { count: totalFiles.toLocaleString(), size: formatBytes(stats.total_size_bytes) })}</p>
      </div>
      <button class="dash-refresh-btn" id="btn-dashboard-refresh" title="${t("dashboard.refresh")}">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M1 1v4h4"/><path d="M1 5a7 7 0 0113.36 2M15 15v-4h-4"/><path d="M15 11A7 7 0 011.64 9"/></svg>
      </button>
    </header>`;

  // ── Stats ribbon
  const statItems = [
    { label: t("dashboard.total_files"), value: totalFiles.toLocaleString(), sub: formatBytes(stats.total_size_bytes), accent: true },
    { label: t("dashboard.hashed"), value: `${hashedPct}%`, sub: `${(stats.hashed_files || 0).toLocaleString()} ${t("dashboard.source_files")}` },
    { label: t("dashboard.gps_files"), value: `${gpsPct}%`, sub: `${(stats.gps_files || 0).toLocaleString()} ${t("dashboard.source_files")}` },
    { label: t("dashboard.dated_files"), value: `${datePct}%`, sub: `${(stats.date_original_count || 0).toLocaleString()} ${t("dashboard.source_files")}` },
  ];

  if (stats.total_faces > 0) {
    statItems.push({ label: t("dashboard.faces_detected"), value: stats.total_faces.toLocaleString(), sub: `${stats.total_persons || 0} ${t("dashboard.persons")}` });
  }
  if (stats.duplicate_groups > 0) {
    statItems.push({ label: t("dashboard.duplicate_groups"), value: stats.duplicate_groups.toLocaleString(), sub: `${(stats.duplicate_files || 0).toLocaleString()} ${t("dashboard.source_files")}`, warn: true, link: "#duplicates" });
  }
  if (favoritesCount > 0) {
    statItems.push({ label: t("dashboard.favorites"), value: favoritesCount.toLocaleString(), sub: "" });
  }

  html += `<section class="dash-stats-ribbon">`;
  for (const item of statItems) {
    const wrapStart = item.link ? `<a href="${item.link}" class="dash-stat-link">` : "";
    const wrapEnd = item.link ? "</a>" : "";
    const cls = item.warn ? "dash-stat dash-stat--warn" : item.accent ? "dash-stat dash-stat--accent" : "dash-stat";
    html += `${wrapStart}<div class="${cls}">
      <span class="dash-stat-value">${item.value}</span>
      <span class="dash-stat-label">${item.label}</span>
      ${item.sub ? `<span class="dash-stat-sub">${item.sub}</span>` : ""}
    </div>${wrapEnd}`;
  }
  html += `</section>`;

  // ── Integrity score
  html += `<div id="integrity-score-widget" class="dash-integrity"></div>`;

  // ── Memories (On This Day)
  if (memoriesData && memoriesData.memories && memoriesData.memories.length > 0) {
    const monthNames = t("months.genitive").split(",");
    html += `<section class="dash-card dash-memories">
      <div class="dash-card-header">
        <h2 class="dash-card-title">${t("dashboard.memories")}</h2>
        <span class="dash-card-badge">${t("dashboard.on_this_day_label")}</span>
      </div>`;
    for (const mem of memoriesData.memories) {
      const yearsAgoLabel = mem.years_ago === 1
        ? t("dashboard.one_year_ago")
        : t("dashboard.years_ago", { count: mem.years_ago });
      const memDate = new Date(memoriesData.date);
      const dateLabel = `${memDate.getDate()}. ${monthNames[memDate.getMonth()]} ${mem.year}`;
      html += `<div class="dash-memories-group">
        <div class="dash-memories-year-label">
          <span class="dash-memories-ago">${escapeHtml(yearsAgoLabel)}</span>
          <span class="dash-memories-date">${escapeHtml(dateLabel)}</span>
        </div>
        <div class="dash-memories-scroll">`;
      for (const f of mem.files) {
        const thumbUrl = `/api/thumbnail${encodeURI(f.path)}?size=280`;
        html += `<img class="dash-memories-img" src="${thumbUrl}" alt="${escapeHtml(f.path.split("/").pop())}" data-memory-path="${escapeHtml(f.path)}" onerror="this.style.display='none'" loading="lazy">`;
      }
      html += `</div></div>`;
    }
    html += `</section>`;
  }

  // ── Quick actions
  html += `
    <section class="dash-card">
      <h2 class="dash-card-title">${t("dashboard.quick_actions")}</h2>
      <div class="dash-actions-grid">
        <a href="#files" class="dash-action" data-page="files">
          <div class="dash-action-icon dash-action-icon--blue">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path d="M2 4a2 2 0 012-2h4.586A2 2 0 0110 2.586L11.414 4H16a2 2 0 012 2v10a2 2 0 01-2 2H4a2 2 0 01-2-2V4z"/></svg>
          </div>
          <span class="dash-action-label">${t("dashboard.view_files")}</span>
        </a>
        <a href="#duplicates" class="dash-action" data-page="duplicates">
          <div class="dash-action-icon dash-action-icon--orange">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path d="M4 4a2 2 0 00-2 2v1h16V6a2 2 0 00-2-2H4z"/><path fill-rule="evenodd" d="M18 9H2v5a2 2 0 002 2h12a2 2 0 002-2V9zM4 13a1 1 0 011-1h1a1 1 0 110 2H5a1 1 0 01-1-1zm5-1a1 1 0 100 2h1a1 1 0 100-2H9z" clip-rule="evenodd"/></svg>
          </div>
          <span class="dash-action-label">${t("dashboard.view_duplicates")}</span>
          ${stats.duplicate_groups > 0 ? `<span class="dash-action-badge">${stats.duplicate_groups}</span>` : ""}
        </a>
        <a href="#timeline" class="dash-action" data-page="timeline">
          <div class="dash-action-icon dash-action-icon--purple">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M6 2a1 1 0 00-1 1v1H4a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V6a2 2 0 00-2-2h-1V3a1 1 0 10-2 0v1H7V3a1 1 0 00-1-1zm0 5a1 1 0 000 2h8a1 1 0 100-2H6z" clip-rule="evenodd"/></svg>
          </div>
          <span class="dash-action-label">${t("nav.timeline")}</span>
        </a>
        <a href="#map" class="dash-action" data-page="map">
          <div class="dash-action-icon dash-action-icon--green">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/></svg>
          </div>
          <span class="dash-action-label">${t("nav.map")}</span>
        </a>
        <a href="#people" class="dash-action" data-page="people">
          <div class="dash-action-icon dash-action-icon--pink">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path d="M9 6a3 3 0 11-6 0 3 3 0 016 0zM17 6a3 3 0 11-6 0 3 3 0 016 0zM12.93 17c.046-.327.07-.66.07-1a6.97 6.97 0 00-1.5-4.33A5 5 0 0119 16v1h-6.07zM6 11a5 5 0 015 5v1H1v-1a5 5 0 015-5z"/></svg>
          </div>
          <span class="dash-action-label">${t("nav.people")}</span>
        </a>
        <button class="dash-action" id="btn-dashboard-scan">
          <div class="dash-action-icon dash-action-icon--teal">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path d="M9 9a2 2 0 114 0 2 2 0 01-4 0z"/><path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-13a4 4 0 00-3.446 6.032l-2.261 2.26a1 1 0 101.414 1.415l2.261-2.261A4 4 0 1011 5z" clip-rule="evenodd"/></svg>
          </div>
          <span class="dash-action-label">${t("dashboard.scan_folder")}</span>
        </button>
        <a href="#iphone" class="dash-action" data-page="iphone">
          <div class="dash-action-icon dash-action-icon--slate">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M7 2a2 2 0 00-2 2v12a2 2 0 002 2h6a2 2 0 002-2V4a2 2 0 00-2-2H7zm3 14a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd"/></svg>
          </div>
          <span class="dash-action-label">iPhone</span>
        </a>
        <button class="dash-action" id="btn-generate-report">
          <div class="dash-action-icon dash-action-icon--amber">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M6 2a2 2 0 00-2 2v12a2 2 0 002 2h8a2 2 0 002-2V7.414A2 2 0 0015.414 6L12 2.586A2 2 0 0010.586 2H6zm2 10a1 1 0 10-2 0v3a1 1 0 102 0v-3zm2-3a1 1 0 011 1v5a1 1 0 11-2 0v-5a1 1 0 011-1zm4-1a1 1 0 10-2 0v7a1 1 0 102 0V8z" clip-rule="evenodd"/></svg>
          </div>
          <span class="dash-action-label">${t("report.generate")}</span>
        </button>
      </div>
    </section>`;

  // ── Smart views
  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  const dateFrom30 = thirtyDaysAgo.toISOString().split("T")[0];

  const smartCards = [
    { icon: `<svg width="18" height="18" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4 5a2 2 0 00-2 2v8a2 2 0 002 2h12a2 2 0 002-2V7a2 2 0 00-2-2h-1.586a1 1 0 01-.707-.293l-1.121-1.121A2 2 0 0011.172 3H8.828a2 2 0 00-1.414.586L6.293 4.707A1 1 0 015.586 5H4zm6 9a3 3 0 100-6 3 3 0 000 6z" clip-rule="evenodd"/></svg>`, label: t("smart.recent_photos"), filter: { ext: "jpg,jpeg,png,gif,bmp,tiff,tif,webp,heic,heif,raw,cr2,nef,arw,dng", date_from: dateFrom30 } },
    { icon: `<svg width="18" height="18" viewBox="0 0 20 20" fill="currentColor"><path d="M2 6a2 2 0 012-2h6a2 2 0 012 2v8a2 2 0 01-2 2H4a2 2 0 01-2-2V6z"/><path d="M14.553 7.106A1 1 0 0014 8v4a1 1 0 00.553.894l2 1A1 1 0 0018 13V7a1 1 0 00-1.447-.894l-2 1z"/></svg>`, label: t("smart.all_videos"), filter: { ext: "mp4,mov,avi,mkv,wmv,flv,webm,m4v,3gp" } },
    { icon: `<svg width="18" height="18" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M5.05 4.05a7 7 0 119.9 9.9L10 18.9l-4.95-4.95a7 7 0 010-9.9zM10 11a2 2 0 100-4 2 2 0 000 4z" clip-rule="evenodd"/></svg>`, label: t("smart.with_location"), filter: { has_gps: true } },
    { icon: `<svg width="18" height="18" viewBox="0 0 20 20" fill="currentColor"><path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z"/></svg>`, label: t("smart.top_rated"), filter: { min_rating: 4 } },
    { icon: `<svg width="18" height="18" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M3 17a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm3.293-7.707a1 1 0 011.414 0L9 10.586V3a1 1 0 112 0v7.586l1.293-1.293a1 1 0 111.414 1.414l-3 3a1 1 0 01-1.414 0l-3-3a1 1 0 010-1.414z" clip-rule="evenodd"/></svg>`, label: t("smart.large_files"), filter: { min_size: 104857600 } },
  ];

  html += `<section class="dash-card">
    <h2 class="dash-card-title">${t("smart.title")}</h2>
    <div class="dash-smart-grid">`;
  for (let i = 0; i < smartCards.length; i++) {
    const sc = smartCards[i];
    html += `<button class="dash-smart-pill" data-smart-idx="${i}">
      <span class="dash-smart-icon">${sc.icon}</span>
      <span>${escapeHtml(sc.label)}</span>
    </button>`;
  }
  html += `</div></section>`;

  // ── Sources / managed folders
  const sources = sourcesData?.sources || [];
  if (sources.length > 0) {
    html += `<section class="dash-card">
      <h2 class="dash-card-title">${t("dashboard.sources")}</h2>
      <div class="dash-sources-grid">`;
    for (const src of sources) {
      const statusCls = src.online ? "dash-source--online" : "dash-source--offline";
      const statusLabel = src.online ? t("dashboard.source_online") : t("dashboard.source_offline");
      const lastScanLabel = src.last_scan
        ? t("dashboard.source_last_scan", { date: new Date(src.last_scan).toLocaleDateString("cs") })
        : "";
      html += `<div class="dash-source ${statusCls}">
        <div class="dash-source-head">
          <span class="dash-source-dot"></span>
          <span class="dash-source-name">${escapeHtml(src.name)}</span>
          <span class="dash-source-status">${statusLabel}</span>
        </div>
        <div class="dash-source-path">${escapeHtml(src.path)}</div>
        <div class="dash-source-meta">
          <span>${src.file_count.toLocaleString()} ${t("dashboard.source_files")}</span>
          <span>${formatBytes(src.total_size)}</span>
        </div>
        ${lastScanLabel ? `<div class="dash-source-scan">${lastScanLabel}</div>` : ""}
        ${src.online ? `<button class="dash-source-sync" data-root="${escapeHtml(src.path)}">${t("dashboard.source_sync")}</button>` : ""}
      </div>`;
    }
    html += `</div></section>`;
  } else if (roots.length > 0) {
    html += `<section class="dash-card">
      <h2 class="dash-card-title">${t("dashboard.sources")}</h2>
      <div class="dash-roots-list">`;
    for (const root of roots) {
      const name = root.split("/").pop() || root;
      html += `<div class="dash-root-chip">
        <span class="dash-root-icon">\u{1F4C1}</span>
        <span class="dash-root-name">${escapeHtml(name)}</span>
        <span class="dash-root-path">${escapeHtml(root)}</span>
      </div>`;
    }
    html += `</div></section>`;
  }

  // ── Top tags
  if (tagsData.length > 0) {
    const topTags = tagsData.filter(tg => tg.file_count > 0).slice(0, 8);
    if (topTags.length > 0) {
      html += `<section class="dash-card">
        <h2 class="dash-card-title">${t("tags.top_tags")}</h2>
        <div class="dash-tags-row">`;
      for (const tag of topTags) {
        html += `<a href="#files" class="dash-tag" style="--tag-color:${tag.color}" data-tag-id="${tag.id}">${escapeHtml(tag.name)}<span class="dash-tag-count">${tag.file_count}</span></a>`;
      }
      html += `</div></section>`;
    }
  }

  // ── Storage breakdown
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

      html += `<section class="dash-card">
        <h2 class="dash-card-title">${t("dashboard.storage_breakdown")}</h2>
        <div class="dash-breakdown">
          <div class="dash-breakdown-bar">
            <div class="dash-breakdown-seg dash-breakdown-seg--image" style="width:${imagePct}%"></div>
            <div class="dash-breakdown-seg dash-breakdown-seg--video" style="width:${videoPct}%"></div>
            <div class="dash-breakdown-seg dash-breakdown-seg--other" style="width:${otherPct}%"></div>
          </div>
          <div class="dash-breakdown-legend">
            <span class="dash-legend-item"><span class="dash-legend-dot dash-legend-dot--image"></span>${t("dashboard.images")} ${imageCount.toLocaleString()} (${imagePct}%)</span>
            <span class="dash-legend-item"><span class="dash-legend-dot dash-legend-dot--video"></span>${t("dashboard.videos")} ${videoCount.toLocaleString()} (${videoPct}%)</span>
            <span class="dash-legend-item"><span class="dash-legend-dot dash-legend-dot--other"></span>${t("dashboard.other")} ${otherCount.toLocaleString()} (${otherPct}%)</span>
          </div>
        </div>
      </section>`;
    }
  }

  // ── Two-column layout: Extensions + Cameras
  const exts = stats.top_extensions;
  const cams = stats.top_cameras;
  if ((exts && exts.length) || (cams && cams.length)) {
    html += `<div class="dash-two-col">`;

    if (exts && exts.length) {
      const maxCount = exts[0][1];
      html += `<section class="dash-card">
        <h2 class="dash-card-title">${t("dashboard.top_extensions")}</h2>
        <div class="dash-bars">`;
      for (const [ext, count] of exts.slice(0, 8)) {
        const pct = Math.round((count / maxCount) * 100);
        html += `<div class="dash-bar-row">
          <span class="dash-bar-label">.${escapeHtml(ext)}</span>
          <div class="dash-bar-track"><div class="dash-bar-fill" style="width:${pct}%"></div></div>
          <span class="dash-bar-value">${count.toLocaleString()}</span>
        </div>`;
      }
      html += `</div></section>`;
    }

    if (cams && cams.length) {
      const maxCam = cams[0][1];
      html += `<section class="dash-card">
        <h2 class="dash-card-title">${t("dashboard.top_cameras")}</h2>
        <div class="dash-bars">`;
      for (const [cam, count] of cams.slice(0, 6)) {
        const pct = Math.round((count / maxCam) * 100);
        html += `<div class="dash-bar-row">
          <span class="dash-bar-label">${escapeHtml(cam)}</span>
          <div class="dash-bar-track"><div class="dash-bar-fill dash-bar-fill--cam" style="width:${pct}%"></div></div>
          <span class="dash-bar-value">${count.toLocaleString()}</span>
        </div>`;
      }
      html += `</div></section>`;
    }

    html += `</div>`;
  }

  // ── Activity feed
  html += `<section class="dash-card">
    <h2 class="dash-card-title">${t("activity.recent_title")}</h2>
    <div id="activity-feed"></div>
  </section>`;

  html += `</div>`; // close .dash

  container.innerHTML = html;

  // ── Bind events ───────────────────────────────────────────────

  // Activity feed
  const activityEl = container.querySelector("#activity-feed");
  if (activityEl) renderActivityFeed(activityEl);

  // Refresh
  const refreshBtn = container.querySelector("#btn-dashboard-refresh");
  if (refreshBtn) refreshBtn.addEventListener("click", () => render(container));

  // Scan
  const scanBtn = container.querySelector("#btn-dashboard-scan");
  if (scanBtn) {
    scanBtn.addEventListener("click", () => {
      const settingsBtn = $("#btn-settings");
      if (settingsBtn) settingsBtn.click();
    });
  }

  // Report
  const reportBtn = container.querySelector("#btn-generate-report");
  if (reportBtn) {
    reportBtn.addEventListener("click", () => window.open("/api/report/generate", "_blank"));
  }

  // Source sync
  container.querySelectorAll(".dash-source-sync").forEach(btn => {
    btn.addEventListener("click", async () => {
      const root = btn.dataset.root;
      btn.disabled = true;
      btn.textContent = t("general.loading");
      try {
        const data = await apiPost("/pipeline", { roots: [root], workers: 1, extract_exiftool: true });
        showToast(t("dashboard.source_sync_started", { name: root.split("/").pop() }), "info");
        showGlobalProgress(data.task_id);
      } catch (e) {
        showToast(e.message, "error");
        btn.disabled = false;
        btn.textContent = t("dashboard.source_sync");
      }
    });
  });

  // Smart views
  container.querySelectorAll(".dash-smart-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      const idx = parseInt(btn.dataset.smartIdx, 10);
      const filter = smartCards[idx].filter;
      applySmartFilter(filter);
      window.location.hash = "#files";
    });
  });

  // Memory thumbnails -> lightbox
  const memThumbs = container.querySelectorAll("[data-memory-path]");
  if (memThumbs.length > 0) {
    const memPaths = Array.from(memThumbs).map(el => el.dataset.memoryPath);
    memThumbs.forEach((thumb, idx) => {
      thumb.addEventListener("click", () => openLightbox(memPaths, idx));
    });
  }

  // Integrity score widget
  loadIntegrityScore();
}

// ── Integrity score ─────────────────────────────────────────────

async function loadIntegrityScore() {
  const el = document.getElementById("integrity-score-widget");
  if (!el) return;
  try {
    const data = await api("/integrity-score");
    const score = data.score || 0;
    const grade = data.grade || "?";
    const circumference = 2 * Math.PI * 40;
    const offset = circumference * (1 - score / 100);
    const color = score >= 80 ? "var(--color-success)" : score >= 60 ? "var(--color-warning)" : "var(--color-error)";

    const factors = Object.values(data.factors || {}).map(f =>
      `<div class="dash-integrity-factor">
        <span>${f.label}</span>
        <span class="dash-integrity-factor-val" style="color:${f.value >= 70 ? 'var(--color-success)' : f.value >= 40 ? 'var(--color-warning)' : 'var(--color-error)'}">${f.value}%</span>
      </div>`
    ).join("");

    el.innerHTML = `
      <div class="dash-card dash-integrity-card">
        <div class="dash-integrity-ring">
          <svg viewBox="0 0 100 100">
            <circle cx="50" cy="50" r="40" fill="none" stroke="var(--color-border)" stroke-width="7"/>
            <circle cx="50" cy="50" r="40" fill="none" stroke="${color}" stroke-width="7"
              stroke-dasharray="${circumference}" stroke-dashoffset="${offset}" stroke-linecap="round"
              style="transform:rotate(-90deg);transform-origin:center;transition:stroke-dashoffset 1s ease"/>
          </svg>
          <div class="dash-integrity-center">
            <span class="dash-integrity-grade" style="color:${color}">${grade}</span>
            <span class="dash-integrity-pct">${score}%</span>
          </div>
        </div>
        <div class="dash-integrity-details">
          <h3 class="dash-card-title">${t("dashboard.library_health")}</h3>
          ${factors}
        </div>
      </div>`;
  } catch {
    el.innerHTML = "";
  }
}
