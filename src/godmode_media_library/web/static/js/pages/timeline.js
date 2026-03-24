/* GOD MODE Media Library — Timeline page (drill-down: years → months → weeks → days) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);
const MONTH_NAMES_CS = ["Led", "Úno", "Bře", "Dub", "Kvě", "Čvn", "Čvc", "Srp", "Zář", "Říj", "Lis", "Pro"];
const MONTH_NAMES_FULL_CS = ["Leden", "Únor", "Březen", "Duben", "Květen", "Červen", "Červenec", "Srpen", "Září", "Říjen", "Listopad", "Prosinec"];

// ── State ──
let _container = null;
let _allFiles = [];
let _level = "years";   // "years" | "months" | "weeks" | "days"
let _filterYear = null;  // e.g. "2025"
let _filterMonth = null; // e.g. "2025-03"
let _filterWeek = null;  // e.g. "2025-W12"

// ── Date helpers ──

function _parseDate(f) {
  const d = f.date_original || "";
  const m = d.match(/^(\d{4})[:\-/](\d{2})[:\-/](\d{2})/);
  if (m) return { year: m[1], month: m[2], day: m[3] };
  // Fallback to timestamp
  const ts = f.birthtime || f.mtime;
  if (ts) {
    const dt = new Date(ts * 1000);
    return {
      year: String(dt.getFullYear()),
      month: String(dt.getMonth() + 1).padStart(2, "0"),
      day: String(dt.getDate()).padStart(2, "0"),
    };
  }
  return null;
}

function _isoWeek(y, m, d) {
  const date = new Date(parseInt(y), parseInt(m) - 1, parseInt(d));
  const jan4 = new Date(date.getFullYear(), 0, 4);
  const dayOfYear = Math.floor((date - new Date(date.getFullYear(), 0, 1)) / 86400000) + 1;
  const weekNum = Math.ceil((dayOfYear + jan4.getDay() - 1) / 7);
  return `${y}-W${String(weekNum).padStart(2, "0")}`;
}

function _weekDateRange(weekKey) {
  // Parse "2025-W12" → start/end dates
  const [y, wStr] = weekKey.split("-W");
  const week = parseInt(wStr);
  const jan1 = new Date(parseInt(y), 0, 1);
  const jan1Day = jan1.getDay() || 7;
  const start = new Date(jan1);
  start.setDate(start.getDate() + (week - 1) * 7 - jan1Day + 1);
  const end = new Date(start);
  end.setDate(end.getDate() + 6);
  const fmt = (d) => d.toLocaleDateString("cs", { day: "numeric", month: "short" });
  return `${fmt(start)} – ${fmt(end)}`;
}

// ── Rendering ──

function _renderBreadcrumb() {
  const parts = [`<span class="tl-bread-item tl-bread-root" data-nav="years">${t("timeline.all_years")}</span>`];
  if (_filterYear) {
    parts.push(`<span class="tl-bread-sep">›</span>`);
    parts.push(`<span class="tl-bread-item ${!_filterMonth ? 'tl-bread-active' : ''}" data-nav="months">${_filterYear}</span>`);
  }
  if (_filterMonth) {
    const [, m] = _filterMonth.split("-");
    const mName = new Date(parseInt(_filterYear), parseInt(m) - 1).toLocaleDateString("cs", { month: "long" });
    parts.push(`<span class="tl-bread-sep">›</span>`);
    parts.push(`<span class="tl-bread-item ${!_filterWeek ? 'tl-bread-active' : ''}" data-nav="weeks">${mName}</span>`);
  }
  if (_filterWeek) {
    const wNum = _filterWeek.split("-W")[1];
    parts.push(`<span class="tl-bread-sep">›</span>`);
    parts.push(`<span class="tl-bread-item tl-bread-active" data-nav="days">${t("timeline.week")} ${parseInt(wNum)}</span>`);
  }
  return `<div class="tl-breadcrumb">${parts.join("")}</div>`;
}

function _renderFileItem(f) {
  const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
  const isVideo = VIDEO_EXTS.has((f.ext || "").toLowerCase());
  const thumb = isImage
    ? `<img data-src="/api/thumbnail${encodeURI(f.path)}?size=200" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}" class="tl-lazy">`
    : isVideo
    ? `<div class="tl-file-icon tl-file-video">▶</div>`
    : `<div class="tl-file-icon">${escapeHtml(f.ext || "?")}</div>`;
  return `<div class="tl-file" tabindex="0" data-file-path="${escapeHtml(f.path)}" title="${escapeHtml(f.path)}">
    ${thumb}
    <div class="tl-file-name">${escapeHtml(fileName(f.path))}</div>
  </div>`;
}

function _renderYears() {
  const groups = {};
  for (const f of _allFiles) {
    const p = _parseDate(f);
    if (!p) continue;
    if (!groups[p.year]) groups[p.year] = [];
    groups[p.year].push(f);
  }
  const years = Object.keys(groups).sort().reverse();

  let html = `<div class="tl-grid tl-grid-years">`;
  for (const year of years) {
    const files = groups[year];
    // Pick up to 4 preview thumbnails
    const previews = files.filter(f => IMAGE_EXTS.has((f.ext || "").toLowerCase())).slice(0, 4);
    html += `
      <div class="tl-card tl-card-year" data-year="${year}">
        <div class="tl-card-previews">
          ${previews.map(f => `<img data-src="/api/thumbnail${encodeURI(f.path)}?size=150" class="tl-lazy tl-card-thumb" onerror="this.style.display='none'">`).join("")}
          ${previews.length === 0 ? `<div class="tl-card-empty-thumb">📁</div>` : ""}
        </div>
        <div class="tl-card-info">
          <span class="tl-card-title">${year}</span>
          <span class="tl-card-count">${t("timeline.file_count", { count: files.length })}</span>
        </div>
      </div>`;
  }
  html += `</div>`;
  return html;
}

function _renderMonths() {
  const groups = {};
  for (const f of _allFiles) {
    const p = _parseDate(f);
    if (!p || p.year !== _filterYear) continue;
    const key = `${p.year}-${p.month}`;
    if (!groups[key]) groups[key] = [];
    groups[key].push(f);
  }
  const months = Object.keys(groups).sort().reverse();

  let html = `<div class="tl-grid tl-grid-months">`;
  for (const month of months) {
    const files = groups[month];
    const [y, m] = month.split("-");
    const mName = new Date(parseInt(y), parseInt(m) - 1).toLocaleDateString("cs", { month: "long" });
    const previews = files.filter(f => IMAGE_EXTS.has((f.ext || "").toLowerCase())).slice(0, 4);
    html += `
      <div class="tl-card tl-card-month" data-month="${month}">
        <div class="tl-card-previews">
          ${previews.map(f => `<img data-src="/api/thumbnail${encodeURI(f.path)}?size=150" class="tl-lazy tl-card-thumb" onerror="this.style.display='none'">`).join("")}
          ${previews.length === 0 ? `<div class="tl-card-empty-thumb">📁</div>` : ""}
        </div>
        <div class="tl-card-info">
          <span class="tl-card-title">${mName}</span>
          <span class="tl-card-count">${t("timeline.file_count", { count: files.length })}</span>
        </div>
      </div>`;
  }
  html += `</div>`;
  return html;
}

function _renderWeeks() {
  const groups = {};
  const [fy, fm] = _filterMonth.split("-");
  for (const f of _allFiles) {
    const p = _parseDate(f);
    if (!p || p.year !== fy || p.month !== fm) continue;
    const wk = _isoWeek(p.year, p.month, p.day);
    if (!groups[wk]) groups[wk] = [];
    groups[wk].push(f);
  }
  const weeks = Object.keys(groups).sort().reverse();

  let html = `<div class="tl-grid tl-grid-weeks">`;
  for (const week of weeks) {
    const files = groups[week];
    const wNum = parseInt(week.split("-W")[1]);
    const range = _weekDateRange(week);
    const previews = files.filter(f => IMAGE_EXTS.has((f.ext || "").toLowerCase())).slice(0, 4);
    html += `
      <div class="tl-card tl-card-week" data-week="${week}">
        <div class="tl-card-previews">
          ${previews.map(f => `<img data-src="/api/thumbnail${encodeURI(f.path)}?size=150" class="tl-lazy tl-card-thumb" onerror="this.style.display='none'">`).join("")}
          ${previews.length === 0 ? `<div class="tl-card-empty-thumb">📁</div>` : ""}
        </div>
        <div class="tl-card-info">
          <span class="tl-card-title">${t("timeline.week")} ${wNum}</span>
          <span class="tl-card-count">${range}</span>
          <span class="tl-card-count">${t("timeline.file_count", { count: files.length })}</span>
        </div>
      </div>`;
  }
  html += `</div>`;
  return html;
}

function _renderDays() {
  // Collect files for this week
  const [fy, fm] = _filterMonth.split("-");
  const dayGroups = {};
  for (const f of _allFiles) {
    const p = _parseDate(f);
    if (!p || p.year !== fy || p.month !== fm) continue;
    const wk = _isoWeek(p.year, p.month, p.day);
    if (wk !== _filterWeek) continue;
    const dayKey = `${p.year}-${p.month}-${p.day}`;
    if (!dayGroups[dayKey]) dayGroups[dayKey] = [];
    dayGroups[dayKey].push(f);
  }
  const days = Object.keys(dayGroups).sort().reverse();

  let html = "";
  for (const day of days) {
    const files = dayGroups[day];
    const dayDate = new Date(day).toLocaleDateString("cs", { weekday: "long", day: "numeric", month: "long", year: "numeric" });
    html += `
      <div class="tl-day-section">
        <div class="tl-day-header">${escapeHtml(dayDate)} <span class="tl-day-count">(${files.length})</span></div>
        <div class="tl-grid tl-grid-files">
          ${files.map(f => _renderFileItem(f)).join("")}
        </div>
      </div>`;
  }
  return html;
}

function _renderContent() {
  switch (_level) {
    case "years": return _renderYears();
    case "months": return _renderMonths();
    case "weeks": return _renderWeeks();
    case "days": return _renderDays();
    default: return "";
  }
}

function _update() {
  if (!_container) return;

  const totalCount = _allFiles.length;
  let html = `
    <div class="page-header">
      <h2>${t("timeline.title")} <span class="header-count">${t("timeline.dated_files", { count: totalCount })}</span></h2>
      <button class="btn btn-secondary tl-gap-btn">${t("timeline.gap_analysis")}</button>
    </div>
    ${_renderBreadcrumb()}
    <div class="tl-content">
      ${_renderContent()}
    </div>`;

  _container.innerHTML = html;
  _bindEvents();
  _lazyLoad();
}

function _bindEvents() {
  // Gap analysis button
  const gapBtn = _container.querySelector(".tl-gap-btn");
  if (gapBtn) gapBtn.addEventListener("click", () => _openGapAnalysis());

  // Breadcrumb navigation
  _container.querySelectorAll(".tl-bread-item").forEach(el => {
    el.addEventListener("click", () => {
      const nav = el.dataset.nav;
      if (nav === "years") {
        _level = "years"; _filterYear = null; _filterMonth = null; _filterWeek = null;
      } else if (nav === "months") {
        _level = "months"; _filterMonth = null; _filterWeek = null;
      } else if (nav === "weeks") {
        _level = "weeks"; _filterWeek = null;
      }
      _update();
    });
  });

  // Year cards
  _container.querySelectorAll(".tl-card-year").forEach(card => {
    card.addEventListener("click", () => {
      _filterYear = card.dataset.year;
      _filterMonth = null;
      _filterWeek = null;
      _level = "months";
      _update();
    });
  });

  // Month cards
  _container.querySelectorAll(".tl-card-month").forEach(card => {
    card.addEventListener("click", () => {
      _filterMonth = card.dataset.month;
      _filterWeek = null;
      _level = "weeks";
      _update();
    });
  });

  // Week cards
  _container.querySelectorAll(".tl-card-week").forEach(card => {
    card.addEventListener("click", () => {
      _filterWeek = card.dataset.week;
      _level = "days";
      _update();
    });
  });

  // File items — open lightbox for images/videos
  const allItems = _container.querySelectorAll("[data-file-path]");
  const allPaths = Array.from(allItems).map(el => el.dataset.filePath);
  const lightboxPaths = allPaths.filter(p => {
    const ext = (p.split(".").pop() || "").toLowerCase();
    return IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
  });

  allItems.forEach(item => {
    const handler = (e) => {
      if (e.type === "keydown" && e.key !== "Enter") return;
      const filePath = item.dataset.filePath;
      const idx = lightboxPaths.indexOf(filePath);
      if (idx >= 0) {
        openLightbox(lightboxPaths, idx);
      } else {
        showFileDetail(filePath);
      }
    };
    item.addEventListener("click", handler);
    item.addEventListener("keydown", handler);
  });
}

function _lazyLoad() {
  const images = _container.querySelectorAll(".tl-lazy");
  if (!images.length) return;
  if ("IntersectionObserver" in window) {
    const obs = new IntersectionObserver((entries) => {
      for (const e of entries) {
        if (e.isIntersecting) {
          e.target.src = e.target.dataset.src;
          e.target.classList.remove("tl-lazy");
          obs.unobserve(e.target);
        }
      }
    }, { rootMargin: "300px" });
    images.forEach(img => obs.observe(img));
  } else {
    images.forEach(img => { img.src = img.dataset.src; });
  }
}

// ── Gap Analysis ──

async function _openGapAnalysis() {
  // Fetch gap data from API
  let data;
  try {
    data = await api("/timeline/gaps");
  } catch (e) {
    return;
  }

  const { months, gaps, coverage } = data;
  if (!months.length) return;

  // Build year→month map for heatmap
  const yearMap = {};
  let maxCount = 0;
  for (const m of months) {
    if (!yearMap[m.year]) yearMap[m.year] = {};
    yearMap[m.year][m.month] = m.count;
    if (m.count > maxCount) maxCount = m.count;
  }
  const years = Object.keys(yearMap).map(Number).sort();

  // Heatmap color class
  function _cellClass(count) {
    if (count === 0) return "";
    if (maxCount <= 1) return "heatmap-max";
    const ratio = count / maxCount;
    if (ratio >= 0.7) return "heatmap-max";
    if (ratio >= 0.4) return "heatmap-high";
    if (ratio >= 0.15) return "heatmap-mid";
    return "heatmap-low";
  }

  // Month name for gap description
  function _monthName(ym) {
    const [y, m] = ym.split("-");
    return `${MONTH_NAMES_FULL_CS[parseInt(m) - 1]} ${y}`;
  }

  // Build heatmap HTML
  let heatmapHtml = `<div class="timeline-heatmap">`;
  // Header row
  heatmapHtml += `<div class="heatmap-header"></div>`;
  for (let i = 0; i < 12; i++) {
    heatmapHtml += `<div class="heatmap-header">${MONTH_NAMES_CS[i]}</div>`;
  }
  // Data rows
  for (const year of years) {
    heatmapHtml += `<div class="heatmap-year">${year}</div>`;
    for (let m = 1; m <= 12; m++) {
      const count = yearMap[year]?.[m];
      if (count === undefined) {
        // Month outside the tracked range
        heatmapHtml += `<div class="heatmap-cell heatmap-empty" title="${MONTH_NAMES_CS[m - 1]} ${year}">–</div>`;
      } else {
        const cls = _cellClass(count);
        heatmapHtml += `<div class="heatmap-cell ${cls}" data-count="${count}" title="${MONTH_NAMES_CS[m - 1]} ${year}: ${count} souborů">${count}</div>`;
      }
    }
  }
  heatmapHtml += `</div>`;

  // Gap list HTML
  let gapHtml = `<ul class="gap-list">`;
  if (gaps.length === 0) {
    gapHtml += `<li class="gap-item gap-item-no-gaps">${t("timeline.no_gaps")}</li>`;
  } else {
    for (const g of gaps) {
      const fromName = _monthName(g.from);
      const toName = _monthName(g.to);
      const desc = g.months === 1
        ? `${fromName} (1 měsíc bez dat)`
        : `${fromName} – ${toName} (${g.months} měsíců bez dat)`;
      gapHtml += `<li class="gap-item">${escapeHtml(desc)}</li>`;
    }
  }
  gapHtml += `</ul>`;

  // Coverage summary HTML
  const covHtml = `
    <div class="coverage-summary">
      <div class="coverage-summary-pct">${coverage.coverage_pct}%</div>
      <div class="coverage-summary-detail">
        <div>${t("timeline.coverage")}: ${t("timeline.months_covered", { covered: coverage.covered_months, total: coverage.total_months })}</div>
        <div style="margin-top:2px;font-size:12px;color:var(--text-muted)">${coverage.first_date} – ${coverage.last_date}</div>
      </div>
    </div>`;

  // Create overlay
  const overlay = document.createElement("div");
  overlay.className = "tl-gap-overlay";
  overlay.innerHTML = `
    <div class="tl-gap-panel">
      <div class="tl-gap-header">
        <h3>${t("timeline.gap_analysis")}</h3>
        <button class="tl-gap-close" title="${t("general.close")}">&times;</button>
      </div>
      ${heatmapHtml}
      ${gapHtml}
      ${covHtml}
    </div>`;

  document.body.appendChild(overlay);

  // Close handlers
  const close = () => overlay.remove();
  overlay.querySelector(".tl-gap-close").addEventListener("click", close);
  overlay.addEventListener("click", (e) => {
    if (e.target === overlay) close();
  });
  const escHandler = (e) => {
    if (e.key === "Escape") { close(); document.removeEventListener("keydown", escHandler); }
  };
  document.addEventListener("keydown", escHandler);
}

// ── Entry point ──

export async function render(container) {
  _container = container;

  try {
    const data = await api("/files?limit=10000");
    // Ensure every file has a parseable date
    _allFiles = data.files.filter(f => {
      if (f.date_original) return true;
      const ts = f.birthtime || f.mtime;
      if (ts) {
        const d = new Date(ts * 1000);
        f.date_original = `${d.getFullYear()}:${String(d.getMonth()+1).padStart(2,"0")}:${String(d.getDate()).padStart(2,"0")} ${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}:${String(d.getSeconds()).padStart(2,"0")}`;
        return true;
      }
      return false;
    });

    if (!_allFiles.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("timeline.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon">&#128197;</div>
          <h3 class="empty-state-title">${t("timeline.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("timeline.empty_hint")}</p>
        </div>`;
      return;
    }

    // Reset to year view on fresh render
    _level = "years";
    _filterYear = null;
    _filterMonth = null;
    _filterWeek = null;
    _update();

  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("timeline.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
