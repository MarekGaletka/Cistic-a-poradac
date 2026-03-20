/* GOD MODE Media Library — Timeline page (upgraded) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

const INITIAL_ITEMS = 20;

// Track expanded months
let _expandedMonths = new Set();

function _renderFileItem(f) {
  const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
  const thumb = isImage
    ? `<img data-src="/api/thumbnail${encodeURI(f.path)}?size=150" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}" class="timeline-lazy">`
    : `<div class="timeline-icon">${escapeHtml(f.ext)}</div>`;
  return `<div class="timeline-item" tabindex="0" role="button" data-file-path="${escapeHtml(f.path)}" title="${escapeHtml(f.path)}">
    ${thumb}
    <div class="timeline-name">${escapeHtml(fileName(f.path))}</div>
  </div>`;
}

function _groupByDay(files) {
  const days = {};
  for (const f of files) {
    const date = f.date_original || "";
    const dayMatch = date.match(/^(\d{4})[:\-/](\d{2})[:\-/](\d{2})/);
    const dayKey = dayMatch ? `${dayMatch[1]}-${dayMatch[2]}-${dayMatch[3]}` : "unknown";
    if (!days[dayKey]) days[dayKey] = [];
    days[dayKey].push(f);
  }
  return days;
}

export async function render(container) {
  try {
    const data = await api("/files?limit=5000");
    const files = data.files.filter(f => f.date_original);

    if (!files.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("timeline.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon" style="font-size:48px">&#128197;</div>
          <h3 class="empty-state-title">${t("timeline.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("timeline.empty_hint")}</p>
        </div>`;
      return;
    }

    // Group files by month
    const groups = {};
    for (const f of files) {
      const date = f.date_original;
      const match = date.match(/^(\d{4})[:\-/](\d{2})/);
      const key = match ? `${match[1]}-${match[2]}` : "Unknown";
      if (!groups[key]) groups[key] = [];
      groups[key].push(f);
    }

    const sortedMonths = Object.keys(groups).sort().reverse();

    // Extract unique years
    const years = [...new Set(sortedMonths.map(m => m.split("-")[0]).filter(y => y !== "Unknown"))].sort().reverse();

    let html = `
      <div class="page-header">
        <h2>${t("timeline.title")} <span class="header-count">${t("timeline.dated_files", { count: files.length })}</span></h2>
      </div>`;

    // Year tabs
    if (years.length > 1) {
      html += `<div class="timeline-year-bar" style="display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap;padding:8px 0">`;
      for (const year of years) {
        const yearCount = sortedMonths.filter(m => m.startsWith(year)).reduce((sum, m) => sum + groups[m].length, 0);
        html += `<button class="timeline-year-btn" data-year="${year}" style="padding:6px 14px;border:1px solid var(--border);border-radius:16px;background:var(--surface);cursor:pointer;font-size:13px;font-weight:600;transition:all 0.2s">${t("timeline.year", { year })} <span style="font-weight:400;opacity:0.7">(${yearCount})</span></button>`;
      }
      html += `</div>`;
    }

    html += '<div class="timeline">';

    for (const month of sortedMonths) {
      const monthFiles = groups[month];
      const [y, m] = month.split("-");
      const monthName = m ? new Date(parseInt(y), parseInt(m) - 1).toLocaleDateString("cs", { year: "numeric", month: "long" }) : month;
      const isExpanded = _expandedMonths.has(month);
      const displayFiles = isExpanded ? monthFiles : monthFiles.slice(0, INITIAL_ITEMS);

      html += `<div class="timeline-month" id="month-${month}" data-year="${y}">
        <div class="timeline-header">${escapeHtml(monthName)} <span class="timeline-count">(${monthFiles.length})</span></div>
        <div class="timeline-grid" id="grid-${month}">`;

      if (isExpanded) {
        // Show with day grouping
        const days = _groupByDay(monthFiles);
        const sortedDays = Object.keys(days).sort().reverse();
        for (const day of sortedDays) {
          const dayDate = day !== "unknown" ? new Date(day).toLocaleDateString("cs", { day: "numeric", month: "long" }) : t("timeline.no_date");
          html += `<div class="timeline-day-header" style="width:100%;font-size:12px;font-weight:600;color:var(--text-muted);padding:8px 0 4px;border-bottom:1px solid var(--border);margin-bottom:8px">${escapeHtml(dayDate)} <span style="font-weight:400">(${days[day].length})</span></div>`;
          for (const f of days[day]) {
            html += _renderFileItem(f);
          }
        }
      } else {
        for (const f of displayFiles) {
          html += _renderFileItem(f);
        }
      }

      html += '</div>';

      if (monthFiles.length > INITIAL_ITEMS) {
        if (isExpanded) {
          html += `<button class="timeline-toggle-btn" data-month="${month}" data-action="collapse" style="margin:8px 0 4px;padding:6px 14px;background:var(--surface);border:1px solid var(--border);border-radius:6px;cursor:pointer;font-size:12px;color:var(--text-muted)">${t("timeline.collapse")}</button>`;
        } else {
          html += `<button class="timeline-toggle-btn" data-month="${month}" data-action="expand" style="margin:8px 0 4px;padding:6px 14px;background:var(--surface);border:1px solid var(--border);border-radius:6px;cursor:pointer;font-size:12px;color:var(--text-muted)">${t("timeline.show_all", { count: monthFiles.length })}</button>`;
        }
      }

      html += '</div>';
    }

    html += '</div>';
    container.innerHTML = html;

    // Lazy load images with IntersectionObserver
    const lazyImages = container.querySelectorAll(".timeline-lazy");
    if (lazyImages.length && "IntersectionObserver" in window) {
      const observer = new IntersectionObserver((entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            const img = entry.target;
            img.src = img.dataset.src;
            img.classList.remove("timeline-lazy");
            observer.unobserve(img);
          }
        }
      }, { rootMargin: "200px" });
      lazyImages.forEach(img => observer.observe(img));
    } else {
      // Fallback: load all
      lazyImages.forEach(img => { img.src = img.dataset.src; });
    }

    // Bind click events on file items — open lightbox for images/videos
    // Collect all visible file paths for lightbox navigation
    const allVisibleItems = container.querySelectorAll("[data-file-path]");
    const allVisiblePaths = Array.from(allVisibleItems).map(el => el.dataset.filePath);
    const lightboxPaths = allVisiblePaths.filter(p => {
      const ext = (p.split(".").pop() || "").toLowerCase();
      return IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
    });

    allVisibleItems.forEach(item => {
      const handler = (e) => {
        if (e.type === "keydown" && e.key !== "Enter") return;
        const filePath = item.dataset.filePath;
        const lbIndex = lightboxPaths.indexOf(filePath);
        if (lbIndex >= 0) {
          openLightbox(lightboxPaths, lbIndex);
        } else {
          showFileDetail(filePath);
        }
      };
      item.addEventListener("click", handler);
      item.addEventListener("keydown", handler);
    });

    // Bind year tab buttons
    container.querySelectorAll(".timeline-year-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        const year = btn.dataset.year;
        const target = container.querySelector(`[data-year="${year}"]`);
        if (target) {
          target.scrollIntoView({ behavior: "smooth", block: "start" });
          // Highlight active year
          container.querySelectorAll(".timeline-year-btn").forEach(b => b.style.background = "var(--surface)");
          btn.style.background = "var(--accent)";
          btn.style.color = "#fff";
          btn.style.borderColor = "var(--accent)";
          setTimeout(() => {
            btn.style.background = "var(--surface)";
            btn.style.color = "";
            btn.style.borderColor = "";
          }, 1500);
        }
      });
    });

    // Bind expand/collapse buttons
    container.querySelectorAll(".timeline-toggle-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        const month = btn.dataset.month;
        if (btn.dataset.action === "expand") {
          _expandedMonths.add(month);
        } else {
          _expandedMonths.delete(month);
        }
        render(container);
        // Scroll back to that month after re-render
        setTimeout(() => {
          const target = container.querySelector(`#month-${month}`);
          if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
        }, 50);
      });
    });
  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("timeline.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
