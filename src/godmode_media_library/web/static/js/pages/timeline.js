/* GOD MODE Media Library — Timeline page (scrubber + sticky headers) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm"]);

const INITIAL_ITEMS = 20;

// Track expanded months
let _expandedMonths = new Set();
let _scrubberFadeTimer = null;
let _monthObserver = null;

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

function _buildScrubber(years, container) {
  // Create the scrubber bar
  const scrubber = document.createElement("div");
  scrubber.className = "timeline-scrubber";
  scrubber.innerHTML = `
    <div class="timeline-scrubber-track">
      <div class="timeline-scrubber-thumb"></div>
      ${years.map(y => `<div class="timeline-scrubber-label" data-year="${y}" title="${t("timeline.scrubber_label", { year: y })}">${y}</div>`).join("")}
    </div>
  `;

  // Click on year label to scroll
  scrubber.querySelectorAll(".timeline-scrubber-label").forEach(label => {
    label.addEventListener("click", () => {
      const year = label.dataset.year;
      const target = container.querySelector(`[data-year="${year}"]`);
      if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

  // Drag to scrub
  let isDragging = false;
  const track = scrubber.querySelector(".timeline-scrubber-track");

  const scrubToPosition = (clientY) => {
    const rect = track.getBoundingClientRect();
    const pct = Math.max(0, Math.min(1, (clientY - rect.top) / rect.height));
    const labels = scrubber.querySelectorAll(".timeline-scrubber-label");
    if (!labels.length) return;
    const idx = Math.min(Math.floor(pct * labels.length), labels.length - 1);
    const year = labels[idx].dataset.year;
    const target = container.querySelector(`[data-year="${year}"]`);
    if (target) target.scrollIntoView({ behavior: "auto", block: "start" });
  };

  track.addEventListener("mousedown", (e) => {
    isDragging = true;
    scrubToPosition(e.clientY);
    e.preventDefault();
  });

  document.addEventListener("mousemove", (e) => {
    if (isDragging) {
      scrubToPosition(e.clientY);
      e.preventDefault();
    }
  });

  document.addEventListener("mouseup", () => { isDragging = false; });

  // Touch support
  track.addEventListener("touchstart", (e) => {
    isDragging = true;
    scrubToPosition(e.touches[0].clientY);
    e.preventDefault();
  }, { passive: false });

  document.addEventListener("touchmove", (e) => {
    if (isDragging) {
      scrubToPosition(e.touches[0].clientY);
    }
  }, { passive: true });

  document.addEventListener("touchend", () => { isDragging = false; });

  return scrubber;
}

function _setupScrollTracking(container, scrubber, sortedMonths) {
  // Observe month headers to update scrubber active state
  if (_monthObserver) _monthObserver.disconnect();

  const monthEls = container.querySelectorAll(".timeline-month");
  if (!monthEls.length) return;

  _monthObserver = new IntersectionObserver((entries) => {
    // Find which months are visible
    const visibleYears = new Set();
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const year = entry.target.dataset.year;
        if (year) visibleYears.add(year);
      }
    });

    // Also check which months are in viewport right now
    const allMonths = container.querySelectorAll(".timeline-month");
    const activeYears = new Set();
    allMonths.forEach(m => {
      const rect = m.getBoundingClientRect();
      if (rect.top < window.innerHeight && rect.bottom > 0) {
        activeYears.add(m.dataset.year);
      }
    });

    // Update scrubber labels
    const labels = scrubber.querySelectorAll(".timeline-scrubber-label");
    labels.forEach(label => {
      label.classList.toggle("active", activeYears.has(label.dataset.year));
    });

    // Update thumb position
    const thumb = scrubber.querySelector(".timeline-scrubber-thumb");
    const track = scrubber.querySelector(".timeline-scrubber-track");
    if (thumb && track && labels.length) {
      // Find first active label
      let firstActiveIdx = -1;
      labels.forEach((l, i) => {
        if (activeYears.has(l.dataset.year) && firstActiveIdx === -1) firstActiveIdx = i;
      });
      if (firstActiveIdx >= 0) {
        const pct = firstActiveIdx / Math.max(labels.length - 1, 1);
        const trackH = track.offsetHeight;
        thumb.style.top = (pct * (trackH - 20)) + "px";
      }
    }

    // Show scrubber on scroll, fade after inactivity
    scrubber.classList.add("visible");
    clearTimeout(_scrubberFadeTimer);
    _scrubberFadeTimer = setTimeout(() => {
      scrubber.classList.remove("visible");
    }, 2000);

  }, { rootMargin: "0px", threshold: 0.1 });

  monthEls.forEach(el => _monthObserver.observe(el));

  // Also show scrubber on scroll
  const mainEl = document.querySelector("main") || document.documentElement;
  const scrollHandler = () => {
    scrubber.classList.add("visible");
    clearTimeout(_scrubberFadeTimer);
    _scrubberFadeTimer = setTimeout(() => {
      scrubber.classList.remove("visible");
    }, 2000);
  };
  window.addEventListener("scroll", scrollHandler, { passive: true });

  // Show on hover
  scrubber.addEventListener("mouseenter", () => {
    scrubber.classList.add("visible");
    clearTimeout(_scrubberFadeTimer);
  });
  scrubber.addEventListener("mouseleave", () => {
    _scrubberFadeTimer = setTimeout(() => {
      scrubber.classList.remove("visible");
    }, 1000);
  });
}

export async function render(container) {
  if (_monthObserver) { _monthObserver.disconnect(); _monthObserver = null; }

  try {
    const data = await api("/files?limit=5000");
    // Use date_original, fallback to birthtime/mtime (converted to date string)
    const files = data.files.filter(f => {
      if (f.date_original) return true;
      // Convert numeric timestamps to date string
      const ts = f.birthtime || f.mtime;
      if (ts) {
        const d = new Date(ts * 1000);
        f.date_original = `${d.getFullYear()}:${String(d.getMonth()+1).padStart(2,"0")}:${String(d.getDate()).padStart(2,"0")} ${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}:${String(d.getSeconds()).padStart(2,"0")}`;
        return true;
      }
      return false;
    });

    if (!files.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("timeline.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon">&#128197;</div>
          <h3 class="empty-state-title">${t("timeline.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("timeline.empty_hint")}</p>
          <button class="empty-state-action-btn" id="btn-timeline-empty-pipeline">${t("timeline.empty_action")}</button>
        </div>`;
      const pipelineBtn = container.querySelector("#btn-timeline-empty-pipeline");
      if (pipelineBtn) {
        pipelineBtn.addEventListener("click", () => {
          const settingsBtn = document.querySelector("#btn-settings");
          if (settingsBtn) settingsBtn.click();
        });
      }
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
        <div class="timeline-month-header">${escapeHtml(monthName)} <span class="timeline-count">(${monthFiles.length})</span></div>
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

    // Add timeline scrubber
    if (years.length > 0) {
      const scrubber = _buildScrubber(years, container);
      container.appendChild(scrubber);
      _setupScrollTracking(container, scrubber, sortedMonths);
    }

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
