/* GOD MODE Media Library — Main entry point (ES module) */

import { t } from "./i18n.js";
import { $, $$, content, showToast } from "./utils.js";
import { closeAllModals } from "./modal.js";
import { closeLightbox } from "./lightbox.js";
import { cleanupTasks } from "./tasks.js";
import { cleanup as cleanupMap } from "./pages/map.js";
import { initGlobalProgress } from "./tasks.js";

// Page modules
import * as dashboard from "./pages/dashboard.js";
import * as files from "./pages/files.js";
import * as duplicates from "./pages/duplicates.js";
import * as similar from "./pages/similar.js";
import * as timeline from "./pages/timeline.js";
import * as map from "./pages/map.js";
import * as pipeline from "./pages/pipeline.js";
import * as doctor from "./pages/doctor.js";

// ── Router ──────────────────────────────────────────

const pages = {
  dashboard,
  files,
  duplicates,
  similar,
  timeline,
  map,
};

let _currentPage = null;

function cleanupCurrentPage() {
  cleanupTasks();
  if (_currentPage === "map") cleanupMap();
}

export function navigate(page) {
  if (!pages[page]) page = "dashboard";
  closeSettingsPanel();
  cleanupCurrentPage();
  _currentPage = page;

  $$("nav a[data-page]").forEach(a => {
    const isActive = a.dataset.page === page;
    a.classList.toggle("active", isActive);
    if (isActive) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });

  const c = content();
  c.innerHTML = `<div class="loading"><div class="spinner" role="status" aria-label="${t("general.loading")}"></div>${t("general.loading")}</div>`;
  c.classList.remove("page-enter");
  // Force reflow to restart animation
  void c.offsetWidth;
  c.classList.add("page-enter");
  pages[page].render(c);
}

// ── Settings panel ──────────────────────────────────

let _settingsRendered = false;

function openSettingsPanel() {
  const panel = $("#settings-panel");
  const overlay = $("#settings-overlay");
  if (panel) {
    panel.classList.add("open");
    panel.setAttribute("aria-hidden", "false");
  }
  if (overlay) overlay.classList.remove("hidden");

  if (!_settingsRendered) {
    renderSettingsContent();
    _settingsRendered = true;
  }
}

function closeSettingsPanel() {
  const panel = $("#settings-panel");
  const overlay = $("#settings-overlay");
  if (panel) {
    panel.classList.remove("open");
    panel.setAttribute("aria-hidden", "true");
  }
  if (overlay) overlay.classList.add("hidden");
}

async function renderSettingsContent() {
  const container = $("#settings-panel-content");
  if (!container) return;

  let html = "";

  // Pipeline section
  html += `<div class="settings-section">
    <h4 class="settings-section-title">${t("settings.pipeline_section")}</h4>
    <div id="settings-pipeline"></div>
  </div>`;

  // Doctor section
  html += `<div class="settings-section">
    <h4 class="settings-section-title">${t("settings.doctor_section")}</h4>
    <div id="settings-doctor"></div>
  </div>`;

  // About section
  html += `<div class="settings-section">
    <h4 class="settings-section-title">${t("settings.about_section")}</h4>
    <p style="font-size:13px;color:var(--text-muted);line-height:1.5">${t("settings.about_text")}</p>
  </div>`;

  container.innerHTML = html;

  // Render pipeline and doctor into their containers
  const pipelineContainer = $("#settings-pipeline");
  const doctorContainer = $("#settings-doctor");
  if (pipelineContainer) pipeline.render(pipelineContainer);
  if (doctorContainer) doctor.render(doctorContainer);
}

// ── Nav badges ──────────────────────────────────────

async function updateDuplicateBadge() {
  try {
    const { api } = await import("./api.js");
    const data = await api("/duplicates?limit=1");
    const badge = $("#dup-badge");
    if (badge) {
      if (data.total_groups > 0) {
        badge.textContent = data.total_groups;
        badge.classList.remove("hidden");
      } else {
        badge.classList.add("hidden");
      }
    }
  } catch {
    // Silent fail — badge is optional
  }
}

async function updateNavBadges() {
  try {
    const { api } = await import("./api.js");
    const [stats, similar] = await Promise.all([
      api("/stats").catch(() => null),
      api("/similar?threshold=10&limit=1").catch(() => null),
    ]);

    // Files badge
    const filesBadge = $("#files-badge");
    if (filesBadge && stats && stats.total_files > 0) {
      filesBadge.textContent = stats.total_files > 999
        ? Math.round(stats.total_files / 1000) + "k"
        : stats.total_files;
      filesBadge.classList.remove("hidden");
    }

    // Similar badge
    const similarBadge = $("#similar-badge");
    if (similarBadge && similar && similar.total_pairs > 0) {
      similarBadge.textContent = similar.total_pairs;
      similarBadge.classList.remove("hidden");
    }
  } catch {
    // Silent fail
  }
}

// ── Init ────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  // Nav toggle (hamburger)
  const navToggle = $(".nav-toggle");
  if (navToggle) {
    navToggle.addEventListener("click", () => {
      const nav = $("nav");
      const isOpen = nav.classList.toggle("open");
      navToggle.setAttribute("aria-expanded", isOpen);
    });
  }

  // Navigation link clicks
  document.addEventListener("click", e => {
    const link = e.target.closest("nav a[data-page]");
    if (link) {
      e.preventDefault();
      const page = link.dataset.page;
      location.hash = page;
      navigate(page);
      $("nav").classList.remove("open");
      $(".nav-toggle")?.setAttribute("aria-expanded", "false");
    }
  });

  // Settings button
  const settingsBtn = $("#btn-settings");
  if (settingsBtn) {
    settingsBtn.addEventListener("click", () => {
      const panel = $("#settings-panel");
      if (panel && panel.classList.contains("open")) {
        closeSettingsPanel();
      } else {
        openSettingsPanel();
      }
    });
  }

  // Settings close button
  const settingsClose = $("#settings-panel-close");
  if (settingsClose) {
    settingsClose.addEventListener("click", closeSettingsPanel);
  }

  // Settings overlay click
  const settingsOverlay = $("#settings-overlay");
  if (settingsOverlay) {
    settingsOverlay.addEventListener("click", closeSettingsPanel);
  }

  // Init global progress bar
  initGlobalProgress();

  // Hash-based routing
  window.addEventListener("hashchange", () => navigate(location.hash.slice(1) || "dashboard"));
  navigate(location.hash.slice(1) || "dashboard");

  // Update nav badges
  updateDuplicateBadge();
  updateNavBadges();
});

// ── Drag & drop folder support ──────────────────────

document.addEventListener("dragover", (e) => {
  e.preventDefault();
  document.body.classList.add("drag-over");
});

document.addEventListener("dragleave", (e) => {
  if (e.relatedTarget === null) document.body.classList.remove("drag-over");
});

document.addEventListener("drop", async (e) => {
  e.preventDefault();
  document.body.classList.remove("drag-over");
  const items = e.dataTransfer?.items;
  if (!items) return;
  const folderPaths = [];
  for (const item of items) {
    if (item.kind === "file") {
      const entry = item.webkitGetAsEntry ? item.webkitGetAsEntry() : null;
      const file = item.getAsFile();
      if (entry && entry.isDirectory) {
        // Directory entry — use the path from file if available
        if (file && file.path) folderPaths.push(file.path);
        else folderPaths.push("/" + entry.fullPath.replace(/^\//, ""));
      } else if (file && file.path) {
        // Single file — extract parent directory
        const dir = file.path.replace(/\/[^/]+$/, "");
        if (dir && !folderPaths.includes(dir)) folderPaths.push(dir);
      }
    }
  }
  if (folderPaths.length > 0) {
    try {
      const { apiPost } = await import("./api.js");
      const existing = await (await import("./api.js")).api("/roots");
      const merged = [...new Set([...(existing.roots || []), ...folderPaths])];
      await apiPost("/roots", { roots: merged });
      showToast(t("folder.add_folder") + ": " + folderPaths.join(", "), "success");
      navigate(_currentPage || "dashboard");
    } catch (err) {
      showToast(t("general.error", { message: err.message }), "error");
    }
  }
});

// ── Keyboard shortcuts ──────────────────────────────

const _pageKeys = { "1": "dashboard", "2": "files", "3": "duplicates", "4": "similar", "5": "timeline", "6": "map" };

function showShortcutsModal() {
  closeAllModals();
  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";
  overlay.setAttribute("role", "dialog");
  overlay.innerHTML = `<div class="shortcuts-modal">
    <h3>${t("shortcuts.title")}</h3>
    <div class="shortcuts-row"><span>${t("shortcuts.navigate")}</span><span class="shortcuts-key">1-6</span></div>
    <div class="shortcuts-row"><span>${t("shortcuts.search")}</span><span class="shortcuts-key">/</span></div>
    <div class="shortcuts-row"><span>${t("shortcuts.close")}</span><span class="shortcuts-key">Esc</span></div>
    <div class="shortcuts-row"><span>${t("shortcuts.help")}</span><span class="shortcuts-key">?</span></div>
  </div>`;
  overlay.addEventListener("click", (ev) => { if (ev.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

document.addEventListener("keydown", e => {
  // Close shortcuts modal on Escape
  const shortcutsOverlay = $(".shortcuts-overlay");
  if (e.key === "Escape") {
    if (shortcutsOverlay) { shortcutsOverlay.remove(); return; }
    closeAllModals();
    closeSettingsPanel();
    return;
  }

  if (e.target.matches("input, textarea, select")) return;

  // Number keys 1-6 navigate to pages
  if (_pageKeys[e.key] && !e.ctrlKey && !e.metaKey && !e.altKey) {
    e.preventDefault();
    const page = _pageKeys[e.key];
    location.hash = page;
    navigate(page);
    return;
  }

  // ? shows shortcuts help
  if (e.key === "?" && !e.ctrlKey && !e.metaKey) {
    e.preventDefault();
    if (shortcutsOverlay) shortcutsOverlay.remove();
    else showShortcutsModal();
    return;
  }

  if (e.key === "/" && !e.ctrlKey && !e.metaKey) {
    const searchInput = $("#f-ext") || $("#f-path");
    if (searchInput) {
      e.preventDefault();
      searchInput.focus();
    }
  }

  if (e.key === "Enter" && e.target.matches("tr[role='button'], [role='button']")) {
    e.target.click();
  }
});

// Export navigate for use by other modules
window._godmodeNavigate = navigate;
