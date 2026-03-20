/* GOD MODE Media Library — Main entry point (ES module) */

import { t } from "./i18n.js";
import { $, $$, content } from "./utils.js";
import { closeAllModals } from "./modal.js";
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

// ── Duplicate badge ─────────────────────────────────

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

  // Update duplicate badge
  updateDuplicateBadge();
});

// ── Keyboard shortcuts ──────────────────────────────

document.addEventListener("keydown", e => {
  if (e.key === "Escape") {
    closeAllModals();
    closeSettingsPanel();
    return;
  }

  if (e.target.matches("input, textarea, select")) return;

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
