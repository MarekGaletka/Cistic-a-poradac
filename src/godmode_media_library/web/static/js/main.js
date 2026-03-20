/* GOD MODE Media Library — Main entry point (ES module) */

import { t } from "./i18n.js";
import { $, $$, content } from "./utils.js";
import { closeAllModals } from "./modal.js";
import { cleanupTasks, openTaskDrawer, closeTaskDrawer } from "./tasks.js";
import { cleanup as cleanupMap } from "./pages/map.js";

// Page modules — lazy imports for clarity
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
  pipeline,
  doctor,
};

let _currentPage = null;

function cleanupCurrentPage() {
  cleanupTasks();
  if (_currentPage === "map") cleanupMap();
}

function navigate(page) {
  if (!pages[page]) page = "dashboard";
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

// ── Sidebar toggle (hamburger) ──────────────────────

document.addEventListener("DOMContentLoaded", () => {
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

  // Task drawer toggle
  const taskBtn = $("#task-drawer-toggle");
  if (taskBtn) {
    taskBtn.addEventListener("click", () => {
      const drawer = $("#task-drawer");
      if (drawer && drawer.classList.contains("open")) {
        closeTaskDrawer();
      } else {
        openTaskDrawer();
      }
    });
  }

  const taskClose = $("#task-drawer-close");
  if (taskClose) {
    taskClose.addEventListener("click", closeTaskDrawer);
  }

  // Hash-based routing
  window.addEventListener("hashchange", () => navigate(location.hash.slice(1) || "dashboard"));
  navigate(location.hash.slice(1) || "dashboard");
});

// ── Keyboard shortcuts ──────────────────────────────

document.addEventListener("keydown", e => {
  // Escape closes modals/drawers
  if (e.key === "Escape") {
    closeAllModals();
    closeTaskDrawer();
    return;
  }

  // Don't intercept when typing in input/textarea
  if (e.target.matches("input, textarea, select")) return;

  // "/" focuses search (if on files page)
  if (e.key === "/" && !e.ctrlKey && !e.metaKey) {
    const searchInput = $("#f-ext") || $("#f-path");
    if (searchInput) {
      e.preventDefault();
      searchInput.focus();
    }
  }

  // Enter on table rows
  if (e.key === "Enter" && e.target.matches("tr[role='button'], [role='button']")) {
    e.target.click();
  }

  // "?" shows keyboard shortcuts help (future)
  // Arrow keys for pagination (future)
});
