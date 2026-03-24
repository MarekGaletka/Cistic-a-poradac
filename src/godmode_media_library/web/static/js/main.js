/* GOD MODE Media Library — Main entry point (ES module) */

import { t } from "./i18n.js";
import { $, $$, content, showToast } from "./utils.js";
import { closeAllModals } from "./modal.js";
import { closeLightbox } from "./lightbox.js";
import { cleanupTasks } from "./tasks.js";
import { cleanup as cleanupMap } from "./pages/map.js";
import { initGlobalProgress } from "./tasks.js";
import { init as initCommandPalette } from "./command-palette.js";

// Page modules
import * as dashboard from "./pages/dashboard.js";
import * as files from "./pages/files.js";
import * as duplicates from "./pages/duplicates.js";
import * as similar from "./pages/similar.js";
import * as timeline from "./pages/timeline.js";
import * as map from "./pages/map.js";
import * as pipeline from "./pages/pipeline.js";
import * as doctor from "./pages/doctor.js";
import * as recovery from "./pages/recovery.js";
import * as scenarios from "./pages/scenarios.js";
import * as reorganize from "./pages/reorganize.js";
import * as gallery from "./pages/gallery.js";
import * as people from "./pages/people.js";
import * as cloud from "./pages/cloud.js";
import * as backup from "./pages/backup.js";

// ── Router ──────────────────────────────────────────

const pages = {
  dashboard,
  files,
  duplicates,
  similar,
  timeline,
  map,
  gallery,
  recovery,
  scenarios,
  reorganize,
  people,
  cloud,
  backup,
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
let _activeSettingsTab = "pipeline";

const _settingsTabs = [
  { id: "pipeline", icon: "\u{1F527}", label: t("settings.pipeline_section") },
  { id: "dedup", icon: "\u{1F4CB}", label: t("dedup.rules_title") },
  { id: "system", icon: "\u{1F4BB}", label: t("settings.doctor_section") },
  { id: "about", icon: "\u2139\uFE0F", label: t("settings.about_section") },
];

function openSettingsPanel() {
  const panel = $("#settings-panel");
  const overlay = $("#settings-overlay");
  if (panel) {
    panel.classList.add("open");
    panel.setAttribute("aria-hidden", "false");
  }
  if (overlay) overlay.classList.remove("hidden");

  if (!_settingsRendered) {
    renderSettingsTabs();
    switchSettingsTab(_activeSettingsTab);
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

function renderSettingsTabs() {
  const tabsContainer = $("#settings-tabs");
  if (!tabsContainer) return;

  tabsContainer.innerHTML = _settingsTabs.map(tab =>
    `<button class="settings-tab${tab.id === _activeSettingsTab ? " active" : ""}" data-tab="${tab.id}" role="tab" aria-selected="${tab.id === _activeSettingsTab}">${tab.icon} ${tab.label}</button>`
  ).join("");

  tabsContainer.querySelectorAll(".settings-tab").forEach(btn => {
    btn.addEventListener("click", () => switchSettingsTab(btn.dataset.tab));
  });
}

async function switchSettingsTab(tabId) {
  _activeSettingsTab = tabId;
  const container = $("#settings-panel-content");
  if (!container) return;

  // Update tab styles
  $$(".settings-tab").forEach(btn => {
    const isActive = btn.dataset.tab === tabId;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive);
  });

  // Render tab content
  container.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  switch (tabId) {
    case "pipeline":
      container.innerHTML = '<div id="settings-pipeline"></div>';
      pipeline.render($("#settings-pipeline"));
      break;
    case "dedup":
      container.innerHTML = '<div id="settings-dedup-rules"></div>';
      renderDedupRules($("#settings-dedup-rules"));
      break;
    case "system":
      container.innerHTML = '<div id="settings-doctor"></div>';
      doctor.render($("#settings-doctor"));
      break;
    case "about":
      container.innerHTML = `
        <div class="settings-about">
          <div class="settings-about-logo">GOD MODE</div>
          <p class="settings-about-subtitle">Media Library</p>
          <p class="settings-about-version">v0.1.0</p>
          <p class="settings-about-desc">${t("settings.about_text")}</p>
          <div class="settings-about-links">
            <span class="settings-about-link">\u{1F4E6} Python + FastAPI</span>
            <span class="settings-about-link">\u{1F3A8} Vanilla JS</span>
            <span class="settings-about-link">\u{1F5C3}\uFE0F SQLite</span>
          </div>
        </div>`;
      break;
  }
}

async function renderDedupRules(container) {
  const { api, apiPut } = await import("./api.js");

  try {
    const rules = await api("/config/dedup-rules");

    const strategies = [
      { value: "richness", label: t("dedup.strategy_richness"), hint: t("dedup.strategy_hint_richness") },
      { value: "newest", label: t("dedup.strategy_newest"), hint: t("dedup.strategy_hint_newest") },
      { value: "largest", label: t("dedup.strategy_largest"), hint: t("dedup.strategy_hint_largest") },
      { value: "manual", label: t("dedup.strategy_manual"), hint: t("dedup.strategy_hint_manual") },
    ];

    let strategyOptions = "";
    for (const s of strategies) {
      const checked = rules.strategy === s.value ? "checked" : "";
      strategyOptions += `
        <label class="dedup-strategy-option ${checked ? "active" : ""}" data-value="${s.value}">
          <input type="radio" name="dedup-strategy" value="${s.value}" ${checked}>
          <div class="dedup-strategy-content">
            <span class="dedup-strategy-label">${s.label}</span>
            <span class="dedup-strategy-hint">${s.hint}</span>
          </div>
        </label>`;
    }

    container.innerHTML = `
      <div class="dedup-rules-form">
        <div class="dedup-field">
          <label class="dedup-field-label">${t("dedup.strategy")}</label>
          <div class="dedup-strategy-grid">${strategyOptions}</div>
        </div>

        <div class="dedup-field">
          <label class="dedup-field-label">${t("dedup.similarity_threshold")}</label>
          <div class="dedup-slider-row">
            <input type="range" id="dedup-threshold" min="1" max="64" value="${rules.similarity_threshold}" class="dedup-slider">
            <span class="dedup-slider-value" id="dedup-threshold-val">${rules.similarity_threshold}</span>
          </div>
          <span class="dedup-field-hint">${t("dedup.similarity_hint")}</span>
        </div>

        <div class="dedup-field dedup-toggle-row">
          <label class="dedup-toggle-label">
            <input type="checkbox" id="dedup-auto-resolve" ${rules.auto_resolve ? "checked" : ""}>
            <span class="dedup-toggle-switch"></span>
            <span>${t("dedup.auto_resolve")}</span>
          </label>
          <span class="dedup-field-hint">${t("dedup.auto_resolve_hint")}</span>
        </div>

        <div class="dedup-field dedup-toggle-row">
          <label class="dedup-toggle-label">
            <input type="checkbox" id="dedup-merge-metadata" ${rules.merge_metadata ? "checked" : ""}>
            <span class="dedup-toggle-switch"></span>
            <span>${t("dedup.merge_metadata")}</span>
          </label>
          <span class="dedup-field-hint">${t("dedup.merge_metadata_hint")}</span>
        </div>

        <div class="dedup-field">
          <label class="dedup-field-label" for="dedup-quarantine">${t("dedup.quarantine_path")}</label>
          <input type="text" id="dedup-quarantine" class="dedup-input" value="${rules.quarantine_path || ""}" placeholder="~/.config/gml/quarantine">
          <span class="dedup-field-hint">${t("dedup.quarantine_hint")}</span>
        </div>

        <div class="dedup-field">
          <label class="dedup-field-label" for="dedup-exclude-ext">${t("dedup.exclude_extensions")}</label>
          <input type="text" id="dedup-exclude-ext" class="dedup-input" value="${(rules.exclude_extensions || []).join(", ")}" placeholder="tmp, log, ds_store">
          <span class="dedup-field-hint">${t("dedup.exclude_extensions_hint")}</span>
        </div>

        <div class="dedup-field">
          <label class="dedup-field-label" for="dedup-exclude-paths">${t("dedup.exclude_paths")}</label>
          <input type="text" id="dedup-exclude-paths" class="dedup-input" value="${(rules.exclude_paths || []).join(", ")}" placeholder="node_modules, .git, __pycache__">
          <span class="dedup-field-hint">${t("dedup.exclude_paths_hint")}</span>
        </div>

        <div class="dedup-field">
          <label class="dedup-field-label" for="dedup-min-size">${t("dedup.min_file_size")}</label>
          <input type="number" id="dedup-min-size" class="dedup-input dedup-input-small" value="${rules.min_file_size_kb}" min="0" step="1">
          <span class="dedup-field-hint">${t("dedup.min_file_size_hint")}</span>
        </div>

        <button class="primary dedup-save-btn" id="btn-dedup-save">${t("general.save")}</button>
      </div>`;

    // Slider live update
    const slider = container.querySelector("#dedup-threshold");
    const sliderVal = container.querySelector("#dedup-threshold-val");
    if (slider && sliderVal) {
      slider.addEventListener("input", () => { sliderVal.textContent = slider.value; });
    }

    // Strategy radio highlight
    container.querySelectorAll(".dedup-strategy-option").forEach(opt => {
      opt.addEventListener("click", () => {
        container.querySelectorAll(".dedup-strategy-option").forEach(o => o.classList.remove("active"));
        opt.classList.add("active");
        opt.querySelector("input").checked = true;
      });
    });

    // Save button
    container.querySelector("#btn-dedup-save").addEventListener("click", async () => {
      const btn = container.querySelector("#btn-dedup-save");
      btn.disabled = true;
      btn.textContent = "...";

      const strategy = container.querySelector('input[name="dedup-strategy"]:checked')?.value || "richness";
      const excludeExtStr = container.querySelector("#dedup-exclude-ext").value;
      const excludePathStr = container.querySelector("#dedup-exclude-paths").value;

      const body = {
        strategy,
        similarity_threshold: parseInt(container.querySelector("#dedup-threshold").value, 10),
        auto_resolve: container.querySelector("#dedup-auto-resolve").checked,
        merge_metadata: container.querySelector("#dedup-merge-metadata").checked,
        quarantine_path: container.querySelector("#dedup-quarantine").value.trim(),
        exclude_extensions: excludeExtStr ? excludeExtStr.split(",").map(s => s.trim()).filter(Boolean) : [],
        exclude_paths: excludePathStr ? excludePathStr.split(",").map(s => s.trim()).filter(Boolean) : [],
        min_file_size_kb: parseInt(container.querySelector("#dedup-min-size").value, 10) || 0,
      };

      try {
        await apiPut("/config/dedup-rules", body);
        showToast(t("dedup.save_success"), "success");
      } catch (e) {
        showToast(t("dedup.save_error", { message: e.message }), "error");
      } finally {
        btn.disabled = false;
        btn.textContent = t("general.save");
      }
    });
  } catch (e) {
    container.innerHTML = `<div class="empty" style="padding:12px">${t("general.error", { message: e.message })}</div>`;
  }
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

  // Init command palette (Cmd+K)
  initCommandPalette();

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
