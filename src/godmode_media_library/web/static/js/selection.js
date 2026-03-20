/* GOD MODE Media Library — Selection state management */

import { $, escapeHtml, showToast } from "./utils.js";
import { apiPost } from "./api.js";
import { t } from "./i18n.js";

const selectedPaths = new Set();

export function toggleSelect(path) {
  if (selectedPaths.has(path)) {
    selectedPaths.delete(path);
  } else {
    selectedPaths.add(path);
  }
  updateActionBar();
  updateCheckboxes();
}

export function selectAll(paths) {
  for (const p of paths) selectedPaths.add(p);
  updateActionBar();
  updateCheckboxes();
}

export function deselectAll() {
  selectedPaths.clear();
  updateActionBar();
  updateCheckboxes();
}

export function isSelected(path) {
  return selectedPaths.has(path);
}

export function getSelectedCount() {
  return selectedPaths.size;
}

export function getSelectedPaths() {
  return Array.from(selectedPaths);
}

function updateCheckboxes() {
  document.querySelectorAll("[data-select-path]").forEach(cb => {
    cb.checked = selectedPaths.has(cb.dataset.selectPath);
  });
}

function updateActionBar() {
  let bar = $("#floating-action-bar");
  if (!bar) return;

  if (selectedPaths.size === 0) {
    bar.classList.add("hidden");
    return;
  }

  bar.classList.remove("hidden");
  bar.innerHTML = `
    <span class="fab-count">${t("general.selected", { count: selectedPaths.size })}</span>
    <button class="fab-btn" data-action="quarantine" title="${t("action.quarantine")}">&#128451; ${t("action.quarantine")}</button>
    <button class="fab-btn danger" data-action="delete" title="${t("action.delete")}">&#128465; ${t("action.delete")}</button>
    <button class="fab-btn" data-action="deselect" title="${t("action.deselect_all")}">&#10060; ${t("action.deselect_all")}</button>
  `;

  bar.querySelector('[data-action="quarantine"]').addEventListener("click", async () => {
    if (!confirm(t("confirm.quarantine", { count: selectedPaths.size }))) return;
    try {
      const result = await apiPost("/files/quarantine", { paths: getSelectedPaths() });
      showToast(`Karanténováno: ${result.moved}, přeskočeno: ${result.skipped}`, "success");
      deselectAll();
    } catch (err) {
      showToast(t("general.error", { message: err.message }), "error");
    }
  });

  bar.querySelector('[data-action="delete"]').addEventListener("click", async () => {
    if (!confirm(t("confirm.delete", { count: selectedPaths.size }))) return;
    try {
      const result = await apiPost("/files/delete", { paths: getSelectedPaths() });
      showToast(`Smazáno: ${result.deleted}, přeskočeno: ${result.skipped}`, "success");
      deselectAll();
    } catch (err) {
      showToast(t("general.error", { message: err.message }), "error");
    }
  });

  bar.querySelector('[data-action="deselect"]').addEventListener("click", deselectAll);
}
