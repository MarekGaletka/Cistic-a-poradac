/* GOD MODE Media Library — Scenarios page */

import { t } from "../i18n.js";
import { api, apiPost, apiPut, apiDelete } from "../api.js";
import { showToast } from "../utils.js";

let _container = null;
let _scenarios = [];
let _templates = [];
let _stepTypes = {};
let _editingId = null;

const SCENARIO_ICONS = [
  "\u{1F4BD}", "\u{1F4F1}", "\u{1F527}", "\u{1F6D1}", "\u26A1",
  "\u{1F3AC}", "\u{1F4F7}", "\u{1F5C3}\uFE0F", "\u{1F4E6}", "\u{1F50D}",
  "\u{1F6E1}\uFE0F", "\u{1F4BE}", "\u{1F4CB}", "\u2699\uFE0F", "\u{1F680}",
];

const SCENARIO_COLORS = [
  "#58a6ff", "#3fb950", "#d29922", "#f85149", "#a371f7",
  "#f0883e", "#79c0ff", "#56d364", "#e3b341", "#ff7b72",
];

export async function render(container) {
  _container = container;
  container.innerHTML = `<div class="loading"><div class="spinner"></div>${t("general.loading")}</div>`;

  try {
    const [scenData, tplData, stData] = await Promise.all([
      api("/scenarios"),
      api("/scenarios/templates"),
      api("/scenarios/step-types"),
    ]);
    _scenarios = scenData.scenarios || [];
    _templates = tplData.templates || [];
    _stepTypes = stData.step_types || {};
  } catch (e) {
    container.innerHTML = `<div class="recovery-error">${t("general.error", { message: e.message })}</div>`;
    return;
  }

  _renderMain();
}

function _renderMain() {
  const c = _container;
  if (!c) return;

  // Check for triggered scenarios
  _checkTriggers();

  const scenarioCards = _scenarios.map(sc => _renderScenarioCard(sc)).join("");

  const templateCards = _templates.map(tpl => `
    <div class="scenario-template-card" data-tpl-id="${tpl.id}">
      <div class="scenario-card-icon" style="color:${tpl.color}">${tpl.icon}</div>
      <div class="scenario-card-body">
        <h4>${_esc(tpl.name)}</h4>
        <p>${_esc(tpl.description)}</p>
        <span class="scenario-step-count">${tpl.steps.length} ${t("scenario.steps_label")}</span>
      </div>
      <button class="btn-sm scenario-use-template" data-tpl-id="${tpl.id}">+ ${t("scenario.use_template")}</button>
    </div>
  `).join("");

  c.innerHTML = `
    <div class="scenarios-page">
      <div class="scenarios-header">
        <div>
          <h2>${t("scenario.title")}</h2>
          <p class="scenarios-subtitle">${t("scenario.subtitle")}</p>
        </div>
        <button class="btn-primary" id="btn-new-scenario">+ ${t("scenario.create_new")}</button>
      </div>

      ${_scenarios.length > 0 ? `
        <div class="scenarios-grid" id="scenarios-grid">${scenarioCards}</div>
      ` : `
        <div class="recovery-empty-state" style="padding:32px 0">
          <div class="recovery-empty-icon">\u{1F3AC}</div>
          <h3>${t("scenario.empty_title")}</h3>
          <p>${t("scenario.empty_hint")}</p>
        </div>
      `}

      <div class="scenarios-templates-section">
        <h3 class="recovery-section-title">\u{1F4D6} ${t("scenario.templates_title")}</h3>
        <p class="scenarios-subtitle">${t("scenario.templates_hint")}</p>
        <div class="scenarios-template-grid">${templateCards}</div>
      </div>
    </div>

    <div id="scenario-editor-overlay" class="scenario-editor-overlay hidden"></div>
  `;

  _bindMainEvents();
}

function _renderScenarioCard(sc) {
  const lastRun = sc.last_run_at
    ? new Date(sc.last_run_at * 1000).toLocaleDateString("cs-CZ")
    : t("scenario.never_run");

  const triggerBadge = sc.trigger?.type === "volume_mount"
    ? `<span class="scenario-trigger-badge">\u{1F4BD} ${_esc(sc.trigger.volume_name)}</span>`
    : sc.trigger?.type === "schedule"
    ? `<span class="scenario-trigger-badge">\u23F0 ${t("scenario.scheduled")}</span>`
    : "";

  const enabledSteps = (sc.steps || []).filter(s => s.enabled);
  const stepIcons = enabledSteps.length > 6
    ? enabledSteps.slice(0, 5).map(s => _stepTypes[s.type]?.icon || "\u2753").join(" \u2192 ") + ` \u2192 +${enabledSteps.length - 5}`
    : enabledSteps.map(s => _stepTypes[s.type]?.icon || "\u2753").join(" \u2192 ");

  return `
    <div class="scenario-card" data-id="${sc.id}" style="border-top:3px solid ${sc.color}">
      <div class="scenario-card-header">
        <span class="scenario-card-icon" style="color:${sc.color}">${sc.icon}</span>
        <button class="scenario-menu-btn" data-id="${sc.id}" title="Menu">\u22EF</button>
      </div>
      <h4 class="scenario-card-title">${_esc(sc.name)}</h4>
      <p class="scenario-card-desc">${_esc(sc.description)}</p>
      <div class="scenario-card-steps">${stepIcons}</div>
      <div class="scenario-card-footer">
        <span class="scenario-card-meta">${t("scenario.runs")}: ${sc.run_count} \u2022 ${lastRun}</span>
        ${triggerBadge}
      </div>
      <button class="btn-primary scenario-run-btn" data-id="${sc.id}">\u25B6 ${t("scenario.run")}</button>
    </div>
  `;
}

function _bindMainEvents() {
  const c = _container;

  // New scenario
  c.querySelector("#btn-new-scenario")?.addEventListener("click", () => _openEditor(null));

  // Use template
  c.querySelectorAll(".scenario-use-template").forEach(btn => {
    btn.addEventListener("click", () => {
      const tpl = _templates.find(t => t.id === btn.dataset.tplId);
      if (tpl) _openEditor(null, tpl);
    });
  });

  // Run scenario
  c.querySelectorAll(".scenario-run-btn").forEach(btn => {
    btn.addEventListener("click", () => _runScenario(btn.dataset.id, btn));
  });

  // Singleton popover menu — appended to document.body, above everything
  c.querySelectorAll(".scenario-menu-btn").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      _showPopoverMenu(id, btn);
    });
  });

  // Close popover on any outside click
  document.addEventListener("click", _closePopoverMenu);
}

// ── Run scenario ─────────────────────────────────────

async function _runScenario(id, btn) {
  btn.disabled = true;
  btn.textContent = t("scenario.running");

  try {
    const { task_id } = await apiPost(`/scenarios/${id}/run`, {});
    // Poll for completion
    for (let i = 0; i < 600; i++) {
      await new Promise(r => setTimeout(r, 1500));
      const task = await api(`/tasks/${task_id}`);

      if (task.progress) {
        const pct = task.progress.progress_pct || 0;
        const stepType = task.progress.step_type || "";
        const icon = _stepTypes[stepType]?.icon || "";
        btn.textContent = `${icon} ${pct}%`;
      }

      if (task.status === "completed") {
        const r = task.result || {};
        showToast(t("scenario.complete", { completed: r.completed || 0, failed: r.failed || 0 }), "success");
        btn.disabled = false;
        btn.textContent = `\u25B6 ${t("scenario.run")}`;
        await render(_container);
        return;
      }
      if (task.status === "failed") {
        showToast(t("task.failed_toast", { error: task.error }), "error");
        break;
      }
    }
  } catch (e) {
    showToast(t("general.error", { message: e.message }), "error");
  }
  btn.disabled = false;
  btn.textContent = `\u25B6 ${t("scenario.run")}`;
}

// ── Editor ───────────────────────────────────────────

async function _openEditor(scenarioId, template = null) {
  let sc;
  if (scenarioId) {
    sc = _scenarios.find(s => s.id === scenarioId);
    if (!sc) return;
  } else if (template) {
    sc = { ...template, id: null, trigger: { type: "manual", volume_name: "", schedule_cron: "" } };
  } else {
    sc = {
      id: null,
      name: "",
      description: "",
      icon: "\u{1F3AC}",
      color: "#58a6ff",
      steps: [],
      trigger: { type: "manual", volume_name: "", schedule_cron: "" },
    };
  }
  _editingId = sc.id;

  const overlay = document.getElementById("scenario-editor-overlay");
  if (!overlay) return;
  overlay.classList.remove("hidden");

  const iconPicker = SCENARIO_ICONS.map(i =>
    `<span class="scenario-icon-option${i === sc.icon ? " active" : ""}" data-icon="${i}">${i}</span>`
  ).join("");

  const colorPicker = SCENARIO_COLORS.map(c =>
    `<span class="scenario-color-option${c === sc.color ? " active" : ""}" data-color="${c}" style="background:${c}"></span>`
  ).join("");

  const stepTypeOptions = Object.entries(_stepTypes).map(([key, meta]) =>
    `<option value="${key}">${meta.icon} ${t(meta.label_key)}</option>`
  ).join("");

  overlay.innerHTML = `
    <div class="scenario-editor">
      <div class="scenario-editor-header">
        <h3>${scenarioId ? t("scenario.edit") : t("scenario.create_new")}</h3>
        <button class="scenario-editor-close" id="editor-close">&times;</button>
      </div>

      <div class="scenario-editor-body">
        <div class="scenario-editor-section">
          <label>${t("scenario.name")}</label>
          <input type="text" id="sc-name" class="recovery-input" value="${_esc(sc.name)}" placeholder="${t("scenario.name_placeholder")}" />
        </div>

        <div class="scenario-editor-section">
          <label>${t("scenario.description")}</label>
          <input type="text" id="sc-desc" class="recovery-input" value="${_esc(sc.description)}" placeholder="${t("scenario.desc_placeholder")}" />
        </div>

        <div class="scenario-editor-row">
          <div class="scenario-editor-section">
            <label>${t("scenario.icon")}</label>
            <div class="scenario-icon-picker" id="icon-picker">${iconPicker}</div>
          </div>
          <div class="scenario-editor-section">
            <label>${t("scenario.color")}</label>
            <div class="scenario-color-picker" id="color-picker">${colorPicker}</div>
          </div>
        </div>

        <div class="scenario-editor-section">
          <label>${t("scenario.trigger")}</label>
          <div class="scenario-trigger-options">
            <label class="scenario-trigger-option">
              <input type="radio" name="sc-trigger" value="manual" ${sc.trigger?.type === "manual" ? "checked" : ""}>
              <span>\u{1F590}\uFE0F ${t("scenario.trigger_manual")}</span>
            </label>
            <label class="scenario-trigger-option">
              <input type="radio" name="sc-trigger" value="volume_mount" ${sc.trigger?.type === "volume_mount" ? "checked" : ""}>
              <span>\u{1F4BD} ${t("scenario.trigger_volume")}</span>
            </label>
          </div>
          <div id="trigger-volume-config" class="${sc.trigger?.type === "volume_mount" ? "" : "hidden"}" style="margin-top:8px">
            <input type="text" id="sc-trigger-volume" class="recovery-input" value="${_esc(sc.trigger?.volume_name || "")}" placeholder="${t("scenario.volume_name_placeholder")}" />
          </div>
        </div>

        <div class="scenario-editor-section">
          <div class="scenario-steps-header">
            <label>${t("scenario.steps")}</label>
            <div class="scenario-add-step">
              <select id="add-step-type" class="recovery-input" style="width:auto">${stepTypeOptions}</select>
              <button class="btn-sm" id="btn-add-step">+ ${t("scenario.add_step")}</button>
            </div>
          </div>
          <div class="scenario-steps-list" id="steps-list"></div>
        </div>
      </div>

      <div class="scenario-editor-footer">
        <button class="btn-secondary" id="editor-cancel">${t("general.cancel")}</button>
        <button class="btn-primary" id="editor-save">${t("general.save")}</button>
      </div>
    </div>
  `;

  // State
  let currentIcon = sc.icon;
  let currentColor = sc.color;
  let currentSteps = (sc.steps || []).map(s => ({ ...s }));

  _renderStepsList(currentSteps);

  // Icon picker
  overlay.querySelectorAll(".scenario-icon-option").forEach(el => {
    el.addEventListener("click", () => {
      overlay.querySelectorAll(".scenario-icon-option").forEach(e => e.classList.remove("active"));
      el.classList.add("active");
      currentIcon = el.dataset.icon;
    });
  });

  // Color picker
  overlay.querySelectorAll(".scenario-color-option").forEach(el => {
    el.addEventListener("click", () => {
      overlay.querySelectorAll(".scenario-color-option").forEach(e => e.classList.remove("active"));
      el.classList.add("active");
      currentColor = el.dataset.color;
    });
  });

  // Trigger radio
  overlay.querySelectorAll('input[name="sc-trigger"]').forEach(radio => {
    radio.addEventListener("change", () => {
      const volConfig = overlay.querySelector("#trigger-volume-config");
      if (volConfig) volConfig.classList.toggle("hidden", radio.value !== "volume_mount");
    });
  });

  // Add step
  overlay.querySelector("#btn-add-step")?.addEventListener("click", () => {
    const select = overlay.querySelector("#add-step-type");
    currentSteps.push({ type: select.value, config: {}, enabled: true });
    _renderStepsList(currentSteps);
  });

  // Close
  overlay.querySelector("#editor-close")?.addEventListener("click", () => overlay.classList.add("hidden"));
  overlay.querySelector("#editor-cancel")?.addEventListener("click", () => overlay.classList.add("hidden"));

  // Save
  overlay.querySelector("#editor-save")?.addEventListener("click", async () => {
    const name = overlay.querySelector("#sc-name")?.value?.trim();
    if (!name) { showToast(t("scenario.name_required"), "error"); return; }

    const triggerType = overlay.querySelector('input[name="sc-trigger"]:checked')?.value || "manual";
    const volumeName = overlay.querySelector("#sc-trigger-volume")?.value?.trim() || "";

    const data = {
      name,
      description: overlay.querySelector("#sc-desc")?.value?.trim() || "",
      icon: currentIcon,
      color: currentColor,
      steps: currentSteps,
      trigger: { type: triggerType, volume_name: volumeName, schedule_cron: "" },
    };

    try {
      if (_editingId) {
        await apiPut(`/scenarios/${_editingId}`, data);
      } else {
        await apiPost("/scenarios", data);
      }
      showToast(t("general.saved"), "success");
      overlay.classList.add("hidden");
      await render(_container);
    } catch (e) {
      showToast(t("general.error", { message: e.message }), "error");
    }
  });

  function _renderStepsList(steps) {
    const list = overlay.querySelector("#steps-list");
    if (!list) return;

    if (steps.length === 0) {
      list.innerHTML = `<p class="scenario-no-steps">${t("scenario.no_steps")}</p>`;
      return;
    }

    list.innerHTML = steps.map((s, i) => {
      const meta = _stepTypes[s.type] || {};
      return `
        <div class="scenario-step-item ${s.enabled ? "" : "disabled"}" data-idx="${i}">
          <span class="scenario-step-number">${i + 1}</span>
          <span class="scenario-step-icon">${meta.icon || "\u2753"}</span>
          <span class="scenario-step-name">${t(meta.label_key || s.type)}</span>
          <div class="scenario-step-actions">
            <button class="btn-sm step-toggle" data-idx="${i}" title="${s.enabled ? t("scenario.disable_step") : t("scenario.enable_step")}">
              ${s.enabled ? "\u2705" : "\u26AA"}
            </button>
            <button class="btn-sm step-up" data-idx="${i}" ${i === 0 ? "disabled" : ""}>\u2191</button>
            <button class="btn-sm step-down" data-idx="${i}" ${i === steps.length - 1 ? "disabled" : ""}>\u2193</button>
            <button class="btn-sm step-remove" data-idx="${i}">\u{1F5D1}\uFE0F</button>
          </div>
        </div>
      `;
    }).join("");

    // Bind step actions
    list.querySelectorAll(".step-toggle").forEach(btn => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        currentSteps[idx].enabled = !currentSteps[idx].enabled;
        _renderStepsList(currentSteps);
      });
    });

    list.querySelectorAll(".step-up").forEach(btn => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        if (idx > 0) [currentSteps[idx - 1], currentSteps[idx]] = [currentSteps[idx], currentSteps[idx - 1]];
        _renderStepsList(currentSteps);
      });
    });

    list.querySelectorAll(".step-down").forEach(btn => {
      btn.addEventListener("click", () => {
        const idx = parseInt(btn.dataset.idx);
        if (idx < currentSteps.length - 1) [currentSteps[idx], currentSteps[idx + 1]] = [currentSteps[idx + 1], currentSteps[idx]];
        _renderStepsList(currentSteps);
      });
    });

    list.querySelectorAll(".step-remove").forEach(btn => {
      btn.addEventListener("click", () => {
        currentSteps.splice(parseInt(btn.dataset.idx), 1);
        _renderStepsList(currentSteps);
      });
    });
  }
}

// ── Volume trigger check ─────────────────────────────

async function _checkTriggers() {
  try {
    const { triggered } = await api("/scenarios/triggers");
    if (triggered && triggered.length > 0) {
      for (const sc of triggered) {
        showToast(`\u{1F4BD} ${t("scenario.trigger_detected", { name: sc.name, volume: sc.trigger?.volume_name })}`, "info");
      }
    }
  } catch { /* ignore */ }
}

// ── Popover menu (singleton, appended to body) ──────

let _popover = null;

function _showPopoverMenu(scenarioId, anchorBtn) {
  _closePopoverMenu();

  const rect = anchorBtn.getBoundingClientRect();

  _popover = document.createElement("div");
  _popover.className = "scenario-menu";
  _popover.innerHTML = `
    <button class="scenario-menu-item" data-action="edit" data-id="${scenarioId}">\u270F\uFE0F ${t("scenario.edit")}</button>
    <button class="scenario-menu-item" data-action="duplicate" data-id="${scenarioId}">\u{1F4CB} ${t("scenario.duplicate")}</button>
    <button class="scenario-menu-item danger" data-action="delete" data-id="${scenarioId}">\u{1F5D1}\uFE0F ${t("scenario.delete")}</button>
  `;
  _popover.style.top = `${rect.bottom + 4}px`;
  _popover.style.right = `${window.innerWidth - rect.right}px`;

  // Bind actions
  _popover.querySelectorAll(".scenario-menu-item").forEach(btn => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.dataset.id;
      const action = btn.dataset.action;
      _closePopoverMenu();
      if (action === "edit") _openEditor(id);
      if (action === "duplicate") {
        try {
          await apiPost(`/scenarios/${id}/duplicate`, {});
          showToast(t("scenario.duplicated"), "success");
          await render(_container);
        } catch (err) { showToast(t("general.error", { message: err.message }), "error"); }
      }
      if (action === "delete") {
        if (!confirm(t("scenario.delete_confirm"))) return;
        try {
          await apiDelete(`/scenarios/${id}`);
          showToast(t("scenario.deleted"), "success");
          await render(_container);
        } catch (err) { showToast(t("general.error", { message: err.message }), "error"); }
      }
    });
  });

  // Prevent click inside popover from closing it
  _popover.addEventListener("click", (e) => e.stopPropagation());

  document.body.appendChild(_popover);
}

function _closePopoverMenu() {
  if (_popover) {
    _popover.remove();
    _popover = null;
  }
}

// ── Helpers ──────────────────────────────────────────

function _esc(str) {
  if (!str) return "";
  return String(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
