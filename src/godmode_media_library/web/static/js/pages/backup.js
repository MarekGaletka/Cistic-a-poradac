/* GOD MODE Media Library — Distributed Backup page */

import { t } from "../i18n.js";
import { $, showToast, formatBytes } from "../utils.js";
import { api, apiPost, apiPut } from "../api.js";

function _esc(s) { const d = document.createElement("div"); d.textContent = s; return d.innerHTML; }

let _container = null;
let _plan = null;
let _manifestPage = 1;

export async function render(container) {
  _container = container;
  _plan = null;
  _manifestPage = 1;

  container.innerHTML = `
    <div class="backup-page">
      <div class="backup-header">
        <h2>\u2601\uFE0F ${t("backup.title")}</h2>
        <p class="backup-subtitle">${t("backup.subtitle")}</p>
      </div>

      <div id="backup-stats" class="backup-stats-bar">
        <div class="loading-sm">${t("general.loading")}</div>
      </div>

      <div class="backup-sections">
        <section class="backup-section">
          <h3>\uD83D\uDCE1 ${t("backup.targets")}</h3>
          <div class="backup-actions-row">
            <button id="btn-probe" class="btn btn-sm">\uD83D\uDD0D ${t("backup.probe")}</button>
          </div>
          <div id="backup-targets" class="backup-targets-grid">
            <div class="loading-sm">${t("general.loading")}</div>
          </div>
        </section>

        <section class="backup-section">
          <h3>\uD83D\uDCCB ${t("backup.plan")}</h3>
          <div class="backup-actions-row">
            <button id="btn-plan" class="btn btn-primary btn-sm">\uD83D\uDCCB ${t("backup.plan")}</button>
            <button id="btn-execute" class="btn btn-accent btn-sm" disabled>\uD83D\uDE80 ${t("backup.execute")}</button>
            <button id="btn-execute-dry" class="btn btn-sm" disabled>\uD83E\uDDEA ${t("backup.simulate")}</button>
          </div>
          <div id="backup-plan" class="backup-plan-summary"></div>
        </section>

        <section class="backup-section">
          <h3>\uD83D\uDEE1\uFE0F ${t("backup.monitoring_title")}</h3>
          <div id="backup-monitor-status"></div>
          <div class="backup-actions-row">
            <button id="btn-health" class="btn btn-sm">\uD83E\uDE7A ${t("backup.health_check")}</button>
            <button id="btn-verify" class="btn btn-sm">\uD83D\uDD12 ${t("backup.verify")}</button>
            <button id="btn-test-notif" class="btn btn-sm">\uD83D\uDD14 ${t("backup.test_notification")}</button>
            <button id="btn-ack-alerts" class="btn btn-sm" style="display:none">\u2705 ${t("backup.acknowledge_all")}</button>
          </div>
          <div id="backup-alerts"></div>
          <div id="backup-verify-result"></div>
        </section>

        <section class="backup-section">
          <h3>\uD83D\uDCD2 ${t("backup.manifest")}</h3>
          <div class="backup-manifest-search">
            <input type="text" id="manifest-search" class="input-sm" placeholder="${t("backup.search_placeholder")}" />
          </div>
          <div id="backup-manifest" class="backup-manifest-table"></div>
        </section>
      </div>
    </div>`;

  // Bind buttons
  $("#btn-probe").addEventListener("click", probeTargets);
  $("#btn-plan").addEventListener("click", createPlan);
  $("#btn-execute").addEventListener("click", () => executePlan(false));
  $("#btn-execute-dry").addEventListener("click", () => executePlan(true));
  $("#btn-verify").addEventListener("click", verifyBackups);
  $("#btn-health").addEventListener("click", runHealthCheck);
  $("#btn-test-notif").addEventListener("click", testNotification);
  $("#btn-ack-alerts").addEventListener("click", acknowledgeAlerts);

  const searchInput = $("#manifest-search");
  let _searchTimeout = null;
  searchInput.addEventListener("input", () => {
    clearTimeout(_searchTimeout);
    _searchTimeout = setTimeout(() => {
      _manifestPage = 1;
      loadManifest(searchInput.value.trim());
    }, 400);
  });

  // Load data
  await Promise.all([loadStats(), loadTargets(), loadManifest(), loadMonitorStatus()]);
}

// ---------------------------------------------------------------------------
// Stats bar
// ---------------------------------------------------------------------------

async function loadStats() {
  const el = $("#backup-stats");
  if (!el) return;

  try {
    const stats = await api("/backup/stats");
    const coveragePct = stats.coverage_pct ?? 0;
    const backedUp = stats.backed_up_files ?? 0;
    const totalSize = stats.total_backup_size ?? 0;
    const remotesUsed = stats.remotes_used ?? 0;
    const lastBackup = stats.last_backup_at
      ? new Date(stats.last_backup_at).toLocaleString("cs-CZ")
      : "\u2014";

    const coverageColor = coveragePct >= 90 ? "var(--color-success)" : coveragePct >= 50 ? "var(--color-warning)" : "var(--color-error)";

    el.innerHTML = `
      <div class="backup-metric-cards">
        <div class="backup-metric-card" style="border-left: 3px solid ${coverageColor}">
          <div class="backup-metric-value" style="color:${coverageColor}">${coveragePct.toFixed(1)}%</div>
          <div class="backup-metric-label">${t("backup.coverage")}</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-primary)">
          <div class="backup-metric-value">${backedUp.toLocaleString("cs-CZ")}</div>
          <div class="backup-metric-label">${t("backup.backed_up_files")}</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-accent)">
          <div class="backup-metric-value">${formatBytes(totalSize)}</div>
          <div class="backup-metric-label">${t("backup.total_size")}</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-info)">
          <div class="backup-metric-value">${remotesUsed}</div>
          <div class="backup-metric-label">${t("backup.storages_used")}</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-muted)">
          <div class="backup-metric-value backup-metric-value-sm">${lastBackup}</div>
          <div class="backup-metric-label">${t("backup.last_backup")}</div>
        </div>
      </div>`;
  } catch (e) {
    el.innerHTML = `<div class="empty">${t("backup.stats_error")}: ${_esc(e.message)}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

async function loadTargets() {
  const el = $("#backup-targets");
  if (!el) return;

  try {
    const data = await api("/backup/targets");
    const targets = data.targets || [];

    if (targets.length === 0) {
      el.innerHTML = `<div class="empty">${t("backup.no_targets")}</div>`;
      return;
    }

    el.innerHTML = targets.map(tgt => {
      const name = tgt.remote_name || tgt.name || "";
      const used = tgt.used_bytes ?? tgt.used ?? 0;
      const total = tgt.total_bytes ?? tgt.total ?? 0;
      const free = tgt.free_bytes ?? tgt.free ?? 0;
      const avail = tgt.available_bytes ?? 0;
      const usedPct = total > 0 ? Math.min((used / total) * 100, 100) : 0;
      const barClass = usedPct > 90 ? "bar-danger" : usedPct > 70 ? "bar-warning" : "";
      const enabled = tgt.enabled !== false;
      const priority = tgt.priority ?? 3;
      const backedFiles = tgt.backed_up_files ?? 0;

      const capacityUnknown = total === 0;
      const capacityRow = capacityUnknown
        ? `<div class="backup-capacity-manual">
             <span style="color:var(--text-secondary);font-size:0.85rem">${t("backup.capacity_unknown")}</span>
             <div style="display:flex;gap:0.5rem;align-items:center;margin-top:0.25rem">
               <select class="backup-capacity-preset" data-target="${name}">
                 <option value="">${t("backup.set_manually")}</option>
                 <option value="107374182400">100 GB</option>
                 <option value="214748364800">200 GB</option>
                 <option value="536870912000">500 GB</option>
                 <option value="1099511627776">1 TB</option>
                 <option value="2199023255552">2 TB</option>
                 <option value="5497558138880">5 TB</option>
                 <option value="6597069766656">6 TB</option>
                 <option value="10995116277760">10 TB</option>
               </select>
               <button class="btn btn-small btn-set-capacity" data-target="${name}">${t("backup.set_btn")}</button>
             </div>
           </div>`
        : `<div class="storage-bar">
             <div class="storage-bar-fill ${barClass}" style="width: ${usedPct.toFixed(1)}%"></div>
             <span class="storage-bar-label">${formatBytes(used)} / ${formatBytes(total)} (volno: ${formatBytes(avail)})</span>
           </div>`;

      return `
        <div class="backup-target-card ${enabled ? "target-enabled" : "target-disabled"}" data-target="${name}">
          <div class="backup-target-header">
            <span class="backup-target-name">\u2601\uFE0F ${name}</span>
            <span style="display:flex;gap:0.4rem;align-items:center">
              ${tgt.encrypted ? `<span class="backup-encrypt-badge" title="${t("backup.encrypted")}">\uD83D\uDD12 E2E</span>` : `<span class="backup-no-encrypt-badge" title="${t("backup.not_encrypted")}">\uD83D\uDD13</span>`}
              <span class="backup-target-badge">${backedFiles.toLocaleString("cs-CZ")} ${t("backup.files_unit")}</span>
            </span>
          </div>

          ${capacityRow}

          <div class="backup-target-actions">
            <label class="backup-toggle-label" style="display:flex;align-items:center;gap:0.4rem;cursor:pointer">
              <button class="backup-toggle ${enabled ? "active" : ""}" data-target="${name}"></button>
              <span style="font-size:0.85rem">${enabled ? t("backup.enabled") : t("backup.disabled")}</span>
            </label>

            <div style="display:flex;align-items:center;gap:0.3rem;margin-left:auto">
              <span style="font-size:0.8rem;color:var(--text-secondary)">${t("backup.priority")}:</span>
              <select class="backup-priority" data-target="${name}">
                ${[1, 2, 3, 4, 5].map(p => `<option value="${p}" ${p === priority ? "selected" : ""}>${p}</option>`).join("")}
              </select>
            </div>

            <button class="btn btn-small btn-probe-single" data-target="${name}">\uD83D\uDD0D</button>
          </div>
        </div>`;
    }).join("");

    // Bind toggle switches
    el.querySelectorAll(".backup-toggle").forEach(toggle => {
      toggle.addEventListener("click", async () => {
        const name = toggle.dataset.target;
        const nowActive = !toggle.classList.contains("active");
        try {
          await apiPut(`/backup/targets/${encodeURIComponent(name)}`, { enabled: nowActive });
          toggle.classList.toggle("active", nowActive);
          const label = toggle.nextElementSibling;
          if (label) label.textContent = nowActive ? t("backup.enabled") : t("backup.disabled");
          const card = toggle.closest(".backup-target-card");
          if (card) {
            card.classList.toggle("target-enabled", nowActive);
            card.classList.toggle("target-disabled", !nowActive);
          }
          showToast(`${name}: ${nowActive ? t("backup.activated") : t("backup.deactivated")}`, "success");
        } catch (e) {
          showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
        }
      });
    });

    // Bind capacity preset buttons (for Shared Drives)
    el.querySelectorAll(".btn-set-capacity").forEach(btn => {
      btn.addEventListener("click", async () => {
        const name = btn.dataset.target;
        const sel = el.querySelector(`.backup-capacity-preset[data-target="${name}"]`);
        const bytes = parseInt(sel?.value, 10);
        if (!bytes) { showToast(t("backup.select_capacity"), "warning"); return; }
        try {
          await apiPut(`/backup/targets/${encodeURIComponent(name)}`, { total_bytes: bytes, free_bytes: bytes });
          showToast(`${name}: ${t("backup.capacity_set")} ${formatBytes(bytes)}`, "success");
          await loadTargets();
        } catch (e) {
          showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
        }
      });
    });

    // Bind priority selectors
    el.querySelectorAll(".backup-priority").forEach(sel => {
      sel.addEventListener("change", async () => {
        const name = sel.dataset.target;
        const priority = parseInt(sel.value, 10);
        try {
          await apiPut(`/backup/targets/${encodeURIComponent(name)}`, { priority });
          showToast(`${name}: ${t("backup.priority_set")} ${priority}`, "success");
        } catch (e) {
          showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
        }
      });
    });

    // Bind individual probe buttons
    el.querySelectorAll(".btn-probe-single").forEach(btn => {
      btn.addEventListener("click", async () => {
        const name = btn.dataset.target;
        btn.disabled = true;
        btn.textContent = `\u23F3 ${t("backup.probing")}`;
        try {
          await apiPost("/backup/probe", { targets: [name] });
          showToast(`${name}: ${t("backup.capacity_updated")}`, "success");
          await loadTargets();
        } catch (e) {
          showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
          btn.disabled = false;
          btn.textContent = `\uD83D\uDD0D ${t("backup.probe")}`;
        }
      });
    });
  } catch (e) {
    el.innerHTML = `<div class="empty">${t("backup.targets_error")}: ${_esc(e.message)}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Probe all targets
// ---------------------------------------------------------------------------

async function probeTargets() {
  const btn = $("#btn-probe");
  if (!btn) return;

  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("backup.probe_running")}`;

  try {
    const result = await apiPost("/backup/probe");
    const count = result.probed ?? 0;
    showToast(t("backup.probed_count", { count }), "success");
    await loadTargets();
    await loadStats();
  } catch (e) {
    showToast(`${t("backup.probe_error")}: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = `\uD83D\uDD0D ${t("backup.probe")}`;
  }
}

// ---------------------------------------------------------------------------
// Backup plan
// ---------------------------------------------------------------------------

async function createPlan() {
  const btn = $("#btn-plan");
  const planEl = $("#backup-plan");
  if (!btn || !planEl) return;

  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("backup.plan_computing")}`;
  planEl.innerHTML = `<div class="loading-sm">${t("backup.plan_creating")}</div>`;

  try {
    const result = await apiPost("/backup/plan");
    _plan = result;

    const totalFiles = result.total_files ?? 0;
    const totalSize = result.total_size ?? 0;
    const remotesUsed = result.remotes_used ?? 0;
    const overflow = result.overflow_files ?? 0;
    const overflowSize = result.overflow_size ?? 0;
    const assignments = result.assignments || [];

    let overflowHtml = "";
    if (overflow > 0) {
      overflowHtml = `
        <div class="backup-overflow-warning">
          \u26A0\uFE0F <strong>${overflow.toLocaleString("cs-CZ")} ${t("backup.plan_overflow_warning", { size: formatBytes(overflowSize) })}</strong>
        </div>`;
    }

    let assignmentsHtml = "";
    if (assignments.length > 0) {
      assignmentsHtml = `
        <div class="backup-assignments">
          <table class="backup-table">
            <thead>
              <tr>
                <th>${t("backup.plan_storage_header")}</th>
                <th>${t("backup.plan_files_header")}</th>
                <th>${t("backup.plan_size_header")}</th>
                <th>${t("backup.plan_priority_header")}</th>
              </tr>
            </thead>
            <tbody>
              ${assignments.map(a => `
                <tr>
                  <td><span class="backup-remote-name">${a.remote || a.name}</span></td>
                  <td>${(a.file_count ?? 0).toLocaleString("cs-CZ")}</td>
                  <td>${formatBytes(a.size ?? 0)}</td>
                  <td><span class="backup-priority-badge priority-${a.priority ?? 3}">${a.priority ?? 3}</span></td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        </div>`;
    }

    planEl.innerHTML = `
      <div class="backup-plan-card">
        <div class="backup-plan-header">
          <div class="backup-plan-stat">
            <span class="backup-plan-stat-value">${totalFiles.toLocaleString("cs-CZ")}</span>
            <span class="backup-plan-stat-label">${t("backup.plan_files")}</span>
          </div>
          <div class="backup-plan-stat">
            <span class="backup-plan-stat-value">${formatBytes(totalSize)}</span>
            <span class="backup-plan-stat-label">${t("backup.plan_total_label")}</span>
          </div>
          <div class="backup-plan-stat">
            <span class="backup-plan-stat-value">${remotesUsed}</span>
            <span class="backup-plan-stat-label">${t("backup.plan_remotes")}</span>
          </div>
        </div>
        ${overflowHtml}
        ${assignmentsHtml}
      </div>`;

    // Enable execute buttons
    const btnExec = $("#btn-execute");
    const btnDry = $("#btn-execute-dry");
    if (btnExec) btnExec.disabled = false;
    if (btnDry) btnDry.disabled = false;
  } catch (e) {
    planEl.innerHTML = `<div class="empty">${t("backup.plan_error")}: ${_esc(e.message)}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = `\uD83D\uDCCB ${t("backup.plan")}`;
  }
}

// ---------------------------------------------------------------------------
// Execute plan
// ---------------------------------------------------------------------------

async function executePlan(dryRun) {
  if (!_plan) {
    showToast(t("backup.create_plan_first"), "error");
    return;
  }

  const btnExec = $("#btn-execute");
  const btnDry = $("#btn-execute-dry");
  const planEl = $("#backup-plan");

  if (btnExec) btnExec.disabled = true;
  if (btnDry) btnDry.disabled = true;

  const label = dryRun ? t("backup.simulation") : t("backup.backup_label");
  showToast(`${label} ${t("backup.started")}`, "info");

  try {
    const result = await apiPost("/backup/execute", {
      plan_id: _plan.plan_id,
      dry_run: dryRun,
    });

    const taskId = result.task_id;
    if (!taskId) {
      showToast(`${label} ${t("backup.completed_success")}`, "success");
      await refreshAll();
      return;
    }

    // Poll for task completion
    if (planEl) {
      planEl.innerHTML += `
        <div class="backup-progress" id="backup-progress">
          <div class="backup-progress-bar">
            <div class="backup-progress-fill" id="backup-progress-fill" style="width:0%"></div>
          </div>
          <span class="backup-progress-label" id="backup-progress-label">${label} ${t("backup.running_task", { task_id: taskId })}</span>
        </div>`;
    }

    await pollTask(taskId, label);
  } catch (e) {
    showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
  } finally {
    if (btnExec) btnExec.disabled = false;
    if (btnDry) btnDry.disabled = false;
  }
}

async function pollTask(taskId, label) {
  const maxAttempts = 300; // 5 minutes
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(r => setTimeout(r, 1000));
    try {
      const status = await api(`/tasks/${taskId}`);
      const progress = status.progress ?? 0;

      const fillEl = document.getElementById("backup-progress-fill");
      const labelEl = document.getElementById("backup-progress-label");
      if (fillEl) fillEl.style.width = `${progress}%`;
      if (labelEl) labelEl.textContent = `${label}: ${progress}% ${status.message || ""}`;

      if (status.status === "completed") {
        showToast(`${label} ${t("backup.completed_success")}`, "success");
        await refreshAll();
        return;
      }
      if (status.status === "failed") {
        showToast(`${label} ${t("backup.failed")}: ${status.error || t("backup.unknown_error")}`, "error", 8000);
        return;
      }
    } catch {
      // keep polling
    }
  }
  showToast(`${label}: ${t("backup.timeout")}`, "error");
}

// ---------------------------------------------------------------------------
// Verify
// ---------------------------------------------------------------------------

async function verifyBackups() {
  const btn = $("#btn-verify");
  const resultEl = $("#backup-verify-result");
  if (!btn || !resultEl) return;

  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("backup.verifying")}`;
  resultEl.innerHTML = `<div class="loading-sm">${t("backup.verifying_files")}</div>`;

  try {
    const result = await apiPost("/backup/verify");
    const total = result.total ?? 0;
    const verified = result.verified ?? 0;
    const missing = result.missing ?? 0;
    const errors = result.errors || [];
    const pct = total > 0 ? ((verified / total) * 100).toFixed(1) : 0;

    const statusClass = missing === 0 ? "verify-ok" : "verify-partial";
    const statusIcon = missing === 0 ? "\u2705" : "\u26A0\uFE0F";

    let errorsHtml = "";
    if (errors.length > 0) {
      errorsHtml = `
        <div class="backup-verify-errors">
          <strong>${t("backup.missing_files")}:</strong>
          <ul>
            ${errors.slice(0, 20).map(err => `
              <li><code>${err.file || err.path}</code> \u2014 ${err.remote || "?"}</li>
            `).join("")}
            ${errors.length > 20 ? `<li class="text-muted">${t("backup.and_more", { count: errors.length - 20 })}</li>` : ""}
          </ul>
        </div>`;
    }

    resultEl.innerHTML = `
      <div class="backup-verify-card ${statusClass}">
        <div class="backup-verify-header">
          <span class="backup-verify-icon">${statusIcon}</span>
          <div class="backup-verify-stats">
            <span class="backup-verify-main">${pct}% ${t("backup.verified_pct")}</span>
            <span class="backup-verify-detail">${verified.toLocaleString("cs-CZ")} / ${total.toLocaleString("cs-CZ")} ${t("backup.verified_detail")}${missing > 0 ? `, ${missing.toLocaleString("cs-CZ")} ${t("backup.missing_label")}` : ""}</span>
          </div>
        </div>
        ${errorsHtml}
      </div>`;
  } catch (e) {
    resultEl.innerHTML = `<div class="empty">${t("backup.verify_error")}: ${_esc(e.message)}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = `\uD83D\uDD12 ${t("backup.verify")}`;
  }
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

async function loadManifest(search = "") {
  const el = $("#backup-manifest");
  if (!el) return;

  const limit = 50;

  try {
    let url = `/backup/manifest?page=${_manifestPage}&limit=${limit}`;
    if (search) url += `&search=${encodeURIComponent(search)}`;

    const data = await api(url);
    const items = data.items || [];
    const totalItems = data.total ?? items.length;
    const totalPages = Math.ceil(totalItems / limit) || 1;

    if (items.length === 0) {
      el.innerHTML = `<div class="empty">${search ? t("backup.no_manifest_search", { search: _esc(search) }) : t("backup.no_manifest")}</div>`;
      return;
    }

    el.innerHTML = `
      <div class="backup-manifest-info">
        ${t("backup.manifest_showing", { from: ((_manifestPage - 1) * limit) + 1, to: Math.min(_manifestPage * limit, totalItems), total: totalItems.toLocaleString("cs-CZ") })}
      </div>
      <table class="backup-table backup-manifest-list">
        <thead>
          <tr>
            <th>${t("backup.manifest_file")}</th>
            <th>${t("backup.manifest_size")}</th>
            <th>${t("backup.manifest_storage")}</th>
            <th>${t("backup.manifest_backed_at")}</th>
            <th>${t("backup.manifest_status")}</th>
          </tr>
        </thead>
        <tbody>
          ${items.map(item => {
            const name = item.file_name || (item.path || "").split("/").pop() || "\u2014";
            const size = formatBytes(item.size ?? 0);
            const remote = item.remote || "\u2014";
            const backedAt = item.backed_up_at
              ? new Date(item.backed_up_at).toLocaleString("cs-CZ")
              : "\u2014";
            const verified = item.verified;
            const statusIcon = verified === true ? "\u2705" : verified === false ? "\u274C" : "\u2014";
            const statusTitle = verified === true ? t("backup.verified") : verified === false ? t("backup.not_verified") : t("backup.manifest_unchecked");
            return `
              <tr>
                <td class="manifest-filename" title="${item.path || name}">${name}</td>
                <td>${size}</td>
                <td><span class="backup-remote-badge">${remote}</span></td>
                <td>${backedAt}</td>
                <td title="${statusTitle}">${statusIcon}</td>
              </tr>`;
          }).join("")}
        </tbody>
      </table>
      <div class="backup-manifest-pagination">
        <button class="btn btn-sm btn-manifest-prev" ${_manifestPage <= 1 ? "disabled" : ""}>\u2190 ${t("backup.prev_page")}</button>
        <span class="backup-manifest-page-info">${t("backup.manifest_page", { current: _manifestPage, total: totalPages })}</span>
        <button class="btn btn-sm btn-manifest-next" ${_manifestPage >= totalPages ? "disabled" : ""}>${t("backup.next_page")} \u2192</button>
      </div>`;

    // Pagination buttons
    const prevBtn = el.querySelector(".btn-manifest-prev");
    const nextBtn = el.querySelector(".btn-manifest-next");
    const currentSearch = search;

    if (prevBtn) {
      prevBtn.addEventListener("click", () => {
        if (_manifestPage > 1) {
          _manifestPage--;
          loadManifest(currentSearch);
        }
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener("click", () => {
        if (_manifestPage < totalPages) {
          _manifestPage++;
          loadManifest(currentSearch);
        }
      });
    }
  } catch (e) {
    el.innerHTML = `<div class="empty">${t("backup.manifest_error")}: ${_esc(e.message)}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function refreshAll() {
  const progressEl = document.getElementById("backup-progress");
  if (progressEl) progressEl.remove();

  await Promise.all([loadStats(), loadTargets(), loadManifest(), loadMonitorStatus()]);
}

// ---------------------------------------------------------------------------
// Monitoring
// ---------------------------------------------------------------------------

async function loadMonitorStatus() {
  const el = $("#backup-monitor-status");
  const alertsEl = $("#backup-alerts");
  const ackBtn = $("#btn-ack-alerts");
  if (!el) return;

  try {
    const data = await api("/backup/monitor");
    const overall = data.overall || "ok";
    const lastCheck = data.last_check_at
      ? new Date(data.last_check_at).toLocaleString("cs-CZ")
      : t("backup.last_check_never");

    const statusColor = overall === "ok" ? "#3fb950" : overall === "warning" ? "#d29922" : "#f85149";
    const statusLabel = overall === "ok" ? t("backup.monitor_ok") : overall === "warning" ? t("backup.monitor_warning") : t("backup.monitor_critical");
    const statusIcon = overall === "ok" ? "\u2705" : overall === "warning" ? "\u26A0\uFE0F" : "\u274C";

    el.innerHTML = `
      <div style="display:flex;align-items:center;gap:1rem;margin-bottom:0.75rem;padding:0.75rem;background:var(--bg);border-radius:8px;border-left:3px solid ${statusColor}">
        <span style="font-size:1.5rem">${statusIcon}</span>
        <div>
          <div style="font-weight:600;color:${statusColor}">${statusLabel}</div>
          <div style="font-size:0.8rem;color:var(--text-secondary)">${t("backup.last_check")}: ${lastCheck}</div>
        </div>
        ${data.critical_count > 0 ? `<span style="background:#f85149;color:#fff;padding:2px 8px;border-radius:999px;font-size:0.75rem;font-weight:600">${t("backup.critical_count", { count: data.critical_count })}</span>` : ""}
        ${data.warning_count > 0 ? `<span style="background:#d29922;color:#fff;padding:2px 8px;border-radius:999px;font-size:0.75rem;font-weight:600">${t("backup.warning_count", { count: data.warning_count })}</span>` : ""}
      </div>`;

    // Show alerts
    const alerts = data.active_alerts || [];
    if (alerts.length > 0 && alertsEl) {
      if (ackBtn) ackBtn.style.display = "";
      alertsEl.innerHTML = alerts.map(a => {
        const sev = a.severity === "critical" ? "#f85149" : "#d29922";
        const icon = a.severity === "critical" ? "\u274C" : "\u26A0\uFE0F";
        const time = a.timestamp ? new Date(a.timestamp).toLocaleString("cs-CZ") : "";
        return `<div style="display:flex;align-items:center;gap:0.5rem;padding:0.5rem;margin-bottom:0.25rem;background:var(--bg);border-radius:6px;border-left:2px solid ${sev};font-size:0.85rem">
          <span>${icon}</span>
          <span style="flex:1">${a.message}</span>
          <span style="color:var(--text-secondary);font-size:0.75rem">${time}</span>
        </div>`;
      }).join("");
    } else {
      if (alertsEl) alertsEl.innerHTML = "";
      if (ackBtn) ackBtn.style.display = "none";
    }
  } catch {
    el.innerHTML = "";
  }
}

async function runHealthCheck() {
  const btn = $("#btn-health");
  if (!btn) return;
  btn.disabled = true;
  btn.textContent = `\u23F3 ${t("backup.health_checking")}`;
  try {
    const { task_id } = await apiPost("/backup/monitor/check");
    const result = await pollTask(task_id, t("backup.health_label"));
    if (result) {
      showToast(t("backup.health_result", { healthy: result.healthy, checked: result.checked }), result.unhealthy > 0 ? "warning" : "success");
    }
    await loadMonitorStatus();
  } catch (e) {
    showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = `\uD83E\uDE7A ${t("backup.health_check")}`;
  }
}

async function testNotification() {
  try {
    await apiPost("/backup/monitor/test-notification");
    showToast(t("backup.test_notif_sent"), "success");
  } catch (e) {
    showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
  }
}

async function acknowledgeAlerts() {
  try {
    const { acknowledged } = await apiPost("/backup/monitor/acknowledge");
    showToast(t("backup.acknowledged_count", { count: acknowledged }), "success");
    await loadMonitorStatus();
  } catch (e) {
    showToast(`${t("backup.error_prefix")}: ${e.message}`, "error");
  }
}
