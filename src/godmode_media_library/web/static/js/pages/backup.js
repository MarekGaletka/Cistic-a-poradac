/* GOD MODE Media Library — Distributed Backup page */

import { t } from "../i18n.js";
import { $, showToast, formatBytes } from "../utils.js";
import { api, apiPost, apiPut } from "../api.js";

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
        <h2>\u2601\uFE0F Distribuovan\u00e1 z\u00e1loha</h2>
        <p class="backup-subtitle">Rozlo\u017ete z\u00e1lohu m\u00e9di\u00ed p\u0159es v\u00edce cloud \u00falo\u017ei\u0161\u0165 pro maxim\u00e1ln\u00ed bezpe\u010dnost</p>
      </div>

      <div id="backup-stats" class="backup-stats-bar">
        <div class="loading-sm">Na\u010d\u00edt\u00e1n\u00ed...</div>
      </div>

      <div class="backup-sections">
        <section class="backup-section">
          <h3>\uD83D\uDCE1 C\u00edlov\u00e1 \u00falo\u017ei\u0161t\u011b</h3>
          <div class="backup-actions-row">
            <button id="btn-probe" class="btn btn-sm">\uD83D\uDD0D Prozkoumat kapacity</button>
          </div>
          <div id="backup-targets" class="backup-targets-grid">
            <div class="loading-sm">Na\u010d\u00edt\u00e1n\u00ed...</div>
          </div>
        </section>

        <section class="backup-section">
          <h3>\uD83D\uDCCB Pl\u00e1n z\u00e1lohy</h3>
          <div class="backup-actions-row">
            <button id="btn-plan" class="btn btn-primary btn-sm">\uD83D\uDCCB Vytvo\u0159it pl\u00e1n</button>
            <button id="btn-execute" class="btn btn-accent btn-sm" disabled>\uD83D\uDE80 Spustit z\u00e1lohu</button>
            <button id="btn-execute-dry" class="btn btn-sm" disabled>\uD83E\uDDEA Simulace</button>
          </div>
          <div id="backup-plan" class="backup-plan-summary"></div>
        </section>

        <section class="backup-section">
          <h3>\u2705 Ov\u011b\u0159en\u00ed z\u00e1lohy</h3>
          <div class="backup-actions-row">
            <button id="btn-verify" class="btn btn-sm">\uD83D\uDD12 Ov\u011b\u0159it z\u00e1lohy</button>
          </div>
          <div id="backup-verify-result"></div>
        </section>

        <section class="backup-section">
          <h3>\uD83D\uDCD2 Manifest z\u00e1loh</h3>
          <div class="backup-manifest-search">
            <input type="text" id="manifest-search" class="input-sm" placeholder="Hledat soubor..." />
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
  await Promise.all([loadStats(), loadTargets(), loadManifest()]);
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
          <div class="backup-metric-label">Pokryt\u00ed</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-primary)">
          <div class="backup-metric-value">${backedUp.toLocaleString("cs-CZ")}</div>
          <div class="backup-metric-label">Z\u00e1lohovan\u00fdch soubor\u016f</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-accent)">
          <div class="backup-metric-value">${formatBytes(totalSize)}</div>
          <div class="backup-metric-label">Celkov\u00e1 velikost</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-info)">
          <div class="backup-metric-value">${remotesUsed}</div>
          <div class="backup-metric-label">Pou\u017eit\u00fdch \u00falo\u017ei\u0161\u0165</div>
        </div>
        <div class="backup-metric-card" style="border-left: 3px solid var(--color-muted)">
          <div class="backup-metric-value backup-metric-value-sm">${lastBackup}</div>
          <div class="backup-metric-label">Posledn\u00ed z\u00e1loha</div>
        </div>
      </div>`;
  } catch (e) {
    el.innerHTML = `<div class="empty">Nepoda\u0159ilo se na\u010d\u00edst statistiky: ${e.message}</div>`;
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
      el.innerHTML = `<div class="empty">\u017d\u00e1dn\u00e1 c\u00edlov\u00e1 \u00falo\u017ei\u0161t\u011b \u2014 p\u0159ipojte cloud \u00falo\u017ei\u0161t\u011b na str\u00e1nce Cloud.</div>`;
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
             <span style="color:var(--text-secondary);font-size:0.85rem">Kapacita nezjistitelna (Shared Drive)</span>
             <div style="display:flex;gap:0.5rem;align-items:center;margin-top:0.25rem">
               <select class="backup-capacity-preset" data-target="${name}">
                 <option value="">— nastavit rucne —</option>
                 <option value="107374182400">100 GB</option>
                 <option value="214748364800">200 GB</option>
                 <option value="536870912000">500 GB</option>
                 <option value="1099511627776">1 TB</option>
                 <option value="2199023255552">2 TB</option>
                 <option value="5497558138880">5 TB</option>
                 <option value="6597069766656">6 TB</option>
                 <option value="10995116277760">10 TB</option>
               </select>
               <button class="btn btn-small btn-set-capacity" data-target="${name}">Nastavit</button>
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
            <span class="backup-target-badge">${backedFiles.toLocaleString("cs-CZ")} souboru</span>
          </div>

          ${capacityRow}

          <div class="backup-target-actions">
            <label class="backup-toggle-label" style="display:flex;align-items:center;gap:0.4rem;cursor:pointer">
              <button class="backup-toggle ${enabled ? "active" : ""}" data-target="${name}"></button>
              <span style="font-size:0.85rem">${enabled ? "Aktivni" : "Neaktivni"}</span>
            </label>

            <div style="display:flex;align-items:center;gap:0.3rem;margin-left:auto">
              <span style="font-size:0.8rem;color:var(--text-secondary)">Priorita:</span>
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
          if (label) label.textContent = nowActive ? "Aktivni" : "Neaktivni";
          const card = toggle.closest(".backup-target-card");
          if (card) {
            card.classList.toggle("target-enabled", nowActive);
            card.classList.toggle("target-disabled", !nowActive);
          }
          showToast(`${name}: ${nowActive ? "aktivovano" : "deaktivovano"}`, "success");
        } catch (e) {
          showToast(`Chyba: ${e.message}`, "error");
        }
      });
    });

    // Bind capacity preset buttons (for Shared Drives)
    el.querySelectorAll(".btn-set-capacity").forEach(btn => {
      btn.addEventListener("click", async () => {
        const name = btn.dataset.target;
        const sel = el.querySelector(`.backup-capacity-preset[data-target="${name}"]`);
        const bytes = parseInt(sel?.value, 10);
        if (!bytes) { showToast("Vyberte kapacitu", "warning"); return; }
        try {
          await apiPut(`/backup/targets/${encodeURIComponent(name)}`, { total_bytes: bytes, free_bytes: bytes });
          showToast(`${name}: kapacita nastavena na ${formatBytes(bytes)}`, "success");
          await loadTargets();
        } catch (e) {
          showToast(`Chyba: ${e.message}`, "error");
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
          showToast(`${name}: priorita nastavena na ${priority}`, "success");
        } catch (e) {
          showToast(`Chyba: ${e.message}`, "error");
        }
      });
    });

    // Bind individual probe buttons
    el.querySelectorAll(".btn-probe-single").forEach(btn => {
      btn.addEventListener("click", async () => {
        const name = btn.dataset.target;
        btn.disabled = true;
        btn.textContent = "\u23F3 Zkoum\u00e1m...";
        try {
          await apiPost("/backup/probe", { targets: [name] });
          showToast(`${name}: kapacita aktualizov\u00e1na`, "success");
          await loadTargets();
        } catch (e) {
          showToast(`Chyba: ${e.message}`, "error");
          btn.disabled = false;
          btn.textContent = "\uD83D\uDD0D Prozkoumat";
        }
      });
    });
  } catch (e) {
    el.innerHTML = `<div class="empty">Nepoda\u0159ilo se na\u010d\u00edst c\u00edle: ${e.message}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Probe all targets
// ---------------------------------------------------------------------------

async function probeTargets() {
  const btn = $("#btn-probe");
  if (!btn) return;

  btn.disabled = true;
  btn.textContent = "\u23F3 Prob\u00edh\u00e1 pr\u016fzkum...";

  try {
    const result = await apiPost("/backup/probe");
    const count = result.probed ?? 0;
    showToast(`Prozkoum\u00e1no ${count} \u00falo\u017ei\u0161\u0165`, "success");
    await loadTargets();
    await loadStats();
  } catch (e) {
    showToast(`Chyba pr\u016fzkumu: ${e.message}`, "error");
  } finally {
    btn.disabled = false;
    btn.textContent = "\uD83D\uDD0D Prozkoumat kapacity";
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
  btn.textContent = "\u23F3 Vypo\u010d\u00edt\u00e1v\u00e1m...";
  planEl.innerHTML = `<div class="loading-sm">Vytvá\u0159\u00edm distribu\u010dn\u00ed pl\u00e1n...</div>`;

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
          \u26A0\uFE0F <strong>${overflow.toLocaleString("cs-CZ")} soubor\u016f (${formatBytes(overflowSize)})</strong> se nevejde do \u017e\u00e1dn\u00e9ho \u00falo\u017ei\u0161t\u011b.
          P\u0159idejte dal\u0161\u00ed \u00falo\u017ei\u0161t\u011b nebo uvoln\u011bte m\u00edsto.
        </div>`;
    }

    let assignmentsHtml = "";
    if (assignments.length > 0) {
      assignmentsHtml = `
        <div class="backup-assignments">
          <table class="backup-table">
            <thead>
              <tr>
                <th>\u00dalo\u017ei\u0161t\u011b</th>
                <th>Soubor\u016f</th>
                <th>Velikost</th>
                <th>Priorita</th>
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
            <span class="backup-plan-stat-label">soubor\u016f</span>
          </div>
          <div class="backup-plan-stat">
            <span class="backup-plan-stat-value">${formatBytes(totalSize)}</span>
            <span class="backup-plan-stat-label">celkem</span>
          </div>
          <div class="backup-plan-stat">
            <span class="backup-plan-stat-value">${remotesUsed}</span>
            <span class="backup-plan-stat-label">\u00falo\u017ei\u0161\u0165</span>
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
    planEl.innerHTML = `<div class="empty">Chyba p\u0159i vytv\u00e1\u0159en\u00ed pl\u00e1nu: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "\uD83D\uDCCB Vytvo\u0159it pl\u00e1n";
  }
}

// ---------------------------------------------------------------------------
// Execute plan
// ---------------------------------------------------------------------------

async function executePlan(dryRun) {
  if (!_plan) {
    showToast("Nejprve vytvo\u0159te pl\u00e1n z\u00e1lohy", "error");
    return;
  }

  const btnExec = $("#btn-execute");
  const btnDry = $("#btn-execute-dry");
  const planEl = $("#backup-plan");

  if (btnExec) btnExec.disabled = true;
  if (btnDry) btnDry.disabled = true;

  const label = dryRun ? "Simulace" : "Z\u00e1loha";
  showToast(`${label} spu\u0161t\u011bna...`, "info");

  try {
    const result = await apiPost("/backup/execute", {
      plan_id: _plan.plan_id,
      dry_run: dryRun,
    });

    const taskId = result.task_id;
    if (!taskId) {
      showToast(`${label} dokon\u010dena`, "success");
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
          <span class="backup-progress-label" id="backup-progress-label">${label} prob\u00edh\u00e1... (\u00faloha ${taskId})</span>
        </div>`;
    }

    await pollTask(taskId, label);
  } catch (e) {
    showToast(`Chyba: ${e.message}`, "error");
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
        showToast(`${label} \u00fasp\u011b\u0161n\u011b dokon\u010dena`, "success");
        await refreshAll();
        return;
      }
      if (status.status === "failed") {
        showToast(`${label} selhala: ${status.error || "nezn\u00e1m\u00e1 chyba"}`, "error", 8000);
        return;
      }
    } catch {
      // keep polling
    }
  }
  showToast(`${label}: \u010dasov\u00fd limit vypr\u0161el`, "error");
}

// ---------------------------------------------------------------------------
// Verify
// ---------------------------------------------------------------------------

async function verifyBackups() {
  const btn = $("#btn-verify");
  const resultEl = $("#backup-verify-result");
  if (!btn || !resultEl) return;

  btn.disabled = true;
  btn.textContent = "\u23F3 Ov\u011b\u0159uji...";
  resultEl.innerHTML = `<div class="loading-sm">Kontroluji existenci soubor\u016f na \u00falo\u017ei\u0161t\u00edch...</div>`;

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
          <strong>Chyb\u011bj\u00edc\u00ed soubory:</strong>
          <ul>
            ${errors.slice(0, 20).map(err => `
              <li><code>${err.file || err.path}</code> \u2014 ${err.remote || "?"}</li>
            `).join("")}
            ${errors.length > 20 ? `<li class="text-muted">... a dal\u0161\u00edch ${errors.length - 20}</li>` : ""}
          </ul>
        </div>`;
    }

    resultEl.innerHTML = `
      <div class="backup-verify-card ${statusClass}">
        <div class="backup-verify-header">
          <span class="backup-verify-icon">${statusIcon}</span>
          <div class="backup-verify-stats">
            <span class="backup-verify-main">${pct}% ov\u011b\u0159eno</span>
            <span class="backup-verify-detail">${verified.toLocaleString("cs-CZ")} / ${total.toLocaleString("cs-CZ")} soubor\u016f v po\u0159\u00e1dku${missing > 0 ? `, ${missing.toLocaleString("cs-CZ")} chyb\u00ed` : ""}</span>
          </div>
        </div>
        ${errorsHtml}
      </div>`;
  } catch (e) {
    resultEl.innerHTML = `<div class="empty">Chyba ov\u011b\u0159en\u00ed: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = "\uD83D\uDD12 Ov\u011b\u0159it z\u00e1lohy";
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
      el.innerHTML = `<div class="empty">\u017d\u00e1dn\u00e9 z\u00e1lohy v manifestu${search ? ` pro "${search}"` : ""}</div>`;
      return;
    }

    el.innerHTML = `
      <div class="backup-manifest-info">
        Zobrazeno ${((_manifestPage - 1) * limit) + 1}\u2013${Math.min(_manifestPage * limit, totalItems)} z ${totalItems.toLocaleString("cs-CZ")} z\u00e1znam\u016f
      </div>
      <table class="backup-table backup-manifest-list">
        <thead>
          <tr>
            <th>Soubor</th>
            <th>Velikost</th>
            <th>\u00dalo\u017ei\u0161t\u011b</th>
            <th>Z\u00e1lohov\u00e1no</th>
            <th>Stav</th>
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
            const statusTitle = verified === true ? "Ov\u011b\u0159eno" : verified === false ? "Neov\u011b\u0159eno" : "Nezkontrolov\u00e1no";
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
        <button class="btn btn-sm btn-manifest-prev" ${_manifestPage <= 1 ? "disabled" : ""}>\u2190 P\u0159edchoz\u00ed</button>
        <span class="backup-manifest-page-info">Str\u00e1nka ${_manifestPage} / ${totalPages}</span>
        <button class="btn btn-sm btn-manifest-next" ${_manifestPage >= totalPages ? "disabled" : ""}>Dal\u0161\u00ed \u2192</button>
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
    el.innerHTML = `<div class="empty">Nepoda\u0159ilo se na\u010d\u00edst manifest: ${e.message}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function refreshAll() {
  const progressEl = document.getElementById("backup-progress");
  if (progressEl) progressEl.remove();

  await Promise.all([loadStats(), loadTargets(), loadManifest()]);
}
