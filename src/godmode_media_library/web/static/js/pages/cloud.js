/* GOD MODE Media Library — Cloud Storage page */

import { t } from "../i18n.js";
import { $, showToast, formatBytes } from "../utils.js";
import { api, apiPost, apiDelete } from "../api.js";

let _container = null;

export async function render(container) {
  _container = container;

  container.innerHTML = `
    <div class="cloud-page">
      <div class="cloud-header">
        <h2>${t("cloud.title")}</h2>
      </div>
      <div id="cloud-status" class="cloud-status"></div>
      <div class="cloud-sections">
        <div id="cloud-sources"></div>
        <div id="cloud-native"></div>
        <div id="cloud-providers"></div>
      </div>
    </div>`;

  await loadStatus();
}

async function loadStatus() {
  try {
    const [status, native] = await Promise.all([
      api("/cloud/status"),
      api("/cloud/native"),
    ]);

    renderStatusBar(status);
    renderSources(status.sources || []);
    renderNativePaths(native.paths || []);
    renderProviders(status);
  } catch (e) {
    const el = $("#cloud-status");
    if (el) el.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

function renderStatusBar(status) {
  const el = $("#cloud-status");
  if (!el) return;

  const rcloneOk = status.rclone_installed;
  const version = status.rclone_version || "";
  const sourceCount = (status.sources || []).length;

  el.innerHTML = `
    <div class="cloud-status-bar">
      <div class="cloud-status-item">
        <span class="status-dot ${rcloneOk ? "status-online" : "status-offline"}"></span>
        <span>rclone ${rcloneOk ? version : t("cloud.not_installed")}</span>
      </div>
      <div class="cloud-status-item">
        <span class="stat-value">${sourceCount}</span>
        <span>${t("cloud.sources_count")}</span>
      </div>
      ${!rcloneOk ? `
        <button class="btn btn-primary btn-small" onclick="window.open('https://rclone.org/install/')">
          ${t("cloud.install_rclone")}
        </button>
      ` : ""}
    </div>`;
}

function _statusBadge(done) {
  if (done) return `<span class="cloud-badge cloud-badge-ok" title="Hotovo">✓</span>`;
  return `<span class="cloud-badge cloud-badge-none" title="Neprovedeno">●</span>`;
}

function _verifyBar(diskCount, catCount) {
  if (diskCount === 0 && catCount === 0) {
    return `<div class="cloud-verify-bar verify-empty">
      <div class="cloud-verify-icon">⚠</div>
      <span>Složka je prázdná — zkontrolujte připojení</span>
    </div>`;
  }
  const allIndexed = diskCount > 0 && catCount >= diskCount;
  const partial = catCount > 0 && catCount < diskCount;
  const pct = diskCount > 0 ? Math.round(catCount / diskCount * 100) : 0;
  const barClass = allIndexed ? "verify-ok" : partial ? "verify-partial" : "verify-none";
  return `
    <div class="cloud-verify-bar ${barClass}">
      <div class="cloud-verify-icon">${allIndexed ? "✓" : partial ? "◐" : "○"}</div>
      <div class="cloud-verify-info">
        <span>${diskCount} na disku → ${catCount} indexováno${!allIndexed && diskCount > 0 ? ` (${pct}%)` : ""}</span>
        <div class="cloud-verify-progress"><div class="cloud-verify-fill" style="width:${Math.min(pct, 100)}%"></div></div>
      </div>
    </div>`;
}

function renderSources(sources) {
  const el = $("#cloud-sources");
  if (!el) return;

  if (sources.length === 0) {
    el.innerHTML = `
      <div class="cloud-section">
        <h3>${t("cloud.connected")}</h3>
        <div class="empty">${t("cloud.no_sources")}</div>
      </div>`;
    return;
  }

  el.innerHTML = `
    <div class="cloud-section">
      <h3>${t("cloud.connected")}</h3>
      <div class="cloud-sources-grid">
        ${sources.map(s => {
          const isRclone = s.source_type === "rclone";
          const connected = s.available;
          const path = s.mount_path || s.sync_path;

          // Build action buttons based on state
          let actions = "";

          if (isRclone) {
            if (!connected) {
              // Not connected — show Mount button
              actions += `<button class="btn btn-small btn-mount" data-remote="${s.name}">
                ${_statusBadge(false)} ${t("cloud.mount")}
              </button>`;
            }
            // Sync (download locally) — show with status
            actions += `<button class="btn btn-small btn-sync" data-remote="${s.name}">
              ${_statusBadge(s.synced)} ${t("cloud.sync")}
            </button>`;
          }

          // Scan — only when available (has local path)
          if (connected) {
            actions += `<button class="btn btn-small btn-scan" data-path="${path}">
              ${_statusBadge(s.scanned)} ${t("cloud.scan")}
            </button>`;
          }

          if (isRclone) {
            // Backup to this remote
            actions += `<button class="btn btn-small btn-primary btn-backup" data-remote="${s.name}">
              ${t("cloud.backup")}
            </button>`;
            // Browse → Otevřít
            actions += `<button class="btn btn-small btn-browse" data-remote="${s.name}">
              ${t("cloud.browse")}
            </button>`;
            // Disconnect — only when connected
            if (connected) {
              actions += `<button class="btn btn-small btn-disconnect" data-remote="${s.name}">
                ${t("cloud.disconnect")}
              </button>`;
            }
          }

          // Info lines
          let infoLine = "";
          if (s.mounted) infoLine += `<div class="cloud-source-path">${t("cloud.mounted_at")}: ${s.mount_path}</div>`;
          if (s.synced) infoLine += `<div class="cloud-source-path">${t("cloud.synced_to")}: ${s.sync_path}</div>`;

          // Verify bar
          const verifyHtml = connected ? _verifyBar(s.disk_count || 0, s.file_count || 0) : "";

          return `
          <div class="cloud-source-card ${connected ? "cloud-source-online" : "cloud-source-offline"}">
            <div class="cloud-source-header">
              <span class="cloud-source-icon">${s.icon}</span>
              <div class="cloud-source-info">
                <span class="cloud-source-name">${s.name}</span>
                <span class="cloud-source-provider">${s.provider} (${s.remote_type})</span>
              </div>
              <span class="status-dot ${connected ? "status-online" : "status-offline"}"></span>
            </div>
            <div class="cloud-source-actions">${actions}</div>
            ${infoLine}
            ${verifyHtml}
          </div>`;
        }).join("")}
      </div>
    </div>`;

  // Helper: set button to spinner state
  function _btnSpinner(btn) {
    btn.disabled = true;
    btn._origHtml = btn.innerHTML;
    btn.innerHTML = `<span class="cloud-badge cloud-badge-spin"></span> ${btn.textContent.trim()}`;
  }
  // Helper: set button to done state (green check)
  function _btnDone(btn) {
    btn.innerHTML = `<span class="cloud-badge cloud-badge-ok">✓</span> ${btn.textContent.trim()}`;
    btn.disabled = false;
  }
  // Helper: restore button
  function _btnRestore(btn) {
    btn.innerHTML = btn._origHtml || btn.innerHTML;
    btn.disabled = false;
  }

  // Mount buttons
  el.querySelectorAll(".btn-mount").forEach(btn => {
    btn.addEventListener("click", async () => {
      const remote = btn.dataset.remote;
      _btnSpinner(btn);
      try {
        const result = await apiPost("/cloud/mount", { remote });
        if (result.success) {
          showToast(t("cloud.mount_success", { name: remote }), "success");
          await loadStatus();
        } else {
          showToast(result.message || t("cloud.mount_failed"), "error", 8000);
          _btnRestore(btn);
        }
      } catch (e) {
        showToast(e.message, "error");
        _btnRestore(btn);
      }
    });
  });

  // Sync buttons
  el.querySelectorAll(".btn-sync").forEach(btn => {
    btn.addEventListener("click", async () => {
      const remote = btn.dataset.remote;
      _btnSpinner(btn);
      try {
        const result = await apiPost("/cloud/sync", { remote });
        showToast(t("cloud.sync_started", { name: remote, task_id: result.task_id }), "success");
        _btnDone(btn);
      } catch (e) {
        showToast(e.message, "error");
        _btnRestore(btn);
      }
    });
  });

  // Scan buttons
  el.querySelectorAll(".btn-scan").forEach(btn => {
    btn.addEventListener("click", async () => {
      const path = btn.dataset.path;
      _btnSpinner(btn);
      try {
        await apiPost("/scan", { roots: [path], workers: 4, extract_exiftool: true });
        showToast(t("cloud.scan_started"), "success");
        _btnDone(btn);
      } catch (e) {
        showToast(e.message, "error");
        _btnRestore(btn);
      }
    });
  });

  // Browse buttons
  el.querySelectorAll(".btn-browse").forEach(btn => {
    btn.addEventListener("click", () => browseRemote(btn.dataset.remote));
  });

  // Disconnect buttons
  el.querySelectorAll(".btn-disconnect").forEach(btn => {
    btn.addEventListener("click", async () => {
      const remote = btn.dataset.remote;
      if (!confirm(t("cloud.disconnect_confirm", { name: remote }))) return;
      _btnSpinner(btn);
      try {
        const result = await apiDelete(`/cloud/remote/${remote}`);
        if (result.success) {
          showToast(t("cloud.disconnected", { name: remote }), "success");
          await loadStatus();
        }
      } catch (e) {
        showToast(e.message, "error");
        _btnRestore(btn);
      }
    });
  });

  // Backup buttons
  el.querySelectorAll(".btn-backup").forEach(btn => {
    btn.addEventListener("click", () => showBackupModal(btn.dataset.remote));
  });
}

function renderNativePaths(paths) {
  const el = $("#cloud-native");
  if (!el) return;

  if (paths.length === 0) return;

  el.innerHTML = `
    <div class="cloud-section">
      <h3>${t("cloud.native_paths")}</h3>
      <div class="cloud-native-list">
        ${paths.map(p => {
          // Grouped entry (e.g. iCloud Apps with sub-paths)
          const nVerify = _verifyBar(p.disk_count || 0, p.file_count || 0);
          if (p.apps && p.apps.length) {
            return `
              <div class="cloud-native-item cloud-native-group">
                <span class="cloud-source-icon">${p.icon}</span>
                <div class="cloud-source-info">
                  <span class="cloud-source-name">${p.name}</span>
                  <span class="cloud-source-path">${p.app_count} ${t("cloud.apps_synced")}</span>
                </div>
                <button class="btn btn-small btn-scan-native" data-path="${p.path}">
                  ${_statusBadge(p.scanned)} ${t("cloud.scan")}
                </button>
                <button class="btn btn-small btn-expand-group" aria-expanded="false" title="${t("cloud.show_details")}">&#9660;</button>
              </div>
              ${nVerify}
              <div class="cloud-native-sublist hidden">
                ${p.apps.map(a => `
                  <div class="cloud-native-subitem">
                    <span class="cloud-sub-name">${a.name}</span>
                    <span class="cloud-sub-path">${a.path}</span>
                  </div>
                `).join("")}
              </div>`;
          }
          return `
            <div class="cloud-native-item">
              <span class="cloud-source-icon">${p.icon}</span>
              <div class="cloud-source-info">
                <span class="cloud-source-name">${p.name}</span>
                <span class="cloud-source-path">${p.path}</span>
              </div>
              <button class="btn btn-small btn-scan-native" data-path="${p.path}">
                ${_statusBadge(p.scanned)} ${t("cloud.scan")}
              </button>
            </div>
            ${nVerify}`;
        }).join("")}
      </div>
    </div>`;

  // Scan buttons
  el.querySelectorAll(".btn-scan-native").forEach(btn => {
    btn.addEventListener("click", async () => {
      const path = btn.dataset.path;
      btn.disabled = true;
      try {
        await apiPost("/scan", { roots: [path], workers: 4, extract_exiftool: true });
        showToast(t("cloud.scan_started"), "success");
      } catch (e) {
        showToast(e.message, "error");
      }
      btn.disabled = false;
    });
  });

  // Expand/collapse grouped entries
  el.querySelectorAll(".btn-expand-group").forEach(btn => {
    btn.addEventListener("click", () => {
      const sublist = btn.closest(".cloud-native-group").nextElementSibling;
      if (sublist && sublist.classList.contains("cloud-native-sublist")) {
        const expanded = sublist.classList.toggle("hidden");
        btn.innerHTML = expanded ? "&#9660;" : "&#9650;";
        btn.setAttribute("aria-expanded", String(!expanded));
      }
    });
  });
}

function renderProviders(status) {
  const el = $("#cloud-providers");
  if (!el) return;

  const providers = status.providers || {};

  el.innerHTML = `
    <div class="cloud-section">
      <h3>${t("cloud.add_provider")}</h3>
      <div class="cloud-providers-grid">
        ${Object.entries(providers).map(([key, info]) => `
          <button class="cloud-provider-card" data-provider="${key}">
            <span class="cloud-provider-icon">${info.icon}</span>
            <span class="cloud-provider-name">${info.label}</span>
          </button>
        `).join("")}
      </div>
    </div>`;

  el.querySelectorAll(".cloud-provider-card").forEach(card => {
    card.addEventListener("click", () => showConnectModal(card.dataset.provider));
  });
}

async function showConnectModal(providerKey) {
  try {
    const info = await api(`/cloud/provider-fields/${providerKey}`);
    const isOAuth = info.auth === "oauth";
    const defaultName = providerKey.replace(/\s+/g, "");

    const overlay = document.createElement("div");
    overlay.className = "shortcuts-overlay";
    overlay.innerHTML = `
      <div class="shortcuts-modal" style="max-width:500px">
        <h3>${info.icon} ${t("cloud.connect_to")} ${info.provider}</h3>
        <form id="cloud-connect-form" class="cloud-connect-form">
          <div class="cloud-field">
            <label>${t("cloud.remote_name")}</label>
            <input type="text" name="name" value="${defaultName}" required
                   placeholder="${t("cloud.remote_name_hint")}" />
          </div>
          ${isOAuth ? `
            <p class="cloud-oauth-hint">${t("cloud.oauth_hint")}</p>
          ` : info.fields.map(f => `
            <div class="cloud-field">
              <label>${f.label}${f.required ? " *" : ""}</label>
              ${f.type === "select" ? `
                <select name="${f.key}" ${f.required ? "required" : ""}>
                  <option value="">-- ${t("cloud.select")} --</option>
                  ${f.options.map(o => `<option value="${o}">${o}</option>`).join("")}
                </select>
              ` : `
                <input type="${f.type}" name="${f.key}" ${f.required ? "required" : ""}
                       placeholder="${f.label}" />
              `}
            </div>
          `).join("")}
          <div class="cloud-connect-actions">
            <button type="submit" class="btn btn-primary" id="btn-connect">
              ${isOAuth ? t("cloud.authorize") : t("cloud.connect")}
            </button>
            <button type="button" class="btn" id="btn-cancel-connect">${t("general.close")}</button>
          </div>
          <div id="cloud-connect-status" class="cloud-connect-status hidden"></div>
        </form>
      </div>`;

    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
    document.body.appendChild(overlay);

    overlay.querySelector("#btn-cancel-connect")?.addEventListener("click", () => overlay.remove());

    overlay.querySelector("#cloud-connect-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const form = e.target;
      const formData = new FormData(form);
      const name = formData.get("name").trim();
      if (!name) return;

      const statusEl = overlay.querySelector("#cloud-connect-status");
      const btn = overlay.querySelector("#btn-connect");
      btn.disabled = true;
      statusEl.classList.remove("hidden");
      statusEl.className = "cloud-connect-status";
      statusEl.textContent = isOAuth ? t("cloud.oauth_waiting") : t("cloud.connecting");

      const credentials = {};
      for (const [k, v] of formData.entries()) {
        if (k !== "name" && v) credentials[k] = v;
      }

      try {
        const result = await apiPost("/cloud/connect", {
          provider_key: providerKey,
          name,
          credentials,
        });

        if (result.oauth) {
          // OAuth flow — poll for completion
          statusEl.textContent = t("cloud.oauth_browser");
          await _pollOAuth(providerKey, name, statusEl, overlay);
        } else {
          // Credential-based — test connection
          statusEl.textContent = t("cloud.testing");
          const test = await apiPost(`/cloud/test/${name}`);
          if (test.success) {
            statusEl.className = "cloud-connect-status status-success";
            statusEl.textContent = t("cloud.connected_ok");
            showToast(t("cloud.connect_success", { name }), "success");
            setTimeout(() => { overlay.remove(); loadStatus(); }, 1500);
          } else {
            statusEl.className = "cloud-connect-status status-error";
            statusEl.textContent = t("cloud.connect_test_failed", { message: test.message });
            btn.disabled = false;
          }
        }
      } catch (err) {
        statusEl.className = "cloud-connect-status status-error";
        statusEl.textContent = err.message;
        btn.disabled = false;
      }
    });
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function _pollOAuth(providerKey, name, statusEl, overlay) {
  const maxAttempts = 120;  // 2 minutes
  for (let i = 0; i < maxAttempts; i++) {
    await new Promise(r => setTimeout(r, 1000));
    try {
      const status = await api(`/cloud/oauth/status/${name}`);
      if (status.status === "completed") {
        statusEl.textContent = t("cloud.oauth_finalizing");
        const result = await apiPost("/cloud/oauth/finalize", {
          provider_key: providerKey,
          name,
          credentials: { token: status.token },
        });
        if (result.success) {
          statusEl.className = "cloud-connect-status status-success";
          statusEl.textContent = t("cloud.connected_ok");
          showToast(t("cloud.connect_success", { name }), "success");
          setTimeout(() => { overlay.remove(); loadStatus(); }, 1500);
        } else {
          statusEl.className = "cloud-connect-status status-error";
          statusEl.textContent = result.message;
        }
        return;
      } else if (status.status === "error") {
        statusEl.className = "cloud-connect-status status-error";
        statusEl.textContent = status.message;
        return;
      }
    } catch { /* keep polling */ }
  }
  statusEl.className = "cloud-connect-status status-error";
  statusEl.textContent = t("cloud.oauth_timeout");
}

async function browseRemote(remoteName, path = "") {
  try {
    const data = await api(`/cloud/remote/${remoteName}/browse?path=${encodeURIComponent(path)}`);
    const items = data.items || [];

    const overlay = document.createElement("div");
    overlay.className = "shortcuts-overlay";

    const breadcrumb = path ? path.split("/").filter(Boolean) : [];

    overlay.innerHTML = `
      <div class="shortcuts-modal" style="max-width:700px;max-height:80vh;overflow-y:auto">
        <h3>${remoteName}:${path || "/"}</h3>
        ${breadcrumb.length ? `
          <div style="margin-bottom:12px">
            <button class="btn btn-small btn-browse-up" data-path="${breadcrumb.slice(0, -1).join("/")}">${t("cloud.up")}</button>
          </div>
        ` : ""}
        <div class="cloud-browse-list">
          ${items.length === 0 ? `<div class="empty">${t("cloud.empty_folder")}</div>` : ""}
          ${items.map(item => `
            <div class="cloud-browse-item ${item.IsDir ? "cloud-browse-dir" : ""}"
                 data-path="${path ? path + "/" : ""}${item.Name}"
                 data-is-dir="${item.IsDir}">
              <span class="cloud-browse-icon">${item.IsDir ? "\U0001f4c1" : "\U0001f4c4"}</span>
              <span class="cloud-browse-name">${item.Name}</span>
              <span class="cloud-browse-size">${item.IsDir ? "" : formatBytes(item.Size || 0)}</span>
            </div>
          `).join("")}
        </div>
        <div style="margin-top:16px;display:flex;gap:8px">
          <button class="btn btn-primary btn-sync-path" data-remote="${remoteName}" data-path="${path}">${t("cloud.sync_this")}</button>
          <button class="btn" id="btn-close-browse">${t("general.close")}</button>
        </div>
      </div>`;

    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
    document.body.appendChild(overlay);

    overlay.querySelector("#btn-close-browse")?.addEventListener("click", () => overlay.remove());

    // Navigate into directories
    overlay.querySelectorAll(".cloud-browse-dir").forEach(item => {
      item.addEventListener("dblclick", () => {
        overlay.remove();
        browseRemote(remoteName, item.dataset.path);
      });
    });

    // Up button
    overlay.querySelector(".btn-browse-up")?.addEventListener("click", () => {
      overlay.remove();
      browseRemote(remoteName, overlay.querySelector(".btn-browse-up").dataset.path);
    });

    // Sync this path
    overlay.querySelector(".btn-sync-path")?.addEventListener("click", async () => {
      overlay.remove();
      try {
        const result = await apiPost("/cloud/sync", { remote: remoteName, remote_path: path });
        showToast(t("cloud.sync_started", { name: remoteName, task_id: result.task_id }), "success");
      } catch (e) {
        showToast(e.message, "error");
      }
    });
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function showBackupModal(remoteName) {
  // Fetch scanned sources to let user pick what to back up
  let sources = [];
  try {
    const stats = await api("/sources");
    sources = (stats.sources || []).filter(s => s.file_count > 0);
  } catch {
    // Fallback: try catalog info
    try {
      const info = await api("/stats");
      if (info.last_scan_root) {
        for (const r of info.last_scan_root.split(";")) {
          if (r.trim()) sources.push({ path: r.trim(), name: r.trim().split("/").pop(), file_count: "?" });
        }
      }
    } catch { /* empty */ }
  }

  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";
  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:560px">
      <h3>☁️ ${t("cloud.backup_title", { name: remoteName })}</h3>
      <form id="backup-form" class="cloud-connect-form">
        <div class="cloud-field">
          <label>${t("cloud.backup_folder")}</label>
          <input type="text" name="remote_path" value="GML-Backup" />
        </div>
        <div class="cloud-field">
          <label>${t("cloud.backup_sources")}</label>
          ${sources.length > 0 ? `
            <div class="backup-source-list">
              ${sources.map((s, i) => `
                <label class="backup-source-item">
                  <input type="checkbox" name="source_${i}" value="${s.path}" checked />
                  <span>${s.name || s.path}</span>
                  <span class="backup-source-count">${s.file_count} souborů</span>
                </label>
              `).join("")}
            </div>
          ` : `<p class="text-muted">${t("cloud.backup_no_sources")}</p>`}
        </div>
        <label class="backup-dry-run">
          <input type="checkbox" name="dry_run" />
          ${t("cloud.backup_dry")}
        </label>
        <div class="cloud-connect-actions">
          <button type="submit" class="btn btn-primary" ${sources.length === 0 ? "disabled" : ""}>
            ${t("cloud.backup_start")}
          </button>
          <button type="button" class="btn" id="btn-cancel-backup">${t("general.close")}</button>
        </div>
        <div id="backup-status" class="cloud-connect-status hidden"></div>
      </form>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  overlay.querySelector("#btn-cancel-backup")?.addEventListener("click", () => overlay.remove());

  overlay.querySelector("#backup-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const form = e.target;
    const remotePath = form.querySelector('[name="remote_path"]').value.trim();
    const dryRun = form.querySelector('[name="dry_run"]').checked;
    const statusEl = overlay.querySelector("#backup-status");

    // Collect selected sources
    const selectedPaths = [];
    form.querySelectorAll('input[type="checkbox"][name^="source_"]').forEach(cb => {
      if (cb.checked) selectedPaths.push(cb.value);
    });

    if (selectedPaths.length === 0) {
      showToast(t("cloud.backup_no_sources"), "error");
      return;
    }

    const submitBtn = form.querySelector('[type="submit"]');
    submitBtn.disabled = true;
    submitBtn.innerHTML = `<span class="cloud-badge cloud-badge-spin"></span> Zálohuji...`;
    statusEl.classList.remove("hidden");
    statusEl.className = "cloud-connect-status";
    statusEl.textContent = `Zálohuji ${selectedPaths.length} zdrojů na ${remoteName}:${remotePath}...`;

    try {
      const result = await apiPost("/cloud/backup", {
        remote: remoteName,
        remote_path: remotePath,
        source_paths: selectedPaths,
        dry_run: dryRun,
      });
      statusEl.className = "cloud-connect-status status-success";
      statusEl.textContent = dryRun
        ? `Zkušební běh dokončen (úloha ${result.task_id})`
        : t("cloud.backup_started", { count: selectedPaths.length, name: remoteName });
      showToast(t("cloud.backup_started", { count: selectedPaths.length, name: remoteName }), "success");
      setTimeout(() => { overlay.remove(); loadStatus(); }, 3000);
    } catch (err) {
      statusEl.className = "cloud-connect-status status-error";
      statusEl.textContent = err.message;
      submitBtn.disabled = false;
      submitBtn.textContent = t("cloud.backup_start");
    }
  });
}
