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
        ${sources.map(s => `
          <div class="cloud-source-card ${s.available ? "cloud-source-online" : "cloud-source-offline"}">
            <div class="cloud-source-header">
              <span class="cloud-source-icon">${s.icon}</span>
              <div class="cloud-source-info">
                <span class="cloud-source-name">${s.name}</span>
                <span class="cloud-source-provider">${s.provider} (${s.remote_type})</span>
              </div>
              <span class="status-dot ${s.available ? "status-online" : "status-offline"}"></span>
            </div>
            <div class="cloud-source-actions">
              ${s.source_type === "rclone" && !s.mounted ? `
                <button class="btn btn-small btn-mount" data-remote="${s.name}">${t("cloud.mount")}</button>
              ` : ""}
              ${s.source_type === "rclone" ? `
                <button class="btn btn-small btn-sync" data-remote="${s.name}">${t("cloud.sync")}</button>
              ` : ""}
              ${s.available ? `
                <button class="btn btn-small btn-scan" data-path="${s.mount_path || s.sync_path}">${t("cloud.scan")}</button>
              ` : ""}
              ${s.source_type === "rclone" ? `
                <button class="btn btn-small btn-browse" data-remote="${s.name}">${t("cloud.browse")}</button>
                <button class="btn btn-small btn-disconnect" data-remote="${s.name}">${t("cloud.disconnect")}</button>
              ` : ""}
            </div>
            ${s.mounted ? `<div class="cloud-source-path">${t("cloud.mounted_at")}: ${s.mount_path}</div>` : ""}
            ${s.synced ? `<div class="cloud-source-path">${t("cloud.synced_to")}: ${s.sync_path}</div>` : ""}
          </div>
        `).join("")}
      </div>
    </div>`;

  // Mount buttons
  el.querySelectorAll(".btn-mount").forEach(btn => {
    btn.addEventListener("click", async () => {
      const remote = btn.dataset.remote;
      btn.disabled = true;
      btn.textContent = "...";
      try {
        const result = await apiPost("/cloud/mount", { remote });
        if (result.success) {
          showToast(t("cloud.mount_success", { name: remote }), "success");
          await loadStatus();
        } else {
          showToast(t("cloud.mount_failed"), "error");
        }
      } catch (e) {
        showToast(e.message, "error");
      }
      btn.disabled = false;
      btn.textContent = t("cloud.mount");
    });
  });

  // Sync buttons
  el.querySelectorAll(".btn-sync").forEach(btn => {
    btn.addEventListener("click", async () => {
      const remote = btn.dataset.remote;
      btn.disabled = true;
      btn.textContent = "...";
      try {
        const result = await apiPost("/cloud/sync", { remote });
        showToast(t("cloud.sync_started", { name: remote, task_id: result.task_id }), "success");
      } catch (e) {
        showToast(e.message, "error");
      }
      btn.disabled = false;
      btn.textContent = t("cloud.sync");
    });
  });

  // Scan buttons
  el.querySelectorAll(".btn-scan").forEach(btn => {
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

  // Browse buttons
  el.querySelectorAll(".btn-browse").forEach(btn => {
    btn.addEventListener("click", () => browseRemote(btn.dataset.remote));
  });

  // Disconnect buttons
  el.querySelectorAll(".btn-disconnect").forEach(btn => {
    btn.addEventListener("click", async () => {
      const remote = btn.dataset.remote;
      if (!confirm(t("cloud.disconnect_confirm", { name: remote }))) return;
      btn.disabled = true;
      try {
        const result = await apiDelete(`/cloud/remote/${remote}`);
        if (result.success) {
          showToast(t("cloud.disconnected", { name: remote }), "success");
          await loadStatus();
        }
      } catch (e) {
        showToast(e.message, "error");
      }
      btn.disabled = false;
    });
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
          if (p.apps && p.apps.length) {
            return `
              <div class="cloud-native-item cloud-native-group">
                <span class="cloud-source-icon">${p.icon}</span>
                <div class="cloud-source-info">
                  <span class="cloud-source-name">${p.name}</span>
                  <span class="cloud-source-path">${p.app_count} ${t("cloud.apps_synced")}</span>
                </div>
                <button class="btn btn-small btn-scan-native" data-path="${p.path}">${t("cloud.scan")}</button>
                <button class="btn btn-small btn-expand-group" aria-expanded="false" title="${t("cloud.show_details")}">&#9660;</button>
              </div>
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
              <button class="btn btn-small btn-scan-native" data-path="${p.path}">${t("cloud.scan")}</button>
            </div>`;
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
