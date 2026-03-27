/* GOD MODE Media Library — File sharing module */

import { api, apiPost, apiDelete } from "./api.js";
import { escapeHtml, fileName, showToast } from "./utils.js";
import { t } from "./i18n.js";

/**
 * Open a modal to create and manage share links for a file.
 * @param {string} filePath - Absolute path of the file to share
 */
export async function openShareModal(filePath) {
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.setAttribute("role", "dialog");
  overlay.setAttribute("aria-label", t("share.title"));

  overlay.innerHTML = `
    <div class="modal share-modal" style="max-width:520px">
      <button class="modal-close share-modal-close">&times;</button>
      <h3 style="margin-bottom:16px">&#128279; ${t("share.title")}</h3>
      <div class="share-file-name" style="font-size:12px;color:var(--text-muted);margin-bottom:16px;word-break:break-all">${escapeHtml(filePath)}</div>

      <div class="share-create-section">
        <div class="share-form-row">
          <label>${t("share.label")}</label>
          <input type="text" id="share-label" class="share-input" placeholder="${t("share.label")}" maxlength="100">
        </div>
        <div class="share-form-row">
          <label>${t("share.password")}</label>
          <input type="password" id="share-password" class="share-input" placeholder="${t("share.password")}">
        </div>
        <div class="share-form-row">
          <label>${t("share.expiration")}</label>
          <select id="share-expiration" class="share-input">
            <option value="">${t("share.never")}</option>
            <option value="1">${t("share.hours", { n: 1 })}</option>
            <option value="24">${t("share.hours", { n: 24 })}</option>
            <option value="168">${t("share.days", { n: 7 })}</option>
            <option value="720">${t("share.days", { n: 30 })}</option>
          </select>
        </div>
        <div class="share-form-row">
          <label>${t("share.max_downloads")}</label>
          <input type="number" id="share-max-downloads" class="share-input" placeholder="${t("share.never")}" min="1">
        </div>
        <button class="primary" id="share-create-btn" style="margin-top:12px;width:100%">${t("share.create")}</button>
      </div>

      <div class="share-link-result" id="share-link-result" style="display:none;margin-top:16px">
        <label style="font-size:13px;font-weight:600;margin-bottom:6px;display:block">${t("share.link")}</label>
        <div class="share-link-container">
          <input type="text" class="share-link-input" id="share-link-input" readonly>
          <button class="share-copy-btn" id="share-copy-btn" title="${t("share.copied")}">&#128203;</button>
        </div>
      </div>

      <div class="share-existing" style="margin-top:20px">
        <h4 style="font-size:13px;margin-bottom:8px">${t("share.existing")}</h4>
        <div id="share-list" class="share-list">
          <div class="loading"><div class="spinner"></div></div>
        </div>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);

  // Close handlers
  overlay.querySelector(".share-modal-close").addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });

  // Load existing shares
  _loadShares(overlay, filePath);

  // Create share handler
  overlay.querySelector("#share-create-btn").addEventListener("click", async () => {
    const label = overlay.querySelector("#share-label").value.trim();
    const password = overlay.querySelector("#share-password").value || null;
    const expiresVal = overlay.querySelector("#share-expiration").value;
    const maxDlVal = overlay.querySelector("#share-max-downloads").value;

    const body = { path: filePath, label };
    if (password) body.password = password;
    if (expiresVal) body.expires_hours = parseFloat(expiresVal);
    if (maxDlVal) body.max_downloads = parseInt(maxDlVal, 10);

    try {
      const share = await apiPost("/shares", body);
      showToast(t("share.created"), "success");

      // Show the link
      const shareUrl = `${window.location.origin}/shared/${share.token}`;
      const linkResult = overlay.querySelector("#share-link-result");
      const linkInput = overlay.querySelector("#share-link-input");
      linkInput.value = shareUrl;
      linkResult.style.display = "block";

      // Reload share list
      _loadShares(overlay, filePath);
    } catch (e) {
      showToast(t("general.error", { message: e.message }), "error");
    }
  });

  // Copy button
  overlay.querySelector("#share-copy-btn").addEventListener("click", () => {
    const linkInput = overlay.querySelector("#share-link-input");
    navigator.clipboard.writeText(linkInput.value).then(() => {
      showToast(t("share.copied"), "success");
    });
  });
}

async function _loadShares(overlay, filePath) {
  const listEl = overlay.querySelector("#share-list");
  if (!listEl) return;

  try {
    const data = await api(`/shares/file?path=${encodeURIComponent(filePath)}`);
    const shares = data.shares || [];

    if (shares.length === 0) {
      listEl.innerHTML = `<div style="font-size:13px;color:var(--text-muted);padding:12px 0;text-align:center">${t("share.no_shares")}</div>`;
      return;
    }

    let html = "";
    for (const share of shares) {
      const shareUrl = `${window.location.origin}/shared/${share.token}`;
      let badges = "";
      if (share.has_password) {
        badges += `<span class="share-badge share-badge-password">&#128274; ${t("share.protected")}</span>`;
      }
      if (share.expires_at) {
        const exp = new Date(share.expires_at);
        const now = new Date();
        if (exp < now) {
          badges += `<span class="share-badge share-badge-expired">${t("share.expired")}</span>`;
        } else {
          badges += `<span class="share-badge share-badge-expiry">${exp.toLocaleDateString()}</span>`;
        }
      }

      html += `<div class="share-item">
        <div class="share-item-info">
          <div class="share-item-label">${share.label ? escapeHtml(share.label) : escapeHtml(share.token.slice(0, 8) + "...")}</div>
          <div class="share-item-meta">
            ${t("share.downloads", { n: share.download_count })}
            ${share.max_downloads ? ` / ${share.max_downloads}` : ""}
            ${badges}
          </div>
        </div>
        <div class="share-item-actions">
          <button class="share-copy-link small" data-url="${escapeHtml(shareUrl)}" title="${t("share.copied")}">&#128203;</button>
          <button class="share-revoke-btn small" data-share-id="${share.id}" title="${t("share.revoke")}">&#128465;</button>
        </div>
      </div>`;
    }
    listEl.innerHTML = html;

    // Bind copy buttons
    listEl.querySelectorAll(".share-copy-link").forEach(btn => {
      btn.addEventListener("click", () => {
        navigator.clipboard.writeText(btn.dataset.url).then(() => {
          showToast(t("share.copied"), "success");
        });
      });
    });

    // Bind revoke buttons
    listEl.querySelectorAll(".share-revoke-btn").forEach(btn => {
      btn.addEventListener("click", async () => {
        const shareId = parseInt(btn.dataset.shareId, 10);
        try {
          await apiDelete(`/shares/${shareId}`);
          showToast(t("share.revoked"), "success");
          _loadShares(overlay, filePath);
        } catch (e) {
          showToast(t("general.error", { message: e.message }), "error");
        }
      });
    });
  } catch {
    listEl.innerHTML = `<div style="color:var(--red);padding:8px">${t("share.load_error")}</div>`;
  }
}
