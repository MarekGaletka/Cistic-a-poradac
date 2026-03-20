/* GOD MODE Media Library — Doctor page (upgraded with system info) */

import { api } from "../api.js";
import { escapeHtml, formatBytes, showToast } from "../utils.js";
import { t } from "../i18n.js";

export async function render(container) {
  try {
    const [depsData, sysData] = await Promise.all([
      api("/deps"),
      api("/system-info").catch(() => null),
    ]);

    let html = "";

    // System info section
    if (sysData) {
      html += `<div style="margin-bottom:16px">
        <h4 style="font-size:13px;font-weight:700;margin:0 0 10px;color:var(--text)">${t("doctor.system_info")}</h4>
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden">`;

      const sysRows = [
        { icon: "&#128196;", label: t("doctor.catalog_path"), value: sysData.catalog_path || "\u2014" },
        { icon: "&#128190;", label: t("doctor.catalog_size"), value: formatBytes(sysData.catalog_size) },
        { icon: "&#128247;", label: t("doctor.total_files"), value: String(sysData.total_files || 0) },
        { icon: "&#128190;", label: t("doctor.total_size"), value: formatBytes(sysData.total_size) },
        { icon: "&#128465;", label: t("doctor.quarantine_size"), value: formatBytes(sysData.quarantine_size) },
        { icon: "&#128013;", label: t("doctor.python_version"), value: sysData.python_version || "\u2014" },
        { icon: "&#128187;", label: t("doctor.platform"), value: sysData.platform || "\u2014" },
      ];

      for (const row of sysRows) {
        html += `<div class="dep-item" style="display:flex;align-items:center;gap:10px;padding:8px 14px;border-bottom:1px solid var(--border)">
          <span style="font-size:16px;width:24px;text-align:center">${row.icon}</span>
          <span style="font-size:12px;color:var(--text-muted);min-width:140px">${escapeHtml(row.label)}</span>
          <span style="font-size:13px;font-weight:500;word-break:break-all">${escapeHtml(row.value)}</span>
        </div>`;
      }
      html += `</div></div>`;
    }

    // Health summary
    const missingCount = depsData.dependencies.filter(d => !d.available).length;
    const healthBadge = missingCount === 0
      ? `<span style="display:inline-block;padding:4px 12px;background:#22c55e;color:#fff;border-radius:12px;font-size:12px;font-weight:600">&#10003; ${t("doctor.all_ok")}</span>`
      : `<span style="display:inline-block;padding:4px 12px;background:var(--red);color:#fff;border-radius:12px;font-size:12px;font-weight:600">&#9888; ${t("doctor.issues_found", { count: missingCount })}</span>`;

    html += `<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
      <h4 style="font-size:13px;font-weight:700;margin:0;color:var(--text)">${t("doctor.title")}</h4>
      ${healthBadge}
    </div>`;

    // Dependency cards
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px">';
    for (const d of depsData.dependencies) {
      const statusIcon = d.available ? "&#10003;" : "&#10007;";
      const statusColor = d.available ? "#22c55e" : "var(--red)";
      const statusLabel = d.available ? t("doctor.available") : t("doctor.missing");
      const ver = d.version ? ` (${escapeHtml(d.version)})` : "";

      let installHtml = "";
      if (!d.available && d.install_hint) {
        installHtml = `
          <div class="doctor-install-box" style="margin-top:8px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:8px 10px;display:flex;align-items:center;gap:8px">
            <code style="font-size:11px;flex:1;word-break:break-all">${escapeHtml(d.install_hint)}</code>
            <button class="btn-copy-cmd" data-cmd="${escapeHtml(d.install_hint)}" style="padding:4px 8px;font-size:11px;background:var(--accent);color:#fff;border:none;border-radius:4px;cursor:pointer;white-space:nowrap">${t("doctor.copy_command")}</button>
          </div>`;
      }

      html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px">
        <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
          <span style="color:${statusColor};font-size:16px;font-weight:700">${statusIcon}</span>
          <strong style="font-size:13px">${escapeHtml(d.name)}</strong>
          <span style="font-size:11px;color:var(--text-muted)">${ver}</span>
        </div>
        <div style="font-size:11px;color:${statusColor}">${statusLabel}</div>
        ${installHtml}
      </div>`;
    }
    html += "</div>";

    container.innerHTML = html;

    // Bind copy buttons
    container.querySelectorAll(".btn-copy-cmd").forEach(btn => {
      btn.addEventListener("click", () => {
        navigator.clipboard.writeText(btn.dataset.cmd).then(() => {
          showToast(t("doctor.copy_command"), "info");
        }).catch(() => {
          // Fallback: select and copy
          const range = document.createRange();
          const codeEl = btn.previousElementSibling;
          if (codeEl) {
            range.selectNode(codeEl);
            window.getSelection().removeAllRanges();
            window.getSelection().addRange(range);
            document.execCommand("copy");
            window.getSelection().removeAllRanges();
          }
        });
      });
    });
  } catch (e) {
    container.innerHTML = `<div class="empty" style="padding:16px">${t("general.error", { message: e.message })}</div>`;
  }
}
