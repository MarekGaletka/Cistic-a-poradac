/* GOD MODE Media Library — Duplicates page */

import { api, apiPost } from "../api.js";
import { $, content, formatBytes, escapeHtml, fileName, showToast } from "../utils.js";
import { t } from "../i18n.js";
import { showVisualDiff } from "../modal.js";

export async function render(container) {
  try {
    const data = await api("/duplicates?limit=50");
    if (!data.groups.length) {
      container.innerHTML = `<h2>${t("duplicates.title")}</h2><div class="empty"><div class="empty-icon">&#9989;</div><div class="empty-text">${t("duplicates.empty_title")}</div><div class="empty-hint">${t("duplicates.empty_hint")}</div></div>`;
      return;
    }
    let html = `<h2>${t("duplicates.title")} <span style="color:var(--text-muted);font-size:14px">(${t("duplicates.groups", { count: data.total_groups })})</span></h2>`;
    html += `<table><tr><th>${t("duplicates.group")}</th><th>${t("duplicates.files")}</th><th>${t("duplicates.size")}</th><th>${t("duplicates.action")}</th></tr>`;
    for (const g of data.groups) {
      html += `<tr>
        <td class="path">${escapeHtml(g.group_id.slice(0, 12))}</td>
        <td>${g.file_count}</td>
        <td>${formatBytes(g.total_size)}</td>
        <td><button class="btn-diff" data-group-id="${escapeHtml(g.group_id)}" aria-label="${t("duplicates.diff")} ${escapeHtml(g.group_id.slice(0, 8))}">${t("duplicates.diff")}</button></td>
      </tr>`;
    }
    html += "</table>";
    html += '<div id="diff-detail" aria-live="polite"></div>';
    container.innerHTML = html;

    // Bind diff buttons
    container.querySelectorAll(".btn-diff").forEach(btn => {
      btn.addEventListener("click", () => showDiff(btn.dataset.groupId));
    });
  } catch (e) {
    container.innerHTML = `<h2>${t("duplicates.title")}</h2><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

async function showDiff(groupId) {
  const el = $("#diff-detail");
  el.innerHTML = `<div class="loading"><div class="spinner" role="status" aria-label="${t("general.loading")}"></div>${t("general.loading")}</div>`;
  try {
    const [groupData, diffData] = await Promise.all([
      api(`/duplicates/${encodeURIComponent(groupId)}`),
      api(`/duplicates/${encodeURIComponent(groupId)}/diff`),
    ]);

    const files = groupData.files || [];
    const scores = diffData.scores || {};

    let winnerPath = null;
    let winnerScore = -1;
    for (const [path, score] of Object.entries(scores)) {
      if (score > winnerScore) { winnerScore = score; winnerPath = path; }
    }

    let html = `<h2 style="margin-top:20px">${t("duplicates.metadata_diff", { id: groupId.slice(0, 12) })}</h2>`;

    // Side-by-side file comparison with thumbnails
    html += '<div class="dup-compare">';
    for (const f of files) {
      const path = f.path;
      const score = scores[path];
      const isWinner = path === winnerPath && files.length > 1;
      html += `<div class="dup-column ${isWinner ? "dup-winner" : ""}">`;
      const thumbSrc = `/api/thumbnail${encodeURI(path)}?size=250`;
      html += `<img class="dup-thumb" src="${thumbSrc}" onerror="this.outerHTML='<div class=\\'dup-thumb-placeholder\\'>&#128444;</div>'" alt="${escapeHtml(fileName(path))}">`;
      html += `<div class="dup-filename">${escapeHtml(fileName(path))}</div>`;
      if (score != null) {
        const level = score >= 30 ? "high" : score >= 15 ? "medium" : "low";
        html += `<span class="richness-badge ${level}">${Number(score).toFixed(1)} pts${isWinner ? " &#9733;" : ""}</span>`;
      }
      html += `<div class="dup-path" title="${escapeHtml(path)}">${escapeHtml(path)}</div>`;
      html += '</div>';
    }
    html += '</div>';

    // Action buttons
    if (files.length >= 2) {
      html += '<div style="text-align:center;margin-bottom:16px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap">';
      if (files.length === 2) {
        const pA = files[0].path;
        const pB = files[1].path;
        const sA = scores[pA] ?? null;
        const sB = scores[pB] ?? null;
        html += `<button class="primary btn-visual-compare" data-pa="${escapeHtml(pA)}" data-pb="${escapeHtml(pB)}" data-sa="${sA}" data-sb="${sB}">${t("duplicates.visual_compare")}</button>`;
      }
      html += `<button class="primary btn-merge-quarantine" data-group-id="${escapeHtml(groupId)}" data-keep-path="${escapeHtml(winnerPath || files[0].path)}">${t("duplicates.merge_quarantine")}</button>`;
      html += '</div>';
    }

    // Diff sections
    if (Object.keys(diffData.unanimous).length) {
      html += `<details class="diff-section"><summary class="diff-toggle unanimous">${t("duplicates.unanimous", { count: Object.keys(diffData.unanimous).length })}</summary>`;
      for (const [tag, val] of Object.entries(diffData.unanimous)) {
        html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(JSON.stringify(val))}</span></div>`;
      }
      html += "</details>";
    }

    if (Object.keys(diffData.partial).length) {
      html += `<details class="diff-section" open><summary class="diff-toggle partial">${t("duplicates.partial", { count: Object.keys(diffData.partial).length })}</summary>`;
      for (const [tag, sources] of Object.entries(diffData.partial)) {
        for (const [path, val] of Object.entries(sources)) {
          html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(fileName(path))}: ${escapeHtml(JSON.stringify(val))}</span></div>`;
        }
      }
      html += "</details>";
    }

    if (Object.keys(diffData.conflicts).length) {
      html += `<details class="diff-section" open><summary class="diff-toggle conflicts">${t("duplicates.conflicts_tags", { count: Object.keys(diffData.conflicts).length })}</summary>`;
      for (const [tag, sources] of Object.entries(diffData.conflicts)) {
        for (const [path, val] of Object.entries(sources)) {
          html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(fileName(path))}: ${escapeHtml(JSON.stringify(val))}</span></div>`;
        }
      }
      html += "</details>";
    }

    el.innerHTML = html;

    // Bind visual compare button
    const vcBtn = el.querySelector(".btn-visual-compare");
    if (vcBtn) {
      vcBtn.addEventListener("click", () => {
        const sa = vcBtn.dataset.sa === "null" ? null : Number(vcBtn.dataset.sa);
        const sb = vcBtn.dataset.sb === "null" ? null : Number(vcBtn.dataset.sb);
        showVisualDiff(vcBtn.dataset.pa, vcBtn.dataset.pb, sa, sb);
      });
    }

    // Bind merge + quarantine button
    const mqBtn = el.querySelector(".btn-merge-quarantine");
    if (mqBtn) {
      mqBtn.addEventListener("click", async () => {
        const gid = mqBtn.dataset.groupId;
        const keepPath = mqBtn.dataset.keepPath;
        if (!confirm(t("confirm.quarantine", { count: files.length - 1 }))) return;
        mqBtn.disabled = true;
        mqBtn.textContent = t("general.loading");
        try {
          const result = await apiPost(`/duplicates/${encodeURIComponent(gid)}/merge`, { keep_path: keepPath });
          showToast(`${t("task.completed_toast")} — ${result.quarantined || 0} karanténováno`, "success");
          // Refresh the duplicates page
          const c = content();
          render(c);
        } catch (err) {
          showToast(t("general.error", { message: err.message }), "error");
          mqBtn.disabled = false;
          mqBtn.textContent = t("duplicates.merge_quarantine");
        }
      });
    }
  } catch (e) {
    el.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
