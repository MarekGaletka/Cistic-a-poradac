/* GOD MODE Media Library — Duplicates page (redesigned) */

import { api, apiPost, apiPut } from "../api.js";
import { $, content, formatBytes, escapeHtml, fileName, showToast, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showVisualDiff } from "../modal.js";

let _allGroups = [];
let _allGroupDetails = {};

export async function render(container) {
  try {
    const data = await api("/duplicates?limit=100");
    if (!data.groups.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("duplicates.title")}</h2></div>
        <div class="empty-state-hero">
          <div class="empty-state-icon" style="font-size:64px">&#9989;</div>
          <h3 class="empty-state-title">${t("duplicates.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("duplicates.empty_hint")}</p>
        </div>`;
      return;
    }

    _allGroups = data.groups;

    // Calculate total wasted space
    let totalWasted = 0;
    for (const g of data.groups) {
      // Approximate: (file_count - 1) * avg_size
      if (g.file_count > 1 && g.total_size > 0) {
        const avgSize = g.total_size / g.file_count;
        totalWasted += avgSize * (g.file_count - 1);
      }
    }

    let html = `
      <div class="page-header">
        <h2>${t("duplicates.title")} <span class="header-count">${t("duplicates.groups", { count: data.total_groups })}</span></h2>
        <div class="page-header-actions">
          <button class="btn-icon" id="btn-dedup-settings" title="${t("dedup.rules_title")}">\u2699</button>
          <button class="primary" id="btn-resolve-all" title="${t("duplicates.resolve_tooltip")}">
            &#9889; ${t("duplicates.resolve_all")}
          </button>
        </div>
      </div>
      <div id="dedup-rules-inline" class="dedup-rules-inline hidden"></div>`;

    // Summary bar
    html += `<div class="dup-summary">
      <div class="dup-summary-item">
        <span class="dup-summary-icon" style="color:var(--yellow)">&#9888;</span>
        <span class="dup-summary-label">${t("duplicates.groups_remaining")}</span>
        <span class="dup-summary-value" id="dup-groups-count">${data.total_groups}</span>
      </div>
      <div class="dup-summary-item">
        <span class="dup-summary-icon" style="color:var(--red)">&#128190;</span>
        <span class="dup-summary-label">${t("duplicates.potential_savings")}</span>
        <span class="dup-summary-value">${formatBytes(totalWasted)}</span>
      </div>
    </div>`;

    // Groups list — each group is an inline card with thumbnails
    html += '<div class="dup-groups-list" id="dup-groups-list">';
    for (const g of data.groups) {
      html += renderGroupCard(g);
    }
    html += '</div>';

    container.innerHTML = html;

    // Load details for each group (thumbnails, scores)
    loadAllGroupDetails(container, data.groups);

    // Bind resolve all
    const resolveAllBtn = container.querySelector("#btn-resolve-all");
    if (resolveAllBtn) {
      resolveAllBtn.addEventListener("click", () => resolveAll(container));
    }

    // Bind dedup settings toggle
    container.querySelector("#btn-dedup-settings")?.addEventListener("click", () => {
      const panel = container.querySelector("#dedup-rules-inline");
      if (panel) {
        const wasHidden = panel.classList.toggle("hidden");
        if (!wasHidden && !panel.dataset.loaded) {
          renderDedupRules(panel);
          panel.dataset.loaded = "1";
        }
      }
    });
  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("duplicates.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

function renderGroupCard(g) {
  return `<div class="dup-group-card" id="dup-group-${escapeHtml(g.group_id)}" data-group-id="${escapeHtml(g.group_id)}">
    <div class="dup-group-header">
      <span class="dup-group-info">
        <strong>${g.file_count} ${t("duplicates.files").toLowerCase()}</strong>
        <span class="dup-group-size">${formatBytes(g.total_size)}</span>
      </span>
      <button class="btn-resolve-single" data-group-id="${escapeHtml(g.group_id)}" title="${t("duplicates.resolve_tooltip")}">
        &#9889; ${t("duplicates.resolve")}
      </button>
    </div>
    <div class="dup-group-thumbs" id="dup-thumbs-${escapeHtml(g.group_id)}">
      <div class="loading-inline"><div class="spinner-small"></div></div>
    </div>
  </div>`;
}

async function loadAllGroupDetails(container, groups) {
  // Load details for all groups in parallel (max 10 at a time)
  const batchSize = 10;
  for (let i = 0; i < groups.length; i += batchSize) {
    const batch = groups.slice(i, i + batchSize);
    await Promise.all(batch.map(g => loadGroupDetail(container, g.group_id)));
  }
}

async function loadGroupDetail(container, groupId) {
  try {
    const [groupData, diffData] = await Promise.all([
      api(`/duplicates/${encodeURIComponent(groupId)}`),
      api(`/duplicates/${encodeURIComponent(groupId)}/diff`),
    ]);

    const files = groupData.files || [];
    const scores = diffData.scores || {};

    // Find winner
    let winnerPath = null;
    let winnerScore = -1;
    for (const [path, score] of Object.entries(scores)) {
      if (score > winnerScore) { winnerScore = score; winnerPath = path; }
    }
    if (!winnerPath && files.length) winnerPath = files[0].path;

    // Store for resolve action
    _allGroupDetails[groupId] = { files, scores, winnerPath, diffData };

    // Render thumbnails inline
    const thumbsEl = $(`#dup-thumbs-${CSS.escape(groupId)}`);
    if (!thumbsEl) return;

    let html = '<div class="dup-inline-compare">';
    for (const f of files) {
      const path = f.path;
      const isWinner = path === winnerPath;
      const score = scores[path];
      const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());

      html += `<div class="dup-inline-file ${isWinner ? 'dup-inline-winner' : 'dup-inline-loser'}">`;

      // Status badge
      if (isWinner) {
        html += `<div class="dup-file-badge dup-badge-keep">&#9989; ${t("duplicates.best_file")}</div>`;
      } else {
        html += `<div class="dup-file-badge dup-badge-quarantine">&#128465; ${t("duplicates.will_quarantine")}</div>`;
      }

      // Thumbnail
      if (isImage) {
        const thumbSrc = `/api/thumbnail${encodeURI(path)}?size=200`;
        html += `<img class="dup-inline-thumb" src="${thumbSrc}" loading="lazy" onerror="this.outerHTML='<div class=\\'dup-inline-thumb-placeholder\\'>&#128444;</div>'" alt="${escapeHtml(fileName(path))}">`;
      } else {
        const icon = (f.ext || "").match(/^(mp4|mov|avi|mkv|wmv|flv|webm)$/i) ? "&#127910;" : "&#128196;";
        html += `<div class="dup-inline-thumb-placeholder">${icon}</div>`;
      }

      // File info
      html += `<div class="dup-inline-info">
        <div class="dup-inline-name" title="${escapeHtml(path)}">${escapeHtml(fileName(path))}</div>
        <div class="dup-inline-meta">${formatBytes(f.size)}</div>`;
      if (f.width && f.height) {
        html += `<div class="dup-inline-meta">${f.width}x${f.height}</div>`;
      }
      if (score != null) {
        const level = score >= 30 ? "high" : score >= 15 ? "medium" : "low";
        html += `<span class="richness-badge ${level}">${Number(score).toFixed(1)} pts</span>`;
      }
      html += `</div>`;
      html += `</div>`;
    }
    html += '</div>';

    // Key differences inline
    if (diffData.conflicts && Object.keys(diffData.conflicts).length > 0) {
      html += `<div class="dup-inline-diffs">
        <span class="dup-diff-label">&#9888; ${Object.keys(diffData.conflicts).length} ${t("duplicates.conflicts").toLowerCase()}</span>
      </div>`;
    }

    // Visual compare for 2-file groups
    if (files.length === 2) {
      const pA = files[0].path;
      const pB = files[1].path;
      html += `<div class="dup-inline-actions">
        <button class="btn-visual-inline" data-pa="${escapeHtml(pA)}" data-pb="${escapeHtml(pB)}" data-sa="${scores[pA] ?? 'null'}" data-sb="${scores[pB] ?? 'null'}">
          &#128065; ${t("duplicates.visual_compare")}
        </button>
      </div>`;
    }

    thumbsEl.innerHTML = html;

    // Bind visual compare
    const vcBtn = thumbsEl.querySelector(".btn-visual-inline");
    if (vcBtn) {
      vcBtn.addEventListener("click", () => {
        const sa = vcBtn.dataset.sa === "null" ? null : Number(vcBtn.dataset.sa);
        const sb = vcBtn.dataset.sb === "null" ? null : Number(vcBtn.dataset.sb);
        showVisualDiff(vcBtn.dataset.pa, vcBtn.dataset.pb, sa, sb);
      });
    }

    // Bind single resolve button
    const card = $(`#dup-group-${CSS.escape(groupId)}`);
    if (card) {
      const resolveBtn = card.querySelector(".btn-resolve-single");
      if (resolveBtn) {
        resolveBtn.addEventListener("click", () => resolveSingle(groupId, card));
      }
    }
  } catch (e) {
    const thumbsEl = $(`#dup-thumbs-${CSS.escape(groupId)}`);
    if (thumbsEl) {
      thumbsEl.innerHTML = `<div class="dup-inline-error">${t("general.error", { message: e.message })}</div>`;
    }
  }
}

async function resolveSingle(groupId, cardEl) {
  const detail = _allGroupDetails[groupId];
  if (!detail) return;

  const resolveBtn = cardEl.querySelector(".btn-resolve-single");
  if (resolveBtn) {
    resolveBtn.disabled = true;
    resolveBtn.innerHTML = `&#8987; ${t("duplicates.resolving")}`;
  }

  try {
    await apiPost(`/duplicates/${encodeURIComponent(groupId)}/merge`, { keep_path: detail.winnerPath });
    // Animate card removal
    cardEl.classList.add("dup-resolved");
    setTimeout(() => {
      cardEl.remove();
      updateGroupsCount();
    }, 500);
    showToast(`${t("duplicates.resolved")} &#9989;`, "success");
  } catch (err) {
    showToast(t("general.error", { message: err.message }), "error");
    if (resolveBtn) {
      resolveBtn.disabled = false;
      resolveBtn.innerHTML = `&#9889; ${t("duplicates.resolve")}`;
    }
  }
}

async function resolveAll(container) {
  if (!confirm(t("confirm.resolve_all"))) return;

  const cards = container.querySelectorAll(".dup-group-card");
  let resolved = 0;
  let failed = 0;

  for (const card of cards) {
    const groupId = card.dataset.groupId;
    const detail = _allGroupDetails[groupId];
    if (!detail) continue;

    try {
      await apiPost(`/duplicates/${encodeURIComponent(groupId)}/merge`, { keep_path: detail.winnerPath });
      card.classList.add("dup-resolved");
      resolved++;
    } catch {
      failed++;
    }
  }

  showToast(`${t("duplicates.resolved")}: ${resolved}${failed > 0 ? `, ${t("task.status.failed").toLowerCase()}: ${failed}` : ""}`, resolved > 0 ? "success" : "error");

  // Refresh after a moment
  setTimeout(() => render(container), 1000);
}

function updateGroupsCount() {
  const countEl = $("#dup-groups-count");
  const remaining = document.querySelectorAll(".dup-group-card:not(.dup-resolved)").length;
  if (countEl) countEl.textContent = remaining;
}

async function renderDedupRules(container) {
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
        </div>
        <div class="dedup-field dedup-toggle-row">
          <label class="dedup-toggle-label">
            <input type="checkbox" id="dedup-merge-metadata" ${rules.merge_metadata ? "checked" : ""}>
            <span class="dedup-toggle-switch"></span>
            <span>${t("dedup.merge_metadata")}</span>
          </label>
        </div>
        <button class="primary dedup-save-btn" id="btn-dedup-save">${t("general.save")}</button>
      </div>`;

    // Slider
    const slider = container.querySelector("#dedup-threshold");
    const sliderVal = container.querySelector("#dedup-threshold-val");
    if (slider && sliderVal) {
      slider.addEventListener("input", () => { sliderVal.textContent = slider.value; });
    }

    // Strategy highlight
    container.querySelectorAll(".dedup-strategy-option").forEach(opt => {
      opt.addEventListener("click", () => {
        container.querySelectorAll(".dedup-strategy-option").forEach(o => o.classList.remove("active"));
        opt.classList.add("active");
        opt.querySelector("input").checked = true;
      });
    });

    // Save
    container.querySelector("#btn-dedup-save").addEventListener("click", async () => {
      const btn = container.querySelector("#btn-dedup-save");
      btn.disabled = true;
      btn.textContent = "...";

      const strategy = container.querySelector('input[name="dedup-strategy"]:checked')?.value || "richness";
      const body = {
        strategy,
        similarity_threshold: parseInt(container.querySelector("#dedup-threshold").value, 10),
        auto_resolve: container.querySelector("#dedup-auto-resolve").checked,
        merge_metadata: container.querySelector("#dedup-merge-metadata").checked,
        quarantine_path: rules.quarantine_path || "",
        exclude_extensions: rules.exclude_extensions || [],
        exclude_paths: rules.exclude_paths || [],
        min_file_size_kb: rules.min_file_size_kb || 0,
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
