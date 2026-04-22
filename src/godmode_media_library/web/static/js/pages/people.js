/* GOD MODE Media Library — People / Face Recognition page (v3 — clean UX rewrite) */

import { t } from "../i18n.js";
import { $, showToast } from "../utils.js";
import { api, apiPost, apiPut, apiDelete } from "../api.js";

let _container = null;
let _persons = [];
let _selectedPersonId = null;
let _view = "overview"; // "overview" | "person"
let _stats = null;
let _autoRunning = false;

/* ================================================================
   ENTRY POINT
   ================================================================ */

export async function render(container) {
  _container = container;
  _selectedPersonId = null;
  _view = "overview";

  container.innerHTML = `
    <div class="people-page">
      <div class="people-header">
        <h2>${t("people.title")}</h2>
      </div>
      <div id="privacy-banner"></div>
      <div id="people-auto-status"></div>
      <div id="face-stats" class="people-stats"></div>
      <div id="people-main-area"></div>
    </div>`;

  await loadPrivacy();
  await loadData();
}

/* ================================================================
   DATA LOADING
   ================================================================ */

async function loadData() {
  try {
    const [statsData, personsData] = await Promise.all([
      api("/faces/stats"),
      api("/persons"),
    ]);
    _stats = statsData;
    _persons = personsData.persons || [];

    renderStats();

    // Auto-detect + cluster if needed
    if (_stats.total_faces === 0) {
      await autoDetectAndCluster();
    } else if (_stats.total_faces > 0 && _persons.length === 0) {
      await autoCluster();
    } else {
      renderMainView();
    }
  } catch (e) {
    const main = $("#people-main-area");
    if (main) main.innerHTML = `<div class="empty">${_esc(e.message)}</div>`;
  }
}

/* ================================================================
   STATS (top bar — 4 cards)
   ================================================================ */

function renderStats() {
  const el = $("#face-stats");
  if (!el || !_stats) return;

  const s = _stats;
  const identified = s.identified_faces || 0;
  const pct = s.total_faces > 0 ? Math.round((identified / s.total_faces) * 100) : 0;

  el.innerHTML = `
    <div class="stat-card">
      <span class="stat-value">${s.total_faces.toLocaleString("cs-CZ")}</span>
      <span class="stat-label">${t("people.total_faces")}</span>
    </div>
    <div class="stat-card">
      <span class="stat-value">${_persons.length}</span>
      <span class="stat-label">${t("people.total_persons")}</span>
    </div>
    <div class="stat-card">
      <span class="stat-value">${pct}%</span>
      <span class="stat-label">${t("people.identified")}</span>
    </div>
    <div class="stat-card ${s.unidentified_faces > 0 ? "stat-card-warn" : ""}">
      <span class="stat-value">${s.unidentified_faces.toLocaleString("cs-CZ")}</span>
      <span class="stat-label">${t("people.unidentified")}</span>
    </div>`;
}

/* ================================================================
   MAIN VIEW — dispatches to overview or person detail
   ================================================================ */

function renderMainView() {
  if (_view === "person" && _selectedPersonId) {
    renderPersonDetail(_selectedPersonId);
  } else {
    renderOverview();
  }
}

/* ================================================================
   OVERVIEW — person grid + unidentified section
   ================================================================ */

function renderOverview() {
  const main = $("#people-main-area");
  if (!main) return;

  const sortedPersons = [..._persons].sort((a, b) => b.face_count - a.face_count);
  const unidCount = _stats?.unidentified_faces || 0;

  let html = `<div class="people-overview">`;

  // ── People grid (if any people exist) ─────────────────────────
  if (sortedPersons.length > 0) {
    html += `
      <div class="people-section">
        <div class="people-section-header">
          <h3>${t("people.persons")}</h3>
        </div>
        <div class="people-grid">
          ${sortedPersons.map(p => `
            <button class="people-person-card" data-person-id="${p.id}">
              <div class="people-person-avatar">
                ${p.sample_face_id
                  ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=120" alt="" loading="lazy">`
                  : `<span class="people-person-avatar-ph">&#128100;</span>`}
              </div>
              <div class="people-person-name">${_esc(p.name || t("people.unnamed"))}</div>
              <div class="people-person-count">${p.face_count} ${t("people.photos")}</div>
            </button>
          `).join("")}
        </div>
      </div>`;
  }

  // ── Unidentified faces section ───────────────────────────────
  if (unidCount > 0) {
    html += `
      <div class="people-section" id="unidentified-section">
        <div class="people-section-header">
          <h3>${t("people.unidentified")} <span class="people-section-badge">${unidCount > 999 ? Math.round(unidCount / 1000) + "k" : unidCount}</span></h3>
        </div>
        <div id="unidentified-clusters">
          <div class="loading"><div class="spinner"></div></div>
        </div>
      </div>`;
  } else if (sortedPersons.length > 0) {
    html += `
      <div class="people-section">
        <div class="people-all-done">
          <span class="people-all-done-icon">&#10003;</span>
          ${t("people.no_faces")}
        </div>
      </div>`;
  }

  // ── Tools row (collapsed by default) ─────────────────────────
  html += `
    <div class="people-tools-row">
      <button class="btn btn-subtle btn-small" id="btn-tools-toggle">
        ${t("people.privacy")} &amp; ${t("nav.group.tools").toLowerCase()}
      </button>
      <div class="people-tools-panel" id="people-tools-panel" style="display:none">
        <button class="btn btn-subtle btn-small" id="btn-redetect">${t("people.scan_faces")}</button>
        <button class="btn btn-subtle btn-small" id="btn-recluster">${t("people.cluster")}</button>
        <button class="btn btn-subtle btn-small" id="btn-cleanup">${t("people.cleanup")}</button>
        <button class="btn btn-subtle btn-small" id="btn-privacy-info">${t("people.privacy")}</button>
      </div>
    </div>`;

  html += `</div>`;
  main.innerHTML = html;

  // ── Bind: person cards ─────────────────────────────────────
  main.querySelectorAll(".people-person-card").forEach(card => {
    card.addEventListener("click", () => {
      _selectedPersonId = parseInt(card.dataset.personId);
      _view = "person";
      renderMainView();
    });
  });

  // ── Bind: tools toggle ─────────────────────────────────────
  $("#btn-tools-toggle")?.addEventListener("click", () => {
    const panel = $("#people-tools-panel");
    if (panel) panel.style.display = panel.style.display === "none" ? "flex" : "none";
  });

  $("#btn-redetect")?.addEventListener("click", () => autoDetectAndCluster());
  $("#btn-recluster")?.addEventListener("click", () => autoCluster());
  $("#btn-cleanup")?.addEventListener("click", cleanupAutoPersons);
  $("#btn-privacy-info")?.addEventListener("click", showPrivacyModal);

  // ── Load unidentified clusters ─────────────────────────────
  if (unidCount > 0) {
    loadUnidentifiedClusters();
  }
}

/* ================================================================
   UNIDENTIFIED CLUSTERS — shown inside overview
   ================================================================ */

async function loadUnidentifiedClusters() {
  const container = document.getElementById("unidentified-clusters");
  if (!container) return;

  try {
    const data = await api("/faces?unidentified=true&limit=500");
    const faces = data.faces || [];

    if (faces.length === 0) {
      container.innerHTML = `<div class="people-all-done"><span class="people-all-done-icon">&#10003;</span> ${t("people.no_faces")}</div>`;
      return;
    }

    // Group by cluster_id
    const clusters = {};
    for (const f of faces) {
      const cid = f.cluster_id ?? -1;
      (clusters[cid] ||= []).push(f);
    }

    const sorted = Object.entries(clusters)
      .sort((a, b) => {
        const aIsNoise = parseInt(a[0]) < 0;
        const bIsNoise = parseInt(b[0]) < 0;
        if (aIsNoise !== bIsNoise) return aIsNoise ? 1 : -1;
        return b[1].length - a[1].length;
      });

    let html = "";

    for (const [clusterId, clusterFaces] of sorted) {
      const isNoise = parseInt(clusterId) < 0;
      const label = isNoise
        ? t("people.noise")
        : `${clusterFaces.length} ${t("people.photos")}`;

      html += `
        <div class="people-cluster-card ${isNoise ? "people-cluster-noise" : ""}">
          <div class="people-cluster-header">
            <span class="people-cluster-label">${label}</span>
            ${!isNoise ? `
              <div class="people-cluster-actions">
                <select class="people-cluster-select" data-cluster-id="${clusterId}">
                  <option value="">${t("people.assign_to_person")}...</option>
                  ${_persons.map(p => `<option value="${p.id}">${_esc(p.name)}</option>`).join("")}
                  <option value="__new__">+ ${t("people.add_person")}...</option>
                </select>
              </div>
            ` : ""}
          </div>
          <div class="people-cluster-faces">
            ${clusterFaces.slice(0, isNoise ? 8 : 16).map(f => `
              <div class="people-face-thumb" data-face-id="${f.id}" title="${_esc(f.path?.split("/").pop() || "")}">
                <img src="/api/faces/${f.id}/thumbnail?size=80" alt="" loading="lazy">
              </div>
            `).join("")}
            ${clusterFaces.length > (isNoise ? 8 : 16)
              ? `<div class="people-face-more">+${clusterFaces.length - (isNoise ? 8 : 16)}</div>` : ""}
          </div>
        </div>`;
    }

    container.innerHTML = html;

    // ── Bind: cluster assign select ────────────────────────────
    container.querySelectorAll(".people-cluster-select").forEach(sel => {
      sel.addEventListener("change", async () => {
        const cid = sel.dataset.clusterId;
        const val = sel.value;
        if (!val) return;

        const clusterFaces = clusters[cid];
        if (!clusterFaces) return;

        if (val === "__new__") {
          // Show inline name prompt
          sel.value = "";
          const name = await showNamePrompt(t("people.enter_person_name"));
          if (!name) return;
          await assignClusterToNewPerson(clusterFaces, name);
        } else {
          const personId = parseInt(val);
          const person = _persons.find(p => p.id === personId);
          await assignClusterToPerson(clusterFaces, personId, person?.name || "?");
        }
      });
    });

    // ── Bind: individual face click → assign modal ─────────────
    container.querySelectorAll(".people-face-thumb").forEach(thumb => {
      thumb.addEventListener("click", () => {
        const faceId = parseInt(thumb.dataset.faceId);
        showAssignModal([faceId], null);
      });
    });
  } catch (e) {
    container.innerHTML = `<div class="empty">${_esc(e.message)}</div>`;
  }
}

async function assignClusterToPerson(clusterFaces, personId, personName) {
  try {
    const faceIds = clusterFaces.map(f => f.id);
    await apiPost("/faces/batch-assign", { face_ids: faceIds, person_id: personId }).catch(async () => {
      // Fallback: assign one by one if batch endpoint doesn't exist
      for (const fid of faceIds) {
        await apiPut(`/faces/${fid}/person`, { person_id: personId });
      }
    });

    showToast(t("people.faces_assigned", { count: faceIds.length, name: personName }), "success");
    await refreshAll();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function assignClusterToNewPerson(clusterFaces, name) {
  try {
    // Create or find person
    let personId;
    const existing = _persons.find(p => p.name.toLowerCase() === name.toLowerCase());
    if (existing) {
      personId = existing.id;
    } else {
      const res = await apiPost("/persons/create", { name });
      personId = res.person_id;
    }

    await assignClusterToPerson(clusterFaces, personId, name);
  } catch (e) {
    showToast(e.message, "error");
  }
}

/* ================================================================
   PERSON DETAIL VIEW
   ================================================================ */

async function renderPersonDetail(personId) {
  const main = $("#people-main-area");
  if (!main) return;

  main.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  try {
    const [person, facesData] = await Promise.all([
      api(`/persons/${personId}`),
      api(`/persons/${personId}/faces?limit=200`),
    ]);

    const faces = facesData.faces || [];

    main.innerHTML = `
      <div class="person-detail-v3">
        <div class="person-detail-topbar">
          <button class="btn btn-subtle btn-small" id="btn-back-overview">&larr; ${t("people.persons")}</button>
        </div>

        <div class="person-detail-hero">
          <div class="person-detail-avatar-lg">
            ${person.sample_face_id
              ? `<img src="/api/faces/${person.sample_face_id}/thumbnail?size=160" alt="">`
              : `<span class="people-person-avatar-ph" style="font-size:64px">&#128100;</span>`}
          </div>
          <div class="person-detail-info">
            <h2 class="person-detail-editable-name" id="person-name-display">${_esc(person.name)}</h2>
            <div class="person-detail-meta">${faces.length} ${t("people.photos")}</div>
            <div class="person-detail-btns">
              <button class="btn btn-subtle btn-small" id="btn-rename">${t("people.rename")}</button>
              <button class="btn btn-subtle btn-small" id="btn-merge-person">${t("people.merge")}</button>
              <button class="btn btn-subtle btn-small btn-danger-text" id="btn-delete-person">${t("people.delete")}</button>
            </div>
          </div>
        </div>

        <div class="person-faces-grid" id="person-faces-grid">
          ${faces.map(f => `
            <div class="face-card" data-face-id="${f.id}">
              <img src="/api/faces/${f.id}/thumbnail?size=150" alt="" loading="lazy">
              <div class="face-card-overlay">
                <button class="face-card-remove" data-face-id="${f.id}" title="${t("people.remove_from_person")}">&#10005;</button>
              </div>
            </div>
          `).join("")}
        </div>
        ${faces.length === 0 ? `<div class="empty">${t("people.no_faces")}</div>` : ""}
      </div>`;

    // ── Bind: back button ──────────────────────────────────────
    $("#btn-back-overview")?.addEventListener("click", () => {
      _view = "overview";
      _selectedPersonId = null;
      renderMainView();
    });

    // ── Bind: rename ───────────────────────────────────────────
    $("#btn-rename")?.addEventListener("click", async () => {
      const name = await showNamePrompt(t("people.enter_person_name"), person.name);
      if (!name || name === person.name) return;
      try {
        await apiPut(`/persons/${personId}/name`, { name });
        showToast(t("people.name_saved"), "success");
        await refreshAll();
        renderPersonDetail(personId);
      } catch (e) { showToast(e.message, "error"); }
    });

    // ── Bind: name click = rename ──────────────────────────────
    $("#person-name-display")?.addEventListener("click", () => {
      $("#btn-rename")?.click();
    });

    // ── Bind: delete ───────────────────────────────────────────
    $("#btn-delete-person")?.addEventListener("click", async () => {
      if (!confirm(t("people.confirm_delete"))) return;
      try {
        await apiDelete(`/persons/${personId}`);
        showToast(t("people.person_deleted"), "success");
        _selectedPersonId = null;
        _view = "overview";
        await refreshAll();
      } catch (e) { showToast(e.message, "error"); }
    });

    // ── Bind: merge ────────────────────────────────────────────
    $("#btn-merge-person")?.addEventListener("click", () => showMergeModal(personId));

    // ── Bind: face remove buttons ──────────────────────────────
    main.querySelectorAll(".face-card-remove").forEach(btn => {
      btn.addEventListener("click", async (e) => {
        e.stopPropagation();
        const faceId = parseInt(btn.dataset.faceId);
        try {
          await apiPut(`/faces/${faceId}/person`, { person_id: null });
          showToast(t("people.face_removed"), "success");
          await refreshAll();
          renderPersonDetail(personId);
        } catch (e2) { showToast(e2.message, "error"); }
      });
    });

    // ── Bind: face card click → reassign ───────────────────────
    main.querySelectorAll(".face-card").forEach(card => {
      card.addEventListener("click", (e) => {
        if (e.target.closest(".face-card-remove")) return;
        const faceId = parseInt(card.dataset.faceId);
        showAssignModal([faceId], personId);
      });
    });

  } catch (e) {
    main.innerHTML = `<div class="empty">${_esc(e.message)}</div>`;
  }
}

/* ================================================================
   AUTO-DETECT & CLUSTER (background tasks)
   ================================================================ */

async function autoDetectAndCluster() {
  if (_autoRunning) return;
  _autoRunning = true;

  const statusEl = $("#people-auto-status");
  if (statusEl) {
    statusEl.innerHTML = `
      <div class="people-auto-bar">
        <span class="wiz-btn-spinner"></span>
        <span>${t("people.detection_started", { task_id: "" }).replace("()", "").trim()}</span>
      </div>`;
  }

  try {
    const result = await apiPost("/faces/detect", { model: "hog" });
    await _waitForTask(result.task_id, (task) => {
      if (statusEl) {
        const prog = task.progress || {};
        statusEl.innerHTML = `
          <div class="people-auto-bar">
            <span class="wiz-btn-spinner"></span>
            <span>${t("people.scan_faces")}... ${prog.processed || prog.done || 0} / ${prog.total || "?"}</span>
          </div>`;
      }
    });

    _stats = await api("/faces/stats");
    renderStats();

    if (_stats.total_faces > 0) {
      await autoCluster();
    } else {
      if (statusEl) statusEl.innerHTML = `<div class="people-auto-bar people-auto-done">${t("people.no_faces")}</div>`;
      _autoRunning = false;
      renderMainView();
    }
  } catch (e) {
    if (statusEl) statusEl.innerHTML = `<div class="people-auto-bar people-auto-err">${_esc(e.message)}</div>`;
    _autoRunning = false;
    renderMainView();
  }
}

async function autoCluster() {
  _autoRunning = true;

  const statusEl = $("#people-auto-status");
  if (statusEl) {
    statusEl.innerHTML = `
      <div class="people-auto-bar">
        <span class="wiz-btn-spinner"></span>
        <span>${t("people.clustering_started")}...</span>
      </div>`;
  }

  try {
    const result = await apiPost("/faces/cluster", { eps: 0.5, min_samples: 2 });
    await _waitForTask(result.task_id);

    await refreshAll();

    if (statusEl) statusEl.innerHTML = "";
    _autoRunning = false;
    renderMainView();
  } catch (e) {
    if (statusEl) statusEl.innerHTML = `<div class="people-auto-bar people-auto-err">${_esc(e.message)}</div>`;
    _autoRunning = false;
    renderMainView();
  }
}

async function cleanupAutoPersons() {
  if (!confirm(t("people.cleanup_confirm"))) return;
  try {
    const result = await apiPost("/persons/cleanup");
    showToast(t("people.cleanup_done", { deleted: result.persons_deleted, freed: result.faces_freed }), "success");
    await refreshAll();
  } catch (e) { showToast(e.message, "error"); }
}

function _waitForTask(taskId, onProgress) {
  return new Promise((resolve, reject) => {
    const interval = setInterval(async () => {
      try {
        const task = await api(`/tasks/${taskId}`);
        if (onProgress) onProgress(task);
        if (task.status === "completed") {
          clearInterval(interval);
          resolve(task);
        } else if (task.status === "failed") {
          clearInterval(interval);
          reject(new Error(task.error || "Task failed"));
        }
      } catch (e) {
        clearInterval(interval);
        reject(e);
      }
    }, 2000);
  });
}

/* ================================================================
   REFRESH HELPER
   ================================================================ */

async function refreshAll() {
  _stats = await api("/faces/stats");
  const personsData = await api("/persons");
  _persons = personsData.persons || [];
  renderStats();
  renderMainView();
}

/* ================================================================
   ASSIGN MODAL — unified "who is this?" modal
   ================================================================ */

async function showAssignModal(faceIds, excludePersonId) {
  const overlay = document.createElement("div");
  overlay.className = "gml-modal-overlay";

  const availablePersons = _persons.filter(p => p.id !== excludePersonId);

  overlay.innerHTML = `
    <div class="gml-modal" style="max-width:420px">
      <div class="gml-modal-header">
        <h3>${t("people.assign_to_person")}</h3>
        <button class="gml-modal-close" id="btn-assign-close">&times;</button>
      </div>
      <div class="gml-modal-body">
        <div class="assign-new-row">
          <input type="text" id="assign-new-name" class="gml-input" placeholder="${t("people.new_person_name")}" autofocus>
          <button class="btn btn-primary btn-small" id="btn-assign-new">${t("people.create_assign")}</button>
        </div>
        ${availablePersons.length > 0 ? `
          <div class="assign-divider">${t("people.or_existing")}</div>
          <div class="assign-person-list">
            ${availablePersons.map(p => `
              <button class="assign-person-item" data-person-id="${p.id}">
                <span class="assign-person-avatar">
                  ${p.sample_face_id
                    ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=40" alt="">`
                    : `<span class="people-person-avatar-ph" style="font-size:20px">&#128100;</span>`}
                </span>
                <span class="assign-person-name">${_esc(p.name || "?")}</span>
                <span class="assign-person-count">${p.face_count}</span>
              </button>
            `).join("")}
          </div>
        ` : ""}
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  const close = () => overlay.remove();

  const doAssign = async (personId, personName) => {
    try {
      for (const fid of faceIds) {
        await apiPut(`/faces/${fid}/person`, { person_id: personId });
      }
      close();
      showToast(t("people.faces_assigned", { count: faceIds.length, name: personName }), "success");
      await refreshAll();
    } catch (e) { showToast(e.message, "error"); }
  };

  overlay.querySelector("#btn-assign-close")?.addEventListener("click", close);

  overlay.querySelector("#btn-assign-new")?.addEventListener("click", async () => {
    const name = overlay.querySelector("#assign-new-name")?.value?.trim();
    if (!name) { overlay.querySelector("#assign-new-name")?.focus(); return; }
    try {
      const existing = _persons.find(p => p.name.toLowerCase() === name.toLowerCase());
      let pid;
      if (existing) {
        pid = existing.id;
      } else {
        const res = await apiPost("/persons/create", { name });
        pid = res.person_id;
      }
      await doAssign(pid, name);
    } catch (e) { showToast(e.message, "error"); }
  });

  overlay.querySelector("#assign-new-name")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") overlay.querySelector("#btn-assign-new")?.click();
    if (e.key === "Escape") close();
  });

  overlay.querySelectorAll(".assign-person-item").forEach(btn => {
    btn.addEventListener("click", async () => {
      const pid = parseInt(btn.dataset.personId);
      const p = _persons.find(x => x.id === pid);
      await doAssign(pid, p?.name || "?");
    });
  });
}

/* ================================================================
   MERGE MODAL
   ================================================================ */

async function showMergeModal(personId) {
  const otherPersons = _persons.filter(p => p.id !== personId);
  if (otherPersons.length === 0) {
    showToast(t("people.no_merge_targets"), "info");
    return;
  }

  const currentPerson = _persons.find(p => p.id === personId);
  const overlay = document.createElement("div");
  overlay.className = "gml-modal-overlay";

  overlay.innerHTML = `
    <div class="gml-modal" style="max-width:480px">
      <div class="gml-modal-header">
        <h3>${t("people.merge")} &rarr; ${_esc(currentPerson?.name || "?")}</h3>
        <button class="gml-modal-close" id="btn-merge-close">&times;</button>
      </div>
      <div class="gml-modal-body">
        <p class="gml-modal-hint">${t("people.merge_hint")}</p>
        <div class="merge-person-list">
          ${otherPersons.map(p => `
            <label class="merge-person-option">
              <input type="checkbox" name="merge-id" value="${p.id}">
              <span class="merge-person-avatar">
                ${p.sample_face_id
                  ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=40" alt="">`
                  : `<span class="people-person-avatar-ph" style="font-size:20px">&#128100;</span>`}
              </span>
              <span class="merge-person-name">${_esc(p.name || "?")}</span>
              <span class="merge-person-count">${p.face_count}</span>
            </label>
          `).join("")}
        </div>
      </div>
      <div class="gml-modal-footer">
        <button class="btn btn-subtle" id="btn-merge-cancel">${t("people.cancel")}</button>
        <button class="btn btn-primary" id="btn-merge-confirm">${t("people.merge")}</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.querySelector("#btn-merge-close")?.addEventListener("click", close);
  overlay.querySelector("#btn-merge-cancel")?.addEventListener("click", close);

  overlay.querySelector("#btn-merge-confirm")?.addEventListener("click", async () => {
    const mergeIds = [...overlay.querySelectorAll('input[name="merge-id"]:checked')].map(cb => parseInt(cb.value));
    if (mergeIds.length === 0) {
      showToast(t("people.select_to_merge"), "info");
      return;
    }
    try {
      await apiPost(`/persons/${personId}/merge`, { merge_ids: mergeIds });
      close();
      showToast(t("people.merge_success"), "success");
      await refreshAll();
      _selectedPersonId = personId;
      _view = "person";
      renderMainView();
    } catch (e) { showToast(e.message, "error"); }
  });
}

/* ================================================================
   NAME PROMPT — custom inline prompt (replaces browser prompt())
   ================================================================ */

function showNamePrompt(label, defaultValue = "") {
  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "gml-modal-overlay";

    overlay.innerHTML = `
      <div class="gml-modal" style="max-width:360px">
        <div class="gml-modal-header">
          <h3>${_esc(label)}</h3>
          <button class="gml-modal-close" id="btn-prompt-close">&times;</button>
        </div>
        <div class="gml-modal-body">
          <input type="text" id="prompt-input" class="gml-input gml-input-lg" value="${_esc(defaultValue)}" autofocus>
        </div>
        <div class="gml-modal-footer">
          <button class="btn btn-subtle" id="btn-prompt-cancel">${t("people.cancel")}</button>
          <button class="btn btn-primary" id="btn-prompt-ok">OK</button>
        </div>
      </div>`;

    document.body.appendChild(overlay);

    const close = (val) => { overlay.remove(); resolve(val); };

    overlay.addEventListener("click", (e) => { if (e.target === overlay) close(null); });
    overlay.querySelector("#btn-prompt-close")?.addEventListener("click", () => close(null));
    overlay.querySelector("#btn-prompt-cancel")?.addEventListener("click", () => close(null));
    overlay.querySelector("#btn-prompt-ok")?.addEventListener("click", () => {
      close(overlay.querySelector("#prompt-input")?.value?.trim() || null);
    });
    overlay.querySelector("#prompt-input")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") overlay.querySelector("#btn-prompt-ok")?.click();
      if (e.key === "Escape") close(null);
    });

    // Focus input after mount
    requestAnimationFrame(() => overlay.querySelector("#prompt-input")?.select());
  });
}

/* ================================================================
   PRIVACY
   ================================================================ */

async function loadPrivacy() {
  try {
    const privacy = await api("/faces/privacy");
    const banner = $("#privacy-banner");
    if (!banner) return;

    if (!privacy.consent_given) {
      banner.innerHTML = `
        <div class="privacy-banner">
          <span class="privacy-icon">&#128274;</span>
          <div class="privacy-text">
            <strong>${t("people.privacy_notice_title")}</strong>
            <p>${t("people.privacy_notice")}</p>
          </div>
          <button class="btn btn-primary" id="btn-consent">${t("people.accept")}</button>
        </div>`;
      $("#btn-consent")?.addEventListener("click", async () => {
        await apiPost("/faces/privacy/consent");
        banner.innerHTML = "";
        showToast(t("people.consent_saved"), "success");
      });
    }
  } catch { /* privacy table may not exist yet */ }
}

function showPrivacyModal() {
  const overlay = document.createElement("div");
  overlay.className = "gml-modal-overlay";
  overlay.innerHTML = `
    <div class="gml-modal" style="max-width:500px">
      <div class="gml-modal-header">
        <h3>&#128274; ${t("people.privacy_title")}</h3>
        <button class="gml-modal-close" id="btn-close-priv">&times;</button>
      </div>
      <div class="gml-modal-body">
        <p>${t("people.privacy_info")}</p>
      </div>
      <div class="gml-modal-footer">
        <button class="btn btn-subtle" id="btn-close-priv2">${t("people.cancel")}</button>
        <button class="btn btn-danger" id="btn-wipe-encodings">${t("people.wipe_encodings")}</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.querySelector("#btn-close-priv")?.addEventListener("click", close);
  overlay.querySelector("#btn-close-priv2")?.addEventListener("click", close);

  overlay.querySelector("#btn-wipe-encodings")?.addEventListener("click", async () => {
    if (!confirm(t("people.wipe_confirm"))) return;
    try {
      const result = await apiDelete("/faces/privacy/encodings");
      showToast(t("people.encodings_wiped", { count: result.encodings_wiped }), "success");
      close();
    } catch (e) { showToast(e.message, "error"); }
  });
}

/* ================================================================
   UTILS
   ================================================================ */

function _esc(s) {
  if (typeof s !== "string") return String(s ?? "");
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

