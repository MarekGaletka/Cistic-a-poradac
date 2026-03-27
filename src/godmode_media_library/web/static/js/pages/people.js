/* GOD MODE Media Library — People / Face Recognition page */

import { t } from "../i18n.js";
import { $, showToast } from "../utils.js";
import { api, apiPost, apiPut, apiDelete } from "../api.js";

let _container = null;
let _persons = [];
let _selectedPersonId = null;

export async function render(container) {
  _container = container;
  _selectedPersonId = null;

  container.innerHTML = `
    <div class="people-page">
      <div class="people-header">
        <h2>${t("people.title")}</h2>
        <div class="people-toolbar">
          <button class="btn btn-primary" id="btn-face-detect">
            <span class="btn-icon">&#128269;</span> ${t("people.scan_faces")}
          </button>
          <button class="btn" id="btn-face-cluster">
            <span class="btn-icon">&#127922;</span> ${t("people.cluster")}
          </button>
          <button class="btn btn-subtle" id="btn-cleanup-auto" title="${t("people.cleanup_hint")}">
            <span class="btn-icon">&#128465;</span> ${t("people.cleanup")}
          </button>
          <button class="btn btn-subtle" id="btn-privacy">
            <span class="btn-icon">&#128274;</span> ${t("people.privacy")}
          </button>
        </div>
      </div>
      <div id="privacy-banner"></div>
      <div id="face-stats" class="people-stats"></div>
      <div class="people-layout">
        <div class="people-sidebar" id="people-sidebar">
          <div class="people-sidebar-header">
            <h3>${t("people.persons")}</h3>
            <button class="btn btn-small" id="btn-add-person" title="${t("people.add_person")}">+</button>
          </div>
          <div id="persons-list" class="persons-list"></div>
          <div class="people-sidebar-section">
            <h4>${t("people.unidentified")}</h4>
            <div id="unidentified-count" class="people-unidentified-count"></div>
          </div>
        </div>
        <div class="people-content" id="people-content">
          <div class="empty">${t("people.select_person")}</div>
        </div>
      </div>
    </div>`;

  // Load data
  await Promise.all([loadPrivacy(), loadStats(), loadPersons()]);

  // Event listeners
  $("#btn-face-detect")?.addEventListener("click", startDetection);
  $("#btn-face-cluster")?.addEventListener("click", startClustering);
  $("#btn-privacy")?.addEventListener("click", showPrivacyModal);
  $("#btn-add-person")?.addEventListener("click", addPerson);
  $("#btn-cleanup-auto")?.addEventListener("click", cleanupAutoPersons);
}

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

async function loadStats() {
  try {
    const stats = await api("/faces/stats");
    const el = $("#face-stats");
    if (!el) return;

    el.innerHTML = `
      <div class="stat-card"><span class="stat-value">${stats.total_faces}</span><span class="stat-label">${t("people.total_faces")}</span></div>
      <div class="stat-card"><span class="stat-value">${stats.total_persons}</span><span class="stat-label">${t("people.total_persons")}</span></div>
      <div class="stat-card"><span class="stat-value">${stats.identified_faces}</span><span class="stat-label">${t("people.identified")}</span></div>
      <div class="stat-card"><span class="stat-value">${stats.unidentified_faces}</span><span class="stat-label">${t("people.unidentified")}</span></div>
      <div class="stat-card"><span class="stat-value">${stats.named_persons}</span><span class="stat-label">${t("people.named")}</span></div>`;
  } catch { /* stats may fail if no faces yet */ }
}

async function loadPersons() {
  try {
    const data = await api("/persons");
    _persons = data.persons || [];
    renderPersonsList();

    // Also load unidentified count
    const unid = await api("/faces?unidentified=true&limit=1");
    const countEl = $("#unidentified-count");
    if (countEl) {
      const count = unid.count || 0;
      countEl.innerHTML = count > 0
        ? `<button class="btn btn-link" id="btn-show-unidentified">${count} ${t("people.unidentified_faces")}</button>`
        : `<span class="text-muted">0</span>`;
      $("#btn-show-unidentified")?.addEventListener("click", () => showUnidentified());
    }
  } catch (e) {
    const list = $("#persons-list");
    if (list) list.innerHTML = `<div class="empty">${t("people.no_persons")}</div>`;
  }
}

function renderPersonsList() {
  const list = $("#persons-list");
  if (!list) return;

  if (_persons.length === 0) {
    list.innerHTML = `<div class="empty">${t("people.no_persons")}</div>`;
    return;
  }

  list.innerHTML = _persons.map(p => `
    <div class="person-item ${p.id === _selectedPersonId ? 'active' : ''}" data-person-id="${p.id}">
      <div class="person-avatar">
        ${p.sample_face_id
          ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=80" alt="${p.name}" loading="lazy">`
          : `<span class="person-avatar-placeholder">&#128100;</span>`
        }
      </div>
      <div class="person-info">
        <span class="person-name">${p.name || t("people.unnamed")}</span>
        <span class="person-count">${p.face_count} ${t("people.photos")}</span>
      </div>
    </div>
  `).join("");

  list.querySelectorAll(".person-item").forEach(el => {
    el.addEventListener("click", () => {
      const pid = parseInt(el.dataset.personId);
      selectPerson(pid);
    });
  });
}

async function selectPerson(personId) {
  _selectedPersonId = personId;
  renderPersonsList();

  const content = $("#people-content");
  if (!content) return;

  content.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  try {
    const [person, facesData] = await Promise.all([
      api(`/persons/${personId}`),
      api(`/persons/${personId}/faces?limit=200`),
    ]);

    const faces = facesData.faces || [];

    content.innerHTML = `
      <div class="person-detail">
        <div class="person-detail-header">
          <div class="person-detail-name-row">
            <input type="text" class="person-name-input" id="person-name-input"
                   value="${person.name}" placeholder="${t("people.enter_name")}">
            <button class="btn btn-primary btn-small" id="btn-save-name">${t("general.save")}</button>
          </div>
          <div class="person-detail-actions">
            <button class="btn btn-small" id="btn-merge-person">${t("people.merge")}</button>
            <button class="btn btn-danger btn-small" id="btn-delete-person">${t("general.delete")}</button>
          </div>
        </div>
        <div class="person-faces-grid" id="person-faces-grid">
          ${faces.map(f => `
            <div class="face-card" data-face-id="${f.id}">
              <img src="/api/faces/${f.id}/thumbnail?size=150" alt="Face" loading="lazy">
              <div class="face-card-path" title="${f.path}">${f.path.split("/").pop()}</div>
              <button class="face-card-action" data-face-id="${f.id}" title="${t("people.reassign_face")}">&hellip;</button>
            </div>
          `).join("")}
        </div>
        ${faces.length === 0 ? `<div class="empty">${t("people.no_faces")}</div>` : ""}
      </div>`;

    // Save name
    $("#btn-save-name")?.addEventListener("click", async () => {
      const name = $("#person-name-input")?.value?.trim();
      if (!name) return;
      try {
        await apiPut(`/persons/${personId}/name`, { name });
        showToast(t("people.name_saved"), "success");
        await loadPersons();
        await loadStats();
      } catch (e) {
        showToast(e.message, "error");
      }
    });

    // Enter to save name
    $("#person-name-input")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") $("#btn-save-name")?.click();
    });

    // Delete person
    $("#btn-delete-person")?.addEventListener("click", async () => {
      if (!confirm(t("people.confirm_delete"))) return;
      try {
        await apiDelete(`/persons/${personId}`);
        _selectedPersonId = null;
        showToast(t("people.person_deleted"), "success");
        content.innerHTML = `<div class="empty">${t("people.select_person")}</div>`;
        await loadPersons();
        await loadStats();
      } catch (e) {
        showToast(e.message, "error");
      }
    });

    // Merge person
    $("#btn-merge-person")?.addEventListener("click", () => showMergeModal(personId));

    // Face card action button — reassign or remove single face
    content.querySelectorAll(".face-card-action").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const faceId = parseInt(btn.dataset.faceId);
        showFaceActionMenu(faceId, personId, btn);
      });
    });
  } catch (e) {
    content.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

async function showUnidentified() {
  _selectedPersonId = null;
  renderPersonsList();

  const content = $("#people-content");
  if (!content) return;

  content.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  try {
    const data = await api("/faces?unidentified=true&limit=200");
    const faces = data.faces || [];

    // Group by cluster_id
    const clusters = {};
    for (const f of faces) {
      const cid = f.cluster_id ?? -1;
      (clusters[cid] ||= []).push(f);
    }

    let html = `<div class="unidentified-view">
      <h3>${t("people.unidentified")} (${faces.length})</h3>`;

    for (const [clusterId, clusterFaces] of Object.entries(clusters).sort((a, b) => b[1].length - a[1].length)) {
      const label = parseInt(clusterId) >= 0
        ? `${t("people.cluster")} #${clusterId} (${clusterFaces.length})`
        : `${t("people.noise")} (${clusterFaces.length})`;

      html += `
        <div class="unidentified-cluster">
          <div class="unidentified-cluster-header">
            <span>${label}</span>
            ${parseInt(clusterId) >= 0 ? `<button class="btn btn-small btn-assign-cluster" data-cluster-id="${clusterId}">${t("people.assign_to_person")}</button>` : ""}
          </div>
          <div class="person-faces-grid">
            ${clusterFaces.map(f => `
              <div class="face-card" data-face-id="${f.id}">
                <img src="/api/faces/${f.id}/thumbnail?size=150" alt="Face" loading="lazy">
                <div class="face-card-path" title="${f.path}">${f.path.split("/").pop()}</div>
                <button class="face-card-action" data-face-id="${f.id}" title="${t("people.assign_to_person")}">&hellip;</button>
              </div>
            `).join("")}
          </div>
        </div>`;
    }

    html += "</div>";
    content.innerHTML = html;

    // Assign cluster to person buttons
    content.querySelectorAll(".btn-assign-cluster").forEach(btn => {
      btn.addEventListener("click", async () => {
        const clusterId = parseInt(btn.dataset.clusterId);
        const clusterFaces = clusters[clusterId] || [];
        await assignClusterToPerson(clusterFaces);
      });
    });

    // Individual face assign buttons in unidentified view
    content.querySelectorAll(".face-card-action").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        const faceId = parseInt(btn.dataset.faceId);
        showAssignModal([faceId], null);
      });
    });
  } catch (e) {
    content.innerHTML = `<div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}

async function assignClusterToPerson(faces) {
  // Show picker: existing person or create new
  await showAssignModal(faces.map(f => f.id), null);
}

/** Show a popover to reassign or unassign a single face from a person. */
function showFaceActionMenu(faceId, currentPersonId, anchorEl) {
  // Close any existing popover
  document.querySelector(".face-action-popover")?.remove();

  const rect = anchorEl.getBoundingClientRect();
  const popover = document.createElement("div");
  popover.className = "face-action-popover";
  popover.style.position = "fixed";
  popover.style.top = `${rect.bottom + 4}px`;
  popover.style.right = `${window.innerWidth - rect.right}px`;
  popover.style.zIndex = "var(--z-popover, 9000)";

  popover.innerHTML = `
    <button class="face-action-item" data-action="reassign">${t("people.move_to_person")}</button>
    <button class="face-action-item face-action-danger" data-action="unassign">${t("people.remove_from_person")}</button>`;

  const close = () => popover.remove();
  setTimeout(() => document.addEventListener("click", close, { once: true }), 0);
  popover.addEventListener("click", (e) => e.stopPropagation());

  popover.querySelector('[data-action="reassign"]').addEventListener("click", async () => {
    close();
    await showAssignModal([faceId], currentPersonId);
  });

  popover.querySelector('[data-action="unassign"]').addEventListener("click", async () => {
    close();
    try {
      await apiPut(`/faces/${faceId}/person`, { person_id: null });
      showToast(t("people.face_removed"), "success");
      await loadPersons();
      await loadStats();
      if (_selectedPersonId) selectPerson(_selectedPersonId);
    } catch (e) {
      showToast(e.message, "error");
    }
  });

  document.body.appendChild(popover);
}

/** Modal to assign face(s) to an existing person or create a new one. */
async function showAssignModal(faceIds, excludePersonId) {
  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";

  const personOptions = _persons
    .filter(p => p.id !== excludePersonId)
    .map(p => `
      <label class="merge-person-option">
        <input type="radio" name="assign-target" value="${p.id}">
        <span class="merge-person-avatar">
          ${p.sample_face_id
            ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=40" alt="">`
            : `<span class="person-avatar-placeholder-sm">&#128100;</span>`}
        </span>
        <span class="merge-person-name">${p.name || t("people.unnamed")}</span>
        <span class="merge-person-count">${p.face_count}</span>
      </label>`).join("");

  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:420px">
      <h3>${t("people.assign_to_person")}</h3>
      <div class="assign-modal-body">
        <div class="assign-new-row">
          <input type="text" id="assign-new-name" class="person-name-input"
                 placeholder="${t("people.new_person_name")}" style="flex:1">
          <button class="btn btn-primary btn-small" id="btn-assign-new">${t("people.create_assign")}</button>
        </div>
        <div class="assign-divider">${t("people.or_existing")}</div>
        <div class="assign-person-list">${personOptions || `<div class="empty">${t("people.no_persons")}</div>`}</div>
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end">
        <button class="btn" id="btn-assign-cancel">${t("general.cancel")}</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  // Create new person + assign
  overlay.querySelector("#btn-assign-new")?.addEventListener("click", async () => {
    const name = overlay.querySelector("#assign-new-name")?.value?.trim();
    if (!name) return;
    try {
      const res = await apiPost("/persons/create", { name });
      for (const fid of faceIds) {
        await apiPut(`/faces/${fid}/person`, { person_id: res.person_id });
      }
      overlay.remove();
      showToast(t("people.faces_assigned", { count: faceIds.length, name }), "success");
      await loadPersons();
      await loadStats();
      selectPerson(res.person_id);
    } catch (e) {
      showToast(e.message, "error");
    }
  });

  overlay.querySelector("#assign-new-name")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") overlay.querySelector("#btn-assign-new")?.click();
  });

  // Pick existing person
  overlay.querySelectorAll('input[name="assign-target"]').forEach(radio => {
    radio.addEventListener("change", async () => {
      const pid = parseInt(radio.value);
      try {
        for (const fid of faceIds) {
          await apiPut(`/faces/${fid}/person`, { person_id: pid });
        }
        overlay.remove();
        const p = _persons.find(x => x.id === pid);
        showToast(t("people.faces_assigned", { count: faceIds.length, name: p?.name || "" }), "success");
        await loadPersons();
        await loadStats();
        selectPerson(pid);
      } catch (e) {
        showToast(e.message, "error");
      }
    });
  });

  overlay.querySelector("#btn-assign-cancel")?.addEventListener("click", () => overlay.remove());
}

async function showMergeModal(personId) {
  const otherPersons = _persons.filter(p => p.id !== personId);
  if (otherPersons.length === 0) {
    showToast(t("people.no_merge_targets"), "info");
    return;
  }

  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";

  const currentPerson = _persons.find(p => p.id === personId);

  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:480px">
      <h3>${t("people.merge")} → ${currentPerson?.name || t("people.unnamed")}</h3>
      <p style="color:var(--text-secondary);margin:0 0 12px">${t("people.merge_hint")}</p>
      <div class="merge-person-list">
        ${otherPersons.map(p => `
          <label class="merge-person-option">
            <input type="checkbox" name="merge-id" value="${p.id}">
            <span class="merge-person-avatar">
              ${p.sample_face_id
                ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=40" alt="">`
                : `<span class="person-avatar-placeholder-sm">&#128100;</span>`}
            </span>
            <span class="merge-person-name">${p.name || t("people.unnamed")}</span>
            <span class="merge-person-count">${p.face_count} ${t("people.photos")}</span>
          </label>`).join("")}
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end">
        <button class="btn" id="btn-merge-cancel">${t("general.cancel")}</button>
        <button class="btn btn-primary" id="btn-merge-confirm">${t("people.merge")}</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  overlay.querySelector("#btn-merge-cancel")?.addEventListener("click", () => overlay.remove());
  overlay.querySelector("#btn-merge-confirm")?.addEventListener("click", async () => {
    const checked = [...overlay.querySelectorAll('input[name="merge-id"]:checked')];
    const mergeIds = checked.map(cb => parseInt(cb.value));
    if (mergeIds.length === 0) {
      showToast(t("people.select_to_merge"), "info");
      return;
    }
    try {
      await apiPost(`/persons/${personId}/merge`, { merge_ids: mergeIds });
      overlay.remove();
      showToast(t("people.merge_success"), "success");
      await loadPersons();
      await loadStats();
      selectPerson(personId);
    } catch (e) {
      showToast(e.message, "error");
    }
  });
}

async function startDetection() {
  const btn = $("#btn-face-detect");
  if (btn) { btn.disabled = true; btn.textContent = "..."; }

  try {
    const result = await apiPost("/faces/detect", { model: "hog" });
    showToast(t("people.detection_started", { task_id: result.task_id }), "success");

    // Poll for completion
    pollTask(result.task_id, async () => {
      if (btn) { btn.disabled = false; btn.innerHTML = `<span class="btn-icon">&#128269;</span> ${t("people.scan_faces")}`; }
      await loadStats();
      await loadPersons();
      showToast(t("people.detection_complete"), "success");
    });
  } catch (e) {
    if (btn) { btn.disabled = false; btn.innerHTML = `<span class="btn-icon">&#128269;</span> ${t("people.scan_faces")}`; }
    showToast(e.message, "error");
  }
}

async function startClustering() {
  const btn = $("#btn-face-cluster");
  if (btn) { btn.disabled = true; btn.textContent = "..."; }

  try {
    const result = await apiPost("/faces/cluster", { eps: 0.5, min_samples: 2 });
    showToast(t("people.clustering_started"), "success");

    pollTask(result.task_id, async () => {
      if (btn) { btn.disabled = false; btn.innerHTML = `<span class="btn-icon">&#127922;</span> ${t("people.cluster")}`; }
      await loadStats();
      await loadPersons();
      showToast(t("people.clustering_complete"), "success");
    });
  } catch (e) {
    if (btn) { btn.disabled = false; btn.innerHTML = `<span class="btn-icon">&#127922;</span> ${t("people.cluster")}`; }
    showToast(e.message, "error");
  }
}

function pollTask(taskId, onComplete) {
  const interval = setInterval(async () => {
    try {
      const task = await api(`/tasks/${taskId}`);
      if (task.status === "completed" || task.status === "failed") {
        clearInterval(interval);
        if (task.status === "failed") {
          showToast(t("general.error", { message: task.error || "Unknown error" }), "error");
        }
        onComplete?.();
      }
    } catch {
      clearInterval(interval);
      onComplete?.();
    }
  }, 2000);
}

async function cleanupAutoPersons() {
  if (!confirm(t("people.cleanup_confirm"))) return;
  try {
    const result = await apiPost("/persons/cleanup");
    showToast(t("people.cleanup_done", { deleted: result.persons_deleted, freed: result.faces_freed }), "success");
    _selectedPersonId = null;
    await loadStats();
    await loadPersons();
    const content = $("#people-content");
    if (content) content.innerHTML = `<div class="empty">${t("people.select_person")}</div>`;
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function addPerson() {
  const name = prompt(t("people.enter_person_name"));
  if (!name) return;
  try {
    const result = await apiPost("/persons/create", { name });
    showToast(t("people.person_created"), "success");
    await loadPersons();
    selectPerson(result.person_id);
  } catch (e) {
    showToast(e.message, "error");
  }
}

function showPrivacyModal() {
  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";
  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:500px">
      <h3>&#128274; ${t("people.privacy_title")}</h3>
      <p>${t("people.privacy_info")}</p>
      <div style="margin-top:16px;display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-danger" id="btn-wipe-encodings">${t("people.wipe_encodings")}</button>
        <button class="btn" id="btn-close-privacy">${t("general.close")}</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  $("#btn-close-privacy", overlay)?.addEventListener?.("click", () => overlay.remove());

  document.body.appendChild(overlay);

  overlay.querySelector("#btn-wipe-encodings")?.addEventListener("click", async () => {
    if (!confirm(t("people.wipe_confirm"))) return;
    try {
      const result = await apiDelete("/faces/privacy/encodings");
      showToast(t("people.encodings_wiped", { count: result.encodings_wiped }), "success");
      overlay.remove();
    } catch (e) {
      showToast(e.message, "error");
    }
  });

  overlay.querySelector("#btn-close-privacy")?.addEventListener("click", () => overlay.remove());
}
