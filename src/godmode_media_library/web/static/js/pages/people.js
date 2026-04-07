/* GOD MODE Media Library — People / Face Recognition page (v2) */

import { t } from "../i18n.js";
import { $, showToast } from "../utils.js";
import { api, apiPost, apiPut, apiDelete } from "../api.js";

let _container = null;
let _persons = [];
let _selectedPersonId = null;
let _view = "clusters"; // "clusters" | "person"
let _stats = null;
let _autoRunning = false;

export async function render(container) {
  _container = container;
  _selectedPersonId = null;
  _view = "clusters";

  container.innerHTML = `
    <div class="people-page">
      <div class="people-header">
        <h2>${t("people.title")}</h2>
        <div class="people-toolbar">
          <button class="btn btn-subtle" id="btn-privacy">
            <span class="btn-icon">&#128274;</span> ${t("people.privacy")}
          </button>
        </div>
      </div>
      <div id="privacy-banner"></div>
      <div id="people-auto-status"></div>
      <div id="face-stats" class="people-stats"></div>
      <div class="people-layout-v2">
        <div class="people-nav" id="people-nav"></div>
        <div class="people-main" id="people-main">
          <div class="loading"><div class="spinner"></div></div>
        </div>
      </div>
    </div>`;

  $("#btn-privacy")?.addEventListener("click", showPrivacyModal);

  await loadPrivacy();
  await loadData();
}

async function loadData() {
  try {
    const [statsData, personsData] = await Promise.all([
      api("/faces/stats"),
      api("/persons"),
    ]);
    _stats = statsData;
    _persons = personsData.persons || [];

    renderStats();
    renderNav();

    // Auto-detect + cluster if needed
    if (_stats.total_faces === 0) {
      await autoDetectAndCluster();
    } else if (_stats.total_faces > 0 && _persons.length === 0) {
      // Faces exist but no persons — auto-cluster
      await autoCluster();
    } else {
      renderMainView();
    }
  } catch (e) {
    const main = $("#people-main");
    if (main) main.innerHTML = `<div class="empty">${e.message}</div>`;
  }
}

function renderStats() {
  const el = $("#face-stats");
  if (!el || !_stats) return;

  const s = _stats;
  const identified = s.identified_faces || 0;
  const pct = s.total_faces > 0 ? Math.round((identified / s.total_faces) * 100) : 0;

  el.innerHTML = `
    <div class="stat-card"><span class="stat-value">${s.total_faces.toLocaleString("cs-CZ")}</span><span class="stat-label">Obličejů celkem</span></div>
    <div class="stat-card"><span class="stat-value">${_persons.length}</span><span class="stat-label">Osob</span></div>
    <div class="stat-card"><span class="stat-value">${identified.toLocaleString("cs-CZ")}</span><span class="stat-label">Přiřazených</span></div>
    <div class="stat-card ${s.unidentified_faces > 0 ? "stat-card-warn" : ""}">
      <span class="stat-value">${s.unidentified_faces.toLocaleString("cs-CZ")}</span>
      <span class="stat-label">K přiřazení</span>
    </div>`;
}

function renderNav() {
  const nav = $("#people-nav");
  if (!nav) return;

  const unidCount = _stats?.unidentified_faces || 0;

  nav.innerHTML = `
    <button class="people-nav-btn ${_view === "clusters" ? "active" : ""}" data-view="clusters">
      K přiřazení ${unidCount > 0 ? `<span class="people-nav-badge">${unidCount > 999 ? Math.round(unidCount/1000) + "k" : unidCount}</span>` : ""}
    </button>
    ${_persons.sort((a, b) => b.face_count - a.face_count).map(p => `
      <button class="people-nav-btn ${_view === "person" && _selectedPersonId === p.id ? "active" : ""}" data-view="person" data-person-id="${p.id}">
        <span class="people-nav-avatar">
          ${p.sample_face_id
            ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=32" alt="" loading="lazy">`
            : `<span class="people-nav-avatar-placeholder">&#128100;</span>`}
        </span>
        <span class="people-nav-name">${_esc(p.name || "?")}</span>
        <span class="people-nav-count">${p.face_count}</span>
      </button>
    `).join("")}
    <div class="people-nav-actions">
      <button class="btn btn-small btn-subtle" id="btn-add-person">+ Přidat osobu</button>
      <button class="btn btn-small btn-subtle" id="btn-redetect">Znovu detekovat</button>
      <button class="btn btn-small btn-subtle" id="btn-recluster">Znovu seskupit</button>
    </div>`;

  nav.querySelectorAll(".people-nav-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const view = btn.dataset.view;
      if (view === "clusters") {
        _view = "clusters";
        _selectedPersonId = null;
      } else if (view === "person") {
        _view = "person";
        _selectedPersonId = parseInt(btn.dataset.personId);
      }
      renderNav();
      renderMainView();
    });
  });

  $("#btn-add-person")?.addEventListener("click", addPerson);
  $("#btn-redetect")?.addEventListener("click", () => autoDetectAndCluster());
  $("#btn-recluster")?.addEventListener("click", () => autoCluster());
}

function renderMainView() {
  if (_view === "clusters") {
    renderClustersView();
  } else if (_view === "person" && _selectedPersonId) {
    selectPerson(_selectedPersonId);
  }
}

// ---------------------------------------------------------------------------
// Auto-detect & cluster
// ---------------------------------------------------------------------------

async function autoDetectAndCluster() {
  if (_autoRunning) return;
  _autoRunning = true;

  const statusEl = $("#people-auto-status");
  if (statusEl) {
    statusEl.innerHTML = `
      <div class="people-auto-bar">
        <span class="wiz-btn-spinner"></span>
        <span>Automaticky detekuji obličeje...</span>
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
            <span>Detekuji obličeje... ${prog.processed || 0} / ${prog.total || "?"}</span>
          </div>`;
      }
    });

    // Refresh stats
    _stats = await api("/faces/stats");
    renderStats();

    if (_stats.total_faces > 0) {
      await autoCluster();
    } else {
      if (statusEl) statusEl.innerHTML = `<div class="people-auto-bar people-auto-done">Žádné obličeje nenalezeny. Nejprve naskenujte složky s fotografiemi.</div>`;
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
  // called from autoDetectAndCluster (already running) or standalone
  _autoRunning = true;

  const statusEl = $("#people-auto-status");
  if (statusEl) {
    statusEl.innerHTML = `
      <div class="people-auto-bar">
        <span class="wiz-btn-spinner"></span>
        <span>Seskupuji podobné obličeje...</span>
      </div>`;
  }

  try {
    const result = await apiPost("/faces/cluster", { eps: 0.5, min_samples: 2 });
    await _waitForTask(result.task_id);

    // Refresh all data
    _stats = await api("/faces/stats");
    const personsData = await api("/persons");
    _persons = personsData.persons || [];

    renderStats();
    renderNav();

    if (statusEl) statusEl.innerHTML = "";
    _autoRunning = false;
    renderMainView();
  } catch (e) {
    if (statusEl) statusEl.innerHTML = `<div class="people-auto-bar people-auto-err">${_esc(e.message)}</div>`;
    _autoRunning = false;
    renderMainView();
  }
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
          reject(new Error(task.error || "Úloha selhala"));
        }
      } catch (e) {
        clearInterval(interval);
        reject(e);
      }
    }, 2000);
  });
}

// ---------------------------------------------------------------------------
// Clusters view — main view showing face groups to name
// ---------------------------------------------------------------------------

async function renderClustersView() {
  const main = $("#people-main");
  if (!main) return;

  main.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  try {
    const data = await api("/faces?unidentified=true&limit=500");
    const faces = data.faces || [];

    if (faces.length === 0) {
      main.innerHTML = `
        <div class="people-empty-state">
          <div class="people-empty-icon">&#9989;</div>
          <h3>Všechny obličeje jsou přiřazeny</h3>
          <p>Žádné nové obličeje k pojmenování.</p>
        </div>`;
      return;
    }

    // Group by cluster_id, sort by size (largest first)
    const clusters = {};
    for (const f of faces) {
      const cid = f.cluster_id ?? -1;
      (clusters[cid] ||= []).push(f);
    }

    const sortedClusters = Object.entries(clusters)
      .sort((a, b) => {
        // Named clusters first, then by size
        const aIsNoise = parseInt(a[0]) < 0;
        const bIsNoise = parseInt(b[0]) < 0;
        if (aIsNoise !== bIsNoise) return aIsNoise ? 1 : -1;
        return b[1].length - a[1].length;
      });

    let html = `<div class="people-clusters-view">`;

    // Intro
    const clusterCount = sortedClusters.filter(([cid]) => parseInt(cid) >= 0).length;
    const noiseCount = clusters[-1]?.length || 0;
    html += `<div class="people-clusters-intro">
      <strong>${faces.length}</strong> obličejů k přiřazení`;
    if (clusterCount > 0) html += ` v <strong>${clusterCount}</strong> skupinách`;
    if (noiseCount > 0) html += ` + ${noiseCount} osamocených`;
    html += `</div>`;

    for (const [clusterId, clusterFaces] of sortedClusters) {
      const isNoise = parseInt(clusterId) < 0;
      const label = isNoise ? "Osamocené obličeje" : `Skupina (${clusterFaces.length} obličejů)`;

      html += `
        <div class="people-cluster-card ${isNoise ? "people-cluster-noise" : ""}">
          <div class="people-cluster-header">
            <span class="people-cluster-label">${label}</span>
            ${!isNoise ? `
              <div class="people-cluster-actions">
                <input type="text" class="people-cluster-name-input" placeholder="Zadejte jméno..." data-cluster-id="${clusterId}">
                <button class="btn btn-primary btn-small people-cluster-assign-btn" data-cluster-id="${clusterId}">Pojmenovat</button>
              </div>
            ` : ""}
          </div>
          <div class="people-cluster-faces">
            ${clusterFaces.slice(0, isNoise ? 12 : 20).map(f => `
              <div class="people-face-thumb" data-face-id="${f.id}" title="${_esc(f.path)}">
                <img src="/api/faces/${f.id}/thumbnail?size=80" alt="" loading="lazy">
              </div>
            `).join("")}
            ${clusterFaces.length > (isNoise ? 12 : 20)
              ? `<div class="people-face-more">+${clusterFaces.length - (isNoise ? 12 : 20)}</div>` : ""}
          </div>
        </div>`;
    }

    html += `</div>`;
    main.innerHTML = html;

    // Bind: name input + assign button
    main.querySelectorAll(".people-cluster-assign-btn").forEach(btn => {
      btn.addEventListener("click", async () => {
        const cid = btn.dataset.clusterId;
        const input = main.querySelector(`.people-cluster-name-input[data-cluster-id="${cid}"]`);
        const name = input?.value?.trim();
        if (!name) { input?.focus(); return; }
        await assignClusterByName(clusters[cid], name, btn);
      });
    });

    // Enter to submit name
    main.querySelectorAll(".people-cluster-name-input").forEach(input => {
      input.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          const btn = main.querySelector(`.people-cluster-assign-btn[data-cluster-id="${input.dataset.clusterId}"]`);
          btn?.click();
        }
      });
    });

    // Click on individual face — assign to person modal
    main.querySelectorAll(".people-face-thumb").forEach(thumb => {
      thumb.addEventListener("click", () => {
        const faceId = parseInt(thumb.dataset.faceId);
        showAssignModal([faceId], null);
      });
    });
  } catch (e) {
    main.innerHTML = `<div class="empty">${_esc(e.message)}</div>`;
  }
}

async function assignClusterByName(clusterFaces, name, btn) {
  const origText = btn.textContent;
  btn.disabled = true;
  btn.innerHTML = `<span class="wiz-btn-spinner"></span>`;

  try {
    // Check if person with this name already exists
    let personId = null;
    const existing = _persons.find(p => p.name.toLowerCase() === name.toLowerCase());
    if (existing) {
      personId = existing.id;
    } else {
      const res = await apiPost("/persons/create", { name });
      personId = res.person_id;
    }

    // Assign all faces in cluster
    const faceIds = clusterFaces.map(f => f.id);
    for (const fid of faceIds) {
      await apiPut(`/faces/${fid}/person`, { person_id: personId });
    }

    showToast(`${faceIds.length} obličejů přiřazeno k "${name}"`, "success");

    // Refresh
    _stats = await api("/faces/stats");
    const personsData = await api("/persons");
    _persons = personsData.persons || [];
    renderStats();
    renderNav();
    renderClustersView();
  } catch (e) {
    showToast(e.message, "error");
    btn.disabled = false;
    btn.textContent = origText;
  }
}

// ---------------------------------------------------------------------------
// Person detail view
// ---------------------------------------------------------------------------

async function selectPerson(personId) {
  _selectedPersonId = personId;
  _view = "person";
  renderNav();

  const main = $("#people-main");
  if (!main) return;

  main.innerHTML = `<div class="loading"><div class="spinner"></div></div>`;

  try {
    const [person, facesData] = await Promise.all([
      api(`/persons/${personId}`),
      api(`/persons/${personId}/faces?limit=200`),
    ]);

    const faces = facesData.faces || [];

    main.innerHTML = `
      <div class="person-detail">
        <div class="person-detail-header">
          <div class="person-detail-name-row">
            <input type="text" class="person-name-input" id="person-name-input"
                   value="${_esc(person.name)}" placeholder="Jméno...">
            <button class="btn btn-primary btn-small" id="btn-save-name">Uložit</button>
          </div>
          <div class="person-detail-actions">
            <button class="btn btn-small" id="btn-merge-person">${t("people.merge")}</button>
            <button class="btn btn-danger btn-small" id="btn-delete-person">Smazat</button>
          </div>
        </div>
        <div class="person-faces-grid" id="person-faces-grid">
          ${faces.map(f => `
            <div class="face-card" data-face-id="${f.id}">
              <img src="/api/faces/${f.id}/thumbnail?size=150" alt="Face" loading="lazy">
              <div class="face-card-path" title="${_esc(f.path)}">${f.path.split("/").pop()}</div>
              <button class="face-card-action" data-face-id="${f.id}" title="Přesunout">&hellip;</button>
            </div>
          `).join("")}
        </div>
        ${faces.length === 0 ? `<div class="empty">Žádné obličeje</div>` : ""}
      </div>`;

    // Save name
    $("#btn-save-name")?.addEventListener("click", async () => {
      const name = $("#person-name-input")?.value?.trim();
      if (!name) return;
      try {
        await apiPut(`/persons/${personId}/name`, { name });
        showToast("Jméno uloženo", "success");
        const personsData = await api("/persons");
        _persons = personsData.persons || [];
        renderNav();
      } catch (e) { showToast(e.message, "error"); }
    });

    $("#person-name-input")?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") $("#btn-save-name")?.click();
    });

    // Delete
    $("#btn-delete-person")?.addEventListener("click", async () => {
      if (!confirm("Opravdu smazat tuto osobu? Obličeje zůstanou jako nepřiřazené.")) return;
      try {
        await apiDelete(`/persons/${personId}`);
        showToast("Osoba smazána", "success");
        _selectedPersonId = null;
        _view = "clusters";
        _stats = await api("/faces/stats");
        const personsData = await api("/persons");
        _persons = personsData.persons || [];
        renderStats();
        renderNav();
        renderMainView();
      } catch (e) { showToast(e.message, "error"); }
    });

    // Merge
    $("#btn-merge-person")?.addEventListener("click", () => showMergeModal(personId));

    // Face action buttons
    main.querySelectorAll(".face-card-action").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        showFaceActionMenu(parseInt(btn.dataset.faceId), personId, btn);
      });
    });
  } catch (e) {
    main.innerHTML = `<div class="empty">${_esc(e.message)}</div>`;
  }
}

// ---------------------------------------------------------------------------
// Modals & helpers
// ---------------------------------------------------------------------------

function showFaceActionMenu(faceId, currentPersonId, anchorEl) {
  document.querySelector(".face-action-popover")?.remove();

  const rect = anchorEl.getBoundingClientRect();
  const popover = document.createElement("div");
  popover.className = "face-action-popover";
  popover.style.cssText = `position:fixed;top:${rect.bottom + 4}px;right:${window.innerWidth - rect.right}px;z-index:var(--z-popover,9000)`;

  popover.innerHTML = `
    <button class="face-action-item" data-action="reassign">Přesunout k jiné osobě...</button>
    <button class="face-action-item face-action-danger" data-action="unassign">Odebrat z osoby</button>`;

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
      showToast("Obličej odebrán", "success");
      _stats = await api("/faces/stats");
      const personsData = await api("/persons");
      _persons = personsData.persons || [];
      renderStats();
      renderNav();
      if (_selectedPersonId) selectPerson(_selectedPersonId);
    } catch (e) { showToast(e.message, "error"); }
  });

  document.body.appendChild(popover);
}

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
        <span class="merge-person-name">${_esc(p.name || "?")}</span>
        <span class="merge-person-count">${p.face_count}</span>
      </label>`).join("");

  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:420px">
      <h3>Přiřadit osobě</h3>
      <div class="assign-modal-body">
        <div class="assign-new-row">
          <input type="text" id="assign-new-name" class="person-name-input" placeholder="Nové jméno..." style="flex:1">
          <button class="btn btn-primary btn-small" id="btn-assign-new">Vytvořit</button>
        </div>
        ${_persons.length > 0 ? `<div class="assign-divider">— nebo existující —</div>
        <div class="assign-person-list">${personOptions}</div>` : ""}
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end">
        <button class="btn" id="btn-assign-cancel">Zrušit</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  const doAssign = async (personId) => {
    for (const fid of faceIds) {
      await apiPut(`/faces/${fid}/person`, { person_id: personId });
    }
    overlay.remove();
    _stats = await api("/faces/stats");
    const personsData = await api("/persons");
    _persons = personsData.persons || [];
    renderStats();
    renderNav();
    renderMainView();
  };

  overlay.querySelector("#btn-assign-new")?.addEventListener("click", async () => {
    const name = overlay.querySelector("#assign-new-name")?.value?.trim();
    if (!name) return;
    try {
      const res = await apiPost("/persons/create", { name });
      await doAssign(res.person_id);
      showToast(`Přiřazeno k "${name}"`, "success");
    } catch (e) { showToast(e.message, "error"); }
  });

  overlay.querySelector("#assign-new-name")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") overlay.querySelector("#btn-assign-new")?.click();
  });

  overlay.querySelectorAll('input[name="assign-target"]').forEach(radio => {
    radio.addEventListener("change", async () => {
      const pid = parseInt(radio.value);
      try {
        await doAssign(pid);
        const p = _persons.find(x => x.id === pid);
        showToast(`Přiřazeno k "${p?.name || ""}"`, "success");
      } catch (e) { showToast(e.message, "error"); }
    });
  });

  overlay.querySelector("#btn-assign-cancel")?.addEventListener("click", () => overlay.remove());
}

async function showMergeModal(personId) {
  const otherPersons = _persons.filter(p => p.id !== personId);
  if (otherPersons.length === 0) {
    showToast("Žádné další osoby ke sloučení", "info");
    return;
  }

  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";
  const currentPerson = _persons.find(p => p.id === personId);

  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:480px">
      <h3>Sloučit → ${_esc(currentPerson?.name || "?")}</h3>
      <p style="color:var(--text-secondary);margin:0 0 12px">Zaškrtněte osoby ke sloučení. Jejich obličeje se přesunou sem.</p>
      <div class="merge-person-list">
        ${otherPersons.map(p => `
          <label class="merge-person-option">
            <input type="checkbox" name="merge-id" value="${p.id}">
            <span class="merge-person-avatar">
              ${p.sample_face_id
                ? `<img src="/api/faces/${p.sample_face_id}/thumbnail?size=40" alt="">`
                : `<span class="person-avatar-placeholder-sm">&#128100;</span>`}
            </span>
            <span class="merge-person-name">${_esc(p.name || "?")}</span>
            <span class="merge-person-count">${p.face_count}</span>
          </label>`).join("")}
      </div>
      <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end">
        <button class="btn" id="btn-merge-cancel">Zrušit</button>
        <button class="btn btn-primary" id="btn-merge-confirm">Sloučit</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  overlay.querySelector("#btn-merge-cancel")?.addEventListener("click", () => overlay.remove());
  overlay.querySelector("#btn-merge-confirm")?.addEventListener("click", async () => {
    const mergeIds = [...overlay.querySelectorAll('input[name="merge-id"]:checked')].map(cb => parseInt(cb.value));
    if (mergeIds.length === 0) { showToast("Vyberte alespoň jednu osobu", "info"); return; }
    try {
      await apiPost(`/persons/${personId}/merge`, { merge_ids: mergeIds });
      overlay.remove();
      showToast("Osoby sloučeny", "success");
      _stats = await api("/faces/stats");
      const personsData = await api("/persons");
      _persons = personsData.persons || [];
      renderStats();
      renderNav();
      selectPerson(personId);
    } catch (e) { showToast(e.message, "error"); }
  });
}

async function addPerson() {
  const name = prompt("Jméno nové osoby:");
  if (!name) return;
  try {
    const result = await apiPost("/persons/create", { name });
    showToast("Osoba vytvořena", "success");
    const personsData = await api("/persons");
    _persons = personsData.persons || [];
    renderNav();
    selectPerson(result.person_id);
  } catch (e) { showToast(e.message, "error"); }
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

function showPrivacyModal() {
  const overlay = document.createElement("div");
  overlay.className = "shortcuts-overlay";
  overlay.innerHTML = `
    <div class="shortcuts-modal" style="max-width:500px">
      <h3>&#128274; ${t("people.privacy_title")}</h3>
      <p>${t("people.privacy_info")}</p>
      <div style="margin-top:16px;display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-danger" id="btn-wipe-encodings">${t("people.wipe_encodings")}</button>
        <button class="btn" id="btn-close-privacy">Zavřít</button>
      </div>
    </div>`;

  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);

  overlay.querySelector("#btn-wipe-encodings")?.addEventListener("click", async () => {
    if (!confirm(t("people.wipe_confirm"))) return;
    try {
      const result = await apiDelete("/faces/privacy/encodings");
      showToast(t("people.encodings_wiped", { count: result.encodings_wiped }), "success");
      overlay.remove();
    } catch (e) { showToast(e.message, "error"); }
  });

  overlay.querySelector("#btn-close-privacy")?.addEventListener("click", () => overlay.remove());
}

function _esc(s) {
  if (typeof s !== "string") return String(s ?? "");
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
