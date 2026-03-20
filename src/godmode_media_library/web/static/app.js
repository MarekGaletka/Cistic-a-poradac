/* GOD MODE Media Library — Vanilla JS Frontend */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);
const content = () => $("#content");

async function api(path) {
  const res = await fetch(`/api${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function apiPost(path) {
  const res = await fetch(`/api${path}`, { method: "POST" });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
  return `${val.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

function fileName(path) {
  return path.split("/").pop();
}

function escapeHtml(str) {
  if (typeof str !== "string") return String(str ?? "");
  return str.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

// ── Modal ────────────────────────────────────────────

function closeModal() {
  const overlay = $(".modal-overlay");
  if (overlay) overlay.remove();
}

document.addEventListener("keydown", e => { if (e.key === "Escape") closeModal(); });

const IMAGE_EXTS = new Set(["jpg","jpeg","png","bmp","tiff","tif","gif","webp","heic","heif"]);

async function showFileDetail(filePath) {
  // Create overlay immediately with loading state
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.innerHTML = `<div class="modal"><button class="modal-close" onclick="closeModal()">&times;</button><div class="loading"><div class="spinner"></div>Loading...</div></div>`;
  overlay.addEventListener("click", e => { if (e.target === overlay) closeModal(); });
  document.body.appendChild(overlay);

  try {
    const data = await api(`/files${filePath}`);
    const f = data.file;
    const meta = data.metadata || {};
    const richness = data.richness;
    const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());

    // Thumbnail or placeholder
    let thumbHtml;
    if (isImage) {
      thumbHtml = `<img class="modal-thumb" src="/api/thumbnail${f.path}?size=400" onerror="this.outerHTML='<div class=\\'modal-thumb-placeholder\\'>&#128444;</div>'" alt="">`;
    } else {
      const icon = (f.ext || "").match(/^(mp4|mov|avi|mkv|wmv|flv|webm)$/i) ? "&#127910;" : "&#128196;";
      thumbHtml = `<div class="modal-thumb-placeholder">${icon}</div>`;
    }

    // Richness badge
    let richnessHtml = "";
    if (richness != null) {
      const level = richness >= 30 ? "high" : richness >= 15 ? "medium" : "low";
      richnessHtml = `<span class="richness-badge ${level}">${Number(richness).toFixed(1)} pts</span>`;
    }

    // GPS link
    let gpsHtml = "";
    if (f.gps_latitude && f.gps_longitude) {
      gpsHtml = `<div class="meta-row"><span class="meta-label">GPS</span><a class="gps-link" href="https://maps.google.com/?q=${f.gps_latitude},${f.gps_longitude}" target="_blank" rel="noopener">${f.gps_latitude.toFixed(6)}, ${f.gps_longitude.toFixed(6)} &#x2197;</a></div>`;
    }

    // Basic info rows
    const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
    const res = f.width && f.height ? `${f.width} x ${f.height}` : "";
    const infoRows = [
      ["Size", formatBytes(f.size)],
      ["Extension", f.ext],
      ["Date", f.date_original || "—"],
      cam ? ["Camera", cam] : null,
      res ? ["Resolution", res] : null,
      f.duration_seconds ? ["Duration", `${f.duration_seconds.toFixed(1)}s`] : null,
      f.video_codec ? ["Video", f.video_codec] : null,
      f.audio_codec ? ["Audio", f.audio_codec] : null,
      f.sha256 ? ["SHA-256", f.sha256.slice(0, 16) + "..."] : null,
      f.phash ? ["PHash", f.phash.slice(0, 16) + "..."] : null,
    ].filter(Boolean);

    // Deep metadata table
    let metaHtml = "";
    const metaKeys = Object.keys(meta);
    if (metaKeys.length) {
      metaHtml = `<div class="modal-section"><h4>ExifTool Metadata (${metaKeys.length} tags)</h4><table class="meta-table">`;
      for (const key of metaKeys.sort()) {
        const val = typeof meta[key] === "object" ? JSON.stringify(meta[key]) : String(meta[key]);
        metaHtml += `<tr><td>${escapeHtml(key)}</td><td>${escapeHtml(val)}</td></tr>`;
      }
      metaHtml += "</table></div>";
    }

    const modalEl = overlay.querySelector(".modal");
    modalEl.innerHTML = `
      <button class="modal-close" onclick="closeModal()">&times;</button>
      <div class="modal-header">
        ${thumbHtml}
        <div class="modal-info">
          <h3>${escapeHtml(fileName(f.path))}</h3>
          <div style="font-size:11px;color:var(--text-muted);margin-bottom:8px;word-break:break-all">${escapeHtml(f.path)}</div>
          ${richnessHtml ? `<div style="margin-bottom:12px">${richnessHtml}</div>` : ""}
          ${infoRows.map(([l,v]) => `<div class="meta-row"><span class="meta-label">${escapeHtml(l)}</span><span>${escapeHtml(v)}</span></div>`).join("")}
          ${gpsHtml}
        </div>
      </div>
      ${metaHtml}
    `;
  } catch (e) {
    const modalEl = overlay.querySelector(".modal");
    modalEl.innerHTML = `<button class="modal-close" onclick="closeModal()">&times;</button><div class="empty">Error loading file detail: ${escapeHtml(e.message)}</div>`;
  }
}
window.showFileDetail = showFileDetail;
window.closeModal = closeModal;

// ── Router ───────────────────────────────────────────

const pages = { dashboard: renderDashboard, files: renderFiles, duplicates: renderDuplicates, similar: renderSimilar, pipeline: renderPipeline, doctor: renderDoctor };

function navigate(page) {
  $$("nav a").forEach(a => a.classList.toggle("active", a.dataset.page === page));
  content().innerHTML = '<div class="loading"><div class="spinner"></div>Loading...</div>';
  if (pages[page]) pages[page]();
}

// Hamburger toggle for mobile
$(".nav-toggle")?.addEventListener("click", () => $("nav").classList.toggle("open"));

document.addEventListener("click", e => {
  const link = e.target.closest("nav a[data-page]");
  if (link) {
    e.preventDefault();
    navigate(link.dataset.page);
    // Close nav on mobile after navigation
    $("nav").classList.remove("open");
  }
});

window.addEventListener("hashchange", () => navigate(location.hash.slice(1) || "dashboard"));
navigate(location.hash.slice(1) || "dashboard");

// ── Dashboard ────────────────────────────────────────

async function renderDashboard() {
  try {
    const stats = await api("/stats");
    const cards = [
      ["Total Files", stats.total_files?.toLocaleString() ?? 0],
      ["Total Size", formatBytes(stats.total_size_bytes)],
      ["Hashed", stats.hashed_files?.toLocaleString() ?? 0],
      ["Duplicate Groups", stats.duplicate_groups ?? 0],
      ["Duplicate Files", stats.duplicate_files ?? 0],
      ["GPS Files", stats.gps_files ?? 0],
      ["Media Probed", stats.media_probed ?? 0],
      ["Labeled", stats.labeled_files ?? 0],
    ];
    let html = "<h2>Dashboard</h2><div class='stats-grid'>";
    for (const [label, value] of cards) {
      html += `<div class="stat-card"><div class="label">${label}</div><div class="value">${value}</div></div>`;
    }
    html += "</div>";

    if (stats.top_extensions?.length) {
      html += "<h2>Top Extensions</h2><table><tr><th>Extension</th><th>Count</th></tr>";
      for (const [ext, count] of stats.top_extensions) {
        html += `<tr><td>.${escapeHtml(ext)}</td><td>${count.toLocaleString()}</td></tr>`;
      }
      html += "</table>";
    }

    if (stats.top_cameras?.length) {
      html += "<h2>Top Cameras</h2><table><tr><th>Camera</th><th>Count</th></tr>";
      for (const [cam, count] of stats.top_cameras) {
        html += `<tr><td>${escapeHtml(cam)}</td><td>${count.toLocaleString()}</td></tr>`;
      }
      html += "</table>";
    }

    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<div class="empty"><div class="empty-icon">&#128202;</div><div class="empty-text">No catalog data yet</div><div class="empty-hint">Run <code>gml scan --roots /path</code> or use the Pipeline page to start scanning.</div></div>`;
  }
}

// ── Files ────────────────────────────────────────────

const FILES_PER_PAGE = 50;
let _filesOffset = 0;

async function renderFiles() {
  _filesOffset = 0;
  let html = `<h2>Files</h2>
    <div class="filters">
      <input type="text" id="f-ext" placeholder="Extension (jpg)" size="10">
      <input type="text" id="f-camera" placeholder="Camera" size="15">
      <input type="text" id="f-path" placeholder="Path contains..." size="20">
      <button onclick="_filesOffset=0;loadFiles()">Search</button>
    </div>
    <div class="filters filters-advanced">
      <div class="filter-group"><label>From</label><input type="date" id="f-date-from"></div>
      <div class="filter-group"><label>To</label><input type="date" id="f-date-to"></div>
      <div class="filter-group"><label>Min KB</label><input type="number" id="f-min-size" min="0" style="width:80px"></div>
      <div class="filter-group"><label>Max KB</label><input type="number" id="f-max-size" min="0" style="width:80px"></div>
      <label class="filter-checkbox"><input type="checkbox" id="f-has-gps"> GPS</label>
      <label class="filter-checkbox"><input type="checkbox" id="f-has-phash"> PHash</label>
    </div>
    <div id="files-table"></div>`;
  content().innerHTML = html;
  loadFiles();
}

async function loadFiles() {
  const ext = $("#f-ext")?.value || "";
  const camera = $("#f-camera")?.value || "";
  const pathC = $("#f-path")?.value || "";
  const dateFrom = $("#f-date-from")?.value || "";
  const dateTo = $("#f-date-to")?.value || "";
  const minSize = $("#f-min-size")?.value || "";
  const maxSize = $("#f-max-size")?.value || "";
  const hasGps = $("#f-has-gps")?.checked;
  const hasPhash = $("#f-has-phash")?.checked;
  let q = `/files?limit=${FILES_PER_PAGE}`;
  if (ext) q += `&ext=${encodeURIComponent(ext)}`;
  if (camera) q += `&camera=${encodeURIComponent(camera)}`;
  if (pathC) q += `&path_contains=${encodeURIComponent(pathC)}`;
  if (dateFrom) q += `&date_from=${encodeURIComponent(dateFrom)}`;
  if (dateTo) q += `&date_to=${encodeURIComponent(dateTo)}`;
  if (minSize) q += `&min_size=${encodeURIComponent(minSize)}`;
  if (maxSize) q += `&max_size=${encodeURIComponent(maxSize)}`;
  if (hasGps) q += "&has_gps=true";
  if (hasPhash) q += "&has_phash=true";
  q += `&offset=${_filesOffset}`;
  try {
    const data = await api(q);
    if (!data.files.length) {
      $("#files-table").innerHTML = '<div class="empty"><div class="empty-icon">&#128269;</div><div class="empty-text">No files match your filters</div><div class="empty-hint">Try broadening your search or clearing some filters.</div></div>';
      return;
    }
    let t = `<table><tr><th>Name</th><th>Ext</th><th>Size</th><th>Camera</th><th>Date</th><th>GPS</th><th>Resolution</th></tr>`;
    for (const f of data.files) {
      const gps = f.gps_latitude ? `${f.gps_latitude.toFixed(4)}, ${f.gps_longitude.toFixed(4)}` : "";
      const res = f.width && f.height ? `${f.width}x${f.height}` : "";
      const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
      t += `<tr style="cursor:pointer" onclick="showFileDetail('${escapeHtml(f.path).replace(/'/g, "\\'")}')">
        <td class="path" title="${escapeHtml(f.path)}">${escapeHtml(fileName(f.path))}</td>
        <td>${escapeHtml(f.ext)}</td>
        <td>${formatBytes(f.size)}</td>
        <td>${escapeHtml(cam)}</td>
        <td>${escapeHtml(f.date_original ?? "")}</td>
        <td>${gps}</td>
        <td>${res}</td>
      </tr>`;
    }
    t += "</table>";
    // Pagination controls
    const pageNum = Math.floor(_filesOffset / FILES_PER_PAGE) + 1;
    const from = _filesOffset + 1;
    const to = _filesOffset + data.count;
    t += `<div class="pagination">
      <button ${_filesOffset === 0 ? "disabled" : ""} onclick="filesPagePrev()">&#8592; Previous</button>
      <span class="page-info">Showing ${from}–${to} (page ${pageNum})</span>
      <button ${!data.has_more ? "disabled" : ""} onclick="filesPageNext()">Next &#8594;</button>
    </div>`;
    $("#files-table").innerHTML = t;
  } catch (e) {
    $("#files-table").innerHTML = `<div class="empty">Error: ${e.message}</div>`;
  }
}
function filesPageNext() { _filesOffset += FILES_PER_PAGE; loadFiles(); }
function filesPagePrev() { _filesOffset = Math.max(0, _filesOffset - FILES_PER_PAGE); loadFiles(); }
// expose to onclick
window.loadFiles = loadFiles;
window.filesPageNext = filesPageNext;
window.filesPagePrev = filesPagePrev;

// ── Duplicates ───────────────────────────────────────

async function renderDuplicates() {
  try {
    const data = await api("/duplicates?limit=50");
    if (!data.groups.length) {
      content().innerHTML = '<h2>Duplicates</h2><div class="empty"><div class="empty-icon">&#9989;</div><div class="empty-text">No duplicates found</div><div class="empty-hint">Your library has no duplicate files. Great!</div></div>';
      return;
    }
    let html = `<h2>Duplicates <span style="color:var(--text-muted);font-size:14px">(${data.total_groups} groups)</span></h2>`;
    html += `<table><tr><th>Group</th><th>Files</th><th>Size</th><th>Action</th></tr>`;
    for (const g of data.groups) {
      html += `<tr>
        <td class="path">${escapeHtml(g.group_id.slice(0, 12))}</td>
        <td>${g.file_count}</td>
        <td>${formatBytes(g.total_size)}</td>
        <td><button onclick="showDiff('${escapeHtml(g.group_id)}')">Diff</button></td>
      </tr>`;
    }
    html += "</table>";
    html += '<div id="diff-detail"></div>';
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Duplicates</h2><div class="empty">Error: ${e.message}</div>`;
  }
}

async function showDiff(groupId) {
  const el = $("#diff-detail");
  el.innerHTML = '<div class="loading">Loading diff...</div>';
  try {
    const data = await api(`/duplicates/${groupId}/diff`);
    let html = `<h2 style="margin-top:20px">Metadata Diff — ${groupId.slice(0,12)}</h2>`;

    if (data.scores) {
      html += '<div class="diff-section"><h3>Richness Scores</h3>';
      for (const [path, score] of Object.entries(data.scores)) {
        html += `<div class="tag-row"><span class="tag-name">${escapeHtml(fileName(path))}</span><span class="tag-value">${Number(score).toFixed(1)} pts</span></div>`;
      }
      html += "</div>";
    }

    if (Object.keys(data.unanimous).length) {
      html += '<div class="diff-section"><h3 class="unanimous">Unanimous (' + Object.keys(data.unanimous).length + ' tags)</h3>';
      for (const [tag, val] of Object.entries(data.unanimous)) {
        html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(JSON.stringify(val))}</span></div>`;
      }
      html += "</div>";
    }

    if (Object.keys(data.partial).length) {
      html += '<div class="diff-section"><h3 class="partial">Partial (' + Object.keys(data.partial).length + ' tags — merge candidates)</h3>';
      for (const [tag, sources] of Object.entries(data.partial)) {
        for (const [path, val] of Object.entries(sources)) {
          html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(fileName(path))}: ${escapeHtml(JSON.stringify(val))}</span></div>`;
        }
      }
      html += "</div>";
    }

    if (Object.keys(data.conflicts).length) {
      html += '<div class="diff-section"><h3 class="conflicts">Conflicts (' + Object.keys(data.conflicts).length + ' tags)</h3>';
      for (const [tag, sources] of Object.entries(data.conflicts)) {
        for (const [path, val] of Object.entries(sources)) {
          html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(fileName(path))}: ${escapeHtml(JSON.stringify(val))}</span></div>`;
        }
      }
      html += "</div>";
    }

    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = `<div class="empty">Error: ${e.message}</div>`;
  }
}
window.showDiff = showDiff;

// ── Similar ──────────────────────────────────────────

async function renderSimilar() {
  try {
    const data = await api("/similar?threshold=10&limit=50");
    if (!data.pairs.length) {
      content().innerHTML = '<h2>Similar Images</h2><div class="empty"><div class="empty-icon">&#127912;</div><div class="empty-text">No similar pairs found</div><div class="empty-hint">Try increasing the threshold for looser matching.</div></div>';
      return;
    }
    let html = `<h2>Similar Images <span style="color:var(--text-muted);font-size:14px">(${data.total_pairs} pairs)</span></h2>`;
    html += '<div class="similar-grid">';
    for (const p of data.pairs) {
      html += `<div class="similar-pair">
        <div class="distance">Distance: ${p.distance}</div>
        <div class="thumbs">
          <img src="/api/thumbnail${p.path_a}?size=200" alt="${escapeHtml(fileName(p.path_a))}" onerror="this.style.display='none'">
          <img src="/api/thumbnail${p.path_b}?size=200" alt="${escapeHtml(fileName(p.path_b))}" onerror="this.style.display='none'">
        </div>
        <div style="margin-top:6px;font-size:11px;color:var(--text-muted)">
          ${escapeHtml(fileName(p.path_a))}<br>${escapeHtml(fileName(p.path_b))}
        </div>
      </div>`;
    }
    html += "</div>";
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Similar Images</h2><div class="empty">Error: ${e.message}</div>`;
  }
}

// ── Pipeline ─────────────────────────────────────────

let _pollInterval = null;

async function renderPipeline() {
  content().innerHTML = `<h2>Pipeline</h2>
    <p style="color:var(--text-muted);margin-bottom:16px">Run the full pipeline: scan → metadata extract → diff → merge</p>
    <div style="display:flex;gap:8px;margin-bottom:20px">
      <button class="primary" onclick="startPipeline()">Start Pipeline</button>
      <button onclick="startScan()">Scan Only</button>
    </div>
    <div id="task-output"></div>`;
}

async function startPipeline() {
  try {
    const data = await apiPost("/pipeline");
    pollTask(data.task_id);
  } catch (e) {
    $("#task-output").innerHTML = `<div class="task-status failed">Error: ${e.message}</div>`;
  }
}

async function startScan() {
  try {
    const data = await apiPost("/scan");
    pollTask(data.task_id);
  } catch (e) {
    $("#task-output").innerHTML = `<div class="task-status failed">Error: ${e.message}</div>`;
  }
}

function pollTask(taskId) {
  if (_pollInterval) clearInterval(_pollInterval);
  const el = $("#task-output");
  el.innerHTML = `<div class="task-status running">Task ${taskId}: running...</div>`;
  _pollInterval = setInterval(async () => {
    try {
      const data = await api(`/tasks/${taskId}`);
      if (data.status === "running") {
        el.innerHTML = `<div class="task-status running">Task ${taskId}: running... (started ${data.started_at})</div>`;
      } else {
        clearInterval(_pollInterval);
        _pollInterval = null;
        if (data.status === "completed") {
          let resultHtml = "";
          if (data.result) {
            resultHtml = "<pre>" + escapeHtml(JSON.stringify(data.result, null, 2)) + "</pre>";
          }
          el.innerHTML = `<div class="task-status completed">Task ${taskId}: completed${resultHtml}</div>`;
        } else {
          el.innerHTML = `<div class="task-status failed">Task ${taskId}: failed — ${escapeHtml(data.error)}</div>`;
        }
      }
    } catch (e) {
      clearInterval(_pollInterval);
      _pollInterval = null;
      el.innerHTML = `<div class="task-status failed">Error polling task: ${e.message}</div>`;
    }
  }, 2000);
}

window.startPipeline = startPipeline;
window.startScan = startScan;

// ── Doctor ───────────────────────────────────────────

async function renderDoctor() {
  try {
    const data = await api("/deps");
    let html = "<h2>Dependency Check</h2>";
    html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden">';
    for (const d of data.dependencies) {
      const status = d.available ? "ok" : "missing";
      const ver = d.version ? ` (${escapeHtml(d.version)})` : "";
      const hint = d.install_hint ? `<span class="dep-hint">${escapeHtml(d.install_hint)}</span>` : "";
      html += `<div class="dep-item">
        <div class="dep-status ${status}"></div>
        <strong>${escapeHtml(d.name)}</strong>${ver}
        ${hint}
      </div>`;
    }
    html += "</div>";
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Doctor</h2><div class="empty">Error: ${e.message}</div>`;
  }
}
