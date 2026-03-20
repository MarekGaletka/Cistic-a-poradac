/* GOD MODE Media Library — Vanilla JS Frontend */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);
const content = () => $("#content");

async function api(path) {
  const res = await fetch(`/api${path}`);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

async function apiPost(path, body = null) {
  const opts = { method: "POST" };
  if (body) {
    opts.headers = { "Content-Type": "application/json" };
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`/api${path}`, opts);
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
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

// ── Toast ────────────────────────────────────────────

function showToast(message, type = "info") {
  const container = $("#toast-container");
  if (!container) return;
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.setAttribute("role", "status");
  toast.textContent = message;
  toast.addEventListener("click", () => toast.remove());
  container.appendChild(toast);
  setTimeout(() => { if (toast.parentNode) toast.remove(); }, 4000);
}

// ── Modal ────────────────────────────────────────────

function closeModal() {
  const overlay = $(".modal-overlay");
  if (overlay) overlay.remove();
}

document.addEventListener("keydown", e => { if (e.key === "Escape") closeModal(); });

const IMAGE_EXTS = new Set(["jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp", "heic", "heif"]);

async function showFileDetail(filePath) {
  // Create overlay immediately with loading state
  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.setAttribute("role", "dialog");
  overlay.setAttribute("aria-label", "File detail");
  overlay.innerHTML = `<div class="modal"><button class="modal-close" aria-label="Close" onclick="closeModal()">&times;</button><div class="loading"><div class="spinner" role="status" aria-label="Loading"></div>Loading...</div></div>`;
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
      const thumbSrc = `/api/thumbnail${encodeURI(f.path)}?size=400`;
      thumbHtml = `<img class="modal-thumb" src="${thumbSrc}" onerror="this.outerHTML='<div class=\\'modal-thumb-placeholder\\'>&#128444;</div>'" alt="${escapeHtml(fileName(f.path))}">`;
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
      gpsHtml = `<div class="meta-row"><span class="meta-label">GPS</span><a class="gps-link" href="https://maps.google.com/?q=${f.gps_latitude},${f.gps_longitude}" target="_blank" rel="noopener noreferrer">${f.gps_latitude.toFixed(6)}, ${f.gps_longitude.toFixed(6)} &#x2197;</a></div>`;
    }

    // Basic info rows
    const cam = [f.camera_make, f.camera_model].filter(Boolean).join(" ");
    const res = f.width && f.height ? `${f.width} x ${f.height}` : "";
    const infoRows = [
      ["Size", formatBytes(f.size)],
      ["Extension", f.ext],
      ["Date", f.date_original || "\u2014"],
      cam ? ["Camera", cam] : null,
      res ? ["Resolution", res] : null,
      f.duration_seconds ? ["Duration", `${f.duration_seconds.toFixed(1)}s`] : null,
      f.video_codec ? ["Video", f.video_codec] : null,
      f.audio_codec ? ["Audio", f.audio_codec] : null,
      f.sha256 ? ["SHA-256", f.sha256.slice(0, 16) + "\u2026"] : null,
      f.phash ? ["PHash", f.phash.slice(0, 16) + "\u2026"] : null,
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
      <button class="modal-close" aria-label="Close" onclick="closeModal()">&times;</button>
      <div class="modal-header">
        ${thumbHtml}
        <div class="modal-info">
          <h3>${escapeHtml(fileName(f.path))}</h3>
          <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;word-break:break-all">${escapeHtml(f.path)}</div>
          ${richnessHtml ? `<div style="margin-bottom:12px">${richnessHtml}</div>` : ""}
          ${infoRows.map(([l, v]) => `<div class="meta-row"><span class="meta-label">${escapeHtml(l)}</span><span>${escapeHtml(v)}</span></div>`).join("")}
          ${gpsHtml}
        </div>
      </div>
      ${metaHtml}
    `;
  } catch (e) {
    const modalEl = overlay.querySelector(".modal");
    modalEl.innerHTML = `<button class="modal-close" aria-label="Close" onclick="closeModal()">&times;</button><div class="empty">Error loading file detail: ${escapeHtml(e.message)}</div>`;
  }
}
window.showFileDetail = showFileDetail;
window.closeModal = closeModal;

// ── Router ───────────────────────────────────────────

const pages = { dashboard: renderDashboard, files: renderFiles, duplicates: renderDuplicates, similar: renderSimilar, timeline: renderTimeline, map: renderMap, pipeline: renderPipeline, doctor: renderDoctor };

function navigate(page) {
  // Cleanup before navigating away
  _cleanupCurrentPage();

  $$("nav a").forEach(a => {
    const isActive = a.dataset.page === page;
    a.classList.toggle("active", isActive);
    if (isActive) a.setAttribute("aria-current", "page");
    else a.removeAttribute("aria-current");
  });
  content().innerHTML = '<div class="loading"><div class="spinner" role="status" aria-label="Loading"></div>Loading...</div>';
  if (pages[page]) pages[page]();
}

function _cleanupCurrentPage() {
  // Clear any polling intervals
  if (_pollInterval) {
    clearInterval(_pollInterval);
    _pollInterval = null;
  }
  // Clean up Leaflet map
  if (_leafletMap) {
    _leafletMap.remove();
    _leafletMap = null;
  }
}

// Hamburger toggle for mobile
const navToggle = $(".nav-toggle");
if (navToggle) {
  navToggle.addEventListener("click", () => {
    const nav = $("nav");
    const isOpen = nav.classList.toggle("open");
    navToggle.setAttribute("aria-expanded", isOpen);
  });
}

document.addEventListener("click", e => {
  const link = e.target.closest("nav a[data-page]");
  if (link) {
    e.preventDefault();
    navigate(link.dataset.page);
    // Close nav on mobile after navigation
    $("nav").classList.remove("open");
    navToggle?.setAttribute("aria-expanded", "false");
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

    // top_extensions is an array of [ext, count] pairs
    const exts = stats.top_extensions;
    if (exts && exts.length) {
      html += "<h2>Top Extensions</h2><table><tr><th>Extension</th><th>Count</th></tr>";
      for (const [ext, count] of exts) {
        html += `<tr><td>.${escapeHtml(ext)}</td><td>${count.toLocaleString()}</td></tr>`;
      }
      html += "</table>";
    }

    // top_cameras is an array of [camera, count] pairs
    const cams = stats.top_cameras;
    if (cams && cams.length) {
      html += "<h2>Top Cameras</h2><table><tr><th>Camera</th><th>Count</th></tr>";
      for (const [cam, count] of cams) {
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
    <div class="filters" role="search" aria-label="File filters">
      <input type="text" id="f-ext" placeholder="Extension (jpg)" size="10" aria-label="Filter by extension">
      <input type="text" id="f-camera" placeholder="Camera" size="15" aria-label="Filter by camera">
      <input type="text" id="f-path" placeholder="Path contains..." size="20" aria-label="Filter by path">
      <button onclick="_filesOffset=0;loadFiles()" aria-label="Search files">Search</button>
    </div>
    <div class="filters filters-advanced">
      <div class="filter-group"><label for="f-date-from">From</label><input type="date" id="f-date-from"></div>
      <div class="filter-group"><label for="f-date-to">To</label><input type="date" id="f-date-to"></div>
      <div class="filter-group"><label for="f-min-size">Min KB</label><input type="number" id="f-min-size" min="0" style="width:80px"></div>
      <div class="filter-group"><label for="f-max-size">Max KB</label><input type="number" id="f-max-size" min="0" style="width:80px"></div>
      <label class="filter-checkbox"><input type="checkbox" id="f-has-gps"> GPS</label>
      <label class="filter-checkbox"><input type="checkbox" id="f-has-phash"> PHash</label>
    </div>
    <div id="files-table" aria-live="polite"></div>`;
  content().innerHTML = html;

  // Enter key triggers search
  const searchInputs = content().querySelectorAll(".filters input");
  searchInputs.forEach(input => {
    input.addEventListener("keydown", e => {
      if (e.key === "Enter") { _filesOffset = 0; loadFiles(); }
    });
  });

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
      t += `<tr style="cursor:pointer" tabindex="0" role="button" aria-label="View ${escapeHtml(fileName(f.path))}" onclick="showFileDetail('${escapeHtml(f.path).replace(/'/g, "\\'")}')">
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
    t += `<div class="pagination" role="navigation" aria-label="Pagination">
      <button ${_filesOffset === 0 ? "disabled" : ""} onclick="filesPagePrev()" aria-label="Previous page">&#8592; Previous</button>
      <span class="page-info" aria-live="polite">Showing ${from}\u2013${to} (page ${pageNum})</span>
      <button ${!data.has_more ? "disabled" : ""} onclick="filesPageNext()" aria-label="Next page">Next &#8594;</button>
    </div>`;
    $("#files-table").innerHTML = t;
  } catch (e) {
    $("#files-table").innerHTML = `<div class="empty">Error: ${escapeHtml(e.message)}</div>`;
  }
}
function filesPageNext() { _filesOffset += FILES_PER_PAGE; loadFiles(); }
function filesPagePrev() { _filesOffset = Math.max(0, _filesOffset - FILES_PER_PAGE); loadFiles(); }
// expose to onclick
window.loadFiles = loadFiles;
window.filesPageNext = filesPageNext;
window.filesPagePrev = filesPagePrev;

// Keyboard support for table rows
document.addEventListener("keydown", e => {
  if (e.key === "Enter" && e.target.matches("tr[role='button']")) {
    e.target.click();
  }
});

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
        <td><button onclick="showDiff('${escapeHtml(g.group_id)}')" aria-label="Show diff for group ${escapeHtml(g.group_id.slice(0, 8))}">Diff</button></td>
      </tr>`;
    }
    html += "</table>";
    html += '<div id="diff-detail" aria-live="polite"></div>';
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Duplicates</h2><div class="empty">Error: ${escapeHtml(e.message)}</div>`;
  }
}

async function showDiff(groupId) {
  const el = $("#diff-detail");
  el.innerHTML = '<div class="loading"><div class="spinner" role="status" aria-label="Loading"></div>Loading diff...</div>';
  try {
    const [groupData, diffData] = await Promise.all([
      api(`/duplicates/${encodeURIComponent(groupId)}`),
      api(`/duplicates/${encodeURIComponent(groupId)}/diff`),
    ]);

    const files = groupData.files || [];
    const scores = diffData.scores || {};

    // Find the winner (highest richness score)
    let winnerPath = null;
    let winnerScore = -1;
    for (const [path, score] of Object.entries(scores)) {
      if (score > winnerScore) { winnerScore = score; winnerPath = path; }
    }

    let html = `<h2 style="margin-top:20px">Metadata Diff \u2014 ${escapeHtml(groupId.slice(0, 12))}</h2>`;

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

    // Diff sections with collapsible details
    if (Object.keys(diffData.unanimous).length) {
      html += `<details class="diff-section"><summary class="diff-toggle unanimous">Unanimous (${Object.keys(diffData.unanimous).length} tags)</summary>`;
      for (const [tag, val] of Object.entries(diffData.unanimous)) {
        html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(JSON.stringify(val))}</span></div>`;
      }
      html += "</details>";
    }

    if (Object.keys(diffData.partial).length) {
      html += `<details class="diff-section" open><summary class="diff-toggle partial">Partial (${Object.keys(diffData.partial).length} tags \u2014 merge candidates)</summary>`;
      for (const [tag, sources] of Object.entries(diffData.partial)) {
        for (const [path, val] of Object.entries(sources)) {
          html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(fileName(path))}: ${escapeHtml(JSON.stringify(val))}</span></div>`;
        }
      }
      html += "</details>";
    }

    if (Object.keys(diffData.conflicts).length) {
      html += `<details class="diff-section" open><summary class="diff-toggle conflicts">Conflicts (${Object.keys(diffData.conflicts).length} tags)</summary>`;
      for (const [tag, sources] of Object.entries(diffData.conflicts)) {
        for (const [path, val] of Object.entries(sources)) {
          html += `<div class="tag-row"><span class="tag-name">${escapeHtml(tag)}</span><span class="tag-value">${escapeHtml(fileName(path))}: ${escapeHtml(JSON.stringify(val))}</span></div>`;
        }
      }
      html += "</details>";
    }

    el.innerHTML = html;
  } catch (e) {
    el.innerHTML = `<div class="empty">Error: ${escapeHtml(e.message)}</div>`;
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
      const srcA = `/api/thumbnail${encodeURI(p.path_a)}?size=200`;
      const srcB = `/api/thumbnail${encodeURI(p.path_b)}?size=200`;
      html += `<div class="similar-pair">
        <div class="distance">Distance: ${p.distance}</div>
        <div class="thumbs">
          <img src="${srcA}" alt="${escapeHtml(fileName(p.path_a))}" onerror="this.style.display='none'">
          <img src="${srcB}" alt="${escapeHtml(fileName(p.path_b))}" onerror="this.style.display='none'">
        </div>
        <div style="margin-top:6px;font-size:12px;color:var(--text-muted)">
          ${escapeHtml(fileName(p.path_a))}<br>${escapeHtml(fileName(p.path_b))}
        </div>
      </div>`;
    }
    html += "</div>";
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Similar Images</h2><div class="empty">Error: ${escapeHtml(e.message)}</div>`;
  }
}

// ── Timeline ─────────────────────────────────────────

async function renderTimeline() {
  try {
    const data = await api("/files?limit=500");
    const files = data.files.filter(f => f.date_original);

    if (!files.length) {
      content().innerHTML = '<h2>Timeline</h2><div class="empty"><div class="empty-icon">&#128197;</div><div class="empty-text">No files with dates found</div><div class="empty-hint">Files need date_original metadata (from EXIF or ExifTool extraction).</div></div>';
      return;
    }

    // Group files by month
    const groups = {};
    for (const f of files) {
      const date = f.date_original;
      const match = date.match(/^(\d{4})[:\-/](\d{2})/);
      const key = match ? `${match[1]}-${match[2]}` : "Unknown";
      if (!groups[key]) groups[key] = [];
      groups[key].push(f);
    }

    // Sort months descending
    const sortedMonths = Object.keys(groups).sort().reverse();

    let html = `<h2>Timeline <span style="color:var(--text-muted);font-size:14px">(${files.length} dated files)</span></h2>`;
    html += '<div class="timeline">';

    for (const month of sortedMonths) {
      const monthFiles = groups[month];
      const [y, m] = month.split("-");
      const monthName = m ? new Date(parseInt(y), parseInt(m) - 1).toLocaleDateString("en", { year: "numeric", month: "long" }) : month;

      html += `<div class="timeline-month">
        <div class="timeline-header">${escapeHtml(monthName)} <span class="timeline-count">(${monthFiles.length})</span></div>
        <div class="timeline-grid">`;

      for (const f of monthFiles.slice(0, 20)) {
        const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
        const thumb = isImage
          ? `<img src="/api/thumbnail${encodeURI(f.path)}?size=150" onerror="this.style.display='none'" alt="${escapeHtml(fileName(f.path))}">`
          : `<div class="timeline-icon">${escapeHtml(f.ext)}</div>`;
        html += `<div class="timeline-item" tabindex="0" role="button" onclick="showFileDetail('${escapeHtml(f.path).replace(/'/g, "\\'")}')" title="${escapeHtml(f.path)}">
          ${thumb}
          <div class="timeline-name">${escapeHtml(fileName(f.path))}</div>
        </div>`;
      }
      if (monthFiles.length > 20) {
        html += `<div class="timeline-more">+${monthFiles.length - 20} more</div>`;
      }
      html += '</div></div>';
    }

    html += '</div>';
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Timeline</h2><div class="empty">Error: ${escapeHtml(e.message)}</div>`;
  }
}

// ── Map ──────────────────────────────────────────────

let _leafletMap = null;
let _mapMarkers = [];

async function renderMap() {
  content().innerHTML = '<h2>Map</h2><div id="map-container"></div>';

  try {
    const data = await api("/files?has_gps=true&limit=5000");
    const files = data.files.filter(f => f.gps_latitude && f.gps_longitude);

    if (!files.length) {
      content().innerHTML = '<h2>Map</h2><div class="empty"><div class="empty-icon">&#127758;</div><div class="empty-text">No geotagged files found</div><div class="empty-hint">Files need GPS metadata from EXIF. Run ExifTool extraction to populate GPS data.</div></div>';
      return;
    }

    // Clean up previous map instance
    if (_leafletMap) { _leafletMap.remove(); _leafletMap = null; }
    _mapMarkers = [];

    if (typeof L === "undefined") {
      content().innerHTML = '<h2>Map</h2><div class="empty">Leaflet.js not loaded. Check your internet connection.</div>';
      return;
    }

    _leafletMap = L.map("map-container").setView([0, 0], 2);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
    }).addTo(_leafletMap);

    // Add markers
    const bounds = [];
    for (const f of files) {
      const lat = f.gps_latitude;
      const lng = f.gps_longitude;
      bounds.push([lat, lng]);

      const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
      const thumbUrl = isImage ? `/api/thumbnail${encodeURI(f.path)}?size=150` : "";
      const thumbHtml = isImage ? `<img src="${thumbUrl}" style="width:120px;height:80px;object-fit:cover;border-radius:4px;margin-bottom:4px;display:block" onerror="this.style.display='none'" alt="">` : "";

      const popup = `<div style="font-size:12px;max-width:160px">
        ${thumbHtml}
        <strong>${escapeHtml(fileName(f.path))}</strong><br>
        <span style="color:#666">${escapeHtml(f.date_original || "")}</span><br>
        <a href="#" onclick="event.preventDefault();closeAllPopups();showFileDetail('${escapeHtml(f.path).replace(/'/g, "\\'")}')">Details</a>
      </div>`;

      const marker = L.marker([lat, lng]).addTo(_leafletMap).bindPopup(popup);
      _mapMarkers.push(marker);
    }

    // Fit map to markers
    if (bounds.length) {
      _leafletMap.fitBounds(bounds, { padding: [20, 20] });
    }

    // Fix Leaflet tile rendering issue with dynamic containers
    setTimeout(() => { if (_leafletMap) _leafletMap.invalidateSize(); }, 100);

  } catch (e) {
    content().innerHTML = `<h2>Map</h2><div class="empty">Error: ${escapeHtml(e.message)}</div>`;
  }
}

function closeAllPopups() {
  if (_leafletMap) _leafletMap.closePopup();
}
window.closeAllPopups = closeAllPopups;

// ── Pipeline ─────────────────────────────────────────

let _pollInterval = null;

async function renderPipeline() {
  content().innerHTML = `<h2>Pipeline</h2>
    <p style="color:var(--text-muted);margin-bottom:16px">Run the full pipeline: scan \u2192 metadata extract \u2192 diff \u2192 merge</p>
    <div class="config-form">
      <div class="form-group">
        <label class="form-label" for="cfg-roots">Roots (one per line)</label>
        <textarea id="cfg-roots" rows="3" placeholder="/Users/me/Photos&#10;/Volumes/External/Backup" aria-label="Scan root directories"></textarea>
      </div>
      <div class="form-row">
        <div class="form-group">
          <label class="form-label" for="cfg-workers">Workers</label>
          <input type="number" id="cfg-workers" value="1" min="1" max="16" style="width:70px">
        </div>
        <label class="filter-checkbox"><input type="checkbox" id="cfg-exiftool" checked> ExifTool extraction</label>
      </div>
    </div>
    <div style="display:flex;gap:8px;margin-bottom:20px">
      <button class="primary" onclick="startPipeline()" aria-label="Start full pipeline">Start Pipeline</button>
      <button onclick="startScan()" aria-label="Start scan only">Scan Only</button>
    </div>
    <div id="task-output" aria-live="polite"></div>`;
}

function _getScanConfig() {
  const rootsText = $("#cfg-roots")?.value || "";
  const roots = rootsText.split("\n").map(s => s.trim()).filter(Boolean);
  const workers = parseInt($("#cfg-workers")?.value || "1", 10);
  const extract_exiftool = $("#cfg-exiftool")?.checked ?? true;
  const body = { workers, extract_exiftool };
  if (roots.length) body.roots = roots;
  return body;
}

async function startPipeline() {
  try {
    const data = await apiPost("/pipeline", _getScanConfig());
    showToast("Pipeline started", "info");
    pollTask(data.task_id);
  } catch (e) {
    showToast("Failed to start pipeline: " + e.message, "error");
    $("#task-output").innerHTML = `<div class="task-status failed">Error: ${escapeHtml(e.message)}</div>`;
  }
}

async function startScan() {
  try {
    const data = await apiPost("/scan", _getScanConfig());
    showToast("Scan started", "info");
    pollTask(data.task_id);
  } catch (e) {
    showToast("Failed to start scan: " + e.message, "error");
    $("#task-output").innerHTML = `<div class="task-status failed">Error: ${escapeHtml(e.message)}</div>`;
  }
}

let _pollErrorCount = 0;

function pollTask(taskId) {
  if (_pollInterval) clearInterval(_pollInterval);
  _pollErrorCount = 0;
  const el = $("#task-output");
  if (!el) return;
  el.innerHTML = `<div class="task-status running">Task ${escapeHtml(taskId)}: running...</div>`;
  _pollInterval = setInterval(async () => {
    // Stop polling if the element is gone (navigated away)
    if (!document.getElementById("task-output")) {
      clearInterval(_pollInterval);
      _pollInterval = null;
      return;
    }
    try {
      const data = await api(`/tasks/${encodeURIComponent(taskId)}`);
      _pollErrorCount = 0;
      if (data.status === "running") {
        let progressHtml = "";
        if (data.progress) {
          const p = data.progress;
          const pct = p.total > 0 ? Math.round((p.processed / p.total) * 100) : 0;
          progressHtml = `<div class="progress-bar" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100"><div class="progress-fill" style="width:${pct}%"></div></div>
            <div style="font-size:12px;color:var(--text-muted);margin-top:4px">${escapeHtml(p.phase)}: ${p.processed.toLocaleString()} / ${p.total.toLocaleString()} (${pct}%)</div>`;
        }
        el.innerHTML = `<div class="task-status running">Task ${escapeHtml(taskId)}: running... (started ${escapeHtml(data.started_at)})${progressHtml}</div>`;
      } else {
        clearInterval(_pollInterval);
        _pollInterval = null;
        if (data.status === "completed") {
          let resultHtml = "";
          if (data.result) {
            resultHtml = "<pre>" + escapeHtml(JSON.stringify(data.result, null, 2)) + "</pre>";
          }
          el.innerHTML = `<div class="task-status completed">Task ${escapeHtml(taskId)}: completed${resultHtml}</div>`;
          showToast("Task completed successfully", "success");
        } else {
          el.innerHTML = `<div class="task-status failed">Task ${escapeHtml(taskId)}: failed \u2014 ${escapeHtml(data.error)}</div>`;
          showToast("Task failed: " + (data.error || "unknown error"), "error");
        }
      }
    } catch (e) {
      _pollErrorCount++;
      if (_pollErrorCount >= 5) {
        clearInterval(_pollInterval);
        _pollInterval = null;
        el.innerHTML = `<div class="task-status failed">Lost connection to server after ${_pollErrorCount} retries: ${escapeHtml(e.message)}</div>`;
      }
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
        <div class="dep-status ${status}" aria-label="${d.available ? "Available" : "Missing"}"></div>
        <strong>${escapeHtml(d.name)}</strong>${ver}
        ${hint}
      </div>`;
    }
    html += "</div>";
    content().innerHTML = html;
  } catch (e) {
    content().innerHTML = `<h2>Doctor</h2><div class="empty">Error: ${escapeHtml(e.message)}</div>`;
  }
}
