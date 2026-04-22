/* GOD MODE Media Library — Global Search */

import { $, escapeHtml, fileName, formatBytes, IMAGE_EXTS } from "./utils.js";
import { api } from "./api.js";

let _searchTimeout = null;
let _lastQuery = "";
let _selectedIndex = -1;
let _results = [];

function getInput() { return $("#global-search-input"); }
function getDropdown() { return $("#global-search-results"); }

export function initSearch() {
  const input = getInput();
  const dropdown = getDropdown();
  if (!input || !dropdown) return;

  input.addEventListener("input", () => {
    clearTimeout(_searchTimeout);
    const q = input.value.trim();
    if (q.length < 2) {
      hideResults();
      return;
    }
    _searchTimeout = setTimeout(() => runSearch(q), 250);
  });

  input.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      hideResults();
      input.blur();
      e.stopPropagation();
      return;
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      _selectedIndex = Math.min(_selectedIndex + 1, _results.length - 1);
      updateSelection();
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      _selectedIndex = Math.max(_selectedIndex - 1, -1);
      updateSelection();
      return;
    }
    if (e.key === "Enter" && _selectedIndex >= 0 && _results[_selectedIndex]) {
      e.preventDefault();
      openResult(_results[_selectedIndex]);
      return;
    }
  });

  input.addEventListener("focus", () => {
    if (_results.length > 0) showResults();
  });

  // Close on outside click
  document.addEventListener("click", (e) => {
    const container = $("#global-search");
    if (container && !container.contains(e.target)) {
      hideResults();
    }
  });

  // Cmd+K / Ctrl+K to focus search
  document.addEventListener("keydown", (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "k") {
      e.preventDefault();
      input.focus();
      input.select();
    }
  });
}

async function runSearch(q) {
  if (q === _lastQuery) return;
  _lastQuery = q;

  try {
    const data = await api(`/search?q=${encodeURIComponent(q)}&limit=20`);
    _results = data.items || [];
    _selectedIndex = -1;
    renderResults(data);
  } catch (err) {
    _results = [];
    renderError(err.message);
  }
}

function renderResults(data) {
  const dropdown = getDropdown();
  if (!dropdown) return;

  if (data.items.length === 0) {
    dropdown.innerHTML = `<div class="search-empty">No results for "${escapeHtml(data.query)}"</div>`;
    showResults();
    return;
  }

  const items = data.items.map((item, i) => {
    const name = fileName(item.path);
    const ext = (item.ext || "").toLowerCase();
    const isImage = IMAGE_EXTS.has(ext);
    const thumbSrc = isImage ? `/api/thumbnail${encodeURI(item.path)}?size=80` : "";
    const icon = isImage ? "" : (ext.match(/^(mp4|mov|avi|mkv|wmv|flv|webm)$/i) ? "&#127910;" : "&#128196;");
    const cam = [item.camera_make, item.camera_model].filter(Boolean).join(" ");
    const meta = [item.date_original, cam, formatBytes(item.size)].filter(Boolean).join(" &middot; ");

    return `<div class="search-result-item${i === _selectedIndex ? " selected" : ""}" data-index="${i}" role="option">
      <div class="search-result-thumb">
        ${isImage ? `<img src="${thumbSrc}" alt="" loading="lazy" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'"><span class="search-thumb-fallback" style="display:none">&#128444;</span>` : `<span class="search-thumb-fallback">${icon}</span>`}
      </div>
      <div class="search-result-info">
        <div class="search-result-name">${escapeHtml(name)}</div>
        <div class="search-result-path">${escapeHtml(item.path)}</div>
        <div class="search-result-meta">${meta}</div>
      </div>
    </div>`;
  }).join("");

  const footer = data.total > data.items.length
    ? `<div class="search-footer">${data.total} total results</div>`
    : "";

  dropdown.innerHTML = items + footer;
  showResults();

  // Click handlers
  dropdown.querySelectorAll(".search-result-item").forEach(el => {
    el.addEventListener("click", () => {
      const idx = parseInt(el.dataset.index, 10);
      if (_results[idx]) openResult(_results[idx]);
    });
    el.addEventListener("mouseenter", () => {
      _selectedIndex = parseInt(el.dataset.index, 10);
      updateSelection();
    });
  });
}

function renderError(msg) {
  const dropdown = getDropdown();
  if (!dropdown) return;
  dropdown.innerHTML = `<div class="search-empty">Search error: ${escapeHtml(msg)}</div>`;
  showResults();
}

function showResults() {
  const dropdown = getDropdown();
  if (dropdown) dropdown.classList.remove("hidden");
}

function hideResults() {
  const dropdown = getDropdown();
  if (dropdown) dropdown.classList.add("hidden");
  _selectedIndex = -1;
}

function updateSelection() {
  const dropdown = getDropdown();
  if (!dropdown) return;
  dropdown.querySelectorAll(".search-result-item").forEach((el, i) => {
    el.classList.toggle("selected", i === _selectedIndex);
    if (i === _selectedIndex) el.scrollIntoView({ block: "nearest" });
  });
}

function openResult(item) {
  hideResults();
  getInput().blur();
  // Navigate to files page and trigger file detail
  location.hash = "files";
  if (window._godmodeNavigate) window._godmodeNavigate("files");
  // Open the file detail after a short delay to let the page render
  setTimeout(() => {
    const event = new CustomEvent("gml-open-file", { detail: { path: item.path } });
    document.dispatchEvent(event);
  }, 300);
}
