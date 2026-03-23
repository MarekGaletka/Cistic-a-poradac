/* GOD MODE Media Library — Tag management module */

import { api, apiPost, apiDelete } from "./api.js";
import { escapeHtml, showToast } from "./utils.js";
import { t } from "./i18n.js";

const TAG_COLORS = [
  { name: "Modr\u00e1", hex: "#58a6ff" },
  { name: "Zelen\u00e1", hex: "#3fb950" },
  { name: "\u010cerven\u00e1", hex: "#f85149" },
  { name: "\u017dlut\u00e1", hex: "#d29922" },
  { name: "Fialov\u00e1", hex: "#bc8cff" },
  { name: "Oran\u017eov\u00e1", hex: "#f0883e" },
];

// Finder-style color presets (macOS-inspired)
const FINDER_PRESETS = [
  { name: "tags.color_red", color: "#f85149" },
  { name: "tags.color_orange", color: "#f0883e" },
  { name: "tags.color_yellow", color: "#d29922" },
  { name: "tags.color_green", color: "#3fb950" },
  { name: "tags.color_blue", color: "#58a6ff" },
  { name: "tags.color_purple", color: "#bc8cff" },
  { name: "tags.color_gray", color: "#8b949e" },
];

let _allTags = [];

// ── Load tags ─────────────────────────────────────────

export async function loadTags() {
  try {
    const data = await api("/tags");
    _allTags = data.tags || [];
  } catch {
    _allTags = [];
  }
  return _allTags;
}

export function getAllTags() {
  return _allTags;
}

// ── Render tag pills for a file ───────────────────────

export function renderFileTags(tags) {
  if (!tags || !tags.length) return "";
  return tags.map(tag =>
    `<span class="tag-pill" style="background:${tag.color}22;color:${tag.color};border:1px solid ${tag.color}44" data-tag-id="${tag.id}">${escapeHtml(tag.name)}</span>`
  ).join("");
}

export function renderFileTagsWithRemove(tags, filePath) {
  if (!tags || !tags.length) return `<div class="tags-empty">${t("tags.no_tags")}</div>`;
  return tags.map(tag =>
    `<span class="tag-pill" style="background:${tag.color}22;color:${tag.color};border:1px solid ${tag.color}44" data-tag-id="${tag.id}">${escapeHtml(tag.name)}<span class="tag-pill-remove" data-tag-id="${tag.id}" data-file-path="${escapeHtml(filePath)}">&times;</span></span>`
  ).join("");
}

// ── Tag dots on thumbnails ────────────────────────────

export function renderTagDots(tags) {
  if (!tags || !tags.length) return "";
  return `<div class="thumb-tags">${tags.map(tag =>
    `<span class="thumb-tag-dot" style="background:${tag.color}" title="${escapeHtml(tag.name)}"></span>`
  ).join("")}</div>`;
}

// ── Tag picker popover ────────────────────────────────

let _activePickerCleanup = null;

export function openTagPicker(anchorEl, paths, onDone) {
  closeTagPicker();

  const picker = document.createElement("div");
  picker.className = "tag-picker";

  _renderPickerContent(picker, paths, onDone);

  // Position near the anchor
  document.body.appendChild(picker);
  const rect = anchorEl.getBoundingClientRect();
  const pickerRect = picker.getBoundingClientRect();

  let left = rect.left;
  let top = rect.bottom + 4;

  // Keep within viewport
  if (left + pickerRect.width > window.innerWidth) {
    left = window.innerWidth - pickerRect.width - 8;
  }
  if (top + pickerRect.height > window.innerHeight) {
    top = rect.top - pickerRect.height - 4;
  }

  picker.style.left = left + "px";
  picker.style.top = top + "px";
  picker.style.position = "fixed";

  // Close on outside click
  const outsideHandler = (e) => {
    if (!picker.contains(e.target) && e.target !== anchorEl) {
      closeTagPicker();
    }
  };
  setTimeout(() => document.addEventListener("click", outsideHandler), 10);

  _activePickerCleanup = () => {
    document.removeEventListener("click", outsideHandler);
    if (picker.parentNode) picker.remove();
    _activePickerCleanup = null;
  };
}

export function closeTagPicker() {
  if (_activePickerCleanup) _activePickerCleanup();
}

async function _renderPickerContent(picker, paths, onDone) {
  await loadTags();

  let html = `<div class="tag-picker-header" style="font-size:13px;font-weight:600;margin-bottom:8px">${t("tags.add_to_file")}</div>`;

  // Finder-style color presets
  html += `<div class="tag-presets-section" style="margin-bottom:8px">
    <div class="tag-presets-label" style="font-size:11px;color:var(--text-muted);margin-bottom:4px">${t("tags.presets")}</div>
    <div class="tag-presets">`;
  for (const preset of FINDER_PRESETS) {
    html += `<span class="tag-preset" data-preset-color="${preset.color}" data-preset-name="${t(preset.name)}" style="background:${preset.color}" title="${t(preset.name)}"></span>`;
  }
  html += `</div></div>`;

  if (_allTags.length === 0) {
    html += `<div style="font-size:12px;color:var(--text-muted);padding:8px 0">${t("tags.no_tags")}</div>`;
  } else {
    html += `<div class="tag-picker-list">`;
    for (const tag of _allTags) {
      html += `<div class="tag-picker-item" data-tag-id="${tag.id}">
        <span class="tag-picker-color" style="background:${tag.color}"></span>
        <span class="tag-picker-name">${escapeHtml(tag.name)}</span>
        <span class="tag-picker-check" data-tag-id="${tag.id}"></span>
      </div>`;
    }
    html += `</div>`;
  }

  // Auto-suggestions placeholder
  html += `<div class="tag-suggestions" id="tag-suggestions" style="display:none">
    <div class="tag-presets-label" style="font-size:11px;color:var(--text-muted);margin-bottom:4px">${t("tags.suggested")}</div>
    <div class="tag-suggestions-list" id="tag-suggestions-list"></div>
  </div>`;

  // Inline create
  html += `<div class="tag-create-row">
    <input class="tag-create-input" type="text" placeholder="${t("tags.create_placeholder")}" maxlength="50">
    <div class="tag-color-presets">`;
  for (const c of TAG_COLORS) {
    html += `<span class="tag-color-preset${c.hex === '#58a6ff' ? ' active' : ''}" data-color="${c.hex}" style="background:${c.hex}" title="${c.name}"></span>`;
  }
  html += `</div></div>`;

  picker.innerHTML = html;

  // Fetch auto-suggestions if single file
  if (paths.length === 1) {
    _loadSuggestions(picker, paths[0], paths, onDone);
  }

  // Bind Finder preset clicks — create tag with color name and apply it
  picker.querySelectorAll(".tag-preset").forEach(preset => {
    preset.addEventListener("click", async () => {
      const name = preset.dataset.presetName;
      const color = preset.dataset.presetColor;
      try {
        // Create tag if it doesn't exist, then apply
        let tag = _allTags.find(t => t.name === name);
        if (!tag) {
          tag = await apiPost("/tags", { name, color });
          await loadTags();
        }
        await apiPost("/files/tag", { paths, tag_id: tag.id });
        showToast(t("tags.tagged"), "success");
        if (onDone) onDone();
        closeTagPicker();
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  });

  // Bind tag toggle
  picker.querySelectorAll(".tag-picker-item").forEach(item => {
    item.addEventListener("click", async () => {
      const tagId = parseInt(item.dataset.tagId, 10);
      try {
        await apiPost("/files/tag", { paths, tag_id: tagId });
        showToast(paths.length > 1 ? t("tags.files_tagged", { count: paths.length }) : t("tags.tagged"), "success");
        if (onDone) onDone();
        closeTagPicker();
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  });

  // Color preset selection
  let selectedColor = "#58a6ff";
  picker.querySelectorAll(".tag-color-preset").forEach(preset => {
    preset.addEventListener("click", () => {
      picker.querySelectorAll(".tag-color-preset").forEach(p => p.classList.remove("active"));
      preset.classList.add("active");
      selectedColor = preset.dataset.color;
    });
  });

  // Inline create on Enter
  const input = picker.querySelector(".tag-create-input");
  if (input) {
    input.addEventListener("keydown", async (e) => {
      if (e.key === "Enter" && input.value.trim()) {
        try {
          const newTag = await apiPost("/tags", { name: input.value.trim(), color: selectedColor });
          showToast(t("tags.created"), "success");
          // Auto-tag the files with the new tag
          await apiPost("/files/tag", { paths, tag_id: newTag.id });
          showToast(paths.length > 1 ? t("tags.files_tagged", { count: paths.length }) : t("tags.tagged"), "success");
          if (onDone) onDone();
          closeTagPicker();
        } catch (e) {
          showToast(t("general.error", { message: e.message }), "error");
        }
      }
    });
    // Focus the input
    setTimeout(() => input.focus(), 50);
  }
}

// ── Tag manager modal ─────────────────────────────────

export async function openTagManager() {
  await loadTags();

  const overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.innerHTML = `
    <div class="modal" style="max-width:480px">
      <button class="modal-close" id="tag-manager-close">&times;</button>
      <h3 style="margin-bottom:16px">${t("tags.manage")}</h3>
      <div class="tag-manager-list" id="tag-manager-list"></div>
      <div class="tag-create-row" style="margin-top:12px">
        <input class="tag-create-input" type="text" id="tag-manager-new-name" placeholder="${t("tags.create_placeholder")}" maxlength="50">
        <div class="tag-color-presets" id="tag-manager-colors"></div>
        <button class="primary" id="tag-manager-create-btn" style="white-space:nowrap">${t("tags.create")}</button>
      </div>
    </div>`;

  document.body.appendChild(overlay);

  _renderManagerList(overlay);
  _renderManagerColors(overlay);

  // Close
  overlay.querySelector("#tag-manager-close").addEventListener("click", () => overlay.remove());
  overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });

  // Create
  let selectedColor = "#58a6ff";
  overlay.querySelectorAll(".tag-color-preset").forEach(preset => {
    preset.addEventListener("click", () => {
      overlay.querySelectorAll(".tag-color-preset").forEach(p => p.classList.remove("active"));
      preset.classList.add("active");
      selectedColor = preset.dataset.color;
    });
  });

  const createBtn = overlay.querySelector("#tag-manager-create-btn");
  const nameInput = overlay.querySelector("#tag-manager-new-name");

  const doCreate = async () => {
    const name = nameInput.value.trim();
    if (!name) return;
    try {
      await apiPost("/tags", { name, color: selectedColor });
      showToast(t("tags.created"), "success");
      nameInput.value = "";
      await loadTags();
      _renderManagerList(overlay);
    } catch (e) {
      showToast(t("general.error", { message: e.message }), "error");
    }
  };

  createBtn.addEventListener("click", doCreate);
  nameInput.addEventListener("keydown", (e) => { if (e.key === "Enter") doCreate(); });
}

function _renderManagerList(overlay) {
  const listEl = overlay.querySelector("#tag-manager-list");
  if (!listEl) return;

  if (_allTags.length === 0) {
    listEl.innerHTML = `<div style="font-size:13px;color:var(--text-muted);padding:16px 0;text-align:center">${t("tags.no_tags")}</div>`;
    return;
  }

  let html = "";
  for (const tag of _allTags) {
    html += `<div class="tag-manager-item" data-tag-id="${tag.id}">
      <span class="tag-picker-color" style="background:${tag.color}"></span>
      <span style="flex:1;font-size:13px">${escapeHtml(tag.name)}</span>
      <span style="font-size:11px;color:var(--text-muted)">${tag.file_count} souborů</span>
      <span class="tag-manager-delete" data-tag-id="${tag.id}" data-tag-name="${escapeHtml(tag.name)}" title="${t("action.delete")}">&times;</span>
    </div>`;
  }
  listEl.innerHTML = html;

  // Bind delete
  listEl.querySelectorAll(".tag-manager-delete").forEach(btn => {
    btn.addEventListener("click", async () => {
      const tagId = parseInt(btn.dataset.tagId, 10);
      const tagName = btn.dataset.tagName;
      if (!confirm(t("tags.delete_confirm", { name: tagName }))) return;
      try {
        await apiDelete(`/tags/${tagId}`);
        showToast(t("tags.deleted"), "success");
        await loadTags();
        _renderManagerList(overlay);
      } catch (e) {
        showToast(t("general.error", { message: e.message }), "error");
      }
    });
  });
}

function _renderManagerColors(overlay) {
  const colorsEl = overlay.querySelector("#tag-manager-colors");
  if (!colorsEl) return;
  let html = "";
  for (const c of TAG_COLORS) {
    html += `<span class="tag-color-preset${c.hex === '#58a6ff' ? ' active' : ''}" data-color="${c.hex}" style="background:${c.hex}" title="${c.name}"></span>`;
  }
  colorsEl.innerHTML = html;
}

// ── Auto-tag suggestions ─────────────────────────────

async function _loadSuggestions(picker, filePath, paths, onDone) {
  try {
    const data = await api(`/tags/suggest?path=${encodeURIComponent(filePath)}`);
    const suggestions = data.suggestions || [];
    if (suggestions.length === 0) return;

    const container = picker.querySelector("#tag-suggestions");
    const list = picker.querySelector("#tag-suggestions-list");
    if (!container || !list) return;

    container.style.display = "block";
    let html = "";
    for (const s of suggestions) {
      html += `<span class="tag-suggestion" data-suggest-name="${escapeHtml(s.name)}" data-suggest-color="${s.color}" style="background:${s.color}22;color:${s.color};border:1px solid ${s.color}44" title="${escapeHtml(s.reason)}">+ ${escapeHtml(s.name)}</span>`;
    }
    list.innerHTML = html;

    // Bind suggestion clicks — create tag and apply
    list.querySelectorAll(".tag-suggestion").forEach(el => {
      el.addEventListener("click", async () => {
        const name = el.dataset.suggestName;
        const color = el.dataset.suggestColor;
        try {
          let tag = _allTags.find(t => t.name === name);
          if (!tag) {
            tag = await apiPost("/tags", { name, color });
            await loadTags();
          }
          await apiPost("/files/tag", { paths, tag_id: tag.id });
          showToast(t("tags.tagged"), "success");
          if (onDone) onDone();
          closeTagPicker();
        } catch (e) {
          showToast(t("general.error", { message: e.message }), "error");
        }
      });
    });
  } catch {
    // Silently ignore suggestion errors
  }
}
