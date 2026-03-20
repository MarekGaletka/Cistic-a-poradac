/* GOD MODE Media Library — Utility functions */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

export { $, $$ };

export function formatBytes(bytes) {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let val = bytes;
  while (val >= 1024 && i < units.length - 1) { val /= 1024; i++; }
  return `${val.toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
}

export function fileName(path) {
  return path.split("/").pop();
}

export function escapeHtml(str) {
  if (typeof str !== "string") return String(str ?? "");
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

export function showToast(message, type = "info") {
  const container = $("#toast-container");
  if (!container) return;
  const icons = { success: "\u2713", error: "\u2717", info: "\u2139" };
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.setAttribute("role", "status");
  toast.innerHTML = `<span class="toast-icon">${icons[type] || icons.info}</span><span class="toast-message"></span><div class="toast-progress"></div>`;
  toast.querySelector(".toast-message").textContent = message;
  const dismiss = () => {
    toast.classList.add("dismissing");
    toast.addEventListener("animationend", () => { if (toast.parentNode) toast.remove(); }, { once: true });
  };
  toast.addEventListener("click", dismiss);
  container.appendChild(toast);
  setTimeout(() => { if (toast.parentNode && !toast.classList.contains("dismissing")) dismiss(); }, 4000);
}

export function content() {
  return $("#content");
}

export const IMAGE_EXTS = new Set(["jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp", "heic", "heif"]);
