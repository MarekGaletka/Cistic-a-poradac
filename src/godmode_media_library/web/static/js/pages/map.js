/* GOD MODE Media Library — Map page */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";

let _leafletMap = null;
let _mapMarkers = [];

export function cleanup() {
  if (_leafletMap) {
    _leafletMap.remove();
    _leafletMap = null;
  }
  _mapMarkers = [];
}

export async function render(container) {
  container.innerHTML = `
    <div class="page-header"><h2>${t("map.title")}</h2></div>
    <div id="map-container"></div>`;

  try {
    const data = await api("/files?has_gps=true&limit=5000");
    const files = data.files.filter(f => f.gps_latitude && f.gps_longitude);

    if (!files.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("map.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon" style="font-size:48px">&#127758;</div>
          <h3 class="empty-state-title">${t("map.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("map.empty_hint")}</p>
        </div>`;
      return;
    }

    cleanup();

    if (typeof L === "undefined") {
      container.innerHTML = `<div class="page-header"><h2>${t("map.title")}</h2></div><div class="empty">${t("map.leaflet_error")}</div>`;
      return;
    }

    _leafletMap = L.map("map-container").setView([0, 0], 2);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
    }).addTo(_leafletMap);

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
        <a href="#" class="map-detail-link" data-path="${escapeHtml(f.path)}">${t("map.details")}</a>
      </div>`;

      const marker = L.marker([lat, lng]).addTo(_leafletMap).bindPopup(popup);
      _mapMarkers.push(marker);
    }

    // Bind popup detail links via event delegation
    _leafletMap.on("popupopen", (e) => {
      const link = e.popup.getElement().querySelector(".map-detail-link");
      if (link) {
        link.addEventListener("click", (ev) => {
          ev.preventDefault();
          _leafletMap.closePopup();
          showFileDetail(link.dataset.path);
        });
      }
    });

    if (bounds.length) {
      _leafletMap.fitBounds(bounds, { padding: [20, 20] });
    }

    setTimeout(() => { if (_leafletMap) _leafletMap.invalidateSize(); }, 100);
  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("map.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
