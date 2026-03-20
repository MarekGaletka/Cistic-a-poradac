/* GOD MODE Media Library — Map page (upgraded with clustering) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";

let _leafletMap = null;
let _clusterGroup = null;

export function cleanup() {
  if (_leafletMap) {
    _leafletMap.remove();
    _leafletMap = null;
  }
  _clusterGroup = null;
}

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v"]);

function _cameraIcon() {
  return L.divIcon({
    html: '<span style="font-size:18px">&#128247;</span>',
    className: "map-custom-icon",
    iconSize: [28, 28],
    iconAnchor: [14, 14],
  });
}

function _videoIcon() {
  return L.divIcon({
    html: '<span style="font-size:18px">&#127910;</span>',
    className: "map-custom-icon",
    iconSize: [28, 28],
    iconAnchor: [14, 14],
  });
}

function _clusterIcon(cluster) {
  const count = cluster.getChildCount();
  let color = "#3b82f6"; // blue
  let size = 36;
  if (count >= 50) {
    color = "#ef4444"; // red
    size = 46;
  } else if (count >= 10) {
    color = "#eab308"; // yellow
    size = 40;
  }
  return L.divIcon({
    html: `<div style="background:${color};color:#fff;border-radius:50%;width:${size}px;height:${size}px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;box-shadow:0 2px 6px rgba(0,0,0,0.3)">${count}</div>`,
    className: "map-cluster-icon",
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
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
          <div class="empty-state-icon">&#127758;</div>
          <h3 class="empty-state-title">${t("map.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("map.empty_hint")}</p>
          <button class="empty-state-action-btn" id="btn-map-empty-pipeline">${t("map.empty_action")}</button>
        </div>`;
      const pipelineBtn = container.querySelector("#btn-map-empty-pipeline");
      if (pipelineBtn) {
        pipelineBtn.addEventListener("click", () => {
          const settingsBtn = document.querySelector("#btn-settings");
          if (settingsBtn) settingsBtn.click();
        });
      }
      return;
    }

    // Update header with file count
    container.innerHTML = `
      <div class="page-header"><h2>${t("map.title")} <span class="header-count">${t("map.files_on_map", { count: files.length })}</span></h2></div>
      <div id="map-container"></div>`;

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

    // Use marker clustering if available, otherwise fall back to plain markers
    const useCluster = typeof L.markerClusterGroup === "function";
    if (useCluster) {
      _clusterGroup = L.markerClusterGroup({
        iconCreateFunction: _clusterIcon,
        maxClusterRadius: 60,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: true,
      });
    }

    const bounds = [];
    for (const f of files) {
      const lat = f.gps_latitude;
      const lng = f.gps_longitude;
      bounds.push([lat, lng]);

      const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
      const isVideo = VIDEO_EXTS.has((f.ext || "").toLowerCase());
      const thumbUrl = isImage ? `/api/thumbnail${encodeURI(f.path)}?size=150` : "";
      const thumbHtml = isImage ? `<img src="${thumbUrl}" style="width:120px;height:80px;object-fit:cover;border-radius:4px;margin-bottom:4px;display:block" onerror="this.style.display='none'" alt="">` : "";
      const typeLabel = isVideo ? t("map.video") : isImage ? t("map.photo") : (f.ext || "").toUpperCase();

      const popup = `<div style="font-size:12px;max-width:160px">
        ${thumbHtml}
        <div style="display:flex;align-items:center;gap:4px;margin-bottom:2px">
          <span style="font-size:10px;background:var(--surface,#f0f0f0);padding:1px 6px;border-radius:3px">${escapeHtml(typeLabel)}</span>
        </div>
        <strong>${escapeHtml(fileName(f.path))}</strong><br>
        <span style="color:#666">${escapeHtml(f.date_original || "")}</span><br>
        <a href="#" class="map-detail-link" data-path="${escapeHtml(f.path)}">${t("map.details")}</a>
      </div>`;

      const icon = isVideo ? _videoIcon() : _cameraIcon();
      const marker = L.marker([lat, lng], { icon }).bindPopup(popup);

      if (useCluster) {
        _clusterGroup.addLayer(marker);
      } else {
        marker.addTo(_leafletMap);
      }
    }

    if (useCluster) {
      _leafletMap.addLayer(_clusterGroup);
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
