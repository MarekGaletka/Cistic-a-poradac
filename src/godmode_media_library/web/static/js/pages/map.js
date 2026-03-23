/* GOD MODE Media Library — Map page (Apple Photos style with thumbnail markers) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";

let _leafletMap = null;
let _clusterGroup = null;

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v"]);

export function cleanup() {
  if (_leafletMap) {
    _leafletMap.remove();
    _leafletMap = null;
  }
  _clusterGroup = null;
}

function _thumbIcon(f) {
  const isImage = IMAGE_EXTS.has((f.ext || "").toLowerCase());
  const isVideo = VIDEO_EXTS.has((f.ext || "").toLowerCase());
  if (isImage) {
    const url = `/api/thumbnail${encodeURI(f.path)}?size=80`;
    return L.divIcon({
      html: `<div class="map-thumb-marker"><img src="${url}" onerror="this.parentElement.innerHTML='&#128247;'"></div>`,
      className: "map-thumb-icon",
      iconSize: [48, 48],
      iconAnchor: [24, 24],
    });
  }
  if (isVideo) {
    return L.divIcon({
      html: `<div class="map-thumb-marker map-thumb-video">▶</div>`,
      className: "map-thumb-icon",
      iconSize: [48, 48],
      iconAnchor: [24, 24],
    });
  }
  return L.divIcon({
    html: `<div class="map-thumb-marker map-thumb-file">${escapeHtml(f.ext || "?")}</div>`,
    className: "map-thumb-icon",
    iconSize: [48, 48],
    iconAnchor: [24, 24],
  });
}

function _clusterIcon(cluster) {
  const children = cluster.getAllChildMarkers();
  const count = children.length;

  // Find first image child for a preview thumbnail
  let thumbHtml = "";
  for (const m of children) {
    const f = m._gmlFile;
    if (f && IMAGE_EXTS.has((f.ext || "").toLowerCase())) {
      const url = `/api/thumbnail${encodeURI(f.path)}?size=80`;
      thumbHtml = `<img src="${url}" onerror="this.style.display='none'">`;
      break;
    }
  }

  const size = count >= 100 ? 64 : count >= 20 ? 56 : 48;
  return L.divIcon({
    html: `<div class="map-cluster-thumb" style="width:${size}px;height:${size}px">
      ${thumbHtml}
      <span class="map-cluster-count">${count}</span>
    </div>`,
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
    const data = await api("/files?has_gps=true&limit=10000");
    const files = data.files.filter(f => f.gps_latitude && f.gps_longitude);

    if (!files.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("map.title")}</h2></div>
        <div class="empty-state-hero" style="padding:40px 0">
          <div class="empty-state-icon">&#127758;</div>
          <h3 class="empty-state-title">${t("map.empty_title")}</h3>
          <p class="empty-state-subtitle">${t("map.empty_hint")}</p>
        </div>`;
      return;
    }

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

    // Collect all lightbox-eligible paths for navigation
    const lightboxPaths = files.filter(f => {
      const ext = (f.ext || "").toLowerCase();
      return IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
    }).map(f => f.path);

    const useCluster = typeof L.markerClusterGroup === "function";
    if (useCluster) {
      _clusterGroup = L.markerClusterGroup({
        iconCreateFunction: _clusterIcon,
        maxClusterRadius: 50,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: true,
        disableClusteringAtZoom: 18,
      });
    }

    const bounds = [];
    for (const f of files) {
      const lat = f.gps_latitude;
      const lng = f.gps_longitude;
      bounds.push([lat, lng]);

      const icon = _thumbIcon(f);
      const marker = L.marker([lat, lng], { icon });
      marker._gmlFile = f; // store reference for cluster icon

      // Click marker → open lightbox directly (like Apple Photos)
      marker.on("click", () => {
        const ext = (f.ext || "").toLowerCase();
        const isMedia = IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext);
        if (isMedia) {
          const idx = lightboxPaths.indexOf(f.path);
          if (idx >= 0) {
            openLightbox(lightboxPaths, idx);
          } else {
            showFileDetail(f.path);
          }
        } else {
          showFileDetail(f.path);
        }
      });

      if (useCluster) {
        _clusterGroup.addLayer(marker);
      } else {
        marker.addTo(_leafletMap);
      }
    }

    if (useCluster) {
      _leafletMap.addLayer(_clusterGroup);
    }

    if (bounds.length) {
      _leafletMap.fitBounds(bounds, { padding: [30, 30] });
    }

    setTimeout(() => { if (_leafletMap) _leafletMap.invalidateSize(); }, 100);
  } catch (e) {
    container.innerHTML = `<div class="page-header"><h2>${t("map.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
  }
}
