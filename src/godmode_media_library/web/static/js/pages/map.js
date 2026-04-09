/* GOD MODE Media Library — Map page (Apple Photos style with thumbnail markers) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";

let _leafletMap = null;
let _clusterGroup = null;
let _cancelled = false;

const VIDEO_EXTS = new Set(["mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v"]);
const PAGE_SIZE = 10000;

export function cleanup() {
  _cancelled = true;
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

/** Fetch all GPS files via pagination */
async function _fetchAllGpsFiles() {
  const allFiles = [];
  let offset = 0;
  let hasMore = true;

  while (hasMore && !_cancelled) {
    const data = await api(`/files?has_gps=true&limit=${PAGE_SIZE}&offset=${offset}`);
    const batch = (data.files || []).filter(f => f.gps_latitude && f.gps_longitude);
    allFiles.push(...batch);
    hasMore = data.has_more === true;
    offset += PAGE_SIZE;
  }

  return allFiles;
}

function _addMarkersToMap(files) {
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
    marker._gmlFile = f;

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
}

export async function render(container) {
  _cancelled = false;

  container.innerHTML = `
    <div class="page-header"><h2>${t("map.title")}</h2></div>
    <div class="loading"><div class="spinner"></div>${t("general.loading")}</div>`;

  try {
    const files = await _fetchAllGpsFiles();

    if (_cancelled) return;

    if (!files.length) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("map.title")}</h2></div>
        <div class="map-empty-wrapper">
          <div id="map-container" class="map-container-empty"></div>
          <div class="map-empty-overlay">
            <div class="map-empty-card">
              <div class="map-empty-icon">&#127758;</div>
              <h3 class="map-empty-title">${t("map.empty_title")}</h3>
              <p class="map-empty-text">${t("map.empty_gps_message")}</p>
              <button class="btn btn-primary" id="btn-map-pipeline">${t("map.go_to_pipeline")}</button>
            </div>
          </div>
        </div>`;

      cleanup();
      _cancelled = false;
      if (typeof L !== "undefined") {
        _leafletMap = L.map("map-container").setView([49.8, 15.5], 7);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          attribution: "&copy; OpenStreetMap contributors",
          maxZoom: 19,
        }).addTo(_leafletMap);
        setTimeout(() => { if (_leafletMap) _leafletMap.invalidateSize(); }, 100);
      }

      document.getElementById("btn-map-pipeline")?.addEventListener("click", () => {
        location.hash = "#settings";
      });
      return;
    }

    container.innerHTML = `
      <div class="page-header"><h2>${t("map.title")} <span class="header-count">${t("map.files_on_map", { count: files.length })}</span></h2></div>
      <div id="map-container"></div>`;

    cleanup();
    _cancelled = false;

    if (typeof L === "undefined") {
      container.innerHTML = `<div class="page-header"><h2>${t("map.title")}</h2></div><div class="empty">${t("map.leaflet_error")}</div>`;
      return;
    }

    _leafletMap = L.map("map-container").setView([0, 0], 2);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
    }).addTo(_leafletMap);

    _addMarkersToMap(files);

    setTimeout(() => { if (_leafletMap) _leafletMap.invalidateSize(); }, 100);
  } catch (e) {
    if (!_cancelled) {
      container.innerHTML = `<div class="page-header"><h2>${t("map.title")}</h2></div><div class="empty">${t("general.error", { message: e.message })}</div>`;
    }
  }
}
