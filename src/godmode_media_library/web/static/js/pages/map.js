/* GOD MODE Media Library — Map page (3D Globe with thumbnail markers) */

import { api } from "../api.js";
import { escapeHtml, fileName, IMAGE_EXTS } from "../utils.js";
import { t } from "../i18n.js";
import { showFileDetail } from "../modal.js";
import { openLightbox } from "../lightbox.js";

let _map = null;
let _cancelled = false;
let _markers = {};
let _allFiles = [];
let _lightboxPaths = [];
let _rafId = 0;

const VIDEO_EXTS = new Set([
  "mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v",
]);
const PAGE_SIZE = 10000;
const STYLE_URL = "https://tiles.openfreemap.org/styles/liberty";

export function cleanup() {
  _cancelled = true;
  if (_rafId) {
    cancelAnimationFrame(_rafId);
    _rafId = 0;
  }
  for (const m of Object.values(_markers)) m.remove();
  _markers = {};
  if (_map) {
    _map.remove();
    _map = null;
  }
  _allFiles = [];
  _lightboxPaths = [];
}

/* ── Data loading ────────────────────────────────── */

async function _fetchAllGpsFiles() {
  const all = [];
  let offset = 0;
  let hasMore = true;
  while (hasMore && !_cancelled) {
    const d = await api(
      `/files?has_gps=true&limit=${PAGE_SIZE}&offset=${offset}`,
    );
    all.push(
      ...(d.files || []).filter((f) => f.gps_latitude && f.gps_longitude),
    );
    hasMore = d.has_more === true;
    offset += PAGE_SIZE;
  }
  return all;
}

/* ── Marker DOM elements ─────────────────────────── */

function _pointEl(f) {
  const el = document.createElement("div");
  const ext = (f.ext || "").toLowerCase();
  const isImg = IMAGE_EXTS.has(ext);
  const isVid = VIDEO_EXTS.has(ext);

  el.className = "map-thumb-marker";
  if (isImg) {
    const img = document.createElement("img");
    img.src = `/api/thumbnail${encodeURI(f.path)}?size=80`;
    img.onerror = () => {
      el.innerHTML = "&#128247;";
    };
    el.appendChild(img);
  } else if (isVid) {
    el.classList.add("map-thumb-video");
    el.textContent = "\u25B6";
  } else {
    el.classList.add("map-thumb-file");
    el.textContent = (f.ext || "?").toUpperCase();
  }

  el.addEventListener("click", (e) => {
    e.stopPropagation();
    if (isImg || isVid) {
      const i = _lightboxPaths.indexOf(f.path);
      i >= 0 ? openLightbox(_lightboxPaths, i) : showFileDetail(f.path);
    } else {
      showFileDetail(f.path);
    }
  });

  return el;
}

function _clusterEl(count, clusterId, lngLat) {
  const sz = count >= 100 ? 64 : count >= 20 ? 56 : 48;
  const el = document.createElement("div");
  el.className = "map-cluster-thumb";
  el.style.cssText = `width:${sz}px;height:${sz}px`;

  const badge = document.createElement("span");
  badge.className = "map-cluster-count";
  badge.textContent =
    count >= 1000 ? Math.round(count / 1000) + "k" : String(count);
  el.appendChild(badge);

  /* thumbnail from first image in cluster */
  const src = _map?.getSource("files");
  if (src) {
    src
      .getClusterLeaves(clusterId, 20, 0)
      .then((leaves) => {
        for (const lf of leaves) {
          const f = _allFiles[lf.properties.idx];
          if (f && IMAGE_EXTS.has((f.ext || "").toLowerCase())) {
            const img = document.createElement("img");
            img.src = `/api/thumbnail${encodeURI(f.path)}?size=80`;
            img.onerror = () => img.remove();
            el.insertBefore(img, badge);
            break;
          }
        }
      })
      .catch(() => {});
  }

  el.addEventListener("click", (e) => {
    e.stopPropagation();
    const s = _map?.getSource("files");
    if (s) {
      s.getClusterExpansionZoom(clusterId)
        .then((z) => _map?.easeTo({ center: lngLat, zoom: Math.min(z, 18) }))
        .catch(() => {});
    }
  });

  return el;
}

/* ── Sync HTML markers with clustered source ─────── */

function _scheduleSync() {
  if (_rafId) return;
  _rafId = requestAnimationFrame(() => {
    _rafId = 0;
    _syncMarkers();
  });
}

function _syncMarkers() {
  if (!_map || _cancelled) return;
  try {
    if (!_map.getSource("files") || !_map.isSourceLoaded("files")) return;
  } catch {
    return;
  }

  const seen = new Set();

  /* clusters */
  for (const ft of _map.queryRenderedFeatures({ layers: ["_cl"] })) {
    const id = "c" + ft.properties.cluster_id;
    if (seen.has(id)) continue;
    seen.add(id);
    if (!_markers[id]) {
      const c = ft.geometry.coordinates;
      _markers[id] = new maplibregl.Marker({
        element: _clusterEl(
          ft.properties.point_count,
          ft.properties.cluster_id,
          c,
        ),
      })
        .setLngLat(c)
        .addTo(_map);
    }
  }

  /* individual points */
  for (const ft of _map.queryRenderedFeatures({ layers: ["_pt"] })) {
    const id = "p" + ft.properties.idx;
    if (seen.has(id)) continue;
    seen.add(id);
    if (!_markers[id]) {
      const f = _allFiles[ft.properties.idx];
      if (!f) continue;
      _markers[id] = new maplibregl.Marker({ element: _pointEl(f) })
        .setLngLat(ft.geometry.coordinates)
        .addTo(_map);
    }
  }

  /* remove markers no longer visible */
  for (const id of Object.keys(_markers)) {
    if (!seen.has(id)) {
      _markers[id].remove();
      delete _markers[id];
    }
  }
}

/* ── Build the globe map ─────────────────────────── */

function _buildMap(containerId, files) {
  _allFiles = files;
  _lightboxPaths = files
    .filter((f) => {
      const e = (f.ext || "").toLowerCase();
      return IMAGE_EXTS.has(e) || VIDEO_EXTS.has(e);
    })
    .map((f) => f.path);

  const geojson = {
    type: "FeatureCollection",
    features: files.map((f, i) => ({
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [f.gps_longitude, f.gps_latitude],
      },
      properties: { idx: i },
    })),
  };

  console.log("[MAP] creating MapLibre map, container:", containerId);

  /* Use inline style with OSM raster tiles as reliable fallback */
  const mapStyle = {
    version: 8,
    projection: { type: "vertical-perspective" },
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        attribution: "&copy; OpenStreetMap contributors",
        maxzoom: 19,
      },
    },
    layers: [
      { id: "osm-tiles", type: "raster", source: "osm" },
    ],
    sky: {
      "atmosphere-blend": [
        "interpolate", ["linear"], ["zoom"],
        0, 1, 5, 1, 7, 0,
      ],
    },
  };

  _map = new maplibregl.Map({
    container: containerId,
    style: mapStyle,
    center: [15.5, 49.8],
    zoom: 1.8,
    attributionControl: false,
  });

  console.log("[MAP] map created, waiting for load...");

  _map.addControl(new maplibregl.NavigationControl(), "top-right");
  _map.addControl(
    new maplibregl.AttributionControl({ compact: true }),
    "bottom-right",
  );

  _map.on("error", (e) => {
    console.error("[MAP] MapLibre error:", e.error?.message || e);
  });

  _map.on("load", () => {
    console.log("[MAP] map loaded, projection:", _map.getProjection());

    /* clustered GeoJSON source */
    _map.addSource("files", {
      type: "geojson",
      data: geojson,
      cluster: true,
      clusterMaxZoom: 16,
      clusterRadius: 50,
    });

    /* invisible helper layers for queryRenderedFeatures */
    _map.addLayer({
      id: "_cl",
      type: "circle",
      source: "files",
      filter: ["has", "point_count"],
      paint: { "circle-radius": 24, "circle-opacity": 0 },
    });
    _map.addLayer({
      id: "_pt",
      type: "circle",
      source: "files",
      filter: ["!", ["has", "point_count"]],
      paint: { "circle-radius": 24, "circle-opacity": 0 },
    });

    /* fit bounds to data */
    if (files.length) {
      const bounds = new maplibregl.LngLatBounds();
      for (const f of files) bounds.extend([f.gps_longitude, f.gps_latitude]);
      _map.fitBounds(bounds, { padding: 50, maxZoom: 15 });
    }

    /* sync markers on view changes */
    _map.on("moveend", _scheduleSync);
    _map.on("sourcedata", (e) => {
      if (e.sourceId === "files") _scheduleSync();
    });
    _scheduleSync();
    console.log("[MAP] setup complete, files:", files.length);
  });
}

/* ── Page entry point ────────────────────────────── */

export async function render(container) {
  _cancelled = false;

  container.innerHTML = `
    <div class="page-header"><h2>${t("map.title")}</h2></div>
    <div class="loading"><div class="spinner"></div>${t("general.loading")}</div>`;

  try {
    const files = await _fetchAllGpsFiles();
    if (_cancelled) return;

    if (typeof maplibregl === "undefined") {
      container.innerHTML = `
        <div class="page-header"><h2>${t("map.title")}</h2></div>
        <div class="empty">${t("map.leaflet_error")}</div>`;
      return;
    }

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

      _map = new maplibregl.Map({
        container: "map-container",
        style: {
          version: 8,
          projection: { type: "vertical-perspective" },
          sources: {
            osm: {
              type: "raster",
              tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
              tileSize: 256, maxzoom: 19,
            },
          },
          layers: [{ id: "osm-tiles", type: "raster", source: "osm" }],
        },
        center: [15.5, 49.8],
        zoom: 7,
        attributionControl: false,
        interactive: false,
      });

      document
        .getElementById("btn-map-pipeline")
        ?.addEventListener("click", () => {
          location.hash = "#settings";
        });
      return;
    }

    container.innerHTML = `
      <div class="page-header">
        <h2>${t("map.title")} <span class="header-count">${t("map.files_on_map", { count: files.length })}</span></h2>
      </div>
      <div id="map-container"></div>`;

    cleanup();
    _cancelled = false;
    _buildMap("map-container", files);
  } catch (e) {
    if (!_cancelled) {
      container.innerHTML = `
        <div class="page-header"><h2>${t("map.title")}</h2></div>
        <div class="empty">${t("general.error", { message: e.message })}</div>`;
    }
  }
}
