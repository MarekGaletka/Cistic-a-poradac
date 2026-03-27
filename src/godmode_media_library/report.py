"""Final Report generator — self-contained HTML report for GOD MODE Media Library."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def generate_report(catalog_path: str | Path, output_path: str | Path | None = None) -> str:
    """Generate a comprehensive HTML report. Returns the output file path."""
    from .catalog import Catalog

    cat = Catalog(Path(catalog_path))
    cat.open()
    try:
        data = _collect_data(cat)
    finally:
        cat.close()

    html = _render_html(data)

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(catalog_path).parent / f"godmode_report_{ts}.html"
    else:
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return str(output_path)


def generate_report_html(catalog_path: str | Path) -> str:
    """Generate report and return the HTML string directly (for API use)."""
    from .catalog import Catalog

    cat = Catalog(Path(catalog_path))
    cat.open()
    try:
        data = _collect_data(cat)
    finally:
        cat.close()

    return _render_html(data)


def _collect_data(cat: Any) -> dict:
    """Collect all data needed for the report from the catalog."""
    conn = cat.conn
    data: dict[str, Any] = {}

    # ── Overview ──────────────────────────────────────────────────
    total_files = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    total_size = conn.execute("SELECT COALESCE(SUM(size), 0) FROM files").fetchone()[0]
    min_date = conn.execute("SELECT MIN(date_original) FROM files WHERE date_original IS NOT NULL").fetchone()[0]
    max_date = conn.execute("SELECT MAX(date_original) FROM files WHERE date_original IS NOT NULL").fetchone()[0]
    min_mtime = conn.execute("SELECT MIN(mtime) FROM files").fetchone()[0]
    max_mtime = conn.execute("SELECT MAX(mtime) FROM files").fetchone()[0]
    last_scan = conn.execute("SELECT MAX(finished_at) FROM scans").fetchone()[0]

    # Sources (scan roots)
    sources = []
    for row in conn.execute("SELECT root, COUNT(*) as cnt, MAX(finished_at) as last FROM scans GROUP BY root ORDER BY cnt DESC"):
        sources.append({"path": row[0], "scan_count": row[1], "last_scan": row[2]})

    # Count files per source root
    for src in sources:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM files WHERE path LIKE ?",
            (src["path"] + "%",),
        ).fetchone()[0]
        src["file_count"] = cnt

    data["overview"] = {
        "total_files": total_files,
        "total_size": total_size,
        "date_range_original": (min_date, max_date),
        "date_range_mtime": (min_mtime, max_mtime),
        "last_scan": last_scan,
        "sources": sources,
    }

    # ── Duplicates ────────────────────────────────────────────────
    dup_groups = conn.execute("SELECT COUNT(DISTINCT group_id) FROM duplicates").fetchone()[0]
    dup_files = conn.execute("SELECT COUNT(*) FROM duplicates").fetchone()[0]
    primary_count = conn.execute("SELECT COUNT(*) FROM duplicates WHERE is_primary = 1").fetchone()[0]
    removable = dup_files - primary_count if dup_files > primary_count else 0

    # Space savings: sum sizes of non-primary duplicates
    savings = 0
    if removable > 0:
        row = conn.execute(
            "SELECT COALESCE(SUM(f.size), 0) FROM duplicates d JOIN files f ON d.file_id = f.id WHERE d.is_primary = 0"
        ).fetchone()
        savings = row[0] if row else 0

    data["duplicates"] = {
        "groups": dup_groups,
        "total_files": dup_files,
        "removable": removable,
        "savings_bytes": savings,
        "after_files": total_files - removable,
        "after_size": total_size - savings,
    }

    # ── Metadata ──────────────────────────────────────────────────
    exif_date_count = conn.execute("SELECT COUNT(*) FROM files WHERE date_original IS NOT NULL").fetchone()[0]
    gps_count = conn.execute("SELECT COUNT(*) FROM files WHERE gps_latitude IS NOT NULL").fetchone()[0]
    camera_count = conn.execute("SELECT COUNT(*) FROM files WHERE camera_model IS NOT NULL").fetchone()[0]
    hashed_count = conn.execute("SELECT COUNT(*) FROM files WHERE sha256 IS NOT NULL").fetchone()[0]

    # Top 5 cameras
    top_cameras = []
    for row in conn.execute(
        "SELECT camera_model, COUNT(*) as cnt FROM files WHERE camera_model IS NOT NULL GROUP BY camera_model ORDER BY cnt DESC LIMIT 5"
    ):
        top_cameras.append((row[0], row[1]))

    # Metadata richness average
    richness_row = conn.execute("SELECT AVG(metadata_richness) FROM files WHERE metadata_richness IS NOT NULL").fetchone()
    avg_richness = richness_row[0] if richness_row and richness_row[0] else 0

    data["metadata"] = {
        "exif_date_count": exif_date_count,
        "gps_count": gps_count,
        "camera_count": camera_count,
        "hashed_count": hashed_count,
        "top_cameras": top_cameras,
        "avg_richness": avg_richness,
        "total_files": total_files,
    }

    # ── Time coverage (aggregated in SQL to avoid loading all rows) ──
    date_month_rows = conn.execute(
        "SELECT DISTINCT SUBSTR(date_original, 1, 7) AS ym "
        "FROM files WHERE date_original IS NOT NULL AND LENGTH(date_original) >= 7 "
        "ORDER BY ym"
    ).fetchall()
    dates_parsed = [row[0] for row in date_month_rows if row[0]]

    coverage_data = _compute_coverage(dates_parsed, min_date, max_date)
    data["coverage"] = coverage_data

    # ── Quality (if quality/score data exists) ────────────────────
    quality_data = {}
    try:
        # Check for media_scores table or quality columns
        image_exts = ("jpg", "jpeg", "png", "bmp", "tiff", "tif", "gif", "webp", "heic", "heif", "raw", "cr2", "nef", "arw", "dng")
        video_exts = ("mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v", "3gp")

        img_placeholders = ",".join("?" * len(image_exts))
        vid_placeholders = ",".join("?" * len(video_exts))

        img_count = conn.execute(
            f"SELECT COUNT(*) FROM files WHERE LOWER(ext) IN ({img_placeholders})",  # noqa: S608
            image_exts,
        ).fetchone()[0]
        vid_count = conn.execute(
            f"SELECT COUNT(*) FROM files WHERE LOWER(ext) IN ({vid_placeholders})",  # noqa: S608
            video_exts,
        ).fetchone()[0]

        # Screenshots: PNG files with typical screenshot dimensions
        screenshot_count = conn.execute(
            "SELECT COUNT(*) FROM files WHERE LOWER(ext) = 'png' "
            "AND width IS NOT NULL AND height IS NOT NULL "
            "AND ((width >= 1920 AND height >= 1080) OR (width >= 750 AND height >= 1334))"
        ).fetchone()[0]

        quality_data = {
            "photos": img_count,
            "videos": vid_count,
            "screenshots": screenshot_count,
            "other": total_files - img_count - vid_count,
        }

        # Blurry detection (if width/height available but very small resolution)
        # Use metadata_richness as a proxy for quality
        low_richness = conn.execute(
            "SELECT COUNT(*) FROM files WHERE metadata_richness IS NOT NULL AND metadata_richness < 0.2"
        ).fetchone()[0]
        quality_data["low_quality"] = low_richness

    except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
        logger.debug("Quality data query failed (table/column may not exist): %s", exc)

    data["quality"] = quality_data

    # ── Faces ─────────────────────────────────────────────────────
    faces_data = {}
    try:
        total_faces = conn.execute("SELECT COUNT(*) FROM faces").fetchone()[0]
        total_persons = conn.execute("SELECT COUNT(*) FROM persons WHERE name != ''").fetchone()[0]
        faces_data = {"total_faces": total_faces, "total_persons": total_persons}
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
        logger.debug("Faces data query failed (table may not exist): %s", exc)
    data["faces"] = faces_data

    # ── Cloud sources ─────────────────────────────────────────────
    cloud_data = []
    try:
        from .cloud import list_remotes

        remotes = list_remotes()
        cloud_data = remotes.get("remotes", [])
    except (ImportError, OSError) as exc:
        logger.debug("Cloud remotes unavailable: %s", exc)
    data["cloud"] = cloud_data

    # ── Recommendations ───────────────────────────────────────────
    data["recommendations"] = _build_recommendations(data)

    return data


def _compute_coverage(months: list[str], min_date: str | None, max_date: str | None) -> dict:
    """Compute month-by-month coverage and detect gaps."""
    if not months or not min_date or not max_date:
        return {"first": None, "last": None, "percentage": 0, "gaps": [], "total_months": 0, "covered_months": 0}

    unique_months = sorted(set(months))
    first = unique_months[0]
    last = unique_months[-1]

    # Generate all months between first and last
    try:
        first_y, first_m = int(first[:4]), int(first[5:7])
        last_y, last_m = int(last[:4]), int(last[5:7])
    except (ValueError, IndexError):
        return {"first": first, "last": last, "percentage": 0, "gaps": [], "total_months": 0, "covered_months": 0}

    all_months = []
    y, m = first_y, first_m
    while (y, m) <= (last_y, last_m):
        all_months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    total_months = len(all_months)
    covered = set(unique_months)
    covered_months = len(covered)
    percentage = round(covered_months / total_months * 100, 1) if total_months > 0 else 0

    # Find gaps (consecutive missing months)
    gaps = []
    gap_start = None
    for month in all_months:
        if month not in covered:
            if gap_start is None:
                gap_start = month
        else:
            if gap_start is not None:
                prev_idx = all_months.index(month) - 1
                gaps.append(f"{gap_start} -- {all_months[prev_idx]}")
                gap_start = None
    if gap_start is not None:
        gaps.append(f"{gap_start} -- {all_months[-1]}")

    return {
        "first": first,
        "last": last,
        "percentage": percentage,
        "gaps": gaps[:20],  # limit
        "total_months": total_months,
        "covered_months": covered_months,
    }


def _build_recommendations(data: dict) -> list[dict]:
    """Build actionable recommendation items."""
    recs = []

    dup = data.get("duplicates", {})
    if dup.get("removable", 0) > 0:
        recs.append(
            {
                "icon": "&#128203;",
                "text": f"{dup['removable']} duplicit k odstraneni",
                "detail": f"Usporite {_fmt_size(dup['savings_bytes'])}",
                "severity": "warning",
            }
        )

    meta = data.get("metadata", {})
    total = meta.get("total_files", 0)
    if total > 0:
        no_date = total - meta.get("exif_date_count", 0)
        if no_date > 0:
            pct = round(no_date / total * 100)
            recs.append(
                {
                    "icon": "&#128197;",
                    "text": f"{no_date} souboru bez data porizeni ({pct}%)",
                    "detail": "Doporuceno doplnit metadata",
                    "severity": "info",
                }
            )

        no_hash = total - meta.get("hashed_count", 0)
        if no_hash > 0:
            recs.append(
                {
                    "icon": "&#128274;",
                    "text": f"{no_hash} souboru bez SHA-256 hashe",
                    "detail": "Spustte pipeline pro doplneni",
                    "severity": "info",
                }
            )

    quality = data.get("quality", {})
    screenshots = quality.get("screenshots", 0)
    if screenshots > 50:
        recs.append(
            {
                "icon": "&#128248;",
                "text": f"{screenshots} screenshotu k provereni",
                "detail": "Zvazit archivaci nebo smazani",
                "severity": "info",
            }
        )

    coverage = data.get("coverage", {})
    gaps = coverage.get("gaps", [])
    if len(gaps) > 3:
        recs.append(
            {
                "icon": "&#128197;",
                "text": f"{len(gaps)} mezer v casovem pokryti",
                "detail": "Chybi data za nektera obdobi",
                "severity": "info",
            }
        )

    if not recs:
        recs.append(
            {
                "icon": "&#9989;",
                "text": "Knihovna je v dobrem stavu",
                "detail": "Zadna nutna akce",
                "severity": "ok",
            }
        )

    return recs


def _fmt_size(b: int | float) -> str:
    """Format bytes to human-readable string."""
    if b < 1024:
        return f"{b} B"
    if b < 1024**2:
        return f"{b / 1024:.1f} KB"
    if b < 1024**3:
        return f"{b / 1024**2:.1f} MB"
    return f"{b / 1024**3:.2f} GB"


def _pct(part: int, total: int) -> str:
    """Return percentage string."""
    if total == 0:
        return "0%"
    return f"{round(part / total * 100)}%"


def _bar_html(label: str, value: int, max_value: int, color: str = "#3b82f6") -> str:
    """Render a CSS bar chart row."""
    pct = round(value / max_value * 100) if max_value > 0 else 0
    return (
        f'<div class="bar-row">'
        f'<span class="bar-label">{label}</span>'
        f'<div class="bar-track"><div class="bar-fill" style="width:{pct}%;background:{color}"></div></div>'
        f'<span class="bar-value">{value:,}</span>'
        f"</div>"
    )


def _render_html(data: dict) -> str:
    """Render the full HTML report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    overview = data["overview"]
    dup = data["duplicates"]
    meta = data["metadata"]
    coverage = data["coverage"]
    quality = data.get("quality", {})
    faces = data.get("faces", {})
    cloud = data.get("cloud", [])
    recs = data.get("recommendations", [])
    total = overview["total_files"]

    # ── Sections ──────────────────────────────────────────────────

    # Overview section
    sources_html = ""
    for src in overview["sources"]:
        sources_html += f"<tr><td>{src['path']}</td><td>{src['file_count']:,}</td><td>{src.get('last_scan', '-')}</td></tr>"

    date_range = ""
    d_min, d_max = overview["date_range_original"]
    if d_min and d_max:
        date_range = f"{d_min[:10]} &mdash; {d_max[:10]}"
    elif overview["date_range_mtime"][0]:
        from datetime import datetime as dt

        try:
            t_min = dt.fromtimestamp(overview["date_range_mtime"][0]).strftime("%Y-%m-%d")
            t_max = dt.fromtimestamp(overview["date_range_mtime"][1]).strftime("%Y-%m-%d")
            date_range = f"{t_min} &mdash; {t_max} (mtime)"
        except (ValueError, TypeError, OSError):
            date_range = "-"

    overview_section = f"""
    <section class="report-section">
      <h2>Prehled</h2>
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-value">{total:,}</div>
          <div class="kpi-label">Celkem souboru</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{_fmt_size(overview["total_size"])}</div>
          <div class="kpi-label">Celkova velikost</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{date_range or "-"}</div>
          <div class="kpi-label">Casovy rozsah</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{overview["last_scan"] or "-"}</div>
          <div class="kpi-label">Posledni sken</div>
        </div>
      </div>
      {
        f'''<h3>Zdroje</h3>
      <table class="data-table">
        <thead><tr><th>Cesta</th><th>Souboru</th><th>Posledni sken</th></tr></thead>
        <tbody>{sources_html}</tbody>
      </table>'''
        if sources_html
        else ""
    }
    </section>
    """

    # Duplicates section
    dup_section = f"""
    <section class="report-section">
      <h2>Duplicity</h2>
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-value">{dup["groups"]:,}</div>
          <div class="kpi-label">Skupin duplicit</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{dup["removable"]:,}</div>
          <div class="kpi-label">Souboru k odstraneni</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{_fmt_size(dup["savings_bytes"])}</div>
          <div class="kpi-label">Mozna uspora</div>
        </div>
      </div>
      <h3>Pred / Po</h3>
      <div class="comparison">
        <div class="comparison-col">
          <div class="comparison-label">Nyni</div>
          <div class="comparison-value">{total:,} souboru</div>
          <div class="comparison-value">{_fmt_size(overview["total_size"])}</div>
        </div>
        <div class="comparison-arrow">&rarr;</div>
        <div class="comparison-col">
          <div class="comparison-label">Po deduplikaci</div>
          <div class="comparison-value">{dup["after_files"]:,} souboru</div>
          <div class="comparison-value">{_fmt_size(dup["after_size"])}</div>
        </div>
      </div>
    </section>
    """

    # Metadata section
    cameras_html = ""
    if meta["top_cameras"]:
        max_cam = meta["top_cameras"][0][1]
        for cam, cnt in meta["top_cameras"]:
            cameras_html += _bar_html(cam, cnt, max_cam, "#8b5cf6")

    meta_section = f"""
    <section class="report-section">
      <h2>Metadata</h2>
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-value">{meta["exif_date_count"]:,} <small>({_pct(meta["exif_date_count"], total)})</small></div>
          <div class="kpi-label">S EXIF datem</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{meta["gps_count"]:,} <small>({_pct(meta["gps_count"], total)})</small></div>
          <div class="kpi-label">S GPS</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{meta["camera_count"]:,} <small>({_pct(meta["camera_count"], total)})</small></div>
          <div class="kpi-label">S info o fotoaparatu</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-value">{round(meta["avg_richness"] * 100) if meta["avg_richness"] else 0}%</div>
          <div class="kpi-label">Prumerna bohatost metadat</div>
        </div>
      </div>
      {f'<h3>Top 5 fotoaparatu</h3><div class="bar-chart">{cameras_html}</div>' if cameras_html else ""}
    </section>
    """

    # Coverage section
    cov_section = ""
    if coverage.get("first"):
        gaps_html = ""
        if coverage["gaps"]:
            gaps_html = "<h3>Mezery</h3><ul class='gap-list'>"
            for g in coverage["gaps"]:
                gaps_html += f"<li>{g}</li>"
            gaps_html += "</ul>"

        cov_section = f"""
        <section class="report-section">
          <h2>Casove pokryti</h2>
          <div class="kpi-grid">
            <div class="kpi-card">
              <div class="kpi-value">{coverage["first"]}</div>
              <div class="kpi-label">Prvni zaznam</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-value">{coverage["last"]}</div>
              <div class="kpi-label">Posledni zaznam</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-value">{coverage["percentage"]}%</div>
              <div class="kpi-label">Pokryti ({coverage["covered_months"]}/{coverage["total_months"]} mesicu)</div>
            </div>
          </div>
          {gaps_html}
        </section>
        """

    # Quality section
    quality_section = ""
    if quality:
        max_q = max(quality.get("photos", 0), quality.get("videos", 0), quality.get("screenshots", 0), quality.get("other", 0), 1)
        bars = ""
        if quality.get("photos", 0):
            bars += _bar_html("Fotografie", quality["photos"], max_q, "#3b82f6")
        if quality.get("videos", 0):
            bars += _bar_html("Videa", quality["videos"], max_q, "#eab308")
        if quality.get("screenshots", 0):
            bars += _bar_html("Screenshoty", quality["screenshots"], max_q, "#ef4444")
        if quality.get("other", 0):
            bars += _bar_html("Ostatni", quality["other"], max_q, "#6b7280")

        extra = ""
        if quality.get("low_quality", 0):
            extra = f'<p class="quality-note">Nizka kvalita metadat: {quality["low_quality"]:,} souboru</p>'

        quality_section = f"""
        <section class="report-section">
          <h2>Kvalita</h2>
          <div class="bar-chart">{bars}</div>
          {extra}
        </section>
        """

    # Faces section
    faces_section = ""
    if faces.get("total_faces", 0) > 0:
        faces_section = f"""
        <section class="report-section">
          <h2>Obliceje</h2>
          <div class="kpi-grid">
            <div class="kpi-card">
              <div class="kpi-value">{faces["total_faces"]:,}</div>
              <div class="kpi-label">Detekovanych obliceju</div>
            </div>
            <div class="kpi-card">
              <div class="kpi-value">{faces["total_persons"]:,}</div>
              <div class="kpi-label">Identifikovanych osob</div>
            </div>
          </div>
        </section>
        """

    # Cloud section
    cloud_section = ""
    if cloud:
        rows = ""
        for remote in cloud:
            name = remote.get("name", remote) if isinstance(remote, dict) else str(remote)
            rtype = remote.get("type", "-") if isinstance(remote, dict) else "-"
            rows += f"<tr><td>{name}</td><td>{rtype}</td></tr>"
        cloud_section = f"""
        <section class="report-section">
          <h2>Cloudove zdroje</h2>
          <table class="data-table">
            <thead><tr><th>Nazev</th><th>Typ</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </section>
        """

    # Recommendations section
    recs_html = ""
    for rec in recs:
        severity_class = f"rec-{rec['severity']}"
        recs_html += f"""
        <div class="rec-card {severity_class}">
          <span class="rec-icon">{rec["icon"]}</span>
          <div class="rec-text">
            <strong>{rec["text"]}</strong>
            <span class="rec-detail">{rec["detail"]}</span>
          </div>
        </div>
        """

    recs_section = f"""
    <section class="report-section">
      <h2>Doporuceni</h2>
      <div class="rec-list">{recs_html}</div>
    </section>
    """

    # ── Full HTML ─────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="cs">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GOD MODE Media Library &mdash; Souhrnny report</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; }}

  :root {{
    --bg: #0d1117;
    --surface: #161b22;
    --surface2: #1c2128;
    --border: #30363d;
    --text: #e6edf3;
    --text-muted: #8b949e;
    --accent: #58a6ff;
    --green: #3fb950;
    --yellow: #d29922;
    --red: #f85149;
    --purple: #8b5cf6;
  }}

  body {{
    margin: 0;
    padding: 0;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    font-size: 14px;
    line-height: 1.5;
  }}

  .report-wrapper {{
    max-width: 900px;
    margin: 0 auto;
    padding: 32px 24px;
  }}

  .report-header {{
    text-align: center;
    margin-bottom: 40px;
    padding-bottom: 24px;
    border-bottom: 1px solid var(--border);
  }}

  .report-header h1 {{
    font-size: 28px;
    font-weight: 700;
    margin: 0 0 8px 0;
    color: var(--accent);
  }}

  .report-header .subtitle {{
    color: var(--text-muted);
    font-size: 13px;
  }}

  .report-section {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 20px;
  }}

  .report-section h2 {{
    font-size: 18px;
    font-weight: 600;
    margin: 0 0 16px 0;
    color: var(--accent);
    border-bottom: 1px solid var(--border);
    padding-bottom: 8px;
  }}

  .report-section h3 {{
    font-size: 14px;
    font-weight: 600;
    margin: 16px 0 8px 0;
    color: var(--text);
  }}

  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
  }}

  .kpi-card {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    text-align: center;
  }}

  .kpi-value {{
    font-size: 22px;
    font-weight: 700;
    color: var(--text);
  }}

  .kpi-value small {{
    font-size: 13px;
    font-weight: 400;
    color: var(--text-muted);
  }}

  .kpi-label {{
    font-size: 12px;
    color: var(--text-muted);
    margin-top: 4px;
  }}

  .data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}

  .data-table th, .data-table td {{
    padding: 8px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border);
  }}

  .data-table th {{
    color: var(--text-muted);
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }}

  .data-table tr:last-child td {{
    border-bottom: none;
  }}

  .bar-chart {{
    display: flex;
    flex-direction: column;
    gap: 6px;
  }}

  .bar-row {{
    display: flex;
    align-items: center;
    gap: 8px;
  }}

  .bar-label {{
    width: 140px;
    font-size: 12px;
    color: var(--text-muted);
    text-align: right;
    flex-shrink: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }}

  .bar-track {{
    flex: 1;
    height: 18px;
    background: var(--surface2);
    border-radius: 4px;
    overflow: hidden;
  }}

  .bar-fill {{
    height: 100%;
    background: var(--accent);
    border-radius: 4px;
    min-width: 2px;
    transition: width 0.3s;
  }}

  .bar-value {{
    width: 60px;
    font-size: 12px;
    color: var(--text-muted);
    text-align: right;
    flex-shrink: 0;
  }}

  .comparison {{
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 24px;
    margin-top: 12px;
  }}

  .comparison-col {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px 24px;
    text-align: center;
    min-width: 180px;
  }}

  .comparison-label {{
    font-size: 11px;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }}

  .comparison-value {{
    font-size: 15px;
    font-weight: 600;
  }}

  .comparison-arrow {{
    font-size: 24px;
    color: var(--green);
  }}

  .gap-list {{
    list-style: none;
    padding: 0;
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }}

  .gap-list li {{
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 4px 10px;
    font-size: 12px;
    color: var(--yellow);
  }}

  .rec-list {{
    display: flex;
    flex-direction: column;
    gap: 8px;
  }}

  .rec-card {{
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 12px 16px;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 8px;
    border-left: 3px solid var(--border);
  }}

  .rec-warning {{
    border-left-color: var(--yellow);
  }}

  .rec-info {{
    border-left-color: var(--accent);
  }}

  .rec-ok {{
    border-left-color: var(--green);
  }}

  .rec-icon {{
    font-size: 20px;
    flex-shrink: 0;
  }}

  .rec-text {{
    display: flex;
    flex-direction: column;
    gap: 2px;
  }}

  .rec-text strong {{
    font-size: 13px;
  }}

  .rec-detail {{
    font-size: 12px;
    color: var(--text-muted);
  }}

  .quality-note {{
    margin-top: 12px;
    font-size: 12px;
    color: var(--yellow);
  }}

  .report-footer {{
    text-align: center;
    padding: 24px 0;
    color: var(--text-muted);
    font-size: 11px;
    border-top: 1px solid var(--border);
    margin-top: 20px;
  }}

  /* Print styles */
  @media print {{
    body {{
      background: #fff;
      color: #000;
    }}
    .report-section {{
      background: #fff;
      border: 1px solid #ddd;
      break-inside: avoid;
    }}
    .kpi-card, .comparison-col, .rec-card {{
      background: #f9f9f9;
      border-color: #ddd;
    }}
    .kpi-value, .report-section h2 {{
      color: #333;
    }}
    .kpi-label, .bar-label, .bar-value, .rec-detail {{
      color: #666;
    }}
    .bar-track {{
      background: #eee;
    }}
    .bar-fill {{
      background: #4a90d9;
    }}
    .report-header h1 {{
      color: #333;
    }}
  }}
</style>
</head>
<body>
<div class="report-wrapper">
  <div class="report-header">
    <h1>GOD MODE Media Library</h1>
    <div class="subtitle">Souhrnny report &mdash; vygenerovano {now}</div>
  </div>

  {overview_section}
  {dup_section}
  {meta_section}
  {cov_section}
  {quality_section}
  {faces_section}
  {cloud_section}
  {recs_section}

  <div class="report-footer">
    GOD MODE Media Library &mdash; Souhrnny report &mdash; {now}
  </div>
</div>
</body>
</html>"""
