from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from ..shared import CreateTagRequest, TagFilesRequest, _open_catalog, _return_catalog, _sanitize_path

router = APIRouter()

# ── Tags ──────────────────────────────────────────────────────────────


@router.get("/tags")
def list_tags(request: Request) -> dict:
    """List all tags with file counts."""
    cat = _open_catalog(request)
    try:
        tags = cat.get_all_tags()
        return {"tags": tags}
    finally:
        _return_catalog(cat)


@router.post("/tags")
def create_tag(request: Request, body: CreateTagRequest) -> dict:
    """Create a new tag."""
    cat = _open_catalog(request)
    try:
        tag = cat.add_tag(body.name, body.color)
        return tag
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=409, detail="Tag name already exists") from e
        raise
    finally:
        _return_catalog(cat)


@router.delete("/tags/{tag_id}")
def delete_tag(request: Request, tag_id: int) -> dict:
    """Delete a tag."""
    cat = _open_catalog(request)
    try:
        cat.delete_tag(tag_id)
        return {"deleted": True}
    finally:
        _return_catalog(cat)


@router.post("/files/tag")
def tag_files(request: Request, body: TagFilesRequest) -> dict:
    """Add a tag to files."""
    sanitized_paths = []
    for p in body.paths:
        try:
            sanitized_paths.append(_sanitize_path(p, param_name="file path"))
        except HTTPException:
            continue
    cat = _open_catalog(request)
    try:
        count = cat.bulk_tag(sanitized_paths, body.tag_id)
        return {"tagged": count}
    finally:
        _return_catalog(cat)


@router.delete("/files/tag")
def untag_files(request: Request, body: TagFilesRequest) -> dict:
    """Remove a tag from files."""
    sanitized_paths = []
    for p in body.paths:
        try:
            sanitized_paths.append(_sanitize_path(p, param_name="file path"))
        except HTTPException:
            continue
    cat = _open_catalog(request)
    try:
        count = cat.bulk_untag(sanitized_paths, body.tag_id)
        return {"untagged": count}
    finally:
        _return_catalog(cat)


# ── Tag suggestions ────────────────────────────────────────────────


@router.get("/tags/suggest")
def suggest_tags(request: Request, path: str = Query(...)) -> dict:
    """Suggest tags based on file metadata."""
    cat = _open_catalog(request)
    try:
        file_row = cat.get_file_by_path(path)
        if file_row is None:
            raise HTTPException(status_code=404, detail="File not found")

        suggestions: list[dict] = []
        ext = (file_row.ext or "").lower()

        # Camera model
        if file_row.camera_model:
            cam = file_row.camera_model.strip()
            if file_row.camera_make:
                cam = f"{file_row.camera_make.strip()} {cam}"
            suggestions.append({"name": cam, "color": "#58a6ff", "reason": "Fotoaparat"})

        # Year from date_original
        if file_row.date_original:
            year = file_row.date_original[:4]
            if year.isdigit() and 1900 <= int(year) <= 2100:
                suggestions.append({"name": year, "color": "#d29922", "reason": "Rok porizeni"})

        # File type category
        image_exts = {"jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp", "heic", "heif", "svg", "raw", "cr2", "nef", "arw", "dng"}
        video_exts = {"mp4", "mov", "avi", "mkv", "wmv", "flv", "webm", "m4v", "3gp"}
        doc_exts = {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp", "rtf", "epub"}
        if ext in image_exts:
            suggestions.append({"name": "Fotky", "color": "#3fb950", "reason": "Typ souboru"})
        elif ext in video_exts:
            suggestions.append({"name": "Videa", "color": "#f0883e", "reason": "Typ souboru"})
        elif ext in doc_exts:
            suggestions.append({"name": "Dokumenty", "color": "#bc8cff", "reason": "Typ souboru"})

        # GPS
        if file_row.gps_latitude and file_row.gps_longitude:
            suggestions.append({"name": "S GPS", "color": "#58a6ff", "reason": "GPS souradnice"})

        # Check for faces
        faces = cat.get_faces_for_file_by_path(path) if hasattr(cat, "get_faces_for_file_by_path") else []
        if not faces:
            # Try alternative method
            fr = cat.get_file_by_path(path)
            if fr:
                faces_cur = cat.conn.execute("SELECT COUNT(*) FROM faces WHERE file_id = ?", (fr.id,))
                face_count = faces_cur.fetchone()[0]
                if face_count > 0:
                    suggestions.append({"name": "Portrety", "color": "#f85149", "reason": "Detekce obliceju"})

        # Rating
        rating_row = cat.conn.execute(
            "SELECT fr.rating FROM file_ratings fr JOIN files f ON fr.file_id = f.id WHERE f.path = ?",
            (path,),
        ).fetchone()
        if rating_row and rating_row[0] >= 4:
            suggestions.append({"name": "Oblibene", "color": "#d29922", "reason": "Vysoke hodnoceni"})

        # Filter out tags that already exist on the file
        existing_tags = cat.get_file_tags(path)
        existing_names = {t["name"].lower() for t in existing_tags}
        suggestions = [s for s in suggestions if s["name"].lower() not in existing_names]

        return {"suggestions": suggestions}
    finally:
        _return_catalog(cat)


# ── Helpers ───────────────────────────────────────────────────────────


_TEXT_EXTS = {
    ".txt",
    ".md",
    ".csv",
    ".json",
    ".xml",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".log",
    ".sh",
    ".bash",
    ".py",
    ".js",
    ".ts",
    ".html",
    ".css",
    ".sql",
    ".r",
    ".swift",
    ".go",
    ".rs",
    ".java",
    ".kt",
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".rb",
    ".php",
    ".pl",
    ".lua",
    ".vim",
    ".env",
    ".gitignore",
    ".dockerfile",
    ".makefile",
}

_TEXT_NAMES = {"makefile", "dockerfile", "readme", "license", "changelog"}


def _detect_language(ext: str) -> str:
    lang_map = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".html": "html",
        ".css": "css",
        ".json": "json",
        ".xml": "xml",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".sql": "sql",
        ".sh": "bash",
        ".bash": "bash",
        ".md": "markdown",
        ".csv": "csv",
        ".go": "go",
        ".rs": "rust",
        ".java": "java",
        ".c": "c",
        ".cpp": "cpp",
        ".rb": "ruby",
        ".php": "php",
        ".swift": "swift",
    }
    return lang_map.get(ext, "text")
