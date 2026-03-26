"""Tests for catalog verification."""

from __future__ import annotations

from pathlib import Path

from godmode_media_library.catalog import Catalog
from godmode_media_library.verify import verify_catalog


def _make_row(**overrides):
    from tests.test_catalog import _make_row as _base

    return _base(**overrides)


def test_verify_all_ok(tmp_path: Path) -> None:
    """All files exist and match."""
    media = tmp_path / "media"
    media.mkdir()
    (media / "a.jpg").write_bytes(b"content_a")
    (media / "b.png").write_bytes(b"content_b")

    with Catalog(tmp_path / "catalog.db") as cat:
        cat.upsert_file(
            _make_row(
                path=str(media / "a.jpg"),
                size=len(b"content_a"),
            )
        )
        cat.upsert_file(
            _make_row(
                path=str(media / "b.png"),
                size=len(b"content_b"),
                ext="png",
            )
        )
        cat.commit()

        result = verify_catalog(cat)

    assert result.total_checked == 2
    assert result.ok == 2
    assert not result.has_issues


def test_verify_missing_file(tmp_path: Path) -> None:
    """Detects files in catalog but not on disk."""
    with Catalog(tmp_path / "catalog.db") as cat:
        cat.upsert_file(_make_row(path="/nonexistent/photo.jpg"))
        cat.commit()

        result = verify_catalog(cat)

    assert result.total_checked == 1
    assert len(result.missing_files) == 1
    assert result.has_issues


def test_verify_size_mismatch(tmp_path: Path) -> None:
    """Detects files with wrong size."""
    f = tmp_path / "photo.jpg"
    f.write_bytes(b"actual_content")

    with Catalog(tmp_path / "catalog.db") as cat:
        cat.upsert_file(
            _make_row(
                path=str(f),
                size=999,  # wrong size
            )
        )
        cat.commit()

        result = verify_catalog(cat)

    assert result.total_checked == 1
    assert len(result.size_mismatches) == 1
    assert result.size_mismatches[0][1] == 999  # catalog size
    assert result.has_issues


def test_verify_with_limit(tmp_path: Path) -> None:
    """Limit restricts number of files checked."""
    media = tmp_path / "media"
    media.mkdir()
    for i in range(5):
        (media / f"f{i}.jpg").write_bytes(b"x")

    with Catalog(tmp_path / "catalog.db") as cat:
        for i in range(5):
            cat.upsert_file(
                _make_row(
                    path=str(media / f"f{i}.jpg"),
                    size=1,
                )
            )
        cat.commit()

        result = verify_catalog(cat, limit=2)

    assert result.total_checked == 2


def test_verify_progress_callback(tmp_path: Path) -> None:
    """Progress callback is called during verification."""
    f = tmp_path / "photo.jpg"
    f.write_bytes(b"content")

    progress_calls = []

    with Catalog(tmp_path / "catalog.db") as cat:
        cat.upsert_file(_make_row(path=str(f), size=len(b"content")))
        cat.commit()

        verify_catalog(cat, progress_callback=lambda p: progress_calls.append(p))

    assert len(progress_calls) > 0
    assert progress_calls[0]["phase"] == "verify"
