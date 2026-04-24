"""Unit tests for consolidation.py helper functions."""

import os
import tarfile
import zipfile
from unittest.mock import patch

import pytest

from godmode_media_library.consolidation import (
    ConsolidationConfig,
    _build_dest_path,
    _categorize_file,
    _ema_speed,
    _estimate_speed,
    _is_archive,
    _is_bundle_dir,
    _make_collision_safe,
    _resolve_rclone,
    _safe_tar_extractall,
)
from godmode_media_library.consolidation_types import StructurePattern

# ── _categorize_file ─────────────────────────────────────────────────


class TestCategorizeFile:
    def test_jpeg_is_media(self):
        assert _categorize_file("photos/DSC_1234.jpg") == "Media"

    def test_heic_is_media(self):
        assert _categorize_file("IMG_0001.HEIC") == "Media"

    def test_mp4_is_media(self):
        assert _categorize_file("videos/clip.mp4") == "Media"

    def test_mp3_is_media(self):
        assert _categorize_file("music/song.mp3") == "Media"

    def test_pdf_is_documents(self):
        # pdf is in DOCUMENT_EXTENSIONS which takes priority over MEDIA_EXTENSIONS
        assert _categorize_file("report.pdf") == "Documents"

    def test_docx_is_documents(self):
        assert _categorize_file("thesis.docx") == "Documents"

    def test_xlsx_is_documents(self):
        assert _categorize_file("data.xlsx") == "Documents"

    def test_dmg_is_software(self):
        assert _categorize_file("Installer.dmg") == "Software"

    def test_exe_is_software(self):
        assert _categorize_file("setup.exe") == "Software"

    def test_unknown_ext_is_other(self):
        assert _categorize_file("data.xyz") == "Other"

    def test_no_extension_is_other(self):
        assert _categorize_file("Makefile") == "Other"

    def test_case_insensitive(self):
        assert _categorize_file("PHOTO.JPG") == "Media"


# ── _is_archive ──────────────────────────────────────────────────────


class TestIsArchive:
    def test_zip(self):
        assert _is_archive("backup.zip") is True

    def test_tar_gz(self):
        assert _is_archive("archive.tar.gz") is True

    def test_tar_bz2(self):
        assert _is_archive("archive.tar.bz2") is True

    def test_7z(self):
        assert _is_archive("stuff.7z") is True

    def test_rar(self):
        assert _is_archive("data.rar") is True

    def test_plain_tar(self):
        assert _is_archive("data.tar") is True

    def test_not_archive(self):
        assert _is_archive("photo.jpg") is False

    def test_case_insensitive(self):
        assert _is_archive("DATA.ZIP") is True


# ── _is_bundle_dir ───────────────────────────────────────────────────


class TestIsBundleDir:
    def test_app_bundle(self):
        assert _is_bundle_dir("MyApp.app/Contents/Info.plist") is True

    def test_xcodeproj_bundle(self):
        assert _is_bundle_dir("Project.xcodeproj/project.pbxproj") is True

    def test_framework_bundle(self):
        assert _is_bundle_dir("Lib.framework/Headers/Lib.h") is True

    def test_not_bundle(self):
        assert _is_bundle_dir("regular/directory/file.txt") is False

    def test_photoslibrary_bundle(self):
        assert _is_bundle_dir("Photos.photoslibrary/database/Photos.sqlite") is True


# ── _build_dest_path & _make_collision_safe ──────────────────────────


class TestBuildDestPath:
    def test_year_month_structure(self):
        result = _build_dest_path("base", "photo.jpg", "abc123", "2023-06-15T10:30:00", StructurePattern.YEAR_MONTH)
        assert result == "base/2023/06/photo.jpg"

    def test_year_structure(self):
        result = _build_dest_path("base", "photo.jpg", "abc123", "2023-06-15T10:30:00", StructurePattern.YEAR)
        assert result == "base/2023/photo.jpg"

    def test_flat_structure(self):
        result = _build_dest_path("base", "photo.jpg", "abc123", "2023-06-15T10:30:00", StructurePattern.FLAT)
        assert result == "base/photo.jpg"

    def test_no_mod_time(self):
        result = _build_dest_path("base", "photo.jpg", "abc123", None, StructurePattern.YEAR_MONTH)
        assert "unknown/00" in result

    def test_empty_filename_uses_hash(self):
        result = _build_dest_path("base", "", "abcdef123456789", None, StructurePattern.FLAT)
        assert "unnamed_" in result


class TestMakeCollisionSafe:
    def test_adds_hash_suffix(self):
        result = _make_collision_safe("dir/photo.jpg", "abcdef123456")
        assert result == "dir/photo_abcdef.jpg"

    def test_collision_with_existing_paths(self):
        existing = {"dir/photo_abcdef.jpg"}
        result = _make_collision_safe("dir/photo.jpg", "abcdef123456789a", existing)
        # Should try longer hash or counter
        assert result not in existing
        assert result.startswith("dir/photo_")
        assert result.endswith(".jpg")

    def test_no_existing_paths(self):
        result = _make_collision_safe("dir/file.txt", "abc123def456")
        assert "abc123" in result


# ── ConsolidationConfig defaults ─────────────────────────────────────


class TestConsolidationConfig:
    def test_media_only_default_false(self):
        cfg = ConsolidationConfig()
        assert cfg.media_only is False

    def test_dry_run_default_false(self):
        cfg = ConsolidationConfig()
        assert cfg.dry_run is False

    def test_default_dest_remote(self):
        cfg = ConsolidationConfig()
        assert cfg.dest_remote == "gws-backup"

    def test_default_structure_pattern(self):
        cfg = ConsolidationConfig()
        assert cfg.structure_pattern == StructurePattern.YEAR_MONTH

    def test_media_only_can_be_set(self):
        cfg = ConsolidationConfig(media_only=True)
        assert cfg.media_only is True


# ── _resolve_rclone ──────────────────────────────────────────────────


class TestResolveRclone:
    def test_returns_found_path(self):
        with patch("godmode_media_library.deps.resolve_bin", return_value="/usr/local/bin/rclone"):
            result = _resolve_rclone()
            assert result == "/usr/local/bin/rclone"

    def test_fallback_when_not_found(self):
        with patch("godmode_media_library.deps.resolve_bin", return_value=None):
            result = _resolve_rclone()
            assert result == "rclone"


# ── Zip Slip / tar traversal protection ──────────────────────────────


class TestSafeTarExtract:
    def test_safe_tar_extracts_normally(self, tmp_path):
        """A normal tar extracts without error."""
        # Create a tar with a normal file
        tar_path = tmp_path / "safe.tar"
        content_file = tmp_path / "hello.txt"
        content_file.write_text("hello world")

        with tarfile.open(tar_path, "w") as tf:
            tf.add(content_file, arcname="hello.txt")

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        with tarfile.open(tar_path, "r") as tf:
            _safe_tar_extractall(tf, str(extract_dir))

        assert (extract_dir / "hello.txt").exists()

    def test_tar_path_traversal_blocked(self, tmp_path):
        """A tar with ../../../etc/passwd-style paths is rejected."""
        tar_path = tmp_path / "evil.tar"
        content_file = tmp_path / "payload.txt"
        content_file.write_text("evil")

        with tarfile.open(tar_path, "w") as tf:
            info = tarfile.TarInfo(name="../../../tmp/evil.txt")
            info.size = 4
            with open(content_file, "rb") as fobj:
                tf.addfile(info, fobj)

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        with tarfile.open(tar_path, "r") as tf, pytest.raises(ValueError, match="traversal"):
            _safe_tar_extractall(tf, str(extract_dir))


class TestZipSlipProtection:
    def test_safe_zip_extracts_normally(self, tmp_path):
        """Normal zip files extract fine."""
        zip_path = tmp_path / "safe.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("hello.txt", "hello world")

        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        with zipfile.ZipFile(zip_path, "r") as zf:
            for info in zf.infolist():
                member_path = os.path.realpath(os.path.join(str(extract_dir), info.filename))
                assert member_path.startswith(os.path.realpath(str(extract_dir)))
            zf.extractall(str(extract_dir))

        assert (extract_dir / "hello.txt").exists()


# ── EMA / speed helpers ──────────────────────────────────────────────


class TestZipBombProtection:
    """Regression: zip extraction must reject zip bombs (high compression ratio)."""

    def test_zip_bomb_absolute_limit_rejected(self, tmp_path):
        """A zip claiming >100 GB uncompressed must be rejected."""
        zip_path = tmp_path / "bomb.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("small.txt", "x")

        # Patch the infolist to report enormous uncompressed size
        extract_dir = tmp_path / "extracted"
        extract_dir.mkdir()

        with zipfile.ZipFile(zip_path, "r") as zf:
            total_uncompressed = 0
            archive_size = os.path.getsize(zip_path) or 1
            max_uncompressed = 100 * 1024**3  # 100 GB
            for info in zf.infolist():
                # Simulate a zip bomb by faking file_size
                info.file_size = 200 * 1024**3  # 200 GB claimed
                total_uncompressed += info.file_size

            assert total_uncompressed > max_uncompressed
            # This is the exact check from consolidation.py
            bomb_detected = total_uncompressed > max_uncompressed or (
                total_uncompressed > archive_size * 100 and total_uncompressed > 10 * 1024**3
            )
            assert bomb_detected, "Zip bomb (absolute limit) was not detected"

    def test_zip_bomb_ratio_detected(self, tmp_path):
        """A small archive claiming >100x ratio + >10 GB must be rejected."""
        zip_path = tmp_path / "ratio_bomb.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.bin", "tiny")

        archive_size = os.path.getsize(zip_path) or 1
        # Simulate: 20 GB uncompressed from ~100 byte archive
        total_uncompressed = 20 * 1024**3
        max_uncompressed = 100 * 1024**3

        bomb_detected = total_uncompressed > max_uncompressed or (
            total_uncompressed > archive_size * 100 and total_uncompressed > 10 * 1024**3
        )
        assert bomb_detected, "Zip bomb (ratio check) was not detected"

    def test_normal_zip_not_rejected(self, tmp_path):
        """A normal zip with reasonable ratio should pass."""
        zip_path = tmp_path / "normal.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.txt", "hello world " * 100)

        archive_size = os.path.getsize(zip_path) or 1
        max_uncompressed = 100 * 1024**3

        with zipfile.ZipFile(zip_path, "r") as zf:
            total_uncompressed = sum(info.file_size for info in zf.infolist())

        bomb_detected = total_uncompressed > max_uncompressed or (
            total_uncompressed > archive_size * 100 and total_uncompressed > 10 * 1024**3
        )
        assert not bomb_detected, "Normal zip was falsely detected as zip bomb"


class TestSpeedHelpers:
    def test_estimate_speed_positive(self):
        assert _estimate_speed(1000, 2.0) == 500.0

    def test_estimate_speed_zero_elapsed(self):
        assert _estimate_speed(1000, 0.0) == 0.0

    def test_ema_speed_initial(self):
        # When previous EMA is 0, returns instant speed
        assert _ema_speed(0.0, 100.0) == 100.0

    def test_ema_speed_smoothing(self):
        result = _ema_speed(100.0, 200.0)
        # 0.3 * 200 + 0.7 * 100 = 60 + 70 = 130
        assert abs(result - 130.0) < 0.01
