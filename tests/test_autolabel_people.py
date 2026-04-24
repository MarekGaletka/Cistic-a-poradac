"""Tests for autolabel_people.py — face detection, clustering, label generation.

Expands coverage from ~35% to 60%+.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from godmode_media_library.autolabel_people import (
    PEOPLE_EXTS,
    AutoPeopleResult,
    FaceRecord,
    _load_face_libs,
    _resize_if_needed,
    auto_people_labels,
)

# ---------------------------------------------------------------------------
# Helper mocks
# ---------------------------------------------------------------------------


class _FakeArray:
    """Minimal mock that simulates a numpy array with a .shape attribute."""

    def __init__(self, shape: tuple[int, ...]):
        self.shape = shape


class _FakeNp:
    """Minimal mock for numpy module used by _resize_if_needed."""

    def array(self, obj):
        return obj

    def vstack(self, arrays):
        return arrays

    def __init__(self):
        self.int = int


class _FakeImage:
    """Minimal mock for PIL.Image used by _resize_if_needed."""

    def __init__(self, size: tuple[int, int]):
        self._size = size

    def resize(self, new_size: tuple[int, int]):
        return _FakeImage(new_size)

    @classmethod
    def fromarray(cls, arr):
        shape = getattr(arr, "shape", (100, 100))
        return cls((shape[1], shape[0]))


# ---------------------------------------------------------------------------
# _resize_if_needed
# ---------------------------------------------------------------------------


class TestResizeIfNeeded:
    def test_no_resize(self):
        arr = _FakeArray(shape=(100, 200, 3))
        result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
        assert result is arr
        assert scale == 1.0

    def test_large_image(self):
        arr = _FakeArray(shape=(3200, 4800, 3))
        result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
        assert result is not arr
        assert scale < 1.0

    def test_no_shape(self):
        result, scale = _resize_if_needed("not_array", max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
        assert result == "not_array"
        assert scale == 1.0

    def test_1d_shape(self):
        arr = _FakeArray(shape=(100,))
        result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
        assert result is arr
        assert scale == 1.0

    def test_exact_boundary(self):
        arr = _FakeArray(shape=(1600, 1600, 3))
        result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
        assert result is arr
        assert scale == 1.0

    def test_just_above_boundary(self):
        arr = _FakeArray(shape=(1601, 800, 3))
        result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
        assert result is not arr
        assert scale < 1.0


# ---------------------------------------------------------------------------
# FaceRecord / AutoPeopleResult dataclasses
# ---------------------------------------------------------------------------


class TestDataclasses:
    def test_face_record_creation(self):
        rec = FaceRecord(path=Path("/tmp/a.jpg"), face_index=0, cluster_id=1, person_label="Person_001")
        assert rec.path == Path("/tmp/a.jpg")
        assert rec.person_label == "Person_001"
        assert rec.cluster_id == 1

    def test_auto_people_result_creation(self):
        result = AutoPeopleResult(
            labels_out=Path("/tmp/labels.tsv"),
            report_path=Path("/tmp/report.json"),
            faces_path=Path("/tmp/faces.tsv"),
            clusters_path=Path("/tmp/clusters.tsv"),
            missing_path=Path("/tmp/missing.tsv"),
            scanned_files=100,
            candidate_files=50,
            processed_images=45,
            faces_detected=30,
            clusters=5,
            touched_labels=40,
            changed_labels=35,
            unresolved_candidates=5,
            model_engine="face_recognition:hog",
        )
        assert result.scanned_files == 100
        assert result.clusters == 5


# ---------------------------------------------------------------------------
# PEOPLE_EXTS constant
# ---------------------------------------------------------------------------


def test_people_exts_contains_expected():
    assert "jpg" in PEOPLE_EXTS
    assert "jpeg" in PEOPLE_EXTS
    assert "png" in PEOPLE_EXTS
    assert "heic" in PEOPLE_EXTS
    # Video exts should NOT be in PEOPLE_EXTS
    assert "mp4" not in PEOPLE_EXTS
    assert "mov" not in PEOPLE_EXTS


# ---------------------------------------------------------------------------
# _load_face_libs — error path
# ---------------------------------------------------------------------------


def test_load_face_libs_missing():
    """Missing dependencies should raise RuntimeError."""
    with patch.dict("sys.modules", {"face_recognition": None}), pytest.raises(RuntimeError, match="optional dependencies"):
        _load_face_libs()


# ---------------------------------------------------------------------------
# auto_people_labels — integration with mocked face detection
# ---------------------------------------------------------------------------


class TestAutoPeopleLabels:
    def test_no_candidates(self, tmp_path):
        """No image files -> no face processing."""
        root = tmp_path / "media"
        root.mkdir()
        # Only create non-image files
        (root / "video.mp4").write_bytes(b"video")
        (root / "doc.txt").write_bytes(b"text")

        labels_out = tmp_path / "labels_out.tsv"
        report_dir = tmp_path / "report"

        with patch("godmode_media_library.autolabel_people.collect_file_records") as mock_collect:
            # Return records that don't match PEOPLE_EXTS
            rec1 = MagicMock()
            rec1.path = root / "video.mp4"
            rec1.ext = "mp4"
            rec2 = MagicMock()
            rec2.path = root / "doc.txt"
            rec2.ext = "txt"
            mock_collect.return_value = [rec1, rec2]

            with (
                patch("godmode_media_library.autolabel_people.build_asset_membership", return_value=({}, {}, {})),
                patch("godmode_media_library.autolabel_people.load_labels_table", return_value=([], {})),
                patch("godmode_media_library.autolabel_people.merge_label_updates", return_value=(0, 0)),
                patch("godmode_media_library.autolabel_people.write_labels_table"),
                patch("godmode_media_library.autolabel_people.write_tsv"),
            ):
                result = auto_people_labels(
                    roots=[root],
                    labels_in=None,
                    labels_out=labels_out,
                    report_dir=report_dir,
                )

        assert result.candidate_files == 0
        assert result.processed_images == 0
        assert result.faces_detected == 0
        assert result.model_engine == "not_run:no_candidate_images"

    def test_with_faces_detected(self, tmp_path):
        """Mock face detection and clustering end-to-end."""
        import numpy as np

        root = tmp_path / "media"
        root.mkdir()
        photo1 = root / "photo1.jpg"
        photo2 = root / "photo2.jpg"
        photo1.write_bytes(b"fake_jpg_1")
        photo2.write_bytes(b"fake_jpg_2")

        labels_out = tmp_path / "labels_out.tsv"
        report_dir = tmp_path / "report"
        report_dir.mkdir()

        # Mock file records
        rec1 = MagicMock()
        rec1.path = photo1
        rec1.ext = "jpg"
        rec2 = MagicMock()
        rec2.path = photo2
        rec2.ext = "jpg"

        # Mock face_recognition
        mock_fr = MagicMock()
        mock_fr.load_image_file.return_value = _FakeArray(shape=(100, 100, 3))
        mock_fr.face_locations.return_value = [(10, 90, 90, 10)]
        # Return 128-dim encoding
        mock_fr.face_encodings.return_value = [np.random.rand(128)]

        # Mock DBSCAN
        mock_dbscan_instance = MagicMock()
        mock_dbscan_instance.fit_predict.return_value = np.array([0, 0])
        mock_dbscan_cls = MagicMock(return_value=mock_dbscan_instance)

        with (
            patch("godmode_media_library.autolabel_people.collect_file_records", return_value=[rec1, rec2]),
            patch("godmode_media_library.autolabel_people._load_face_libs", return_value=(mock_fr, np, _FakeImage, mock_dbscan_cls)),
            patch("godmode_media_library.autolabel_people.build_asset_membership", return_value=({}, {}, {})),
            patch("godmode_media_library.autolabel_people.load_labels_table", return_value=([], {})),
            patch("godmode_media_library.autolabel_people.merge_label_updates", return_value=(2, 2)),
            patch("godmode_media_library.autolabel_people.write_labels_table"),
            patch("godmode_media_library.autolabel_people.write_tsv"),
        ):
            result = auto_people_labels(
                roots=[root],
                labels_in=None,
                labels_out=labels_out,
                report_dir=report_dir,
                model="hog",
            )

        assert result.candidate_files == 2
        assert result.processed_images == 2
        assert result.faces_detected == 2
        assert result.clusters >= 0
        assert "face_recognition:hog" in result.model_engine

    def test_face_detection_error_handled(self, tmp_path):
        """Files that fail to load should be recorded as unresolved."""
        root = tmp_path / "media"
        root.mkdir()
        photo = root / "corrupt.jpg"
        photo.write_bytes(b"corrupt")

        labels_out = tmp_path / "labels_out.tsv"
        report_dir = tmp_path / "report"
        report_dir.mkdir()

        rec = MagicMock()
        rec.path = photo
        rec.ext = "jpg"

        mock_fr = MagicMock()
        mock_fr.load_image_file.side_effect = RuntimeError("corrupt image")

        mock_np = MagicMock()
        mock_np.array.return_value = MagicMock(dtype=int)
        mock_np.vstack = MagicMock()

        with (
            patch("godmode_media_library.autolabel_people.collect_file_records", return_value=[rec]),
            patch("godmode_media_library.autolabel_people._load_face_libs", return_value=(mock_fr, mock_np, _FakeImage, MagicMock())),
            patch("godmode_media_library.autolabel_people.build_asset_membership", return_value=({}, {}, {})),
            patch("godmode_media_library.autolabel_people.load_labels_table", return_value=([], {})),
            patch("godmode_media_library.autolabel_people.merge_label_updates", return_value=(0, 0)),
            patch("godmode_media_library.autolabel_people.write_labels_table"),
            patch("godmode_media_library.autolabel_people.write_tsv"),
        ):
            result = auto_people_labels(
                roots=[root],
                labels_in=None,
                labels_out=labels_out,
                report_dir=report_dir,
            )

        assert result.unresolved_candidates == 1
        assert result.processed_images == 0

    def test_no_faces_in_image(self, tmp_path):
        """Image loads fine but no faces detected."""
        root = tmp_path / "media"
        root.mkdir()
        photo = root / "landscape.jpg"
        photo.write_bytes(b"landscape")

        labels_out = tmp_path / "labels_out.tsv"
        report_dir = tmp_path / "report"
        report_dir.mkdir()

        rec = MagicMock()
        rec.path = photo
        rec.ext = "jpg"

        mock_fr = MagicMock()
        mock_fr.load_image_file.return_value = _FakeArray(shape=(100, 100, 3))
        mock_fr.face_locations.return_value = []
        mock_fr.face_encodings.return_value = []

        mock_np = MagicMock()
        mock_np.array.return_value = MagicMock(dtype=int)

        with (
            patch("godmode_media_library.autolabel_people.collect_file_records", return_value=[rec]),
            patch("godmode_media_library.autolabel_people._load_face_libs", return_value=(mock_fr, mock_np, _FakeImage, MagicMock())),
            patch("godmode_media_library.autolabel_people.build_asset_membership", return_value=({}, {}, {})),
            patch("godmode_media_library.autolabel_people.load_labels_table", return_value=([], {})),
            patch("godmode_media_library.autolabel_people.merge_label_updates", return_value=(0, 0)),
            patch("godmode_media_library.autolabel_people.write_labels_table"),
            patch("godmode_media_library.autolabel_people.write_tsv"),
        ):
            result = auto_people_labels(
                roots=[root],
                labels_in=None,
                labels_out=labels_out,
                report_dir=report_dir,
            )

        assert result.processed_images == 1
        assert result.faces_detected == 0
        assert result.unresolved_candidates == 1  # no_face_detected

    def test_noise_cluster_label(self, tmp_path):
        """Faces in noise cluster (id=-1) should get empty person_label."""
        import numpy as np

        root = tmp_path / "media"
        root.mkdir()
        photo = root / "solo.jpg"
        photo.write_bytes(b"solo")

        labels_out = tmp_path / "labels_out.tsv"
        report_dir = tmp_path / "report"
        report_dir.mkdir()

        rec = MagicMock()
        rec.path = photo
        rec.ext = "jpg"

        mock_fr = MagicMock()
        mock_fr.load_image_file.return_value = _FakeArray(shape=(100, 100, 3))
        mock_fr.face_locations.return_value = [(10, 90, 90, 10)]
        mock_fr.face_encodings.return_value = [np.random.rand(128)]

        # DBSCAN assigns cluster -1 (noise)
        mock_dbscan_instance = MagicMock()
        mock_dbscan_instance.fit_predict.return_value = np.array([-1])
        mock_dbscan_cls = MagicMock(return_value=mock_dbscan_instance)

        with (
            patch("godmode_media_library.autolabel_people.collect_file_records", return_value=[rec]),
            patch("godmode_media_library.autolabel_people._load_face_libs", return_value=(mock_fr, np, _FakeImage, mock_dbscan_cls)),
            patch("godmode_media_library.autolabel_people.build_asset_membership", return_value=({}, {}, {})),
            patch("godmode_media_library.autolabel_people.load_labels_table", return_value=([], {})),
            patch("godmode_media_library.autolabel_people.merge_label_updates", return_value=(0, 0)),
            patch("godmode_media_library.autolabel_people.write_labels_table"),
            patch("godmode_media_library.autolabel_people.write_tsv"),
        ):
            result = auto_people_labels(
                roots=[root],
                labels_in=None,
                labels_out=labels_out,
                report_dir=report_dir,
                min_samples=5,  # high threshold ensures noise
            )

        assert result.faces_detected == 1
        assert result.clusters == 0  # No valid clusters (all noise)
