from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from godmode_media_library.metadata_merge import (
    MergeAction,
    MergePlan,
    create_merge_plan,
    execute_merge,
    write_merge_plan_tsv,
)
from godmode_media_library.metadata_richness import MetadataDiff


def _make_diff_with_partial() -> MetadataDiff:
    return MetadataDiff(
        unanimous={"EXIF:Make": "Canon"},
        partial={
            "EXIF:GPSLatitude": {"/donor.jpg": 50.0875},
            "EXIF:GPSLongitude": {"/donor.jpg": 14.4214},
            "EXIF:DateTimeOriginal": {"/donor.jpg": "2024:06:15"},
        },
        conflicts={
            "EXIF:ISO": {"/survivor.jpg": 400, "/donor.jpg": 800},
        },
        scores={"/survivor.jpg": 70.0, "/donor.jpg": 40.0},
    )


def test_create_merge_plan_basic():
    survivor_meta = {"EXIF:Make": "Canon", "EXIF:Model": "EOS R5"}
    diff = _make_diff_with_partial()

    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc123"):
        plan = create_merge_plan("/survivor.jpg", survivor_meta, diff)

    assert plan.survivor_path == "/survivor.jpg"
    assert plan.survivor_hash == "abc123"
    # GPS and DateTimeOriginal should be copy actions
    copy_tags = {a.tag for a in plan.actions}
    assert "EXIF:GPSLatitude" in copy_tags
    assert "EXIF:GPSLongitude" in copy_tags
    assert "EXIF:DateTimeOriginal" in copy_tags
    # ISO conflict should be logged
    assert len(plan.conflicts) == 1
    assert plan.conflicts[0].tag == "EXIF:ISO"


def test_create_merge_plan_uncopyable_tags():
    diff = MetadataDiff(
        partial={
            "System:FileName": {"/donor.jpg": "photo.jpg"},
            "System:FileSize": {"/donor.jpg": 12345},
            "EXIF:Make": {"/donor.jpg": "Canon"},
        },
    )
    survivor_meta = {}
    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc"):
        plan = create_merge_plan("/survivor.jpg", survivor_meta, diff)
    # FileName and FileSize should be skipped
    copy_tags = {a.tag for a in plan.actions}
    assert "System:FileName" not in copy_tags
    assert "System:FileSize" not in copy_tags
    assert "EXIF:Make" in copy_tags


def test_create_merge_plan_makernotes_skip():
    diff = MetadataDiff(
        partial={
            "MakerNotes:SerialNumber": {"/donor.jpg": "12345"},
        },
    )
    survivor_meta = {"MakerNotes:InternalSerialNumber": "67890"}
    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc"):
        plan = create_merge_plan("/survivor.jpg", survivor_meta, diff)
    # Survivor already has MakerNotes → skip donor's MakerNotes
    assert len(plan.actions) == 0
    assert len(plan.skipped) == 1
    assert plan.skipped[0].action_type == "skip_makernotes"


def test_create_merge_plan_makernotes_copy():
    diff = MetadataDiff(
        partial={
            "MakerNotes:SerialNumber": {"/donor.jpg": "12345"},
        },
    )
    survivor_meta = {"EXIF:Make": "Canon"}  # No MakerNotes
    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc"):
        plan = create_merge_plan("/survivor.jpg", survivor_meta, diff)
    # Survivor has no MakerNotes → copy
    assert len(plan.actions) == 1
    assert plan.actions[0].action_type == "copy"


def test_create_merge_plan_list_merge():
    diff = MetadataDiff(
        conflicts={
            "IPTC:Keywords": {"/survivor.jpg": ["travel"], "/donor.jpg": ["europe", "prague"]},
        },
    )
    survivor_meta = {"IPTC:Keywords": ["travel"]}
    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc"):
        plan = create_merge_plan("/survivor.jpg", survivor_meta, diff)
    # Keywords should be merged (union)
    merge_actions = [a for a in plan.actions if a.action_type == "merge_list"]
    assert len(merge_actions) == 1
    # New values should be europe and prague (travel already in survivor)
    new_vals = set(merge_actions[0].value)
    assert "europe" in new_vals
    assert "prague" in new_vals


def test_write_merge_plan_tsv(tmp_path: Path):
    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[
            MergeAction(tag="EXIF:GPS", value=50.0, source_path="/donor.jpg", action_type="copy"),
        ],
        conflicts=[
            MergeAction(tag="EXIF:ISO", value=800, source_path="/donor.jpg", action_type="skip_conflict"),
        ],
    )
    out = tmp_path / "merge_plan.tsv"
    write_merge_plan_tsv(out, plan)
    assert out.exists()
    content = out.read_text()
    assert "EXIF:GPS" in content
    assert "EXIF:ISO" in content
    assert "copy" in content
    assert "skip_conflict" in content


def test_execute_merge_dry_run():
    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[
            MergeAction(tag="EXIF:GPSLatitude", value=50.0, source_path="/donor.jpg", action_type="copy"),
            MergeAction(tag="EXIF:GPSLongitude", value=14.0, source_path="/donor.jpg", action_type="copy"),
        ],
        conflicts=[
            MergeAction(tag="EXIF:ISO", value=800, source_path="/donor.jpg", action_type="skip_conflict"),
        ],
    )
    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc123"):
        result = execute_merge(plan, dry_run=True)
    assert result.applied == 2
    assert result.conflicts == 1
    assert result.error is None


def test_execute_merge_no_actions():
    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[],
    )
    result = execute_merge(plan, dry_run=False)
    assert result.applied == 0


def test_execute_merge_hash_mismatch():
    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="expected_hash",
        actions=[
            MergeAction(tag="EXIF:GPS", value=50.0, source_path="/donor.jpg", action_type="copy"),
        ],
    )
    with patch("godmode_media_library.metadata_merge.sha256_file", return_value="different_hash"):
        result = execute_merge(plan, dry_run=False)
    assert result.error is not None
    assert "hash changed" in result.error


def test_execute_merge_exiftool_success():
    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[
            MergeAction(tag="EXIF:GPSLatitude", value=50.0875, source_path="/donor.jpg", action_type="copy"),
            MergeAction(tag="IPTC:Keywords", value=["europe", "prague"], source_path="/donor.jpg", action_type="merge_list"),
        ],
    )
    mock_proc = MagicMock()
    mock_proc.returncode = 0
    mock_proc.stdout = ""
    mock_proc.stderr = ""

    with (
        patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc123"),
        patch("godmode_media_library.metadata_merge.shutil.which", return_value="/usr/bin/exiftool"),
        patch("subprocess.run", return_value=mock_proc) as mock_run,
        patch("pathlib.Path.exists", return_value=False),
    ):
        result = execute_merge(plan, dry_run=False)

    assert result.applied == 2
    assert result.error is None
    # Verify ExifTool command was built correctly
    cmd = mock_run.call_args[0][0]
    assert "-EXIF:GPSLatitude=50.0875" in cmd
    assert "-IPTC:Keywords+=europe" in cmd
    assert "-IPTC:Keywords+=prague" in cmd


def test_execute_merge_no_exiftool():
    plan = MergePlan(
        survivor_path="/survivor.jpg",
        survivor_hash="abc123",
        actions=[MergeAction(tag="EXIF:GPS", value=50.0, source_path="/d.jpg", action_type="copy")],
    )
    with (
        patch("godmode_media_library.metadata_merge.sha256_file", return_value="abc123"),
        patch("godmode_media_library.metadata_merge.shutil.which", return_value=None),
    ):
        result = execute_merge(plan, dry_run=False)
    assert result.error == "ExifTool not available"
