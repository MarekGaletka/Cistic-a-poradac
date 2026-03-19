from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pytest

from godmode_media_library.models import (
    PlanPolicy,
    TreePlanRow,
)


def test_file_record_frozen(make_file_record):
    rec = make_file_record()
    with pytest.raises(AttributeError):
        rec.size = 999


def test_file_record_fields(make_file_record):
    rec = make_file_record()
    expected_names = {
        "path",
        "size",
        "mtime",
        "ctime",
        "birthtime",
        "ext",
        "meaningful_xattr_count",
        "asset_key",
        "asset_component",
    }
    actual_names = {f.name for f in fields(rec)}
    assert actual_names == expected_names

    assert isinstance(rec.path, Path)
    assert isinstance(rec.size, int)
    assert isinstance(rec.mtime, float)
    assert isinstance(rec.ctime, float)
    assert isinstance(rec.ext, str)
    assert isinstance(rec.meaningful_xattr_count, int)
    assert isinstance(rec.asset_component, bool)


def test_duplicate_row_frozen(make_duplicate_row):
    row = make_duplicate_row()
    with pytest.raises(AttributeError):
        row.digest = "new_hash"


def test_plan_policy_defaults():
    policy = PlanPolicy()
    assert policy.protect_asset_components is True
    assert policy.prefer_earliest_origin_time is True
    assert policy.prefer_richer_metadata is True
    assert policy.prefer_roots == ()


def test_tree_plan_row_fields():
    row = TreePlanRow(
        unit_id="abc123",
        source_path=Path("/src/photo.jpg"),
        destination_path=Path("/dst/photo.jpg"),
        mode="time",
        bucket="2024/01/15",
        asset_key="parent\tstem",
        is_asset_component=True,
    )
    expected_names = {
        "unit_id",
        "source_path",
        "destination_path",
        "mode",
        "bucket",
        "asset_key",
        "is_asset_component",
    }
    actual_names = {f.name for f in fields(row)}
    assert actual_names == expected_names
    assert isinstance(row.unit_id, str)
    assert isinstance(row.source_path, Path)
    assert isinstance(row.is_asset_component, bool)
