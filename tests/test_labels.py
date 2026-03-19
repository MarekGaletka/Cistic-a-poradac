from __future__ import annotations

from pathlib import Path

from godmode_media_library.labels import (
    load_labels_table,
    merge_label_updates,
    write_labels_table,
)


def test_load_labels_table_empty():
    header, table = load_labels_table(None)
    assert header == ["path", "people", "place"]
    assert table == {}


def test_load_labels_table_roundtrip(tmp_path: Path):
    tsv_path = tmp_path / "labels.tsv"

    p1 = Path("/photos/img1.jpg")
    p2 = Path("/photos/img2.jpg")

    header = ["path", "people", "place"]
    table = {
        p1.expanduser().resolve(): {"path": str(p1.expanduser().resolve()), "people": "Alice", "place": "Paris"},
        p2.expanduser().resolve(): {"path": str(p2.expanduser().resolve()), "people": "", "place": "Berlin"},
    }
    write_labels_table(tsv_path, header, table)

    loaded_header, loaded_table = load_labels_table(tsv_path)
    assert "path" in loaded_header
    assert "people" in loaded_header
    assert "place" in loaded_header
    assert len(loaded_table) == 2

    # Check values
    for p, row in loaded_table.items():
        if "img1" in str(p):
            assert row["people"] == "Alice"
            assert row["place"] == "Paris"


def test_merge_label_updates_no_overwrite():
    table = {
        Path("/photos/img.jpg"): {"path": "/photos/img.jpg", "people": "Alice", "place": ""},
    }
    updates = {
        Path("/photos/img.jpg"): {"people": "Bob"},
    }
    touched, changed = merge_label_updates(table, updates, overwrite_people=False, overwrite_place=False)
    # Alice should be preserved because overwrite is False and she already has a value
    assert table[Path("/photos/img.jpg")]["people"] == "Alice"
    assert touched == 1
    assert changed == 0


def test_merge_label_updates_overwrite():
    table = {
        Path("/photos/img.jpg"): {"path": "/photos/img.jpg", "people": "Alice", "place": ""},
    }
    updates = {
        Path("/photos/img.jpg"): {"people": "Bob"},
    }
    touched, changed = merge_label_updates(table, updates, overwrite_people=True, overwrite_place=False)
    assert table[Path("/photos/img.jpg")]["people"] == "Bob"
    assert touched == 1
    assert changed == 1


def test_merge_label_updates_fill_empty():
    table = {
        Path("/photos/img.jpg"): {"path": "/photos/img.jpg", "people": "", "place": ""},
    }
    updates = {
        Path("/photos/img.jpg"): {"people": "Charlie", "place": "London"},
    }
    touched, changed = merge_label_updates(table, updates, overwrite_people=False, overwrite_place=False)
    assert table[Path("/photos/img.jpg")]["people"] == "Charlie"
    assert table[Path("/photos/img.jpg")]["place"] == "London"
    assert touched == 1
    assert changed == 1
