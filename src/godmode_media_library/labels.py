from __future__ import annotations

from pathlib import Path

from .utils import ensure_dir, read_tsv_dict, write_tsv

MANDATORY_COLUMNS = ("path", "people", "place")


def load_labels_table(path: Path | None) -> tuple[list[str], dict[Path, dict[str, str]]]:
    if path is None or not path.exists():
        return list(MANDATORY_COLUMNS), {}

    rows = read_tsv_dict(path)
    if not rows:
        return list(MANDATORY_COLUMNS), {}

    header = list(rows[0].keys())
    for col in MANDATORY_COLUMNS:
        if col not in header:
            header.append(col)

    by_path: dict[Path, dict[str, str]] = {}
    for row in rows:
        raw_path = (row.get("path") or "").strip()
        if not raw_path:
            continue
        p = Path(raw_path).expanduser().resolve()
        normalized = {key: (row.get(key, "") or "").strip() for key in header}
        normalized["path"] = str(p)
        by_path[p] = normalized

    return header, by_path


def write_labels_table(path: Path, header: list[str], rows: dict[Path, dict[str, str]]) -> None:
    ensure_dir(path.parent)
    fields = list(header)
    for col in MANDATORY_COLUMNS:
        if col not in fields:
            fields.append(col)

    def iter_rows() -> list[tuple[str, ...]]:
        ordered: list[tuple[str, ...]] = []
        for p, row in sorted(rows.items(), key=lambda x: str(x[0])):
            norm = {k: (row.get(k, "") or "").strip() for k in fields}
            norm["path"] = str(p)
            ordered.append(tuple(norm[col] for col in fields))
        return ordered

    write_tsv(path, fields, iter_rows())


def merge_label_updates(
    table: dict[Path, dict[str, str]],
    updates: dict[Path, dict[str, str]],
    *,
    overwrite_people: bool = False,
    overwrite_place: bool = False,
) -> tuple[int, int]:
    touched = 0
    changed = 0

    for p, patch in updates.items():
        if not patch:
            continue

        existing = table.get(p, {"path": str(p), "people": "", "place": ""})
        before_people = (existing.get("people") or "").strip()
        before_place = (existing.get("place") or "").strip()

        incoming_people = (patch.get("people") or "").strip()
        incoming_place = (patch.get("place") or "").strip()

        if incoming_people:
            if overwrite_people or not before_people:
                existing["people"] = incoming_people
        if incoming_place:
            if overwrite_place or not before_place:
                existing["place"] = incoming_place

        table[p] = existing
        touched += 1

        after_people = (existing.get("people") or "").strip()
        after_place = (existing.get("place") or "").strip()
        if after_people != before_people or after_place != before_place:
            changed += 1

    return touched, changed
