from __future__ import annotations

from pathlib import Path

from godmode_media_library.actions import apply_plan, promote_from_manifest, restore_from_log
from godmode_media_library.utils import write_tsv


def _create_plan_tsv(plan_path: Path, rows: list[tuple[str, ...]]) -> None:
    header = ["hash", "size", "keep_path", "move_path", "reason", "keep_score", "move_score"]
    write_tsv(plan_path, header, rows)


def _create_executed_log_tsv(log_path: Path, rows: list[tuple[str, ...]]) -> None:
    header = ["hash", "size", "keep_path", "move_path", "quarantine_path", "reason", "verified_hash"]
    write_tsv(log_path, header, rows)


def test_apply_plan_moves_file(tmp_path: Path):
    # Create two identical files
    keep_file = tmp_path / "keep" / "photo.jpg"
    move_file = tmp_path / "move" / "photo.jpg"
    keep_file.parent.mkdir(parents=True)
    move_file.parent.mkdir(parents=True)
    content = b"IDENTICAL_CONTENT" * 100
    keep_file.write_bytes(content)
    move_file.write_bytes(content)

    from godmode_media_library.utils import sha256_file

    digest = sha256_file(keep_file)

    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(content)), str(keep_file), str(move_file), "test", "100.0", "50.0"),
    ])

    quarantine = tmp_path / "quarantine"
    executed_log = tmp_path / "executed.tsv"
    skipped_log = tmp_path / "skipped.tsv"

    result = apply_plan(plan_path, quarantine, executed_log, skipped_log, dry_run=False)

    assert result.moved == 1
    assert result.skipped == 0
    assert not move_file.exists()
    assert keep_file.exists()


def test_apply_plan_dry_run(tmp_path: Path):
    keep_file = tmp_path / "keep" / "photo.jpg"
    move_file = tmp_path / "move" / "photo.jpg"
    keep_file.parent.mkdir(parents=True)
    move_file.parent.mkdir(parents=True)
    content = b"IDENTICAL_CONTENT" * 100
    keep_file.write_bytes(content)
    move_file.write_bytes(content)

    from godmode_media_library.utils import sha256_file

    digest = sha256_file(keep_file)

    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(content)), str(keep_file), str(move_file), "test", "100.0", "50.0"),
    ])

    quarantine = tmp_path / "quarantine"
    executed_log = tmp_path / "executed.tsv"
    skipped_log = tmp_path / "skipped.tsv"

    result = apply_plan(plan_path, quarantine, executed_log, skipped_log, dry_run=True)

    assert result.moved == 1  # counted as moved even in dry run
    # File should still exist since it's dry run
    assert move_file.exists()


def test_apply_plan_skip_missing(tmp_path: Path):
    keep_file = tmp_path / "keep" / "photo.jpg"
    keep_file.parent.mkdir(parents=True)
    keep_file.write_bytes(b"content")

    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        ("abc", "100", str(keep_file), str(tmp_path / "nonexistent.jpg"), "test", "100.0", "50.0"),
    ])

    quarantine = tmp_path / "quarantine"
    result = apply_plan(plan_path, quarantine, tmp_path / "exec.tsv", tmp_path / "skip.tsv", dry_run=False)

    assert result.moved == 0
    assert result.skipped == 1


def test_apply_plan_skip_hash_mismatch(tmp_path: Path):
    keep_file = tmp_path / "keep" / "photo.jpg"
    move_file = tmp_path / "move" / "photo.jpg"
    keep_file.parent.mkdir(parents=True)
    move_file.parent.mkdir(parents=True)
    keep_file.write_bytes(b"CONTENT_A" * 100)
    move_file.write_bytes(b"CONTENT_B" * 100)  # different content

    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        ("abc", "900", str(keep_file), str(move_file), "test", "100.0", "50.0"),
    ])

    quarantine = tmp_path / "quarantine"
    result = apply_plan(plan_path, quarantine, tmp_path / "exec.tsv", tmp_path / "skip.tsv", dry_run=False)

    assert result.moved == 0
    assert result.skipped == 1
    assert move_file.exists()  # not moved because of mismatch


def test_restore_from_log(tmp_path: Path):
    # Simulate a file that was moved to quarantine
    original_dir = tmp_path / "original"
    original_dir.mkdir()
    quarantine_dir = tmp_path / "quarantine"
    quarantine_dir.mkdir()

    original_path = original_dir / "photo.jpg"
    quarantine_path = quarantine_dir / "photo.jpg"

    # File is in quarantine, not at original location
    quarantine_path.write_bytes(b"photo data")
    assert not original_path.exists()

    log_path = tmp_path / "executed_log.tsv"
    _create_executed_log_tsv(log_path, [
        ("abc", "10", str(tmp_path / "keep.jpg"), str(original_path), str(quarantine_path), "test", "abc"),
    ])

    restored, skipped = restore_from_log(log_path, dry_run=False)

    assert restored == 1
    assert skipped == 0
    assert original_path.exists()
    assert not quarantine_path.exists()


def test_apply_plan_hash_mismatch_skips(tmp_path: Path):
    """When keep and move files have different content, the row should be skipped."""
    keep_file = tmp_path / "keep" / "photo.jpg"
    move_file = tmp_path / "move" / "photo.jpg"
    keep_file.parent.mkdir(parents=True)
    move_file.parent.mkdir(parents=True)
    keep_file.write_bytes(b"CONTENT_KEEP" * 50)
    move_file.write_bytes(b"CONTENT_MOVE" * 50)

    from godmode_media_library.utils import sha256_file

    # Use keep file's hash in the plan — move file has different content
    digest = sha256_file(keep_file)

    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(b"CONTENT_KEEP" * 50)), str(keep_file), str(move_file), "test", "100.0", "50.0"),
    ])

    quarantine = tmp_path / "quarantine"
    result = apply_plan(plan_path, quarantine, tmp_path / "exec.tsv", tmp_path / "skip.tsv", dry_run=False)

    assert result.moved == 0
    assert result.skipped == 1
    assert move_file.exists()  # file was not moved


def test_restore_skips_if_quarantine_missing(tmp_path: Path):
    """When quarantine file is gone, restore should skip."""
    original_path = tmp_path / "original" / "photo.jpg"
    quarantine_path = tmp_path / "quarantine" / "photo.jpg"
    # Neither file exists — quarantine is missing

    log_path = tmp_path / "executed_log.tsv"
    _create_executed_log_tsv(log_path, [
        ("abc", "10", str(tmp_path / "keep.jpg"), str(original_path), str(quarantine_path), "test", "abc"),
    ])

    restored, skipped = restore_from_log(log_path, dry_run=False)

    assert restored == 0
    assert skipped == 1
    assert not original_path.exists()


def test_restore_skips_if_original_exists(tmp_path: Path):
    """When the original path already exists, restore should skip."""
    original_dir = tmp_path / "original"
    original_dir.mkdir()
    quarantine_dir = tmp_path / "quarantine"
    quarantine_dir.mkdir()

    original_path = original_dir / "photo.jpg"
    quarantine_path = quarantine_dir / "photo.jpg"

    # Both exist — original is already in place
    original_path.write_bytes(b"original data")
    quarantine_path.write_bytes(b"quarantine data")

    log_path = tmp_path / "executed_log.tsv"
    _create_executed_log_tsv(log_path, [
        ("abc", "10", str(tmp_path / "keep.jpg"), str(original_path), str(quarantine_path), "test", "abc"),
    ])

    restored, skipped = restore_from_log(log_path, dry_run=False)

    assert restored == 0
    assert skipped == 1
    # Both files should still exist unchanged
    assert original_path.exists()
    assert quarantine_path.exists()


def _create_promote_manifest_tsv(path: Path, rows: list[tuple[str, ...]]) -> None:
    header = ["size", "moved_from", "quarantine_path", "primary_path"]
    write_tsv(path, header, rows)


def test_promote_from_manifest_swaps(tmp_path: Path):
    """Promote swaps quarantine copy into primary location."""
    primary = tmp_path / "primary" / "photo.jpg"
    quarantine = tmp_path / "quarantine" / "photo.jpg"
    primary.parent.mkdir(parents=True)
    quarantine.parent.mkdir(parents=True)

    content = b"IDENTICAL_CONTENT" * 50
    primary.write_bytes(content)
    quarantine.write_bytes(content)

    manifest_path = tmp_path / "manifest.tsv"
    _create_promote_manifest_tsv(manifest_path, [
        (str(len(content)), str(quarantine), str(quarantine), str(primary)),
    ])

    backup_root = tmp_path / "backup"
    executed_log = tmp_path / "promote_exec.tsv"
    skipped_log = tmp_path / "promote_skip.tsv"

    swapped, skipped, bytes_swapped = promote_from_manifest(
        manifest_path=manifest_path,
        backup_root=backup_root,
        executed_log_path=executed_log,
        skipped_log_path=skipped_log,
        dry_run=False,
    )

    assert swapped == 1
    assert skipped == 0
    assert bytes_swapped == len(content)
    assert primary.exists()
    assert not quarantine.exists()


def test_promote_from_manifest_skips_missing(tmp_path: Path):
    """Promote skips when quarantine or primary path is missing."""
    manifest_path = tmp_path / "manifest.tsv"
    _create_promote_manifest_tsv(manifest_path, [
        ("100", str(tmp_path / "gone_from"), str(tmp_path / "gone_q"), str(tmp_path / "gone_p")),
    ])

    swapped, skipped, _ = promote_from_manifest(
        manifest_path=manifest_path,
        backup_root=tmp_path / "backup",
        executed_log_path=tmp_path / "exec.tsv",
        skipped_log_path=tmp_path / "skip.tsv",
        dry_run=False,
    )

    assert swapped == 0
    assert skipped == 1


def test_promote_from_manifest_skips_hash_mismatch(tmp_path: Path):
    """Promote skips when quarantine and primary have different content."""
    primary = tmp_path / "primary" / "photo.jpg"
    quarantine = tmp_path / "quarantine" / "photo.jpg"
    primary.parent.mkdir(parents=True)
    quarantine.parent.mkdir(parents=True)

    primary.write_bytes(b"PRIMARY_CONTENT" * 50)
    quarantine.write_bytes(b"QUARANTINE_CONTENT" * 50)

    manifest_path = tmp_path / "manifest.tsv"
    _create_promote_manifest_tsv(manifest_path, [
        ("100", str(quarantine), str(quarantine), str(primary)),
    ])

    swapped, skipped, _ = promote_from_manifest(
        manifest_path=manifest_path,
        backup_root=tmp_path / "backup",
        executed_log_path=tmp_path / "exec.tsv",
        skipped_log_path=tmp_path / "skip.tsv",
        dry_run=False,
    )

    assert swapped == 0
    assert skipped == 1


def test_promote_from_manifest_dry_run(tmp_path: Path):
    """Promote dry run counts swap but doesn't move files."""
    primary = tmp_path / "primary" / "photo.jpg"
    quarantine = tmp_path / "quarantine" / "photo.jpg"
    primary.parent.mkdir(parents=True)
    quarantine.parent.mkdir(parents=True)

    content = b"SAME_CONTENT" * 50
    primary.write_bytes(content)
    quarantine.write_bytes(content)

    manifest_path = tmp_path / "manifest.tsv"
    _create_promote_manifest_tsv(manifest_path, [
        (str(len(content)), str(quarantine), str(quarantine), str(primary)),
    ])

    swapped, skipped, _ = promote_from_manifest(
        manifest_path=manifest_path,
        backup_root=tmp_path / "backup",
        executed_log_path=tmp_path / "exec.tsv",
        skipped_log_path=tmp_path / "skip.tsv",
        dry_run=True,
    )

    assert swapped == 1
    assert skipped == 0
    # Both files still exist since it's dry run
    assert primary.exists()
    assert quarantine.exists()


# ── Rollback tests ──────────────────────────────────────────────────


def test_apply_plan_rollback_on_error(tmp_path: Path):
    """When apply_plan hits an unexpected error mid-way, already-moved files are rolled back."""
    from unittest.mock import patch

    from godmode_media_library.utils import sha256_file

    # Create 3 identical file pairs
    content = b"ROLLBACK_TEST" * 100
    files = []
    for i in range(3):
        keep = tmp_path / f"keep_{i}" / "photo.jpg"
        move = tmp_path / f"move_{i}" / "photo.jpg"
        keep.parent.mkdir(parents=True)
        move.parent.mkdir(parents=True)
        keep.write_bytes(content)
        move.write_bytes(content)
        files.append((keep, move))

    digest = sha256_file(files[0][0])

    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(content)), str(f[0]), str(f[1]), "test", "100.0", "50.0")
        for f in files
    ])

    quarantine = tmp_path / "quarantine"
    executed_log = tmp_path / "executed.tsv"
    skipped_log = tmp_path / "skipped.tsv"

    # Patch shutil.move to fail on the 3rd call
    original_move = __import__("shutil").move
    call_count = 0

    def failing_move(src, dst):
        nonlocal call_count
        call_count += 1
        if call_count == 3:
            raise PermissionError("Simulated disk error")
        return original_move(src, dst)

    with patch("godmode_media_library.actions.shutil.move", side_effect=failing_move):
        result = apply_plan(plan_path, quarantine, executed_log, skipped_log, dry_run=False)

    assert result.error is not None
    assert result.rolled_back > 0
    # The first two files that were moved should be restored
    for _, move in files[:2]:
        assert move.exists(), f"Rolled-back file should be restored: {move}"


def test_apply_plan_result_has_rollback_fields(tmp_path: Path):
    """ApplyResult includes rolled_back and error fields."""
    from godmode_media_library.utils import sha256_file

    content = b"FIELD_TEST" * 50
    keep = tmp_path / "keep" / "photo.jpg"
    move = tmp_path / "move" / "photo.jpg"
    keep.parent.mkdir(parents=True)
    move.parent.mkdir(parents=True)
    keep.write_bytes(content)
    move.write_bytes(content)

    digest = sha256_file(keep)
    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(content)), str(keep), str(move), "test", "100.0", "50.0"),
    ])

    result = apply_plan(
        plan_path, tmp_path / "q", tmp_path / "exec.tsv", tmp_path / "skip.tsv"
    )
    assert result.rolled_back == 0
    assert result.error is None


# ── Selective restore tests ─────────────────────────────────────────


def test_selective_restore_last_n(tmp_path: Path):
    """Selective restore with --last N restores only the last N moves."""
    from godmode_media_library.actions import selective_restore
    from godmode_media_library.utils import sha256_file

    content = b"SELECTIVE_TEST" * 50

    # Create 3 file pairs, apply plan, then selectively restore last 1
    files = []
    for i in range(3):
        keep = tmp_path / f"keep_{i}" / "photo.jpg"
        move = tmp_path / f"move_{i}" / "photo.jpg"
        keep.parent.mkdir(parents=True)
        move.parent.mkdir(parents=True)
        keep.write_bytes(content)
        move.write_bytes(content)
        files.append((keep, move))

    digest = sha256_file(files[0][0])
    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(content)), str(f[0]), str(f[1]), "test", "100.0", "50.0")
        for f in files
    ])

    quarantine = tmp_path / "quarantine"
    exec_log = tmp_path / "exec.tsv"
    skip_log = tmp_path / "skip.tsv"

    apply_plan(plan_path, quarantine, exec_log, skip_log)

    # All moves gone
    for _, move in files:
        assert not move.exists()

    # Restore only last 1
    restored, skipped = selective_restore(exec_log, last_n=1)
    assert restored == 1


def test_selective_restore_by_file(tmp_path: Path):
    """Selective restore with --file restores specific files."""
    from godmode_media_library.actions import selective_restore
    from godmode_media_library.utils import sha256_file

    content = b"FILE_RESTORE" * 50
    keep = tmp_path / "keep" / "a.jpg"
    move = tmp_path / "move" / "a.jpg"
    keep.parent.mkdir(parents=True)
    move.parent.mkdir(parents=True)
    keep.write_bytes(content)
    move.write_bytes(content)

    digest = sha256_file(keep)
    plan_path = tmp_path / "plan.tsv"
    _create_plan_tsv(plan_path, [
        (digest, str(len(content)), str(keep), str(move), "test", "100.0", "50.0"),
    ])

    quarantine = tmp_path / "quarantine"
    exec_log = tmp_path / "exec.tsv"
    skip_log = tmp_path / "skip.tsv"

    apply_plan(plan_path, quarantine, exec_log, skip_log)
    assert not move.exists()

    restored, skipped = selective_restore(exec_log, file_paths=[move])
    assert restored == 1
    assert move.exists()
