from __future__ import annotations

from pathlib import Path

from godmode_media_library.models import PlanPolicy
from godmode_media_library.prune_recommend import _is_noise_file, recommend_prune
from godmode_media_library.utils import read_tsv_dict


def test_recommend_prune_finds_duplicates(tmp_media_tree: Path, tmp_path: Path):
    # Use a separate output dir to avoid scanning run artifacts
    out_dir = tmp_path / "prune_output_separate"
    out_dir.mkdir()
    run_dir = out_dir / "prune_run"
    policy = PlanPolicy()
    result = recommend_prune(
        roots=[tmp_media_tree],
        run_dir=run_dir,
        policy=policy,
        min_size_bytes=0,
        include_system_noise=False,
    )
    # photo1.jpg and photo3.jpg are duplicates — should appear as recommendations
    # They may be quarantine_candidate or manual_review (if detected as hardlink aliases)
    assert result.total_recommendations >= 1

    # Read the recommendations TSV to verify entries exist
    rows = read_tsv_dict(result.recommendations_tsv)
    assert len(rows) >= 1
    # At least one recommendation should reference the duplicate paths
    rec_paths = {Path(r["path"]).name for r in rows}
    assert "photo1.jpg" in rec_paths or "photo3.jpg" in rec_paths


def test_recommend_prune_noise_files(tmp_media_tree: Path, tmp_path: Path):
    run_dir = tmp_path / "prune_noise"
    policy = PlanPolicy()
    result = recommend_prune(
        roots=[tmp_media_tree],
        run_dir=run_dir,
        policy=policy,
        min_size_bytes=0,
        include_system_noise=True,
    )
    # .DS_Store and ._photo1.jpg should be flagged
    rows = read_tsv_dict(result.recommendations_tsv)
    noise_paths = {r["path"] for r in rows if "noise" in r.get("reason", "")}
    noise_names = {Path(p).name for p in noise_paths}
    assert ".DS_Store" in noise_names or "._photo1.jpg" in noise_names


def test_recommend_prune_output_files(tmp_media_tree: Path, tmp_path: Path):
    run_dir = tmp_path / "prune_output"
    policy = PlanPolicy()
    result = recommend_prune(
        roots=[tmp_media_tree],
        run_dir=run_dir,
        policy=policy,
        min_size_bytes=0,
    )
    assert result.recommendations_tsv.exists()
    assert result.recommended_paths_txt.exists()
    assert result.summary_json.exists()
    # Verify the file names
    assert result.recommendations_tsv.name == "prune_recommendations.tsv"
    assert result.recommended_paths_txt.name == "recommended_paths.txt"
    assert result.summary_json.name == "prune_summary.json"


def test_is_noise_file_ds_store():
    is_noise, reason = _is_noise_file(Path("/tmp/.DS_Store"))
    assert is_noise
    assert reason == "system_noise_file"


def test_is_noise_file_thumbs_db():
    is_noise, reason = _is_noise_file(Path("/tmp/Thumbs.db"))
    assert is_noise
    assert reason == "system_noise_file"


def test_is_noise_file_appledouble():
    is_noise, reason = _is_noise_file(Path("/tmp/._photo.jpg"))
    assert is_noise
    assert reason == "appledouble_sidecar_noise"


def test_is_noise_file_normal():
    is_noise, reason = _is_noise_file(Path("/tmp/photo.jpg"))
    assert not is_noise
    assert reason == ""


def test_is_noise_file_desktop_ini():
    is_noise, reason = _is_noise_file(Path("/tmp/desktop.ini"))
    assert is_noise


def test_recommend_prune_reclaimable(tmp_media_tree: Path, tmp_path: Path):
    run_dir = tmp_path / "prune_reclaim"
    policy = PlanPolicy()
    result = recommend_prune(
        roots=[tmp_media_tree],
        run_dir=run_dir,
        policy=policy,
        min_size_bytes=0,
        include_system_noise=True,
    )
    assert result.estimated_reclaim_bytes >= 0
    assert result.quarantine_candidates >= 0
    assert result.manual_review >= 0
