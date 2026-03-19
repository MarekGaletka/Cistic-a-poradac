from __future__ import annotations

from pathlib import Path

from godmode_media_library.config import GMLConfig, format_config, load_config


def test_default_config():
    cfg = GMLConfig()
    assert cfg.min_size_kb == 500
    assert cfg.large_file_threshold_mb == 500
    assert cfg.protect_asset_components is True
    assert cfg.prefer_earliest_origin_time is True
    assert cfg.prefer_richer_metadata is True
    assert cfg.prefer_roots == []
    assert cfg.exiftool_bin == "exiftool"
    assert cfg.person_prefix == "Person"
    assert cfg.geocode_min_delay_seconds == 1.1
    assert cfg.max_dimension == 1600
    assert cfg.eps == 0.5
    assert cfg.min_samples == 2


def test_load_config_no_files(tmp_path: Path):
    cfg = load_config(
        global_path=tmp_path / "nonexistent_global.toml",
        project_path=tmp_path / "nonexistent_project.toml",
    )
    assert cfg.min_size_kb == 500
    assert cfg.exiftool_bin == "exiftool"


def test_load_config_global(tmp_path: Path):
    global_toml = tmp_path / "global.toml"
    global_toml.write_text('min_size_kb = 1024\nexiftool_bin = "/usr/local/bin/exiftool"\n')
    cfg = load_config(
        global_path=global_toml,
        project_path=tmp_path / "nonexistent.toml",
    )
    assert cfg.min_size_kb == 1024
    assert cfg.exiftool_bin == "/usr/local/bin/exiftool"


def test_load_config_project_overrides_global(tmp_path: Path):
    global_toml = tmp_path / "global.toml"
    global_toml.write_text("min_size_kb = 1024\nlarge_file_threshold_mb = 200\n")
    project_toml = tmp_path / "project.toml"
    project_toml.write_text("min_size_kb = 2048\n")
    cfg = load_config(
        global_path=global_toml,
        project_path=project_toml,
    )
    assert cfg.min_size_kb == 2048
    assert cfg.large_file_threshold_mb == 200


def test_load_config_cli_overrides_all(tmp_path: Path):
    global_toml = tmp_path / "global.toml"
    global_toml.write_text("min_size_kb = 1024\n")
    project_toml = tmp_path / "project.toml"
    project_toml.write_text("min_size_kb = 2048\n")
    cfg = load_config(
        cli_overrides={"min_size_kb": 4096},
        global_path=global_toml,
        project_path=project_toml,
    )
    assert cfg.min_size_kb == 4096


def test_load_config_cli_none_ignored(tmp_path: Path):
    global_toml = tmp_path / "global.toml"
    global_toml.write_text("min_size_kb = 1024\n")
    cfg = load_config(
        cli_overrides={"min_size_kb": None, "exiftool_bin": None},
        global_path=global_toml,
        project_path=tmp_path / "nonexistent.toml",
    )
    assert cfg.min_size_kb == 1024
    assert cfg.exiftool_bin == "exiftool"


def test_format_config():
    cfg = GMLConfig(min_size_kb=999, exiftool_bin="/bin/et")
    output = format_config(cfg)
    assert "min_size_kb = 999" in output
    assert 'exiftool_bin = "/bin/et"' in output


def test_format_config_bool():
    cfg = GMLConfig(protect_asset_components=True, prefer_earliest_origin_time=False)
    output = format_config(cfg)
    assert "protect_asset_components = true" in output
    assert "prefer_earliest_origin_time = false" in output


def test_format_config_list():
    cfg = GMLConfig(prefer_roots=["/a", "/b"])
    output = format_config(cfg)
    assert 'prefer_roots = ["/a", "/b"]' in output
