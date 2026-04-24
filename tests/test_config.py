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


# ── Range validation tests ────────────────────────────────────────────

import pytest

from godmode_media_library.config import ConfigValidationError, validate_config


class TestRangeValidation:
    def test_scan_workers_too_low(self):
        cfg = GMLConfig(scan_workers=0)
        with pytest.raises(ConfigValidationError, match="scan_workers"):
            validate_config(cfg)

    def test_scan_workers_too_high(self):
        cfg = GMLConfig(scan_workers=33)
        with pytest.raises(ConfigValidationError, match="scan_workers"):
            validate_config(cfg)

    def test_eps_too_low(self):
        cfg = GMLConfig(eps=0.001)
        with pytest.raises(ConfigValidationError, match="eps"):
            validate_config(cfg)

    def test_eps_too_high(self):
        cfg = GMLConfig(eps=3.0)
        with pytest.raises(ConfigValidationError, match="eps"):
            validate_config(cfg)

    def test_min_samples_too_low(self):
        cfg = GMLConfig(min_samples=0)
        with pytest.raises(ConfigValidationError, match="min_samples"):
            validate_config(cfg)

    def test_min_samples_too_high(self):
        cfg = GMLConfig(min_samples=101)
        with pytest.raises(ConfigValidationError, match="min_samples"):
            validate_config(cfg)

    def test_min_size_kb_negative(self):
        cfg = GMLConfig(min_size_kb=-1)
        with pytest.raises(ConfigValidationError, match="min_size_kb"):
            validate_config(cfg)

    def test_large_file_threshold_zero(self):
        cfg = GMLConfig(large_file_threshold_mb=0)
        with pytest.raises(ConfigValidationError, match="large_file_threshold_mb"):
            validate_config(cfg)

    def test_max_dimension_zero(self):
        cfg = GMLConfig(max_dimension=0)
        with pytest.raises(ConfigValidationError, match="max_dimension"):
            validate_config(cfg)

    def test_geocode_delay_negative(self):
        cfg = GMLConfig(geocode_min_delay_seconds=-0.5)
        with pytest.raises(ConfigValidationError, match="geocode_min_delay_seconds"):
            validate_config(cfg)

    def test_rate_limit_negative(self):
        cfg = GMLConfig(rate_limit_per_minute=-1)
        with pytest.raises(ConfigValidationError, match="rate_limit_per_minute"):
            validate_config(cfg)

    def test_dedup_min_file_size_negative(self):
        cfg = GMLConfig(dedup_min_file_size_kb=-1)
        with pytest.raises(ConfigValidationError, match="dedup_min_file_size_kb"):
            validate_config(cfg)

    def test_dedup_strategy_invalid(self):
        cfg = GMLConfig(dedup_strategy="invalid")
        with pytest.raises(ConfigValidationError, match="dedup_strategy"):
            validate_config(cfg)

    def test_dedup_similarity_threshold_too_low(self):
        cfg = GMLConfig(dedup_similarity_threshold=0)
        with pytest.raises(ConfigValidationError, match="dedup_similarity_threshold"):
            validate_config(cfg)

    def test_dedup_similarity_threshold_too_high(self):
        cfg = GMLConfig(dedup_similarity_threshold=65)
        with pytest.raises(ConfigValidationError, match="dedup_similarity_threshold"):
            validate_config(cfg)

    def test_valid_config_passes(self):
        cfg = GMLConfig()
        validate_config(cfg)  # Should not raise


# ── TOML error handling tests ─────────────────────────────────────────


class TestTomlErrorHandling:
    def test_invalid_toml_raises_config_error(self, tmp_path):
        bad_toml = tmp_path / "bad.toml"
        bad_toml.write_text("this is not [ valid toml ]]]]")
        with pytest.raises(ConfigValidationError, match="Failed to parse TOML"):
            load_config(
                global_path=bad_toml,
                project_path=tmp_path / "nonexistent.toml",
            )

    def test_no_tomllib_returns_empty(self, tmp_path, monkeypatch):
        """When tomllib is None, _load_toml returns empty dict."""
        import godmode_media_library.config as config_mod

        monkeypatch.setattr(config_mod, "tomllib", None)
        cfg = load_config(
            global_path=tmp_path / "whatever.toml",
            project_path=tmp_path / "nonexistent.toml",
        )
        assert cfg.min_size_kb == 500  # defaults


# ── Type validation tests ─────────────────────────────────────────────


class TestTypeValidation:
    def test_bool_field_rejects_int(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text("protect_asset_components = 1\n")
        with pytest.raises(ConfigValidationError, match="expected bool"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_int_field_rejects_bool(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text("min_size_kb = true\n")
        with pytest.raises(ConfigValidationError, match="expected int"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_int_field_rejects_string(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text('min_size_kb = "hello"\n')
        with pytest.raises(ConfigValidationError, match="expected int"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_float_field_rejects_bool(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text("eps = true\n")
        with pytest.raises(ConfigValidationError, match="expected float"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_float_field_accepts_int(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text("eps = 1\n")
        cfg = load_config(global_path=toml, project_path=tmp_path / "no.toml")
        assert cfg.eps == 1.0

    def test_str_field_rejects_int(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text("exiftool_bin = 42\n")
        with pytest.raises(ConfigValidationError, match="expected str"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_list_str_field_rejects_non_list(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text('prefer_roots = "not a list"\n')
        with pytest.raises(ConfigValidationError, match="expected list"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_list_str_field_rejects_non_string_items(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text("prefer_roots = [1, 2, 3]\n")
        with pytest.raises(ConfigValidationError, match="expected list"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_float_field_rejects_string(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text('eps = "not_float"\n')
        with pytest.raises(ConfigValidationError, match="expected float"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")


# ── Quarantine path validation ─────────────────────────────────────────


class TestQuarantinePathValidation:
    def test_system_dir_rejected(self, tmp_path):
        toml = tmp_path / "cfg.toml"
        toml.write_text('dedup_quarantine_path = "/"\n')
        with pytest.raises(ConfigValidationError, match="system directory"):
            load_config(global_path=toml, project_path=tmp_path / "no.toml")

    def test_valid_quarantine_path(self, tmp_path):
        qpath = tmp_path / "quarantine"
        toml = tmp_path / "cfg.toml"
        toml.write_text(f'dedup_quarantine_path = "{qpath}"\n')
        cfg = load_config(global_path=toml, project_path=tmp_path / "no.toml")
        assert cfg.dedup_quarantine_path == str(qpath)
