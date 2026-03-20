"""Tests for i18n module."""

from __future__ import annotations

from unittest.mock import patch

from godmode_media_library.i18n import get_lang, set_lang, t


def setup_function():
    """Reset language before each test."""
    import godmode_media_library.i18n as mod
    mod._current_lang = None


def test_default_language_english():
    with patch.dict("os.environ", {}, clear=True):
        import godmode_media_library.i18n as mod
        mod._current_lang = None
        assert get_lang() == "en"


def test_gml_lang_cs():
    with patch.dict("os.environ", {"GML_LANG": "cs"}):
        import godmode_media_library.i18n as mod
        mod._current_lang = None
        assert get_lang() == "cs"


def test_gml_lang_cz():
    with patch.dict("os.environ", {"GML_LANG": "cz"}):
        import godmode_media_library.i18n as mod
        mod._current_lang = None
        assert get_lang() == "cs"


def test_system_lang_cs():
    with patch.dict("os.environ", {"LANG": "cs_CZ.UTF-8"}, clear=True):
        import godmode_media_library.i18n as mod
        mod._current_lang = None
        assert get_lang() == "cs"


def test_set_lang():
    set_lang("cs")
    assert get_lang() == "cs"
    set_lang("en")
    assert get_lang() == "en"


def test_set_lang_invalid():
    set_lang("xx")
    assert get_lang() == "en"


def test_translate_english():
    set_lang("en")
    result = t("help.scan")
    assert "scan" in result.lower()


def test_translate_czech():
    set_lang("cs")
    result = t("help.scan")
    assert "sken" in result.lower() or "katalog" in result.lower()


def test_translate_with_params():
    set_lang("en")
    result = t("msg.scan_complete", scanned=10, new=5, changed=2)
    assert "10" in result
    assert "5" in result


def test_translate_missing_key():
    result = t("nonexistent.key")
    assert result == "nonexistent.key"


def test_translate_czech_with_params():
    set_lang("cs")
    result = t("msg.duplicate_groups", count=3)
    assert "3" in result
