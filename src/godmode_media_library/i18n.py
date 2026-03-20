"""Internationalization support (English / Czech).

Language is detected from:
1. GML_LANG env var (explicit override)
2. LANG env var (system locale)
3. Default: English
"""

from __future__ import annotations

import os

_TRANSLATIONS: dict[str, dict[str, str]] = {
    # ── CLI help texts ──────────────────────────────────────────────
    "help.scan": {
        "en": "Incremental scan: update catalog from filesystem",
        "cs": "Inkrementální sken: aktualizace katalogu ze souborového systému",
    },
    "help.scan.roots": {
        "en": "Root directories to scan",
        "cs": "Kořenové adresáře ke skenování",
    },
    "help.scan.catalog": {
        "en": "Catalog DB path (default: ~/.config/gml/catalog.db)",
        "cs": "Cesta ke katalogu (výchozí: ~/.config/gml/catalog.db)",
    },
    "help.scan.force_rehash": {
        "en": "Recompute SHA-256 for all files",
        "cs": "Přepočítat SHA-256 pro všechny soubory",
    },
    "help.scan.exiftool": {
        "en": "Run deep ExifTool metadata extraction after scan",
        "cs": "Spustit hloubkovou extrakci metadat ExifToolem po skenu",
    },
    "help.scan.workers": {
        "en": "Parallel workers for hashing/media extraction (default: 1)",
        "cs": "Paralelní vlákna pro hashování/extrakci médií (výchozí: 1)",
    },
    "help.query": {
        "en": "Search files in catalog",
        "cs": "Vyhledávání souborů v katalogu",
    },
    "help.dups": {
        "en": "List duplicate groups",
        "cs": "Zobrazit skupiny duplicit",
    },
    "help.similar": {
        "en": "Find visually similar files",
        "cs": "Najít vizuálně podobné soubory",
    },
    "help.stats": {
        "en": "Show catalog statistics",
        "cs": "Zobrazit statistiky katalogu",
    },
    "help.doctor": {
        "en": "Check required dependencies",
        "cs": "Zkontrolovat potřebné závislosti",
    },
    "help.auto": {
        "en": "Run full pipeline: scan → extract → diff → merge",
        "cs": "Spustit celý pipeline: sken → extrakce → diff → merge",
    },
    "help.serve": {
        "en": "Start web UI server",
        "cs": "Spustit webový server",
    },
    "help.cloud": {
        "en": "Show cloud storage status and setup guide",
        "cs": "Zobrazit stav cloudového úložiště a návod k nastavení",
    },
    "help.extract": {
        "en": "Deep metadata extraction via ExifTool",
        "cs": "Hloubková extrakce metadat přes ExifTool",
    },
    "help.diff": {
        "en": "Compare metadata across duplicate groups",
        "cs": "Porovnat metadata mezi skupinami duplicit",
    },
    "help.merge": {
        "en": "Merge metadata across duplicates",
        "cs": "Sloučit metadata mezi duplicitami",
    },
    "help.plan": {
        "en": "Create deduplication plan",
        "cs": "Vytvořit plán deduplikace",
    },
    "help.apply": {
        "en": "Apply deduplication plan",
        "cs": "Aplikovat plán deduplikace",
    },

    # ── Output messages ─────────────────────────────────────────────
    "msg.scan_complete": {
        "en": "Scan complete: {scanned} scanned, {new} new, {changed} changed",
        "cs": "Sken dokončen: {scanned} prohledáno, {new} nových, {changed} změněných",
    },
    "msg.no_duplicates": {
        "en": "No duplicate groups found.",
        "cs": "Nebyly nalezeny žádné skupiny duplicit.",
    },
    "msg.duplicate_groups": {
        "en": "{count} duplicate group(s) found",
        "cs": "Nalezeno {count} skupin(a) duplicit",
    },
    "msg.extraction_complete": {
        "en": "Metadata extracted for {count} file(s).",
        "cs": "Metadata extrahována pro {count} soubor(ů).",
    },
    "msg.merge_applied": {
        "en": "Merge applied: {count} tag(s) written.",
        "cs": "Sloučení provedeno: {count} tag(ů) zapsáno.",
    },
    "msg.pipeline_step": {
        "en": "Step {n}/{total}: {name}",
        "cs": "Krok {n}/{total}: {name}",
    },
    "msg.confirm_merge": {
        "en": "Apply merge plans? (y/N): ",
        "cs": "Provést sloučení metadat? (a/N): ",
    },
}


def _detect_lang() -> str:
    """Detect language from environment variables."""
    lang = os.environ.get("GML_LANG", "").lower()
    if lang in ("cs", "cz", "czech"):
        return "cs"
    if lang in ("en", "english"):
        return "en"

    system_lang = os.environ.get("LANG", "").lower()
    if system_lang.startswith("cs"):
        return "cs"

    return "en"


_current_lang: str | None = None


def get_lang() -> str:
    """Get the current language."""
    global _current_lang
    if _current_lang is None:
        _current_lang = _detect_lang()
    return _current_lang


def set_lang(lang: str) -> None:
    """Override the current language."""
    global _current_lang
    _current_lang = lang if lang in ("cs", "en") else "en"


def t(key: str, **kwargs) -> str:
    """Translate a key to the current language.

    Supports format placeholders: t("msg.scan_complete", scanned=10, new=5)
    Falls back to English if key or language is missing.
    """
    entry = _TRANSLATIONS.get(key)
    if entry is None:
        return key

    lang = get_lang()
    text = entry.get(lang, entry.get("en", key))

    if kwargs:
        try:
            return text.format(**kwargs)
        except (KeyError, ValueError):
            return text
    return text
