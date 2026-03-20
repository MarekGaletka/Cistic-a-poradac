/* GOD MODE Media Library — Czech i18n */

const translations = {
  cs: {
    // Navigation
    "nav.dashboard": "Přehled",
    "nav.files": "Soubory",
    "nav.duplicates": "Duplicity",
    "nav.similar": "Podobné",
    "nav.timeline": "Časová osa",
    "nav.map": "Mapa",
    "nav.pipeline": "Pipeline",
    "nav.doctor": "Diagnostika",
    "nav.settings": "Nastavení",

    // Sidebar groups
    "nav.group.library": "KNIHOVNA",
    "nav.group.duplicates": "DUPLICITY",
    "nav.group.tools": "NÁSTROJE",

    // Dashboard
    "dashboard.title": "Přehled",
    "dashboard.total_files": "Celkem souborů",
    "dashboard.total_size": "Celková velikost",
    "dashboard.hashed": "Zahashováno",
    "dashboard.duplicate_groups": "Skupin duplicit",
    "dashboard.duplicate_files": "Souborů duplicit",
    "dashboard.gps_files": "Souborů s GPS",
    "dashboard.media_probed": "Zpracováno médií",
    "dashboard.labeled": "Označeno",
    "dashboard.top_extensions": "Nejčastější přípony",
    "dashboard.top_cameras": "Nejčastější fotoaparáty",
    "dashboard.extension": "Přípona",
    "dashboard.count": "Počet",
    "dashboard.camera": "Fotoaparát",
    "dashboard.empty_title": "Zatím nemáte žádná data",
    "dashboard.empty_hint": "Začněte skenováním složky s vašimi médii.",
    "dashboard.empty_scan_btn": "Naskenovat složku",
    "dashboard.scan_path_placeholder": "/Users/me/Photos",
    "dashboard.quick_actions": "Rychlé akce",
    "dashboard.view_duplicates": "Zkontrolovat duplicity",
    "dashboard.view_files": "Procházet soubory",
    "dashboard.refresh": "Aktualizovat",
    "dashboard.scan_folder": "Naskenovat složku",
    "dashboard.last_scan": "Poslední sken",
    "dashboard.no_scan_yet": "Zatím neprovedeno",
    "dashboard.managed_folders": "Spravované složky",
    "dashboard.storage_breakdown": "Rozložení úložiště",
    "dashboard.images": "Obrázky",
    "dashboard.videos": "Videa",
    "dashboard.other": "Ostatní",
    "dashboard.last_scan_info": "Poslední sken: {date}",
    "dashboard.no_folders": "Žádné složky nenastaveny",
    "dashboard.open_settings": "Otevřít nastavení",

    // Files
    "files.title": "Soubory",
    "files.ext_placeholder": "Přípona (jpg)",
    "files.camera_placeholder": "Fotoaparát",
    "files.path_placeholder": "Cesta obsahuje...",
    "files.date_from": "Datum od",
    "files.date_to": "Datum do",
    "files.min_size": "Velikost od (KB)",
    "files.max_size": "Velikost do (KB)",
    "files.has_gps": "Má GPS",
    "files.has_phash": "Má PHash",
    "files.search": "Hledat",
    "files.name": "Název",
    "files.ext": "Přípona",
    "files.size": "Velikost",
    "files.camera": "Fotoaparát",
    "files.date": "Datum",
    "files.gps": "GPS",
    "files.resolution": "Rozlišení",
    "files.empty_title": "Žádné soubory neodpovídají filtrům",
    "files.empty_hint": "Zkuste rozšířit hledání nebo zrušit některé filtry.",
    "files.previous": "Předchozí",
    "files.next": "Další",
    "files.showing": "Zobrazeno {from}–{to} (stránka {page})",
    "files.selection_mode": "Režim výběru",
    "files.exit_selection": "Zrušit výběr",
    "files.filters_toggle": "Filtry",

    // File detail
    "detail.title": "Detail souboru",
    "detail.size": "Velikost",
    "detail.extension": "Přípona",
    "detail.date": "Datum",
    "detail.camera": "Fotoaparát",
    "detail.resolution": "Rozlišení",
    "detail.duration": "Délka",
    "detail.video": "Video",
    "detail.audio": "Audio",
    "detail.sha256": "SHA-256",
    "detail.phash": "PHash",
    "detail.quality_score": "Skóre kvality",
    "detail.metadata_tags": "ExifTool Metadata ({count} tagů)",
    "detail.loading": "Načítání...",
    "detail.error": "Chyba při načítání detailu souboru: {message}",

    // Duplicates
    "duplicates.title": "Duplicity",
    "duplicates.groups": "{count} skupin",
    "duplicates.group": "Skupina",
    "duplicates.files": "Soubory",
    "duplicates.size": "Velikost",
    "duplicates.action": "Akce",
    "duplicates.diff": "Porovnat",
    "duplicates.keep": "Ponechat",
    "duplicates.resolve": "Vyřešit",
    "duplicates.resolve_all": "Vyřešit vše",
    "duplicates.resolve_tooltip": "Ponechá nejlepší soubor a ostatní přesune do karantény",
    "duplicates.resolving": "Řeším...",
    "duplicates.resolved": "Vyřešeno",
    "duplicates.merge_quarantine": "Sloučit a karanténovat",
    "duplicates.match": "Shoda",
    "duplicates.partial_match": "Částečná shoda",
    "duplicates.conflicts": "Konflikty",
    "duplicates.unanimous": "Shodné ({count} tagů)",
    "duplicates.partial": "Částečné ({count} tagů — kandidáti na sloučení)",
    "duplicates.conflicts_tags": "Konflikty ({count} tagů)",
    "duplicates.metadata_diff": "Metadata Diff — {id}",
    "duplicates.visual_compare": "Porovnat vizuálně",
    "duplicates.empty_title": "Žádné duplicity nenalezeny",
    "duplicates.empty_hint": "Vaše knihovna neobsahuje duplicitní soubory. Skvělé!",
    "duplicates.best_file": "Nejlepší",
    "duplicates.will_quarantine": "Do karantény",
    "duplicates.wasted_space": "Plýtvání místem",
    "duplicates.potential_savings": "Úspora místa",
    "duplicates.groups_remaining": "Zbývajících skupin",

    // Similar
    "similar.title": "Podobné obrázky",
    "similar.pairs": "{count} párů",
    "similar.distance": "Vzdálenost: {value}",
    "similar.compare": "Porovnat",
    "similar.empty_title": "Žádné podobné páry nenalezeny",
    "similar.empty_hint": "Zkuste zvýšit práh pro volnější porovnávání.",
    "similar.threshold": "Práh podobnosti",
    "similar.threshold_hint": "Nižší = přísnější shoda",
    "similar.almost_identical": "Téměř identické",
    "similar.very_similar": "Velmi podobné",
    "similar.somewhat_similar": "Podobné",
    "similar.total_pairs": "{count} nalezených párů",
    "similar.resolve_pair": "Vyřešit",
    "similar.resolve_all_pairs": "Vyřešit všechny páry",

    // Timeline
    "timeline.title": "Časová osa",
    "timeline.dated_files": "{count} souborů s datem",
    "timeline.more": "+{count} dalších",
    "timeline.empty_title": "Žádné soubory s datem nenalezeny",
    "timeline.empty_hint": "Soubory potřebují metadata date_original (z EXIF nebo ExifTool extrakce).",
    "timeline.show_all": "Zobrazit vše ({count})",
    "timeline.collapse": "Sbalit",
    "timeline.year": "Rok {year}",
    "timeline.no_date": "Bez data",

    // Map
    "map.title": "Mapa",
    "map.details": "Detaily",
    "map.empty_title": "Žádné geotagované soubory nenalezeny",
    "map.empty_hint": "Soubory potřebují GPS metadata z EXIF. Spusťte ExifTool extrakci pro vyplnění GPS dat.",
    "map.leaflet_error": "Leaflet.js není načtený. Zkontrolujte připojení k internetu.",
    "map.files_on_map": "{count} souborů na mapě",
    "map.cluster_files": "{count} souborů",
    "map.photo": "Fotografie",
    "map.video": "Video",

    // Pipeline / Settings
    "pipeline.title": "Pipeline",
    "pipeline.description": "Spustit celou pipeline: sken → extrakce metadat → diff → sloučení",
    "pipeline.roots": "Kořenové složky (jedna na řádek)",
    "pipeline.roots_placeholder": "/Users/me/Photos\n/Volumes/External/Backup",
    "pipeline.workers": "Počet workerů",
    "pipeline.exiftool": "Extrahovat ExifTool",
    "pipeline.start_pipeline": "Spustit pipeline",
    "pipeline.scan_only": "Spustit sken",
    "pipeline.started": "Pipeline spuštěna",
    "pipeline.scan_started": "Sken spuštěn",
    "pipeline.start_failed": "Nepodařilo se spustit pipeline: {message}",
    "pipeline.scan_failed": "Nepodařilo se spustit sken: {message}",

    // Settings
    "settings.title": "Nastavení",
    "settings.pipeline_section": "Pipeline a skenování",
    "settings.doctor_section": "Kontrola závislostí",
    "settings.about_section": "O aplikaci",
    "settings.about_text": "GOD MODE Media Library — pokročilý správce médií s důrazem na metadata, deduplikaci a bezpečnost.",

    // Tasks
    "task.connecting": "Úloha {id}: připojování...",
    "task.running": "Úloha {id}: běží... (zahájeno {started})",
    "task.completed": "Úloha {id}: dokončeno",
    "task.failed": "Úloha {id}: selhalo — {error}",
    "task.completed_toast": "Úloha dokončena úspěšně",
    "task.failed_toast": "Úloha selhala: {error}",
    "task.lost_connection": "Ztraceno spojení po {count} pokusech: {message}",
    "task.status.running": "Probíhá",
    "task.status.completed": "Dokončeno",
    "task.status.failed": "Selhalo",
    "task.status.pending": "Čeká",
    "task.drawer_title": "Úlohy",

    // Doctor
    "doctor.title": "Kontrola závislostí",
    "doctor.dependency": "Závislost",
    "doctor.available": "Dostupné",
    "doctor.missing": "Nedostupné",
    "doctor.install": "Nainstalovat",
    "doctor.system_info": "Systémové informace",
    "doctor.catalog_path": "Cesta ke katalogu",
    "doctor.catalog_size": "Velikost katalogu",
    "doctor.quarantine_size": "Velikost karantény",
    "doctor.python_version": "Python",
    "doctor.platform": "Platforma",
    "doctor.total_files": "Celkem souborů",
    "doctor.total_size": "Celková velikost",
    "doctor.install_command": "Příkaz k instalaci",
    "doctor.copy_command": "Kopírovat příkaz",
    "doctor.all_ok": "Vše v pořádku",
    "doctor.issues_found": "{count} problémů nalezeno",

    // Actions
    "action.quarantine": "Přesunout do karantény",
    "action.merge_metadata": "Sloučit metadata",
    "action.visual_compare": "Porovnat vizuálně",
    "action.delete": "Smazat",
    "action.rename": "Přejmenovat",
    "action.move": "Přesunout",
    "action.tag": "Tagovat",
    "action.select_all": "Vybrat vše",
    "action.deselect_all": "Zrušit výběr",

    // Confirmations
    "confirm.quarantine": "Opravdu přesunout {count} souborů do karantény?",
    "confirm.delete": "Opravdu smazat?",
    "confirm.resolve_all": "Opravdu vyřešit všechny skupiny duplicit najednou? U každé skupiny bude ponechán nejlepší soubor a ostatní budou přesunuty do karantény.",

    // Visual diff
    "vdiff.side_by_side": "Vedle sebe",
    "vdiff.slider": "Posuvník",
    "vdiff.overlay": "Překryv",

    // Lightbox
    "lightbox.info": "Informace",
    "lightbox.close": "Zavřít",
    "lightbox.zoom_in": "Přiblížit",
    "lightbox.zoom_out": "Oddálit",
    "lightbox.counter": "{current} / {total}",
    "lightbox.no_preview": "Náhled není k dispozici",
    "lightbox.video": "Video",
    "lightbox.download": "Stáhnout",

    // Folder picker
    "folder.picker_title": "Vyberte složku",
    "folder.select_this": "Vybrat tuto složku",
    "folder.selected_folders": "Vybrané složky",
    "folder.add_folders": "Přidat složky",
    "folder.add_folder": "Přidat složku",
    "folder.quick_add": "Rychle přidat",
    "folder.no_folders": "Zatím žádné složky",
    "folder.start_scan": "Spustit skenování",
    "folder.bookmark_desktop": "Plocha",
    "folder.bookmark_pictures": "Obrázky",
    "folder.bookmark_documents": "Dokumenty",
    "folder.bookmark_downloads": "Stažené",
    "folder.bookmark_home": "Domovská složka",
    "folder.empty_folder": "Prázdná složka",
    "folder.items": "{count} položek",
    "folder.remove": "Odebrat",
    "folder.browse": "Procházet...",
    "dashboard.empty_hint_v2": "Začněte přidáním složek s vašimi médii.",
    "dashboard.or_quick_add": "Nebo rychle přidejte:",
    "pipeline.configured_roots": "Nastavené složky",
    "pipeline.no_roots": "Žádné složky nejsou nastaveny",

    // File sorting
    "files.sort_by": "Řadit podle",
    "files.sort_name": "Název",
    "files.sort_date": "Datum",
    "files.sort_size": "Velikost",
    "files.sort_ext": "Přípona",
    "files.sort_asc": "Vzestupně",
    "files.sort_desc": "Sestupně",

    // Keyboard shortcuts
    "shortcuts.title": "Klávesové zkratky",
    "shortcuts.navigate": "1\u20136: Navigace mezi str\u00e1nkami",
    "shortcuts.search": "/: Hledat",
    "shortcuts.close": "Esc: Zav\u0159\u00edt",
    "shortcuts.help": "?: N\u00e1pov\u011bda",

    // General
    "general.loading": "Načítání...",
    "general.no_results": "Žádné výsledky",
    "general.close": "Zavřít",
    "general.confirm": "Potvrdit",
    "general.cancel": "Zrušit",
    "general.previous": "Předchozí",
    "general.next": "Další",
    "general.show_more": "Zobrazit více",
    "general.error": "Chyba: {message}",
    "general.selected": "{count} vybráno",
  },
};

let currentLang = "cs";

/**
 * Translate a key, optionally interpolating {param} placeholders.
 * @param {string} key - Translation key like "nav.dashboard"
 * @param {Object} [params] - Interpolation params like { count: 5 }
 * @returns {string}
 */
export function t(key, params) {
  const dict = translations[currentLang] || translations.cs;
  let text = dict[key];
  if (text === undefined) {
    // Fallback: return the key itself
    return key;
  }
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      text = text.replaceAll(`{${k}}`, String(v));
    }
  }
  return text;
}

export function setLang(lang) {
  if (translations[lang]) currentLang = lang;
}

export function getLang() {
  return currentLang;
}
