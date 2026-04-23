# GOD MODE Media Library — Zadání pro Claude Code

## Kontext

Toto je konsolidované zadání vycházející ze 4 nezávislých auditů kódové báze.
Projekt: `/Users/marekgaletka/New project/godmode_media_library`
Stav: ~69K řádků Python, ~50 modulů, 1349 testů, coverage ~40-55%.

> **Důležité:** Každou opravu doprovoď odpovídajícím testem. Před úpravou souboru si přečti aktuální kód a ověř, že problém stále existuje — audity mohly být provedeny na starší verzi.

---

## Fáze 1 — CRITICAL: Data loss & Security (priorita: okamžitě)

### 1.1 Path traversal (5+ endpointů)
- `api.py ~1362` — rename: `new_name` nevaliduje `../` ani directory separátory
- `api.py ~1380` — move: `destination` je user-supplied, `mkdir(parents=True)`, žádná kontrola
- `api.py ~974` — preview endpoint: `scan_roots` se nenastavuje v `create_app()`
- `api.py ~3451` — Signal decrypt: destination bez validace
- `api.py ~2763` — PhotoRec: source/output bez validace
- `api.py ~2668-2713` — recovery endpointy: žádná sanitizace
- `api.py:238` — `_sanitize_path` vrací `path_str` místo `stripped`
- **Akce:** Přidat `_check_path_within_roots()` / `resolve() + prefix check` na všechny tyto endpointy. Validovat `new_name` proti `../` a `os.sep`.

### 1.2 Zip/Tar Slip v consolidation.py
- `consolidation.py:1579-1589` — `zipfile.extractall()` a `tarfile.extractall()` bez sanitizace cest
- **Akce:** Na Python 3.12+: `tar.extractall(filter='data')`. Pro zip: validovat každý member přes `resolved.is_relative_to(dest)`. Přidat size limit proti zip bomb.

### 1.3 delete_ops.py — nlink bug a alias unlink po selhání
- `delete_ops.py:412-429` — nlink check po move primary vždy selže → alias unlink se tiše přeskočí
- `delete_ops.py:397-425` — pokud move_primary selže, unlink_alias stále běží → trvalá ztráta dat
- **Akce:** Dekrementovat expected nlink po primary move NEBO porovnávat `current_nlink < expected - already_moved_count`. Trackovat úspěšně přesunuté inody v `moved_inodes: set`.

### 1.4 Quarantine path traversal
- `actions.py:26-39` — `_quarantine_path` chybí `..` kontrola a `.resolve()` containment
- `api.py ~1246` — quarantine root user-controlled bez validace
- **Akce:** Přidat `resolve() + prefix check` (vzor z `delete_ops.py:63-74`).

### 1.5 Neatomické zápisy — scenarios.py
- `scenarios.py:242-260` — `write_text()` není atomický, crash = corrupted JSON, blanket except vrací `[]`
- **Akce:** Atomic write via `tempfile.mkstemp + os.replace()`. File lock pro concurrent access. Změnit except na `json.JSONDecodeError` + logování + `.bak` záloha.

### 1.6 Další kritické bezpečnostní problémy
- `app.py:253` — SHA-256 místo bcrypt pro share passwords → brute-force triviální. **Akce:** Nahradit za `bcrypt` nebo `argon2`.
- `app.py:1205` — WebSocket bez autentizace. **Akce:** Přidat token validaci na WS upgrade.
- `web/app.py:139,148-150` — API token v query parametru → viditelný v logech. **Akce:** Odstranit query parameter podporu nebo ticket/nonce pattern pro WS.
- `web/app.py:274-307` — Žádný rate limit na share password pokusy. **Akce:** Přidat rate limit.
- `web/app.py:322` — Content-Disposition header injection. **Akce:** Escapovat `"`, `\n`, `\r` ve filename.
- `face_crypto.py:36-41` — TOCTOU v key creation (klíč zapsán s default permissions před chmod). **Akce:** Použít `os.open()` s explicitními permissions + `os.fdopen()`.

### 1.7 Concurrent consolidation race
- `checkpoint.py` — `reset_stale_in_progress()` resetuje soubory jiného procesu → duplicitní transfery
- **Akce:** PID-based locking nebo SQLite advisory lock.

### 1.8 Manifest a archiv integrity
- `recovery.py:275-278` — manifest přepsání prázdným dictem při selhání čtení. **Akce:** Guard: neukládat pokud `deleted == 0 and failed > 0`.
- `consolidation.py:1630-1639` — archiv se maže bez verifikace uploadu. **Akce:** Verifikovat hash/size po uploadu, pak teprve mazat.

---

## Fáze 2 — HIGH: Runtime bugy a logické chyby

### 2.1 checkpoint.py
- `:59` — `_tables_cache` keyed by `id(conn)` → recyklace adresy po GC → false positive. **Akce:** Nahradit za weakref-based nebo connection path key.
- `:181` — Job ID `uuid4().hex[:8]` = 32 bitů → birthday collision ~65K jobs. **Akce:** `uuid4().hex[:16]` nebo plný `str(uuid.uuid4())`.
- `:206` — Mutace `conn.row_factory` na sdílené connection. **Akce:** Izolovat row_factory nastavení.

### 2.2 media_score.py
- `:304` — `date_original` parsing nefunguje (scanner ukládá `YYYY:MM:DD`, ale `fromisoformat()` to neparsuje). **Akce:** `datetime.strptime(val, "%Y:%m:%d %H:%M:%S")`.
- `:231` — Camera tier matching příliš volný (`"nikon d8"` matchuje d80, d800, d850). **Akce:** Word-boundary regex.

### 2.3 consolidation.py
- `:134` — `_pause_events` type mismatch: `entry[0].set()` ale entry je `threading.Event`, ne tuple → TypeError. **Akce:** Sjednotit typ.
- `:660` — `getattr(stats, "total_files", 0)` — atribut neexistuje, vždy vrací 0. **Akce:** Opravit na `files_scanned`.
- `:580` — Surrogate hash neodlišitelný od reálného SHA-256 → dedup kolize. **Akce:** Přidat prefix/flag.

### 2.4 scanner.py
- `:199-215` — Chybějící exception handling v media extraction futures — jedna chyba crashne celý scan. **Akce:** `try/except` na `future.result()`.
- `:295-296` — Commit condition mimo upsert loop → nikdy necommitne periodicky. **Akce:** Přesunout do loop.

### 2.5 Další runtime bugy
- `cloud.py:1519` — `Any` není importovaný → `rclone_dedupe()` crashne na `NameError`. **Akce:** `from typing import Any`.
- `face_detect.py:106` — Face encodings stored as NULL když `encrypt_fn` je None → clustering nefunguje. **Akce:** Fallback na raw bytes.
- `actions.py:159-165` — Quarantine přípona: `photo.jpg.dup1` místo `photo_dup1.jpg`. **Akce:** Suffix PŘED příponou.
- `actions.py:310-311` — `promote_from_manifest` hash comparison invertovaný. **Akce:** Opravit logiku.
- `delete_ops.py:86-90` — Unbounded while loop v collision avoidance. **Akce:** Horní limit 100K.
- `report.py:401+` — XSS v HTML reportech (unescaped filenames, camera models). **Akce:** `html.escape()`.
- `quality.py:120,132` — `Pillow getdata()` deprecated, odstraněno v Pillow 14. **Akce:** Nahradit za `get_flattened_data()`.
- `perceptual_hash.py:138-180` — False negatives v similarity search. **Akce:** Generovat 3-bit a 4-bit prefix flips.
- `api.py:3830-3854` — Task status mutations bez lock. **Akce:** Přidat `_tasks_lock`.
- `catalog.py:786-788` — LIKE bez ESCAPE → root path s `_` nebo `%` matchuje neočekávané cesty. **Akce:** Přidat ESCAPE clause.
- `recovery.py:1581-1604` — Signal HMAC extrahován ale nikdy verifikován. **Akce:** `hmac.compare_digest()` před dekrypcí.
- `recovery.py:1817-1836` — JPEG repair: při výjimce originál přepsán, backup neobnoven. **Akce:** try/except + restore backup.
- `pipeline.py:143-150` — `auto_merge` flag ignorován → metadata writes bez souhlasu. **Akce:** Přidat guard.

---

## Fáze 3 — Výkon a paměť

### 3.1 N+1 query problémy (scanner.py)
- `:95` — `get_file_mtime_size()` per soubor → milion queries. **Akce:** Batch load na začátku scanu.
- `:139-162` — `get_file_by_path()` individuálně pro každý nezměněný soubor. **Akce:** Batch query.
- `:299-302` — `all_paths()` načte VŠECHNY cesty do paměti. **Akce:** Cursor iterator.

### 3.2 Face detection N+1
- `face_detect.py:233-247` — individuální SELECT per face_id v `cluster_faces()`. **Akce:** Batch query.
- `face_detect.py:314-339` — O(n) individuální DB queries v `match_face_to_known()`. **Akce:** Batch.
- `face_crypto.py:50-60` — Fernet instance čte key z disku při každém volání. **Akce:** Cache instance.

### 3.3 Memory a unbounded growth
- `api.py:497-498` — až 100K řádků do paměti při file listing s filtry. **Akce:** SQL-based filtering + pagination.
- `api.py ~263` — `_reorganize_plans` bez eviction. **Akce:** Přidat eviction / TTL.
- `app.py:26-51` — Rate limiting dict bez eviction a bez thread safety. **Akce:** `asyncio.Lock` + eviction.
- `cloud.py ~203` — `_oauth_processes` leak — Popen objekty se nikdy nečistí. **Akce:** Cleanup.
- `bitrot.py:88` — `fetchall()` pro všechny soubory. **Akce:** Cursor iterator.
- `media_score.py:396-427` — Celý katalog do paměti pro scoring. **Akce:** SQL ORDER BY + LIMIT.

### 3.4 Algoritmická složitost
- `tree_ops.py:146` — O(N²) reserved set rebuild při každém `_allocate_destination`. **Akce:** Akumulovat set.
- `tree_ops.py:298` — Collision tracking reset na prázdný set. **Akce:** Persistovat across calls.
- `consolidation.py:848-861` — `dest_paths_used` full rebuild při každém resume. **Akce:** Incremental.
- `consolidation.py:1017-1039` — 2 separate queries pro stejný řádek. **Akce:** Merge do 1.

---

## Fáze 4 — Robustnost a konzistence

- `metadata_merge.py:314-315` — Group prefix strip → EXIF a XMP tags se oba zapíší jako `-DateTimeOriginal=`. **Akce:** Zachovat group prefix.
- `metadata_merge.py:327` — List join se ztrátou struktury. **Akce:** Správná serializace.
- `asset_sets.py:7` / `perceptual_hash.py` — Nekonzistentní IMAGE_EXTS (RAW formáty chybí v phash). **Akce:** Sjednotit.
- `exiftool_extract.py:99` — Paths s leading dash jako ExifTool flags. **Akce:** Přidat `--` separator.
- `backup_monitor.py:349` — AppleScript injection (incomplete escaping). **Akce:** `subprocess.run()` s oddělenými argumenty, nebo pyobjc.
- `catalog.py:310` — `fcntl` import → Windows crash. **Akce:** Podmíněný import.
- `catalog.py:1945-1950` — `_date_to_timestamp` naive datetime. **Akce:** Explicitně UTC.

---

## Fáze 5 — Frontend opravy

- `web/static/js/modal.js:130-136` — Favorite toggle: `api()` ignoruje options → vždy GET místo POST. **Akce:** Použít `apiPost()`.
- `js/main.js:519-526` — Lightbox klávesy 1-5 navigují pryč. **Akce:** `if (e.defaultPrevented) return;`.
- `js/pages/scenarios.js:60` — `_esc()` ReferenceError. **Akce:** Import `escapeHtml` z utils.js.
- `js/api.js:38` — `res.json()` na 204 No Content → parse error. **Akce:** `return res.status === 204 ? null : res.json()`.
- `js/tasks.js:138` — WebSocket `JSON.parse` bez try/catch. **Akce:** Wrap.
- `js/pages/cloud.js:354-361` — expand/collapse šipky invertované. **Akce:** Invertovat logiku.
- `js/main.js:51-54` — Event listener leak across pages. **Akce:** Přidat `cleanup()` exporty.
- `js/pages/similar.js:121-137` — Single pair resolve bez confirm dialogu. **Akce:** Přidat `confirm()`.

---

## Fáze 6 — CLI UX a validace

- Přidat confirmation prompt na: `apply`, `tree-apply --operation move`, `batch-rename`, `metadata-write`, `promote`.
- Přidat `--dry-run` na `metadata-write`.
- Změnit default `tree-apply --operation` z `move` na `hardlink`.
- Přidat `--version` flag.
- Validace parametrů: `--min-size-kb ≥ 0`, `--port 1-65535`, `--limit ≥ 1`, `--workers ≥ 1`.
- Config: range validace na numerické fieldy, Linux forbidden roots (`/home`, `/root`), TOML error handling.
- Konzistentní exit codes: 0=ok, 1=error, 2=user error.
- Konzistentní output formát: přidat `--output-format` (JSON/TSV).

---

## Fáze 7 — Test coverage → 70%+

- Testy pro path traversal fixes (rename, move, quarantine, Signal, PhotoRec, recovery).
- Testy pro delete_ops nlink bug (regresní).
- Testy pro 14+ nepokrytých CLI subcommands (scan, query, stats, vacuum, similar, verify, export, auto, serve, watch, cloud, batch-rename, metadata-*).
- Scanner testy: permission errors, symlink loops, TOCTOU, unicode filenames, multi-worker.
- Consolidation pipeline testy s reálnými soubory (ne mocked).
- media_score testy: date parsing, camera tier matching.
- Catalog concurrent write testy.
- Full lifecycle E2E: scan → dedup → plan → delete → verify s reálnými soubory.
- API endpointy: ~30% nepokrytých (duplicates merge, similar, faces, quality, timeline).
- Zvýšit CI coverage threshold na 70%.

---

## Fáze 8 — Cleanup a infrastruktura

### Lint a dead code
- Opravit 9 ruff chyb (3× I001, 2× B904, 2× UP038, 1× E501, 1× SIM105).
- Odstranit dead code: `PlanPolicy.require_metadata_merge`, duplicitní `/preview/` route, `_is_media_file()` v consolidation.
- `perceptual_hash.py:68-70` — `register_heif_opener()` při každém `dhash()` → jednou na module level.

### Dockerfile
- Přidat non-root user.
- Přidat HEALTHCHECK (bez auth).
- Pinovat závislosti (requirements.lock).
- Optimalizovat layer caching (COPY pyproject.toml first).
- Přidat tzdata package.

### Další
- Přidat `mypy` a `pip-audit` do CI.
- `build/` do .gitignore, smazat staré artefakty.
- `consolidation.py:1519` — `tar.extractall(filter="data")`.
- Rate limit dict per-IP list cap.
- CSRF ochrana (Origin header check) když `GML_API_TOKEN` není nastaven.

---

## Fáze 9 — Dlouhodobé (měsíc+)

- Rozdělit `web/api.py` (5075 řádků) na menší routery.
- Zvýšit PBKDF2 iterace z 100 000 na 600 000+ (OWASP 2023).
- Přidat Fernet key rotation a versioning (face_crypto.py).
- SQLite schema: UNIQUE constraint na duplicates, index na labels, přejít na `sqlite3.Row`.
- Connection pooling pro SQLite ve web kontextu.
- SHA-1[:16] → SHA-256[:24] v unit_id a inode_id (tree_ops.py:70, delete_ops.py:47).
- Windows podpora: podmíněný fcntl, testování na Windows CI.
- Range request support pro video streaming (api.py:1921-1969).

---

## Prioritizace a odhad

| Pořadí | Fáze | Odhad |
|--------|------|-------|
| 1 | Fáze 1 — Critical security & data loss | 2-3 dny |
| 2 | Fáze 2 — Runtime bugy | 1-2 dny |
| 3 | Fáze 3 — Výkon | 1-2 dny |
| 4 | Fáze 4 — Robustnost | 1 den |
| 5 | Fáze 5 — Frontend | 1 den |
| 6 | Fáze 7 — Testy | 2-3 dny |
| 7 | Fáze 6 — CLI UX | 1 den |
| 8 | Fáze 8 — Cleanup | 0.5 dne |
| 9 | Fáze 9 — Dlouhodobé | průběžně |

**Celkem: ~10-14 dní práce. Fáze 1-2 jsou blokující před nasazením na reálná data.**

## Pravidla pro implementaci

1. **Ověř aktuální stav** — před každou opravou přečti příslušný soubor a ověř, že problém existuje.
2. **Každá oprava = test** — ke každé opravě přidej regresní test.
3. **Atomické commity** — jeden commit = jeden logický fix.
4. **Spouštěj testy** — po každé změně spusť `pytest` a ověř, že nic nerozbíjíš.
5. **Neměň chování** — pokud není explicitně řečeno, zachovej stávající API kontrakt.
