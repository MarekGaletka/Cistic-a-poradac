# Ultimátní konsolidace 🌍🚀 — Krokový scénář

> **Sdílený dokument**: Uprav cokoliv potřebuješ, já si jej přečtu a zapracuji změny.
> Poslední aktualizace: 2026-03-27 v4

---

## Jak to vypadá v aplikaci

Scénář je dostupný v GOD MODE na stránce **Scénáře → Ultimátní konsolidace 🌍🚀**.

**Každá operace má v UI:**
- 🟢 Zelená pulzující tečka = právě běží
- 🔵 Modrý progress bar = kolik % hotovo + počet souborů + rychlost + ETA
- ⏱️ Watchdog = pokud se 60s nic nepřenese, zobrazí varování "Možná zaseklé — zkontroluj připojení"
- ✅ Zelená fajfka = hotovo
- ❌ Červený křížek = chyba (s popisem)
- 📊 Živé statistiky = přeneseno X/Y souborů, X GB, rychlost X MB/s

**Celý scénář je jeden velký wizard** — provede tě krok po kroku. Nemusíš nic spouštět ručně z terminálu. Vše přes tlačítka v aplikaci.

---

## FÁZE A: Sběr dat ze VŠECH zdrojů → Google 6TB

> ⚠️ **Důležité**: Ve fázi A se **neprovádí deduplikace**. Všechna data ze všech zdrojů se nejdřív bezpečně zkopírují na Google 6TB. Deduplikace přijde až ve fázi B, až budou pohromadě VŠECHNA data.

### Krok 1 — Připojení a kontrola zdrojů
Před spuštěním pipeline **automaticky zkontroluje** všechny dostupné zdroje:
- ✅ Zobrazí seznam VŠECH nakonfigurovaných cloud remotes (pCloud, GDrive, Mega, Dropbox...)
- ✅ U každého ukáže 🟢 dostupný / 🔴 nedostupný
- ⚠️ Pokud nějaký zdroj chybí nebo je offline, zobrazí varování: "Zdroj XY není dostupný — chceš pokračovat bez něj?"
- 📋 Checklist: "Máš připojené všechny zdroje? Nic nechybí?"
- **Ty potvrdíš**, že je vše připojené, a klikneš "Pokračovat"

### Krok 2 — Katalogizace všech cloud zdrojů
Pipeline prochází každý cloud remote a zapisuje si metadata o každém souboru:
- 📂 Prochází pCloud... (1 247 souborů) ✅
- 📂 Prochází Google Drive... (3 891 souborů) ✅
- 📂 Prochází Mega... (2 105 souborů) ✅
- 📂 Prochází Dropbox... (856 souborů) ✅
- **Výsledek**: katalog všech cloud souborů v databázi
- **V UI vidíš**: progress bar pro každý remote, celkový počet souborů, živý stav

### Krok 3 — Přenos cloud → Google 6TB
Všechny soubory ze všech cloudů se zkopírují na `gws-backup:GML-Consolidated`:
- **BEZ deduplikace** — všechno se přenese, i duplicity (deduplikace bude až ve fázi B)
- Soubory, které už na cíli existují (z předchozího běhu), se přeskočí
- Každý přenesený soubor se okamžitě ověří (velikost + hash)
- **V UI vidíš**: aktuálně přenášený soubor, rychlost, ETA, progress X/Y souborů
- **Watchdog**: pokud se 60s nic nepřenese → varování
- **Google limit**: max 750 GB/den upload — pipeline automaticky pausne a pokračuje další den

### Krok 4 — Lokální složky z Macu (volitelné, můžeš přidat kdykoliv)
Přidáš cesty do "Lokální složky" v aplikaci a klikneš "Pokračovat":
- Zkatalogizuje lokální soubory
- Nahraje VŠECHNY na Google 6TB (bez deduplikace — ta přijde později)
- Soubory co už na cíli existují se přeskočí
- Ověří integritu
- **V UI vidíš**: stejné metriky jako v kroku 3

### Krok 5 — iPhone přes USB (volitelné, můžeš přidat kdykoliv)
Připojíš iPhone, v aplikaci přidáš `/Volumes/iPhone/DCIM` a klikneš "Pokračovat":
- Zkatalogizuje fotky/videa z iPhonu
- Nahraje na Google 6TB
- Ověří
- **V UI vidíš**: stejné metriky

### Krok 6 — 4TB externí disk přes USB (volitelné, můžeš přidat kdykoliv)
Připojíš 4TB disk, přidáš jeho cestu a klikneš "Pokračovat":
- Zkatalogizuje soubory z 4TB disku
- Nahraje na Google 6TB
- Ověří integritu
- **V UI vidíš**: stejné metriky

### Krok 7 — Další zdroje? (volitelné)
Pokud máš cokoliv dalšího (USB flashky, staré disky, SD karty...), opakuješ stejný postup — přidáš cestu, klikneš "Pokračovat", pipeline přenese jen to co chybí.

---

## FÁZE B: Deduplikace a verifikace na Google 6TB

> Teď jsou na Google 6TB VŠECHNA data ze VŠECH zdrojů (včetně duplicit). Čas uklidit.

### Krok 8 — Rozbalení archivů
Před deduplikací pipeline zpracuje všechny archivy (.zip, .rar, .7z, .tar.gz):
- Stáhne archiv do temp složky na Macu
- Rozbalí obsah
- Obsah projde stejným procesem (upload na Google 6TB)
- Samotný archiv se na cíli **smaže** (jeho obsah už máme rozbalený)
- Pokud obsah archivu je duplicitní s již existujícími soubory → chytne to deduplikace v kroku 9
- **V UI vidíš**: které archivy se rozbalují, kolik souborů v nich bylo, kolik je nových

### Krok 9 — Finální deduplikace nad VŠEMI daty
Pipeline spustí deduplikaci přímo na Google 6TB:
- Najde duplicity podle přesných MD5 hashů (porovnává VŠECHNA data dohromady — cloudy + Mac + iPhone + disk + rozbalené archivy)
- Zachová "nejbohatší" verzi (nejvíc metadat / největší / nejnovější)
- Smaže duplicitní kopie
- **V UI vidíš**: počet nalezených skupin duplicit, kolik se odstraní, kolik místa se uvolní
- **Výsledek**: na Google 6TB jen unikátní soubory

> **Google limity**: deduplikace probíhá server-side (Google maže duplicity sám přes API). Limit je ~10 000 000 API calls/den — pro 100k+ souborů naprosto v pohodě. Nepotřebuje stahovat/nahrávat data.

### Krok 10 — Verifikace integrity
Pipeline ověří 100% souborů na cíli:
- Kontrola velikosti + hash každého souboru
- Report o chybách (pokud nějaké)
- Pokud něco selže → automatický retry
- **V UI vidíš**: progress bar ověřování, počet OK / chyba
- **Výsledek**: 100% jistota, že všechna data jsou neporušená

### Krok 11 — Finální report
V aplikaci se zobrazí přehledný report:
- 📊 Celkem souborů na Google 6TB
- 🗑️ Kolik duplicit odstraněno + kolik místa uvolněno
- 📦 Kolik archivů rozbaleno + kolik souborů z nich bylo nových
- ❌ Kolik přenosů selhalo (a proč) — s možností retry
- ✅ Kolik souborů ověřeno
- 📁 Rozložení: kolik fotek, videí, dokumentů, software...
- **Ty zkontoluješ a potvrdíš**: "Vše OK, pokračovat"

---

## FÁZE C: Organizace na Google 6TB

### Krok 12 — Roztřídění do složek podle kategorie a datumu
Soubory se přesunou do struktury **nejdřív kategorie, pak rok/měsíc**:

```
GML-Consolidated/
├── Media/
│   ├── 2020/
│   │   ├── 01/
│   │   ├── 02/
│   │   └── ...
│   ├── 2021/
│   └── ...
├── Documents/
│   ├── 2020/
│   ├── 2021/
│   └── ...
├── Software/
│   ├── macOS/
│   ├── Windows/
│   └── Other/
└── Other/
    └── ...
```

**Kategorie:**

- **Media**: fotky (.jpg, .png, .heic, .raw...) + videa (.mp4, .mov, .avi...)
- **Documents**: dokumenty (.pdf, .docx, .xlsx, .txt, .pptx...)
- **Software**: instalačky a aplikační balíky — zachovají svou celistvost:
  - macOS: .app, .dmg, .pkg
  - Windows: .exe, .msi
  - Kontejnery: .iso, .img
  - **Složkové formáty** (.app bundle, .xcodeproj, Photoshop projekty): přenesou se jako celek, nikdy se neroztrhají do jiných kategorií
- ~~Archives~~: **kategorie neexistuje** — archivy byly rozbaleny v kroku 8
- **Other**: vše ostatní co se neřadí jinam
- **V UI vidíš**: progress přesouvání, kolik souborů v jaké kategorii

> **Pravidlo celistvosti**: Pipeline rozpozná "komplexní soubory" — složky, které tvoří jeden logický celek (např. `.app` bundle je složka se stovkami souborů uvnitř). Tyto se VŽDY přenesou a třídí jako jeden objekt. Nikdy se nerozloží na jednotlivé soubory.

---

## FÁZE D: Vyčištění starých úložišť (ručně, ty sám)

### Krok 13 — Manuální vyčištění
Až máš 100% jistotu, že vše je na Google 6TB:
- Vyčistíš pCloud, Mega, Dropbox, starý Google Drive
- Vyčistíš Mac
- Vyčistíš iPhone
- Zformátuješ 4TB disk

---

## FÁZE E: Kopie na čistý 4TB disk

### Krok 14 — Sync z Google 6TB → 4TB disk
V aplikaci klikneš na tlačítko **"📥 Stáhnout na disk"**:
- Připojíš prázdný zformátovaný 4TB disk
- V aplikaci vybereš cílový disk z rozbalovacího menu
- Klikneš "Spustit stahování"
- **V UI vidíš**: progress bar, rychlost, ETA, počet souborů
- Ověří integritu po stažení
- **Výsledek**: identická kopie na 4TB disku = offline záloha

---

## FÁZE F: Průběžná údržba (po dokončení konsolidace)

> Tohle řeší dlouhodobé udržování dvou identických kopií a katalog.

### Jak funguje katalog

Katalog = databáze v GOD MODE, kde vidíš **všechny soubory** s jejich vlastnostmi:
- Název, velikost, typ, datum vytvoření
- Kde fyzicky leží (Google 6TB / 4TB disk / obojí)
- Hash (otisk pro ověření identity)
- Kategorie (Media/Documents/Software/Other)
- Náhled (u fotek a videí)

**Katalog se aktualizuje automaticky** při každé synchronizaci. Nemusíš ho ručně obnovovat.

V aplikaci v katalogu můžeš:
- Procházet soubory podle kategorií, datumu, typu
- Hledat soubory podle názvu
- Vidět kde každý soubor leží
- Prohlížet náhledy fotek/videí

### Model: Google 6TB = MASTER, 4TB disk = ZÁLOHA

```
Google 6TB (gws-backup)    ←── MASTER (sem přidáváš, mažeš, měníš)
        │
        │  "🔄 Synchronizovat"  (tlačítko v aplikaci)
        ▼
4TB disk                   ←── ZÁLOHA (pasivní kopie, automaticky se přizpůsobí)
```

**Prakticky:**
- **Přidáš soubor?** → Nahraješ na Google 6TB (přímo nebo přes GOD MODE). Při příští synchronizaci se zkopíruje na 4TB disk.
- **Smažeš soubor?** → Smažeš na Google 6TB. Při příští synchronizaci se smaže i z 4TB disku.
- **Nová fotka z iPhone?** → Nahraje se na Google 6TB (přes GOD MODE nebo iCloud sync). Při synchronizaci se objeví na disku.

### Tlačítko "🔄 Synchronizovat disk"
Dostupné na stránce Scénáře. Po kliknutí:
1. Porovná obsah Google 6TB a 4TB disku
2. Stáhne nové soubory z Google → disk
3. Smaže soubory z disku, které už na Google nejsou
4. Ověří integritu
5. **V UI vidíš**: kolik přibylo, kolik smazáno, progress
6. **Výsledek**: obě kopie jsou opět identické

### Jak často synchronizovat?
- **Doporučení**: 1× týdně nebo po každé větší změně
- **Není nutné** synchronizovat po každé jedné fotce — klidně nech nahromadit změny
- Můžeš si nastavit **připomínku** v aplikaci ("Připoj disk a synchronizuj")

---

## Shrnutí toku dat

```
ZDROJE (pouze čtení)        CÍL (jediný zápis)        ZÁLOHA (až nakonec)
────────────────────         ──────────────────         ───────────────────
pCloud             ─┐
Google Drive       ─┤
Mega               ─┤
Dropbox            ─┼──→  Google 6TB (gws-backup)  ──→  4TB disk
Mac složky         ─┤     GML-Consolidated              (po vyčištění
iPhone (USB)       ─┤     Kategorie/Rok/Měsíc           a formátování)
4TB disk (USB)     ─┘
                              │                              │
                              └──── 🔄 Synchronizovat ───────┘
                              (po konsolidaci: průběžná údržba)
```

---

## Přerušení a pokračování

**Scénář**: Spustíš v práci → odejdeš → doma pokračuješ

| Situace | Co se stane |
|---------|-------------|
| Zavřeš víko Macu / uspíš | Pipeline se automaticky pausne, každý soubor je checkpointovaný |
| Otevřeš Mac doma | Otevřeš GOD MODE → klikneš ▶️ Pokračovat → pokračuje přesně kde skončil |
| Vypadne internet | Pipeline čeká na reconnect (watchdog hlídá), automaticky pokračuje |
| Chceš přerušit ručně | Klikneš ⏸️ Pozastavit → pipeline bezpečně dokončí aktuální soubor a pausne |
| Chceš přidat nový zdroj | Přidáš cestu/remote → klikneš "Pokračovat" → zpracuje jen nové soubory |
| Google 750GB/den limit | Pipeline automaticky pausne, zobrazí "Denní limit — pokračuje zítra", další den sám pokračuje |

---

## Co se stane s novými / zmizivšími soubory?

Pipeline je **blbuvzdorná**:

| Situace | Chování |
|---------|---------|
| **Nový soubor** (fotka v telefonu, soubor v cloudu) | Pipeline ho **ignoruje** — pracuje jen s katalogem z počátku. Nový soubor se zpracuje při příštím spuštění. |
| **Zmizí soubor** (smazaný z cloudu během přenosu) | Pipeline zapíše **chybu** u daného souboru ("zdroj nedostupný"), pokračuje dál. V reportu uvidíš co selhalo. |
| **Změní se soubor** (přepsaný během přenosu) | Pipeline ověří hash po přenosu — pokud nesedí, zapíše chybu a soubor se retryuje. |
| **Nic se nikdy nerozsype** | Každý soubor se zpracovává nezávisle. Chyba u jednoho neovlivní ostatní. |

---

## Google Workspace limity

| Limit | Hodnota | Dopad |
|-------|---------|-------|
| Upload/den | 750 GB | 4TB dat = ~6 dní uploadu. Pipeline automaticky pausne a další den pokračuje. |
| API calls/den | 10 000 000 | Deduplikace + verifikace 100k souborů = ~200k calls. Bez problémů. |
| Stahování | Bez limitu | Čtení vlastních souborů nemá denní limit. |
| Úložiště | 6 TB | 4TB dat po dedup bude pravděpodobně 2-3TB. Vejde se. |

---

## Poznámky k šifrování

Šifrování **nebudeme implementovat**. Data na Google 6TB zůstanou čitelná a přístupná.

---

## Technické změny potřebné v aplikaci

1. **Přejmenovat stránku** na "Scénáře → Ultimátní konsolidace 🌍🚀"
2. **Wizard UI** — krokový průvodce místo jedné stránky s formulářem
3. **Odložit deduplikaci** — neprovádět při přenosu, až ve fázi B nad všemi daty
4. **Rozbalování archivů** — nová fáze před deduplikací
5. **Odebrat fázi sync_to_disk z hlavního pipeline** — nahradit tlačítkem "📥 Stáhnout na disk"
6. **Přidat watchdog** — detekce zaseklého přenosu (60s bez aktivity → varování)
7. **Přidat organizaci podle kategorií** (Media/Documents/Software/Other) + rok/měsíc
8. **Pravidlo celistvosti** — .app bundle, projekty, instalačky přenášet jako celek
9. **Finální report** — přehledný dashboard s možností exportu
10. **Checklist zdrojů** — kontrola že jsou připojeny všechny zdroje před startem
11. **4TB disk = zdroj** — přesunout z "cíl" do "lokální složky"
12. **Bez šifrování** — odebrat zmínky o šifrování z pipeline
13. **Google 750GB limit** — auto-pause + pokračování další den
14. **Nové soubory / zmizení** — blbuvzdorné chování (ignorovat nové, logovat zmizení)
15. **Tlačítko "🔄 Synchronizovat disk"** — průběžná jednosměrná sync Google → 4TB disk
16. **Automatická aktualizace katalogu** při každé synchronizaci

---

## Poznámky / Otázky

-
