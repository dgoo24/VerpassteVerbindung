🚆# MVP: Echtzeit-Events und Streaming – Leitstelle für den Bahnverkehr

Funktionsfähiges MVP für die **Betriebsleitstelle**. Überwacht Zugverspätungen mithilfe von GTFS-RT-Feeds, erkennt automatisch gefährdete Anschlüsse und veröffentlicht **priorisierte betriebliche Störungsmeldungen**, damit das Betriebspersonal kritische Anschlussverbindungen im Netz frühzeitig erkennen und verwalten kann.

## Architektur

```
gtfsrt_nationwide_producer  --(nationales GTFS-RT)-->  [train-telemetry]  (Kafka)
        (realtime.gtfs.de)  + synthetisierte Reiseverbindungen       |
                                                                  v
                                   [passenger-itineraries] -->  connection_detector
                                                                  |
                                                                  v
                                          [operational-incidents] (Kafka)
                                                         |
                                        +----------------+----------------+
                                        |                                 |
                                        v                                 v
                                 incident_console                    dashboard
                                    (Betrieb)                         (Betrieb)
```

(Im Modus `mock`/`db` wird die Quelle links durch `mock_gtfs_server + gtfs_producer` oder durch `db_timetables_producer` ersetzt.)

## Kafka-Topics

- `train-telemetry`: trip_id, train_number, station_id, delay_seconds, scheduled_arrival, estimated_arrival, timestamp.
- `passenger-itineraries`: passenger_id (geschätzte Kohorte), origin_trip_id, destination_trip_id, transfer_station_id, min_transfer_time_minutes, destination_scheduled_departure, estimated_travelers (+ die Felder estimated_travelers_low/high, die die Schätzung als Bereich angeben).
- `operational-incidents` (Betriebsebene): station_id, origin_trip, destination_trip, severity (ALTA/MEDIA/BAJA), recommended_action, passengers_affected (+ passengers_affected_low/high als Bereich), available_transfer_minutes, required_minutes, deficit_minutes, delay_seconds, created_at.

## Datenquelle: national, DB (ein Bahnhof) oder Mock

Das System unterstützt drei Quellen, die über `DATA_SOURCE` in `.env` ausgewählt werden:

- `DATA_SOURCE=nationwide` (**Standard**): **nationaler GTFS-RT-Livestream** von `realtime.gtfs.de/realtime-free.pb` (gtfs.de, Aktualisierung alle 10 s). Deckt Fernverkehrszüge in **ganz Deutschland** sowie die Betreiber ab, die Daten in diesen Stream einspeisen (VRR, VBB, SIRI je Bundesland usw.). Der Produzent `gtfsrt_nationwide_producer` gleicht jedes TripUpdate mit dem statischen Fernverkehrsfahrplan (`gtfs_schedule`, Feed `fv` von gtfs.de) ab und veröffentlicht Telemetrie sowie Anschlussverbindungen für alle aktiven Bahnhöfe. Im Dashboard kann nach **Bundesland und Stadt** gefiltert werden.
- `DATA_SOURCE=db`: Reale Daten der **Timetables-API der Deutschen Bahn** (DB API Marketplace) für **einen** Bahnhof (standardmäßig Fulda, EVA 8000115).
- `DATA_SOURCE=mock`: lokaler simulierter GTFS-RT-Server (`mock_gtfs_server` + `gtfs_producer`), ideal für Demos ohne Netzwerkverbindung.

In allen realen Modi werden die Fahrgastanschlüsse **synthetisch** anhand des Fahrplans erzeugt (jede Ankunft wird mit der nächsten Abfahrt und einem angemessenen Zeitpuffer verknüpft). Da die Quellen keine realen Reiseverbindungen enthalten, ist die Fahrgastzahl eine **Schätzung**, die als Bereich veröffentlicht wird.

> Hinweis zur Datenabdeckung: Der nationale Echtzeit-Feed deckt den **Fernverkehr** und diejenigen Regionalverkehrsbetreiber ab, die Daten in `realtime.gtfs.de` veröffentlichen. Der übrige Nahverkehr ist nicht in Echtzeit enthalten; seine Fahrplandaten sind jedoch in den statischen Feeds verfügbar. Für Echtzeitdaten einer bestimmten Region müsste deren Feed integriert werden (VBB, MVG, NASA usw.).

### DB-Zugang konfigurieren (nur `DATA_SOURCE=db`)

1. Erstelle ein Konto unter https://developers.deutschebahn.com und eine *Application*.
2. Abonniere die **Timetables**-API im **Free**-Tarif (60 Aufrufe/Minute, Lizenz CC BY 4.0 – Quellenangabe: „© DB Station&Service AG“).
3. Kopiere `.env.example` nach `.env` und trage `DB_CLIENT_ID` und `DB_API_KEY` ein. Die Datei `.env` steht in `.gitignore`: Lade sie nicht ins Repository hoch.

Wichtige Variablen in `.env`:

| Variable | Beschreibung | Standardwert |
| --- | --- | --- |
| `DATA_SOURCE` | `nationwide`, `db` oder `mock` | `nationwide` |
| `DASHBOARD_MODE` | `auto`, `cloud` oder `kafka` (Datenquelle des Dashboards) | `auto` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka-Broker (Modus `kafka`) | `localhost:9092` |
| `NATIONWIDE_REFRESH_SECONDS` | Abfrageintervall des nationalen Streams | `60` |
| `NATIONWIDE_HORIZON_MINUTES` | Vorauszeitraum veröffentlichter Ereignisse | `90` |
| `NATIONWIDE_MAX_STATIONS` | Maximale Anzahl von Bahnhöfen je Zyklus | `300` |
| `DB_CLIENT_ID` / `DB_API_KEY` | Zugangsdaten für den API Marketplace (nur `db`) | - |
| `DB_STATION_EVA` | EVA-Nummer des zu überwachenden Bahnhofs (nur `db`) | `8000115` (Fulda) |
| `DB_REFRESH_SECONDS` | Abfrageintervall (nur `db`) | `30` |
| `MIN_TRANSFER_TIME_MINUTES` | Mindestumsteigezeit | `7` |
| `MAX_CONNECTION_WINDOW_MINUTES` | Maximales Zeitfenster zum Zuordnen von Anschlüssen | `45` |
| `DASHBOARD_TABLE_ROW_LIMIT` | Maximale Zeilenanzahl in Dashboard-Tabellen | `300` |

> Im nationalen Modus wird die Telemetrie durch Abgleich der `trip_id` aus GTFS-RT mit dem statischen Fahrplan `fv` (Linie, Ziel und geplante Zeiten) angereichert.

### Echte Zugnummern (Option A)

Der nationale GTFS-RT-Feed von gtfs.de veröffentlicht **nur die Liniennummer** (z. B. `ICE 25`), nicht die Zugnummer (z. B. `ICE 274`). Um die tatsächliche Zugnummer anzuzeigen, verwendet der nationale Produzent die **Timetables**-API der DB (dieselbe wie im Modus `db`) für die **am stärksten frequentierten Bahnhöfe** des Fahrplans:

1. Wähle die `DB_ENRICH_MAX_STATIONS` Bahnhöfe mit den meisten Fernverkehrshalten aus und ermittle ihre EVA-Nummern über die **Stations**-API der DB (Katalog im Cache `data/db_stations.json`).
2. `train_number_enricher` fragt im Hintergrund den Fahrplan (`/plan`) und die Änderungen (`/fchg`) dieser Bahnhöfe ab und erstellt einen Index `(Bahnhof, Tag, Kategorie, Minute) -> Zugnummer`.
3. Wenn bei einem Ereignis `(Bahnhof, Linie, geplante Uhrzeit)` mit dem DB-Fahrplan übereinstimmt, wird das Feld `train_number` auf die tatsächliche Zugnummer gesetzt (`ICE 274`); andernfalls bleibt die Liniennummer erhalten (`ICE 25`). Das Feld `line` enthält immer die Liniennummer.

Ergebnis: **vollständige nationale Abdeckung** über GTFS-RT mit **echten Zugnummern** an großen Knotenpunkten. Konfigurierbar über `DB_ENRICH_*` in `.env`.

> Einschränkung: Die Timetables-API arbeitet bahnhofsbezogen (Free-Tarif, 60 Anfragen/Minute). Daher erhalten nur abgefragte Bahnhöfe echte Zugnummern; bei den übrigen wird die Liniennummer angezeigt. Durch Erhöhen von `DB_ENRICH_MAX_STATIONS` lässt sich die Abdeckung auf Kosten zusätzlicher API-Aufrufe erweitern.

Variablen in `.env` für die Anreicherung:

| Variable | Beschreibung | Standardwert |
| --- | --- | --- |
| `DB_ENRICH_ENABLED` | Aktiviert echte Zugnummern | `true` |
| `DB_ENRICH_MAX_STATIONS` | Anzahl der nach Verkehrsaufkommen abgefragten Bahnhöfe | `40` |
| `DB_ENRICH_PLAN_HOURS` | Fahrplanstunden je Bahnhof und Durchlauf | `2` |
| `DB_ENRICH_REFRESH_SECONDS` | Intervall des Hintergrunddurchlaufs | `180` |
| `DB_ENRICH_WORKERS` | Parallel abgefragte Bahnhöfe | `6` |
| `DB_ENRICH_MATCH_TOLERANCE` | Toleranz (s) beim Abgleich von Uhrzeit und Linie | `300` |

## Voraussetzungen

- Python 3.11+
- Docker und Docker Compose (Plugin `docker compose`)
- Bash (Linux, macOS oder WSL/Git Bash)
- Windows: PowerShell (standardmäßig enthalten)

## Ausführung

### Windows (ein Klick, ohne Git Bash)

Führe einen **Doppelklick auf `start.bat`** aus. Das Skript startet bei Bedarf Docker Desktop, startet Kafka, richtet die Python-Umgebung ein, startet alle Prozesse und **öffnet das Dashboard automatisch im Browser**.

Zum Beenden doppelklicke auf `stop.bat`.

### Linux / macOS / WSL / Git Bash

Ein einziger Befehl:

```bash
chmod +x run.sh && ./run.sh
```

Das Skript erstellt die virtuelle Umgebung, installiert Abhängigkeiten, startet Kafka im KRaft-Modus, wartet auf Port 9092, startet den GTFS-RT-Server, veröffentlicht die Reiseverbindungen und startet parallel den Detektor, den Produzenten, die Störungskonsole und das Web-Dashboard.

## Web-Dashboard (Leitstelle)

`run.sh` / `start.bat` starten ein Streamlit-Dashboard, das die Kafka-Topics in Echtzeit konsumiert (automatische Aktualisierung alle 2 Sekunden). Es ist die **Bedienoberfläche des Betriebspersonals**:

- Netzwerk-KPIs: überwachte Züge, verspätete Züge, Pünktlichkeit, offene Störungen und betroffene Fahrgäste.
- **Priorisierte Störungen**: strikt nach Schweregrad sortiert (`ALTA` = HOCH in Rot -> `MEDIA` = MITTEL -> `BAJA` = NIEDRIG), einschließlich empfohlener Maßnahme.
- **Status aller Züge** im Netz, mit farblich hervorgehobener Verspätung.
- **Bahnhofsfilter** (`selectbox`), um einen logistischen Knoten isoliert zu betrachten.
- **Störungsverwaltung**: Schaltflächen zum Markieren von Störungen als behoben und zum Leeren der Ansicht.

### Visuelle Identität (DB-/ICE-Stil)

Das Dashboard verwendet eine **Betriebsleitstellen**-Ästhetik, die von deutschen Zügen inspiriert ist:

- Festgelegte Farbpalette in `.streamlit/config.toml`: anthrazitfarbener Hintergrund (Dark Mode), DB-Unternehmensrot (`#EC0016`), Verspätungsorange und Pünktlichkeitsgrün.
- Schriftarten **Barlow** für Überschriften und **IBM Plex Mono** für Code.
- Kopfzeile mit `DB`-Logo, Datum auf Deutsch und Statusanzeige **LIVE · KAFKA OK**, die bei eingehenden Ereignissen pulsiert.
- Abschnitte mit rotem Balken, KPI-Karten und nach Schweregrad und Verspätung eingefärbten Zeilen. Die Störungskonsole verwendet dieselbe Farbpalette in ANSI.

Nach dem Start des Systems öffne im Browser:

```text
http://localhost:8501
```

## Bereitstellung auf Streamlit Community Cloud

Das Dashboard kann ohne zusätzliche Infrastruktur als Web-App veröffentlicht werden. Der entscheidende Punkt: In der Cloud gibt es **keinen Kafka-Broker**, daher führt die App die Datenpipeline **innerhalb ihres eigenen Prozesses** aus (`src/cloud_pipeline.py`):

```text
Nationales GTFS-RT + statischer Fernverkehrsfahrplan (öffentliche Feeds von gtfs.de)
    -> Telemetrie + synthetisierte Reiseverbindungen
    -> ConnectionDetector (dieselben Geschäftsregeln)
    -> Störungen -> LiveStore des Dashboards
```

Der Ablauf ist identisch mit dem lokalen Betrieb; nur der Transportweg ändert sich (es gibt keine Kafka-Topics). Die Auswahl erfolgt über `DASHBOARD_MODE`:

| `DASHBOARD_MODE` | Verhalten |
| --- | --- |
| `auto` (Standard) | Verwendet Kafka, wenn der Broker antwortet; andernfalls wird auf die In-Process-Pipeline umgeschaltet |
| `cloud` | Immer In-Process-Pipeline (ohne Broker) |
| `kafka` | Immer die Kafka-Topics |

Schritte:

1. Lade das Repository auf GitHub hoch und erstelle unter [share.streamlit.io](https://share.streamlit.io) eine App, die auf dieses Repository verweist.
2. **Main file path**: `streamlit_app.py` (liegt im Stammverzeichnis; es ist ein Wrapper, der an `src/dashboard_app.py` delegiert).
3. Füge unter **Settings -> Secrets** die Zugangsdaten für die DB-Timetables-API hinzu (nur wenn du echte Zugnummern wie `ICE 274` statt der Liniennummer anzeigen möchtest):

   ```toml
   DB_CLIENT_ID = "deine-client-id"
   DB_API_KEY = "dein-client-secret"
   ```

   Die Schlüssel müssen **auf der obersten Ebene** stehen (ohne `[section]`). Der Code liest sie aus `st.secrets` und auch aus Umgebungsvariablen, sodass `.env` lokal weiterhin funktioniert.

4. (Optional) Setze `DASHBOARD_MODE = "cloud"` als Secret, um den Modus ohne Broker zu erzwingen.

Die GTFS-RT-Feeds werden zur Laufzeit heruntergeladen und in `data/` zwischengespeichert; das erstmalige Laden des statischen Fahrplans dauert einige Sekunden. Ohne DB-Zugangsdaten funktioniert die App weiterhin, zeigt dann aber keine echten Zugnummern an.

## Verringerung der Alarmmüdigkeit

- **Reine Geschäftslogik** in `src/business_rules.py` (`evaluate_incident_severity`), unabhängig von der Benutzeroberfläche.
- **Cooldown für Störungen** im Detektor, indiziert nach `(Bahnhof, Ausgangszug, Anschlusszug)`, um eine Überflutung des Systems bei kontinuierlich eintreffender Telemetrie zu verhindern.
- **Speicherverwaltung**: Das Dashboard begrenzt die gespeicherten Telemetrie- und Störungsdatensätze auf `MAX_RECORDS` (100).
- **Strikte Sortierung nach Schweregrad**, Bahnhofsfilter und Schaltfläche zum Beheben von Störungen.

## Demonstrationsszenario

- `USR-101`: Umstieg von **ICE 593** (starke Verspätung von 15–20 Min.) auf **ICE 590**. Die verfügbare Zeit ist negativ (ca. -8 Min.) und liegt unter dem Mindestwert von 7 Min. -> **Störung mit hoher Priorität** für das Betriebspersonal.
- `USR-102`: Umstieg von **ICE 591** (pünktlich) auf **ICE 592**. Es stehen etwa 30 Min. zur Verfügung -> keine Störung.

> Hinweis: `estimated_travelers` (betroffene Fahrgäste) und die Regeln für den Schweregrad sind **Demonstrationsschätzungen**; die empfohlenen Maßnahmen sind Hinweise und führen keine realen Vorgänge aus.
>
> Die Fahrgastschätzung wird als **Bereich** veröffentlicht, um Scheingenauigkeit zu vermeiden: Der Mittelwert entspricht 50 % der typischen Kapazität des ankommenden Zuges (ICE 445, IC 380, EC 340, TGV 500, Regionalzug 220); der Bereich reicht von **35 % bis 65 %** (z. B. ICE: 155-222-289). Dashboard und Konsole zeigen den Bereich („155-289 Reisende (geschätzt)“); wenn eine Reiseverbindung keinen Bereich enthält, wird der Mittelwert verwendet.

## Struktur

```text
train-missed-connection-mvp/
  docker-compose.yml
  requirements.txt
  README.md
  streamlit_app.py         # Einstiegspunkt (Streamlit Cloud / Stammverzeichnis)
  run.sh
  start.bat        # Windows: Start per Doppelklick
  run.ps1          # Windows: Startlogik
  stop.bat         # Windows: Stopp per Doppelklick
  stop.ps1         # Windows: Stopp-Logik
  src/
    __init__.py
    config.py
    business_rules.py
    dashboard_app.py
    cloud_pipeline.py         # In-Process-Pipeline (Bereitstellung ohne Kafka)
    mock_gtfs_server.py
    gtfs_regional.py
    gtfs_schedule.py
    train_number_enricher.py
    traveler_estimate.py
    db_stations_client.py       # Client für die DB-Stations-API
    db_timetables_client.py     # Client für die DB-Timetables-API
    producers/
      __init__.py
      gtfs_producer.py
      passenger_producer.py
      db_timetables_producer.py   # Echtzeit-Telemetrie (DATA_SOURCE=db)
      gtfsrt_nationwide_producer.py  # Nationale Telemetrie (gtfs.de)
    processors/
      connection_detector.py
    consumers/
      __init__.py
      incident_console.py
```

## System stoppen

- Windows: Doppelklick auf `stop.bat`.
- Linux/macOS/Git Bash: `Ctrl+C` im Terminal beendet die Python-Prozesse. Um Kafka herunterzufahren:

```bash
docker compose down
```
