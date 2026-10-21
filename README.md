# MVP: Eventos y Streaming en Tiempo Real - Centro de Control Ferroviario

MVP funcional orientado al **Centro de Control de Operaciones**. Monitoriza
retrasos de trenes a traves de feeds GTFS-RT, detecta automaticamente los
transbordos en riesgo y publica **incidencias operativas priorizadas** para que
el operario anticipe y gestione los enlaces criticos de la red.

## Arquitectura

```
gtfsrt_nationwide_producer  --(GTFS-RT nacional)-->  [train-telemetry]  (Kafka)
        (realtime.gtfs.de)  + itinerarios sintetizados       |
                                                             v
                                   [passenger-itineraries] -->  connection_detector
                                                             |
                                                             v
                                          [operational-incidents] (Kafka)
                                                          |
                                        +-----------------+-----------------+
                                        |                                   |
                                        v                                   v
                                 incident_console                      dashboard
                                     (operario)                        (operario)
```

(En modo `mock`/`db` la fuente de la izquierda se sustituye por
`mock_gtfs_server + gtfs_producer` o por `db_timetables_producer`.)

## Topics de Kafka

- `train-telemetry`: trip_id, train_number, station_id, delay_seconds,
  scheduled_arrival, estimated_arrival, timestamp.
- `passenger-itineraries`: passenger_id (cohorte estimada), origin_trip_id,
  destination_trip_id, transfer_station_id, min_transfer_time_minutes,
  destination_scheduled_departure, estimated_travelers (+ los campos
  estimated_travelers_low/high, que abren el estimado como rango).
- `operational-incidents` (capa operario): station_id, origin_trip,
  destination_trip, severity (ALTA/MEDIA/BAJA), recommended_action,
  passengers_affected (+ passengers_affected_low/high como rango),
  available_transfer_minutes, required_minutes,
  deficit_minutes, delay_seconds, created_at.

## Fuente de datos: nacional, DB (una estacion) o mock

El sistema admite tres fuentes, seleccionables con `DATA_SOURCE` en `.env`:

- `DATA_SOURCE=nationwide` (**por defecto**): **GTFS-RT nacional en tiempo
  real** de `realtime.gtfs.de/realtime-free.pb` (gtfs.de, actualizado cada
  10 s). Cubre trenes Fernverkehr de **toda Alemania** y los operadores que
  alimentan ese stream (VRR, VBB, SIRI por estado...). El productor
  `gtfsrt_nationwide_producer` cruza cada TripUpdate con el horario estatico
  Fernverkehr (`gtfs_schedule`, feed `fv` de gtfs.de) y publica telemetria +
  itinerarios de transbordo para todas las estaciones activas. El dashboard
  permite **filtrar por Region (Bundesland) y Ciudad**.
- `DATA_SOURCE=db`: datos reales de la **API Timetables de Deutsche Bahn**
  (DB API Marketplace) para **una** estacion (por defecto Fulda, EVA 8000115).
- `DATA_SOURCE=mock`: servidor GTFS-RT simulado local
  (`mock_gtfs_server` + `gtfs_producer`), ideal para demos sin red.

En todos los modos reales los transbordos de pasajeros se **sintetizan** a
partir de la malla horaria (cada llegada se empareja con la siguiente salida
con un margen razonable); no existen itinerarios reales en las fuentes, por lo
que la cifra de pasajeros es una **estimacion** que se publica como rango.

> Nota de cobertura (honesta): el tiempo real nacional cubre **Fernverkehr** y
> los operadores regionales que publican en `realtime.gtfs.de`. El resto del
> Nahverkehr no aparece en tiempo real; su horario si esta en los feeds
> estaticos. Para tiempo real de una region concreta habria que integrar su
> feed (VBB, MVG, NASA...).

### Configurar el acceso a DB (solo `DATA_SOURCE=db`)

1. Crea una cuenta en https://developers.deutschebahn.com y una *application*.
2. Sususcribete a la API **Timetables**, plan **Free** (60 llamadas/minuto,
   licencia CC BY 4.0 - atribucion: "© DB Station&Service AG").
3. Copia `.env.example` a `.env` y rellena `DB_CLIENT_ID` y `DB_API_KEY`.
   El archivo `.env` esta en `.gitignore`: no lo subas al repositorio.

Variables principales de `.env`:

| Variable | Descripcion | Defecto |
| --- | --- | --- |
| `DATA_SOURCE` | `nationwide`, `db` o `mock` | `nationwide` |
| `NATIONWIDE_REFRESH_SECONDS` | Cadencia de sondeo del stream nacional | `60` |
| `NATIONWIDE_HORIZON_MINUTES` | Ventana hacia delante de eventos publicados | `90` |
| `NATIONWIDE_MAX_STATIONS` | Estaciones maximas por ciclo | `300` |
| `DB_CLIENT_ID` / `DB_API_KEY` | Credenciales del API Marketplace (solo `db`) | - |
| `DB_STATION_EVA` | EVA de la estacion a monitorizar (solo `db`) | `8000115` (Fulda) |
| `DB_REFRESH_SECONDS` | Cadencia de sondeo (solo `db`) | `30` |
| `MIN_TRANSFER_TIME_MINUTES` | Margen minimo de transbordo | `7` |
| `MAX_CONNECTION_WINDOW_MINUTES` | Ventana maxima para emparejar conexiones | `45` |
| `DASHBOARD_TABLE_ROW_LIMIT` | Filas maximas en las tablas del dashboard | `300` |

> En modo nacional la telemetria se enriquece cruzando el `trip_id` del
> GTFS-RT con el horario estatico `fv` (linea, destino y horarios planificados).

### Numeros de tren reales (Opcion A)

El GTFS-RT nacional de gtfs.de **solo publica el numero de linea** (p. ej.
`ICE 25`), no el numero de tren (p. ej. `ICE 274`). Para mostrar el numero de
tren real, el productor nacional usa la API **Timetables** de DB (la misma del
modo `db`) sobre las **estaciones mas concurridas** del horario:

1. Selecciona las `DB_ENRICH_MAX_STATIONS` estaciones con mas paradas
   Fernverkehr y resuelve su EVA con la API **Stations** de DB (catalogo
   cacheado en `data/db_stations.json`).
2. `train_number_enricher` sondea en segundo plano el plan (`/plan`) y los
   cambios (`/fchg`) de esas estaciones y construye un indice
   `(estacion, dia, categoria, minuto) -> numero de tren`.
3. En cada evento, si el `(estacion, linea, hora planificada)` casa con el
   plan de DB, el campo `train_number` pasa a ser el numero real (`ICE 274`);
   si no hay match, se mantiene la linea (`ICE 25`). El campo `line` siempre
   conserva la linea.

Resultado: **cobertura nacional completa** (via GTFS-RT) con **numeros de tren
reales** en los grandes nodos. Configurable con `DB_ENRICH_*` en `.env`.

> Limitacion honesta: la API Timetables es por estacion (plan Free, 60
> req/min), asi que solo las estaciones sondeadas reciben numero de tren; el
> resto muestra la linea. Aumentar `DB_ENRICH_MAX_STATIONS` amplia cobertura a
> costa de mas llamadas al API.

Variables de `.env` para el enriquecimiento:

| Variable | Descripcion | Defecto |
| --- | --- | --- |
| `DB_ENRICH_ENABLED` | Activa los numeros de tren reales | `true` |
| `DB_ENRICH_MAX_STATIONS` | Nº de estaciones (por trafico) a sondear | `40` |
| `DB_ENRICH_PLAN_HOURS` | Horas de plan por estacion y barrido | `2` |
| `DB_ENRICH_REFRESH_SECONDS` | Cadencia del barrido de fondo | `180` |
| `DB_ENRICH_WORKERS` | Estaciones sondeadas en paralelo | `6` |
| `DB_ENRICH_MATCH_TOLERANCE` | Tolerancia (s) al casar hora/linea | `300` |


## Requisitos

- Python 3.11+
- Docker y Docker Compose (plugin `docker compose`)
- Bash (Linux, macOS o WSL/Git Bash)
- Windows: PowerShell (incluido por defecto)

## Ejecucion

### Windows (un solo clic, sin Git Bash)

Haz **doble clic en `start.bat`**. El script arranca Docker Desktop si hace
falta, levanta Kafka, prepara el entorno Python, pone en marcha todos los
procesos y **abre el dashboard en el navegador automaticamente**.

Para detenerlo, haz doble clic en `stop.bat`.

### Linux / macOS / WSL / Git Bash

Un solo comando:

```bash
chmod +x run.sh && ./run.sh
```

El script crea el entorno virtual, instala dependencias, levanta Kafka en modo
KRaft, espera al puerto 9092, arranca el servidor GTFS-RT, publica los
itinerarios y lanza en paralelo el detector, el productor, la consola de
incidencias y el dashboard web.

## Dashboard web (Centro de Control)

`run.sh` / `start.bat` levantan un dashboard en Streamlit que consume los topics
de Kafka en tiempo real (auto-refresco cada 2 segundos). Es el **panel del
operario**:

- KPIs de red: trenes monitorizados, trenes con retraso, puntualidad,
  incidencias abiertas y pasajeros afectados.
- **Incidencias priorizadas**: ordenadas estrictamente por severidad
  (`ALTA` en rojo -> `MEDIA` -> `BAJA`), con la accion recomendada.
- **Estado de todos los trenes** de la red, con el retraso coloreado.
- **Filtro por estacion** (`selectbox`) para aislar el nodo logistico.
- **Gestion de incidencias**: botones para marcar incidencias como
  resueltas y limpiar la vista.

### Identidad visual (estilo DB / ICE)

El panel usa una estetica de **Betriebsleitstelle** inspirada en los trenes
alemanes:

- Paleta definida en `.streamlit/config.toml`: fondo *anthrazit* (modo oscuro),
  rojo corporativo DB (`#EC0016`), ambar de retrasos y verde de puntualidad.
- Tipografia **Barlow** para titulos y **IBM Plex Mono** para codigo.
- Cabecera con logotipo `DB`, fecha en aleman y semaforo **LIVE · KAFKA OK**
  que pulsa cuando llegan eventos.
- Secciones con franja roja, KPIs en tarjetas y filas coloreadas por severidad
  y retraso. La consola de incidencias usa la misma paleta en ANSI.

Una vez arrancado el sistema, abre en el navegador:

```
http://localhost:8501
```

## Mitigacion de fatiga de alarmas

- **Logica de negocio pura** en `src/business_rules.py`
  (`evaluate_incident_severity`), independiente de la interfaz.
- **Cooldown de incidencias** en el detector, indexado por
  `(estacion, tren_origen, tren_destino)`, para no inundar el sistema cuando
  llega telemetria continua.
- **Gestion de memoria**: el dashboard limita a `MAX_RECORDS` (100) los
  registros de telemetria e incidencias que conserva.
- **Orden estricto por severidad**, filtro por estacion y boton de resolucion.

## Escenario de demostracion

- `USR-101`: transborda de **ICE 593** (retraso severo de 15-20 min) a
  **ICE 590**. El tiempo disponible es negativo (aprox. -8 min) e inferior al
  minimo de 7 min -> **incidencia ALTA** para el operario.
- `USR-102`: transborda de **ICE 591** (puntual) a **ICE 592**. Dispone de
  ~30 min -> sin incidencia.

> Nota: `estimated_travelers` (pasajeros afectados) y las reglas de severidad
> son **estimaciones de demostracion**; las acciones recomendadas son texto
> orientativo, no ejecutan ninguna operacion real.
>
> La estimacion de viajeros se publica como **rango** para no dar un falso
> exacto: el punto medio es el 50% de la capacidad tipica del tren de llegada
> (ICE 445, IC 380, EC 340, TGV 500, regional 220) y el rango lo abre a
> **35%–65%** (p. ej. un ICE: 155-222-289). El dashboard y la consola muestran
> la franja («155-289 Reisende (est.)»); si un itinerario no trae rango, se
> usa el punto medio.

## Estructura

```
train-missed-connection-mvp/
  docker-compose.yml
  requirements.txt
  README.md
  run.sh
  start.bat        # Windows: arranque con un doble clic
  run.ps1          # Windows: logica de arranque
  stop.bat         # Windows: parada con un doble clic
  stop.ps1         # Windows: logica de parada
  src/
    __init__.py
    config.py
    business_rules.py
    dashboard_app.py
    mock_gtfs_server.py
    db_timetables_client.py       # cliente de la API Timetables de DB
    producers/
      __init__.py
      gtfs_producer.py
      passenger_producer.py
      db_timetables_producer.py   # telemetria real (DATA_SOURCE=db)
    processors/
      __init__.py
      connection_detector.py
    consumers/
      __init__.py
      incident_console.py
```

## Detener el sistema

- Windows: doble clic en `stop.bat`.
- Linux/macOS/Git Bash: `Ctrl+C` en la terminal detiene los procesos Python. Para
  apagar Kafka:

```bash
docker compose down
```
