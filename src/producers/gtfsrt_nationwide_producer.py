"""Productor nacional de telemetria: GTFS-RT de toda Alemania en tiempo real.

Fuente: ``realtime.gtfs.de/realtime-free.pb`` (gtfs.de, actualizado cada
10 s, licencia abierta). El feed agrega TripUpdates de multiples operadores
(Fernverkehr DB, VRR, VBB, SIRI por estado...). Cada ciclo:

1. Descarga el protobuf y lo cruza con el horario estatico Fernverkehr
   (``gtfs_schedule``) para recuperar horarios planificados, linea y destino.
2. Emite un evento de ``train-telemetry`` por (tren, estacion) con llegada y
   salida estimadas en las proximas ``NATIONWIDE_HORIZON_MINUTES``.
3. Sintetiza ``passenger-itineraries``: en cada estacion activa, la llegada
   con la salida siguiente mas ajustada (transbordos viables). El detector
   convertira en incidencia los que queden por debajo del minimo requerido.

Cobertura honesta: tiempo real nacional para Fernverkehr y operadores que
alimentan el stream de gtfs.de; el Nahverkehr restante aparece solo si su
operador publica en ese feed.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (  # noqa: E402
    DB_ENRICH_ENABLED,
    DB_ENRICH_MAX_STATIONS,
    GTFS_RT_NATIONAL_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_CONNECTION_WINDOW_MINUTES,
    MIN_TRANSFER_TIME_MINUTES,
    NATIONWIDE_HORIZON_MINUTES,
    NATIONWIDE_LOOKBACK_MINUTES,
    NATIONWIDE_MAX_STATIONS,
    NATIONWIDE_REFRESH_SECONDS,
    TOPIC_PASSENGER_ITINERARIES,
    TOPIC_TRAIN_TELEMETRY,
)
from db_stations_client import build_name_index, fetch_stations, normalize_name  # noqa: E402
from gtfs_schedule import load_fern_schedule  # noqa: E402
from train_number_enricher import TrainNumberEnricher  # noqa: E402
from traveler_estimate import estimate_connection_travelers  # noqa: E402

BERLIN = ZoneInfo("Europe/Berlin")
REQUEST_TIMEOUT_SECONDS = 30


def _service_datetime(start_date: str, seconds: int) -> datetime:
    """Marca de tiempo GTFS (segundos desde el inicio del dia de servicio)."""
    day = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=BERLIN)
    return day + timedelta(seconds=seconds)


def _epoch_to_dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, BERLIN)


def _to_naive_local(value: datetime) -> datetime:
    """El resto del sistema usa hora local naive (Europe/Berlin)."""
    return value.astimezone(BERLIN).replace(tzinfo=None)


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(
        GTFS_RT_NATIONAL_URL, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def _resolve_stop(info, stu) -> object | None:
    if stu.stop_sequence:
        stop = info.stops.get(stu.stop_sequence)
        if stop is not None:
            return stop
    return info.stops_by_id.get(stu.stop_id)


def build_cycle(feed, schedule, now: datetime, enricher=None, name_to_eva=None):
    """Convierte el feed en eventos de telemetria + eventos por estacion.

    Devuelve ``(records, station_events, trips_seen)``. Cada record lleva la
    llegada y, si se conoce, la salida del tren en la estacion.

    Si se pasa ``enricher`` (``TrainNumberEnricher``) y ``name_to_eva``, el
    campo ``train_number`` se sustituye por el numero de tren real de DB
    (p. ej. 'ICE 274') cuando hay coincidencia; si no, se mantiene la linea.
    """
    now_epoch = now.timestamp()
    window_start = now_epoch - NATIONWIDE_LOOKBACK_MINUTES * 60
    window_end = now_epoch + NATIONWIDE_HORIZON_MINUTES * 60
    name_to_eva = name_to_eva or {}

    records: list[dict] = []
    station_events: dict[str, dict] = {}
    trips_seen = 0

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        info = schedule.trips.get(tu.trip.trip_id)
        if info is None:
            continue
        trips_seen += 1
        start_date = tu.trip.start_date or datetime.now(BERLIN).strftime("%Y%m%d")

        for stu in tu.stop_time_update:
            stop = _resolve_stop(info, stu)
            if stop is None:
                continue

            arr_abs = stu.arrival.time if stu.HasField("arrival") and stu.arrival.time else None
            dep_abs = stu.departure.time if stu.HasField("departure") and stu.departure.time else None
            arr_delay = stu.arrival.delay if stu.HasField("arrival") else None
            dep_delay = stu.departure.delay if stu.HasField("departure") else None

            sched_arr = (
                _service_datetime(start_date, stop.arrival_seconds)
                if stop.arrival_seconds is not None
                else None
            )
            sched_dep = (
                _service_datetime(start_date, stop.departure_seconds)
                if stop.departure_seconds is not None
                else None
            )

            est_arr = None
            if arr_abs is not None:
                est_arr = _epoch_to_dt(arr_abs)
            elif sched_arr is not None and arr_delay is not None:
                est_arr = sched_arr + timedelta(seconds=arr_delay)

            est_dep = None
            if dep_abs is not None:
                est_dep = _epoch_to_dt(dep_abs)
            elif sched_dep is not None and dep_delay is not None:
                est_dep = sched_dep + timedelta(seconds=dep_delay)

            # Evento primario: llegada si existe; si no, salida (parada origen).
            primary = est_arr or est_dep
            if primary is None:
                continue
            primary_epoch = primary.timestamp()
            if not (window_start <= primary_epoch <= window_end):
                continue

            sched_primary = sched_arr if est_arr is not None else sched_dep
            delay_seconds = (
                int((primary - sched_primary).total_seconds())
                if sched_primary is not None
                else 0
            )

            line = info.line or info.trip_id
            # Numero de tren real de DB (Opcion A); si no hay match, la linea.
            train_number = line
            if enricher is not None:
                eva = name_to_eva.get(normalize_name(stop.stop_name or ""))
                if eva:
                    real = enricher.lookup(eva, line, sched_primary or primary)
                    if real:
                        train_number = real
            records.append(
                {
                    "trip_id": info.trip_id,
                    "train_number": train_number,
                    "station_id": stop.stop_id,
                    "delay_seconds": delay_seconds,
                    "scheduled_arrival": (
                        (sched_primary or primary).isoformat()
                    ),
                    "estimated_arrival": primary.isoformat(),
                    "timestamp": now_epoch,
                    "station_name": stop.stop_name,
                    "line": line,
                    "destination": info.destination,
                    "scheduled_departure": (
                        sched_dep.isoformat() if sched_dep is not None else None
                    ),
                    "estimated_departure": (
                        est_dep.isoformat() if est_dep is not None else None
                    ),
                }
            )

            slot = station_events.setdefault(
                stop.stop_id, {"name": stop.stop_name, "arr": {}, "dep": {}}
            )
            if est_arr is not None:
                slot["arr"][info.trip_id] = (est_arr, sched_arr, line)
            if est_dep is not None:
                slot["dep"][info.trip_id] = (est_dep, sched_dep, line)

    return records, station_events, trips_seen


def synthesize_itineraries(station_events: dict[str, dict]) -> list[dict]:
    """Una conexion por llegada: la salida siguiente mas ajustada."""
    itineraries: list[dict] = []
    active = sorted(
        station_events.items(),
        key=lambda kv: len(kv[1]["arr"]) + len(kv[1]["dep"]),
        reverse=True,
    )[:NATIONWIDE_MAX_STATIONS]

    for station_id, events in active:
        arrivals = events["arr"]
        departures = events["dep"]
        if not arrivals or not departures:
            continue
        for origin_trip, (arr_est, _arr_sched, arr_line) in arrivals.items():
            best = None
            for dep_trip, (dep_est, dep_sched, dep_line) in departures.items():
                if dep_trip == origin_trip:
                    continue
                gap = (dep_est - arr_est).total_seconds() / 60.0
                if gap < 0 or gap > MAX_CONNECTION_WINDOW_MINUTES:
                    continue
                if best is None or gap < best[0]:
                    best = (gap, dep_trip, dep_est, dep_sched, dep_line)
            if best is None:
                continue
            _gap, dep_trip, _dep_est, dep_sched, _dep_line = best
            low, mid, high = estimate_connection_travelers(arr_line)
            itineraries.append(
                {
                    "passenger_id": f"PAX-{station_id}-{origin_trip}-{dep_trip}",
                    "origin_trip_id": origin_trip,
                    "destination_trip_id": dep_trip,
                    "transfer_station_id": station_id,
                    "min_transfer_time_minutes": MIN_TRANSFER_TIME_MINUTES,
                    "destination_scheduled_departure": (
                        dep_sched or _dep_est
                    ).isoformat(),
                    "estimated_travelers": mid,
                    "estimated_travelers_low": low,
                    "estimated_travelers_high": high,
                }
            )
    return itineraries


def _top_station_names(schedule, limit: int) -> list[str]:
    """Nombres de las estaciones con mas paradas Fernverkehr en el horario."""
    counts: dict[str, int] = {}
    for info in schedule.trips.values():
        for stop in info.stops.values():
            if stop.stop_name:
                counts[stop.stop_name] = counts.get(stop.stop_name, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return [name for name, _count in ranked[:limit]]


def _build_name_to_eva(schedule, limit: int) -> dict[str, str]:
    """Resuelve nombre-normalizado -> EVA para las top-N estaciones.

    Descarga el catalogo de estaciones de DB (cacheado) y cruza sus nombres
    con los del horario fv.
    """
    try:
        stations = fetch_stations()
    except Exception as exc:  # noqa: BLE001 - sin catalogo, seguimos sin enriquecer
        print(f"[nationwide] No se pudo cargar el catalogo de estaciones DB: {exc}")
        return {}
    db_index = build_name_index(stations)
    top_names = _top_station_names(schedule, limit)
    mapping: dict[str, str] = {}
    for name in top_names:
        eva = db_index.get(normalize_name(name))
        if eva:
            mapping[normalize_name(name)] = eva
    return mapping


def main() -> None:
    print("[nationwide] Cargando horario Fernverkehr (GTFS-DE fv)...")
    schedule = load_fern_schedule()
    print(
        f"[nationwide] {len(schedule.trips)} viajes, "
        f"{len(schedule.stops)} estaciones en el indice."
    )

    # Opcion A: enriquecer con numeros de tren reales de DB en las grandes
    # estaciones (el GTFS-RT nacional solo trae la linea).
    enricher = None
    name_to_eva: dict[str, str] = {}
    if DB_ENRICH_ENABLED:
        name_to_eva = _build_name_to_eva(schedule, DB_ENRICH_MAX_STATIONS)
        if name_to_eva:
            enricher = TrainNumberEnricher()
            enricher.set_stations(list(name_to_eva.values()))
            if enricher.start():
                print(
                    f"[nationwide] Enriquecimiento DB activo para "
                    f"{len(name_to_eva)} estaciones "
                    f"(numeros de tren reales)."
                )
            else:
                enricher = None
        else:
            print(
                "[nationwide] Enriquecimiento DB inactivo "
                "(sin coincidencias de estaciones)."
            )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )
    print(
        f"[nationwide] Sondeando {GTFS_RT_NATIONAL_URL} cada "
        f"{NATIONWIDE_REFRESH_SECONDS}s (ventana "
        f"+{NATIONWIDE_HORIZON_MINUTES} min)..."
    )

    while True:
        cycle_start = time.time()
        try:
            feed = fetch_feed()
            now = datetime.now(BERLIN)
            records, station_events, trips_seen = build_cycle(
                feed, schedule, now, enricher=enricher, name_to_eva=name_to_eva
            )
            itineraries = synthesize_itineraries(station_events)

            for record in records:
                producer.send(
                    TOPIC_TRAIN_TELEMETRY,
                    key=record["trip_id"],
                    value=record,
                )
            for itinerary in itineraries:
                producer.send(
                    TOPIC_PASSENGER_ITINERARIES,
                    key=itinerary["passenger_id"],
                    value=itinerary,
                )
            producer.flush()

            elapsed = time.time() - cycle_start
            enrich_info = ""
            if enricher is not None:
                enrich_info = (
                    f" | numeros DB: {enricher.last_entry_count} entradas "
                    f"de {enricher.last_station_count} estaciones"
                )
            print(
                f"[nationwide] {trips_seen} trenes fv | {len(records)} eventos "
                f"en {len(station_events)} estaciones | "
                f"{len(itineraries)} conexiones | {elapsed:.1f}s{enrich_info}"
            )
        except KeyboardInterrupt:
            print("\n[nationwide] Deteniendo...")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[nationwide] Error de ciclo, reintento: {exc}")

        time.sleep(max(5, NATIONWIDE_REFRESH_SECONDS - (time.time() - cycle_start)))

    producer.flush()
    producer.close()
    if enricher is not None:
        enricher.stop()


if __name__ == "__main__":
    main()
