"""Productor de telemetria real desde la API Timetables de Deutsche Bahn.

Sondea el horario planificado y los cambios en tiempo real de una estacion
(Fulda por defecto), los convierte en eventos de telemetria compatibles con el
topic ``train-telemetry`` y sintetiza itinerarios de pasajeros a partir de los
transbordos mas ajustados de la estacion.

Sustituye al par ``mock_gtfs_server`` + ``gtfs_producer`` cuando
``DATA_SOURCE=db`` y hay credenciales configuradas.
"""

from __future__ import annotations

import json
import os
import sys
import time

from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (  # noqa: E402
    DB_PLAN_HOURS_AHEAD,
    DB_REFRESH_SECONDS,
    DB_STATION_EVA,
    DB_STATION_PATTERN,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_CONNECTION_WINDOW_MINUTES,
    MIN_TRANSFER_TIME_MINUTES,
    STATION_ID,
    TOPIC_PASSENGER_ITINERARIES,
    TOPIC_TRAIN_TELEMETRY,
    db_enabled,
)
from db_timetables_client import (  # noqa: E402
    TrainStop,
    get_timetable,
    resolve_station,
    synthesize_itineraries,
)
from gtfs_regional import (  # noqa: E402
    is_regional,
    load_long_distance_index,
    load_regional_index,
    match_events,
)


def _telemetry_record(
    stop: TrainStop, kind: str, timestamp: float
) -> dict | None:
    """Construye un evento de telemetria para la llegada o la salida."""
    if kind == "ARR":
        planned = stop.planned_arrival
        estimated = stop.effective_arrival
        delay = stop.arrival_delay_seconds
        trip_id = stop.arrival_trip_id
    else:
        planned = stop.planned_departure
        estimated = stop.effective_departure
        delay = stop.departure_delay_seconds
        trip_id = stop.departure_trip_id

    if planned is None or estimated is None:
        return None

    return {
        "trip_id": trip_id,
        "train_number": stop.train_number,
        "station_id": STATION_ID,
        "delay_seconds": delay,
        "scheduled_arrival": planned.isoformat(),
        "estimated_arrival": estimated.isoformat(),
        "line": stop.line,
        "destination": stop.destination,
        "timestamp": timestamp,
    }


def _emit_telemetry(
    producer: KafkaProducer, stops: list[TrainStop], timestamp: float
) -> int:
    count = 0
    for stop in stops:
        for kind in ("ARR", "DEP"):
            record = _telemetry_record(stop, kind, timestamp)
            if record is None:
                continue
            producer.send(
                TOPIC_TRAIN_TELEMETRY,
                key=record["trip_id"],
                value=record,
            )
            count += 1
            extra = ""
            if record.get("line") or record.get("destination"):
                extra = (
                    f" | linea={record.get('line')} -> "
                    f"{record.get('destination')}"
                )
            print(
                f"[db-producer] {record['train_number']} {kind} en "
                f"{record['station_id']} "
                f"plan={record['scheduled_arrival']} "
                f"real={record['estimated_arrival']} "
                f"retraso={record['delay_seconds']}s{extra}"
            )
    return count


def _emit_itineraries(producer: KafkaProducer, stops: list[TrainStop]) -> int:
    itineraries = synthesize_itineraries(
        stops,
        station_id=STATION_ID,
        min_transfer_minutes=MIN_TRANSFER_TIME_MINUTES,
        max_window_minutes=MAX_CONNECTION_WINDOW_MINUTES,
        estimated_travelers=None,
    )
    for itinerary in itineraries:
        producer.send(
            TOPIC_PASSENGER_ITINERARIES,
            key=itinerary["passenger_id"],
            value=itinerary,
        )
    return len(itineraries)


def main() -> None:
    if not db_enabled():
        print(
            "[db-producer] DATA_SOURCE != 'db' o faltan credenciales "
            "(DB_CLIENT_ID / DB_API_KEY). Nada que hacer."
        )
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )

    try:
        eva, name = resolve_station(DB_STATION_PATTERN)
        print(f"[db-producer] Estacion '{name}' (EVA {eva}).")
    except Exception as exc:  # noqa: BLE001
        print(f"[db-producer] No se pudo resolver la estacion: {exc}")
        eva, name = DB_STATION_EVA, DB_STATION_PATTERN

    print(
        f"[db-producer] Leyendo plan+cambios de {name} (EVA {eva}) cada "
        f"{DB_REFRESH_SECONDS}s -> topics '{TOPIC_TRAIN_TELEMETRY}' y "
        f"'{TOPIC_PASSENGER_ITINERARIES}'"
    )

    regional_index = load_regional_index()
    long_distance_index = load_long_distance_index()
    print(
        f"[db-producer] Indices GTFS-DE en la estacion: "
        f"{len(regional_index)} regionales, {len(long_distance_index)} "
        f" de larga distancia."
    )

    try:
        while True:
            try:
                stops = get_timetable(eva, hours_ahead=DB_PLAN_HOURS_AHEAD)
            except Exception as exc:  # noqa: BLE001
                print(f"[db-producer] Error leyendo la API de DB: {exc}")
                time.sleep(DB_REFRESH_SECONDS)
                continue

            for stop in stops:
                when = stop.effective_departure or stop.effective_arrival
                if when is None:
                    continue
                hour_min = when.strftime("%H:%M")
                if is_regional(stop.category):
                    line, dest = match_events(regional_index, hour_min)
                    if not line and not dest:
                        line = stop.category
                else:
                    line, dest = match_events(long_distance_index, hour_min)
                    if not line:
                        line = stop.category
                if line or dest:
                    stop.line, stop.destination = line, dest

            timestamp = time.time()
            n_telemetry = _emit_telemetry(producer, stops, timestamp)
            n_itineraries = _emit_itineraries(producer, stops)
            producer.flush()
            print(
                f"[db-producer] {len(stops)} trenes -> "
                f"{n_telemetry} telemetrias, {n_itineraries} itinerarios."
            )
            time.sleep(DB_REFRESH_SECONDS)
    except KeyboardInterrupt:
        print("\n[db-producer] Deteniendo...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
