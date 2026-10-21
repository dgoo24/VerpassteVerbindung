"""Productor de telemetria de trenes.

Descarga el feed GTFS-RT del servidor simulado, convierte los TripUpdates
Protobuf en registros JSON y los publica continuamente en el topic
train-telemetry.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime

import requests
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (  # noqa: E402
    GTFS_RT_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    TELEMETRY_INTERVAL_SECONDS,
    TOPIC_TRAIN_TELEMETRY,
)


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    """Descarga y parsea el feed GTFS-RT."""
    response = requests.get(GTFS_RT_URL, timeout=10)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def _iso(epoch: int) -> str:
    """Convierte una marca epoch a ISO 8601 local."""
    return datetime.fromtimestamp(epoch).isoformat()


def feed_to_records(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Transforma un FeedMessage en la lista de telemetrias del topic."""
    records: list[dict] = []
    event_timestamp = time.time()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip_id = trip_update.trip.trip_id
        train_number = trip_update.trip.route_id or trip_id

        for stop_time_update in trip_update.stop_time_update:
            event = None
            if stop_time_update.HasField("arrival"):
                event = stop_time_update.arrival
            elif stop_time_update.HasField("departure"):
                event = stop_time_update.departure

            if event is None or event.time == 0:
                continue

            delay = int(event.delay)
            estimated = int(event.time)
            scheduled = estimated - delay

            records.append(
                {
                    "trip_id": trip_id,
                    "train_number": train_number,
                    "station_id": stop_time_update.stop_id,
                    "delay_seconds": delay,
                    "scheduled_arrival": _iso(scheduled),
                    "estimated_arrival": _iso(estimated),
                    "timestamp": event_timestamp,
                }
            )

    return records


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )

    print(
        f"[gtfs-producer] Leyendo '{GTFS_RT_URL}' -> topic "
        f"'{TOPIC_TRAIN_TELEMETRY}' cada {TELEMETRY_INTERVAL_SECONDS}s"
    )

    try:
        while True:
            try:
                feed = fetch_feed()
            except Exception as exc:  # noqa: BLE001
                print(f"[gtfs-producer] Error leyendo el feed: {exc}")
                time.sleep(TELEMETRY_INTERVAL_SECONDS)
                continue

            for record in feed_to_records(feed):
                producer.send(
                    TOPIC_TRAIN_TELEMETRY,
                    key=record["trip_id"],
                    value=record,
                )
                print(
                    f"[gtfs-producer] {record['train_number']} "
                    f"({record['trip_id']}) en {record['station_id']} "
                    f"retraso={record['delay_seconds']}s"
                )
            producer.flush()
            time.sleep(TELEMETRY_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[gtfs-producer] Deteniendo...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
