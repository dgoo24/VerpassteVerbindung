"""Servidor HTTP que expone un feed GTFS-RT simulado.

Cada peticion a /gtfs-rt genera un FeedMessage (Protobuf) nuevo. El viaje
TRIP_ICE_593 acumula un retraso severo aleatorio de 15 a 20 minutos, mientras
que el resto de viajes de demostracion circulan puntuales.
"""

from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from google.transit import gtfs_realtime_pb2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (  # noqa: E402
    DEMO_TRIPS,
    GTFS_RT_HOST,
    GTFS_RT_PATH,
    GTFS_RT_PORT,
    GTFS_RT_REFRESH_SECONDS,
)


def _delay_for(trip: dict) -> int:
    """Devuelve el retraso (segundos) del viaje para este refresco."""
    variation = trip.get("delay_variation")
    if variation:
        return random.randint(int(variation[0]), int(variation[1]))
    return int(trip.get("delay_seconds", 0))


def build_feed() -> gtfs_realtime_pb2.FeedMessage:
    """Construye un FeedMessage GTFS-RT a partir de los datos de demo."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    for trip_id, trip in DEMO_TRIPS.items():
        entity = feed.entity.add()
        entity.id = trip_id

        trip_update = entity.trip_update
        trip_update.trip.trip_id = trip_id
        # Usamos route_id para transportar el numero de tren legible (ICE 593).
        trip_update.trip.route_id = trip["train_number"]
        trip_update.vehicle.id = trip_id
        trip_update.timestamp = int(time.time())

        scheduled = datetime.fromisoformat(trip["scheduled_time"])
        delay = _delay_for(trip)
        estimated = int(scheduled.timestamp()) + delay

        stop_time_update = trip_update.stop_time_update.add()
        stop_time_update.stop_id = trip["station_id"]
        stop_time_update.stop_sequence = 1
        stop_time_update.schedule_relationship = (
            gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED
        )

        if trip["event_type"] == "arrival":
            stop_time_update.arrival.delay = delay
            stop_time_update.arrival.time = estimated
        else:
            stop_time_update.departure.delay = delay
            stop_time_update.departure.time = estimated

    return feed


class GTFSRequestHandler(BaseHTTPRequestHandler):
    """Handler HTTP que sirve el feed GTFS-RT binario."""

    server_version = "MockGTFSRT/1.0"

    def do_GET(self) -> None:  # noqa: N802 (nombre exigido por BaseHTTPRequestHandler)
        if self.path.rstrip("/") != GTFS_RT_PATH.rstrip("/"):
            self.send_error(404, "Not Found")
            return

        payload = build_feed().SerializeToString()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        print(f"[mock-gtfs] {self.address_string()} - {fmt % args}")


def main() -> None:
    server = ThreadingHTTPServer((GTFS_RT_HOST, GTFS_RT_PORT), GTFSRequestHandler)
    print(
        f"[mock-gtfs] Feed GTFS-RT disponible en "
        f"http://{GTFS_RT_HOST}:{GTFS_RT_PORT}{GTFS_RT_PATH}"
    )
    print(
        f"[mock-gtfs] TRIP_ICE_593 con retrasos severos de 15-20 min "
        f"(refresco sugerido: {GTFS_RT_REFRESH_SECONDS}s)"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[mock-gtfs] Deteniendo servidor...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
