"""Productor de itinerarios de pasajeros.

Publica los itinerarios de demostracion en el topic passenger-itineraries.
Incluye un pasajero que perdera el transbordo (USR-101) y otro con tiempo
suficiente (USR-102).
"""

from __future__ import annotations

import json
import os
import sys

from kafka import KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (  # noqa: E402
    DEMO_ITINERARIES,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_PASSENGER_ITINERARIES,
)


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )

    print(f"[passenger-producer] Publicando itinerarios en '{TOPIC_PASSENGER_ITINERARIES}'")

    for itinerary in DEMO_ITINERARIES:
        producer.send(
            TOPIC_PASSENGER_ITINERARIES,
            key=itinerary["passenger_id"],
            value=itinerary,
        )
        print(
            f"[passenger-producer] {itinerary['passenger_id']}: "
            f"{itinerary['origin_trip_id']} -> {itinerary['destination_trip_id']} "
            f"en {itinerary['transfer_station_id']} "
            f"(min {itinerary['min_transfer_time_minutes']} min)"
        )

    producer.flush()
    producer.close()
    print("[passenger-producer] Itinerarios publicados.")


if __name__ == "__main__":
    main()
