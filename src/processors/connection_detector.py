"""Procesador que detecta transbordos en riesgo.

Consume simultaneamente los topics passenger-itineraries y train-telemetry,
cruza la hora estimada de llegada del tren de origen con la hora de salida del
tren de destino y, cuando el tiempo disponible es menor que el margen minimo
requerido, emite una incidencia operativa para el centro de control en el
topic operational-incidents.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from business_rules import evaluate_incident_severity  # noqa: E402
from config import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    OperationalIncident,
    PassengerItinerary,
    TOPIC_OPERATIONAL_INCIDENTS,
    TOPIC_PASSENGER_ITINERARIES,
    TOPIC_TRAIN_TELEMETRY,
    TrainTelemetry,
)

# Tiempo minimo entre dos incidencias para el mismo enlace (evita fatiga de
# alarmas: la telemetria llega cada pocos segundos y no debe inundar el panel).
INCIDENT_COOLDOWN_SECONDS = 60


class ConnectionDetector:
    """Mantiene el estado de telemetria + itinerarios y evalua transbordos."""

    def __init__(self) -> None:
        # (trip_id, station_id) -> TrainTelemetry (ultima conocida)
        self.telemetry: dict[tuple[str, str], TrainTelemetry] = {}
        # passenger_id -> PassengerItinerary (cohortes de viajeros)
        self.itineraries: dict[str, PassengerItinerary] = {}
        # station_id -> {passenger_id: itinerario} (indice para buscar por
        # estacion en O(1) en vez de escanear todos los itinerarios: con la
        # telemetria nacional son miles de eventos por ciclo).
        self._by_station: dict[str, dict[str, PassengerItinerary]] = {}
        # (station, origin_trip, destination_trip) -> epoch de la ultima incidencia
        self.last_incident_at: dict[tuple[str, str, str], float] = {}

    # ------------------------------------------------------------------ #
    # Ingreso de eventos
    # ------------------------------------------------------------------ #
    def add_telemetry(self, raw: dict) -> list[PassengerItinerary]:
        """Registra una telemetria y devuelve los itinerarios afectados."""
        telemetry = TrainTelemetry.model_validate(raw)
        self.telemetry[(telemetry.trip_id, telemetry.station_id)] = telemetry
        return self._itineraries_for(telemetry.trip_id, telemetry.station_id)

    def add_itinerary(self, raw: dict) -> PassengerItinerary:
        """Registra un itinerario."""
        itinerary = PassengerItinerary.model_validate(raw)
        self.itineraries[itinerary.passenger_id] = itinerary
        self._by_station.setdefault(itinerary.transfer_station_id, {})[
            itinerary.passenger_id
        ] = itinerary
        return itinerary

    def _itineraries_for(
        self, trip_id: str, station_id: str
    ) -> list[PassengerItinerary]:
        return [
            itinerary
            for itinerary in self._by_station.get(station_id, {}).values()
            if trip_id
            in (itinerary.origin_trip_id, itinerary.destination_trip_id)
        ]

    # ------------------------------------------------------------------ #
    # Logica de deteccion
    # ------------------------------------------------------------------ #
    def evaluate(
        self, itinerary: PassengerItinerary
    ) -> OperationalIncident | None:
        """Evalua un itinerario contra la telemetria conocida.

        Devuelve, si hay riesgo, la incidencia operativa derivada; en caso
        contrario, ``None``.
        """
        origin = self.telemetry.get(
            (itinerary.origin_trip_id, itinerary.transfer_station_id)
        )
        if origin is None:
            # Aun no tenemos la llegada estimada del tren de origen.
            return None

        # Hora de salida del tren de destino: usamos su salida estimada si la
        # conocemos (incluye su propio retraso), luego su llegada (convencion
        # de las fuentes que no publican salida) y por ultimo la programada.
        destination = self.telemetry.get(
            (itinerary.destination_trip_id, itinerary.transfer_station_id)
        )
        departure = (
            destination.estimated_departure or destination.estimated_arrival
            if destination is not None
            else itinerary.destination_scheduled_departure
        )

        available_minutes = (
            departure - origin.estimated_arrival
        ).total_seconds() / 60.0

        required_minutes = itinerary.min_transfer_time_minutes
        if available_minutes >= required_minutes:
            return None

        deficit_minutes = required_minutes - available_minutes
        delay_seconds = int(
            (origin.estimated_arrival - origin.scheduled_arrival).total_seconds()
        )
        verdict = evaluate_incident_severity(deficit_minutes, delay_seconds)
        now_iso = datetime.now().isoformat(timespec="seconds")

        incident = OperationalIncident(
            station_id=itinerary.transfer_station_id,
            origin_trip=itinerary.origin_trip_id,
            destination_trip=itinerary.destination_trip_id,
            severity=verdict.severity,
            recommended_action=verdict.recommended_action,
            passengers_affected=itinerary.estimated_travelers,
            passengers_affected_low=itinerary.estimated_travelers_low,
            passengers_affected_high=itinerary.estimated_travelers_high,
            available_transfer_minutes=round(available_minutes, 1),
            required_minutes=required_minutes,
            deficit_minutes=round(deficit_minutes, 1),
            delay_seconds=delay_seconds,
            created_at=now_iso,
        )
        return incident

    def emit_incident(
        self, incident: OperationalIncident, producer: KafkaProducer
    ) -> bool:
        """Publica la incidencia operativa respetando el cooldown por enlace."""
        key = (incident.station_id, incident.origin_trip, incident.destination_trip)
        now = time.time()
        last = self.last_incident_at.get(key, 0.0)
        if now - last < INCIDENT_COOLDOWN_SECONDS:
            return False

        self.last_incident_at[key] = now
        # Poda de cooldowns antiguos (evita crecimiento en ejecuciones largas).
        if len(self.last_incident_at) > 20000:
            cutoff = now - INCIDENT_COOLDOWN_SECONDS * 10
            self.last_incident_at = {
                k: v for k, v in self.last_incident_at.items() if v >= cutoff
            }
        producer.send(
            TOPIC_OPERATIONAL_INCIDENTS,
            key=f"{incident.station_id}:{incident.origin_trip}->{incident.destination_trip}",
            value=incident.model_dump(mode="json"),
        )
        print(
            f"[connection-detector] INCIDENCIA {incident.severity} en "
            f"{incident.station_id}: {incident.origin_trip} -> "
            f"{incident.destination_trip} | deficit={incident.deficit_minutes}min "
            f"| afectados={incident.passengers_affected} "
            f"| {incident.recommended_action}"
        )
        return True

    def process(self, itinerary: PassengerItinerary, producer: KafkaProducer) -> None:
        incident = self.evaluate(itinerary)
        if incident is None:
            return
        self.emit_incident(incident, producer)


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_PASSENGER_ITINERARIES,
        TOPIC_TRAIN_TELEMETRY,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="connection-detector",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )

    detector = ConnectionDetector()
    print(
        "[connection-detector] Escuchando "
        f"'{TOPIC_PASSENGER_ITINERARIES}' + '{TOPIC_TRAIN_TELEMETRY}'..."
    )

    # Bucle de reconexion: el estado del detector se conserva entre reconexiones.
    while True:
        consumer = _build_consumer()
        try:
            for message in consumer:
                try:
                    if message.topic == TOPIC_PASSENGER_ITINERARIES:
                        itinerary = detector.add_itinerary(message.value)
                        detector.process(itinerary, producer)
                    elif message.topic == TOPIC_TRAIN_TELEMETRY:
                        for itinerary in detector.add_telemetry(message.value):
                            detector.process(itinerary, producer)
                except Exception as exc:  # noqa: BLE001
                    print(f"[connection-detector] Evento ignorado por error: {exc}")
        except KeyboardInterrupt:
            print("\n[connection-detector] Deteniendo...")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[connection-detector] Error de consumo, reconectando: {exc}")
            time.sleep(3)
        finally:
            try:
                consumer.close()
            except Exception:  # noqa: BLE001
                pass

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
