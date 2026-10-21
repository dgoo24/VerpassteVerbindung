"""Pipeline de datos in-process para despliegues sin broker Kafka.

En desarrollo local el sistema usa Kafka: productores -> topics -> detector de
conexiones -> dashboard. En un despliegue de una sola instancia (por ejemplo
Streamlit Community Cloud) no hay broker, asi que este modulo replica el mismo
flujo dentro del proceso de la app:

    GTFS-RT nacional + horario estatico Fernverkehr
        -> telemetria (train-telemetry) + itinerarios (passenger-itineraries)
        -> ConnectionDetector (mismas reglas de negocio)
        -> incidencias (operational-incidents)
        -> LiveStore del dashboard

Los dos caminos comparten ``build_cycle``, ``synthesize_itineraries`` y
``ConnectionDetector``, de modo que la logica de negocio es identica: lo unico
que cambia es el transporte.

Los datos son los mismos feeds publicos que usa el productor nacional
(gtfs.de): no hace falta ninguna infraestructura adicional.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (  # noqa: E402
    DB_ENRICH_ENABLED,
    DB_ENRICH_MAX_STATIONS,
    NATIONWIDE_REFRESH_SECONDS,
    TOPIC_OPERATIONAL_INCIDENTS,
    TOPIC_PASSENGER_ITINERARIES,
    TOPIC_TRAIN_TELEMETRY,
)
from gtfs_schedule import load_fern_schedule  # noqa: E402
from processors.connection_detector import ConnectionDetector  # noqa: E402
from producers.gtfsrt_nationwide_producer import (  # noqa: E402
    BERLIN,
    _build_name_to_eva,
    build_cycle,
    fetch_feed,
    synthesize_itineraries,
)
from train_number_enricher import TrainNumberEnricher  # noqa: E402


class CloudPipeline:
    """Ejecuta el flujo completo de datos dentro del proceso de la app.

    Se instancia con el ``LiveStore`` del dashboard y arranca un hilo en
    segundo plano que refresca los datos periodicamente.
    """

    def __init__(self, store) -> None:
        self.store = store
        self.detector = ConnectionDetector()
        self.schedule = None
        self.enricher: TrainNumberEnricher | None = None
        self.name_to_eva: dict[str, str] = {}
        self.status = "iniciando"
        self.cycles = 0
        self._enricher_attempted = False
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Arranque
    # ------------------------------------------------------------------ #
    def start(self) -> None:
        threading.Thread(target=self._run, name="cloud-pipeline", daemon=True).start()

    def _run(self) -> None:
        try:
            self.status = "cargando horario GTFS-DE Fernverkehr"
            print("[cloud] Cargando horario estatico Fernverkehr (GTFS-DE fv)...")
            self.schedule = load_fern_schedule()
            print(
                f"[cloud] Horario cargado: {len(self.schedule.trips)} viajes, "
                f"{len(self.schedule.stops)} estaciones."
            )
        except Exception as exc:  # noqa: BLE001
            self.status = f"error al cargar el horario: {exc}"
            print(f"[cloud] ERROR al cargar el horario: {exc}")
            return

        while True:
            cycle_start = time.time()
            try:
                self._cycle()
                self.cycles += 1
            except Exception as exc:  # noqa: BLE001
                self.status = f"error en el ultimo ciclo: {exc}"
                print(f"[cloud] Error de ciclo, reintento: {exc}")
            elapsed = time.time() - cycle_start
            time.sleep(
                max(5, NATIONWIDE_REFRESH_SECONDS - elapsed)
            )

    # ------------------------------------------------------------------ #
    # Ciclo
    # ------------------------------------------------------------------ #
    def _cycle(self) -> None:
        feed = fetch_feed()
        now = datetime.now(BERLIN)
        records, station_events, trips_seen = build_cycle(
            feed,
            self.schedule,
            now,
            enricher=self.enricher,
            name_to_eva=self.name_to_eva,
        )
        itineraries = synthesize_itineraries(station_events)

        # Orden importante: primero la telemetria (el detector la necesita
        # para estimar la hora de llegada real) y despues los itinerarios.
        for record in records:
            self.detector.add_telemetry(record)
            self.store.add(TOPIC_TRAIN_TELEMETRY, record)

        for itinerary in itineraries:
            self.detector.add_itinerary(itinerary)
            self.store.add(TOPIC_PASSENGER_ITINERARIES, itinerary)

        incidents = self._detect_incidents(records)

        # El enriquecimiento con numeros de tren reales (API DB) se activa
        # despues del primer ciclo: asi el panel muestra datos cuanto antes.
        if self.enricher is None:
            self._start_enricher()

        self.status = (
            f"{trips_seen} trenes fv · {len(records)} eventos · "
            f"{len(itineraries)} conexiones · {incidents} incidencias"
        )
        print(f"[cloud] {self.status}")

    def _detect_incidents(self, records: list[dict]) -> int:
        """Evalua los enlaces cuyo tren de origen llega con retraso."""
        delayed = {r["trip_id"] for r in records if r["delay_seconds"] > 0}
        if not delayed:
            return 0

        emitted = 0
        for itinerary in list(self.detector.itineraries.values()):
            if itinerary.origin_trip_id not in delayed:
                continue
            incident = self.detector.evaluate(itinerary)
            if incident is None:
                continue
            if not self.detector.claim_incident_slot(incident):
                continue
            self.store.add(
                TOPIC_OPERATIONAL_INCIDENTS, incident.model_dump(mode="json")
            )
            print(
                f"[cloud] INCIDENCIA {incident.severity} en "
                f"{incident.station_id}: {incident.origin_trip} -> "
                f"{incident.destination_trip} | "
                f"deficit={incident.deficit_minutes}min | "
                f"afectados={incident.passengers_affected}"
            )
            emitted += 1
        return emitted

    # ------------------------------------------------------------------ #
    # Enriquecimiento con numeros de tren reales (API DB)
    # ------------------------------------------------------------------ #
    def _start_enricher(self) -> None:
        if not DB_ENRICH_ENABLED or self._enricher_attempted:
            return
        self._enricher_attempted = True
        try:
            self.name_to_eva = _build_name_to_eva(
                self.schedule, DB_ENRICH_MAX_STATIONS
            )
            if not self.name_to_eva:
                print("[cloud] Sin catalogo de estaciones: no se enriquece.")
                return
            enricher = TrainNumberEnricher()
            enricher.set_stations(list(self.name_to_eva.values()))
            if enricher.start():
                self.enricher = enricher
                print(
                    f"[cloud] Enriquecimiento DB activo para "
                    f"{len(self.name_to_eva)} estaciones."
                )
        except Exception as exc:  # noqa: BLE001 - sin credenciales, seguimos igual
            print(f"[cloud] Enriquecimiento DB no disponible: {exc}")
