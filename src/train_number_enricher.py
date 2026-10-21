"""Enriquecimiento nacional con numeros de tren reales (Opcion A).

El feed GTFS-RT nacional de gtfs.de solo publica el numero de linea
(p. ej. 'ICE 25'). La API Timetables de DB si trae el numero de tren real
(p. ej. 'ICE 274', campo ``<tl c="ICE" n="274"/>``), pero es por estacion.

Este modulo construye, sondeando las N estaciones mas concurridas del feed
nacional, un indice ``(estacion, hora, categoria+linea) -> numero de tren``
que luego se usa para enriquecer la telemetria GTFS-RT (que sigue siendo la
fuente de cobertura nacional). Asi se mantiene Alemania entera y se gana
precision real donde mas importa (grandes nodos).
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta

from config import (
    DB_ENRICH_ENABLED,
    DB_ENRICH_MATCH_TOLERANCE,
    DB_ENRICH_MAX_STATIONS,
    DB_ENRICH_PLAN_HOURS,
    DB_ENRICH_REFRESH_SECONDS,
    DB_ENRICH_WORKERS,
)
from db_timetables_client import get_changes, get_plan


@dataclass
class _EnrichedTrain:
    """Entrada del indice: numero de tren y linea para una estacion/hora."""

    number_label: str
    line: str
    category: str
    minute_of_day: int
    day_key: str


def _category_prefix(label: str) -> str:
    """Extrae la categoria de una linea: 'ICE 25' -> 'ICE'."""
    return label.split(" ", 1)[0].strip().upper()


class TrainNumberEnricher:
    """Indice (estacion, hora, linea) -> numero de tren real.

    Se refresca en un hilo de fondo sondeando la API Timetables de DB para un
    conjunto de estaciones (las de mayor trafico del feed nacional). El
    productor consulta ``lookup`` en cada evento: es O(1) y nunca bloquea la
    emision si el hilo aun no ha poblado el indice.
    """

    def __init__(self, refresh_seconds: int = DB_ENRICH_REFRESH_SECONDS):
        self.refresh_seconds = refresh_seconds
        # Clave: (eva, dia, categoria) -> lista de trenes con su minuto del dia.
        self._index: dict[tuple, list[_EnrichedTrain]] = {}
        self._lock = threading.Lock()
        self._stations: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_refresh: float = 0.0
        self.last_station_count: int = 0
        self.last_entry_count: int = 0

    # ------------------------------------------------------------------ #
    # Configuracion de estaciones y arranque
    # ------------------------------------------------------------------ #
    def set_stations(self, evas: list[str]) -> None:
        """Fija las estaciones (EVA) a sondear (top-N del feed nacional)."""
        with self._lock:
            self._stations = list(dict.fromkeys(evas))[:DB_ENRICH_MAX_STATIONS]

    def start(self) -> bool:
        """Arranca el hilo de refresco. Devuelve False si esta desactivado."""
        if not DB_ENRICH_ENABLED or not self._stations:
            return False
        if self._thread and self._thread.is_alive():
            return True
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="db-train-enricher", daemon=True
        )
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------ #
    # Refresco
    # ------------------------------------------------------------------ #
    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.time()
            try:
                self.refresh_once()
            except Exception as exc:  # noqa: BLE001
                print(f"[enrich] Error refrescando numeros de tren: {exc}")
            elapsed = time.time() - started
            self._stop.wait(max(5, self.refresh_seconds - elapsed))

    def refresh_once(self) -> None:
        """Repuebla el indice sondeando el plan (y cambios) de cada estacion.

        Para no dejar el indice vacio durante el primer barrido (32 estaciones
        tardan mas que el intervalo de refresco), cada estacion se resuelve en
        paralelo y se publica en el indice compartido en cuanto termina. Asi el
        enriquecimiento esta disponible de forma progresiva.
        """
        with self._lock:
            stations = list(self._stations)
        if not stations:
            return

        now = datetime.now()
        date = now.strftime("%y%m%d")
        hours = sorted(
            {(now + timedelta(hours=off)).hour for off in range(DB_ENRICH_PLAN_HOURS + 1)}
        )

        with ThreadPoolExecutor(max_workers=DB_ENRICH_WORKERS) as pool:
            futures = {
                pool.submit(self._fetch_station, eva, date, hours): eva
                for eva in stations
            }
            done = 0
            for future in as_completed(futures):
                eva = futures[future]
                done += 1
                try:
                    entries = future.result()
                except Exception as exc:  # noqa: BLE001
                    print(f"[enrich] Estacion {eva} fallo: {exc}")
                    entries = []
                self._publish_station(eva, entries)
                self.last_station_count = done

        with self._lock:
            self.last_refresh = time.time()
            self.last_entry_count = sum(len(v) for v in self._index.values())

    def _fetch_station(
        self, eva: str, date: str, hours: list[int]
    ) -> list[_EnrichedTrain]:
        """Descarga plan + cambios de una estacion y devuelve sus entradas."""
        entries: list[_EnrichedTrain] = []

        def collect(stop) -> None:
            if not stop.number:
                return
            label = stop.train_number
            moment = (
                stop.changed_arrival
                or stop.planned_arrival
                or stop.changed_departure
                or stop.planned_departure
            )
            if moment is None:
                return
            category = _category_prefix(stop.category or label)
            entries.append(
                _EnrichedTrain(
                    number_label=label,
                    line=stop.line or stop.category or label,
                    category=category,
                    minute_of_day=moment.hour * 60 + moment.minute,
                    day_key=moment.strftime("%Y%m%d"),
                )
            )

        for hour in hours:
            try:
                for stop in get_plan(eva, date, hour):
                    collect(stop)
            except Exception:  # noqa: BLE001 - una hora caida no aborta la estacion
                continue
        try:
            for stop in get_changes(eva).values():
                collect(stop)
        except Exception:  # noqa: BLE001
            pass
        return entries

    def _publish_station(self, eva: str, entries: list[_EnrichedTrain]) -> None:
        """Sustituye en el indice las entradas de una estacion."""
        grouped: dict[tuple, list[_EnrichedTrain]] = {}
        for entry in entries:
            grouped.setdefault(
                (eva, entry.day_key, entry.category), []
            ).append(entry)
        with self._lock:
            for key in [k for k in self._index if k[0] == eva]:
                del self._index[key]
            self._index.update(grouped)

    # ------------------------------------------------------------------ #
    # Consulta
    # ------------------------------------------------------------------ #
    def lookup(self, eva: str, line: str, when: datetime | None) -> str | None:
        """Numero de tren real para (estacion, linea, hora), si se conoce.

        Devuelve el tren de la misma categoria cuyo minuto de paso planificado
        este mas cerca del de ``when`` dentro de la tolerancia configurada.
        """
        if not eva or not line or when is None:
            return None
        with self._lock:
            if not self._index:
                return None
            index = self._index

        category = _category_prefix(line)
        day = when.strftime("%Y%m%d")
        target_minute = when.hour * 60 + when.minute
        candidates = index.get((eva, day, category))
        if not candidates:
            return None

        tolerance_minutes = max(1, DB_ENRICH_MATCH_TOLERANCE // 60)
        best: _EnrichedTrain | None = None
        best_delta = tolerance_minutes + 1
        for candidate in candidates:
            delta = abs(candidate.minute_of_day - target_minute)
            if delta < best_delta:
                best = candidate
                best_delta = delta
        if best is None or best_delta > tolerance_minutes:
            return None
        return best.number_label
