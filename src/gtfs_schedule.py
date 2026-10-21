"""Horario Fernverkehr de Alemania (GTFS-DE 'fv') cargado en memoria.

El productor nacional cruza cada TripUpdate del GTFS-RT nacional
(``realtime.gtfs.de``) contra este indice para recuperar los horarios
planificados, la linea (route_short_name) y el destino de cada tren.

El zip se descarga de ``download.gtfs.de`` (Creative Commons 4.0), se cachea
en ``data/gtfs_fv/latest.zip`` y solo se re-descarga si tiene mas de
``GTFS_REFRESH_DAYS`` dias. Solo se indexan los viajes cuyo servicio opera
hoy o manana (el GTFS permite horas > 24:00 tras la medianoche).
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from gtfs_regional import GTFS_FV_URL, GTFS_REFRESH_DAYS, _download

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
FV_ZIP_PATH = DATA_DIR / "gtfs_fv" / "latest.zip"

_WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def _hhmmss_to_seconds(value: str) -> int | None:
    """Convierte 'HH:MM:SS' (GTFS, horas > 24 permitidas) a segundos."""
    parts = value.split(":")
    if len(parts) < 2:
        return None
    try:
        hours, minutes = int(parts[0]), int(parts[1])
        seconds = int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        return None
    return hours * 3600 + minutes * 60 + seconds


@dataclass
class ScheduledStop:
    stop_sequence: int
    stop_id: str
    stop_name: str
    arrival_seconds: int | None
    departure_seconds: int | None


@dataclass
class TripInfo:
    trip_id: str
    line: str
    destination: str
    stops: dict[int, ScheduledStop] = field(default_factory=dict)
    stops_by_id: dict[str, ScheduledStop] = field(default_factory=dict)


class FernSchedule:
    """Indice en memoria de los viajes Fernverkehr activos (hoy + manana)."""

    def __init__(self) -> None:
        self.stops: dict[str, str] = {}
        self.trips: dict[str, TripInfo] = {}

    # ------------------------------------------------------------------ #
    # Carga
    # ------------------------------------------------------------------ #
    @staticmethod
    def ensure_zip(max_age_days: int = GTFS_REFRESH_DAYS) -> Path:
        """Descarga el fv si falta o esta caducado; devuelve la ruta."""
        FV_ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
        stale = (
            not FV_ZIP_PATH.exists()
            or FV_ZIP_PATH.stat().st_mtime < datetime.now().timestamp() - max_age_days * 86400
        )
        if stale:
            _download(GTFS_FV_URL, FV_ZIP_PATH)
        return FV_ZIP_PATH

    def load(self, zip_path: Path | None = None) -> None:
        zip_path = zip_path or self.ensure_zip()
        today = date.today()

        with zipfile.ZipFile(zip_path) as z:
            def rows(name: str):
                with z.open(name) as f:
                    yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))

            self.stops = {
                row["stop_id"]: row.get("stop_name", "")
                for row in rows("stops.txt")
            }

            routes = {
                row["route_id"]: row.get("route_short_name", "")
                for row in rows("routes.txt")
            }

            active_service_ids = self._active_service_ids(rows, today)

            trips: dict[str, TripInfo] = {}
            for row in rows("trips.txt"):
                if row.get("service_id") not in active_service_ids:
                    continue
                route_id = row.get("route_id", "")
                trips[row.get("trip_id", "")] = TripInfo(
                    trip_id=row.get("trip_id", ""),
                    line=routes.get(route_id, ""),
                    destination="",
                )
            if not trips:
                raise RuntimeError("fv: sin viajes activos para hoy/manana")

            for row in rows("stop_times.txt"):
                info = trips.get(row.get("trip_id", ""))
                if info is None:
                    continue
                stop_id = row.get("stop_id", "")
                try:
                    sequence = int(row.get("stop_sequence", "0"))
                except ValueError:
                    continue
                stop = ScheduledStop(
                    stop_sequence=sequence,
                    stop_id=stop_id,
                    stop_name=self.stops.get(stop_id, ""),
                    arrival_seconds=_hhmmss_to_seconds(row.get("arrival_time", "")),
                    departure_seconds=_hhmmss_to_seconds(row.get("departure_time", "")),
                )
                info.stops[sequence] = stop
                info.stops_by_id[stop_id] = stop
                if not info.destination and row.get("stop_headsign"):
                    info.destination = row["stop_headsign"]

            # Destino de respaldo: ultima parada del viaje.
            for info in trips.values():
                if not info.destination and info.stops:
                    last = max(info.stops.values(), key=lambda s: s.stop_sequence)
                    info.destination = last.stop_name

            self.trips = trips

    def _active_service_ids(self, rows, today: date) -> set[str]:
        """Servicios cuyos viajes pueden estar en curso: hoy y ayer (el GTFS
        permite horas > 24:00, asi que tras la medianoche siguen corriendo
        viajes del dia anterior)."""
        active: set[str] = set()
        yesterday = today - timedelta(days=1)
        for row in rows("calendar.txt"):
            try:
                start = datetime.strptime(row["start_date"], "%Y%m%d").date()
                end = datetime.strptime(row["end_date"], "%Y%m%d").date()
            except ValueError:
                continue
            relevant = [d for d in (today, yesterday) if start <= d <= end]
            if not relevant:
                continue
            if any(row.get(_WEEKDAYS[d.weekday()]) == "1" for d in relevant):
                active.add(row["service_id"])
        for row in rows("calendar_dates.txt"):
            try:
                when = datetime.strptime(row["date"], "%Y%m%d").date()
            except ValueError:
                continue
            if when not in (today, yesterday):
                continue
            if row.get("exception_type") == "1":
                active.add(row["service_id"])
            elif row.get("exception_type") == "2":
                active.discard(row["service_id"])
        return active

    # ------------------------------------------------------------------ #
    # Consultas
    # ------------------------------------------------------------------ #
    def stop_name(self, stop_id: str) -> str:
        return self.stops.get(stop_id, "")


def load_fern_schedule() -> FernSchedule:
    """Carga (una vez) el horario Fernverkehr completo en memoria."""
    schedule = FernSchedule()
    schedule.load()
    return schedule
