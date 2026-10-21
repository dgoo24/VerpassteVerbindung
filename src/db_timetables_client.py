"""Cliente de la API Timetables de Deutsche Bahn (DB API Marketplace).

Envuelve los endpoints de planificacion y cambios de una estacion y expone un
modelo sencillo (``TrainStop``) con los horarios planificados y reales, ademas
de utilidades para sintetizar transbordos a partir de la propia malla horaria.

La API es por estacion/hora: ``GET /plan/{evaNo}/{date}/{hour}`` devuelve el
horario planificado y ``GET /fchg/{evaNo}`` los cambios en tiempo real
(retrasos, cambios de anden, mensajes). Este modulo fusiona ambas fuentes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

import requests

from config import (
    DB_API_KEY,
    DB_CLIENT_ID,
    DB_TIMETABLES_BASE,
)

TIME_FMT = "%y%m%d%H%M"
REQUEST_TIMEOUT = 30


# --------------------------------------------------------------------------- #
# Modelo
# --------------------------------------------------------------------------- #
@dataclass
class TrainStop:
    """Un tren en una estacion, con horas planificadas y reales."""

    stop_id: str
    category: str = ""
    number: str = ""
    operator: str = ""
    station: str = ""
    planned_arrival: datetime | None = None
    changed_arrival: datetime | None = None
    planned_departure: datetime | None = None
    changed_departure: datetime | None = None
    planned_platform: str | None = None
    changed_platform: str | None = None
    messages: list[str] = field(default_factory=list)
    line: str = ""
    destination: str = ""

    @property
    def train_number(self) -> str:
        """Etiqueta legible del tren, p. ej. 'ICE 593'."""
        label = f"{self.category} {self.number}".strip()
        return label or self.stop_id

    @property
    def effective_arrival(self) -> datetime | None:
        return self.changed_arrival or self.planned_arrival

    @property
    def effective_departure(self) -> datetime | None:
        return self.changed_departure or self.planned_departure

    @staticmethod
    def _delay_seconds(
        planned: datetime | None, changed: datetime | None
    ) -> int:
        if planned is None or changed is None:
            return 0
        return int((changed - planned).total_seconds())

    @property
    def arrival_delay_seconds(self) -> int:
        return self._delay_seconds(self.planned_arrival, self.changed_arrival)

    @property
    def departure_delay_seconds(self) -> int:
        return self._delay_seconds(self.planned_departure, self.changed_departure)

    # Identificadores de telemetria (un tren puede llegar y salir).
    @property
    def arrival_trip_id(self) -> str:
        return f"{self.stop_id}#ARR"

    @property
    def departure_trip_id(self) -> str:
        return f"{self.stop_id}#DEP"


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _headers() -> dict[str, str]:
    return {
        "DB-Client-Id": DB_CLIENT_ID,
        "DB-Api-Key": DB_API_KEY,
        "Accept": "application/xml",
    }


def _get(path: str) -> ET.Element:
    url = f"{DB_TIMETABLES_BASE}{path}"
    response = requests.get(url, headers=_headers(), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return ET.fromstring(response.text)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, TIME_FMT)
    except ValueError:
        return None


def _attr(element: ET.Element | None, name: str) -> str | None:
    if element is None:
        return None
    return element.get(name)


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
def resolve_station(pattern: str) -> tuple[str, str]:
    """Devuelve ``(eva, nombre)`` de la estacion que mejor casa con ``pattern``."""
    root = _get(f"/station/{requests.utils.quote(pattern)}")
    station = root.find("station")
    if station is None:
        raise LookupError(f"Estacion no encontrada: {pattern!r}")
    return station.get("eva", ""), station.get("name", "")


def get_plan(eva: str, date: str, hour: int) -> list[TrainStop]:
    """Horario planificado de una estacion para una fecha y hora concretas."""
    root = _get(f"/plan/{eva}/{date}/{hour:02d}")
    stops: list[TrainStop] = []
    for s in root.findall("s"):
        tl = s.find("tl")
        if tl is None:
            continue
        ar = s.find("ar")
        dp = s.find("dp")
        stops.append(
            TrainStop(
                stop_id=s.get("id", ""),
                category=tl.get("c", ""),
                number=tl.get("n", ""),
                operator=tl.get("o", ""),
                station=root.get("station", ""),
                planned_arrival=_parse_dt(_attr(ar, "pt")),
                planned_departure=_parse_dt(_attr(dp, "pt")),
                planned_platform=_attr(ar, "pp") or _attr(dp, "pp"),
            )
        )
    return stops


def get_changes(eva: str) -> dict[str, TrainStop]:
    """Cambios en tiempo real de una estacion, indexados por ``stop_id``."""
    root = _get(f"/fchg/{eva}")
    changes: dict[str, TrainStop] = {}
    for s in root.findall("s"):
        sid = s.get("id", "")
        if not sid:
            continue
        ar = s.find("ar")
        dp = s.find("dp")
        tl = s.find("tl")
        stop = TrainStop(stop_id=sid, station=root.get("station", ""))
        if tl is not None:
            stop.category = tl.get("c", "")
            stop.number = tl.get("n", "")
            stop.operator = tl.get("o", "")
        if ar is not None:
            stop.planned_arrival = _parse_dt(_attr(ar, "pt"))
            stop.changed_arrival = _parse_dt(_attr(ar, "ct"))
            stop.planned_platform = _attr(ar, "pp")
            stop.changed_platform = _attr(ar, "cp")
        if dp is not None:
            if stop.planned_arrival is None:
                stop.planned_arrival = _parse_dt(_attr(dp, "pt"))
            stop.planned_departure = _parse_dt(_attr(dp, "pt"))
            stop.changed_departure = _parse_dt(_attr(dp, "ct"))
            if stop.planned_platform is None:
                stop.planned_platform = _attr(dp, "pp")
            if stop.changed_platform is None:
                stop.changed_platform = _attr(dp, "cp")
        stop.messages = [
            m.get("c", "") for m in s.iter("m") if m.get("c")
        ]
        changes[sid] = stop
    return changes


def _merge(base: TrainStop, change: TrainStop) -> None:
    """Aplica los cambios sobre el horario planificado (in place)."""
    if not base.category:
        base.category = change.category
    if not base.number:
        base.number = change.number
    if not base.operator:
        base.operator = change.operator
    if change.planned_arrival is not None:
        base.planned_arrival = change.planned_arrival
    if change.changed_arrival is not None:
        base.changed_arrival = change.changed_arrival
    if change.planned_departure is not None:
        base.planned_departure = change.planned_departure
    if change.changed_departure is not None:
        base.changed_departure = change.changed_departure
    if change.planned_platform:
        base.planned_platform = change.planned_platform
    if change.changed_platform:
        base.changed_platform = change.changed_platform
    if change.messages:
        base.messages = change.messages


def get_timetable(eva: str, hours_ahead: int = 1) -> list[TrainStop]:
    """Malla horaria fusionada (plan + cambios) para las proximas horas."""
    now = datetime.now()
    stops: dict[str, TrainStop] = {}
    for offset in range(hours_ahead + 1):
        moment = now + timedelta(hours=offset)
        for stop in get_plan(eva, moment.strftime("%y%m%d"), moment.hour):
            stops[stop.stop_id] = stop

    try:
        changes = get_changes(eva)
    except requests.HTTPError:
        changes = {}

    for sid, change in changes.items():
        base = stops.get(sid)
        if base is None:
            # Tren no presente en la ventana de plan: se incorpora igualmente.
            stops[sid] = change
        else:
            _merge(base, change)

    # El endpoint de cambios puede arrastrar trenes de dias anteriores: se
    # descarta todo lo que quede fuera de una ventana alrededor de "ahora".
    window_start = now - timedelta(minutes=30)
    window_end = now + timedelta(hours=hours_ahead + 1)
    return [
        stop
        for stop in stops.values()
        if _within_window(stop, window_start, window_end)
    ]


def _within_window(
    stop: TrainStop, start: datetime, end: datetime
) -> bool:
    """Comprueba si la llegada o la salida caen dentro de la ventana."""
    moments = [
        moment
        for moment in (stop.effective_arrival, stop.effective_departure)
        if moment is not None
    ]
    return any(start <= moment <= end for moment in moments)


# --------------------------------------------------------------------------- #
# Estimacion de viajeros (no hay datos reales de ocupacion en la API)
# --------------------------------------------------------------------------- #
# La logica vive en traveler_estimate.py (compartida con el productor
# nacional); se re-exporta aqui por compatibilidad.
from traveler_estimate import (  # noqa: E402,F401
    CONNECTION_LOAD_FACTOR,
    CONNECTION_LOAD_FACTOR_HIGH,
    CONNECTION_LOAD_FACTOR_LOW,
    REGIONAL_CAPACITY,
    TRAIN_CAPACITY_BY_CATEGORY,
    estimate_connection_travelers,
)


# --------------------------------------------------------------------------- #
# Sintesis de transbordos
# --------------------------------------------------------------------------- #
def synthesize_itineraries(
    stops: list[TrainStop],
    station_id: str,
    min_transfer_minutes: int,
    max_window_minutes: int,
    estimated_travelers: int | None = None,
) -> list[dict]:
    """Empareja cada llegada con la siguiente salida para simular transbordos.

    No existen itinerarios de pasajeros reales en la API, asi que se aproximan
    con la conexion programada mas ajustada de la estacion: una llegada y la
    siguiente salida con un margen planificado entre ``min_transfer_minutes`` y
    ``max_window_minutes``. Asi el transbordo es viable si todo circula puntual
    y solo entra en riesgo cuando el tren de origen acumula retraso.
    """
    arrivals = [
        (stop, stop.planned_arrival or stop.effective_arrival)
        for stop in stops
        if (stop.planned_arrival or stop.effective_arrival) is not None
    ]
    departures = [
        (stop, stop.planned_departure or stop.effective_departure)
        for stop in stops
        if (stop.planned_departure or stop.effective_departure) is not None
    ]

    itineraries: list[dict] = []
    for arrival_stop, arrival_time in arrivals:
        candidates: list[tuple[TrainStop, datetime, float]] = []
        for dep_stop, dep_time in departures:
            if dep_stop.stop_id == arrival_stop.stop_id:
                continue
            gap_minutes = (dep_time - arrival_time).total_seconds() / 60.0
            if min_transfer_minutes <= gap_minutes <= max_window_minutes:
                candidates.append((dep_stop, dep_time, gap_minutes))
        if not candidates:
            continue

        dep_stop, dep_time, _ = min(candidates, key=lambda item: item[2])
        if estimated_travelers is None:
            travelers_low, travelers, travelers_high = (
                estimate_connection_travelers(arrival_stop.category)
            )
        else:
            travelers_low = travelers = travelers_high = estimated_travelers
        itineraries.append(
            {
                "passenger_id": (
                    f"PAX-{arrival_stop.number}-{dep_stop.number}"
                ),
                "origin_trip_id": arrival_stop.arrival_trip_id,
                "destination_trip_id": dep_stop.departure_trip_id,
                "transfer_station_id": station_id,
                "min_transfer_time_minutes": min_transfer_minutes,
                "destination_scheduled_departure": dep_time.isoformat(),
                "estimated_travelers": travelers,
                "estimated_travelers_low": travelers_low,
                "estimated_travelers_high": travelers_high,
            }
        )

    return itineraries
