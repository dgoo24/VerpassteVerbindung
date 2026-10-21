"""Dashboard Streamlit en tiempo real - enfoque Centro de Control (operario).

Consume los topics de Kafka en un hilo de fondo y muestra, con auto-refresco:

* KPIs de red (trenes con retraso, enlaces en riesgo, pasajeros afectados...).
* Incidencias operativas priorizadas por severidad (ALTA -> MEDIA -> BAJA).
* Estado de todos los trenes de la red.
* Detalle por estacion con filtro.

Incluye mitigacion de fatiga de alarmas: orden estricto por severidad, filtro
por estacion y botones para marcar incidencias como resueltas.

Se lanza con:
    python -m streamlit run src/dashboard_app.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import socket
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from business_rules import (  # noqa: E402
    severity_label,
    severity_rank,
)
from config import (  # noqa: E402
    DASHBOARD_MODE,
    DASHBOARD_MAX_INCIDENTS,
    DASHBOARD_MAX_ITINERARIES,
    DASHBOARD_MAX_TELEMETRY,
    DASHBOARD_MAX_TRIP_LABELS,
    DASHBOARD_TABLE_ROW_LIMIT,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_OPERATIONAL_INCIDENTS,
    TOPIC_PASSENGER_ITINERARIES,
    TOPIC_TRAIN_TELEMETRY,
)

REFRESH_SECONDS = 2

# Indice trip_id -> numero de tren: se conserva mas tiempo que la telemetria
# para que las alertas/incidencias antiguas sigan mostrando el tren legible.
MAX_TRIP_LABELS = DASHBOARD_MAX_TRIP_LABELS

# Mapa ciudad -> Bundesland para los filtros de Region y Ciudad.
CITY_CSV = Path(__file__).resolve().parent.parent / "data" / "city_bundesland.csv"
UNKNOWN_LAND = "Unbekannt"
UNKNOWN_CITY = "Sin dato"

# Sufijos de nombre de estacion que no forman parte de la ciudad.
_STATION_SUFFIXES = {
    "Hbf",
    "Hauptbahnhof",
    "Bahnhof",
    "Bf",
    "Flughafen",
    "Terminal",
}

STATUS_RISK = "GEFÄHRDET"
STATUS_OK = "OK"
STATUS_UNKNOWN = "KEINE DATEN"


class LiveStore:
    """Estado en memoria actualizado por el hilo consumidor de Kafka."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.itineraries: dict[str, dict] = {}
        self.telemetry: dict[tuple[str, str], dict] = {}
        self.incidents: dict[tuple[str, str, str], dict] = {}
        self.trip_labels: dict[str, str] = {}
        self.resolved: set[tuple[str, str, str]] = set()
        self.incident_total = 0
        self.last_event_at: float | None = None
        # Como se alimentan los datos: "kafka" o "cloud" (pipeline in-process).
        self.source_mode = "kafka"

    # ------------------------------------------------------------------ #
    # Utilidades
    # ------------------------------------------------------------------ #
    @staticmethod
    def incident_key(value: dict) -> tuple[str, str, str]:
        return (value["station_id"], value["origin_trip"], value["destination_trip"])

    @staticmethod
    def _evict(container: dict, limit: int = DASHBOARD_MAX_TELEMETRY) -> None:
        """Elimina los registros mas antiguos cuando se supera el limite."""
        while len(container) > limit:
            oldest = next(iter(container))
            container.pop(oldest, None)

    # ------------------------------------------------------------------ #
    # Ingesta
    # ------------------------------------------------------------------ #
    def add(self, topic: str, value: dict) -> None:
        with self._lock:
            self.last_event_at = time.time()
            if topic == TOPIC_PASSENGER_ITINERARIES:
                self.itineraries[value["passenger_id"]] = value
                self._evict(self.itineraries, DASHBOARD_MAX_ITINERARIES)
            elif topic == TOPIC_TRAIN_TELEMETRY:
                self.telemetry[(value["trip_id"], value["station_id"])] = value
                self.trip_labels[value["trip_id"]] = value["train_number"]
                self._evict(self.telemetry, DASHBOARD_MAX_TELEMETRY)
                self._evict(self.trip_labels, MAX_TRIP_LABELS)
            elif topic == TOPIC_OPERATIONAL_INCIDENTS:
                self.incidents[self.incident_key(value)] = value
                self.incident_total += 1
                self._evict(self.incidents, DASHBOARD_MAX_INCIDENTS)

    # ------------------------------------------------------------------ #
    # Gestion de incidencias (fatiga de alarmas)
    # ------------------------------------------------------------------ #
    def resolve_incident(self, key: tuple[str, str, str]) -> None:
        with self._lock:
            self.resolved.add(key)

    def resolve_all(self) -> None:
        with self._lock:
            self.resolved.update(self.incidents.keys())

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "itineraries": dict(self.itineraries),
                "telemetry": dict(self.telemetry),
                "incidents": dict(self.incidents),
                "trip_labels": dict(self.trip_labels),
                "resolved": set(self.resolved),
                "incident_total": self.incident_total,
                "last_event_at": self.last_event_at,
            }


def _kafka_reachable(timeout: float = 2.0) -> bool:
    """Comprueba si hay un broker Kafka escuchando en el bootstrap configurado."""
    for server in KAFKA_BOOTSTRAP_SERVERS.split(","):
        server = server.strip()
        if not server:
            continue
        host, _, port = server.partition(":")
        try:
            with socket.create_connection(
                (host or "localhost", int(port or 9092)), timeout=timeout
            ):
                return True
        except (OSError, ValueError):
            continue
    return False


@st.cache_resource
def start_consumer() -> LiveStore:
    """Arranca (una sola vez) la fuente de datos del dashboard.

    Hay dos modos, gobernados por ``DASHBOARD_MODE``:

    * ``kafka`` - consume los topics de Kafka (desarrollo local con broker).
    * ``cloud`` - ejecuta el pipeline dentro del propio proceso: es lo que
      permite desplegar la app en un hosting sin broker (Streamlit Cloud).
    * ``auto``  - usa Kafka si el broker responde; si no, cae a ``cloud``.
    """
    store = LiveStore()

    mode = DASHBOARD_MODE
    if mode not in {"kafka", "cloud", "auto"}:
        mode = "auto"

    if mode in {"auto", "cloud"} and (
        mode == "cloud" or not _kafka_reachable()
    ):
        # Sin broker (hosting de una sola instancia): los datos se generan
        # dentro del proceso con los feeds publicos de gtfs.de.
        from cloud_pipeline import CloudPipeline

        pipeline = CloudPipeline(store)
        pipeline.start()
        store.pipeline = pipeline
        store.source_mode = "cloud"
        print("[dashboard] Modo cloud: pipeline de datos in-process.")
        return store

    def run() -> None:
        while True:
            try:
                consumer = KafkaConsumer(
                    TOPIC_PASSENGER_ITINERARIES,
                    TOPIC_TRAIN_TELEMETRY,
                    TOPIC_OPERATIONAL_INCIDENTS,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id="dashboard-view",
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                )
                for message in consumer:
                    store.add(message.topic, message.value)
            except Exception:  # noqa: BLE001
                time.sleep(3)

    threading.Thread(target=run, daemon=True).start()
    store.source_mode = "kafka"
    return store


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _strip_tz(value: datetime) -> datetime:
    """Hora local naive: si trae zona horaria, se descarta (Europe/Berlin)."""
    return value.replace(tzinfo=None)


_DIAS_DE = [
    "Montag",
    "Dienstag",
    "Mittwoch",
    "Donnerstag",
    "Freitag",
    "Samstag",
    "Sonntag",
]
_MESES_DE = [
    "Januar",
    "Februar",
    "März",
    "April",
    "Mai",
    "Juni",
    "Juli",
    "August",
    "September",
    "Oktober",
    "November",
    "Dezember",
]


def _fecha_hoy() -> str:
    """Fecha de hoy en aleman (p. ej. 'Dienstag, 13. Oktober 2026')."""
    now = datetime.now()
    return f"{_DIAS_DE[now.weekday()]}, {now.day}. {_MESES_DE[now.month - 1]} {now.year}"


def _solo_hora(value: str) -> str:
    """Devuelve solo la hora HH:MM de una marca de tiempo ISO (sin el dia)."""
    try:
        return datetime.fromisoformat(value).strftime("%H:%M")
    except (TypeError, ValueError):
        return str(value)


# --------------------------------------------------------------------------- #
# Meta de estaciones y filtros Region / Ciudad / Bahnhof
# --------------------------------------------------------------------------- #
@st.cache_data
def load_city_lands() -> dict[str, str]:
    """Mapa ciudad -> Bundesland (data/city_bundesland.csv)."""
    mapping: dict[str, str] = {}
    try:
        with CITY_CSV.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                mapping[row["stadt"].strip()] = row["bundesland"].strip()
    except OSError:
        pass
    return mapping


def _city_from_name(station_name: str) -> str:
    """'Frankfurt (Main) Hbf' -> 'Frankfurt'; 'Bahnhof, Elsterwerda' -> 'Elsterwerda'."""
    tokens = []
    for token in station_name.replace(",", " ").split():
        cleaned = token.strip(",.;")
        if cleaned in _STATION_SUFFIXES:
            continue
        tokens.append(token.strip(",.;"))
    city = " ".join(tokens).strip()
    return city or station_name


def _land_for_city(city: str, mapping: dict[str, str]) -> str:
    if city in mapping:
        return mapping[city]
    base = re.sub(r"\s*\(.*\)", "", city).strip()
    if base in mapping:
        return mapping[base]
    if base:
        first = base.split()[0]
        if first in mapping:
            return mapping[first]
    return UNKNOWN_LAND


def build_station_meta(state: dict) -> dict[str, dict]:
    """station_id -> {name, stadt, land} a partir de la telemetria recibida."""
    mapping = load_city_lands()
    meta: dict[str, dict] = {}
    for (_trip, station_id), telemetry in state["telemetry"].items():
        if station_id in meta:
            continue
        name = telemetry.get("station_name") or station_id
        city = _city_from_name(name)
        meta[station_id] = {
            "name": name,
            "stadt": city,
            "land": _land_for_city(city, mapping),
        }
    return meta


def render_filters(meta: dict[str, dict]) -> dict:
    """Filtros de Region y Ciudad dentro del dashboard."""
    lands = sorted({m["land"] for m in meta.values()})
    if UNKNOWN_LAND in lands and len(lands) > 1:
        lands = sorted([l for l in lands if l != UNKNOWN_LAND]) + [UNKNOWN_LAND]

    if "filter_land" not in st.session_state:
        st.session_state["filter_land"] = "Alle"

    with st.container():
        c1, c2, c3 = st.columns([4, 3, 1])
        with c1:
            land = st.selectbox(
                "Bundesland",
                ["Alle"] + lands,
                index=(["Alle"] + lands).index(st.session_state.get("filter_land", "Alle"))
                if st.session_state.get("filter_land", "Alle") in (["Alle"] + lands)
                else 0,
                label_visibility="collapsed",
                key="filter_land_sb",
            )
            st.session_state["filter_land"] = land

        cities = sorted(
            {
                m["stadt"]
                for m in meta.values()
                if land == "Alle" or m["land"] == land
            }
        )
        city_options = ["Alle Städte"] + cities
        current_city = st.session_state.get("filter_city", "Alle Städte")
        if current_city not in city_options:
            current_city = "Alle Städte"
            st.session_state["filter_city"] = "Alle Städte"

        with c2:
            city = st.selectbox(
                "Stadt",
                city_options,
                index=city_options.index(current_city),
                label_visibility="collapsed",
                key="filter_city_sb",
            )
            st.session_state["filter_city"] = city if city != "Alle Städte" else "Alle"

        with c3:
            if st.button(
                "×",
                width="stretch",
                help="Filter zurücksetzen",
                key="filter_reset",
            ):
                st.session_state["filter_land"] = "Alle"
                st.session_state["filter_city"] = "Alle Städte"
                st.session_state.pop("filter_land_sb", None)
                st.session_state.pop("filter_city_sb", None)
                st.rerun()

    return {"land": st.session_state.get("filter_land", "Alle"), "stadt": st.session_state.get("filter_city", "Alle") if st.session_state.get("filter_city", "Alle Städte") != "Alle Städte" else "Alle"}


def filter_state(state: dict, meta: dict, filters: dict) -> dict:
    """Copia del estado limitada a la Region/Ciudad seleccionadas."""

    def allowed(station_id: str) -> bool:
        info = meta.get(station_id)
        if filters["land"] != "Alle" and (info or {}).get("land") != filters["land"]:
            return False
        if filters["stadt"] != "Alle" and (info or {}).get("stadt") != filters["stadt"]:
            return False
        return True

    return {
        **state,
        "telemetry": {
            key: value
            for key, value in state["telemetry"].items()
            if allowed(value["station_id"])
        },
        "itineraries": {
            key: value
            for key, value in state["itineraries"].items()
            if allowed(value["transfer_station_id"])
        },
        "incidents": {
            key: value
            for key, value in state["incidents"].items()
            if allowed(value["station_id"])
        },
    }


def _station_label(station_id: str, meta: dict[str, dict]) -> str:
    """Nombre legible de la estacion si lo sabemos; si no, el id."""
    return (meta.get(station_id) or {}).get("name") or station_id


def _train_by_trip(state: dict) -> dict[str, str]:
    """Mapa trip_id -> numero de tren legible (p. ej. 'ICE 593').

    Se complementa con el indice persistente trip_labels, que sobrevive al
    desalojo de la telemetria (para alertas e incidencias antiguas).
    """
    by_trip = {
        trip_id: telemetry["train_number"]
        for (trip_id, _station), telemetry in state["telemetry"].items()
    }
    by_trip.update(state.get("trip_labels", {}))
    return by_trip


def _trip_label(value: str, by_trip: dict[str, str]) -> str:
    """Devuelve el numero de tren si se conoce el trip_id; si no, el codigo."""
    return by_trip.get(value, value)


def evaluate_transfer(itinerary: dict, telemetry: dict) -> tuple[float | None, str]:
    """Calcula el tiempo disponible (min) y el estado de un transbordo."""
    origin = telemetry.get(
        (itinerary["origin_trip_id"], itinerary["transfer_station_id"])
    )
    if origin is None:
        return None, STATUS_UNKNOWN

    destination = telemetry.get(
        (itinerary["destination_trip_id"], itinerary["transfer_station_id"])
    )
    departure_raw = (
        destination["estimated_arrival"]
        if destination is not None
        else itinerary["destination_scheduled_departure"]
    )

    available = (
        _parse(departure_raw) - _parse(origin["estimated_arrival"])
    ).total_seconds() / 60.0
    required = itinerary["min_transfer_time_minutes"]
    return available, (STATUS_RISK if available < required else STATUS_OK)


# --------------------------------------------------------------------------- #
# Estilos (paleta DB: rojo #EC0016, ambar de retraso, verde puntual)
# --------------------------------------------------------------------------- #
def _style_severity(value: str) -> str:
    rank = severity_rank(value)
    if rank == 0:
        return (
            "background-color: #EC0016; color: #ffffff; font-weight: 700;"
            "border-left: 4px solid #FF6B7A;"
        )
    if rank == 1:
        return (
            "background-color: #3A2A06; color: #FFD98A; font-weight: 600;"
            "border-left: 4px solid #FFB000;"
        )
    return (
        "background-color: #0E2A40; color: #B9DEF7; font-weight: 600;"
        "border-left: 4px solid #4CA6E8;"
    )


def _style_delay(value: float) -> str:
    if value is None:
        return ""
    if value >= 10:
        return (
            "background-color: #3A1016; color: #FFB4BC;"
            "border-left: 4px solid #EC0016;"
        )
    if value > 0:
        return (
            "background-color: #3A2A06; color: #FFD98A;"
            "border-left: 4px solid #FFB000;"
        )
    return (
        "background-color: #0E2C1C; color: #9CE6B8;"
        "border-left: 4px solid #3DD68C;"
    )


def _style_affected(value: object) -> str:
    if value in (None, "", 0, "0"):
        return ""
    return (
        "background-color: #3A1016; color: #FFB4BC; font-weight: 700;"
        "border-left: 4px solid #EC0016;"
    )


_SEVERITY_BADGE = {0: "red", 1: "orange", 2: "blue"}


def _severity_badge(value: str) -> str:
    """Insignia nativa de Streamlit con los colores de la casa DB."""
    rank = severity_rank(value)
    color = _SEVERITY_BADGE.get(rank, "gray")
    return f":{color}-badge[{severity_label(value)}]"


def format_affected(low: int | None, mid: int, high: int | None) -> str:
    """Representa una estimacion con incertidumbre: rango si low/high difieren
    del punto medio (p. ej. '155-289'); si no, solo el numero."""
    if low and high and low != high:
        return f"{low}-{high}"
    return str(mid)


def _itinerary_triple(itinerary: dict) -> tuple[int, int, int]:
    """(baja, media, alta) de una cohort de viajeros con fallback al punto
    medio cuando el itinerario no trae rango."""
    mid = int(itinerary.get("estimated_travelers", 1))
    low = itinerary.get("estimated_travelers_low") or mid
    high = itinerary.get("estimated_travelers_high") or mid
    return low, mid, high


# --------------------------------------------------------------------------- #
# Construccion de tablas
# --------------------------------------------------------------------------- #
def incidents_frame(state: dict, meta: dict[str, dict]) -> pd.DataFrame:
    by_trip = _train_by_trip(state)
    rows = []
    for key, inc in state["incidents"].items():
        if key in state["resolved"]:
            continue
        rows.append(
            {
                "Schweregrad": severity_label(inc["severity"]),
                "Bahnhof": _station_label(inc["station_id"], meta),
                "Ankunftszug": _trip_label(inc["origin_trip"], by_trip),
                "Abfahrtszug": _trip_label(inc["destination_trip"], by_trip),
                "Betroffene": format_affected(
                    inc.get("passengers_affected_low"),
                    inc["passengers_affected"],
                    inc.get("passengers_affected_high"),
                ),
                "Defizit (min)": inc["deficit_minutes"],
                "Verspätung (min)": round(inc.get("delay_seconds", 0) / 60.0, 1),
                "Maßnahme": inc["recommended_action"],
                "Zeit": _solo_hora(inc["created_at"]),
                "_rank": severity_rank(inc["severity"]),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values(
            ["_rank", "Zeit"], ascending=[True, False]
        ).drop(columns=["_rank"])
        frame = frame.head(DASHBOARD_TABLE_ROW_LIMIT)
    return frame


def affected_by_train(state: dict, train_number: str) -> tuple[int, int, int]:
    """Viajeros en riesgo asociados a un tren: (baja, media, alta)."""
    train_trips = {
        trip_id
        for (trip_id, _station), telemetry in state["telemetry"].items()
        if telemetry["train_number"] == train_number
    }
    low_sum = mid_sum = high_sum = 0
    for itinerary in state["itineraries"].values():
        _, status = evaluate_transfer(itinerary, state["telemetry"])
        if status != STATUS_RISK:
            continue
        if (
            itinerary["origin_trip_id"] in train_trips
            or itinerary["destination_trip_id"] in train_trips
        ):
            low, mid, high = _itinerary_triple(itinerary)
            low_sum += low
            mid_sum += mid
            high_sum += high
    return low_sum, mid_sum, high_sum


def network_frame(state: dict, meta: dict[str, dict]) -> pd.DataFrame:
    merged: dict[tuple[str, str], dict] = {}
    for (_trip_id, station_id), telemetry in state["telemetry"].items():
        train_number = telemetry["train_number"]
        key = (train_number, station_id)
        delay_min = round(telemetry["delay_seconds"] / 60.0, 1)
        arrival = _strip_tz(_parse(telemetry["estimated_arrival"]))
        row = merged.get(key)
        if row is None:
            merged[key] = {
                "Zug": train_number,
                "Linie": telemetry.get("line") or train_number,
                "Ziel": telemetry.get("destination", ""),
                "Bahnhof": _station_label(station_id, meta),
                "Verspätung (min)": delay_min,
                "Ankunft (voraussichtl.)": arrival,
            }
        else:
            row["Verspätung (min)"] = max(row["Verspätung (min)"], delay_min)
            if arrival > row["Ankunft (voraussichtl.)"]:
                row["Ankunft (voraussichtl.)"] = arrival

    rows = []
    now = datetime.now()
    for (_train_number, _station_id), row in merged.items():
        # Solo trenes aun no pasados: si la hora estimada ya es del pasado,
        # el tren ya paso por la estacion y no debe seguir en el panel.
        if row["Ankunft (voraussichtl.)"] < now:
            continue
        rows.append(
            {
                **row,
                "Ankunft (voraussichtl.)": row["Ankunft (voraussichtl.)"].strftime("%H:%M"),
                "Betroffene": format_affected(*affected_by_train(state, _train_number)),
            }
        )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values("Verspätung (min)", ascending=False)
        frame = frame.head(DASHBOARD_TABLE_ROW_LIMIT)
    return frame


def compute_kpis(state: dict) -> dict:
    telemetry = state["telemetry"]
    onboarding = len(telemetry)
    delayed = sum(1 for t in telemetry.values() if t["delay_seconds"] > 0)
    punctuality = (
        round(100 * (onboarding - delayed) / onboarding, 1) if onboarding else 0.0
    )

    at_risk = 0
    affected = affected_low = affected_high = 0
    for itinerary in state["itineraries"].values():
        _, status = evaluate_transfer(itinerary, telemetry)
        if status == STATUS_RISK:
            at_risk += 1
            low, mid, high = _itinerary_triple(itinerary)
            affected += mid
            affected_low += low
            affected_high += high

    return {
        "trains": onboarding,
        "delayed": delayed,
        "punctuality": punctuality,
        "at_risk": at_risk,
        "affected": affected,
        "affected_low": affected_low,
        "affected_high": affected_high,
        "open_incidents": sum(
            1 for key in state["incidents"] if key not in state["resolved"]
        ),
    }


def active_incidents(state: dict) -> list[tuple[tuple[str, str, str], dict]]:
    items = [
        (key, inc)
        for key, inc in state["incidents"].items()
        if key not in state["resolved"]
    ]
    items.sort(
        key=lambda kv: (severity_rank(kv[1]["severity"]), kv[1]["created_at"]),
    )
    return items


# --------------------------------------------------------------------------- #
# Tema visual "Betriebsleitstelle DB"
# --------------------------------------------------------------------------- #
_DB_CSS = """
<style>
/* --------------------------- Barra de cabecera --------------------------- */
.db-header{
  display:flex; align-items:center; gap:1rem;
  background:linear-gradient(100deg,#1C2029 0%,#15181E 55%,#111318 100%);
  border:1px solid #2A2F38; border-left:6px solid #EC0016;
  border-radius:10px; padding:0.85rem 1.15rem; margin:0 0 0.75rem 0;
  box-shadow:0 20px 45px -35px rgba(0,0,0,0.95);
}
.db-logo{
  width:50px; height:50px; flex:0 0 50px; border-radius:9px;
  background:#EC0016; color:#fff;
  display:flex; align-items:center; justify-content:center;
  font-family:'Barlow',sans-serif; font-weight:800; font-size:1.35rem; letter-spacing:0.04em;
  box-shadow:0 10px 24px -12px rgba(236,0,22,0.9);
}
.db-title{
  font-weight:800; font-size:clamp(1.2rem,2.4vw,1.8rem);
  line-height:1.1; color:#ffffff; letter-spacing:0.01em; margin:0;
}
.db-sub{
  color:#98A2B3; font-size:0.84rem; line-height:1.45; margin-top:0.2rem;
}
.db-meta{margin-left:auto; text-align:right;}
.db-date{
  color:#E6E9EF; font-weight:600; font-size:0.98rem; white-space:nowrap;
}
.db-live{
  display:inline-flex; align-items:center; gap:0.4rem;
  font-size:0.75rem; font-weight:700; letter-spacing:0.08em; margin-top:0.3rem;
}
.db-live.ok{color:#9CE6B8;}
.db-live.off{color:#FFB4BC;}
.db-dot{
  width:8px; height:8px; border-radius:50%; background:currentColor;
}
.db-live.ok .db-dot{animation:db-pulse 2s infinite;}
@keyframes db-pulse{
  0%{box-shadow:0 0 0 0 rgba(61,214,140,0.65);}
  70%{box-shadow:0 0 0 9px rgba(61,214,140,0);}
  100%{box-shadow:0 0 0 0 rgba(61,214,140,0);}
}
/* Franja roja estilo ICE sobre el contenido */
.db-stripe{
  height:3px; border-radius:999px; margin:0 0 0.9rem 0;
  background:linear-gradient(90deg,#EC0016 0%,#EC0016 38%,#2A2F38 38%,#2A2F38 100%);
}

/* ------------------------------ Tipografia ------------------------------ */
[data-testid="stTitle"] h1{color:#ffffff;}
[data-testid="stSubheader"] h2,
[data-testid="stSubheader"] h3{
  color:#ffffff; border-left:4px solid #EC0016;
  padding-left:0.6rem; margin:0 0 0.15rem 0;
}
[data-testid="stCaptionContainer"] p{color:#98A2B3;}
[data-testid="stMarkdownContainer"] code{
  background:#2A1216; color:#FFB4BC; border:1px solid #4A1B22;
  border-radius:4px; padding:0.05rem 0.35rem;
  font-family:'IBM Plex Mono',monospace; font-size:0.78rem;
}

/* --------------------------------- KPIs --------------------------------- */
div[data-testid="stMetric"]{
  background:linear-gradient(180deg,#1A1E25 0%,#14171C 100%);
  border:1px solid #2A2F38; border-top:3px solid #EC0016;
  border-radius:8px; padding:0.85rem 0.95rem;
}
div[data-testid="stMetricLabel"]{
  color:#98A2B3; text-transform:uppercase;
  font-size:0.72rem; letter-spacing:0.08em; font-weight:600;
}
div[data-testid="stMetricValue"]{
  color:#ffffff; font-size:1.85rem; font-weight:700; line-height:1.15;
}
div[data-testid="stMetricDelta"]{font-size:0.78rem;}

/* ------------------------------ Contenedores ----------------------------- */
div[data-testid="stVerticalBlockBorderWrapper"]{
  background:#14171C; border:1px solid #2A2F38; border-radius:10px;
}

/* -------------------------------- Tablas --------------------------------- */
[data-testid="stDataFrame"]{
  border:1px solid #2A2F38; border-radius:8px; overflow:hidden;
}
[data-testid="stDataFrameHeader"]{
  background-color:#22272F; color:#FFFFFF;
}

/* -------------------------------- Filtros -------------------------------- */
div[data-testid="stSelectbox"] > label{display:none;}
div[data-testid="stSelectbox"] div[data-baseweb="select"] > div{
  background:#191C22; border:1px solid #2A2F38; border-radius:6px;
  font-weight:600; height:42px; color:#E6E9EF;
  box-shadow:inset 0 1px 0 rgba(255,255,255,0.03);
  transition:all 0.15s ease-in-out;
}
div[data-testid="stSelectbox"] div[data-baseweb="select"]:hover > div{
  border-color:#EC0016;
}
div[data-testid="stSelectbox"] svg{color:#EC0016;}

/* -------------------------------- Botones -------------------------------- */
div.stButton > button[kind="primary"]{
  background:#EC0016; border:1px solid #EC0016; color:#ffffff;
  font-weight:700; letter-spacing:0.02em; transition:all 0.15s ease-in-out;
}
div.stButton > button[kind="primary"]:hover{
  background:#B80011; border-color:#B80011; color:#ffffff;
}
div.stButton > button[kind="secondary"]{
  background:#191C22; border:1px solid #2A2F38; color:#E6E9EF;
  transition:all 0.15s ease-in-out;
}
div.stButton > button[kind="secondary"]:hover{
  border-color:#EC0016; color:#FF8090;
}

/* ------------------------------- Avisos ---------------------------------- */
[data-testid="stInfo"]{
  background:rgba(76,166,232,0.12); border:1px solid #1E4C74; color:#B9DEF7;
}
[data-testid="stSuccess"]{
  background:rgba(61,214,140,0.10); border:1px solid #1B6B45; color:#9CE6B8;
}
[data-testid="stWarning"]{
  background:rgba(255,176,0,0.12); border:1px solid #7A5600; color:#FFD98A;
}
[data-testid="stError"]{
  background:rgba(236,0,22,0.14); border:1px solid #7A1420; color:#FFB4BC;
}
</style>
"""


def _inject_db_theme() -> None:
    """Aplica la identidad visual DB (la paleta base vive en config.toml)."""
    st.html(_DB_CSS)


def _render_header(last_event_at: float | None) -> None:
    """Cabecera tipo Leitstelle: logotipo DB, titulo, fecha y estado del bus."""
    live_ok = last_event_at is not None and (time.time() - last_event_at) < 30
    if last_event_at is None:
        estado, clase = "VERBINDUNG FEHLT", "off"
    elif live_ok:
        estado, clase = "LIVE · KAFKA OK", "ok"
    else:
        estado, clase = "STANDBY · KEINE DATEN", "off"

    st.html(
        f"""
        <div class="db-header">
          <div class="db-logo">DB</div>
          <div>
            <div class="db-title">Betriebsleitstelle Bahn</div>
            <div class="db-sub">
              MVP „Ereignisse &amp; Streaming“ · Echtzeit-Überwachung von
              Verspätungen und Anschluss-Störungen im Fernverkehr
            </div>
          </div>
          <div class="db-meta">
            <div class="db-date">{_fecha_hoy()}</div>
            <div class="db-live {clase}">
              <span class="db-dot"></span>{estado}
            </div>
          </div>
        </div>
        <div class="db-stripe"></div>
        """
    )


# --------------------------------------------------------------------------- #
# Render
# --------------------------------------------------------------------------- #
@st.fragment(run_every=REFRESH_SECONDS)
def live_dashboard() -> None:
    store = start_consumer()
    full_state = store.snapshot()
    meta = build_station_meta(full_state)

    _render_header(full_state["last_event_at"])

    filters = render_filters(meta)

    state = filter_state(full_state, meta, filters)
    kpis = compute_kpis(state)

    last = state["last_event_at"]
    source = (
        "GTFS-RT direkt (kein Broker)"
        if getattr(store, "source_mode", "kafka") == "cloud"
        else f"Kafka ({KAFKA_BOOTSTRAP_SERVERS})"
    )
    if last is None:
        st.caption(f"Warte auf Ereignisse … Quelle: {source}")
        pipeline = getattr(store, "pipeline", None)
        if pipeline is not None:
            st.caption(f"Pipeline-Status: {pipeline.status}")
    else:
        scope = filters["land"] if filters["land"] != "Alle" else "Ganz Deutschland"
        if filters["stadt"] != "Alle":
            scope = f"{filters['stadt']} · {scope}"
        st.caption(
            f"Letztes Ereignis: "
            f"{datetime.fromtimestamp(last).strftime('%H:%M:%S')} · "
            f"Auto-Refresh alle {REFRESH_SECONDS}s · Scope: {scope} · "
            f"Quelle: {source}"
        )

    with st.container(horizontal=True):
        st.metric("Überwachte Züge", kpis["trains"])
        st.metric("Züge mit Verspätung", kpis["delayed"])
        st.metric("Pünktlichkeit", f"{kpis['punctuality']}%")
        st.metric("Offene Störungen", kpis["open_incidents"])
        st.metric(
            "Betroffene Reisende (est.)",
            format_affected(
                kpis["affected_low"], kpis["affected"], kpis["affected_high"]
            ),
            help="Spanne aus Niedrig-/Hochrechnung der Fahrgastschätzung.",
        )

    render_operator_panel(state, store, meta)


def render_operator_panel(
    state: dict, store: LiveStore, meta: dict[str, dict]
) -> None:
    with st.container(border=True):
        st.subheader(
            "Priorisierte Störungen", icon=":material/warning:"
        )
        frame = incidents_frame(state, meta)
        if frame.empty:
            st.success("Keine offenen Störungen. Betrieb läuft.")
        else:
            st.dataframe(
                frame.style.map(_style_severity, subset=["Schweregrad"]),
                hide_index=True,
                column_config={
                    "Defizit (min)": st.column_config.NumberColumn(format="%.1f"),
                    "Verspätung (min)": st.column_config.NumberColumn(format="%.1f"),
                },
                alt="Betriebsstörungen nach Schweregrad sortiert",
            )

    with st.container(border=True):
        st.subheader(
            "Störungsmanagement", icon=":material/task_alt:"
        )
        items = active_incidents(state)[:DASHBOARD_TABLE_ROW_LIMIT]
        by_trip = _train_by_trip(state)
        if not items:
            st.caption("Keine Störungen zu bearbeiten.")
        else:
            if st.button("Alle als bearbeitet markieren"):
                store.resolve_all()
                st.rerun()
            for key, inc in items:
                col_info, col_btn = st.columns([6, 1])
                with col_info:
                    st.markdown(
                        f"{_severity_badge(inc['severity'])} "
                        f"**{_station_label(inc['station_id'], meta)}** · "
                        f"{_trip_label(inc['origin_trip'], by_trip)} → "
                        f"{_trip_label(inc['destination_trip'], by_trip)} · "
                        f"Defizit {inc['deficit_minutes']} min · "
                        f"{format_affected(inc.get('passengers_affected_low'), inc['passengers_affected'], inc.get('passengers_affected_high'))} Reisende (est.)"
                    )
                with col_btn:
                    if st.button(
                        "Beheben",
                        key=f"resolve|{key[0]}|{key[1]}|{key[2]}",
                        type="primary",
                    ):
                        store.resolve_incident(key)
                        st.rerun()

    with st.container(border=True):
        st.subheader(
            "Status aller Züge", icon=":material/directions_railway:"
        )
        st.caption(
            "Echtzeit aus dem bundesweiten GTFS-RT-Stream (gtfs.de), "
            "gekreuzt mit den Fernverkehrsfahrplänen der DB."
        )
        frame = network_frame(state, meta)
        if frame.empty:
            st.info("Noch keine Zugtelemetrie empfangen.")
        else:
            st.dataframe(
                frame.style.map(_style_delay, subset=["Verspätung (min)"]).map(
                    _style_affected, subset=["Betroffene"]
                ),
                hide_index=True,
                column_config={
                    "Verspätung (min)": st.column_config.NumberColumn(format="%.1f"),
                    "Betroffene": st.column_config.TextColumn(
                        help="Geschätzte Spanne der betroffenen Reisenden."
                    ),
                },
                alt="Verspätungsstatus aller Züge im Netz",
            )


def main() -> None:
    st.set_page_config(
        page_title="Betriebsleitstelle Bahn | Fernverkehr Deutschland",
        page_icon=":material/directions_railway:",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    _inject_db_theme()
    start_consumer()
    live_dashboard()


if __name__ == "__main__":
    main()
