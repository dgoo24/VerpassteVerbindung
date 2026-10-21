"""Configuracion central del MVP de deteccion de transbordos perdidos.

Contiene la conexion a Kafka, los nombres de los topics, la URL del servidor
GTFS-RT simulado, los modelos Pydantic que describen los eventos y los datos
de demostracion (trenes e itinerarios de pasajeros).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field

try:  # python-dotenv es opcional: si falta, se usan las variables de entorno.
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except Exception:  # noqa: BLE001
    pass

# --------------------------------------------------------------------------- #
# Kafka
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC_TRAIN_TELEMETRY = "train-telemetry"
TOPIC_PASSENGER_ITINERARIES = "passenger-itineraries"
TOPIC_OPERATIONAL_INCIDENTS = "operational-incidents"

TOPICS = [
    TOPIC_TRAIN_TELEMETRY,
    TOPIC_PASSENGER_ITINERARIES,
    TOPIC_OPERATIONAL_INCIDENTS,
]

# --------------------------------------------------------------------------- #
# Servidor GTFS-RT simulado
# --------------------------------------------------------------------------- #
GTFS_RT_HOST = "0.0.0.0"
GTFS_RT_PORT = 8080
GTFS_RT_PATH = "/gtfs-rt"
GTFS_RT_URL = f"http://localhost:{GTFS_RT_PORT}{GTFS_RT_PATH}"
GTFS_RT_REFRESH_SECONDS = 10

# Cadencia con la que el productor envia telemetria al topic.
TELEMETRY_INTERVAL_SECONDS = 5

# --------------------------------------------------------------------------- #
# Fuente de datos: "nationwide" (GTFS-RT nacional), "db" (API DB Timetables,
# una estacion) o "mock" (demo local).
# --------------------------------------------------------------------------- #
DATA_SOURCE = os.getenv("DATA_SOURCE", "mock").strip().lower()

# GTFS-RT nacional (gtfs.de, beta, cada 10 s): TripUpdates de toda Alemania
# (Fernverkehr + feeds locales agregados). Se cruza con el horario estatico fv.
GTFS_RT_NATIONAL_URL = os.getenv(
    "GTFS_RT_NATIONAL_URL", "https://realtime.gtfs.de/realtime-free.pb"
)
NATIONWIDE_REFRESH_SECONDS = int(os.getenv("NATIONWIDE_REFRESH_SECONDS", "60"))
NATIONWIDE_MAX_STATIONS = int(os.getenv("NATIONWIDE_MAX_STATIONS", "300"))
NATIONWIDE_HORIZON_MINUTES = int(os.getenv("NATIONWIDE_HORIZON_MINUTES", "90"))
NATIONWIDE_LOOKBACK_MINUTES = int(os.getenv("NATIONWIDE_LOOKBACK_MINUTES", "20"))

# Limites del store del dashboard (modo nacional: mucha mas telemetria).
DASHBOARD_MAX_TELEMETRY = int(os.getenv("DASHBOARD_MAX_TELEMETRY", "30000"))
DASHBOARD_MAX_ITINERARIES = int(os.getenv("DASHBOARD_MAX_ITINERARIES", "10000"))
DASHBOARD_MAX_INCIDENTS = int(os.getenv("DASHBOARD_MAX_INCIDENTS", "1000"))
DASHBOARD_MAX_TRIP_LABELS = int(os.getenv("DASHBOARD_MAX_TRIP_LABELS", "30000"))
# Filas maximas mostradas en las tablas grandes del dashboard.
DASHBOARD_TABLE_ROW_LIMIT = int(os.getenv("DASHBOARD_TABLE_ROW_LIMIT", "300"))

# DB API Marketplace - API Timetables (plan Free, 60 req/min, CC BY 4.0).
DB_CLIENT_ID = os.getenv("DB_CLIENT_ID", "").strip()
DB_API_KEY = os.getenv("DB_API_KEY", "").strip()
DB_TIMETABLES_BASE = os.getenv(
    "DB_TIMETABLES_BASE",
    "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1",
).rstrip("/")

# Estacion a monitorizar (por defecto Fulda Hbf).
DB_STATION_PATTERN = os.getenv("DB_STATION_PATTERN", "Fulda")
DB_STATION_EVA = os.getenv("DB_STATION_EVA", "8000115")
STATION_ID = os.getenv("STATION_ID", "FULDA_HBF")

# --------------------------------------------------------------------------- #
# Enriquecimiento nacional con numeros de tren reales (Opcion A).
# Se sondean las N estaciones mas concurridas del feed nacional con la API
# Timetables de DB (60 req/min) para construir un mapa
# (estacion, hora, linea) -> numero de tren, con el que se enriquece la
# telemetria GTFS-RT nacional (que solo trae la linea, p. ej. 'ICE 25').
# --------------------------------------------------------------------------- #
DB_ENRICH_ENABLED = os.getenv("DB_ENRICH_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
# Numero maximo de estaciones a son dear por ciclo (limite del plan Free: 60/min).
DB_ENRICH_MAX_STATIONS = int(os.getenv("DB_ENRICH_MAX_STATIONS", "40"))
# Horas de plan por estacion y sondeo.
DB_ENRICH_PLAN_HOURS = int(os.getenv("DB_ENRICH_PLAN_HOURS", "2"))
DB_ENRICH_REFRESH_SECONDS = int(os.getenv("DB_ENRICH_REFRESH_SECONDS", "180"))
# Estaciones sondeadas en paralelo (acelera el barrido sin superar 60 req/min).
DB_ENRICH_WORKERS = int(os.getenv("DB_ENRICH_WORKERS", "6"))
# Tolerancia (segundos) al casar la hora planificada del feed nacional con la
# del plan de DB para asignar el numero de tren.
DB_ENRICH_MATCH_TOLERANCE = int(os.getenv("DB_ENRICH_MATCH_TOLERANCE", "300"))

# Cadencia de sondeo y parametros de deteccion de conexiones.
DB_REFRESH_SECONDS = int(os.getenv("DB_REFRESH_SECONDS", "30"))
DB_PLAN_HOURS_AHEAD = int(os.getenv("DB_PLAN_HOURS_AHEAD", "1"))
MIN_TRANSFER_TIME_MINUTES = int(os.getenv("MIN_TRANSFER_TIME_MINUTES", "7"))
MAX_CONNECTION_WINDOW_MINUTES = int(os.getenv("MAX_CONNECTION_WINDOW_MINUTES", "45"))
DEFAULT_ESTIMATED_TRAVELERS = int(os.getenv("DEFAULT_ESTIMATED_TRAVELERS", "80"))


def db_enabled() -> bool:
    """Indica si debemos usar la API de DB (fuente real) con credenciales."""
    return (
        DATA_SOURCE == "db"
        and bool(DB_CLIENT_ID)
        and bool(DB_API_KEY)
    )

# --------------------------------------------------------------------------- #
# Modelos de eventos
# --------------------------------------------------------------------------- #
class TrainTelemetry(BaseModel):
    """Evento del topic train-telemetry.

    Convencion de tiempos: ``estimated_arrival``/``scheduled_arrival`` son la
    llegada al punto (o la salida, si el evento es de una parada origen sin
    llegada). ``estimated_departure``/``scheduled_departure`` son opcionales
    y traen la salida del mismo tren en la misma estacion (necesaria para
    evaluar el lado de salida de un transbordo).
    """

    trip_id: str
    train_number: str
    station_id: str
    delay_seconds: int
    scheduled_arrival: datetime
    estimated_arrival: datetime
    timestamp: float
    # Campos opcionales presentes en las fuentes reales (nacional/mock).
    station_name: str | None = None
    line: str | None = None
    destination: str | None = None
    scheduled_departure: datetime | None = None
    estimated_departure: datetime | None = None


class PassengerItinerary(BaseModel):
    """Evento del topic passenger-itineraries."""

    passenger_id: str
    origin_trip_id: str
    destination_trip_id: str
    transfer_station_id: str
    min_transfer_time_minutes: int = Field(gt=0)
    destination_scheduled_departure: datetime
    # Cohorte estimada de viajeros representada por este itinerario. GTFS-RT no
    # incluye ocupacion real, por lo que es una cifra de demostracion.
    # Se publica como rango (low/high) para comunicar la incertidumbre; si no
    # se indican, el consumidor usa el punto medio.
    estimated_travelers: int = Field(default=1, ge=1)
    estimated_travelers_low: int | None = Field(default=None, ge=1)
    estimated_travelers_high: int | None = Field(default=None, ge=1)


class OperationalIncident(BaseModel):
    """Evento del topic operational-incidents (capa operario / centro de control)."""

    station_id: str
    origin_trip: str
    destination_trip: str
    severity: str  # ALTA | MEDIA | BAJA
    recommended_action: str
    passengers_affected: int
    available_transfer_minutes: float
    required_minutes: int
    deficit_minutes: float
    delay_seconds: int
    # Rango estimado de viajeros afectados (low/high); si falta, se usa el
    # punto medio passengers_affected.
    passengers_affected_low: int | None = Field(default=None, ge=1)
    passengers_affected_high: int | None = Field(default=None, ge=1)
    created_at: str


# --------------------------------------------------------------------------- #
# Datos de demostracion
# --------------------------------------------------------------------------- #
# Horarios fijos: solo importan las diferencias entre llegada y salida.
DEMO_TRIPS = {
    # Tren que llega con retraso severo (15-20 min). Provoca la perdida.
    "TRIP_ICE_593": {
        "train_number": "ICE 593",
        "station_id": "FULDA_HBF",
        "event_type": "arrival",
        "scheduled_time": "2026-01-01T12:00:00",
        "delay_seconds": 1080,          # 18 min (nominal)
        "delay_variation": (900, 1200),  # 15-20 min, aleatorio en cada refresco
    },
    # Tren de destino del pasajero USR-101: sale 10 min despues de la llegada.
    "TRIP_ICE_590": {
        "train_number": "ICE 590",
        "station_id": "FULDA_HBF",
        "event_type": "departure",
        "scheduled_time": "2026-01-01T12:10:00",
        "delay_seconds": 0,
    },
    # Tren puntual: transbordo sin problemas para USR-102.
    "TRIP_ICE_591": {
        "train_number": "ICE 591",
        "station_id": "FULDA_HBF",
        "event_type": "arrival",
        "scheduled_time": "2026-01-01T12:00:00",
        "delay_seconds": 0,
    },
    # Tren de destino de USR-102: sale 30 min despues de la llegada.
    "TRIP_ICE_592": {
        "train_number": "ICE 592",
        "station_id": "FULDA_HBF",
        "event_type": "departure",
        "scheduled_time": "2026-01-01T12:30:00",
        "delay_seconds": 0,
    },
}

DEMO_ITINERARIES = [
    {
        # Retraso de ICE 593 => -8 min disponibles (menos que 7 requeridos).
        "passenger_id": "USR-101",
        "origin_trip_id": "TRIP_ICE_593",
        "destination_trip_id": "TRIP_ICE_590",
        "transfer_station_id": "FULDA_HBF",
        "min_transfer_time_minutes": 7,
        "destination_scheduled_departure": "2026-01-01T12:10:00",
        "estimated_travelers": 140,
        "estimated_travelers_low": 105,
        "estimated_travelers_high": 185,
    },
    {
        # ICE 591 puntual => 30 min disponibles (mas que 7 requeridos).
        "passenger_id": "USR-102",
        "origin_trip_id": "TRIP_ICE_591",
        "destination_trip_id": "TRIP_ICE_592",
        "transfer_station_id": "FULDA_HBF",
        "min_transfer_time_minutes": 7,
        "destination_scheduled_departure": "2026-01-01T12:30:00",
        "estimated_travelers": 60,
        "estimated_travelers_low": 45,
        "estimated_travelers_high": 80,
    },
]
